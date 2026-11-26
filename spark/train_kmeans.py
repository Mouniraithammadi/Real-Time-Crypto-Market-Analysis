"""
train_kmeans.py
───────────────
Part 3 – MLlib  |  Offline KMeans training for anomaly detection

Loads historical OHLCV data (from Binance klines REST API or a local
CSV/Parquet file), engineers three features (return, volatility,
volume_ratio), trains a KMeans pipeline (StandardScaler + KMeans),
computes the 99th-percentile distance threshold, and saves both the
model and the threshold so ``gold_job.py`` and ``anomaly_stream.py``
can load them at runtime.

Usage
-----
spark-submit spark/train_kmeans.py
  --data_path  ./data/historical/btcusdt_klines.csv   # or parquet
  --k          5
  --seed       42
"""

import json
import math
import os
import sys
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lag,
    avg as spark_avg,
    stddev,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

# ─── Configuration ────────────────────────────────────────────────────────────
LAKE_PATH   = os.getenv("LAKE_PATH", "./data")
MODEL_PATH  = os.getenv("MODEL_PATH", "./models/kmeans_model")
SYMBOLS     = os.getenv("SYMBOLS", "btcusdt,ethusdt,bnbusdt,solusdt").split(",")
K           = int(os.getenv("KMEANS_K", "5"))
SEED        = int(os.getenv("KMEANS_SEED", "42"))
THRESHOLD_PERCENTILE = 0.99
VOLATILITY_WINDOW    = 5   # periods for rolling stddev


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("train_kmeans")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def load_data(spark: SparkSession, data_path: str):
    """
    Load historical OHLCV data.
    Accepts CSV with columns: symbol, open_time, open, high, low, close, volume
    or a Hudi/Parquet Silver table with the same schema.
    """
    ext = Path(data_path).suffix.lower()
    if ext in (".parquet", ""):
        df = spark.read.parquet(data_path)
    else:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(data_path)
        )
    return df


def engineer_features(df):
    """
    Compute:
      return       = (close - prev_close) / prev_close
      volatility   = rolling stddev of return over VOLATILITY_WINDOW periods
      volume_ratio = volume / rolling avg volume over VOLATILITY_WINDOW periods
    """
    sym_time_window = (
        Window.partitionBy("symbol")
              .orderBy("minute")
    )
    rolling_window = (
        Window.partitionBy("symbol")
              .orderBy("minute")
              .rowsBetween(-VOLATILITY_WINDOW + 1, 0)
    )

    df = (
        df
        .withColumn("prev_close", lag("close", 1).over(sym_time_window))
        .withColumn(
            "return",
            ((col("close") - col("prev_close")) / col("prev_close")).cast(DoubleType()),
        )
        .withColumn("volatility",    stddev("return").over(rolling_window))
        .withColumn("rolling_vol",   spark_avg("volume").over(rolling_window))
        .withColumn(
            "volume_ratio",
            (col("volume") / col("rolling_vol")).cast(DoubleType()),
        )
        # Drop rows with nulls (from lag / rolling at partition boundaries)
        .dropna(subset=["return", "volatility", "volume_ratio"])
        .filter(
            col("return").isNotNull()
            & col("volatility").isNotNull()
            & col("volume_ratio").isNotNull()
        )
    )
    return df


def euclidean_distance(features, center):
    """Compute Euclidean distance between a DenseVector and a list/array center."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(features, center)))


def main(data_path: str = None, k: int = K) -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Load historical data ────────────────────────────────────────────
    if data_path is None:
        # Fallback: try to read from the Silver Hudi table
        data_path = os.path.join(LAKE_PATH, "silver")
        print(f"[train_kmeans] No data_path provided. Reading Silver table: {data_path}")

    df = load_data(spark, data_path)
    print(f"[train_kmeans] Loaded {df.count()} rows from {data_path}")

    # ── 2. Feature engineering ─────────────────────────────────────────────
    df_feat = engineer_features(df)
    print(f"[train_kmeans] Rows after feature engineering: {df_feat.count()}")

    # ── 3. Assemble feature vector ─────────────────────────────────────────
    assembler = VectorAssembler(
        inputCols=["return", "volatility", "volume_ratio"],
        outputCol="raw_features",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=k,
        seed=SEED,
    )
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])

    # ── 4. Train ───────────────────────────────────────────────────────────
    print(f"[train_kmeans] Training KMeans with k={k}, seed={SEED} …")
    model = pipeline.fit(df_feat)

    # ── 5. Evaluate ────────────────────────────────────────────────────────
    predictions = model.transform(df_feat)

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
    )
    silhouette = evaluator.evaluate(predictions)
    print(f"[train_kmeans] Silhouette score: {silhouette:.4f}")

    # Cluster sizes
    cluster_sizes = (
        predictions.groupBy("cluster").count().orderBy("cluster").collect()
    )
    for row in cluster_sizes:
        print(f"  Cluster {row['cluster']}: {row['count']} points")

    # ── 6. Compute 99th-percentile distance threshold ──────────────────────
    kmeans_model = model.stages[-1]  # the fitted KMeansModel
    centers = kmeans_model.clusterCenters()

    # Collect feature vectors and clusters to driver for distance computation
    # (training data is bounded; this is acceptable for an offline job)
    rows = predictions.select("features", "cluster").collect()

    distances = [
        euclidean_distance(row["features"], centers[row["cluster"]])
        for row in rows
    ]
    distances.sort()

    idx = int(THRESHOLD_PERCENTILE * len(distances))
    threshold = distances[min(idx, len(distances) - 1)]
    print(f"[train_kmeans] 99th-percentile distance threshold: {threshold:.6f}")

    # ── 7. Save model and threshold ────────────────────────────────────────
    Path(MODEL_PATH).mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(MODEL_PATH)
    print(f"[train_kmeans] Model saved → {MODEL_PATH}")

    threshold_file = os.path.join(MODEL_PATH, "threshold.json")
    with open(threshold_file, "w") as fh:
        json.dump({"threshold": threshold, "percentile": THRESHOLD_PERCENTILE}, fh)
    print(f"[train_kmeans] Threshold saved → {threshold_file}")

    spark.stop()


if __name__ == "__main__":
    # Allow optional CLI overrides: python train_kmeans.py <data_path> [k]
    _data_path = sys.argv[1] if len(sys.argv) > 1 else None
    _k         = int(sys.argv[2]) if len(sys.argv) > 2 else K
    main(data_path=_data_path, k=_k)
