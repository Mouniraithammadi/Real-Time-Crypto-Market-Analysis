"""
anomaly_stream.py
─────────────────
Part 3 – MLlib  |  Standalone streaming anomaly detector

Reads enriched rows from the Gold Hudi table (which already contain
``return``, ``volatility`` and ``volume_ratio``), applies the pre-trained
KMeans PipelineModel to assign clusters, computes each point's Euclidean
distance to its centroid, and writes records that exceed the saved 99th-
percentile threshold to the Alerts table.

This job is intentionally kept separate from gold_job.py so it can be
restarted independently (e.g., after retraining the model) without
interrupting the indicator-computation stream.

Usage
-----
spark-submit \\
  --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1 \\
  spark/anomaly_stream.py
"""

import json
import math
import os

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import BooleanType, DoubleType

# ─── Configuration ────────────────────────────────────────────────────────────
LAKE_PATH       = os.getenv("LAKE_PATH", "./data")
MODEL_PATH      = os.getenv("MODEL_PATH", "./models/kmeans_model")
GOLD_PATH       = os.path.join(LAKE_PATH, "gold")
ALERTS_PATH     = os.path.join(LAKE_PATH, "gold", "alerts")
CHECKPOINT_PATH = os.path.join(LAKE_PATH, "_checkpoints", "anomaly_stream")

HUDI_ALERTS = {
    "hoodie.table.name":                            "gold_alerts",
    "hoodie.datasource.write.recordkey.field":      "record_key",
    "hoodie.datasource.write.precombine.field":     "window_end",
    "hoodie.datasource.write.operation":            "insert",
    "hoodie.datasource.write.table.type":           "COPY_ON_WRITE",
    "hoodie.datasource.write.partitionpath.field":  "symbol",
    "hoodie.datasource.hive_sync.enable":           "false",
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("anomaly_stream")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )


def load_model_and_threshold():
    model = PipelineModel.load(MODEL_PATH)
    threshold_file = os.path.join(MODEL_PATH, "threshold.json")
    with open(threshold_file) as fh:
        threshold = json.load(fh)["threshold"]
    return model, threshold


def make_distance_udf(centers: list):
    """Create a UDF that returns the Euclidean distance from a feature vector to its centroid."""
    def _dist(features, cluster_id):
        if features is None or cluster_id is None:
            return None
        c = centers[cluster_id]
        return float(math.sqrt(sum((a - b) ** 2 for a, b in zip(features, c))))
    return udf(_dist, DoubleType())


def process_batch(batch_df: DataFrame, batch_id: int, model, threshold: float) -> None:
    """
    foreachBatch handler:
      1. Run the KMeans pipeline on the micro-batch.
      2. Compute distance to assigned centroid.
      3. Flag anomalies and write them to the Alerts Hudi table.
    """
    if batch_df.isEmpty():
        return

    kmeans_model = model.stages[-1]
    centers = kmeans_model.clusterCenters()
    dist_udf = make_distance_udf(centers)

    predictions = model.transform(batch_df)

    alerts = (
        predictions
        .withColumn(
            "distance_to_centroid",
            dist_udf(col("features"), col("cluster")),
        )
        .withColumn(
            "is_anomaly",
            col("distance_to_centroid") > lit(threshold),
        )
        .filter(col("is_anomaly") == True)
        # Keep only the columns relevant for the alerts table
        .select(
            "record_key",
            "symbol",
            "window_end",
            "return",
            "volatility",
            "volume_ratio",
            "cluster",
            "distance_to_centroid",
            "is_anomaly",
        )
        .drop("raw_features", "features")
    )

    if not alerts.isEmpty():
        count = alerts.count()
        (
            alerts.write
            .format("hudi")
            .options(**HUDI_ALERTS)
            .mode("append")
            .save(ALERTS_PATH)
        )
        print(f"[anomaly_stream] batch {batch_id}: {count} anomaly alert(s) written.")
    else:
        print(f"[anomaly_stream] batch {batch_id}: no anomalies detected.")


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Load model and threshold ────────────────────────────────────────
    model, threshold = load_model_and_threshold()
    print(f"[anomaly_stream] Model loaded. Threshold = {threshold:.6f}")

    # ── 2. Stream the Gold table ───────────────────────────────────────────
    # The Gold table must already exist (written by gold_job.py).
    # We read it as a streaming source using Spark's file-based source.
    gold_stream = (
        spark.readStream
        .format("hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(GOLD_PATH)
    )

    # ── 3. Apply anomaly detection in foreachBatch ─────────────────────────
    query = (
        gold_stream.writeStream
        .foreachBatch(
            lambda df, bid: process_batch(df, bid, model, threshold)
        )
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="1 minute")
        .outputMode("append")
        .start()
    )

    print(f"[anomaly_stream] Monitoring Gold → {GOLD_PATH}")
    print(f"[anomaly_stream] Writing alerts  → {ALERTS_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
