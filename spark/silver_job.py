"""
silver_job.py
─────────────
Part 2 – Storage  |  Raw ticks → 1-minute OHLCV (Hudi Merge-on-Read)

Reads from the same Kafka topic as the Bronze job, applies a 2-minute
watermark and groups ticks into 1-minute tumbling windows to produce
standard OHLCV candles per symbol.  Late data is handled gracefully by
Hudi upsert (the composite record key symbol+minute is idempotent).

Usage
-----
spark-submit \\
  --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1,\\
             org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \\
  spark/silver_job.py
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    date_format,
    first,
    from_json,
    last,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "prix")
LAKE_PATH       = os.getenv("LAKE_PATH", "./data")
WATERMARK       = os.getenv("WATERMARK", "2 minutes")
CHECKPOINT_PATH = os.path.join(LAKE_PATH, "_checkpoints", "silver")
SILVER_PATH     = os.path.join(LAKE_PATH, "silver")

TICK_SCHEMA = StructType([
    StructField("symbol",     StringType(),    nullable=False),
    StructField("price",      DoubleType(),    nullable=False),
    StructField("quantity",   DoubleType(),    nullable=False),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("trade_id",   LongType(),      nullable=False),
])

# ─── Hudi write options ───────────────────────────────────────────────────────
HUDI_OPTIONS = {
    "hoodie.table.name":                            "silver_ohlcv",
    "hoodie.datasource.write.recordkey.field":      "record_key",
    "hoodie.datasource.write.precombine.field":     "window_end",
    "hoodie.datasource.write.operation":            "upsert",
    "hoodie.datasource.write.table.type":           "MERGE_ON_READ",
    # Airflow triggers compaction; disable inline compaction to reduce write latency
    "hoodie.compact.inline":                        "false",
    "hoodie.compact.inline.max.delta.commits":      "10",
    "hoodie.datasource.write.partitionpath.field":  "symbol",
    "hoodie.datasource.hive_sync.enable":           "false",
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_job")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )


def parse_ticks(raw_stream) -> DataFrame:
    """Cast Kafka bytes → string → typed tick struct."""
    return (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), TICK_SCHEMA).alias("tick"))
        .select("tick.*")
        .filter(col("trade_id").isNotNull() & col("event_time").isNotNull())
    )


def compute_ohlcv(ticks: DataFrame) -> DataFrame:
    """
    Apply watermark + 1-minute tumbling window and aggregate to OHLCV.
    The composite record key (symbol + minute string) enables Hudi upserts
    to overwrite partial candles when late data arrives within the watermark.
    """
    windowed = (
        ticks
        .withWatermark("event_time", WATERMARK)
        .groupBy(
            col("symbol"),
            window(col("event_time"), "1 minute").alias("win"),
        )
        .agg(
            first("price",  ignorenulls=True).alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            last("price",   ignorenulls=True).alias("close"),
            spark_sum("quantity").alias("volume"),
        )
    )

    return (
        windowed
        .withColumn("minute",     col("win.start"))
        .withColumn("window_end", col("win.end"))
        # Composite record key: "BTCUSDT_2026-11-25T10:15"
        .withColumn(
            "record_key",
            concat(
                col("symbol"),
                lit("_"),
                date_format(col("minute"), "yyyy-MM-dd'T'HH:mm"),
            ),
        )
        .drop("win")
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read from Kafka ─────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    ticks  = parse_ticks(raw_stream)
    ohlcv  = compute_ohlcv(ticks)

    # ── 2. Write to Hudi (Silver, MoR, upsert) ────────────────────────────
    query = (
        ohlcv.writeStream
        .format("hudi")
        .options(**HUDI_OPTIONS)
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="1 minute")
        .start()
    )

    print(f"[silver_job] Streaming OHLCV → {SILVER_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
