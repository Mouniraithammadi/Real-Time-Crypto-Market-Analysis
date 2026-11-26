"""
bronze_job.py
─────────────
Part 2 – Storage  |  Kafka → Bronze layer (raw ticks)

Reads every trade tick from the Kafka topic ``prix``, parses the JSON
payload, and writes it to a Hudi Copy-on-Write table at LAKE_PATH/bronze.
No transformation is applied at this layer; the goal is a faithful,
append-only replica of every raw event.

Usage
-----
spark-submit \\
  --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1,\\
             org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \\
  spark/bronze_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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
CHECKPOINT_PATH = os.path.join(LAKE_PATH, "_checkpoints", "bronze")
BRONZE_PATH     = os.path.join(LAKE_PATH, "bronze")

# ─── Tick schema ─────────────────────────────────────────────────────────────
# Every Kafka message value is a JSON object matching this structure:
# {"symbol":"BTCUSDT","price":67250.12,"quantity":0.0042,
#  "event_time":"2026-11-25T10:15:03.412Z","trade_id":3849201123}
TICK_SCHEMA = StructType([
    StructField("symbol",     StringType(),    nullable=False),
    StructField("price",      DoubleType(),    nullable=False),
    StructField("quantity",   DoubleType(),    nullable=False),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("trade_id",   LongType(),      nullable=False),
])

# ─── Hudi write options ───────────────────────────────────────────────────────
HUDI_OPTIONS = {
    "hoodie.table.name":                       "bronze_ticks",
    "hoodie.datasource.write.recordkey.field": "trade_id",
    "hoodie.datasource.write.precombine.field":"event_time",
    "hoodie.datasource.write.operation":       "insert",
    "hoodie.datasource.write.table.type":      "COPY_ON_WRITE",
    # partitioning by symbol keeps each coin's data co-located
    "hoodie.datasource.write.partitionpath.field": "symbol",
    "hoodie.datasource.hive_sync.enable":      "false",
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze_job")
        # Hudi requires Kryo serializer
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read from Kafka ────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka delivers the payload as bytes; cast to string first
    value_stream = raw_stream.selectExpr("CAST(value AS STRING) AS json_str")

    # ── 2. Parse JSON ticks ───────────────────────────────────────────────────
    parsed = (
        value_stream
        .select(from_json(col("json_str"), TICK_SCHEMA).alias("tick"))
        .select("tick.*")
        # Drop malformed records where mandatory fields are null
        .filter(col("trade_id").isNotNull() & col("symbol").isNotNull())
    )

    # ── 3. Write to Hudi (Bronze, append-only) ────────────────────────────────
    query = (
        parsed.writeStream
        .format("hudi")
        .options(**HUDI_OPTIONS)
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"[bronze_job] Streaming ticks → {BRONZE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
