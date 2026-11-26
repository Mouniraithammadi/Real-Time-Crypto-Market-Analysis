# bronze_job.py
#
# PURPOSE: Kafka -> Bronze layer (Part 2 – Storage, raw ticks)
#
# STEPS TO IMPLEMENT:
#
# 1. CREATE A SPARK SESSION
#    - Include the Hudi bundle in the jars/packages config.
#    - Set "spark.serializer" to KryoSerializer (required by Hudi).
#    - Set "spark.sql.extensions" and "spark.sql.catalog.spark_catalog" for Hudi.
#
# 2. READ FROM KAFKA (Structured Streaming)
#    - Use spark.readStream with format("kafka").
#    - Set kafka.bootstrap.servers = KAFKA_BOOTSTRAP and subscribe = KAFKA_TOPIC.
#    - Cast the Kafka "value" column from bytes to string.
#
# 3. PARSE JSON TICKS
#    - Define the tick schema: symbol (string), price (double), quantity (double),
#      event_time (timestamp), trade_id (long).
#    - Use from_json() to deserialize each Kafka value into the schema.
#    - Select the nested struct fields as top-level columns.
#
# 4. WRITE TO HUDI (Bronze table, append only)
#    - Use writeStream with format("hudi").
#    - Set Hudi options:
#        hoodie.table.name                          = "bronze_ticks"
#        hoodie.datasource.write.recordkey.field    = "trade_id"
#        hoodie.datasource.write.precombine.field   = "event_time"
#        hoodie.datasource.write.operation          = "insert"   (no dedup needed at Bronze)
#        hoodie.datasource.write.table.type         = "COPY_ON_WRITE"
#    - Set output path to LAKE_PATH/bronze.
#    - Use trigger(processingTime="10 seconds") and checkpointLocation.
#
# 5. AWAIT TERMINATION
#    - Call query.awaitTermination() to keep the job running.
