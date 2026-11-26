# silver_job.py
#
# PURPOSE: Raw ticks -> 1-minute OHLCV aggregation into Silver (Part 2 – Storage, Hudi MoR)
#
# STEPS TO IMPLEMENT:
#
# 1. CREATE A SPARK SESSION
#    - Same Hudi/Kryo configuration as bronze_job.py.
#
# 2. READ FROM KAFKA OR BRONZE (Structured Streaming)
#    - Option A (streaming): read the Kafka topic again, parse ticks the same way as Bronze.
#    - Option B (micro-batch): read the Bronze Hudi table with spark.readStream on files.
#    - Add a watermark of WATERMARK (default 2 minutes) on the event_time column.
#
# 3. COMPUTE 1-MINUTE OHLCV PER SYMBOL
#    - Use window("event_time", "1 minute") grouped by symbol.
#    - Aggregate:
#        open   = first(price, ignorenulls=True)
#        high   = max(price)
#        low    = min(price)
#        close  = last(price, ignorenulls=True)
#        volume = sum(quantity)
#    - Add a "minute" column = window start (used as part of the record key).
#
# 4. BUILD THE COMPOSITE RECORD KEY
#    - Concatenate symbol and minute as "SYMBOL_YYYY-MM-DDTHH:mm" to form a unique record key.
#    - This key enables Hudi upserts to handle late-arriving data correctly.
#
# 5. WRITE TO HUDI (Silver table, Merge-on-Read, upsert)
#    - Use writeStream with format("hudi").
#    - Set Hudi options:
#        hoodie.table.name                          = "silver_ohlcv"
#        hoodie.datasource.write.recordkey.field    = composite key column
#        hoodie.datasource.write.precombine.field   = "event_time" (or window end)
#        hoodie.datasource.write.operation          = "upsert"
#        hoodie.datasource.write.table.type         = "MERGE_ON_READ"
#        hoodie.compact.inline                      = "false"  (Airflow triggers compaction)
#    - Set output path to LAKE_PATH/silver.
#    - Use trigger(processingTime="1 minute") and checkpointLocation.
#
# 6. AWAIT TERMINATION
