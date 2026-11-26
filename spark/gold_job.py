# gold_job.py
#
# PURPOSE: Silver OHLCV -> Technical indicators and alerts in Gold (Part 3 – Processing)
#
# STEPS TO IMPLEMENT:
#
# 1. CREATE A SPARK SESSION
#    - Same Hudi/Kryo configuration as the other jobs.
#
# 2. READ FROM SILVER (Structured Streaming with sliding window)
#    - Read the Silver Hudi table or Kafka topic.
#    - Add a watermark of WATERMARK (2 minutes) on event_time.
#    - Define a sliding window of WINDOW (5 minutes) with SLIDE (1 minute) on event_time.
#    - Group by symbol and sliding window.
#
# 3. COMPUTE TECHNICAL INDICATORS
#    - SMA (Simple Moving Average): average of close prices within the window.
#    - EMA (Exponential Moving Average): implement or approximate using a weighted average.
#    - Volatility: standard deviation of period-over-period returns within the window.
#        return_i  = (close_i - close_{i-1}) / close_{i-1}
#        volatility = stddev(return_i)
#    - Volume ratio: window volume / average volume over a longer reference period.
#
# 4. LOAD AND APPLY THE KMEANS MODEL (anomaly detection)
#    - Load the pre-trained KMeans PipelineModel from models/kmeans_model/.
#    - Assemble feature vector: [return, volatility, volume_ratio].
#    - Use the model to predict the cluster for each row.
#    - Compute the Euclidean distance from each point to its assigned centroid.
#    - Load the 99th-percentile threshold saved during training.
#    - Flag rows where distance > threshold as anomalies (is_anomaly = True).
#
# 5. WRITE INDICATORS TO GOLD
#    - Write the enriched DataFrame (OHLCV + indicators + is_anomaly) to LAKE_PATH/gold
#      using Hudi with operation = "upsert".
#
# 6. WRITE ANOMALIES TO THE ALERTS TABLE
#    - Filter rows where is_anomaly = True.
#    - Write them to a separate Hudi table at LAKE_PATH/gold/alerts.
#
# 7. AWAIT TERMINATION
