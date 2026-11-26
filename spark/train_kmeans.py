# train_kmeans.py
#
# PURPOSE: Offline KMeans training on historical data for anomaly detection (Part 3 – MLlib)
#
# STEPS TO IMPLEMENT:
#
# 1. CREATE A SPARK SESSION (batch, no streaming needed here)
#
# 2. LOAD HISTORICAL DATA
#    - Option A: Read historical klines (OHLCV) from Binance REST API for all SYMBOLS.
#    - Option B: Load a Kaggle crypto dataset (CSV or Parquet) stored locally.
#    - Ensure the data covers a representative period (at least several weeks).
#
# 3. COMPUTE FEATURES ON HISTORICAL DATA
#    - Return:    (close - prev_close) / prev_close  using lag() window function.
#    - Volatility: rolling stddev of return over a fixed window (e.g., 5 periods).
#    - Volume ratio: volume / rolling average volume.
#    - Drop rows with nulls (from lag/rolling computations at the start of each partition).
#
# 4. ASSEMBLE THE FEATURE VECTOR
#    - Use VectorAssembler with inputCols = ["return", "volatility", "volume_ratio"].
#    - Apply StandardScaler to normalize features before clustering.
#
# 5. TRAIN KMEANS
#    - Choose k (number of clusters); a good default is k=5, tune with the elbow method.
#    - Use KMeans from pyspark.ml.clustering with a fixed seed for reproducibility.
#    - Fit the pipeline (scaler + KMeans) on the assembled feature DataFrame.
#
# 6. COMPUTE THE ANOMALY THRESHOLD
#    - For each training point, compute the distance to its assigned centroid.
#    - Compute the 99th percentile of all distances using approxQuantile().
#    - Save this threshold value to models/kmeans_model/threshold.json.
#
# 7. SAVE THE MODEL
#    - Save the fitted PipelineModel (scaler + KMeans) to models/kmeans_model/
#      using model.save(path) so gold_job.py can load it with PipelineModel.load(path).
#
# 8. LOG METRICS
#    - Print cluster sizes and silhouette score for a quick sanity check.
