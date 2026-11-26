-- queries.sql
-- PURPOSE: Spark SQL analytical queries for asset comparison (Part 3 – Spark SQL)
-- Run these via spark.sql() or spark-sql shell after registering the Gold table as a temp view.

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 1: BTC vs ETH price correlation
-- Calculate the Pearson correlation between BTC and ETH close prices
-- over the last 24 hours using the Gold indicators table.
-- Expected columns: correlation_coefficient
-- ─────────────────────────────────────────────────────────────────────────────

-- TODO: implement BTC vs ETH correlation query here

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 2: Relative performance of all symbols
-- For each symbol, compute the percentage change from the opening price
-- of the current day to the latest close price.
-- Expected columns: symbol, open_price, latest_close, pct_change, ranked_position
-- ─────────────────────────────────────────────────────────────────────────────

-- TODO: implement relative performance query here

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 3: Ranking by volatility
-- Rank all symbols by their average volatility over the last hour.
-- Expected columns: rank, symbol, avg_volatility, anomaly_count
-- ─────────────────────────────────────────────────────────────────────────────

-- TODO: implement volatility ranking query here

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 4: Anomaly summary
-- Count anomalies per symbol in the last 24 hours and flag any symbol
-- that has more than N anomalies (threshold to be defined).
-- Expected columns: symbol, anomaly_count, is_high_alert
-- ─────────────────────────────────────────────────────────────────────────────

-- TODO: implement anomaly summary query here

