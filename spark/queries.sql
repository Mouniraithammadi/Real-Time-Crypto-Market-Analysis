-- queries.sql
-- PURPOSE : Spark SQL analytical queries for asset comparison (Part 3)
-- Usage   : Register the Gold Hudi table as a temp view, then run:
--             spark.sql(open("spark/queries.sql").read())
--           Or run individual queries via spark-sql shell.
--
-- Assumptions:
--   • Temp view "gold"   exposes columns:
--       symbol, window_start, window_end, open, high, low, close,
--       volume, sma, ema, volatility, volume_ratio, return,
--       cluster, distance_to_centroid, is_anomaly
--   • Temp view "alerts" exposes the same schema filtered to is_anomaly = true


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 1: BTC vs ETH price correlation
-- Pearson correlation between BTC and ETH close prices over the last 24 hours.
-- Expected output column: correlation_coefficient
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    corr(btc.close, eth.close) AS correlation_coefficient
FROM (
    SELECT window_start, close
    FROM gold
    WHERE symbol = 'BTCUSDT'
      AND window_start >= current_timestamp() - INTERVAL 24 HOURS
) AS btc
JOIN (
    SELECT window_start, close
    FROM gold
    WHERE symbol = 'ETHUSDT'
      AND window_start >= current_timestamp() - INTERVAL 24 HOURS
) AS eth
  ON btc.window_start = eth.window_start;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 2: Relative performance of all symbols
-- For each symbol, percentage change from today's first open to the latest close.
-- Expected output columns: symbol, open_price, latest_close, pct_change, ranked_position
-- ─────────────────────────────────────────────────────────────────────────────

WITH daily AS (
    SELECT
        symbol,
        -- First open price of today (UTC day boundary)
        first_value(open) OVER (
            PARTITION BY symbol
            ORDER BY window_start ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS open_price,
        -- Latest close price of today
        last_value(close) OVER (
            PARTITION BY symbol
            ORDER BY window_start ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS latest_close,
        window_start
    FROM gold
    WHERE window_start >= date_trunc('day', current_timestamp())
),
agg AS (
    SELECT DISTINCT
        symbol,
        open_price,
        latest_close,
        ROUND(((latest_close - open_price) / open_price) * 100, 4) AS pct_change
    FROM daily
)
SELECT
    symbol,
    open_price,
    latest_close,
    pct_change,
    RANK() OVER (ORDER BY pct_change DESC) AS ranked_position
FROM agg
ORDER BY ranked_position;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 3: Ranking by volatility
-- Rank all symbols by average volatility over the last hour.
-- Also include the anomaly count in that same period.
-- Expected output columns: rank, symbol, avg_volatility, anomaly_count
-- ─────────────────────────────────────────────────────────────────────────────

WITH vol_agg AS (
    SELECT
        symbol,
        AVG(volatility)             AS avg_volatility,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count
    FROM gold
    WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY symbol
)
SELECT
    RANK() OVER (ORDER BY avg_volatility DESC) AS rank,
    symbol,
    ROUND(avg_volatility, 6)                   AS avg_volatility,
    anomaly_count
FROM vol_agg
ORDER BY rank;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 4: Anomaly summary
-- Count anomalies per symbol in the last 24 hours.
-- Flag any symbol whose anomaly count exceeds the threshold (default: 10).
-- Expected output columns: symbol, anomaly_count, is_high_alert
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    symbol,
    COUNT(*)                        AS anomaly_count,
    COUNT(*) > 10                   AS is_high_alert
FROM alerts
WHERE window_end >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY symbol
ORDER BY anomaly_count DESC;
