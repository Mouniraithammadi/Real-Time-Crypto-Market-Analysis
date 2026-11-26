# Real-Time Crypto Market Analysis

A streaming data pipeline that ingests live crypto prices from Binance, stores them in a Hudi lakehouse (Bronze / Silver / Gold), computes technical indicators on sliding windows with Spark, detects abnormal market movements with KMeans, and displays everything on a live Grafana dashboard.

> Analysis project only. Nothing here is investment advice.

## Architecture

```
Binance WebSocket
       |
       v
Python producer ---> Kafka (topic "prix")
                           |
                           v
              Spark Structured Streaming
              (windows + watermark, KMeans, SQL)
                           |
                           v
        Hudi lakehouse: Bronze -> Silver -> Gold
                           |
              +------------+------------+
              v                         v
          Grafana                 Airflow (daily)
   (live charts, alerts)    (compaction, quality, report)
```

| Layer | Tool | Role |
|---|---|---|
| Source | Binance WebSocket | Live trades (BTC, ETH, BNB, SOL) |
| Ingestion | Kafka | Topic `prix`, key = symbol |
| Processing | PySpark (Structured Streaming, SQL, MLlib) | Indicators, anomaly detection, comparisons |
| Storage | Apache Hudi on MinIO / local FS | Bronze, Silver (MoR), Gold |
| Orchestration | Airflow | Daily compaction, quality check, report |
| Visualization | Grafana | Live prices, indicators, alerts |

## Project structure

```
.
├── docker-compose.yml
├── README.md
├── producer/
│   ├── producer.py          # Binance WebSocket -> Kafka
│   └── requirements.txt
├── spark/
│   ├── bronze_job.py        # Kafka -> raw ticks
│   ├── silver_job.py        # ticks -> 1-min OHLCV (Hudi MoR)
│   ├── gold_job.py          # indicators (SMA, EMA, volatility)
│   ├── train_kmeans.py      # offline training on historical data
│   ├── anomaly_stream.py    # apply KMeans in streaming
│   └── queries.sql          # asset comparisons (Spark SQL)
├── airflow/
│   └── dags/
│       └── daily_maintenance.py
├── grafana/
│   ├── provisioning/
│   └── dashboards/
├── models/
│   └── kmeans_model/
└── data/
    ├── bronze/
    ├── silver/
    └── gold/
```

## Prerequisites

- Docker and Docker Compose
- Python 3.10+
- Java 11 or 17 (for Spark)
- Spark 3.4+ with the matching Hudi bundle (`hudi-spark3.4-bundle`)

## Quick start

```bash
# 1. Start the infrastructure (Kafka, MinIO, Airflow, Grafana)
docker compose up -d

# 2. Create the Kafka topic
docker exec -it kafka kafka-topics.sh --create \
  --topic prix --partitions 4 --replication-factor 1 \
  --bootstrap-server localhost:9092

# 3. Start the producer
cd producer
pip install -r requirements.txt
python producer.py

# 4. Check that ticks arrive
docker exec -it kafka kafka-console-consumer.sh \
  --topic prix --from-beginning --bootstrap-server localhost:9092

# 5. Start the Spark jobs
spark-submit --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1 spark/bronze_job.py
spark-submit --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1 spark/silver_job.py
spark-submit --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1 spark/gold_job.py

# 6. Open the dashboards
# Grafana: http://localhost:3000   Airflow: http://localhost:8080
```

## Tick schema

Every message in topic `prix` is JSON:

```json
{
  "symbol": "BTCUSDT",
  "price": 67250.12,
  "quantity": 0.0042,
  "event_time": "2026-11-25T10:15:03.412Z",
  "trade_id": 3849201123
}
```

## The four parts

### Part 1: Ingestion (Kafka + Binance)

- Connect to `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/bnbusdt@trade/solusdt@trade`.
- Parse each message into the tick schema above.
- Send to topic `prix` with `key = symbol`.
- Reconnect automatically with backoff and log errors.

**Deliverable:** continuous stream of ticks in `prix`.

### Part 2: Storage (Hudi lakehouse)

- **Bronze:** raw ticks, no transformation.
- **Silver:** 1-minute OHLCV per symbol, Hudi Merge-on-Read.
  - record key = `symbol + minute`
  - precombine field = `event_time`
  - operation = `upsert` (handles late data)
  - inline compaction disabled (Airflow triggers it)
- **Gold:** indicators and alerts tables.

**Deliverable:** Bronze, Silver and Gold tables readable with Spark.

### Part 3: Processing (Spark)

- **Structured Streaming:** watermark of 2 minutes, sliding window of 5 minutes with a 1-minute slide. Computes SMA, EMA, volatility (std dev of returns) and volume ratio.
- **MLlib KMeans:** features are return, volatility and volume ratio. Train offline on historical data (Binance klines or Kaggle), flag points whose distance to their centroid exceeds the 99th percentile, write them to the `alerts` table.
- **Spark SQL:** BTC vs ETH correlation, relative performance, ranking by volatility.

**Deliverable:** Gold indicators and anomalies updated in near real time.

### Part 4: Orchestration and dashboard

**Airflow, daily DAG (`daily_maintenance`):**

1. Trigger Hudi compaction on Silver.
2. Data quality check (missing minutes, duplicates).
3. Generate the daily report (top movers, volatility, anomaly count).
4. Save or send the report.

**Grafana:**

- Data source: PostgreSQL, Trino or Spark Thrift Server pointing at Gold and alerts.
- Panels: live price, moving averages, volatility, anomaly markers.
- Alert rules: anomaly count and sudden price move.

**Deliverable:** automated daily DAG and a live dashboard with alerts.

## Team organization

| Part | Owner | Depends on |
|---|---|---|
| 1. Ingestion | Dev 1 | Nothing, starts first |
| 2. Storage | Dev 2 | Tick schema from Part 1 |
| 3. Processing | Dev 3 | Topic `prix` and Gold schema; KMeans training can start immediately |
| 4. Orchestration + dashboard | Dev 4 | Table paths from Part 2; start Grafana on fake data |

Suggested order: Part 1 and Part 2 (Bronze) in parallel, then Part 3 streaming indicators, then Silver/Gold and anomalies, then Part 4.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `prix` | Topic name |
| `SYMBOLS` | `btcusdt,ethusdt,bnbusdt,solusdt` | Binance symbols to stream |
| `LAKE_PATH` | `./data` | Root of the data lake |
| `WATERMARK` | `2 minutes` | Late data tolerance |
| `WINDOW` / `SLIDE` | `5 minutes` / `1 minute` | Sliding window |

## Troubleshooting

- **No messages in `prix`:** check the producer logs and that the Binance stream URL is reachable.
- **Spark cannot find Hudi classes:** make sure the `--packages` bundle matches your Spark and Scala versions.
- **Duplicate Silver rows:** verify the record key (`symbol + minute`) and precombine field (`event_time`).
- **Grafana shows no data:** check the data source connection and the table names in the queries.

## Disclaimer

This project is for data analysis and learning purposes. It does not constitute investment advice.
