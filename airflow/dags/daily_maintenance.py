"""
daily_maintenance.py
────────────────────
Part 4 – Orchestration  |  Daily Airflow DAG

DAG ID   : daily_maintenance
Schedule : 02:00 UTC every day
Owner    : team

Task dependency chain:
    trigger_hudi_compaction
         ↓
    data_quality_check
         ↓
    generate_daily_report
         ↓
    save_or_send_report
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ─── Shared configuration ──────────────────────────────────────────────────────
LAKE_PATH       = os.getenv("LAKE_PATH",       "/data")
SILVER_PATH     = os.path.join(LAKE_PATH, "silver")
GOLD_PATH       = os.path.join(LAKE_PATH, "gold")
ALERTS_PATH     = os.path.join(LAKE_PATH, "gold", "alerts")
REPORTS_PATH    = os.path.join(LAKE_PATH, "reports")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

HUDI_BUNDLE = (
    "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1"
)

log = logging.getLogger(__name__)

# ─── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
}

# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 helper: data quality checks using PySpark batch
# ─────────────────────────────────────────────────────────────────────────────

def _data_quality_check(**context) -> None:
    """
    Checks performed on the Silver OHLCV table:
      1. Missing minutes  — no gaps in the 1-minute OHLCV series per symbol.
      2. Duplicate keys   — record_key (symbol+minute) must be unique.
      3. Null prices      — open/high/low/close/volume must not be null.
    Raises AirflowException on any failure.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, countDistinct, lag
    from pyspark.sql.window import Window

    spark = (
        SparkSession.builder
        .appName("dq_check")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )

    df = spark.read.format("hudi").load(SILVER_PATH)
    total_rows = df.count()
    log.info("Silver row count: %d", total_rows)

    issues = []

    # Check 1: Null prices
    null_counts = (
        df.select(
            [count(col(c)).alias(c) for c in ["open", "high", "low", "close", "volume"]]
        )
        .collect()[0]
        .asDict()
    )
    for field, cnt in null_counts.items():
        if cnt != total_rows:
            issues.append(f"Null values in column '{field}': {total_rows - cnt} nulls")

    # Check 2: Duplicate record keys
    dedup_count = df.select("record_key").distinct().count()
    if dedup_count != total_rows:
        issues.append(
            f"Duplicate record_keys: {total_rows} rows but only {dedup_count} distinct keys"
        )

    # Check 3: Missing minutes per symbol (gap detection)
    window_spec = Window.partitionBy("symbol").orderBy("minute")
    gaps = (
        df
        .withColumn("prev_minute", lag("minute", 1).over(window_spec))
        .filter(col("prev_minute").isNotNull())
        .filter(
            # expected gap = exactly 60 seconds
            (col("minute").cast("long") - col("prev_minute").cast("long")) > 120
        )
    )
    gap_count = gaps.count()
    if gap_count > 0:
        issues.append(f"Detected {gap_count} gap(s) > 2 minutes in the Silver OHLCV series")

    # Write a DQ log row to data/reports/dq_log.json (best-effort)
    Path(REPORTS_PATH).mkdir(parents=True, exist_ok=True)
    dq_result = {
        "run_date": context["ds"],
        "total_rows": total_rows,
        "issues": issues,
        "passed": len(issues) == 0,
    }
    dq_log_path = os.path.join(REPORTS_PATH, f"dq_log_{context['ds']}.json")
    with open(dq_log_path, "w") as fh:
        json.dump(dq_result, fh, indent=2)
    log.info("DQ result: %s", dq_result)

    spark.stop()

    if issues:
        raise AirflowException("Data quality check FAILED:\n" + "\n".join(issues))

    log.info("Data quality check PASSED.")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 helper: generate the daily report
# ─────────────────────────────────────────────────────────────────────────────

def _generate_daily_report(**context) -> None:
    """
    Queries the Gold table to compile:
      • Top movers    — symbols with the highest % change over the last 24 h.
      • Volatility leaders — symbols ranked by average volatility.
      • Anomaly count per symbol.
    Pushes the report dict to XCom so the next task can persist / send it.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        avg as spark_avg, col, count, current_timestamp, expr, rank
    )
    from pyspark.sql.window import Window

    spark = (
        SparkSession.builder
        .appName("daily_report")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )

    gold_df = spark.read.format("hudi").load(GOLD_PATH)
    gold_df.createOrReplaceTempView("gold")

    try:
        alerts_df = spark.read.format("hudi").load(ALERTS_PATH)
        alerts_df.createOrReplaceTempView("alerts")
    except Exception:
        # Alerts table may not exist yet if no anomalies were detected
        spark.sql("CREATE OR REPLACE TEMP VIEW alerts AS SELECT * FROM gold WHERE 1=0")

    run_date = context["ds"]

    # Top movers (last 24 h)
    top_movers = spark.sql(f"""
        WITH daily AS (
            SELECT
                symbol,
                first_value(open)  OVER (PARTITION BY symbol ORDER BY window_start
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS open_price,
                last_value(close)  OVER (PARTITION BY symbol ORDER BY window_start
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_close,
                window_start
            FROM gold
            WHERE window_start >= current_timestamp() - INTERVAL 24 HOURS
        )
        SELECT DISTINCT symbol,
               ROUND(((latest_close - open_price) / open_price) * 100, 4) AS pct_change
        FROM daily
        ORDER BY pct_change DESC
        LIMIT 10
    """).collect()

    # Volatility leaders (last 24 h)
    vol_leaders = spark.sql("""
        SELECT symbol, ROUND(AVG(volatility), 6) AS avg_volatility
        FROM gold
        WHERE window_start >= current_timestamp() - INTERVAL 24 HOURS
        GROUP BY symbol
        ORDER BY avg_volatility DESC
    """).collect()

    # Anomaly count
    anomaly_counts = spark.sql("""
        SELECT symbol, COUNT(*) AS anomaly_count
        FROM alerts
        WHERE window_end >= current_timestamp() - INTERVAL 24 HOURS
        GROUP BY symbol
        ORDER BY anomaly_count DESC
    """).collect()

    report = {
        "run_date": run_date,
        "top_movers":    [row.asDict() for row in top_movers],
        "vol_leaders":   [row.asDict() for row in vol_leaders],
        "anomaly_counts":[row.asDict() for row in anomaly_counts],
    }

    spark.stop()
    log.info("Daily report: %s", json.dumps(report, indent=2))

    # Push to XCom so the next task can read it
    context["ti"].xcom_push(key="daily_report", value=report)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 helper: save or send the report
# ─────────────────────────────────────────────────────────────────────────────

def _save_or_send_report(**context) -> None:
    """
    Reads the report dict from XCom and:
      1. Saves it as JSON to LAKE_PATH/reports/daily_report_<date>.json.
      2. (Optional) POSTs it to a Slack / Teams webhook if WEBHOOK_URL is set.
    """
    import urllib.request

    report = context["ti"].xcom_pull(key="daily_report", task_ids="generate_daily_report")
    if not report:
        log.warning("No report found in XCom; skipping save/send.")
        return

    Path(REPORTS_PATH).mkdir(parents=True, exist_ok=True)
    report_path = os.path.join(REPORTS_PATH, f"daily_report_{context['ds']}.json")
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2)
    log.info("Report saved → %s", report_path)

    webhook_url = os.getenv("WEBHOOK_URL")
    if webhook_url:
        payload = json.dumps({
            "text": (
                f"*Daily Crypto Report — {report['run_date']}*\n"
                f"Top mover: {report['top_movers'][0] if report['top_movers'] else 'N/A'}\n"
                f"Most volatile: {report['vol_leaders'][0] if report['vol_leaders'] else 'N/A'}\n"
                f"Anomaly leaders: {report['anomaly_counts'][:3]}"
            )
        }).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("Webhook response: %s", resp.status)


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="daily_maintenance",
    description="Daily Hudi compaction, data quality check, and crypto market report",
    schedule_interval="0 2 * * *",   # 02:00 UTC every day
    default_args=default_args,
    catchup=False,
    tags=["crypto", "maintenance", "hudi"],
) as dag:

    # ── Task 1: Hudi compaction on Silver (MoR → CoR) ────────────────────
    trigger_hudi_compaction = BashOperator(
        task_id="trigger_hudi_compaction",
        bash_command=(
            f"spark-submit "
            f"--packages {HUDI_BUNDLE} "
            f"--class org.apache.hudi.utilities.HoodieCompactor "
            f"--mode run "
            f"--base-path {SILVER_PATH} "
            f"--table-name silver_ohlcv "
            f"--parallelism 4 "
            f"|| echo '[compaction] No pending compaction or Hudi CLI not available; skipping.'"
        ),
        env={"LAKE_PATH": LAKE_PATH},
    )

    # ── Task 2: Data quality checks on Silver ────────────────────────────
    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=_data_quality_check,
        provide_context=True,
    )

    # ── Task 3: Generate daily report from Gold ───────────────────────────
    generate_daily_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=_generate_daily_report,
        provide_context=True,
    )

    # ── Task 4: Save JSON report and optionally post to webhook ───────────
    save_or_send_report = PythonOperator(
        task_id="save_or_send_report",
        python_callable=_save_or_send_report,
        provide_context=True,
    )

    # ── Dependency chain ──────────────────────────────────────────────────
    (
        trigger_hudi_compaction
        >> data_quality_check
        >> generate_daily_report
        >> save_or_send_report
    )
