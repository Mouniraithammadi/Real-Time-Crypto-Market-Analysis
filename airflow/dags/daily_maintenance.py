# daily_maintenance.py
#
# PURPOSE: Airflow daily DAG for maintenance and reporting (Part 4 – Orchestration)
#
# DAG ID: daily_maintenance
# Schedule: once a day (e.g., @daily or cron "0 2 * * *" at 02:00 UTC)
# Owner: team (set in default_args)
#
# TASKS (in dependency order):
#
# TASK 1: trigger_hudi_compaction
#    - Run a BashOperator or SparkSubmitOperator to trigger Hudi compaction on the Silver table.
#    - Command: spark-submit with the Hudi bundle and a small compaction script,
#      or use the Hudi CLI: "compaction run --table-name silver_ohlcv --base-path LAKE_PATH/silver".
#    - Ensures the Silver MoR table's log files are merged for faster reads.
#
# TASK 2: data_quality_check
#    - Run a PythonOperator or SparkSubmitOperator.
#    - Connect to the Silver Hudi table and check:
#        a. Missing minutes: for each symbol, verify there are no gaps in the 1-minute OHLCV series.
#        b. Duplicates: verify the composite record key (symbol + minute) is unique.
#        c. Null values: check for unexpected nulls in price or volume columns.
#    - Raise an AirflowException if any check fails.
#    - Optionally write a quality report row to a "dq_log" table.
#
# TASK 3: generate_daily_report
#    - Run a PythonOperator.
#    - Query the Gold table (or run queries.sql via spark.sql()) to compute:
#        a. Top movers: symbols with the highest percentage change over the past 24 hours.
#        b. Volatility leaders: symbols ranked by average volatility.
#        c. Anomaly count per symbol for the day.
#    - Compile results into a structured report (dict or DataFrame).
#
# TASK 4: save_or_send_report
#    - Save the report as a JSON or CSV file to LAKE_PATH/reports/.
#    - Optionally send the report by email using Airflow's EmailOperator,
#      or POST it to a Slack/Teams webhook using an HttpOperator.
#
# DEPENDENCY CHAIN:
#    trigger_hudi_compaction >> data_quality_check >> generate_daily_report >> save_or_send_report
