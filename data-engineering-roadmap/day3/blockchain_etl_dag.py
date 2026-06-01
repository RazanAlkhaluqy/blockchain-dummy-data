"""
Day 3 — Apache Airflow DAG
===========================
File: blockchain_etl_dag.py
Place in: ~/airflow/dags/

Install Airflow (local):
    pip install apache-airflow
    airflow db init
    airflow users create --username admin --password admin \
        --firstname A --lastname B --role Admin --email a@b.com
    airflow webserver --port 8080   # open http://localhost:8080
    airflow scheduler               # run in separate terminal

This DAG runs the full blockchain ETL pipeline on a daily schedule.
It mirrors exactly what day3_pipeline.py does manually.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import json
import logging

# ── Default args (applies to all tasks) ──────────────────────
default_args = {
    "owner":            "data-engineering-team",
    "depends_on_past":  False,
    "email":            ["alerts@yourstartup.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id="blockchain_etl_pipeline",
    default_args=default_args,
    description="Daily blockchain events ETL: extract → transform → load → dbt",
    schedule_interval="0 2 * * *",   # runs at 2am every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["blockchain", "etl", "daily"],
    doc_md="""
    ## Blockchain ETL Pipeline
    Extracts on-chain events from JSON feed, enriches with token prices,
    validates quality, loads to PostgreSQL staging, then builds dbt marts.
    SLA: must complete within 30 minutes.
    """,
) as dag:

    # ── TASK FUNCTIONS ────────────────────────────────────────

    def extract_blockchain_events(**context):
        logging.info("Extracting blockchain events from JSON...")
        with open("raw_blockchain_events.json") as f:
            events = json.load(f)
        df = pd.DataFrame(events)
        row_count = len(df)
        # push row count to XCom so downstream tasks can read it
        context["ti"].xcom_push(key="events_count", value=row_count)
        logging.info(f"Extracted {row_count:,} events")
        return row_count

    def extract_api_prices(**context):
        logging.info("Extracting token prices from API CSV...")
        df = pd.read_csv("raw_api_prices.csv")
        context["ti"].xcom_push(key="prices_count", value=len(df))
        logging.info(f"Extracted {len(df):,} price records")
        return len(df)

    def validate_events(**context):
        logging.info("Validating extracted events...")
        with open("raw_blockchain_events.json") as f:
            events = json.load(f)
        df = pd.DataFrame(events)
        issues = []
        if df["tx_hash"].duplicated().sum() > 0:
            issues.append("duplicate tx_hashes found")
        if (df["amount_usd"] < 0).any():
            issues.append("negative amount_usd values")
        if df["event_id"].isnull().any():
            issues.append("null event_ids")
        if issues:
            raise ValueError(f"Validation failed: {issues}")
        logging.info("Validation passed — no issues found")
        return "validation_passed"

    def check_data_quality(**context):
        """BranchOperator: routes to transform if OK, alert if not"""
        events_count = context["ti"].xcom_pull(
            task_ids="extract_blockchain_events", key="events_count")
        if events_count and events_count > 100:
            return "transform_enrich"
        else:
            return "alert_low_data"

    def transform_enrich(**context):
        logging.info("Transforming and enriching events...")
        with open("raw_blockchain_events.json") as f:
            events = json.load(f)
        df = pd.DataFrame(events)
        df_confirmed = df[df["status"] == "confirmed"].drop_duplicates("tx_hash")
        df_confirmed = df_confirmed[df_confirmed["amount_usd"] > 0]
        df_confirmed["pipeline_loaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        context["ti"].xcom_push(key="clean_count", value=len(df_confirmed))
        logging.info(f"Transformed: {len(df_confirmed):,} clean rows")
        return len(df_confirmed)

    def load_to_postgres(**context):
        from sqlalchemy import create_engine
        logging.info("Loading to PostgreSQL staging table...")
        clean_count = context["ti"].xcom_pull(
            task_ids="transform_enrich", key="clean_count")
        logging.info(f"Would load {clean_count:,} rows → stg_blockchain_events")
        # In production: df.to_sql("stg_blockchain_events", engine, ...)
        return "load_complete"

    def run_dbt_models(**context):
        import subprocess
        logging.info("Running dbt models...")
        # In production: subprocess.run(["dbt", "run", "--models", "stg+ mart+"])
        logging.info("dbt stg_blockchain_events — OK")
        logging.info("dbt mart_token_daily_volume — OK")
        logging.info("dbt mart_wallet_summary — OK")
        return "dbt_complete"

    def alert_low_data(**context):
        logging.warning("LOW DATA ALERT: fewer than 100 events extracted!")
        # In production: send Slack/email alert here

    def notify_success(**context):
        clean_count = context["ti"].xcom_pull(
            task_ids="transform_enrich", key="clean_count") or 0
        logging.info(f"Pipeline complete! {clean_count:,} events processed.")

    # ── TASK DEFINITIONS ─────────────────────────────────────

    start = EmptyOperator(task_id="start")

    t_extract_events = PythonOperator(
        task_id="extract_blockchain_events",
        python_callable=extract_blockchain_events,
    )

    t_extract_prices = PythonOperator(
        task_id="extract_api_prices",
        python_callable=extract_api_prices,
    )

    t_validate = PythonOperator(
        task_id="validate_events",
        python_callable=validate_events,
    )

    t_branch = BranchPythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    t_transform = PythonOperator(
        task_id="transform_enrich",
        python_callable=transform_enrich,
    )

    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t_dbt = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_models,
    )

    t_alert = PythonOperator(
        task_id="alert_low_data",
        python_callable=alert_low_data,
    )

    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # ── TASK DEPENDENCIES (the DAG graph) ────────────────────

    start >> [t_extract_events, t_extract_prices]
    t_extract_events >> t_validate >> t_branch
    t_branch >> [t_transform, t_alert]
    t_extract_prices >> t_transform
    t_transform >> t_load >> t_dbt >> t_notify
    t_alert >> t_notify
