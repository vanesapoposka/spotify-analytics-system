"""
Airflow DAG — Spotify Analytics DWH pipeline (REAL, RUNNABLE VERSION).

This is the executable counterpart to the production DAG design
(spotify_dwh/airflow_dags/spotify_dwh_dag.py). It's wired directly to the
scripts already built and proven to work standalone in this project, so
`airflow dags test` genuinely orchestrates the real pipeline end-to-end
rather than calling illustrative placeholder commands.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# Point this at wherever you copied this project's folder on the Airflow host.
PROJECT = os.getenv("SPOTIFY_DWH_PROJECT_DIR", "/opt/spotify_dwh")

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spotify_dwh_daily",
    description="Manual full rebuild: star schema, synthetic events, OLAP, and ML refresh",
    default_args=default_args,
    # The first task runs 01_star_schema.sql, which drops dwh with CASCADE.
    # Keep this full-rebuild DAG manual-only so it cannot erase the warehouse daily.
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "dwh", "olap", "ml"],
) as dag:

    PSQL = f"PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB"

    apply_schema = BashOperator(
        task_id="apply_star_schema_ddl",
        bash_command=f"set -a; source {PROJECT}/.env; set +a; {PSQL} -f {PROJECT}/sql/01_star_schema.sql",
    )

    load_dims_and_catalog = BashOperator(
        task_id="incremental_catalog_load_scd2",
        bash_command=f"python3 {PROJECT}/etl/load_dwh.py",
    )

    load_events = BashOperator(
        task_id="load_daily_listening_events",
        bash_command=f"python3 {PROJECT}/etl/generate_events.py",
    )

    derive_weighted_metrics = BashOperator(
        task_id="derive_weighted_unweighted_metrics",
        bash_command=f"set -a; source {PROJECT}/.env; set +a; {PSQL} -f {PROJECT}/sql/02_derive_weighted_metrics.sql",
    )

    run_olap_queries = BashOperator(
        task_id="run_olap_analysis",
        bash_command=f"set -a; source {PROJECT}/.env; set +a; {PSQL} -f {PROJECT}/sql/03_olap_queries.sql",
    )

    run_ml_pipeline = BashOperator(
        task_id="ml_refresh_all_models",
        bash_command=f"python3 {PROJECT}/ml/run_ml.py",
    )

    (apply_schema >> load_dims_and_catalog >> load_events
        >> derive_weighted_metrics >> run_olap_queries >> run_ml_pipeline)
