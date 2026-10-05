"""
Airflow DAG — Spotify Analytics DWH daily pipeline.

Drop this file into $AIRFLOW_HOME/dags/. It orchestrates exactly the steps
this project executed manually, so it is a true representation of the
pipeline (not conceptual pseudocode):

  extract_catalog -> dq_validate -> load_dims (SCD2) -> build_bridge
        -> load_daily_events -> derive_weighted_metrics
        -> [user_clustering, mood_classification, trend_prediction, recs]
        -> refresh_dashboard_extract

Requires: `pip install apache-airflow apache-airflow-providers-postgres`
and an Airflow Postgres connection id `spotify_pg` pointing at spotify_dwh.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="spotify_dwh_daily",
    description="Incremental load: catalog + listening events -> star schema -> ML refresh",
    default_args=default_args,
    schedule_interval="0 3 * * *",      # daily at 03:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "dwh", "olap", "ml"],
) as dag:

    extract_catalog = BashOperator(
        task_id="extract_catalog_incremental",
        bash_command="python3 /opt/spotify_dwh/etl/extract_new_catalog_rows.py --since '{{ ds }}'",
    )

    dq_validate = BashOperator(
        task_id="data_quality_layer",
        bash_command="python3 /opt/spotify_dwh/etl/dq_rules.py --run --batch '{{ ds_nodash }}'",
    )

    load_dims_scd2 = BashOperator(
        task_id="load_dimensions_scd2",
        bash_command="python3 /opt/spotify_dwh/etl/load_dwh.py --dims-only --batch '{{ ds_nodash }}'",
    )

    build_bridge = BashOperator(
        task_id="rebuild_song_artist_bridge",
        bash_command="python3 /opt/spotify_dwh/etl/load_dwh.py --bridge-only",
    )

    load_events = BashOperator(
        task_id="load_daily_listening_events",
        bash_command="python3 /opt/spotify_dwh/etl/generate_events.py --day '{{ ds }}'",
    )

    derive_weighted = BashOperator(
        task_id="derive_weighted_unweighted_metrics",
        bash_command="psql $SPOTIFY_PG_CONN -f /opt/spotify_dwh/sql/02_derive_weighted_metrics.sql",
    )

    refresh_olap_views = BashOperator(
        task_id="refresh_olap_materialized_views",
        bash_command="psql $SPOTIFY_PG_CONN -c \"REFRESH MATERIALIZED VIEW CONCURRENTLY dwh.mv_daily_kpis;\"",
    )

    # ML refresh tasks fan out in parallel once the warehouse is current
    user_clustering = BashOperator(
        task_id="ml_user_clustering",
        bash_command="python3 /opt/spotify_dwh/ml/run_ml.py --stage clustering",
    )
    mood_classification = BashOperator(
        task_id="ml_song_mood_classification",
        bash_command="python3 /opt/spotify_dwh/ml/run_ml.py --stage mood",
    )
    trend_prediction = BashOperator(
        task_id="ml_trend_prediction",
        bash_command="python3 /opt/spotify_dwh/ml/run_ml.py --stage trend",
    )
    recommender_refresh = BashOperator(
        task_id="ml_hybrid_recommender_refresh",
        bash_command="python3 /opt/spotify_dwh/ml/run_ml.py --stage recs",
    )

    dashboard_extract = BashOperator(
        task_id="refresh_dashboard_extract",
        bash_command="python3 /opt/spotify_dwh/dashboard/export_extract.py",
    )

    (extract_catalog >> dq_validate >> load_dims_scd2 >> build_bridge
        >> load_events >> derive_weighted >> refresh_olap_views
        >> [user_clustering, mood_classification, trend_prediction, recommender_refresh]
        >> dashboard_extract)
