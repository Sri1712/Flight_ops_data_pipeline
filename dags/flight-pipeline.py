import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingest
from scripts.silver_transform import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate
from scripts.gold_to_snowflake import gold_to_snowflake

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='flight_data_pipeline',
    default_args=default_args,
    description='A real time flight data pipeline',
    schedule_interval= "*/30 * * * *",  # Every 30 minutes
    start_date=datetime(2025, 12, 10),
    catchup=False,
) as dag:

    bronze_ingest_task = PythonOperator(
        task_id='bronze_ingest',
        python_callable=run_bronze_ingest,
    )

    silver_transform_task = PythonOperator(
        task_id='silver_transform',
        python_callable=run_silver_transform,
    )

    gold_aggregate_task = PythonOperator(
        task_id='gold_aggregate',
        python_callable=run_gold_aggregate,
    )

    gold_to_snowflake_task = PythonOperator(
        task_id='gold_to_snowflake',
        python_callable=gold_to_snowflake,
    )

    bronze_ingest_task >> silver_transform_task >> gold_aggregate_task >> gold_to_snowflake_task