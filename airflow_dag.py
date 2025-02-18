from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dq_check import dq_check

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "bigquery_data_quality_check",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

run_dq_check = PythonOperator(
    task_id="run_dq_check",
    python_callable=dq_check,
    op_kwargs={"use_airflow": True},  # Uses Airflow variables
    dag=dag,
)

run_dq_check
