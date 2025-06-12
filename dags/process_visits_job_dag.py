from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'start_date': datetime(2025, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'catchup': False,
}

with DAG(
    dag_id="process_visits_job_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:
    file_sensor = FileSensor()

    spark_task = SparkSubmitOperator(
        task_id="process_visits_job",
        application="scripts/spark_jobs/process_visits_job.py",
        application_args=["--input", "data/input/visits.csv", "--output", "data/output/visits.parquet"],
        conn_id="spark_default",
        env_vars={"PYTHONPATH": "/Users/mateiotniel/Projects/airflow-server"},
        verbose=True,
        packages="org.apache.spark:spark-sql_2.12:3.5.1"
    )

    file_sensor >> spark_task