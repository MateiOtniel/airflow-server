import os
import logging as log
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from pyspark.sql import SparkSession

BASE_DIR = os.environ.get("AIRFLOW_HOME")

default_args = {
    'start_date': datetime(2025, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'catchup': False,
}


def build_paths(ti):
    input_path = os.path.join(BASE_DIR, "data", "input", "visits.csv")
    output_path = os.path.join(BASE_DIR, "data", "output", "visits.parquet")
    ti.xcom_push(key="input_path", value=input_path)
    ti.xcom_push(key="output_path", value=output_path)

def validate_output(ti):
    output_path = ti.xcom_pull(task_ids='build_paths', key='output_path')
    spark = SparkSession.builder.appName("validate_visits_output").getOrCreate()
    df = spark.read.parquet(output_path)
    count = df.count()
    if count == 0:
        raise ValueError(f"No rows found in {output_path}")
    log.info("The output has values!")

with DAG(
    dag_id="process_visits_job_dag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:
    wait_for_file_task = FileSensor(
        task_id="wait_for_input_file",
        filepath=os.path.join(BASE_DIR, "data/input/visits.csv"),
        poke_interval=10,
        timeout=300,
        mode="reschedule"
    )

    build_paths_task = PythonOperator(
        task_id="build_paths",
        python_callable=build_paths,
        provide_context=True  # ca sa primeasca ti
    )

    spark_task = SparkSubmitOperator(
        task_id="process_visits_job",
        application="scripts/spark_jobs/process_visits_job.py",
        application_args=[
            "--input", "{{ ti.xcom_pull(task_ids='build_paths', key='input_path') }}",
            "--output", "{{ ti.xcom_pull(task_ids='build_paths', key='output_path') }}"
        ],
        conn_id="spark_default",
        env_vars={"PYTHONPATH": "/Users/mateiotniel/Projects/airflow-server"},
        verbose=True,
    )

    validate_output_task = PythonOperator(
        task_id="validate_parquet_files",
        python_callable=validate_output,
        provide_context=True # ca sa primeasca ti si aici
    )

    wait_for_file_task >> build_paths_task >> spark_task >> validate_output_task
