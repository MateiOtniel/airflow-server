import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

BASE_DIR = os.environ.get("AIRFLOW_HOME")

default_args = {
    'start_date': datetime(2025, 6, 10),
}


def build_paths(ti):
    input_users_path = os.path.join(BASE_DIR, "data/input/users.csv")
    input_events_path = os.path.join(BASE_DIR, "data/input/events.csv")
    output_path = os.path.join(BASE_DIR, "data/output/processed_sessions.parquet")

    ti.xcom_push("--input_users_path", value = input_users_path)
    ti.xcom_push("--input_events_path", value = input_events_path)
    ti.xcom_push("--output_path", value = output_path)


with DAG(
        dag_id = "process_sessions_job_dag",
        default_args = default_args
):
    build_paths_task = PythonOperator(
        task_id = "build_paths",
        python_callable = build_paths,
        provide_context = True
    )

    check_users_task = FileSensor(
        task_id = "check_users_file",
        filepath = "{{ ti.xcom_pull(task_ids='build_paths', key='--input_users_path') }}",
        poke_interval = 10,
        timeout = 600,
        mode = "reschedule"
    )

    check_events_task = FileSensor(
        task_id = "check_events_file",
        filepath = "{{ ti.xcom_pull(task_ids='build_paths', key='--input_events_path') }}",
        poke_interval = 10,
        timeout = 600,
        mode = "reschedule"
    )

    spark_job_task = SparkSubmitOperator(
        task_id = "process_sessions_job",
        application = os.path.join(BASE_DIR, "scripts/spark_jobs/process_sessions.py"),
        application_args = [
            "--input_users_path", "{{ ti.xcom_pull(task_ids='build_paths', key='--input_users_path') }}",
            "--input_events_path", "{{ ti.xcom_pull(task_ids='build_paths', key='--input_events_path') }}",
            "--output_path", "{{ ti.xcom_pull(task_ids='build_paths', key='--output_path') }}",
            "--date", "2025-06-14"
        ],
        conn_id = "spark_default",
        env_vars = {"PYTHONPATH": "/Users/mateiotniel/Projects/airflow-server"},
        verbose = True
    )

    build_paths_task >> [check_events_task, check_events_task] >> spark_job_task
