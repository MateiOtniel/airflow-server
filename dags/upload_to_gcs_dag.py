# dags/upload_to_gcs_dag.py
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

default_args = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="upload_to_gcs_dag",
    default_args=default_args,
    start_date=datetime(2025, 7, 15),
    catchup=False,
    schedule_interval="@daily",
    tags=["gcs", "upload"],
) as dag:

    # 1) Task: create an empty file
    make_empty = BashOperator(
        task_id="make_empty_file",
        bash_command=(
            "mkdir -p /tmp/airflow/{{ ds }} && "
            "touch /tmp/airflow/{{ ds }}/empty_file_{{ ds }}.txt"
        ),
    )

    # 2) Task: upload that file to GCS
    upload_empty = LocalFilesystemToGCSOperator(
        task_id="upload_empty_file",
        src="/tmp/airflow/{{ ds }}/empty_file_{{ ds }}.txt",
        dst="empty_file_{{ ds }}.txt",
        bucket=os.getenv("GCS_BUCKET", "bank-raw-daily-ingest"),
        gcp_conn_id="google_cloud_default",
        mime_type="text/plain",
    )

    make_empty >> upload_empty
