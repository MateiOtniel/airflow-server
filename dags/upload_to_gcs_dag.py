from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.gcs_utils import umpload_empty_txt
import os

default_args = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes = 1),
}

with DAG(
    dag_id = "upload_to_gcs_dag",
    start_date = datetime(2025, 7, 15),
    catchup = False,
) as dag:

    upload_task = PythonOperator(
        task_id = "upload_empty_file",
        python_callable = umpload_empty_txt,
        op_kwargs = {
            "bucket_name": os.getenv("GCS_BUCKET", ""),
            "destination_blob_name": "empty_file_{{ ds }}.txt"
        }
    )

    upload_task