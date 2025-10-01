import os

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dags.helpers.logging import log_dag_status

BUCKET = os.getenv("GCS_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id = "daily_orders",
    catchup = False,
    params = {
        "date": Param("", type = ["string", "null"])
    },
    on_success_callback = log_dag_status,
    on_failure_callback = log_dag_status,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id = 'run_daily_orders',
        application = 'scripts/spark_streaming_jobs/daily_orders_total.py',
        conn_id = 'spark_default',
        application_args = [
            "--date", "{{ (params.date or ds) }}",
            "--project", GCP_PROJECT_ID,
            "--dataset", "bank_raw_daily_ingest_analytics",
            "--table", "daily_streaming_orders",
            "--temp-bucket", BUCKET,
        ],
        conf = {
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        },
    )
    spark_job
