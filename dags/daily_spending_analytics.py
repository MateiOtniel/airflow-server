import os

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from dags.helpers.logging import log_dag_status

BUCKET = os.getenv("GCS_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id = "daily_spending_analytics",
    catchup = False,
    params = {
        "date": Param("", type = ["string", "null"])
    },
    on_success_callback = log_dag_status,
    on_failure_callback = log_dag_status,
) as dag:
    wait_accounts = GCSObjectExistenceSensor(
        task_id = 'wait_for_accounts',
        bucket = BUCKET,
        object = 'accounts/accounts_{{ (params.date or ds) | replace("-", "_") }}.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    wait_sales = GCSObjectExistenceSensor(
        task_id = 'wait_for_sales',
        bucket = BUCKET,
        object = 'sales/sales_{{ (params.date or ds) | replace("-", "_") }}.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    spark_job = SparkSubmitOperator(
        task_id = 'run_spending_analytics',
        application = 'scripts/spark_sql_jobs/daily_spending_analytics.py',
        conn_id = 'spark_default',
        application_args = [
            "--date", "{{ (params.date or ds) }}",
            "--project", GCP_PROJECT_ID,
            "--dataset", "bank_raw_daily_ingest_analytics",
            "--table", "daily_spending_analytics",
            "--temp-bucket", BUCKET,
        ],
        conf = {
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        },
    )

    [wait_accounts, wait_sales] >> spark_job
