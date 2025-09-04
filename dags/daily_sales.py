import os

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.email import EmailOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from dags.helpers.logging import log_dag_status

BUCKET = os.getenv("GCS_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id = "daily_sales",
    catchup = False,
    params = {
        "date": Param("", type = ["string", "null"])
    },
    on_success_callback = log_dag_status,
    on_failure_callback = log_dag_status,
) as dag:
    wait_sales = GCSObjectExistenceSensor(
        task_id = 'wait_for_sales',
        bucket = BUCKET,
        object = 'sales/sales_{{ (params.date or ds) | replace("-", "_") }}.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    wait_accounts = GCSObjectExistenceSensor(
        task_id = 'wait_for_accounts',
        bucket = BUCKET,
        object = 'accounts/accounts_{{ (params.date or ds) | replace("-", "_") }}.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    spark_job = SparkSubmitOperator(
        task_id = 'run_daily_sales',
        application = 'scripts/spark_jobs/daily_sales.py',
        conn_id = 'spark_default',
        application_args = [
            "--date", "{{ (params.date or ds) }}",
            "--project", GCP_PROJECT_ID,
            "--dataset", "bank_raw_daily_ingest_analytics",
            "--table", "daily_sales",
            "--temp-bucket", BUCKET,
        ],
        conf = {
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        },
    )

    mail = EmailOperator(
        task_id = 'send_email',
        to = 'matei.otniel20@gmail.com',
        subject = 'Dag Run Succes',
        html_content = '<p>It works! :)</p>'
    )

    [wait_sales, wait_accounts] >> spark_job >> mail
