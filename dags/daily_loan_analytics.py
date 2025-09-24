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
    dag_id = "daily_loan_analytics",
    catchup = False,
    params = {
        "date": Param("", type = ["string", "null"])
    },
    on_success_callback = log_dag_status,
    on_failure_callback = log_dag_status,
) as dag:
    wait_loans = GCSObjectExistenceSensor(
        task_id = 'wait_for_loans',
        bucket = BUCKET,
        object = 'loans/loans_{{ (params.date or ds) | replace("-", "_") }}.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    wait_delay_fees = GCSObjectExistenceSensor(
        task_id = 'wait_for_delay_fees',
        bucket = BUCKET,
        object = 'delay_fees/delay_fees_{{ (params.date or ds) | replace("-", "_") }}.csv',
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
        task_id = 'run_loan_analytics',
        application = 'scripts/spark_jobs/daily_loan_analytics.py',
        conn_id = 'spark_default',
        application_args = [
            "--date", "{{ (params.date or ds) }}",
            "--project", GCP_PROJECT_ID,
            "--dataset", "bank_raw_daily_ingest_analytics",
            "--table", "daily_loan_analytics",
            "--temp-bucket", BUCKET,
        ],
        conf = {
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            # "spark.sql.adaptive.enabled": "true",
            # "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
    )

    mail = EmailOperator(
        task_id = 'send_email',
        to = 'matei.otniel20@gmail.com',
        subject = 'Loan Analytics DAG Run Success',
        html_content = '<p>Loan Analytics job completed successfully! 📊</p>'
    )

    [wait_loans, wait_delay_fees, wait_sales] >> spark_job >> mail