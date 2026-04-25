import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from dags.helpers.logging import log_dag_status

BUCKET = os.getenv("GCS_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id = "accesa_temporal_trends",
    catchup = False,
    on_success_callback = log_dag_status,
    on_failure_callback = log_dag_status,
) as dag:
    wait_transactions = GCSObjectExistenceSensor(
        task_id = 'wait_for_transactions',
        bucket = BUCKET,
        object = 'accesa_transactions/accesa_transactions.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    spark_job = SparkSubmitOperator(
        task_id = 'run_accesa_temporal_trends',
        application = 'scripts/spark_jobs/accesa_temporal_trends.py',
        conn_id = 'spark_default',
        application_args = [
            "--project", GCP_PROJECT_ID,
            "--dataset", "bank_raw_daily_ingest_analytics",
            "--table", "accesa_temporal_trends",
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
        subject = 'Accesa Temporal Trends done',
        html_content = '<p>Temporal trend analytics done.</p>'
    )

    wait_transactions >> spark_job >> mail
