import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from dags.helpers.logging import log_dag_status

BUCKET = os.getenv("GCS_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

with DAG(
    dag_id = "accesa_financial_revenue",
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

    wait_products = GCSObjectExistenceSensor(
        task_id = 'wait_for_products',
        bucket = BUCKET,
        object = 'accesa_products/accesa_products.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    wait_discounts = GCSObjectExistenceSensor(
        task_id = 'wait_for_discounts',
        bucket = BUCKET,
        object = 'accesa_discounts/accesa_discounts.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    wait_customers = GCSObjectExistenceSensor(
        task_id = 'wait_for_customers',
        bucket = BUCKET,
        object = 'accesa_customers/accesa_customers.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    wait_prices = GCSObjectExistenceSensor(
        task_id = 'wait_for_prices',
        bucket = BUCKET,
        object = 'accesa_product_prices/accesa_product_prices.csv',
        google_cloud_conn_id = 'google_cloud_default',
        poke_interval = 60,
        timeout = 30 * 60,
    )

    spark_job = SparkSubmitOperator(
        task_id = 'run_accesa_financial_revenue',
        application = 'scripts/spark_jobs/accesa_financial_revenue.py',
        conn_id = 'spark_default',
        application_args = [
            "--project", GCP_PROJECT_ID,
            "--dataset", "bank_raw_daily_ingest_analytics",
            "--table", "accesa_financial_revenue",
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
        subject = 'Accesa Financial Revenue done',
        html_content = '<p>Revenue + TVA analytics done.</p>'
    )

    [wait_transactions, wait_products, wait_discounts, wait_customers, wait_prices] >> spark_job >> mail
