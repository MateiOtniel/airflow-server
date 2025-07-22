from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
import os
from airflow.models.param import Param

BUCKET = os.getenv("GCS_BUCKET")

with DAG(
    dag_id = "daily_sales",
    catchup = False,
    params = {
        "date": Param("", type = ["string", "null"])
    }
) as dag:
    wait_sales = GCSObjectExistenceSensor(
        task_id = 'wait_for_sales',
        bucket = BUCKET,
        object = 'sales/sales_{{ params.date or ds | replace("-", "_") }}.csv',
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
        verbose = True,
    )

    [wait_sales, wait_accounts] >> spark_job
