from airflow import DAG
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id = "sales_job_dag",
    start_date=datetime(2025, 6, 11),
    schedule_interval="@daily",
    catchup=False
) as dag:
    spark_task = SparkSubmitOperator(
        task_id="sales_job",
        application="scripts/spark_jobs/sales_job.py",
        conn_id="spark_default",
        env_vars={"PYTHONPATH": "/Users/mateiotniel/Projects/airflow-server"},
        verbose=True,
        packages="org.apache.spark:spark-sql_2.12:3.5.1"
    )

    spark_task
