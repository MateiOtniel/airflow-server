from airflow import DAG
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_latencies_by_dataproduct_job_dag",
    start_date=datetime(2025, 5, 6),
    schedule_interval="@daily",
    catchup=False
) as dag:
    spark_task = SparkSubmitOperator(
        task_id="run_latencies_by_dataproduct_job",
        application="scripts/spark_jobs/latencies_by_dataproduct_job.py",
        conn_id="spark_default",
        verbose=True,
        packages="org.apache.spark:spark-sql_2.12:3.5.1"
    )

    spark_task