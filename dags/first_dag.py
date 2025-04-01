from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id="bash_operator_1",
        bash_command="echo Hello from bash 1"
    )

    t2 = BashOperator(
        task_id="bash_operator2",
        bash_command="echo Hello from bash 2"
    )

    t1 >> t2
