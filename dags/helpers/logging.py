from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


def log_dag_status(context: Context) -> None:
    dag_id = context['dag'].dag_id
    run_id = context['dag_run'].run_id
    state = context['dag_run'].state
    executed_at = context['ts']

    hook = PostgresHook(postgres_conn_id='metadata_db')
    hook.run(
        """
        INSERT INTO dag_metadata(dag_id, run_id, state, executed_at)
        VALUES (%s, %s, %s, %s)
        """,
        parameters = (dag_id, run_id, state, executed_at)
    )