from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

from failure_plugin.failure_callback import failure_callback
from success_plugin.success_callback import success_callback


# =========================
# TASK LOGIC
# =========================
def fail_then_succeed(**context):
    # raise Exception("S3 connection timed out")
    pass


# =========================
# DAG
# =========================
with DAG(
    dag_id="test_retry_success_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,        # 👈 ensures retry happens
        "retry_delay": 0
    },
) as dag:

    test_task = PythonOperator(
        task_id="fail_then_succeed_task",
        python_callable=fail_then_succeed,
        on_failure_callback=failure_callback,
        on_success_callback=success_callback,
    )
