from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import time
from failure_plugin.failure_callback import failure_callback
from success_plugin.success_callback import success_callback


def simulate_task(task_name):

    scenarios = [
        ("success", None),

        ("s3_timeout", "botocore.exceptions.ConnectTimeoutError: S3 connection timed out"),
        ("db_timeout", "psycopg2.OperationalError: could not connect to server: Connection timed out"),
        ("api_failure", "requests.exceptions.HTTPError: 503 Service Unavailable"),

        ("schema_mismatch", "ValueError: Schema mismatch for column 'user_id'"),
        ("missing_column", "KeyError: 'customer_id'"),
        ("null_violation", "ValueError: Null values found in non-nullable column"),

        ("spark_oom", "org.apache.spark.SparkException: Executor lost due to OutOfMemoryError"),
        ("shuffle_failure", "org.apache.spark.shuffle.FetchFailedException: Failed to fetch shuffle blocks"),

        ("type_error", "TypeError: unsupported operand type(s)"),
        ("index_error", "IndexError: list index out of range"),

        ("file_missing", "FileNotFoundError: /data/input/events.parquet not found"),
        ("permission", "PermissionError: Access denied to S3 path"),

        ("timeout", "TimeoutError: Task exceeded execution timeout"),
    ]

    weights = [6] + [1] * (len(scenarios) - 1)
    scenario, message = random.choices(scenarios, weights=weights)[0]

    print(f"[{task_name}] Scenario: {scenario}")
    time.sleep(random.uniform(0.2, 1.0))

    if scenario == "success":
        return

    raise Exception(message)


with DAG(
    dag_id="dag_subscription_billing_pipeline",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "realistic"],
    on_success_callback=success_callback
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=lambda: simulate_task("extract"),
        on_failure_callback=failure_callback
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=lambda: simulate_task("transform"),
        on_failure_callback=failure_callback
    )

    load = PythonOperator(
        task_id="load",
        python_callable=lambda: simulate_task("load"),
        on_failure_callback=failure_callback
    )

    extract >> transform >> load
