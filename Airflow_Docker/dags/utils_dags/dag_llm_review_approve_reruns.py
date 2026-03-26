from datetime import datetime
from airflow import DAG
from operators.rerun_operator import ProcessApprovedRerunsOperator
from operators.ollama_ai_operator import OllamaAIFailureOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_llm_review_approve_reruns",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rerun", "ai"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    llm_review = OllamaAIFailureOperator(
        task_id="ai_failure_analysis",

        db_config={
            "host": "host.docker.internal",
            "port": 5432,
            "dbname": "flow_feed_db",
            "user": "admin",
            "password": "admin",
        },

        ollama_url="http://host.docker.internal:11434/api/generate",
        embed_url="http://host.docker.internal:11434/api/embed",
    )

    process_approved_reruns = ProcessApprovedRerunsOperator(
        task_id="process_approved_reruns",
        base_url="http://airflow-apiserver:8080",
        username="airflow",
        password="airflow",
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> [llm_review] >> process_approved_reruns >> end_task
