from datetime import datetime
from airflow import DAG
from operators.yaml_builder_operator import YAMLBuilderOperator
from operators.kb_loader_operator import KBLoaderOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="dag_ai_learning_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start_task = EmptyOperator(task_id="start")

    build_yaml = YAMLBuilderOperator(
        task_id="build_yaml",
        kb_folder="/opt/airflow/knowledge",
    )

    load_kb = KBLoaderOperator(
        task_id="load_kb",
        kb_folder="/opt/airflow/knowledge",
        ollama_url="http://host.docker.internal:11434/api/embed",
        model="nomic-embed-text"
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> build_yaml >> load_kb >> end_task
