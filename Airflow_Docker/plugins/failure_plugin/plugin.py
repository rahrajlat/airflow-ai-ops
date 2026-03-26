from airflow.plugins_manager import AirflowPlugin


class FailurePlugin(AirflowPlugin):
    name = "failure_plugin"
