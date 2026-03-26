from airflow.models import BaseOperator
from collections import defaultdict
import psycopg2
import yaml
import re
from datetime import datetime


class YAMLBuilderOperator(BaseOperator):
    def __init__(self, kb_folder, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kb_folder = kb_folder

    def get_conn(self):
        return psycopg2.connect(
            host="host.docker.internal",
            port=5432,
            dbname="flow_feed_db",
            user="admin",
            password="admin"
        )

    def normalize(self, text):
        text = text.lower()
        text = re.sub(r"\d+", "", text)
        text = re.sub(r"0x[a-f0-9]+", "", text)
        return " ".join(text.split())

    def map_service(self, dag_id):
        dag_id = dag_id.lower()

        for key in ["spark", "dbt", "s3", "redshift", "snowflake", "postgres", "network", "iam"]:
            if key in dag_id:
                return key

        return "airflow"

    def execute(self, context):

        self.log.info("🧠 Building YAML from DB...")

        conn = self.get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT dag_id, error, ai_review
            FROM airflow_feed.pipeline_signals
            WHERE rerun_status = 'fixed on rerun'
            AND error IS NOT NULL
        """)

        rows = cur.fetchall()
        conn.close()

        self.log.info(f"Fetched {len(rows)} rows")

        groups = defaultdict(list)

        for dag_id, error, review in rows:
            service = self.map_service(dag_id)
            key = self.normalize(error)
            groups[(service, key)].append((error, review))

        data = defaultdict(list)

        for (service, _), items in groups.items():

            sample_error = items[0][0]
            sample_reason = items[0][1]

            data[service].append({
                "error": sample_error,
                "root_cause": sample_reason or "Auto learned",
                "decision": "RERUN",
                "source": "auto",
                "support": len(items),
                "success_rate": 1.0
            })

        filename = f"{self.kb_folder}/kb_auto_{datetime.today().date()}.yml"

        with open(filename, "w") as f:
            yaml.dump(dict(data), f, sort_keys=False)

        self.log.info(f"✅ YAML generated: {filename}")
