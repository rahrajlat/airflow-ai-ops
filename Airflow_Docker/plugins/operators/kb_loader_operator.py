from airflow.models import BaseOperator
import psycopg2
import requests
import yaml
import os
import re


class KBLoaderOperator(BaseOperator):

    def __init__(self, kb_folder, ollama_url, model, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kb_folder = kb_folder
        self.ollama_url = ollama_url
        self.model = model

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

    def get_embedding(self, text):
        res = requests.post(
            self.ollama_url,
            json={"model": self.model, "input": text}
        )
        return res.json()["embedding"]

    def load_all_yaml(self):
        merged = {}
        files = [f for f in os.listdir(self.kb_folder) if f.endswith(".yml")]

        self.log.info(f"📂 Found {len(files)} YAML files")

        for file in files:
            with open(os.path.join(self.kb_folder, file)) as f:
                data = yaml.safe_load(f)

            if not data:
                continue

            for service, entries in data.items():
                merged.setdefault(service, []).extend(entries)

        return merged

    def execute(self, context):

        self.log.info("🚀 Loading KB into vector DB...")

        data = self.load_all_yaml()

        conn = self.get_conn()
        cur = conn.cursor()

        for service, entries in data.items():

            for e in entries:

                try:
                    error = e["error"]
                    action = e["decision"]
                    reason = e.get("root_cause", "")
                    source = e.get("source", "unknown")

                    norm = self.normalize(error)

                    emb = self.get_embedding(error)

                    cur.execute("""
                        INSERT INTO airflow_feed.error_kb (
                            service, error_text, normalized_error,
                            embedding, suggested_action, root_cause, source
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (service, normalized_error)
                        DO NOTHING
                    """, (
                        service, error, norm, emb, action, reason, source
                    ))

                except Exception as e:
                    self.log.error(e)

            conn.commit()

        cur.close()
        conn.close()

        self.log.info("✅ KB Loaded")
