from airflow.models import BaseOperator
import psycopg2
import requests


class ProcessApprovedRerunsOperator(BaseOperator):

    def __init__(
        self,
        base_url,
        username,
        password,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.base_url = base_url
        self.username = username
        self.password = password

    # =========================
    # AUTH
    # =========================
    def get_token(self):
        res = requests.post(
            f"{self.base_url}/auth/token",
            json={
                "username": self.username,
                "password": self.password
            }
        )
        return res.json()["access_token"]

    # =========================
    # DB
    # =========================
    def get_pg_conn(self):
        return psycopg2.connect(
            host="host.docker.internal",
            port=5432,
            dbname="flow_feed_db",
            user="admin",
            password="admin",
        )

    # =========================
    # CLEAR TASK
    # =========================
    def clear_task(self, headers, dag_id, task_id, run_id):

        url = f"{self.base_url}/api/v2/dags/{dag_id}/clearTaskInstances"

        payload = {
            "task_ids": [task_id],
            "dag_run_id": run_id,
            "only_failed": True,
            "reset_dag_runs": True,
            "dry_run": False
        }

        res = requests.post(url, json=payload, headers=headers)

        if res.status_code != 200:
            raise Exception(res.text)

        self.log.info(f"✅ Cleared {dag_id}.{task_id} ({run_id})")

    # =========================
    # EXECUTE
    # =========================
    def execute(self, context):

        self.log.info("🚀 Processing approved reruns...")

        token = self.get_token()
        headers = {"Authorization": f"Bearer {token}"}

        conn = self.get_pg_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, dag_id, task_id, run_id
    FROM airflow_feed.pipeline_signals
    WHERE signal_type = 'failure'
      AND ai_action = 'RERUN'
      AND (
            user_confirmation = 'approved'
            OR (
                user_confirmation = 'pending'
                AND cast(confidence_score as int) >= 80
                AND created_at <= NOW() - INTERVAL '15 minutes'
            )
          )
      AND COALESCE(no_of_attempts, 0) = 0
        """)

        rows = cur.fetchall()

        self.log.info(f"Found {len(rows)} rows")

        for id, dag_id, task_id, run_id in rows:

            try:
                self.log.info(f"🔄 Clearing {dag_id}.{task_id} ({run_id})")

                self.clear_task(headers, dag_id, task_id, run_id)

                cur.execute("""
                    UPDATE airflow_feed.pipeline_signals
                    SET 
                        user_confirmation = 'cleared',
                        no_of_attempts = 1,
                        rerun_status = 'processing'
                    WHERE id = %s
                """, (id,))

                conn.commit()

                self.log.info("✅ Rerun triggered")

            except Exception as e:
                self.log.error(f"❌ Error: {e}")

        cur.close()
        conn.close()
