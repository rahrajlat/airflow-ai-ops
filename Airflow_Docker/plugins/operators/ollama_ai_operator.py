from airflow.models import BaseOperator

import psycopg2
import json
import requests
import re


class OllamaAIFailureOperator(BaseOperator):
    """
    Airflow Operator that analyzes failed tasks using an LLM and a custom knowledge base.

    This operator:
    - Queries a PostgreSQL database for recent task failures
    - Searches a knowledge base for similar past failures using vector embeddings
    - Generates a contextual prompt with error details and historical context
    - Calls an Ollama LLM to analyze the failure and recommend an action
    - Returns a structured decision (RERUN or NO_RERUN) with confidence score

    Args:
        db_config (dict): PostgreSQL connection configuration
        ollama_url (str): URL to Ollama LLM endpoint
        embed_url (str): URL to embedding model endpoint
        model (str): Ollama model name (default: "llama3.1")
        embed_model (str): Embedding model name (default: "nomic-embed-text")

    Returns:
        Updates pipeline_signals table with AI review, action decision, and confidence score
    """

    def __init__(
        self,
        db_config: dict,
        ollama_url: str,
        embed_url: str,
        model: str = "llama3.1",
        embed_model: str = "nomic-embed-text",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.db_config = db_config
        self.ollama_url = ollama_url
        self.embed_url = embed_url
        self.model = model
        self.embed_model = embed_model

    def get_conn(self):
        """Establishes a connection to the PostgreSQL database.

        Returns:
            psycopg2.extensions.connection: A connection object to the PostgreSQL database.
        """
        return psycopg2.connect(**self.db_config)

    def normalize(self, text):
        """Normalizes text by converting to lowercase, removing digits, and collapsing whitespace.

        Args:
            text (str): The text to normalize.

        Returns:
            str: The normalized text.
        """

        text = text.lower()
        text = re.sub(r"\d+", "", text)
        return " ".join(text.split())

    # =========================
    # EMBEDDING
    # =========================
    def get_embedding(self, text):
        """Generates an embedding for the given text using the specified embedding model.

        Args:
            text (str): The text to generate an embedding for.

        Returns:
            list: The embedding vector.
        """

        res = requests.post(
            self.embed_url,
            json={"model": self.embed_model, "input": text},
            timeout=30
        )

        if res.status_code != 200:
            raise Exception(f"Embedding failed: {res.text}")

        data = res.json()

        if "embeddings" in data:
            return data["embeddings"][0]

        return data.get("embedding")

    # =========================
    # KB SEARCH
    # =========================
    def find_similar(self, cur, error):
        """Finds similar errors in the knowledge base using vector embeddings.

        Args:
            cur (psycopg2.extensions.cursor): The database cursor.
            error (str): The error message to search for.

        Returns:
            list: A list of similar errors with their suggested actions and root causes.
        """

        emb = self.get_embedding(self.normalize(error))

        cur.execute("""
            SELECT error_text, suggested_action, root_cause,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM airflow_feed.error_kb
            ORDER BY embedding <=> %s::vector
            LIMIT 3
        """, (emb, emb))

        return cur.fetchall()

    # =========================
    # KB CONTEXT
    # =========================
    def build_kb_context(self, rows):
        """Builds a context string from the knowledge base rows.

        Args:
            rows (list): A list of knowledge base rows.

        Returns:
            str: A formatted string representing the knowledge base context.
        """

        return "\n".join(
            f"- {e} → {a} ({int(s*100)}%)"
            for e, a, _, s in rows if s > 0.65
        )

    # =========================
    # PROMPT
    # =========================
    def build_prompt(self, dag_id, task_id, error, metadata, kb):
        """Builds a prompt for the AI model.

        Args:
            dag_id (str): The DAG ID.
            task_id (str): The task ID.
            error (str): The error message.
            metadata (dict): Additional metadata.
            kb (str): Knowledge base context.

        Returns:
            str: The formatted prompt.
        """

        return f"""
    You are a senior Airflow reliability engineer.

    DAG: {dag_id}
    Task: {task_id}

    Error:
    {error}

    Context:
    {json.dumps(metadata, indent=2)}

    Similar failures:
    {kb if kb else "No strong matches"}

    Rules:
    - Timeout / network errors → RERUN
    - Code / schema errors → NO_RERUN
    - If unsure → RERUN

    Return STRICT JSON ONLY:
    {{
        "summary": "<brief explanation>",
        "decision": "RERUN or NO_RERUN",
        "confidence": <0-100>
    }}
    """

    def extract_json(self, text):
        """Extracts JSON from the given text.

        Args:
            text (str): The text containing JSON.

        Returns:
            dict: The extracted JSON or a default structure if extraction fails.
        """

        try:
            return json.loads(re.search(r"\{.*\}", text, re.DOTALL).group())
        except:
            return {
                "summary": text[:200],
                "decision": "RERUN",
                "confidence": 50
            }

    def execute(self, context):
        """Main execution method for the operator.  Processes recent failures, analyzes them using the LLM, and updates the database with AI decisions.

        Args:
            context (dict): The Airflow context.

        Returns:
            None
        """

        self.log.info("🚀 AI Failure Analysis Started")

        conn = self.get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, dag_id, task_id, error, metadata
            FROM airflow_feed.pipeline_signals
            WHERE signal_type = 'failure'
              AND ai_review IS NULL
              AND ai_action IS NULL
              AND user_confirmation = 'pending'
            LIMIT 25
        """)

        rows = cur.fetchall()

        self.log.info(f"Found {len(rows)} failures")

        for id, dag_id, task_id, error, metadata in rows:

            self.log.info(f"➡️ Processing {dag_id} | {task_id}")

            kb_rows = self.find_similar(cur, error)
            kb_context = self.build_kb_context(kb_rows)

            if kb_context:
                self.log.info(f"🔍 KB Context:\n{kb_context}")

            try:
                res = requests.post(
                    self.ollama_url,
                    json={
                        "model": self.model,
                        "prompt": self.build_prompt(dag_id, task_id, error, metadata, kb_context),
                        "stream": False
                    },
                    timeout=100
                )

                if res.status_code != 200:
                    raise Exception(res.text)

                parsed = self.extract_json(res.json()["response"])

                summary = parsed.get("summary", "")
                decision = parsed.get("decision", "RERUN")
                confidence = parsed.get("confidence", 50)

            except Exception as e:
                self.log.error(f"LLM failed: {e}")
                summary = "LLM failed"
                decision = "RERUN"
                confidence = 40

            self.log.info(f"👉 Final decision: {decision} (conf={confidence})")

            cur.execute("""
                UPDATE airflow_feed.pipeline_signals
                SET ai_review=%s, ai_action=%s, confidence_score=%s
                WHERE id=%s
            """, (summary, decision, confidence, id))

            conn.commit()

        conn.close()

        self.log.info("✅ AI Analysis Complete")
