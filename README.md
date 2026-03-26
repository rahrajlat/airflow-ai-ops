# <u>airflow-ai-ops</u>
A self-healing data pipeline platform built on Airflow that uses AI to classify failures, intelligently rerun tasks, and continuously learn from outcomes via vector embeddings and feedback loops.

# <u>Overview</u>

Airflow AI Ops is an intelligent platform that automatically detects, analyzes, and recovers from Airflow task failures.

Instead of manually debugging logs and retrying tasks blindly, the system uses LLMs + vector search + feedback loops to make context-aware recovery decisions and continuously improve over time.

**What it does?**

📡 Captures Airflow task failures in real-time via **callbacks**
🔍 Uses **semantic** search (pgvector) to find similar historical errors
🤖 Applies **LLM reasoning + context** to classify failures
⚡ Recommends actions: **RERUN / NO_RERUN** with confidence scoring
🧑‍💻 Supports **human-in-the-loop** approvals for edge cases
🔁 Automatically reruns tasks based on **confidence + SLA rules**
📚 Learns from successful recoveries and updates its **knowledge base**

## <u>Tech Stack</u>

| Layer                | Technology | Description |
|---------------------|-----------|------------|
| Orchestration       | [Apache Airflow](https://airflow.apache.org/) | Manages DAGs, callbacks, and recovery workflows |
| Storage             | [PostgreSQL](https://www.postgresql.org/) | Central state store for pipeline signals |
| Semantic Search     | [pgvector](https://github.com/pgvector/pgvector) | Enables similarity-based error retrieval |
| AI Reasoning        | [Ollama](https://ollama.com/) | Runs LLMs locally for classification |
| Embeddings          | [nomic-embed-text](https://ollama.com/library/nomic-embed-text) | Generates vector embeddings |
| Application Layer   | [Python](https://www.python.org/) | Implements AI logic and Airflow operators |
| User Interface      | [Streamlit](https://streamlit.io/) | Review, approve, and monitor decisions |
| Knowledge Layer     | [YAML](https://yaml.org/) | Human-readable + auto-learned knowledge base |
| Integration         | [Airflow REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html) | Programmatic task reruns |
| Infrastructure      | [Docker](https://www.docker.com/) | Containerized runtime |

## <u>Architecture</u>

<div style="padding:12px; background:#0f172a; border-radius:16px; border:1px solid #8d9db3;">
  <img src="assets/Arch.png" width="100%" style="border-radius:12px;">
</div>

## <u>Process Flow</u>
