-- =========================
-- EXTENSIONS
-- =========================
CREATE EXTENSION IF NOT EXISTS vector;

-- =========================
-- SCHEMA
-- =========================
CREATE SCHEMA IF NOT EXISTS airflow_feed;

-- =========================
-- TABLE: error_kb
-- =========================
CREATE TABLE IF NOT EXISTS airflow_feed.error_kb (
    id SERIAL PRIMARY KEY,
    service TEXT,
    error_text TEXT,
    normalized_error TEXT,
    embedding VECTOR(768),
    suggested_action TEXT,
    root_cause TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    source TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_error_kb_unique
ON airflow_feed.error_kb (service, normalized_error);

-- =========================
-- TABLE: pipeline_signals
-- =========================
CREATE TABLE IF NOT EXISTS airflow_feed.pipeline_signals (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT NOW(),
    signal_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    dag_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    error TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    ai_review TEXT,
    ai_action TEXT,
    user_confirmation TEXT DEFAULT 'pending',
    headline TEXT,
    status TEXT,
    no_of_attempts INT DEFAULT 0,
    rerun_status TEXT,
    confidence_score FLOAT,
    run_status TEXT,
    CONSTRAINT unique_signal UNIQUE (dag_id, run_id, task_id, signal_type)
);

-- =========================
-- VECTOR INDEX (IMPORTANT)
-- =========================
CREATE INDEX IF NOT EXISTS idx_embedding
ON airflow_feed.error_kb
USING ivfflat (embedding vector_cosine_ops);