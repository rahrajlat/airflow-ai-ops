import psycopg2
import json


def get_pg_conn():
    return psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        dbname="flow_feed_db",
        user="admin",
        password="admin",
    )


def extract_error(context):
    exception = context.get("exception")

    if not exception:
        return "Unknown failure"

    error = str(exception)

    if error.startswith("Exception: "):
        error = error[len("Exception: "):]

    return error[:200]


def failure_callback(context):

    ti = context["task_instance"]

    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = context["run_id"]

    error_msg = extract_error(context)

    print(f"🚨 Failure captured: {dag_id} | {task_id} | {error_msg}")

    conn = get_pg_conn()
    cur = conn.cursor()

    # =========================
    # 🔥 STEP 1: CHECK IF THIS IS A RERUN
    # =========================
    cur.execute("""
        SELECT id, no_of_attempts
        FROM airflow_feed.pipeline_signals
        WHERE dag_id = %s
          AND task_id = %s
          AND rerun_status = 'processing'
        ORDER BY created_at DESC
        LIMIT 1
    """, (dag_id, task_id))

    row = cur.fetchone()

    if row:
        signal_id, attempts = row

        print(f"🔁 Rerun failure detected → updating existing row {signal_id}")

        # =========================
        # 🔥 UPDATE SAME ROW (NO INSERT)
        # =========================
        cur.execute("""
            UPDATE airflow_feed.pipeline_signals
            SET 
                error = %s,
                no_of_attempts = COALESCE(no_of_attempts, 0) + 1,
                rerun_status = 'failed',
                metadata = jsonb_set(
                    COALESCE(metadata, '{}'),
                    '{last_error}',
                    to_jsonb(%s::text)
                ),
                created_at = NOW()
            WHERE id = %s
        """, (
            error_msg,
            error_msg,
            signal_id
        ))

    else:
        print("🆕 First failure → inserting new row")

        # =========================
        # 🔥 NORMAL INSERT (FIRST FAILURE)
        # =========================
        cur.execute("""
            INSERT INTO airflow_feed.pipeline_signals (
                created_at,
                signal_type,
                severity,
                dag_id,
                task_id,
                run_id,
                error,
                metadata,
                ai_review,
                ai_action,
                user_confirmation,
                no_of_attempts,
                rerun_status
            )
            VALUES (
                NOW(),
                'failure',
                'critical',
                %s, %s, %s, %s, %s,
                NULL, NULL, 'pending',
                0,
                NULL
            )
            ON CONFLICT (dag_id, run_id, task_id, signal_type)
            DO UPDATE SET
                error = EXCLUDED.error,
                metadata = EXCLUDED.metadata,
                created_at = NOW()
        """, (
            dag_id,
            task_id,
            run_id,
            error_msg,
            json.dumps({
                "log_url": ti.log_url,
                "try_number": ti.try_number,
                "execution_date": str(context["logical_date"])
            })
        ))

    conn.commit()
    cur.close()
    conn.close()
