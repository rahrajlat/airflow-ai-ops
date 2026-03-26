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


def success_callback(context):

    ti = context.get("task_instance")

    dag_id = ti.dag_id if ti else context.get("dag").dag_id
    task_id = ti.task_id if ti else "DAG"
    run_id = context["run_id"]
    try_number = ti.try_number if ti else 1

    signal_type = "load" if ti else "success"
    error_msg = "Task completed" if ti else "DAG completed successfully"

    print(f"📊 SUCCESS signal: {dag_id} | {task_id} | try={try_number}")

    conn = get_pg_conn()
    cur = conn.cursor()

    # =========================
    # CASE 1: FIRST RUN SUCCESS
    # =========================
    if try_number == 1:

        cur.execute(
            """
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
                status,
                run_status,
                rerun_status,
                no_of_attempts
            )
            VALUES (
                NOW(),
                %s,
                'info',
                %s, %s, %s, %s, %s,
                NULL, NULL, 'pending', 'completed',
                'success', NULL, %s
            )
            ON CONFLICT (dag_id, run_id, task_id, signal_type)
            DO UPDATE SET
                run_status = 'success',
                no_of_attempts = EXCLUDED.no_of_attempts,
                created_at = NOW()
            """,
            (
                signal_type,
                dag_id,
                task_id,
                run_id,
                error_msg,
                json.dumps({
                    "try_number": try_number,
                    "execution_date": str(context["logical_date"])
                }),
                try_number
            )
        )

    # =========================
    # CASE 2: RETRY SUCCESS
    # =========================
    else:

        cur.execute(
            """
    UPDATE airflow_feed.pipeline_signals
    SET rerun_status = 'fixed on rerun',
        no_of_attempts = %s,
        status = 'completed',
        metadata = %s,
        created_at = NOW()
    WHERE id = (
        SELECT id FROM airflow_feed.pipeline_signals
        WHERE dag_id = %s
        AND task_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    )
    """,
            (
                try_number,
                json.dumps({
                    "try_number": try_number,
                    "execution_date": str(context["logical_date"])
                }),
                dag_id,
                task_id
            )
        )

    conn.commit()
    cur.close()
    conn.close()
