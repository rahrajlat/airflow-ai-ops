import streamlit as st
import psycopg2
import pandas as pd
import json


# =========================
# CONFIG
# =========================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "flow_feed_db",
    "user": "admin",
    "password": "admin",
}


# =========================
# DB
# =========================
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def fetch_df(query):
    conn = get_conn()
    df = pd.read_sql(query, conn)
    conn.close()

    # Fix types
    if "confidence_score" in df.columns:
        df["confidence_score"] = pd.to_numeric(
            df["confidence_score"], errors="coerce")

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    return df


def update_status(row_id, status):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE airflow_feed.pipeline_signals
        SET user_confirmation = %s
        WHERE id = %s
    """, (status, row_id))
    conn.commit()
    conn.close()


def parse_summary(x):
    try:
        return json.loads(x).get("summary", "")
    except:
        return x


# =========================
# PAGE CONFIG
# =========================
st.set_page_config(page_title="Airflow AI Ops", layout="wide")
st.title("🤖 Airflow AI Ops Control Center")


# =========================
# LOAD DATA (BASE)
# =========================
base_df = fetch_df("""
SELECT *
FROM airflow_feed.pipeline_signals
WHERE signal_type='failure'
""")


# =========================
# SIDEBAR NAV
# =========================
st.sidebar.title("Navigation")

page = st.sidebar.radio(
    "Go to",
    ["📊 Overview", "🟢 Pending", "🔴 Manual", "🟡 Approved",
        "❌ Failed", "📈 Analytics", "🧠 Intelligence"]
)


if page == "📊 Overview":

    st.subheader("System Overview")

    df = base_df.copy()

    if df.empty:
        st.info("No data available")
    else:

        # =========================
        # PREP
        # =========================
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

        # =========================
        # CORE METRICS
        # =========================
        total = len(df)
        success = len(df[df["rerun_status"] == "success"])
        failed = len(df[df["rerun_status"] == "failed"])

        success_rate = (success / (success + failed) *
                        100) if (success + failed) > 0 else 0

        c1, c2, c3 = st.columns(3)

        c1.metric("Failures", total)
        c2.metric("Recovery Rate", f"{round(success_rate, 1)}%")
        c3.metric("Failed Reruns", failed)

        st.markdown("---")

        # =========================
        # LOAD PATTERN
        # =========================
        st.markdown("### Load Pattern")

        df["hour"] = df["created_at"].dt.hour
        hourly = df["hour"].value_counts().sort_index()

        st.area_chart(hourly)

        if not hourly.empty:
            peak = hourly.idxmax()
            low = hourly.idxmin()
            st.caption(f"Peak: {peak}:00  •  Low: {low}:00")

        st.markdown("---")

        # =========================
        # PROBLEMATIC DAGS (TABLE)
        # =========================
        st.markdown("### Problematic DAGs")

        dag_table = (
            df.groupby("dag_id")
            .agg(
                failures=("dag_id", "count"),
                rerun_success=("rerun_status", lambda x: (
                    x == "success").sum()),
                rerun_failed=("rerun_status", lambda x: (x == "failed").sum())
            )
            .sort_values("failures", ascending=False)
            .head(10)
            .reset_index()
        )

        # Success rate
        dag_table["success_rate"] = (
            dag_table["rerun_success"] /
            (dag_table["rerun_success"] + dag_table["rerun_failed"])
        ).fillna(0).round(2)

        # Rename columns
        dag_table.columns = [
            "DAG",
            "Failures",
            "Rerun Success",
            "Rerun Failed",
            "Success Rate"
        ]

        st.dataframe(dag_table, use_container_width=True)

        st.markdown("---")

        # =========================
        # SIMPLE SYSTEM INSIGHT
        # =========================
        if success_rate > 75:
            st.success("System stable")
        elif success_rate > 50:
            st.warning("Some instability detected")
        else:
            st.error("Frequent failures — needs attention")

elif page == "🔴 Manual":

    st.subheader("🔴 Manual Review")

    df = base_df[
        (base_df["ai_action"] == "NO_RERUN") &
        (base_df["user_confirmation"] == "pending")
    ]

    if df.empty:
        st.success("No manual items")
    else:
        for _, row in df.iterrows():

            summary = ""
            log_url = None

            # =========================
            # Parse AI + Metadata
            # =========================
            try:
                parsed = json.loads(row["ai_review"])
                summary = parsed.get("summary", "")
            except:
                summary = row["ai_review"]

            try:
                metadata = row.get("metadata")
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)

                log_url = metadata.get("log_url") or metadata.get("logUrl")
            except:
                log_url = None

            with st.container(border=True):

                # =========================
                # HEADER
                # =========================
                st.markdown(f"### {row['dag_id']} → `{row['task_id']}`")

                # =========================
                # METRICS ROW (same as Pending)
                # =========================
                conf = row.get("confidence_score", 0) or 0

                col1, col2, col3, col4 = st.columns(4)

                col1.metric("Attempts", row.get("no_of_attempts", 0))
                col2.metric("AI Decision", row.get("ai_action", ""))

                # Confidence
                if conf >= 80:
                    col3.success(f"Confidence\n{conf}%")
                elif conf >= 60:
                    col3.warning(f"Confidence\n{conf}%")
                else:
                    col3.error(f"Confidence\n{conf}%")

                col4.metric("Status", row.get("rerun_status") or "manual")

                # =========================
                # ERROR
                # =========================
                st.markdown("**🚨 Error Message**")
                st.code(row["error"], language="text")

                # =========================
                # AI SUMMARY
                # =========================
                st.markdown("**🧠 AI Summary**")
                st.write(summary or "No summary available")

                # =========================
                # LOG LINK
                # =========================
                if log_url:
                    st.markdown(f"🔗 [View Airflow Logs]({log_url})")

                # =========================
                # ACTIONS
                # =========================
                st.markdown("---")

                col1, col2 = st.columns(2)

                with col1:
                    if st.button("✅ Mark Reviewed", key=f"m_{row['id']}"):
                        update_status(row["id"], "reviewed")
                        st.rerun()

                with col2:
                    if st.button("🔁 Move to RERUN", key=f"r_{row['id']}"):
                        update_status(row["id"], "approved")
                        st.rerun()


# =========================
# PENDING
# =========================
elif page == "🟢 Pending":

    st.subheader("🟢 Pending Approvals")

    df = base_df[
        (base_df["ai_action"] == "RERUN") &
        (base_df["user_confirmation"] == "pending")
    ]

    if df.empty:
        st.success("No pending approvals")
    else:
        for _, row in df.iterrows():

            summary = ""
            log_url = None

            # =========================
            # Parse AI summary
            # =========================
            try:
                parsed = json.loads(row["ai_review"])
                summary = parsed.get("summary", "")
            except:
                summary = row["ai_review"]

            # =========================
            # Parse metadata (safe)
            # =========================
            try:
                metadata = row.get("metadata")

                if isinstance(metadata, str):
                    metadata = json.loads(metadata)

                log_url = metadata.get("log_url") or metadata.get("logUrl")
            except:
                log_url = None

            with st.container(border=True):

                # =========================
                # HEADER
                # =========================
                st.markdown(f"### {row['dag_id']} → `{row['task_id']}`")

                # =========================
                # METRICS ROW
                # =========================
                conf = row.get("confidence_score", 0)
                if pd.isna(conf):
                    conf = 0

                col1, col2, col3, col4 = st.columns(4)

                col1.metric("Attempts", row.get("no_of_attempts", 0))
                col2.metric("AI Decision", row.get("ai_action", ""))

                # FIX: confidence safely cast to int
                conf_int = int(conf)

                if conf_int >= 80:
                    col3.success(f"Confidence\n{conf_int}%")
                elif conf_int >= 60:
                    col3.warning(f"Confidence\n{conf_int}%")
                else:
                    col3.error(f"Confidence\n{conf_int}%")

                col4.metric("Status", row.get("rerun_status") or "ready")

                # =========================
                # ERROR
                # =========================
                st.markdown("**🚨 Error Message**")
                st.code(str(row["error"]), language="text")

                # =========================
                # AI SUMMARY
                # =========================
                st.markdown("**🧠 AI Summary**")
                st.write(summary or "No summary available")

                # =========================
                # LOG LINK
                # =========================
                if log_url:
                    st.markdown(f"🔗 [View Airflow Logs]({log_url})")

                # =========================
                # ACTIONS
                # =========================
                st.markdown("---")

                colA, colB = st.columns(2)

                with colA:
                    if st.button("✅ Approve Rerun", key=f"a_{row['id']}"):
                        update_status(row["id"], "approved")
                        st.rerun()

                with colB:
                    if st.button("❌ Send to Manual", key=f"r_{row['id']}"):
                        update_status(row["id"], "manual")
                        st.rerun()

# =========================
# APPROVED
# =========================
elif page == "🟡 Approved":

    st.subheader("🟡 Approved (Awaiting Execution)")

    df = base_df[
        (base_df["user_confirmation"] == "approved") &
        (base_df["ai_action"] == "RERUN")
    ]

    for _, row in df.iterrows():
        with st.container(border=True):
            st.markdown(f"{row['dag_id']} → `{row['task_id']}`")
            st.write("Waiting for rerun execution...")

elif page == "❌ Failed":

    st.subheader("❌ Failed Reruns")

    df = base_df[base_df["rerun_status"] == "failed"]

    if df.empty:
        st.success("No rerun failures 🎉")
    else:
        for _, row in df.iterrows():

            summary = ""
            log_url = None

            # =========================
            # Parse AI + Metadata
            # =========================
            try:
                parsed = json.loads(row["ai_review"])
                summary = parsed.get("summary", "")
            except:
                summary = row["ai_review"]

            try:
                metadata = row.get("metadata")
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)

                log_url = metadata.get("log_url") or metadata.get("logUrl")
            except:
                log_url = None

            with st.container(border=True):

                # =========================
                # HEADER
                # =========================
                st.markdown(f"### {row['dag_id']} → `{row['task_id']}`")

                # =========================
                # METRICS ROW (same structure)
                # =========================
                conf = row.get("confidence_score", 0) or 0

                col1, col2, col3, col4 = st.columns(4)

                col1.metric("Attempts", row.get("no_of_attempts", 0))
                col2.metric("AI Decision", row.get("ai_action", ""))

                # Confidence
                if conf >= 80:
                    col3.success(f"Confidence\n{conf}%")
                elif conf >= 60:
                    col3.warning(f"Confidence\n{conf}%")
                else:
                    col3.error(f"Confidence\n{conf}%")

                col4.metric("Status", "failed")

                # =========================
                # ERROR
                # =========================
                st.markdown("**🚨 Error Message**")
                st.code(row["error"], language="text")

                # =========================
                # AI SUMMARY
                # =========================
                st.markdown("**🧠 AI Summary**")
                st.write(summary or "No summary available")

                # =========================
                # LOG LINK
                # =========================
                if log_url:
                    st.markdown(f"🔗 [View Airflow Logs]({log_url})")

                # =========================
                # FAILURE ALERT (🔥 KEY ADDITION)
                # =========================
                st.error(
                    "⚠️ Rerun failed again — AI decision may be incorrect or incomplete")

                # =========================
                # ACTIONS
                # =========================
                st.markdown("---")

                col1, col2 = st.columns(2)

                with col1:
                    if st.button("🔁 Retry Again", key=f"retry_{row['id']}"):
                        update_status(row["id"], "approved")  # requeue
                        st.rerun()

                with col2:
                    if st.button("🧠 Send to Manual Review", key=f"manual_{row['id']}"):
                        update_status(row["id"], "manual")
                        st.rerun()


# =========================
# ANALYTICS
# =========================
elif page == "📈 Analytics":

    st.subheader("📈 Analytics")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Decision Distribution")
        st.bar_chart(base_df["ai_action"].value_counts())

    with col2:
        st.markdown("### Confidence Distribution")
        st.bar_chart(base_df["confidence_score"].value_counts().sort_index())

    st.markdown("### Top Errors")
    base_df["error_short"] = base_df["error"].str[:80]
    st.bar_chart(base_df["error_short"].value_counts().head(10))


# =========================
# INTELLIGENCE (HEADLINES)
# =========================
elif page == "🧠 Intelligence":

    st.subheader("🧠 System Intelligence")

    now = pd.Timestamp.now()
    last_24h = base_df[base_df["created_at"] >= now - pd.Timedelta(hours=24)]

    total = len(last_24h)
    rerun = len(last_24h[last_24h["ai_action"] == "RERUN"])

    avg_conf = last_24h["confidence_score"].mean()

    st.metric("Failures (24h)", total)
    st.metric("Rerun Suggested", rerun)
    st.metric("Avg Confidence", round(avg_conf, 2)
              if pd.notna(avg_conf) else 0)

    st.markdown("---")

    st.markdown("### Peak Hours")
    last_24h["hour"] = last_24h["created_at"].dt.hour
    st.bar_chart(last_24h["hour"].value_counts().sort_index())

    st.markdown("### Smart Headline")

    if avg_conf > 75:
        st.success("System stable — high confidence decisions")
    elif avg_conf > 50:
        st.warning("Moderate system performance")
    else:
        st.error("Low confidence — needs attention")
