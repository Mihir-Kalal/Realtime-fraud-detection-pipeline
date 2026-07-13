"""
monitoring/dashboard.py — Phase 7 (Monitoring Dashboard)

Streamlit dashboard showing live fraud rate, traffic split, latency SLA, and drift.
"""
import sys
import os
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.db import get_conn, dict_cursor

st.set_page_config(page_title="Fraud Pipeline Monitor", layout="wide")

st.title("Fraud Pipeline Monitoring Dashboard")

# --- Locust Load Testing ---
st.subheader("Manual Load Testing Controls")
components.iframe("http://localhost:8089", height=500, scrolling=True)

@st.cache_data(ttl=5)
def fetch_data(query: str) -> pd.DataFrame:
    try:
        with get_conn(retries=1) as conn:
            with dict_cursor(conn) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"DB Error: {e}")
        return pd.DataFrame()

# 1. Traffic Split
st.subheader("Model Traffic Split (Last 1000 Predictions)")
split_df = fetch_data("SELECT model_version, COUNT(*) as count FROM (SELECT model_version FROM predictions ORDER BY txn_id DESC LIMIT 1000) sub GROUP BY model_version")
if not split_df.empty:
    fig_split = px.pie(split_df, values='count', names='model_version', title="Traffic Split by Model Version")
    st.plotly_chart(fig_split, use_container_width=True)
else:
    st.write("No prediction data available.")

# 2. Latency SLA Compliance
st.subheader("Latency SLA Compliance (<100ms)")
latency_df = fetch_data("SELECT txn_id, latency_ms FROM predictions ORDER BY txn_id DESC LIMIT 500")
if not latency_df.empty:
    latency_df['SLA_Met'] = latency_df['latency_ms'] < 100
    fig_lat = px.scatter(latency_df, y='latency_ms', color='SLA_Met', title="Latency per Transaction (ms)",
                         color_discrete_map={True: 'green', False: 'red'})
    fig_lat.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="100ms SLA")
    st.plotly_chart(fig_lat, use_container_width=True)

# 3. Live Fraud Rate
st.subheader("Live Fraud Rate (Flagged % over recent txns)")
recent_preds = fetch_data("SELECT is_flagged FROM predictions ORDER BY scored_at DESC LIMIT 1000")
if not recent_preds.empty:
    rate = recent_preds['is_flagged'].mean() * 100
    st.metric(label="Recent Fraud Flag Rate (%)", value=f"{rate:.2f}%")
fraud_rate_df = fetch_data("""
    SELECT
        date_trunc('minute', scored_at) as minute,
        COUNT(*) as total,
        SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) as flagged
    FROM predictions
    WHERE scored_at >= now() - interval '1 hour'
    GROUP BY minute ORDER BY minute DESC LIMIT 60
""")
if not fraud_rate_df.empty:
    fraud_rate_df['fraud_rate_pct'] = (fraud_rate_df['flagged'] / fraud_rate_df['total'].replace(0, 1)) * 100
    fig_rate = px.line(fraud_rate_df, x='minute', y='fraud_rate_pct', title="Fraud Flag Rate (%) per Minute")
    st.plotly_chart(fig_rate, use_container_width=True)

# 4. Drift Metrics
st.subheader("Latest Drift Checks (Phase 5)")
drift_df = fetch_data("SELECT checked_at, model_version, max_feature_psi, drift_detected, retrain_triggered FROM drift_events ORDER BY checked_at DESC LIMIT 10")
if not drift_df.empty:
    st.dataframe(drift_df)
else:
    st.write("No drift events recorded.")

# 5. A/B Test Metrics (from labels)
st.subheader("A/B Test Metrics (Label Joined)")
ab_df = fetch_data("""
    SELECT 
        p.model_version,
        COUNT(*) as count,
        SUM(CASE WHEN p.is_flagged AND l.is_fraud THEN 1 ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN p.is_flagged THEN 1 ELSE 0 END), 0) as precision,
        SUM(CASE WHEN p.is_flagged AND l.is_fraud THEN 1 ELSE 0 END)::FLOAT / NULLIF(SUM(CASE WHEN l.is_fraud THEN 1 ELSE 0 END), 0) as recall
    FROM predictions p
    JOIN labels l ON p.txn_id = l.txn_id
    GROUP BY p.model_version
""")
if not ab_df.empty:
    st.dataframe(ab_df)
else:
    st.write("No joined label metrics available yet.")
