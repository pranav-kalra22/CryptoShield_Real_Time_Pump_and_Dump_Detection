"""
╔══════════════════════════════════════════════════════════════╗
║      Crypto-Shield — Dashboard (v2)                         ║
║  Live alerts + Precision/Recall metrics + Source breakdown  ║
╚══════════════════════════════════════════════════════════════╝

Run:
    streamlit run dashboard.py
"""

import time
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from pymongo import MongoClient
from datetime import datetime

# ──────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────
MONGO_URI   = "mongodb://localhost:27017/"
MONGO_DB    = "crypto_shield"
REFRESH_SEC = 5

st.set_page_config(page_title="Crypto-Shield", page_icon="🛡️", layout="wide")

# ──────────────────────────────────────────────────────────
# CUSTOM CSS
# ──────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #1e1e2e; border-radius: 12px;
        padding: 16px; text-align: center;
    }
    .critical { color: #EF4444; font-size: 2rem; font-weight: bold; }
    .high     { color: #F97316; font-size: 2rem; font-weight: bold; }
    .ok       { color: #22C55E; font-size: 2rem; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

st.title("🛡️ Crypto-Shield — Real-Time Fraud Detection")
st.caption("Detecting Pump-and-Dump via behavioral graph analysis on Kaggle + CoinGecko data")

# ──────────────────────────────────────────────────────────
# MONGO HELPERS
# ──────────────────────────────────────────────────────────
@st.cache_resource
def get_mongo():
    return MongoClient(MONGO_URI)

def fetch_alerts(limit: int = 300) -> pd.DataFrame:
    docs = list(
        get_mongo()[MONGO_DB]["alerts"]
        .find({}, {"_id": 0})
        .sort("detected_at", -1)
        .limit(limit)
    )
    return pd.DataFrame(docs) if docs else pd.DataFrame()

def fetch_metrics() -> dict:
    doc = get_mongo()[MONGO_DB]["metrics"].find_one({"_id": "running_metrics"})
    return doc or {}

# ──────────────────────────────────────────────────────────
# SIDEBAR — Controls
# ──────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Controls")
    severity_filter = st.multiselect(
        "Filter by Severity",
        ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        default=["CRITICAL", "HIGH"],
    )
    alert_type_filter = st.multiselect(
        "Filter by Alert Type",
        ["PUMP_AND_DUMP_CONFIRMED", "CLIQUE_DETECTED", "STAR_TOPOLOGY"],
        default=["PUMP_AND_DUMP_CONFIRMED", "CLIQUE_DETECTED", "STAR_TOPOLOGY"],
    )
    st.divider()
    st.markdown("**Data Sources Active**")
    st.markdown("🟡 CoinGecko (live)")
    st.markdown("📦 Kaggle tweets replay")
    st.markdown("📦 Kaggle OHLCV replay")
    st.divider()
    st.markdown("**Web UIs**")
    st.markdown("[Kafka UI →](http://localhost:8080)")
    st.markdown("[MongoDB UI →](http://localhost:8082)")

# ──────────────────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────────────────
placeholder = st.empty()

while True:
    df      = fetch_alerts()
    metrics = fetch_metrics()

    with placeholder.container():

        # ── Row 1: KPIs ────────────────────────────────────
        k1, k2, k3, k4, k5 = st.columns(5)
        total    = len(df)
        critical = len(df[df["severity"] == "CRITICAL"]) if not df.empty else 0
        high     = len(df[df["severity"] == "HIGH"])     if not df.empty else 0
        confirmed = len(df[df["alert_type"] == "PUMP_AND_DUMP_CONFIRMED"]) if not df.empty else 0
        precision = metrics.get("precision", 0)

        k1.metric("🚨 Total Alerts",          total)
        k2.metric("🔴 Critical",              critical)
        k3.metric("🟠 High",                  high)
        k4.metric("⚡ P&D Confirmed",          confirmed)
        k5.metric("🎯 Precision",             f"{precision*100:.1f}%")

        st.divider()

        # ── Row 2: Precision/Recall gauge + Alert timeline ─
        col_left, col_right = st.columns([1, 2])

        with col_left:
            st.subheader("📊 Detection Quality")
            if metrics:
                p  = metrics.get("precision", 0)
                r  = metrics.get("recall", 0)
                f1 = metrics.get("f1_score", 0)

                fig_gauge = go.Figure()
                for label, val, color, row, col in [
                    ("Precision", p,  "#22C55E", 1, 1),
                    ("Recall",    r,  "#3B82F6", 1, 2),
                    ("F1 Score",  f1, "#A855F7", 2, 1),
                ]:
                    fig_gauge.add_trace(go.Indicator(
                        mode="gauge+number",
                        value=val * 100,
                        title={"text": label, "font": {"size": 13}},
                        number={"suffix": "%", "font": {"size": 18}},
                        gauge={
                            "axis": {"range": [0, 100]},
                            "bar":  {"color": color},
                            "steps": [
                                {"range": [0, 50],  "color": "#2d2d2d"},
                                {"range": [50, 85], "color": "#3d3d3d"},
                                {"range": [85, 100],"color": "#4d4d4d"},
                            ],
                            "threshold": {
                                "line": {"color": "white", "width": 2},
                                "thickness": 0.75,
                                "value": 85,
                            },
                        },
                        domain={"row": row - 1, "column": col - 1},
                    ))

                fig_gauge.update_layout(
                    grid={"rows": 2, "columns": 2, "pattern": "independent"},
                    height=320,
                    margin=dict(t=20, b=10, l=10, r=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_gauge, use_container_width=True)

                tp = metrics.get("true_positives", 0)
                fp = metrics.get("false_positives", 0)
                fn = metrics.get("false_negatives", 0)
                st.caption(f"TP={tp}  FP={fp}  FN={fn}  "
                           f"Batches={metrics.get('total_batches', 0)}")
            else:
                st.info("No metrics yet — start the consumer.")

        with col_right:
            st.subheader("📈 Alert Volume Over Time")
            if not df.empty:
                df["detected_at"] = pd.to_datetime(df["detected_at"], errors="coerce")
                ts = (
                    df.groupby([pd.Grouper(key="detected_at", freq="1min"), "alert_type"])
                    .size()
                    .reset_index(name="count")
                )
                fig_area = px.area(
                    ts, x="detected_at", y="count", color="alert_type",
                    color_discrete_map={
                        "PUMP_AND_DUMP_CONFIRMED": "#EF4444",
                        "CLIQUE_DETECTED":         "#F97316",
                        "STAR_TOPOLOGY":           "#EAB308",
                    },
                )
                fig_area.update_layout(
                    margin=dict(t=0, b=30, l=30, r=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    legend_title_text="Alert Type",
                )
                st.plotly_chart(fig_area, use_container_width=True)
            else:
                st.info("No alerts yet — start producer + consumer.")

        st.divider()

        # ── Row 3: Alert type breakdown + Token heatmap ────
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🍩 Alert Type Breakdown")
            if not df.empty:
                counts = df["alert_type"].value_counts().reset_index()
                counts.columns = ["Type", "Count"]
                fig_pie = go.Figure(go.Pie(
                    labels=counts["Type"], values=counts["Count"], hole=0.45,
                    marker_colors=["#EF4444", "#F97316", "#EAB308", "#A855F7"],
                ))
                fig_pie.update_layout(margin=dict(t=0, b=0), paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_pie, use_container_width=True)

        with c2:
            st.subheader("🪙 Most Targeted Tokens")
            if not df.empty and "pumping_tokens" in df.columns:
                token_exploded = df["pumping_tokens"].dropna().explode()
                if len(token_exploded) > 0:
                    token_counts = token_exploded.value_counts().head(10).reset_index()
                    token_counts.columns = ["Token", "Alert Count"]
                    fig_bar = px.bar(
                        token_counts, x="Alert Count", y="Token",
                        orientation="h",
                        color="Alert Count",
                        color_continuous_scale="Reds",
                    )
                    fig_bar.update_layout(
                        margin=dict(t=0, b=0),
                        paper_bgcolor="rgba(0,0,0,0)",
                        coloraxis_showscale=False,
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)

        st.divider()

        # ── Row 4: Raw alert table ──────────────────────────
        st.subheader("📋 Recent Alerts")
        if not df.empty:
            # Apply sidebar filters
            filtered = df
            if severity_filter:
                filtered = filtered[filtered.get("severity", pd.Series()).isin(severity_filter)]
            if alert_type_filter:
                filtered = filtered[filtered.get("alert_type", pd.Series()).isin(alert_type_filter)]

            display_cols = [c for c in [
                "detected_at", "alert_type", "severity", "confidence",
                "social_edge_count", "hub_node", "node_count",
                "edge_density", "price_corroborated", "pumping_tokens",
            ] if c in filtered.columns]

            st.dataframe(filtered[display_cols].head(50),
                         use_container_width=True, hide_index=True)
        else:
            st.info("No alerts yet.")

        st.caption(
            f"🕐 Last refreshed: {datetime.now().strftime('%H:%M:%S')} — "
            f"auto-refresh every {REFRESH_SEC}s"
        )

    time.sleep(REFRESH_SEC)
    st.rerun()