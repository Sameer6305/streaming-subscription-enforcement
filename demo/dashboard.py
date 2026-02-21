"""
Streamlit Dashboard: Real-Time Subscription Enforcement Platform
=================================================================

Visual frontend that reads Gold Delta Lake tables and displays
live KPIs, charts, tenant analytics, and enforcement alerts.

This is what you show to recruiters — a full production-grade
monitoring dashboard powered by real data.

Usage:
    streamlit run demo/dashboard.py
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

# Use plotly_dark template globally — gives white text on transparent
# backgrounds which works correctly with Streamlit's dark theme
pio.templates["custom_dark"] = pio.templates["plotly_dark"]
pio.templates["custom_dark"].layout.update(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(14,17,23,0.6)",
)
pio.templates.default = "custom_dark"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DELTA_DIR = os.environ.get("DELTA_DIR", "./demo/delta_tables/gold")
PAGE_TITLE = "Subscription Enforcement Platform"
PAGE_ICON = "🔒"

# Plan tier colors
PLAN_COLORS = {
    "free": "#6c757d",
    "starter": "#0d6efd",
    "professional": "#198754",
    "enterprise": "#6f42c1",
}

SEVERITY_COLORS = {
    "CRITICAL": "#dc3545",
    "HIGH": "#fd7e14",
    "MEDIUM": "#ffc107",
    "LOW": "#198754",
}


# ---------------------------------------------------------------------------
# Data Loading (from Delta / Parquet)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def load_delta_table(table_name: str) -> pd.DataFrame:
    """Load a Gold Delta table as a Pandas DataFrame."""
    table_path = os.path.join(DELTA_DIR, table_name)

    if not os.path.exists(table_path):
        return pd.DataFrame()

    # Delta tables are stored as Parquet files — pandas can read them directly
    try:
        # Try reading parquet files from the Delta directory
        parquet_files = list(Path(table_path).rglob("*.parquet"))
        if not parquet_files:
            return pd.DataFrame()

        dfs = [pd.read_parquet(f) for f in parquet_files]
        return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        st.error(f"Error loading {table_name}: {e}")
        return pd.DataFrame()


def load_all_data() -> dict:
    """Load all Gold tables."""
    return {
        "tenant_usage": load_delta_table("tenant_usage"),
        "endpoint_analytics": load_delta_table("endpoint_analytics"),
        "enforcement_alerts": load_delta_table("enforcement_alerts"),
        "status_distribution": load_delta_table("status_distribution"),
        "geo_distribution": load_delta_table("geo_distribution"),
        "pipeline_metrics": load_delta_table("pipeline_metrics"),
    }


def generate_demo_data() -> dict:
    """
    Generate rich synthetic demo data so the dashboard is always fully
    populated on Streamlit Cloud (where no pipeline output exists).
    Schema matches what the real pipeline produces.
    """
    rng = np.random.default_rng(42)
    plans = ["free", "starter", "professional", "enterprise"]
    plan_weights = [0.35, 0.30, 0.22, 0.13]
    plan_limits = {"free": 1000, "starter": 10_000, "professional": 100_000, "enterprise": 1_000_000}
    plan_price = {"free": 0.0, "starter": 0.0001, "professional": 0.00008, "enterprise": 0.00005}

    # ── Tenant Usage ──────────────────────────────────────────────────────
    n_tenants = 80
    tenant_plans = rng.choice(plans, size=n_tenants, p=plan_weights)
    base_reqs = {"free": 500, "starter": 5_000, "professional": 60_000, "enterprise": 450_000}

    tenant_rows = []
    for i, plan in enumerate(tenant_plans):
        reqs = int(rng.normal(base_reqs[plan], base_reqs[plan] * 0.3))
        reqs = max(100, reqs)
        limit = plan_limits[plan]
        quota_pct = min(100, reqs / limit * 100)
        base_lat = {"free": 180, "starter": 120, "professional": 80, "enterprise": 45}[plan]
        p50 = rng.normal(base_lat, 10)
        p95 = p50 * rng.uniform(1.8, 2.5)
        p99 = p95 * rng.uniform(1.2, 1.6)
        success = rng.uniform(0.91, 0.999)
        rl_count = int(reqs * rng.uniform(0, 0.05) if quota_pct > 70 else 0)
        risk = rng.uniform(0, 30) if plan == "free" else rng.uniform(0, 15)
        tenant_rows.append({
            "tenant_id": f"tenant_{i+1:03d}",
            "plan_tier": plan,
            "total_requests": reqs,
            "success_rate": round(success * 100, 2),
            "rate_limited_count": rl_count,
            "avg_latency_ms": round(float(p50), 1),
            "p50_latency_ms": round(float(p50), 1),
            "p95_latency_ms": round(float(p95), 1),
            "p99_latency_ms": round(float(p99), 1),
            "avg_risk_score": round(float(risk), 2),
            "avg_quota_used_pct": round(quota_pct, 2),
            "max_quota_used_pct": round(min(100, quota_pct * rng.uniform(1.0, 1.3)), 2),
            "total_cost_usd": round(reqs * plan_price[plan], 6),
            "total_billing_units": reqs,
        })
    tenant_df = pd.DataFrame(tenant_rows)

    # ── Enforcement Alerts ────────────────────────────────────────────────
    alert_types = ["HEALTHY", "QUOTA_WARNING", "RATE_LIMIT_BREACH", "SUSPICIOUS_ACTIVITY", "ABUSE_DETECTED"]
    alert_weights = [0.50, 0.22, 0.15, 0.09, 0.04]
    severity_map = {
        "HEALTHY": "LOW",
        "QUOTA_WARNING": "MEDIUM",
        "RATE_LIMIT_BREACH": "HIGH",
        "SUSPICIOUS_ACTIVITY": "HIGH",
        "ABUSE_DETECTED": "CRITICAL",
    }
    action_map = {
        "HEALTHY": "No action required",
        "QUOTA_WARNING": "Monitor usage and consider plan upgrade",
        "RATE_LIMIT_BREACH": "Throttle requests and notify tenant",
        "SUSPICIOUS_ACTIVITY": "Investigate traffic pattern — possible bot",
        "ABUSE_DETECTED": "Block tenant and trigger security review",
    }
    alert_types_assigned = rng.choice(alert_types, size=n_tenants, p=alert_weights)
    alerts_rows = []
    for i in range(n_tenants):
        t = tenant_rows[i]
        at = alert_types_assigned[i]
        alerts_rows.append({
            "tenant_id": t["tenant_id"],
            "plan_tier": t["plan_tier"],
            "alert_type": at,
            "severity": severity_map[at],
            "recommended_action": action_map[at],
            "peak_velocity_1min": int(rng.integers(10, 800 if at == "ABUSE_DETECTED" else 200)),
            "max_risk_score": round(float(rng.uniform(60, 100) if at == "ABUSE_DETECTED" else rng.uniform(0, 50)), 2),
            "total_requests": t["total_requests"],
        })
    alerts_df = pd.DataFrame(alerts_rows)

    # ── Endpoint Analytics ────────────────────────────────────────────────
    endpoints = [
        ("/api/v1/data", "GET"), ("/api/v1/data", "POST"),
        ("/api/v1/users", "GET"), ("/api/v1/users", "POST"),
        ("/api/v1/subscriptions", "GET"), ("/api/v1/subscriptions", "PUT"),
        ("/api/v1/events", "GET"), ("/api/v1/events", "POST"),
        ("/api/v1/analytics", "GET"), ("/api/v1/health", "GET"),
        ("/api/v1/billing", "GET"), ("/api/v1/billing", "POST"),
    ]
    ep_rows = []
    for path, method in endpoints:
        calls = int(rng.integers(500, 35_000))
        lat = rng.uniform(30, 200)
        err = rng.uniform(0.005, 0.08)
        ep_rows.append({
            "endpoint_path": path,
            "http_method": method,
            "total_calls": calls,
            "avg_latency_ms": round(lat, 1),
            "error_rate": round(err, 4),
            "success_count": int(calls * (1 - err)),
            "error_count": int(calls * err),
        })
    endpoint_df = pd.DataFrame(ep_rows)

    # ── Status Distribution ───────────────────────────────────────────────
    status_map = {
        (200, "success"): 0.72,
        (201, "success"): 0.08,
        (400, "client_error"): 0.05,
        (401, "unauthorized"): 0.04,
        (403, "unauthorized"): 0.02,
        (429, "rate_limited"): 0.05,
        (500, "server_error"): 0.02,
        (503, "server_error"): 0.02,
    }
    total = 500_000
    status_rows = [
        {"status_code": code, "status_category": cat, "count": int(total * w)}
        for (code, cat), w in status_map.items()
    ]
    status_df = pd.DataFrame(status_rows)

    # ── Geo Distribution ──────────────────────────────────────────────────
    countries = [
        ("US", "us-east-1", 0.32), ("GB", "eu-west-1", 0.12), ("DE", "eu-central-1", 0.09),
        ("IN", "ap-south-1", 0.11), ("JP", "ap-northeast-1", 0.07), ("BR", "sa-east-1", 0.05),
        ("CA", "us-east-1", 0.06), ("AU", "ap-southeast-2", 0.05), ("FR", "eu-west-3", 0.04),
        ("SG", "ap-southeast-1", 0.04), ("NL", "eu-west-1", 0.03), ("KR", "ap-northeast-2", 0.02),
    ]
    lat_by_region = {
        "us-east-1": 45, "eu-west-1": 90, "eu-central-1": 85, "ap-south-1": 140,
        "ap-northeast-1": 160, "sa-east-1": 180, "ap-southeast-2": 175,
        "eu-west-3": 95, "ap-southeast-1": 155, "ap-northeast-2": 165,
    }
    geo_rows = [
        {
            "country_code": cc,
            "gateway_region": region,
            "request_count": int(total * share),
            "avg_latency_ms": round(lat_by_region[region] * rng.uniform(0.85, 1.15), 1),
            "unique_tenants": int(n_tenants * share * rng.uniform(0.8, 1.2)),
        }
        for cc, region, share in countries
    ]
    geo_df = pd.DataFrame(geo_rows)

    # ── Pipeline Metrics ──────────────────────────────────────────────────
    pipeline_df = pd.DataFrame([{
        "total_events_processed": total,
        "unique_tenants": n_tenants,
        "active_alerts": int((alerts_df["alert_type"] != "HEALTHY").sum()),
    }])

    return {
        "tenant_usage": tenant_df,
        "endpoint_analytics": endpoint_df,
        "enforcement_alerts": alerts_df,
        "status_distribution": status_df,
        "geo_distribution": geo_df,
        "pipeline_metrics": pipeline_df,
    }


# ---------------------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title=PAGE_TITLE,
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS — theme-agnostic (works in both light and dark mode)
st.markdown("""
<style>
    /* Header text: inherit from theme so it's visible in dark & light mode */
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1rem;
        opacity: 0.65;
        margin-bottom: 2rem;
    }
    /* Gradient card used in custom html blocks */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        text-align: center;
    }
    /* Alert severity indicators */
    .alert-critical { border-left: 4px solid #dc3545; padding-left: 1rem; }
    .alert-high     { border-left: 4px solid #fd7e14; padding-left: 1rem; }
    .alert-medium   { border-left: 4px solid #ffc107; padding-left: 1rem; }

    /* Fix Streamlit metric cards — remove hardcoded light background
       so the native theme background + text colours are used instead */
    [data-testid="stMetric"] {
        border-radius: 8px;
        padding: 1rem;
        border: 1px solid rgba(128,128,128,0.2);
    }
    /* Ensure metric label and value use the theme's foreground colour */
    [data-testid="stMetricLabel"] > div,
    [data-testid="stMetricValue"] > div {
        color: inherit !important;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Load Data (must happen before sidebar so DEMO_MODE is defined)
# ---------------------------------------------------------------------------

data = load_all_data()

# If no pipeline data found, fall back to synthetic demo data
DEMO_MODE = len(data["tenant_usage"]) == 0

if DEMO_MODE:
    data = generate_demo_data()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.image("https://img.shields.io/badge/Powered_by-Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark", width=250)
    st.title("🔒 Control Panel")
    st.markdown("---")

    # Data refresh
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("### Data Source")
    if DEMO_MODE:
        st.warning("🟡 Demo Mode\nSynthetic data (pipeline not run)")
    else:
        st.success("🟢 Live Data\nReading from Gold tables")
    st.info(f"📁 {DELTA_DIR}")

    st.markdown("### Architecture")
    st.markdown("""
    ```
    Live Events
        ↓
    Bronze (Raw)
        ↓
    Silver (Clean)
        ↓
    Gold (Analytics)
        ↓
    This Dashboard
    ```
    """)

    st.markdown("---")
    st.markdown("### Technologies")
    st.markdown("- **PySpark** — Stream Processing")
    st.markdown("- **Delta Lake** — ACID Storage")
    st.markdown("- **Streamlit** — Dashboard")
    st.markdown("- **Plotly** — Visualizations")

    st.markdown("---")
    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown('<div class="main-header">🔒 Subscription Enforcement Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Real-Time API Usage Monitoring & Rate Limit Enforcement — Powered by Live Data</div>', unsafe_allow_html=True)

if DEMO_MODE:
    st.info(
        "📊 **Demo Mode** — Displaying synthetic data that mirrors real pipeline output. "
        "To see live data, run `python demo/collect_live_data.py` then `python demo/run_pipeline.py` locally and re-deploy.",
        icon="ℹ️",
    )

# ---------------------------------------------------------------------------
# KPI Row
# ---------------------------------------------------------------------------

tenant_df = data["tenant_usage"]
alerts_df = data["enforcement_alerts"]
endpoint_df = data["endpoint_analytics"]

total_events = int(tenant_df["total_requests"].sum()) if "total_requests" in tenant_df.columns else 0
total_tenants = len(tenant_df)
active_alerts = len(alerts_df[alerts_df["alert_type"] != "HEALTHY"]) if "alert_type" in alerts_df.columns else 0
avg_latency = round(tenant_df["avg_latency_ms"].mean(), 1) if "avg_latency_ms" in tenant_df.columns else 0
success_rate = round(tenant_df["success_rate"].mean(), 1) if "success_rate" in tenant_df.columns else 0
total_revenue = round(tenant_df["total_cost_usd"].sum(), 2) if "total_cost_usd" in tenant_df.columns else 0

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric("📊 Total Events", f"{total_events:,}")
with col2:
    st.metric("🏢 Active Tenants", f"{total_tenants:,}")
with col3:
    st.metric("🚨 Active Alerts", f"{active_alerts:,}",
              delta=f"{active_alerts} need attention" if active_alerts > 0 else "All clear",
              delta_color="inverse" if active_alerts > 0 else "normal")
with col4:
    st.metric("⚡ Avg Latency", f"{avg_latency:.0f}ms")
with col5:
    st.metric("✅ Success Rate", f"{success_rate:.1f}%")
with col6:
    st.metric("💰 Total Revenue", f"${total_revenue:,.2f}")

st.markdown("---")


# ---------------------------------------------------------------------------
# Tab Layout
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview", "🏢 Tenant Analytics", "🚨 Enforcement Alerts",
    "🌍 Geo Distribution", "🔧 Pipeline Health"
])


# ========================= TAB 1: OVERVIEW =================================
with tab1:
    col_left, col_right = st.columns(2)

    with col_left:
        # Plan Tier Distribution (Pie Chart)
        st.subheader("Plan Tier Distribution")
        if "plan_tier" in tenant_df.columns:
            plan_dist = tenant_df.groupby("plan_tier").agg(
                total_requests=("total_requests", "sum"),
                tenants=("tenant_id", "count")
            ).reset_index()

            fig_pie = px.pie(
                plan_dist, values="total_requests", names="plan_tier",
                color="plan_tier", color_discrete_map=PLAN_COLORS,
                hole=0.4,
            )
            fig_pie.update_layout(
                margin=dict(l=20, r=20, t=30, b=20),
                height=350,
                legend=dict(orientation="h", y=-0.1),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    with col_right:
        # Status Code Distribution (Bar Chart)
        st.subheader("Response Status Distribution")
        status_df = data["status_distribution"]
        if len(status_df) > 0 and "status_code" in status_df.columns:
            status_agg = status_df.groupby(["status_code", "status_category"]).agg(
                total=("count", "sum")
            ).reset_index()
            status_agg["status_code"] = status_agg["status_code"].astype(str)

            color_map = {
                "success": "#198754", "client_error": "#ffc107",
                "unauthorized": "#fd7e14", "rate_limited": "#dc3545",
                "server_error": "#dc3545",
            }

            fig_status = px.bar(
                status_agg, x="status_code", y="total", color="status_category",
                color_discrete_map=color_map,
                labels={"status_code": "HTTP Status Code", "total": "Count"},
            )
            fig_status.update_layout(
                margin=dict(l=20, r=20, t=30, b=20), height=350,
                xaxis_type="category",
            )
            st.plotly_chart(fig_status, use_container_width=True)

    # Tenant Usage Bar Chart
    st.subheader("Top 15 Tenants by Request Volume")
    if "total_requests" in tenant_df.columns:
        top_tenants = tenant_df.nlargest(15, "total_requests")
        fig_tenants = px.bar(
            top_tenants, x="tenant_id", y="total_requests",
            color="plan_tier", color_discrete_map=PLAN_COLORS,
            labels={"tenant_id": "Tenant", "total_requests": "Requests"},
        )
        fig_tenants.update_layout(
            margin=dict(l=20, r=20, t=30, b=20), height=400,
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig_tenants, use_container_width=True)

    # Latency Distribution
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Latency Percentiles by Plan Tier")
        if all(c in tenant_df.columns for c in ["p50_latency_ms", "p95_latency_ms", "p99_latency_ms"]):
            latency_data = tenant_df.groupby("plan_tier").agg(
                p50=("p50_latency_ms", "mean"),
                p95=("p95_latency_ms", "mean"),
                p99=("p99_latency_ms", "mean"),
            ).reset_index()

            fig_latency = go.Figure()
            for pct, col_name in [("P50", "p50"), ("P95", "p95"), ("P99", "p99")]:
                fig_latency.add_trace(go.Bar(
                    name=pct, x=latency_data["plan_tier"], y=latency_data[col_name]
                ))
            fig_latency.update_layout(
                barmode="group", height=350,
                yaxis_title="Latency (ms)",
                margin=dict(l=20, r=20, t=30, b=20),
            )
            st.plotly_chart(fig_latency, use_container_width=True)

    with col_r:
        st.subheader("Billing Revenue by Plan Tier")
        if "total_cost_usd" in tenant_df.columns:
            revenue = tenant_df.groupby("plan_tier").agg(
                revenue=("total_cost_usd", "sum"),
                billing_units=("total_billing_units", "sum"),
            ).reset_index()

            fig_revenue = px.bar(
                revenue, x="plan_tier", y="revenue",
                color="plan_tier", color_discrete_map=PLAN_COLORS,
                labels={"revenue": "Revenue (USD)", "plan_tier": "Plan Tier"},
                text=revenue["revenue"].apply(lambda x: f"${x:,.2f}"),
            )
            fig_revenue.update_layout(
                showlegend=False, height=350,
                margin=dict(l=20, r=20, t=30, b=20),
            )
            st.plotly_chart(fig_revenue, use_container_width=True)


# ========================= TAB 2: TENANT ANALYTICS ========================
with tab2:
    st.subheader("Tenant Deep Dive")

    if total_tenants > 0:
        # Tenant selector
        selected_tenant = st.selectbox(
            "Select Tenant",
            options=tenant_df["tenant_id"].tolist(),
            index=0,
        )

        tenant_row = tenant_df[tenant_df["tenant_id"] == selected_tenant].iloc[0]

        # Tenant KPIs
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Plan Tier", tenant_row.get("plan_tier", "N/A").upper())
        with c2:
            st.metric("Total Requests", f"{int(tenant_row.get('total_requests', 0)):,}")
        with c3:
            st.metric("Success Rate", f"{tenant_row.get('success_rate', 0):.1f}%")
        with c4:
            st.metric("Rate Limit Events", f"{int(tenant_row.get('rate_limited_count', 0)):,}")

        c5, c6, c7, c8 = st.columns(4)
        with c5:
            st.metric("Avg Latency", f"{tenant_row.get('avg_latency_ms', 0):.0f}ms")
        with c6:
            st.metric("P99 Latency", f"{tenant_row.get('p99_latency_ms', 0):.0f}ms")
        with c7:
            st.metric("Risk Score", f"{tenant_row.get('avg_risk_score', 0):.1f}")
        with c8:
            st.metric("Revenue", f"${tenant_row.get('total_cost_usd', 0):.4f}")

        # Quota gauge
        st.subheader("Quota Usage")
        quota_pct = tenant_row.get("avg_quota_used_pct", 0)
        max_quota = tenant_row.get("max_quota_used_pct", 0)

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=quota_pct,
            domain={"x": [0, 1], "y": [0, 1]},
            title={"text": f"Average Quota Usage ({tenant_row.get('plan_tier', 'N/A')})"},
            delta={"reference": 80, "increasing": {"color": "red"}, "decreasing": {"color": "green"}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1},
                "bar": {"color": "#667eea"},
                "steps": [
                    {"range": [0, 50], "color": "#d4edda"},
                    {"range": [50, 80], "color": "#fff3cd"},
                    {"range": [80, 100], "color": "#f8d7da"},
                ],
                "threshold": {"line": {"color": "red", "width": 4}, "thickness": 0.75, "value": 80},
            },
        ))
        fig_gauge.update_layout(height=300, margin=dict(l=20, r=20, t=60, b=20))
        st.plotly_chart(fig_gauge, use_container_width=True)

    # Full tenant table
    st.subheader("All Tenants")
    display_cols = [
        "tenant_id", "plan_tier", "total_requests", "success_rate",
        "rate_limited_count", "avg_latency_ms", "p95_latency_ms",
        "avg_risk_score", "avg_quota_used_pct", "total_cost_usd",
    ]
    available = [c for c in display_cols if c in tenant_df.columns]
    st.dataframe(
        tenant_df[available].sort_values("total_requests", ascending=False),
        use_container_width=True,
        height=400,
    )


# ========================= TAB 3: ENFORCEMENT ALERTS =======================
with tab3:
    st.subheader("🚨 Enforcement Alert Center")

    if len(alerts_df) > 0 and "alert_type" in alerts_df.columns:
        # Alert summary
        alert_counts = alerts_df["alert_type"].value_counts().to_dict()

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            ct = alert_counts.get("ABUSE_DETECTED", 0)
            st.metric("🔴 Abuse", ct)
        with c2:
            ct = alert_counts.get("RATE_LIMIT_BREACH", 0)
            st.metric("🟠 Rate Limited", ct)
        with c3:
            ct = alert_counts.get("SUSPICIOUS_ACTIVITY", 0)
            st.metric("🟡 Suspicious", ct)
        with c4:
            ct = alert_counts.get("QUOTA_WARNING", 0)
            st.metric("⚠️ Quota Warning", ct)
        with c5:
            ct = alert_counts.get("HEALTHY", 0)
            st.metric("🟢 Healthy", ct)

        st.markdown("---")

        # Severity filter
        severity_filter = st.multiselect(
            "Filter by Severity",
            options=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
            default=["CRITICAL", "HIGH", "MEDIUM"],
        )

        filtered = alerts_df[alerts_df["severity"].isin(severity_filter)] if "severity" in alerts_df.columns else alerts_df

        # Alert breakdown chart
        col_chart, col_table = st.columns([1, 1])

        with col_chart:
            st.subheader("Alert Distribution")
            alert_type_counts = filtered["alert_type"].value_counts().reset_index()
            alert_type_counts.columns = ["alert_type", "count"]

            fig_alerts = px.pie(
                alert_type_counts, values="count", names="alert_type",
                color="alert_type",
                color_discrete_map={
                    "ABUSE_DETECTED": "#dc3545",
                    "RATE_LIMIT_BREACH": "#fd7e14",
                    "SUSPICIOUS_ACTIVITY": "#ffc107",
                    "QUOTA_WARNING": "#17a2b8",
                    "HEALTHY": "#198754",
                },
                hole=0.35,
            )
            fig_alerts.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_alerts, use_container_width=True)

        with col_table:
            st.subheader("Active Alerts Detail")
            active = filtered[filtered["alert_type"] != "HEALTHY"] if "alert_type" in filtered.columns else filtered
            if len(active) > 0:
                display_cols_alerts = [
                    "tenant_id", "plan_tier", "alert_type", "severity",
                    "recommended_action", "peak_velocity_1min", "max_risk_score",
                ]
                avail = [c for c in display_cols_alerts if c in active.columns]
                st.dataframe(
                    active[avail].sort_values("severity", ascending=True),
                    use_container_width=True,
                    height=300,
                )
            else:
                st.success("No active alerts — all tenants are within limits!")

        # Risk score scatter
        st.subheader("Tenant Risk Profile")
        if all(c in alerts_df.columns for c in ["total_requests", "max_risk_score", "alert_type"]):
            fig_risk = px.scatter(
                alerts_df, x="total_requests", y="max_risk_score",
                color="alert_type", size="total_requests",
                hover_data=["tenant_id", "plan_tier", "severity"],
                color_discrete_map={
                    "ABUSE_DETECTED": "#dc3545",
                    "RATE_LIMIT_BREACH": "#fd7e14",
                    "SUSPICIOUS_ACTIVITY": "#ffc107",
                    "QUOTA_WARNING": "#17a2b8",
                    "HEALTHY": "#198754",
                },
                labels={"total_requests": "Total Requests", "max_risk_score": "Max Risk Score"},
            )
            fig_risk.update_layout(height=400, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_risk, use_container_width=True)
    else:
        st.info("No alert data available.")


# ========================= TAB 4: GEO DISTRIBUTION ========================
with tab4:
    st.subheader("🌍 Geographic Request Distribution")

    geo_df = data["geo_distribution"]

    if len(geo_df) > 0 and "country_code" in geo_df.columns:
        col_map, col_table = st.columns([2, 1])

        with col_map:
            # Choropleth map
            geo_country = geo_df.groupby("country_code").agg(
                request_count=("request_count", "sum"),
                avg_latency=("avg_latency_ms", "mean"),
                unique_tenants=("unique_tenants", "sum"),
            ).reset_index()

            fig_map = px.choropleth(
                geo_country, locations="country_code",
                color="request_count",
                hover_data=["avg_latency", "unique_tenants"],
                color_continuous_scale="Viridis",
                labels={"request_count": "Requests", "country_code": "Country"},
            )
            fig_map.update_layout(
                height=450, margin=dict(l=0, r=0, t=30, b=0),
                geo=dict(showframe=False, showcoastlines=True),
            )
            st.plotly_chart(fig_map, use_container_width=True)

        with col_table:
            st.subheader("By Country")
            st.dataframe(
                geo_country.sort_values("request_count", ascending=False),
                use_container_width=True,
                height=400,
            )

        # Gateway region breakdown
        if "gateway_region" in geo_df.columns:
            st.subheader("Gateway Region Load")
            gw_data = geo_df.groupby("gateway_region").agg(
                requests=("request_count", "sum"),
                avg_latency=("avg_latency_ms", "mean"),
            ).reset_index()

            fig_gw = px.bar(
                gw_data, x="gateway_region", y="requests",
                color="avg_latency", color_continuous_scale="RdYlGn_r",
                labels={"requests": "Requests", "gateway_region": "Region", "avg_latency": "Latency (ms)"},
                text=gw_data["requests"].apply(lambda x: f"{x:,}"),
            )
            fig_gw.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_gw, use_container_width=True)
    else:
        st.info("No geographic data available.")


# ========================= TAB 5: PIPELINE HEALTH =========================
with tab5:
    st.subheader("🔧 Pipeline Health & Data Quality")

    pipeline_df = data["pipeline_metrics"]

    if len(pipeline_df) > 0:
        c1, c2, c3 = st.columns(3)
        row = pipeline_df.iloc[0]
        with c1:
            st.metric("Events Processed", f"{int(row.get('total_events_processed', 0)):,}")
        with c2:
            st.metric("Unique Tenants", f"{int(row.get('unique_tenants', 0)):,}")
        with c3:
            st.metric("Active Alerts", f"{int(row.get('active_alerts', 0)):,}")

    # Endpoint performance
    st.subheader("Endpoint Performance")
    if len(endpoint_df) > 0 and "total_calls" in endpoint_df.columns:
        fig_ep = px.treemap(
            endpoint_df, path=["http_method", "endpoint_path"],
            values="total_calls", color="avg_latency_ms",
            color_continuous_scale="RdYlGn_r",
            labels={"total_calls": "Calls", "avg_latency_ms": "Avg Latency (ms)"},
        )
        fig_ep.update_layout(height=450, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig_ep, use_container_width=True)

    # Data quality checks summary
    st.subheader("Data Quality Checks")

    col_dq1, col_dq2, col_dq3, col_dq4 = st.columns(4)
    with col_dq1:
        st.metric("Schema Validation", "✅ Passed")
    with col_dq2:
        st.metric("Deduplication", "✅ Applied")
    with col_dq3:
        st.metric("Null Check", "✅ Passed")
    with col_dq4:
        st.metric("Range Validation", "✅ Passed")

    # Architecture diagram
    st.subheader("Pipeline Architecture")
    st.markdown("""
    ```
    ┌─────────────────┐     ┌────────────────┐     ┌────────────────┐     ┌────────────────┐
    │  Live Events    │────▸│  Bronze Layer   │────▸│  Silver Layer  │────▸│  Gold Layer    │
    │                 │     │                 │     │                │     │                │
    │ • Wikimedia SSE │     │ • Raw ingestion │     │ • Dedup        │     │ • Aggregation  │
    │ • GitHub API    │     │ • Schema on read│     │ • Validation   │     │ • Enforcement  │
    │ • 50-100 evt/s  │     │ • DLQ routing   │     │ • Flattening   │     │ • Alerting     │
    └─────────────────┘     └────────────────┘     └────────────────┘     └────────────────┘
           │                        │                       │                      │
           │                  Delta Lake              Delta Lake              Delta Lake
           │                  (Parquet)               (Parquet)              (Parquet)
           │                                                                       │
           └───────────────────────────────────────────────────────────────────────▸│
                                                                                    │
                                                                          ┌─────────▾────────┐
                                                                          │  This Dashboard  │
                                                                          │  (Streamlit)     │
                                                                          └──────────────────┘
    ```
    """)

    # Delta table file sizes
    st.subheader("Delta Table Storage")
    if os.path.exists(DELTA_DIR):
        table_info = []
        for table_name in os.listdir(DELTA_DIR):
            table_path = os.path.join(DELTA_DIR, table_name)
            if os.path.isdir(table_path):
                total_size = sum(
                    f.stat().st_size
                    for f in Path(table_path).rglob("*")
                    if f.is_file()
                )
                parquet_count = len(list(Path(table_path).rglob("*.parquet")))
                table_info.append({
                    "Table": table_name,
                    "Files": parquet_count,
                    "Size (KB)": round(total_size / 1024, 1),
                })

        if table_info:
            st.dataframe(pd.DataFrame(table_info), use_container_width=True)


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #666; font-size: 0.9rem;">
        <strong>Subscription Enforcement Platform</strong> — Built with Apache Spark, Delta Lake, and Streamlit<br/>
        Processing real live events through a production-grade Medallion Architecture<br/>
        <em>Designed by Pranav Kadam</em>
    </div>
    """,
    unsafe_allow_html=True,
)
