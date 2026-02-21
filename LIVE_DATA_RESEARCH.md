# Live Data Sources, Demo Patterns & Dashboard Research

> Research conducted 2026-02-22. Focus: free real-time data sources, zero-Docker/Kafka demo patterns, and dashboard approaches for the Subscription Tracker streaming pipeline.

---

## 1. FREE Real-Time Data Sources (Verified Working)

### Tier 1: High-Volume True Streaming (SSE / Continuous)

#### Wikimedia EventStreams (BEST FIT)
- **URL**: `https://stream.wikimedia.org/v2/stream/recentchange`
- **Protocol**: Server-Sent Events (SSE) — native HTTP streaming
- **Volume**: ~50-100 events/second across all Wikimedia wikis (Wikipedia, Wikidata, Commons)
- **Auth**: None required, completely free, no API key
- **Rate Limit**: None for SSE stream consumption
- **Why it's perfect**: The event schema maps remarkably well to API usage tracking:
  - Each event has: `user`, `timestamp`, `server_name` (≈ API endpoint), `type` (edit/log/categorize ≈ API method), `namespace`, `title`, `length.old/new` (≈ request/response size)
  - Natural "subscription tier" mapping: bot users vs. human users, different wikis as different "tenants"
  - Volume is genuinely real-time with sustained throughput — perfect for demonstrating rate limiting

**Python consumption pattern:**
```python
import json
import requests

url = "https://stream.wikimedia.org/v2/stream/recentchange"
response = requests.get(url, stream=True)

for line in response.iter_lines():
    if line and line.startswith(b'data:'):
        event = json.loads(line[5:])
        # Fields: type, namespace, user, bot, timestamp,
        #         server_name, wiki, title, length, revision
        print(f"{event['user']} -> {event['server_name']} ({event['type']})")
```

**Available streams** (all at `https://stream.wikimedia.org/v2/stream/`):
| Stream | Events/sec | Best For |
|--------|-----------|----------|
| `recentchange` | ~50-100 | API usage simulation (edits, logs, categorize) |
| `revision-create` | ~10-20 | Content creation events |
| `page-create` | ~2-5 | New resource creation |
| `page-delete` | ~1-2 | Resource deletion |
| `page-move` | ~0.5-1 | Resource mutation |

---

### Tier 2: High-Volume Polling APIs (REST, Sub-Second Updates)

#### GitHub Events API
- **URL**: `https://api.github.com/events`
- **Protocol**: REST JSON (poll every 60s, rate limited)
- **Volume**: 30 events per page, ~300 events/5 min (public timeline)
- **Auth**: Optional (unauthenticated: 60 req/hr, authenticated: 5000 req/hr)
- **Rate Limit**: Strict — monitor `X-RateLimit-Remaining` header
- **Schema**: Each event has `id`, `type` (PushEvent, CreateEvent, DeleteEvent, etc.), `actor`, `repo`, `payload`, `created_at`
- **Demo value**: Perfect for demonstrating rate limiting enforcement — the API itself rate-limits you, so you can show subscription tiers (free=60/hr, pro=5000/hr)

**Python consumption pattern:**
```python
import requests, time

while True:
    resp = requests.get("https://api.github.com/events",
                       headers={"Accept": "application/json"})
    events = resp.json()
    remaining = int(resp.headers.get("X-RateLimit-Remaining", 0))
    print(f"Got {len(events)} events, {remaining} requests remaining")
    
    # Respect polling interval from headers
    poll_interval = int(resp.headers.get("X-Poll-Interval", 60))
    time.sleep(poll_interval)
```

#### CoinGecko Crypto API
- **URL**: `https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd`
- **Protocol**: REST JSON
- **Auth**: None for free tier
- **Rate Limit**: 10-30 calls/minute (free)
- **Response**: `{"bitcoin":{"usd":68578},"ethereum":{"usd":1989.97}}`
- **Demo value**: Good for showing price-tick style data; each poll = 1 "API call" for usage tracking

#### Open Exchange Rates
- **URL**: `https://open.er-api.com/v6/latest/USD`
- **Protocol**: REST JSON
- **Auth**: None for free endpoint
- **Rate Limit**: ~1500 requests/month
- **Response**: Full currency exchange rates (161 currencies)
- **Demo value**: Financial data enrichment; can simulate "premium" vs "free" tier access

---

### Tier 3: WebSocket Real-Time (Continuous Bidirectional)

#### Binance WebSocket (Crypto Tickers)
- **URL**: `wss://stream.binance.com:9443/ws/btcusdt@trade`
- **Protocol**: WebSocket
- **Volume**: 10-50 trades/second for BTC/USDT alone
- **Auth**: None for public streams
- **Demo value**: Extreme volume for stress testing; each trade is a measurable API event

```python
import websocket, json

def on_message(ws, message):
    trade = json.loads(message)
    # Fields: symbol, price, quantity, timestamp, buyer/seller
    print(f"Trade: {trade['s']} @ {trade['p']}")

ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/btcusdt@trade",
                            on_message=on_message)
ws.run_forever()
```

#### CoinCap WebSocket
- **URL**: `wss://ws.coincap.io/prices?assets=bitcoin,ethereum,dogecoin`
- **Protocol**: WebSocket
- **Volume**: ~1-5 updates/second
- **Auth**: None

---

### Summary: Recommended Data Source Stack (Zero Cost)

| Source | Volume | Protocol | Best Use in Demo |
|--------|--------|----------|-----------------|
| **Wikimedia EventStreams** | ~50-100/s | SSE | Primary ingest source (replaces Kafka) |
| **GitHub Events API** | ~6/min | REST | Rate limiting demo (shows enforcement) |
| **CoinGecko API** | ~30/min | REST | Price enrichment / tier gating |
| **Binance WebSocket** | ~10-50/s | WS | High-volume stress testing |

---

## 2. Demo Patterns WITHOUT Docker/Kafka

### Pattern A: SSE-to-File Streaming (Replaces Kafka → Spark)

The most professional pattern found in production-quality repos:

```
┌─────────────────┐     ┌──────────────┐     ┌───────────────┐     ┌───────────────┐
│  Wikimedia SSE  │────▶│  Python SSE  │────▶│  JSON Files   │────▶│  PySpark      │
│  (Real Events)  │     │  Consumer    │     │  (Landing)    │     │  fileStream() │
└─────────────────┘     └──────────────┘     └───────────────┘     └───────┬───────┘
                                                                          │
                                                                   ┌──────▼──────┐
                                                                   │ Delta Lake  │
                                                                   │ Bronze/     │
                                                                   │ Silver/Gold │
                                                                   └──────┬──────┘
                                                                          │
                                                                   ┌──────▼──────┐
                                                                   │  Streamlit  │
                                                                   │  Dashboard  │
                                                                   └─────────────┘
```

**Key insight**: Spark's `readStream.format("json").schema(schema).load("landing/")` watches a directory for new files — this is the **standard local replacement for Kafka** in every major demo repo.

**How it works:**
1. A Python process consumes SSE and writes JSON files every N seconds to a `landing/` directory
2. PySpark `fileStream` picks up new files automatically (exactly like a Kafka micro-batch)
3. Bronze/Silver/Gold logic runs identically to the Kafka version
4. Checkpoint recovery works the same way

```python
# Producer: SSE → JSON files (replaces Kafka producer)
import json, time, os, requests

os.makedirs("data/landing", exist_ok=True)
url = "https://stream.wikimedia.org/v2/stream/recentchange"
response = requests.get(url, stream=True)

batch, batch_num = [], 0
for line in response.iter_lines():
    if line and line.startswith(b'data:'):
        event = json.loads(line[5:])
        batch.append(event)
        if len(batch) >= 100:  # Flush every 100 events
            path = f"data/landing/batch_{batch_num:06d}.json"
            with open(path, 'w') as f:
                for e in batch:
                    f.write(json.dumps(e) + '\n')
            batch, batch_num = [], batch_num + 1

# Consumer: PySpark fileStream (replaces Kafka source)
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

stream = (spark.readStream
    .format("json")
    .schema(event_schema)
    .load("data/landing/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "data/checkpoints/bronze")
    .outputMode("append")
    .start("data/warehouse/bronze/events"))
```

### Pattern B: Batch-Mode Demo (Same Logic, No Streaming)

Found in **CashBarnes/finance_qc** — the best reference repo discovered:

1. Generate synthetic data with Python → write to Delta Lake directly
2. Run the same SQL/PySpark transformation logic in batch
3. Display results in Streamlit

This is **the dominant pattern** on GitHub for local demos. The key insight: your Bronze→Silver→Gold SQL and PySpark transformations are **identical** whether the source is streaming or batch. The demo proves the logic works.

```python
# Batch mode: same transformations, no streaming overhead
bronze_df = spark.read.format("delta").load("warehouse/bronze")
silver_df = (bronze_df
    .filter(col("_is_valid") == True)
    .withColumn("amount_usd", col("amount") * col("fx_rate"))
    .withColumn("processed_at", current_timestamp()))
silver_df.write.format("delta").mode("overwrite").save("warehouse/silver")
```

### Pattern C: Rate-Simulated Real API Polling

Combine real API calls with rate tracking:

```python
import time, json
from collections import defaultdict
from datetime import datetime

# Simulate subscription tiers
TIERS = {
    "free": {"rpm": 10, "daily": 100},
    "pro": {"rpm": 60, "daily": 5000},
    "enterprise": {"rpm": 600, "daily": 50000},
}

usage = defaultdict(lambda: {"minute": 0, "daily": 0, "last_reset": time.time()})

def check_rate_limit(user_id, tier):
    limits = TIERS[tier]
    u = usage[user_id]
    now = time.time()
    if now - u["last_reset"] > 60:
        u["minute"] = 0
        u["last_reset"] = now
    u["minute"] += 1
    u["daily"] += 1
    return {
        "allowed": u["minute"] <= limits["rpm"] and u["daily"] <= limits["daily"],
        "minute_used": u["minute"],
        "minute_limit": limits["rpm"],
        "daily_used": u["daily"],
        "daily_limit": limits["daily"],
    }
```

---

## 3. Dashboard Approaches (Proven Working Locally)

### Approach 1: Streamlit + Delta Lake (RECOMMENDED)

**Found in**: CashBarnes/finance_qc, VijayAdithyaBK/spark-etl-pipeline, Mohith-akash/olist-analytics-platform

This is the **dominant pattern** in every repo we found. The architecture:

```python
# dashboard.py — Streamlit reading Delta Lake
import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px

st.set_page_config(page_title="API Usage Dashboard", layout="wide")

@st.cache_resource
def get_spark():
    return (SparkSession.builder
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate())

spark = get_spark()

# Read gold layer
daily_kpis = spark.read.format("delta").load("warehouse/gold/daily_kpis").toPandas()
rate_violations = spark.read.format("delta").load("warehouse/gold/rate_violations").toPandas()

# Layout
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total API Calls", f"{daily_kpis['total_calls'].sum():,.0f}")
col2.metric("Unique Users", f"{daily_kpis['unique_users'].iloc[-1]:,.0f}")
col3.metric("Rate Violations", f"{rate_violations.shape[0]:,}")
col4.metric("Avg Latency", f"{daily_kpis['avg_latency_ms'].mean():.0f}ms")

# Charts
fig = px.line(daily_kpis, x="date", y="total_calls", title="API Calls Over Time")
st.plotly_chart(fig, use_container_width=True)

fig2 = px.bar(rate_violations.groupby("tier").size().reset_index(name="count"),
              x="tier", y="count", title="Rate Limit Violations by Tier")
st.plotly_chart(fig2, use_container_width=True)
```

**Launch**: `streamlit run dashboard.py`

### Approach 2: Streamlit + Pandas (Lighter Weight, No Spark in Dashboard)

For dashboards that don't need Spark at query time — export Gold tables to Parquet/CSV:

```python
# In pipeline: export gold tables
gold_df.toPandas().to_parquet("dashboard_data/daily_kpis.parquet")

# In dashboard: read with pandas only
import streamlit as st
import pandas as pd

df = pd.read_parquet("dashboard_data/daily_kpis.parquet")
```

### Approach 3: Auto-Refreshing Dashboard (Near Real-Time)

```python
import streamlit as st
import time

# Auto-refresh every 10 seconds
placeholder = st.empty()

while True:
    with placeholder.container():
        df = pd.read_parquet("warehouse/gold/latest_metrics.parquet")
        st.metric("Events/sec", f"{df['events_per_sec'].iloc[-1]:.1f}")
        st.line_chart(df.set_index("timestamp")["events_per_sec"])
    time.sleep(10)
    st.rerun()
```

### Approach 4: Jupyter Notebook Visualization

Found in several repos as an alternative to Streamlit:

```python
# In a notebook cell
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()
gold = spark.read.format("delta").load("warehouse/gold/daily_kpis").toPandas()

fig, axes = plt.subplots(2, 2, figsize=(14, 8))
gold.plot(x="date", y="total_calls", ax=axes[0,0], title="API Calls")
gold.plot(x="date", y="error_rate", ax=axes[0,1], title="Error Rate %")
gold.plot(x="date", y="p99_latency", ax=axes[1,0], title="P99 Latency")
gold.plot(x="date", y="rate_violations", ax=axes[1,1], title="Rate Violations")
plt.tight_layout()
plt.show()
```

---

## 4. Reference Repos (Best Matches Found)

### [CashBarnes/finance_qc](https://github.com/CashBarnes/finance_qc) ⭐ BEST MATCH
- **What**: Full Medallion Architecture (Bronze→Silver→Gold), Delta Lake, Streamlit dashboard
- **Runs locally**: Yes — pure PySpark + delta-spark, zero Docker
- **Steps**: Generate data → DLT pipeline → Reconciliation SQL → Gold aggregates → Anomaly detection → Streamlit dashboard
- **Key patterns to adopt**:
  - `run_sql_file.py` helper to run SQL files through Spark
  - `reset_local_metastore.py` for clean state management
  - `LOCATION`-based Delta table registration with `${hivevar:project_root}`
  - Streamlit reads Delta directly via a cached Spark session

### [VijayAdithyaBK/spark-etl-pipeline](https://github.com/VijayAdithyaBK/spark-etl-pipeline)
- **What**: 500K+ financial transactions, feature engineering, ML fraud detection, Streamlit dashboard
- **Runs locally**: Yes — Python 3.11, PySpark, `uv` package manager
- **Key patterns**: `Makefile` for running steps, `dashboard/app.py` for Streamlit, test suite with `pytest`

### [Sharmaaditya22/Realtime-Fraud-Detection-Pipeline](https://github.com/Sharmaaditya22/Realtime-Fraud-Detection-Pipeline)
- **What**: Kafka → Spark Structured Streaming → Delta Lake → Streamlit
- **Note**: Uses Kafka/Docker but the Delta + Streamlit portion is reusable locally

### [jmuncor/tokentap](https://github.com/jmuncor/tokentap) (737 stars)
- **What**: Intercept LLM API traffic, visualize token usage in real-time terminal dashboard
- **Pattern**: Proxy-based API usage tracking — interesting architecture for demonstrating API metering

---

## 5. Recommended Implementation Plan for This Project

### Phase 1: Live Data Ingestion (Replace Kafka with SSE)

Create a `live_ingest.py` that:
1. Connects to Wikimedia EventStreams SSE
2. Transforms wiki events → API usage event schema (mapping `user` → `user_id`, `server_name` → `endpoint`, `type` → `http_method`, etc.)
3. Writes JSON-line files to `data/landing/` every 5 seconds
4. PySpark `fileStream` picks them up → Bronze Delta Lake

### Phase 2: Rate Limiting Demonstration

Create `rate_limiter_demo.py` that:
1. Reads the Silver layer
2. Assigns synthetic subscription tiers to users (bot=enterprise, registered=pro, anonymous=free)
3. Applies sliding window rate checks using Spark window functions
4. Writes violations to Gold layer `rate_violations` table

### Phase 3: Streamlit Dashboard

Create `dashboard.py` with:
- **Overview cards**: Total events, unique users, rate violations, avg latency
- **Time series**: Events/minute over time, by tier
- **Rate limiting view**: Violations by tier, top offenders
- **Data quality**: Schema validation pass rate, null rates
- Auto-refresh capability reading Delta Lake gold tables

### Minimum Dependencies (Zero Docker, Zero Kafka)

```
pyspark==3.5.0
delta-spark==3.1.0
streamlit==1.31.0
plotly==5.18.0
requests==2.31.0       # For SSE consumption
sseclient-py==1.8.0    # Optional: cleaner SSE parsing
pandas==2.1.4
```

---

## 6. Key API URLs Reference

| API | URL | Free Tier |
|-----|-----|-----------|
| Wikimedia SSE | `https://stream.wikimedia.org/v2/stream/recentchange` | Unlimited |
| Wikimedia (all streams) | `https://stream.wikimedia.org/?doc` | Unlimited |
| GitHub Events | `https://api.github.com/events` | 60 req/hr (unauth) |
| GitHub Repo Events | `https://api.github.com/repos/{owner}/{repo}/events` | 60 req/hr |
| CoinGecko Ping | `https://api.coingecko.com/api/v3/ping` | 30 req/min |
| CoinGecko Prices | `https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd` | 30 req/min |
| Exchange Rates | `https://open.er-api.com/v6/latest/USD` | ~1500/month |
| Binance WS | `wss://stream.binance.com:9443/ws/btcusdt@trade` | Unlimited |
| CoinCap WS | `wss://ws.coincap.io/prices?assets=bitcoin` | Unlimited |

---

## 7. How Wikimedia Events Map to API Usage Events

This mapping makes the demo authentic — it's not "fake" data, it's real events reinterpreted:

| Wikimedia Field | → | API Usage Field | Mapping Logic |
|----------------|---|----------------|---------------|
| `user` | → | `user_id` | Hash to UUID |
| `bot` (true/false) | → | `subscription_tier` | bot=enterprise, registered=pro, anon=free |
| `server_name` | → | `api_endpoint` | e.g., `en.wikipedia.org` → `/api/v1/content` |
| `type` (edit/log/categorize) | → | `http_method` | edit=POST, log=GET, categorize=PUT |
| `timestamp` | → | `event_timestamp` | Direct |
| `length.new - length.old` | → | `response_bytes` | Absolute delta |
| `namespace` | → | `resource_type` | 0=article, 14=category, 2=user |
| `wiki` | → | `tenant_id` | Multi-tenant simulation |
| `revision.new` | → | `request_id` | Unique identifier |

This gives you **50-100 events/second of real, timestamped, user-attributed API-like traffic** with zero simulation code needed.
