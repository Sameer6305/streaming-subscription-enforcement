# 5-Minute System Design Interview: Real-Time Subscription Enforcement

## Interview Structure

**Time Budget:**
- Problem & Requirements: 60 seconds
- Why Real-Time: 45 seconds
- Architecture Overview: 120 seconds
- Failure Handling: 60 seconds
- Trade-offs: 45 seconds

---

## 1. Problem Statement (60 seconds)

**"I'm designing a real-time subscription enforcement system for a SaaS API platform."**

### Business Context
- **B2B SaaS API** serving enterprise customers
- **Usage-based pricing**: Free (60 req/min), Starter (300), Pro (1000), Enterprise (5000)
- **Requirements:**
  - Real-time rate limiting (block over-limit requests)
  - Usage tracking for billing
  - Abuse detection (velocity spikes, unusual patterns)
  - Multi-tenant isolation

### Scale
- **100M+ API requests/day** across 10,000+ tenants
- **Sub-second enforcement latency** (rate limit decisions)
- **High throughput**: 1,000-5,000 events/second peak
- **30-day data retention** for audit/billing

### Key Challenge
> "How do we enforce subscription limits in real-time while maintaining accurate usage tracking for billing, without blocking legitimate traffic?"

---

## 2. Why Real-Time? (45 seconds)

**"Why can't we use batch processing every 5 minutes?"**

### Business Impact of Latency

| Approach | Latency | Problem |
|----------|---------|---------|
| **Batch (5 min)** | 5 minutes | Customer exceeds Free plan limit (60 req/min). System doesn't block for 5 minutes. Customer makes 10,000 requests. Company loses money, customer gets surprise bill. |
| **Real-Time (<1 sec)** | <1 second | Customer hits limit at 60 requests. 61st request blocked immediately. Customer upgrades plan or waits. No revenue leakage. |

### Why It Matters
1. **Revenue Protection**: Block over-limit usage immediately (no free tier abuse)
2. **Customer Experience**: Instant feedback (upgrade prompt vs surprise bill)
3. **Resource Protection**: Prevent API server overload from runaway clients
4. **Compliance**: Real-time audit logs for SOC2/GDPR

**Decision: Event-driven streaming architecture with 1-minute tumbling windows.**

---

## 3. Architecture Overview (120 seconds)

### High-Level Design

```
┌──────────────┐
│ API Gateway  │ (Receives 1000s req/sec)
└──────┬───────┘
       │ Emit event per request
       ↓
┌──────────────┐
│ Kafka        │ (Event bus, 64 partitions by tenant_id)
└──────┬───────┘
       │
       ↓
┌──────────────────────────────────────────────────────┐
│ Spark Structured Streaming (Medallion Architecture) │
└──────────────────────────────────────────────────────┘
       │
       ├──→ Bronze (Raw events, immutable audit log)
       │
       ├──→ Silver (Deduplicated, validated events)
       │
       └──→ Gold (1-min aggregations by tenant)
                 ↓
           ┌─────────────┐
           │ Redis Cache │ (tenant_id → usage counts)
           └─────────────┘
                 ↓
           ┌─────────────┐
           │ API Gateway │ (Rate limit check: current_usage < plan_limit?)
           └─────────────┘
```

### Key Components

#### 1. Event Schema (JSON)
```json
{
  "event_id": "uuid",
  "tenant_id": "tenant-123",
  "event_timestamp": "2026-01-28T10:30:15Z",
  "endpoint": "/api/v1/search",
  "http_method": "POST",
  "response_status_code": 200,
  "subscription_plan": "free",
  "api_key_id": "key-xyz",
  "risk_score": 0.05
}
```

#### 2. Kafka Topic Configuration
- **Topic**: `api-usage-events.v1`
- **Partitions**: 64 (partition key = `tenant_id` for locality)
- **Retention**: 7 days (replay buffer)
- **Throughput**: 5,000 events/sec peak

#### 3. Spark Streaming Pipeline (Medallion)

**Bronze Layer** (Raw Ingestion)
- **Purpose**: Immutable source of truth
- **Processing**: Read from Kafka, preserve raw JSON, append to Delta Lake
- **Why**: Audit compliance, reprocessing capability

**Silver Layer** (Cleansing)
- **Purpose**: Clean, deduplicated business events
- **Processing**: 
  - Deduplication by `event_id` (1-hour watermark handles late arrivals)
  - Schema validation (required fields, value ranges)
  - Quarantine invalid records
- **Why**: Data quality, idempotency

**Gold Layer** (Aggregation)
- **Purpose**: Usage metrics for enforcement
- **Processing**:
  - Tumbling window: 1 minute (groupBy tenant_id, window)
  - Aggregate: `COUNT(*) AS total_requests`
  - Compare: `total_requests > plan_limit`
  - Generate alerts if over-limit
- **Output**: Write to Redis (tenant_id → {requests: 150, limit: 300})

#### 4. Redis Cache (Enforcement Layer)
- **Key-Value Store**: `tenant:tenant-123:usage:1min:2026-01-28T10:30:00` → `{count: 150}`
- **TTL**: 2 minutes (1-min window + buffer)
- **API Gateway Reads Redis**: 
  ```python
  current_usage = redis.get(f"tenant:{tenant_id}:usage:1min:{current_window}")
  if current_usage >= plan_limits[tenant_id]:
      return 429  # Rate Limited
  ```

### Data Flow Example

```
10:30:15 - API request arrives at Gateway
10:30:15 - Gateway emits event to Kafka
10:30:16 - Spark Bronze ingests event (1-sec lag)
10:30:17 - Spark Silver deduplicates, validates
10:30:18 - Spark Gold aggregates 1-min window (10:30:00-10:31:00)
10:30:19 - Write to Redis: tenant-123 → 58 requests in current window
10:30:20 - Gateway checks Redis for next request → 59 requests → Allow
10:31:05 - Gateway checks Redis → 61 requests → Block (429 Too Many Requests)
```

**End-to-End Latency: ~5 seconds** (event to enforcement decision)

---

## 4. Failure Handling (60 seconds)

**"How does the system recover from failures without data loss?"**

### Failure Scenarios

| Failure | Impact | Recovery Mechanism | Data Loss? |
|---------|--------|-------------------|------------|
| **Spark Crash** | Pipeline stops processing | Checkpoints store Kafka offsets. Restart from last offset. | ❌ No |
| **Kafka Broker Restart** | Events buffered in producer memory | Producer retries, Kafka replication (3 replicas) | ❌ No |
| **Duplicate Events** | Over-counting usage | Silver layer deduplicates by `event_id` (1-hour watermark) | ❌ No |
| **Late-Arriving Events** | Event arrives 10 mins late | Watermark allows 1-hour delay. Late events flagged but processed. | ❌ No |
| **Redis Failure** | Enforcement disabled | Gateway falls back to "allow all" (fail open) until Redis recovers. Batch reconciliation corrects billing. | ⚠️ Temporary over-usage |

### Key Design Decisions

1. **Checkpointing**: Spark writes Kafka offsets to HDFS/S3. Restart resumes from checkpoint.
2. **Delta Lake ACID**: All writes are atomic. No partial data corruption.
3. **Bronze Immutability**: Never delete Bronze data. It's the source of truth for reprocessing.
4. **Idempotent MERGE**: Silver uses `MERGE` with `event_id` deduplication. Same event replayed → no duplicates.
5. **Fail Open**: Redis failure → allow all requests temporarily (customer experience > strict enforcement).

### Recovery Playbook Example

**Scenario: Spark crashes during processing**

```bash
# Spark restarts automatically (Kubernetes/YARN)
# Reads checkpoint: last processed offset = 12500000
# Resumes from offset 12500001

# Verify no data loss
spark-sql -e "
SELECT 
    kafka_partition,
    MIN(kafka_offset) AS min_offset,
    MAX(kafka_offset) AS max_offset
FROM bronze_api_usage
WHERE ingestion_date = CURRENT_DATE
GROUP BY kafka_partition
ORDER BY kafka_partition;
"
# Output: No gaps in offsets → No data loss
```

---

## 5. Trade-offs (45 seconds)

**"What are the trade-offs in this design?"**

### 1. Real-Time vs. Accuracy

| Approach | Latency | Accuracy | Cost |
|----------|---------|----------|------|
| **Batch (Hourly)** | 1 hour | 100% accurate (full data) | Low (simple ETL) |
| **Streaming (1-min windows)** | 5 seconds | 99.9% (watermark handles late events) | High (Kafka + Spark infra) |

**Decision**: **Streaming** for business requirements (revenue protection), accept 0.1% late events (flagged for reconciliation).

---

### 2. Exactly-Once vs. At-Least-Once

| Guarantee | Duplicates? | Performance | Complexity |
|-----------|-------------|-------------|------------|
| **Exactly-Once** | No duplicates | Slower (2-phase commit) | High (Kafka transactions) |
| **At-Least-Once + Deduplication** | Duplicates in Bronze, removed in Silver | Faster | Medium (idempotent MERGE) |

**Decision**: **At-Least-Once** with Silver deduplication. Bronze keeps duplicates for audit, Silver is clean for billing.

---

### 3. Fail Open vs. Fail Closed

| Strategy | Redis Failure Behavior | Risk |
|----------|------------------------|------|
| **Fail Closed** | Block all requests | Outage → Revenue loss (customers can't use API) |
| **Fail Open** | Allow all requests | Temporary over-usage → Reconcile later in batch |

**Decision**: **Fail Open**. Customer experience > strict enforcement. Batch job reconciles billing.

---

### 4. Watermark Duration

| Watermark | Late Events Handled | Window Emission Delay | Memory Usage |
|-----------|---------------------|----------------------|--------------|
| **10 minutes** | Events up to 10 min late | 10 minutes | Low |
| **1 hour** | Events up to 1 hour late | 1 hour | High (state kept longer) |

**Decision**: **1 hour watermark**. Handles network delays, timezone issues. Late events flagged (not dropped).

---

### 5. Bronze Layer: Store or Skip?

| Approach | Storage Cost | Reprocessing? | Audit Compliance? |
|----------|--------------|---------------|-------------------|
| **Skip Bronze** | Low (only Silver/Gold) | ❌ No (Kafka 7-day retention) | ❌ No (data lost after 7 days) |
| **Store Bronze** | High (full JSON, 2 years) | ✅ Yes (forever) | ✅ Yes (SOC2/GDPR) |

**Decision**: **Store Bronze**. Compliance requirement (2-year retention). Cost: ~$500/TB/year S3.

---

## Interview Tips

### Opening Statement (15 seconds)
> "I'm designing a real-time subscription enforcement platform for a SaaS API. The system processes 100M+ API requests daily, enforces usage limits within seconds, and maintains accurate billing data. The architecture uses Kafka for event streaming, Spark for processing in a medallion pattern, and Redis for sub-second enforcement decisions."

### Key Talking Points to Emphasize

1. **Business Value First**: "Real-time enforcement prevents revenue leakage from free tier abuse."
2. **Scalability**: "Kafka partitioning by tenant_id ensures parallel processing. 64 partitions handle 5K events/sec."
3. **Reliability**: "Bronze layer immutability enables reprocessing. Checkpointing prevents data loss."
4. **Trade-offs**: "I chose fail-open for Redis failures to prioritize customer experience over strict enforcement."

### Handling Follow-Up Questions

**Q: "Why Spark instead of Flink/Storm?"**
> "Spark Structured Streaming provides exactly-once semantics with checkpointing, Delta Lake integration for ACID transactions, and mature ecosystem. Flink is faster but higher operational complexity for our team."

**Q: "Why 1-minute windows instead of real-time per-request?"**
> "Per-request would require distributed counters (race conditions, high Redis load). 1-minute windows batch updates, reducing Redis writes from 1000s/sec to 1/min per tenant while meeting business SLA."

**Q: "How do you handle tenant data isolation?"**
> "Kafka partitioning by tenant_id ensures tenant data stays together. Delta Lake supports row-level security with tenant_id filtering. Redis keys namespaced by tenant."

**Q: "What's the cost?"**
> "~$10K/month: Kafka cluster ($3K), Spark cluster ($5K), Redis ($1K), S3 storage ($1K). Scales linearly with event volume."

**Q: "How do you test this system?"**
> "Unit tests for each layer, integration tests with embedded Kafka, chaos engineering (kill Spark executors), load tests (10K events/sec), data quality tests (deduplication, late events)."

---

## Visual Aids for Whiteboard

### Draw This First (30 seconds)

```
[API Gateway] → [Kafka] → [Spark Streaming] → [Delta Lake]
                                    ↓
                              [Redis Cache]
                                    ↑
                              [API Gateway]
                              (Rate Limit Check)
```

### Add Details as You Explain

```
Medallion Architecture:

Bronze (Raw)     Silver (Clean)     Gold (Aggregated)
    ↓                  ↓                    ↓
[Immutable]      [Deduplicated]      [1-min windows]
[Audit Log]      [Validated]         [Tenant usage]
                 [Quarantine]        [Enforcement]
```

---

## Closing Statement (15 seconds)

> "This architecture balances real-time enforcement requirements with data accuracy and cost efficiency. The medallion pattern provides flexibility for reprocessing, and the fail-open strategy prioritizes customer experience. The system handles 100M+ events/day with <5-second enforcement latency and zero data loss."

---

## Summary Cheat Sheet

| Aspect | Key Point |
|--------|-----------|
| **Problem** | Real-time rate limiting for usage-based SaaS API |
| **Scale** | 100M+ events/day, 5K events/sec peak, 10K+ tenants |
| **Architecture** | Kafka → Spark (Medallion) → Delta Lake → Redis → Gateway |
| **Latency** | 5 seconds end-to-end (event to enforcement) |
| **Reliability** | Checkpointing, ACID transactions, fail-open strategy |
| **Trade-off** | Real-time (5s) vs. batch (1hr) → Chose real-time for revenue protection |
| **Cost** | ~$10K/month infrastructure |

**Practice Time: 4 minutes 45 seconds**
