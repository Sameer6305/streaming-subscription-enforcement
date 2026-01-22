# API Usage Event Schema - Field Explanations

This document explains the production-grade JSON event schema for API usage events emitted by a SaaS API gateway.

---

## Core Design Principles

### 1. Idempotency Support
- **`event_id`** (UUIDv7): Globally unique identifier for deduplication. Consumers can safely reprocess events by tracking seen IDs.
- **`sequence_number`**: Per-tenant monotonic counter for ordering and gap detection.

### 2. Late-Arriving Event Support
- **`event_timestamp`**: When the API request actually occurred (authoritative time).
- **`ingestion_timestamp`**: When the event entered the pipeline.
- **`processing_timestamp`**: When consumers processed the event.
- **Latency Detection**: `ingestion_timestamp - event_timestamp` reveals delivery delays.

### 3. Abuse Detection Support
- **`client`**: IP address, user-agent, device fingerprinting.
- **`geo`**: Geographic data including VPN/Tor/datacenter detection.
- **`rate_limiting`**: Current rate limit state and enforcement.
- **`abuse_signals`**: Risk scores, velocity metrics, and action taken.

---

## Field Groups Explained

### Event Envelope

| Field | Purpose |
|-------|---------|
| `event_id` | UUID for idempotency - consumers deduplicate on this |
| `event_version` | Schema version for backward compatibility |
| `event_timestamp` | When request occurred (source of truth for time) |
| `ingestion_timestamp` | When event was ingested (for latency detection) |
| `sequence_number` | Ordering within tenant - detects gaps/reordering |

### Tenant & Subscription

| Field | Purpose |
|-------|---------|
| `tenant_id` | Multi-tenant isolation key |
| `subscription.plan_tier` | Rate limit and billing tier |
| `subscription.billing_cycle_id` | Accurate usage aggregation per billing period |

### Identity

| Field | Purpose |
|-------|---------|
| `identity.api_key_id` | Hashed key ID for per-key tracking (never raw keys) |
| `identity.api_key_prefix` | First 8 chars for identification |
| `identity.auth_method` | Authentication type used |
| `identity.scopes` | Authorized permissions |

### Request Details

| Field | Purpose |
|-------|---------|
| `request.request_id` | Unique request ID for tracing |
| `request.trace_id` | W3C distributed trace ID |
| `request.path` | Normalized path (`/users/{id}`) for aggregation |
| `request.path_raw` | Original path for debugging |
| `request.endpoint_id` | Per-endpoint analytics and rate limiting |
| `request.content_length` | Bandwidth billing, abuse detection (large payloads) |

### Response Details

| Field | Purpose |
|-------|---------|
| `response.status_code` | HTTP status |
| `response.status_category` | Aggregated status (success/error/rate_limited) |
| `response.latency_ms` | Total request latency |
| `response.upstream_latency_ms` | Backend latency (separate from gateway overhead) |
| `response.cache_status` | Cache hit/miss for optimization |

### Client Information (Abuse Detection)

| Field | Purpose |
|-------|---------|
| `client.ip_address` | Client IP (may be anonymized) |
| `client.ip_address_hash` | Privacy-preserving IP tracking |
| `client.user_agent_parsed.is_bot` | Bot detection |
| `client.sdk_name/version` | Official SDK usage tracking |

### Geolocation (Abuse Detection)

| Field | Purpose |
|-------|---------|
| `geo.country_code` | Geographic analytics, compliance |
| `geo.asn` / `geo.as_org` | Hosting provider identification |
| `geo.is_datacenter` | Flag automated/bot traffic |
| `geo.is_vpn` / `geo.is_tor` / `geo.is_proxy` | Anonymization detection |

### Rate Limiting

| Field | Purpose |
|-------|---------|
| `rate_limiting.limit` | Current rate limit ceiling |
| `rate_limiting.remaining` | Requests left in window |
| `rate_limiting.was_rate_limited` | Whether request was blocked |
| `rate_limiting.quota_used_percent` | High values indicate abuse risk |

### Abuse Signals

| Field | Purpose |
|-------|---------|
| `abuse_signals.risk_score` | 0-100 computed risk score |
| `abuse_signals.risk_factors` | List of detected issues |
| `abuse_signals.velocity_*` | Request velocity at multiple windows |
| `abuse_signals.unique_ips_24h` | Detects key sharing/compromise |
| `abuse_signals.unique_endpoints_1h` | Detects enumeration attacks |
| `abuse_signals.fingerprint_hash` | Device/client fingerprinting |
| `abuse_signals.action_taken` | What the system did (allowed/blocked/etc) |

### Billing

| Field | Purpose |
|-------|---------|
| `billing.billable` | Excludes health checks, internal calls |
| `billing.billing_units` | Metered usage units |
| `billing.cost_estimate_usd` | Real-time cost estimation |

### Gateway Metadata

| Field | Purpose |
|-------|---------|
| `gateway.gateway_id` | Which gateway instance handled the request |
| `gateway.gateway_region` | Geographic distribution analytics |
| `gateway.upstream_service` | Backend service routing |

---

## Usage Examples

### Idempotency Check (Consumer Side)
```python
def process_event(event):
    event_id = event['event_id']
    if redis.sismember('processed_events', event_id):
        return  # Already processed, skip
    
    # Process event...
    redis.sadd('processed_events', event_id)
    redis.expire('processed_events', 86400 * 7)  # 7-day TTL
```

### Late-Arriving Event Detection
```python
from datetime import datetime, timedelta

def is_late_arriving(event, threshold_minutes=5):
    event_time = datetime.fromisoformat(event['event_timestamp'])
    ingestion_time = datetime.fromisoformat(event['ingestion_timestamp'])
    delay = ingestion_time - event_time
    return delay > timedelta(minutes=threshold_minutes)
```

### Abuse Detection Query
```sql
-- Find API keys with suspicious activity
SELECT 
    identity.api_key_id,
    COUNT(*) as request_count,
    AVG(abuse_signals.risk_score) as avg_risk_score,
    COUNT(DISTINCT client.ip_address_hash) as unique_ips
FROM api_usage_events
WHERE event_timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY identity.api_key_id
HAVING 
    AVG(abuse_signals.risk_score) > 70 
    OR COUNT(DISTINCT client.ip_address_hash) > 10
ORDER BY avg_risk_score DESC;
```

### Billing Aggregation
```sql
-- Monthly billing per tenant
SELECT 
    tenant_id,
    subscription.plan_tier,
    SUM(billing.billing_units) as total_units,
    SUM(billing.cost_estimate_usd) as estimated_cost
FROM api_usage_events
WHERE 
    billing.billable = true
    AND event_timestamp >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY tenant_id, subscription.plan_tier;
```

---

## Best Practices

1. **Partitioning**: Use `tenant_id` or `metadata.partition_key` for Kafka/Kinesis partitioning to maintain ordering per tenant.

2. **Retention**: Set `metadata.retention_days` based on compliance requirements (GDPR: 30 days, billing: 7 years).

3. **Privacy**: Use `ip_address_hash` instead of raw IPs in analytics. Store raw IPs only for fraud investigation with access controls.

4. **Schema Evolution**: Always check `event_version` before processing. Use backward-compatible changes only.

5. **Deduplication Window**: Keep `event_id` in a deduplication cache for at least 7 days to handle delayed retries.
