# Kafka Topic Configuration for API Usage Events

## Topic Overview

| Setting | Value | Rationale |
|---------|-------|-----------|
| **Topic Name** | `api-usage-events.v1` | Versioned for schema evolution |
| **Partitions** | 64 | Balances parallelism with overhead |
| **Replication Factor** | 3 | High availability, tolerates 2 broker failures |
| **Retention** | 7 days (time) + 500GB (size) | Replay window + cost control |

---

## Topic Configuration

```properties
# Topic: api-usage-events.v1
# ========================

# Partitioning
num.partitions=64

# Replication
replication.factor=3
min.insync.replicas=2

# Retention (dual policy - whichever triggers first)
retention.ms=604800000           # 7 days
retention.bytes=536870912000     # 500 GB per partition

# Cleanup
cleanup.policy=delete
delete.retention.ms=86400000     # 1 day tombstone retention

# Segment management  
segment.ms=3600000               # 1 hour segments
segment.bytes=1073741824         # 1 GB segments

# Compression
compression.type=lz4             # Fast compression, good ratio

# Message limits
max.message.bytes=1048576        # 1 MB max message size
message.timestamp.type=CreateTime

# Performance
unclean.leader.election.enable=false  # Prevent data loss
```

---

## Partition Key Strategy

### Recommended Key: `tenant_id`

```json
{
  "partition_key": "tenant_id",
  "example": "tenant_abc123"
}
```

### Why `tenant_id`?

| Benefit | Explanation |
|---------|-------------|
| **Event Ordering** | All events for a tenant go to same partition → guaranteed order |
| **Billing Accuracy** | Sequential processing prevents race conditions in usage aggregation |
| **Rate Limit State** | Consumers can maintain per-tenant state without coordination |
| **Consumer Locality** | Related events processed by same consumer instance |
| **Fair Distribution** | Tenants naturally distribute across partitions |

### Alternative Keys Considered

| Key | Pros | Cons | Verdict |
|-----|------|------|---------|
| `tenant_id` | Ordering, state locality | Hot tenants can skew | ✅ **Recommended** |
| `api_key_id` | Finer granularity | Too many keys, fragmented state | ❌ |
| `tenant_id + endpoint_id` | Per-endpoint ordering | Over-partitioned, complex | ❌ |
| `request_id` | Perfect distribution | No ordering guarantees | ❌ |
| `event_id` | Perfect distribution | Random, no locality | ❌ |

### Handling Hot Tenants

For tenants with extreme traffic (>10% of total), use a composite key:

```python
def compute_partition_key(event):
    tenant_id = event['tenant_id']
    
    # Check if tenant is in "hot tenant" list
    if tenant_id in HOT_TENANTS:
        # Sub-partition by API key to spread load
        # Still maintains per-API-key ordering
        return f"{tenant_id}:{event['identity']['api_key_id']}"
    
    return tenant_id
```

---

## Partition Count: 64

### Calculation

```
Target Throughput: 100,000 events/sec
Per-Partition Throughput: ~2,000 events/sec (conservative)
Min Partitions = 100,000 / 2,000 = 50

Consumer Instances: Up to 32 (max parallelism needed)
Partitions should be >= consumer count

Growth Factor: 1.5x for headroom
Final: max(50, 32) × 1.5 ≈ 64 partitions
```

### Why 64?

| Factor | Consideration |
|--------|---------------|
| **Parallelism** | Supports up to 64 concurrent consumers |
| **Throughput** | ~128K events/sec at 2K/partition |
| **Power of 2** | Better hash distribution with murmur2 |
| **Growth** | Room to scale without repartitioning |
| **Overhead** | Acceptable metadata/leader overhead |

### Partition Scaling Rules

| Cluster Size | Partitions | Reasoning |
|--------------|------------|-----------|
| Small (<10K events/sec) | 16 | Minimal overhead |
| Medium (10K-100K events/sec) | 64 | Balanced |
| Large (100K-1M events/sec) | 256 | High parallelism |
| Enterprise (>1M events/sec) | 512+ | Maximum throughput |

---

## Retention Policy

### Time-Based: 7 Days

```properties
retention.ms=604800000  # 7 days in milliseconds
```

**Rationale:**
- Sufficient replay window for failed consumers
- Covers weekend outages + recovery time
- Supports late-arriving event reprocessing
- Balances storage cost vs. operational safety

### Size-Based: 500GB per Partition

```properties
retention.bytes=536870912000  # 500 GB
```

**Rationale:**
- Total topic capacity: 64 × 500GB = 32TB
- Prevents runaway storage during traffic spikes
- Size limit triggers before time during anomalies

### Retention Tiers (By Use Case)

| Consumer | Needs | Retention Requirement |
|----------|-------|----------------------|
| Real-time billing | Latest only | 1 hour |
| Abuse detection | Recent patterns | 24 hours |
| Analytics pipeline | Batch processing | 7 days |
| Compliance audit | Full history | ∞ (separate topic) |

**Solution:** Use tiered topics:

```
api-usage-events.v1           → 7 days (primary)
api-usage-events.v1.archive   → 90 days (S3 sink)
api-usage-events.v1.audit     → 7 years (compliance)
```

---

## What Breaks If Partitioning Is Wrong?

### ❌ Problem 1: Wrong Partition Key (e.g., `request_id`)

**Symptom:** Events for same tenant scattered across partitions

**What Breaks:**
```
┌─────────────────────────────────────────────────────────────┐
│ BILLING RACE CONDITION                                       │
├─────────────────────────────────────────────────────────────┤
│ Partition 1: tenant_A request at T+0 → count = 1            │
│ Partition 2: tenant_A request at T+1 → count = 1 (parallel) │
│ Partition 3: tenant_A request at T+2 → count = 1 (parallel) │
│                                                              │
│ Result: 3 requests counted as 3 separate "1s"               │
│ Expected: Sequential count 1 → 2 → 3                        │
│ Actual: Billing shows random values, revenue loss           │
└─────────────────────────────────────────────────────────────┘
```

**Impact:**
- ❌ Incorrect usage counts (billing disputes)
- ❌ Rate limiting fails (can't track request velocity)
- ❌ Abuse detection misses patterns (events out of order)

---

### ❌ Problem 2: Too Few Partitions (e.g., 4)

**Symptom:** Consumer lag grows unboundedly

**What Breaks:**
```
┌─────────────────────────────────────────────────────────────┐
│ CONSUMER BOTTLENECK                                          │
├─────────────────────────────────────────────────────────────┤
│ Incoming: 100,000 events/sec                                │
│ Partitions: 4                                               │
│ Per-partition: 25,000 events/sec                            │
│ Consumer capacity: 2,000 events/sec per partition           │
│                                                              │
│ Result: 23,000 events/sec backing up per partition          │
│ Lag after 1 hour: 82.8 million events                       │
│ Time to catch up: Never (permanently behind)                │
└─────────────────────────────────────────────────────────────┘
```

**Impact:**
- ❌ Real-time dashboards show stale data (hours old)
- ❌ Rate limiting decisions based on old state
- ❌ Abuse detected hours after it happens
- ❌ Billing delays cause revenue recognition issues

---

### ❌ Problem 3: Too Many Partitions (e.g., 10,000)

**Symptom:** Cluster instability, high latency

**What Breaks:**
```
┌─────────────────────────────────────────────────────────────┐
│ CLUSTER OVERHEAD                                             │
├─────────────────────────────────────────────────────────────┤
│ Partitions: 10,000                                          │
│ Replicas: 3                                                 │
│ Total partition-replicas: 30,000                            │
│                                                              │
│ Controller failover time: 30+ seconds (vs 1-2 normal)       │
│ Memory per broker: 10GB+ just for metadata                  │
│ End-to-end latency: 100ms+ (vs 10ms normal)                 │
│ Producer batching: Inefficient (too many small batches)     │
└─────────────────────────────────────────────────────────────┘
```

**Impact:**
- ❌ Broker restarts take minutes, not seconds
- ❌ High latency breaks real-time use cases
- ❌ Increased operational cost (more brokers needed)
- ❌ Consumer rebalancing takes forever

---

### ❌ Problem 4: Hot Partition (One Tenant = 50% Traffic)

**Symptom:** One partition at 100% CPU, others idle

**What Breaks:**
```
┌─────────────────────────────────────────────────────────────┐
│ PARTITION SKEW                                               │
├─────────────────────────────────────────────────────────────┤
│ Tenant "BigCorp" = 50% of all traffic                       │
│ All BigCorp events → Partition 17                           │
│                                                              │
│ Partition 17: 50,000 events/sec (overloaded)               │
│ Other 63 partitions: 800 events/sec each (underutilized)   │
│                                                              │
│ Consumer for P17: Falling behind                            │
│ Other consumers: Mostly idle                                │
└─────────────────────────────────────────────────────────────┘
```

**Impact:**
- ❌ BigCorp's events delayed (SLA violation)
- ❌ Wasted consumer capacity (63 underutilized)
- ❌ Can't scale horizontally (one partition is bottleneck)

**Solution:** Composite key for hot tenants (see above)

---

## Complete Kafka Configuration

### Producer Configuration

```properties
# Producer settings for API Gateway
bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092

# Reliability
acks=all
retries=2147483647
max.in.flight.requests.per.connection=5
enable.idempotence=true

# Batching (latency vs throughput tradeoff)
batch.size=65536
linger.ms=5
buffer.memory=67108864

# Compression
compression.type=lz4

# Serialization
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=io.confluent.kafka.serializers.KafkaJsonSchemaSerializer
```

### Consumer Configuration

```properties
# Consumer settings for downstream processors
bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092
group.id=api-usage-billing-processor

# Offset management
auto.offset.reset=earliest
enable.auto.commit=false

# Processing
max.poll.records=500
max.poll.interval.ms=300000
session.timeout.ms=45000
heartbeat.interval.ms=15000

# Fetch settings
fetch.min.bytes=1
fetch.max.wait.ms=500
max.partition.fetch.bytes=1048576
```

---

## Monitoring Alerts

```yaml
alerts:
  - name: HighConsumerLag
    condition: kafka_consumer_lag > 100000
    severity: warning
    action: Scale up consumers
    
  - name: PartitionSkew
    condition: max(partition_messages) / avg(partition_messages) > 3
    severity: warning
    action: Review partition key for hot tenants
    
  - name: UnderReplicatedPartitions
    condition: kafka_under_replicated_partitions > 0
    severity: critical
    action: Check broker health
    
  - name: RetentionBreach
    condition: partition_size_bytes > retention_bytes * 0.9
    severity: warning
    action: Increase retention or reduce traffic
```
