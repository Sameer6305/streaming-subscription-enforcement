# Pipeline Failure Recovery

## Overview

All failures recover **without data loss** through:
- Checkpointing (Kafka offsets)
- Delta Lake ACID transactions
- Bronze immutable storage
- Idempotent processing

---

## Failure Scenarios

### 1. Spark Crash

**What Happens:**
- Driver or executor dies mid-batch
- Processing stops immediately

**Recovery:**
```
Checkpoint stores last committed offset
→ Restart reads from checkpoint
→ Reprocesses uncommitted batch
→ Delta MERGE deduplicates on event_id
→ Exactly-once delivery maintained
```

**Result:** Zero data loss, automatic recovery

---

### 2. Kafka Restart

**What Happens:**
- Kafka cluster unavailable
- Spark can't read new events

**Recovery:**
```
Spark retries with backoff
→ Offsets safe in checkpoint (not Kafka)
→ Kafka retains data (7 days)
→ Pipeline resumes when Kafka returns
→ Catches up from checkpoint offset
```

**Result:** Temporary lag, full catch-up, no loss

---

### 3. Duplicate Events

**What Happens:**
- Producer retries → same event sent twice
- Network issues cause duplicates

**Recovery:**
```
Bronze: Stores all (including duplicates)
→ Silver: dropDuplicates(event_id) with watermark
→ Gold: Aggregates deduplicated data
→ Delta MERGE: event_id exists → skip

Replay: Same data 100x = same result
```

**Result:** Duplicates eliminated, idempotent

---

### 4. Late-Arriving Data

**What Happens:**
- Event arrives 2+ hours late (past watermark)
- Misses streaming aggregation

**Recovery:**
```
Silver: Event written with _is_late_arriving=true
→ Streaming: May miss window (state expired)
→ Batch Job: Daily reconciliation from Silver
→ Billing: Uses batch totals (not streaming)
```

**Result:** Late events preserved, corrected in batch

---

### 5. Schema Changes

**What Happens:**
- Upstream adds/removes fields
- JSON structure changes

**Recovery:**
```
Bronze: Permissive mode, stores _raw_value
→ Invalid JSON → Dead Letter Queue
→ Schema update: Modify Silver schema
→ Replay: Bronze → Silver with new schema
→ Backfill: Correct historical data
```

**Result:** No loss, controlled evolution

---

## Recovery Mechanisms

### Checkpointing
```
Location: S3/HDFS
Contains: Kafka offsets, processing state
Guarantees: Exactly-once semantics
Recovery: Restart from last commit
```

### Delta Lake ACID
```
Transactions: Atomic writes
MERGE: Idempotent upserts
Isolation: Readers see consistent snapshots
Recovery: Rollback on failure
```

### Bronze Immutability
```
Purpose: Always replayable
Retention: 90 days hot, 7 years archive
Recovery: Reprocess any time range
Use Cases: Bug fixes, schema changes
```

### Batch Reconciliation
```
Frequency: Daily
Purpose: Correct late events, gaps
Process: Recompute Gold from Silver
Source of Truth: Batch (not streaming)
```

---

## Failure Impact Matrix

| Failure | Data Loss? | Recovery Time | Manual Action? |
|---------|------------|---------------|----------------|
| Spark crash | ❌ No | < 1 min | ❌ Auto-restart |
| Kafka restart | ❌ No | 5-15 min | ✅ Ops restarts Kafka |
| Duplicates | ❌ No | Immediate | ❌ Automatic |
| Late data | ❌ No | < 24 hours | ❌ Batch reconciles |
| Schema change | ❌ No | 10-30 min | ✅ Deploy schema update |

---

## Verification Commands

### Check Checkpoint Health
```bash
# List checkpoint files
aws s3 ls s3://data-lake/checkpoints/bronze/api-usage-events/offsets/

# Latest committed offset
aws s3 cp s3://data-lake/checkpoints/bronze/.../offsets/123 - | cat
```

### Verify No Data Loss
```sql
-- Check Bronze offset continuity
SELECT 
    _kafka_partition,
    MIN(_kafka_offset) as min_offset,
    MAX(_kafka_offset) as max_offset,
    MAX(_kafka_offset) - MIN(_kafka_offset) + 1 as expected_count,
    COUNT(*) as actual_count
FROM bronze.api_usage_events
WHERE _event_date = '2026-01-28'
GROUP BY _kafka_partition;

-- Gaps indicate loss
-- expected_count != actual_count → investigate
```

### Check for Duplicates in Silver
```sql
-- Should return 0 duplicates
SELECT event_id, COUNT(*) as dup_count
FROM silver.api_usage_events
WHERE _event_date = '2026-01-28'
GROUP BY event_id
HAVING COUNT(*) > 1;
```

### Identify Late Events
```sql
-- Late events requiring reconciliation
SELECT 
    COUNT(*) as late_event_count,
    AVG(_arrival_delay_seconds) as avg_delay_sec
FROM silver.api_usage_events
WHERE _is_late_arriving = true
AND _event_date = '2026-01-28';
```

---

## Recovery Playbooks

### Spark Crash Recovery
```bash
# 1. Check if auto-restarted
kubectl get pods -n streaming | grep gold-aggregation

# 2. If not, manually restart
spark-submit ... gold_aggregation.py --checkpoint-path <existing>

# 3. Verify catch-up
# Check Spark UI: lag should decrease
```

### Schema Change Recovery
```bash
# 1. Identify schema issue
SELECT _quality_flags FROM bronze.api_usage_events_dlq
WHERE _dlq_date = CURRENT_DATE LIMIT 10;

# 2. Update Silver schema in code
vim silver_processing.py

# 3. Replay affected date range
spark-submit silver_processing.py \
  --starting-offsets '{"0":123456,...}' \
  --checkpoint-path <new-checkpoint>
```

### Late Event Reconciliation
```bash
# Daily batch job
spark-submit \
  --class GoldReconciliationBatch \
  reconcile_gold.py \
  --date 2026-01-27 \
  --silver-path s3://silver/api_usage_events \
  --gold-path s3://gold
```

---

## Design Principles

1. **Bronze = Safety Net**  
   Immutable raw data enables any recovery

2. **Checkpoints = Restart Points**  
   Exactly-once semantics via offset tracking

3. **MERGE = Idempotency**  
   Safe to replay same data multiple times

4. **Batch Reconciles Streaming**  
   Late events corrected, billing uses batch

5. **Flag, Don't Drop**  
   Late/invalid events preserved for investigation
