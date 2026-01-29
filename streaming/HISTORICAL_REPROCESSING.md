# Historical Data Reprocessing Guide

## Overview

When entitlement rules change (plan limits, billing logic, abuse thresholds), historical data must be reprocessed to apply the new business logic. The **medallion architecture** enables safe, efficient replay by preserving immutable Bronze data and recomputing downstream layers.

---

## Reprocessing Triggers

| Trigger | Example | Impact |
|---------|---------|--------|
| **Plan Limit Changes** | Free plan changes from 60 → 100 req/min | Gold aggregations recalculated |
| **New Business Rules** | Add "burst allowance" for 5-min windows | Silver/Gold logic updated |
| **Bug Fixes** | Fix deduplication logic in Silver | Silver/Gold recomputed |
| **Schema Evolution** | Add new field `data_residency_region` | Bronze→Silver→Gold replay |
| **Regulatory Compliance** | GDPR right-to-erasure | Tombstone records, recompute aggregates |

---

## Step-by-Step Reprocessing Workflow

### Step 1: Identify Layers to Recompute

```
Rule Change → Determine Impact:

┌─────────────────────────────────────────────────┐
│ Bronze (Raw Data)                               │
│ ✅ NEVER RECOMPUTED - Source of Truth          │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│ Silver (Cleaned/Deduplicated)                   │
│ ⚠️  Recompute if: dedup logic, validation rules│
│     or schema parsing changes                   │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│ Gold (Aggregations/Enforcement)                 │
│ 🔄 ALWAYS RECOMPUTE - Business logic layer     │
└─────────────────────────────────────────────────┘
```

**Decision Matrix:**

| Change Type | Bronze | Silver | Gold |
|-------------|--------|--------|------|
| Plan limits change | ❌ | ❌ | ✅ |
| Deduplication logic fix | ❌ | ✅ | ✅ |
| New field added | ❌ | ✅ | ✅ |
| Aggregation window size | ❌ | ❌ | ✅ |
| Abuse threshold change | ❌ | ❌ | ✅ |

---

### Step 2: Stop Streaming Jobs

```bash
# Stop all streaming jobs to avoid conflicts
spark-submit --kill <application-id>

# Verify no active streams
spark-sql -e "SHOW STREAMS"
```

---

### Step 3: Back Up Current State (Optional)

```sql
-- Clone current Silver table for rollback
CREATE TABLE silver_api_usage_backup 
DEEP CLONE silver_api_usage;

-- Clone current Gold tables
CREATE TABLE tenant_usage_1min_backup 
DEEP CLONE tenant_usage_1min;

CREATE TABLE tenant_usage_hourly_backup 
DEEP CLONE tenant_usage_hourly;

-- Verify backup
SELECT COUNT(*), MIN(window_start), MAX(window_end) 
FROM tenant_usage_1min_backup;
```

---

### Step 4: Update Business Logic

**Example: Change Free Plan Limit from 60 → 100 req/min**

```python
# gold_aggregation.py - Update plan limits
PLAN_LIMITS = {
    "free": 100,       # Changed from 60
    "starter": 300,
    "professional": 1000,
    "enterprise": 5000
}

# DESIGN_ADDENDUM.md - Document change
# Added note: "2026-01-28: Free plan limit increased to 100 req/min"
```

---

### Step 5: Recompute Affected Layers

#### Option A: Batch Reprocessing (Full Historical Replay)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Gold-Reprocessing-Batch") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read entire Silver history (not a stream)
silver_df = spark.read.format("delta").load("/data/silver_api_usage")

# Apply updated Gold logic
from gold_aggregation import aggregate_1_minute, aggregate_hourly

# Recompute 1-minute aggregations
agg_1min = aggregate_1_minute(silver_df)

# OVERWRITE Gold table with recomputed data
agg_1min.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/data/gold_tenant_usage_1min")

# Recompute hourly aggregations
agg_hourly = aggregate_hourly(silver_df)
agg_hourly.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/data/gold_tenant_usage_hourly")
```

**Execution:**

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 50 \
  --executor-memory 16G \
  --conf spark.sql.shuffle.partitions=200 \
  reprocess_gold_batch.py
```

---

#### Option B: Incremental Reprocessing (Time Range)

```python
# Reprocess only last 7 days of data
from datetime import datetime, timedelta

start_date = datetime.now() - timedelta(days=7)

silver_df = spark.read.format("delta") \
    .load("/data/silver_api_usage") \
    .filter(f"event_timestamp >= '{start_date.isoformat()}'")

# Rest of reprocessing logic same as Option A
```

---

#### Option C: Stream Replay (Checkpoint Reset)

```bash
# Delete checkpoint to force full replay from Bronze
rm -rf /checkpoints/silver_processing/*
rm -rf /checkpoints/gold_aggregation/*

# Restart streaming jobs - will replay from Bronze offset 0
spark-submit streaming/silver_processing.py
spark-submit streaming/gold_aggregation.py
```

**⚠️ Warning:** This replays ALL Bronze data. Use only when Silver logic changes.

---

### Step 6: Validate Reprocessed Data

```sql
-- Verify record counts match
SELECT COUNT(*) FROM tenant_usage_1min;  -- Should be same or more

-- Check new plan limits applied
SELECT 
    tenant_id,
    subscription_plan,
    window_start,
    total_requests,
    over_limit_count,
    CASE 
        WHEN subscription_plan = 'free' AND total_requests > 100 THEN 'OVER_LIMIT'
        ELSE 'OK'
    END AS new_enforcement_status
FROM tenant_usage_1min
WHERE subscription_plan = 'free'
    AND window_start >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
ORDER BY total_requests DESC
LIMIT 20;

-- Compare with backup (if created)
SELECT 
    'OLD' AS version, COUNT(*) AS alert_count
FROM enforcement_alerts_backup
WHERE alert_type = 'over_limit' 
    AND alert_time >= CURRENT_DATE

UNION ALL

SELECT 
    'NEW' AS version, COUNT(*) AS alert_count
FROM enforcement_alerts
WHERE alert_type = 'over_limit' 
    AND alert_time >= CURRENT_DATE;
```

---

### Step 7: Restart Streaming Jobs

```bash
# Start Silver processing (if logic changed)
spark-submit streaming/silver_processing.py &

# Start Gold aggregation (always restart after reprocessing)
spark-submit streaming/gold_aggregation.py &

# Verify streaming lag
spark-sql -e "
SELECT 
    name, 
    batchId, 
    inputRowsPerSecond, 
    processedRowsPerSecond 
FROM streaming_metrics;
"
```

---

## Why Bronze is Preserved

### Immutability Principle

```
Bronze Layer = "Immutable Audit Log"

┌──────────────────────────────────────────────┐
│ Raw JSON payloads from Kafka                 │
│ - event_id, event_timestamp                  │
│ - kafka_offset, kafka_partition              │
│ - ingestion_timestamp                        │
│                                              │
│ ✅ NEVER DELETED OR MODIFIED                 │
│ ✅ Append-only writes                        │
│ ✅ Source of truth for ALL reprocessing      │
└──────────────────────────────────────────────┘
```

**Why?**

1. **Regulatory Compliance**: Audit logs required for SOC2, PCI-DSS, GDPR
2. **Bug Recovery**: Reprocess data after fixing bugs in Silver/Gold logic
3. **Business Agility**: Test new business rules on historical data
4. **Root Cause Analysis**: Debug production incidents by replaying exact events
5. **Cost Efficiency**: Cheaper to store Bronze than re-ingest from Kafka (which has 7-day retention)

**Storage Strategy:**

```python
# Bronze retention: 2 years (compliance requirement)
# Partitioned by ingestion_date for efficient time-range queries

spark.sql("""
    OPTIMIZE bronze_api_usage
    ZORDER BY (tenant_id, event_timestamp)
""")

# Archive old Bronze partitions to S3 Glacier after 1 year
spark.sql("""
    SELECT * FROM bronze_api_usage
    WHERE ingestion_date < CURRENT_DATE - INTERVAL 365 DAYS
""").write.parquet("s3://archive-bucket/bronze/")
```

---

## How Delta Lake Enables Safe Replay

### 1. ACID Transactions

```python
# Reprocessing writes are atomic
# Either ALL records commit or NONE (no partial writes)

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/data/gold_tenant_usage_1min")

# ✅ Readers see either:
#    - Old version (before overwrite commits)
#    - New version (after overwrite commits)
# ❌ NEVER see partial/corrupted data
```

---

### 2. Time Travel (Version History)

```sql
-- Query Gold table BEFORE reprocessing
SELECT * FROM tenant_usage_1min VERSION AS OF 42;

-- Compare before/after reprocessing
SELECT 
    old.tenant_id,
    old.total_requests AS old_requests,
    new.total_requests AS new_requests,
    new.total_requests - old.total_requests AS delta
FROM tenant_usage_1min VERSION AS OF 42 AS old
JOIN tenant_usage_1min AS new
    ON old.tenant_id = new.tenant_id
    AND old.window_start = new.window_start
WHERE new.total_requests != old.total_requests
LIMIT 100;

-- Rollback if reprocessing failed
RESTORE TABLE tenant_usage_1min TO VERSION AS OF 42;
```

**Version Retention:**

```sql
-- Keep 30 days of versions for rollback
ALTER TABLE tenant_usage_1min 
SET TBLPROPERTIES ('delta.logRetentionDuration' = '30 days');

-- Vacuum old versions (frees storage)
VACUUM tenant_usage_1min RETAIN 168 HOURS;  -- 7 days
```

---

### 3. Schema Evolution

```python
# Add new column during reprocessing without breaking readers
gold_df_v2 = gold_df.withColumn("burst_allowance_used", lit(False))

gold_df_v2.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \  # ✅ Allow schema changes
    .save("/data/gold_tenant_usage_1min")

# Old queries still work (new column defaults to NULL)
# New queries can use burst_allowance_used
```

---

### 4. Optimistic Concurrency Control

```python
# Safe concurrent writes during reprocessing
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/data/gold_tenant_usage_1min")

# Conditional update prevents race conditions
target.alias("target").merge(
    reprocessed_df.alias("source"),
    "target.tenant_id = source.tenant_id AND target.window_start = source.window_start"
).whenMatchedUpdate(
    condition="source.updated_at > target.updated_at",  # Only update if newer
    set={
        "total_requests": "source.total_requests",
        "over_limit_count": "source.over_limit_count"
    }
).execute()
```

---

### 5. Z-Ordering for Fast Replay

```sql
-- Optimize Bronze for tenant-based replay
OPTIMIZE bronze_api_usage
ZORDER BY (tenant_id, event_timestamp);

-- Replay single tenant's history (fast query)
SELECT * FROM bronze_api_usage
WHERE tenant_id = 'tenant-123'
    AND ingestion_date >= '2026-01-01';

-- Execution time: 2 seconds (vs 5 minutes without Z-ORDER)
```

---

## Example Reprocessing Scenarios

### Scenario 1: Plan Limit Increase (Free: 60 → 100 req/min)

**Layers Affected:** Gold only

**Steps:**
1. Update `PLAN_LIMITS` in `gold_aggregation.py`
2. Batch reprocess Gold from Silver (no Bronze/Silver changes)
3. Validate: Past "over_limit" alerts should decrease for Free tier

**Expected Outcome:**
- Free tier customers previously flagged as over-limit (61-100 req/min) now show as compliant
- No Silver reprocessing needed (deduplication unchanged)

---

### Scenario 2: Bug Fix in Deduplication Logic

**Layers Affected:** Silver + Gold

**Steps:**
1. Fix `Deduplicator.streaming_dedup()` in `silver_processing.py`
2. Delete Silver checkpoint, replay Bronze → Silver (full replay)
3. Delete Gold checkpoint, replay Silver → Gold
4. Validate: Duplicate count should decrease

**Expected Outcome:**
- Silver table has fewer duplicate events
- Gold aggregations more accurate (lower total_requests)

---

### Scenario 3: Add New Field "data_residency_region"

**Layers Affected:** Bronze + Silver + Gold

**Steps:**
1. Update JSON schema in `api_usage_event.schema.json`
2. Gateway starts emitting new field (forward-compatible)
3. Bronze ingests new field automatically (schema-on-read)
4. Update Silver `transform_to_silver()` to parse new field
5. Update Gold to aggregate by `data_residency_region`
6. Reprocess Silver/Gold with `mergeSchema=true`

**Expected Outcome:**
- Old records have `data_residency_region = NULL`
- New records have populated values
- No data loss, backward-compatible

---

## Best Practices

### 1. Test on Small Time Window First

```python
# Test reprocessing on 1 day before full replay
test_df = spark.read.format("delta") \
    .load("/data/silver_api_usage") \
    .filter("event_date = '2026-01-27'")

# Validate results before full reprocessing
```

---

### 2. Monitor Reprocessing Progress

```sql
-- Track reprocessing job progress
SELECT 
    COUNT(*) AS processed_records,
    MIN(window_start) AS earliest_window,
    MAX(window_start) AS latest_window
FROM tenant_usage_1min_temp;  -- Temporary output table
```

---

### 3. Use Delta Lake Time Travel for A/B Comparison

```sql
-- Compare old vs new business logic
CREATE TEMP VIEW old_logic AS
SELECT * FROM tenant_usage_1min VERSION AS OF 42;

CREATE TEMP VIEW new_logic AS
SELECT * FROM tenant_usage_1min;

-- Find discrepancies
SELECT 
    old.tenant_id,
    old.window_start,
    old.over_limit_count AS old_violations,
    new.over_limit_count AS new_violations
FROM old_logic old
JOIN new_logic new
    ON old.tenant_id = new.tenant_id 
    AND old.window_start = new.window_start
WHERE old.over_limit_count != new.over_limit_count;
```

---

### 4. Document All Reprocessing Events

```sql
-- Create audit log for reprocessing
CREATE TABLE reprocessing_audit (
    reprocessing_id STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    layer STRING,  -- "silver", "gold"
    reason STRING,  -- "plan_limit_change", "bug_fix"
    affected_date_range STRUCT<start: DATE, end: DATE>,
    record_count BIGINT,
    status STRING  -- "success", "failed", "rolled_back"
);

INSERT INTO reprocessing_audit VALUES (
    'reprocess-20260128-001',
    '2026-01-28T10:00:00Z',
    '2026-01-28T11:30:00Z',
    'gold',
    'Free plan limit increased 60→100 req/min',
    named_struct('start', '2025-01-01', 'end', '2026-01-28'),
    12500000,
    'success'
);
```

---

## Summary

| Aspect | Details |
|--------|---------|
| **Bronze Role** | Immutable source of truth, never recomputed |
| **Silver Replay** | Only if dedup/validation logic changes |
| **Gold Replay** | Always recomputed when business rules change |
| **Delta Lake ACID** | Atomic writes prevent partial/corrupted data |
| **Time Travel** | Compare versions, rollback failed reprocessing |
| **Schema Evolution** | Add fields without breaking existing queries |
| **Z-Ordering** | Fast tenant/time-based replay queries |

**Key Principle:**  
> "Bronze is cheap storage. Silver is curated data. Gold is flexible business logic. Preserve Bronze, rebuild downstream layers as needed."
