# Data Quality Framework

## Overview

Comprehensive data quality validation for the streaming pipeline, ensuring data integrity across Bronze → Silver → Gold layers.

**Quality Dimensions:**
1. **Schema Validation**: Structure, types, required fields
2. **Value Validation**: Ranges, formats, business rules
3. **Volume Validation**: Anomaly detection (spikes/drops)
4. **Null Validation**: Critical fields must be non-null

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Streaming Data (Kafka → Bronze → Silver → Gold)        │
└────────────────────┬────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────┐
│ DataQualityChecker.validate_and_flag()                  │
│ - validate_schema()                                     │
│ - validate_values()                                     │
│ - check_null_critical_fields()                          │
│ - detect_volume_anomalies()                             │
└────────────────────┬────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────┐
│ Quality Flags Added to Every Record                     │
│ - quality_check_passed: bool                            │
│ - quality_check_errors: map<string, array<string>>      │
│ - quality_check_timestamp: timestamp                    │
└────────────────────┬────────────────────────────────────┘
                     ↓
          ┌──────────┴──────────┐
          ↓                     ↓
┌──────────────────┐  ┌──────────────────┐
│ Valid Records    │  │ Failed Records   │
│ (Continue Flow)  │  │ (Quarantine)     │
└──────────────────┘  └──────────────────┘
          ↓                     ↓
┌──────────────────┐  ┌──────────────────┐
│ Next Layer       │  │ quarantine_table │
│ (Silver/Gold)    │  │ (Manual Review)  │
└──────────────────┘  └──────────────────┘
          ↓
┌─────────────────────────────────────────────────────────┐
│ Quality Metrics & Alerts                                │
│ - quality_metrics table (pass rates, error counts)      │
│ - quality_alerts table (violations, anomalies)          │
└─────────────────────────────────────────────────────────┘
```

---

## 1. Schema Validation

### Purpose
Ensure data structure matches expected schema and critical fields are present.

### Checks Performed

| Check | Description | Failure Action |
|-------|-------------|----------------|
| **Required Fields Present** | All critical fields exist in DataFrame | Flag as `schema_error: missing_<field>` |
| **No Nulls in Critical Fields** | Critical fields are non-null | Flag as `schema_error: null_<field>` |
| **Correct Data Types** | Fields match expected types | Flag as `schema_error: type_mismatch_<field>` |

### Critical Fields by Layer

```python
CRITICAL_FIELDS = {
    "bronze": [
        "event_id",              # Unique identifier
        "event_timestamp",       # When event occurred
        "tenant_id",             # Customer identifier
        "ingestion_timestamp",   # When ingested
        "kafka_offset",          # Source tracking
        "kafka_partition"        # Source tracking
    ],
    "silver": [
        "event_id",
        "event_timestamp",
        "tenant_id",
        "api_key_id",
        "endpoint",
        "http_method",
        "response_status_code",
        "subscription_plan"      # For enforcement
    ],
    "gold": [
        "tenant_id",
        "window_start",
        "window_end",
        "total_requests",
        "subscription_plan"
    ]
}
```

### Example: Schema Validation SQL

```sql
-- Check for missing critical fields in Silver
SELECT 
    COUNT(*) AS records_with_schema_errors,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_api_usage) AS error_rate_pct
FROM silver_api_usage
WHERE NOT quality_check_passed
    AND exists(
        SELECT 1 
        FROM explode(quality_check_errors['schema_errors']) AS error
        WHERE error LIKE 'null_%' OR error LIKE 'missing_%'
    );

-- Top 5 schema errors
SELECT 
    error,
    COUNT(*) AS occurrence_count
FROM silver_api_usage
LATERAL VIEW explode(quality_check_errors['schema_errors']) AS error
WHERE NOT quality_check_passed
GROUP BY error
ORDER BY occurrence_count DESC
LIMIT 5;
```

---

## 2. Value Validation

### Purpose
Ensure field values conform to business rules, ranges, and formats.

### Validation Rules

#### Numeric Ranges

```python
VALUE_CONSTRAINTS = {
    "response_status_code": (100, 599),       # Valid HTTP codes
    "response_time_ms": (0, 300000),          # 0-5 minutes
    "request_size_bytes": (0, 100_000_000),   # 0-100 MB
    "response_size_bytes": (0, 500_000_000),  # 0-500 MB
    "risk_score": (0.0, 1.0),                 # Normalized 0-1
    "velocity_last_1min": (0, 10000),         # Max 10k req/min
    "velocity_last_5min": (0, 50000),         # Max 50k req/5min
}
```

**Example Validation:**
```python
# response_status_code must be 100-599
invalid_status = df.filter(
    (col("response_status_code") < 100) | 
    (col("response_status_code") > 599)
)
# These records get flagged: value_error: invalid_range_response_status_code
```

#### Enum Values

```python
VALID_ENUMS = {
    "http_method": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    "subscription_plan": ["free", "starter", "professional", "enterprise"],
    "rate_limit_status": ["allowed", "rate_limited", "quota_exceeded"],
}
```

**Example Validation:**
```python
# http_method must be one of allowed verbs
invalid_method = df.filter(~col("http_method").isin(VALID_ENUMS["http_method"]))
# These records get flagged: value_error: invalid_enum_http_method
```

#### String Formats

| Field | Validation | Example Error |
|-------|------------|---------------|
| `tenant_id` | Non-empty string | `empty_tenant_id` |
| `event_id` | UUID format (36 chars) | `invalid_event_id_format` |
| `endpoint` | Valid URL path | `invalid_endpoint_format` |

#### Timestamp Logic

```python
# Event timestamp should not be in the future relative to ingestion
invalid_timestamps = df.filter(col("event_timestamp") > col("ingestion_timestamp"))
# These records get flagged: value_error: future_event_timestamp
```

### Example: Value Validation SQL

```sql
-- Records with out-of-range values
SELECT 
    tenant_id,
    event_id,
    response_status_code,
    response_time_ms,
    quality_check_errors
FROM silver_api_usage
WHERE NOT quality_check_passed
    AND exists(
        SELECT 1 
        FROM explode(quality_check_errors['value_errors']) AS error
        WHERE error LIKE 'invalid_range_%'
    )
LIMIT 100;

-- Distribution of value errors
SELECT 
    error,
    COUNT(*) AS count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
FROM silver_api_usage
LATERAL VIEW explode(quality_check_errors['value_errors']) AS error
WHERE NOT quality_check_passed
GROUP BY error
ORDER BY count DESC;
```

---

## 3. Volume Anomaly Detection

### Purpose
Detect unexpected spikes or drops in data volume that may indicate:
- Upstream system failures
- Attack/abuse patterns
- Configuration errors
- Data pipeline issues

### Algorithm: Z-Score Based Detection

```
Z-Score = (Current Volume - Historical Mean) / Historical StdDev

Anomaly if: |Z-Score| > 3.0 (3 standard deviations)
```

### Configuration

```python
VOLUME_ANOMALY_THRESHOLD = 3.0   # 3 std deviations
VOLUME_HISTORY_HOURS = 24        # Look back 24 hours
```

### How It Works

1. **Collect Baseline**: Aggregate last 24 hours into 1-hour windows
2. **Calculate Stats**: Mean and standard deviation of window volumes
3. **Compare Current Batch**: Calculate Z-score for current batch
4. **Flag Anomalies**: If |Z-score| > 3.0, flag as anomaly

### Example Scenario

```
Historical Baseline (last 24 hours):
  Mean volume per hour: 100,000 records
  Std Dev: 10,000 records

Current Batch:
  Volume: 150,000 records
  Z-score = (150,000 - 100,000) / 10,000 = 5.0
  
  ✅ Anomaly Detected! (5.0 > 3.0 threshold)
  Alert: "Volume spike detected in bronze layer"
```

### Volume Anomaly Detection SQL

```sql
-- Calculate baseline volume statistics (last 24 hours)
WITH baseline AS (
    SELECT 
        DATE_TRUNC('hour', ingestion_timestamp) AS hour_window,
        COUNT(*) AS record_count
    FROM bronze_api_usage
    WHERE ingestion_timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    GROUP BY hour_window
),
stats AS (
    SELECT 
        AVG(record_count) AS mean_volume,
        STDDEV(record_count) AS stddev_volume,
        COUNT(*) AS sample_size
    FROM baseline
)
SELECT 
    mean_volume,
    stddev_volume,
    sample_size,
    -- Current batch volume (replace with actual)
    120000 AS current_volume,
    (120000 - mean_volume) / NULLIF(stddev_volume, 0) AS z_score,
    CASE 
        WHEN ABS((120000 - mean_volume) / NULLIF(stddev_volume, 0)) > 3.0 
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS status
FROM stats;
```

### Volume Anomaly Alerts

```sql
-- Query recent volume anomalies
SELECT 
    alert_timestamp,
    layer,
    severity,
    message,
    get_json_object(details, '$.total_records') AS current_volume,
    get_json_object(details, '$.volume_z_score') AS z_score,
    get_json_object(details, '$.volume_baseline.mean_volume') AS baseline_mean
FROM quality_alerts
WHERE alert_type = 'volume_anomaly'
    AND alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
ORDER BY alert_timestamp DESC;
```

---

## 4. Null Checks on Critical Fields

### Purpose
Explicit validation that business-critical fields are never null.

### Critical Field Categories

| Category | Fields | Rationale |
|----------|--------|-----------|
| **Identity** | `event_id`, `tenant_id`, `api_key_id` | Required for tracking and billing |
| **Timing** | `event_timestamp`, `ingestion_timestamp` | Required for windowing and watermarks |
| **Request** | `endpoint`, `http_method` | Required for usage analysis |
| **Response** | `response_status_code` | Required for error tracking |
| **Enforcement** | `subscription_plan` | Required for rate limiting |

### Null Check Example

```python
# Check if any critical field is null
critical_fields = ["event_id", "tenant_id", "event_timestamp", "subscription_plan"]

null_check_df = df.select(
    "*",
    *[col(field).isNull().alias(f"{field}_is_null") for field in critical_fields]
)

# Flag records with any null critical field
null_check_df = null_check_df.withColumn(
    "has_null_critical_field",
    array(*[col(f"{field}_is_null") for field in critical_fields]).cast("array<boolean>")
).withColumn(
    "null_check_passed",
    ~array_contains(col("has_null_critical_field"), True)
)
```

### Null Check SQL

```sql
-- Records failing null checks
SELECT 
    COUNT(*) AS null_error_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_api_usage) AS null_error_rate_pct
FROM silver_api_usage
WHERE NOT quality_check_passed
    AND cardinality(quality_check_errors['null_fields']) > 0;

-- Which fields have nulls most frequently?
SELECT 
    null_field,
    COUNT(*) AS null_count
FROM silver_api_usage
LATERAL VIEW explode(quality_check_errors['null_fields']) AS null_field
WHERE NOT quality_check_passed
GROUP BY null_field
ORDER BY null_count DESC;

-- Example: Find records with null tenant_id (critical error)
SELECT 
    event_id,
    event_timestamp,
    endpoint,
    quality_check_errors
FROM silver_api_usage
WHERE NOT quality_check_passed
    AND array_contains(quality_check_errors['null_fields'], 'tenant_id')
LIMIT 100;
```

---

## 5. How Failures Are Handled

### Failure Handling Strategy

```
Every Record → Quality Validation → Pass/Fail Decision

Pass (quality_check_passed = true):
  ↓
  Continue to next layer (Silver → Gold)

Fail (quality_check_passed = false):
  ↓
  Route to QUARANTINE table
  ↓
  Log errors in quality_check_errors
  ↓
  Generate quality alerts (if severe)
  ↓
  Manual review or automated reprocessing
```

### Quarantine Table Structure

```sql
CREATE TABLE quarantine_<layer> (
    -- Original record columns (all fields from source)
    event_id STRING,
    tenant_id STRING,
    ...
    
    -- Quality check metadata
    quality_check_passed BOOLEAN,
    quality_check_errors MAP<STRING, ARRAY<STRING>>,
    quality_check_timestamp TIMESTAMP,
    
    -- Quarantine metadata
    quarantine_timestamp TIMESTAMP,
    quarantine_layer STRING,      -- "bronze", "silver", "gold"
    quarantine_reason STRING,     -- "schema_error", "value_error", "null_error"
    reprocessed BOOLEAN,          -- Has this been fixed and reprocessed?
    reprocessed_timestamp TIMESTAMP
)
PARTITIONED BY (quarantine_date DATE);
```

### Failure Handling Code

```python
from data_quality_checks import DataQualityChecker, handle_failed_records

# Initialize checker
quality_checker = DataQualityChecker(layer="silver", alert_on_failure=True)

# Validate batch
validated_df = quality_checker.validate_and_flag(silver_df)

# Separate valid from failed records
valid_df = handle_failed_records(
    validated_df=validated_df,
    quarantine_path="/data/quarantine_silver",
    layer="silver"
)

# Continue processing with only valid records
valid_df.write.format("delta").mode("append").save("/data/silver_api_usage")
```

### Failure Severity Levels

| Severity | Trigger | Action |
|----------|---------|--------|
| **INFO** | Individual record fails validation | Log to quarantine, continue processing |
| **WARNING** | Quality pass rate < 95% | Write quality alert, notify on-call |
| **CRITICAL** | Quality pass rate < 80% OR volume anomaly Z-score > 4 | Page on-call, halt pipeline (optional) |

### Alert Generation

```python
# Alerts written to quality_alerts table
if quality_pass_rate < 0.95:
    write_quality_alert(
        alert_type="low_quality_rate",
        severity="warning",
        message=f"Quality below 95%: {quality_pass_rate:.2%}"
    )

if quality_pass_rate < 0.80:
    write_quality_alert(
        alert_type="critical_quality_rate",
        severity="critical",
        message=f"CRITICAL: Quality below 80%: {quality_pass_rate:.2%}"
    )

if volume_anomaly_detected:
    write_quality_alert(
        alert_type="volume_anomaly",
        severity="warning" if abs(z_score) < 4 else "critical",
        message=f"Volume anomaly: Z-score={z_score:.2f}"
    )
```

### Query Quarantined Records

```sql
-- Review recent quarantine entries
SELECT 
    quarantine_timestamp,
    quarantine_layer,
    event_id,
    tenant_id,
    quality_check_errors,
    reprocessed
FROM quarantine_silver
WHERE quarantine_date >= CURRENT_DATE - INTERVAL 7 DAYS
    AND NOT reprocessed
ORDER BY quarantine_timestamp DESC
LIMIT 100;

-- Quarantine summary by error type
SELECT 
    DATE(quarantine_timestamp) AS quarantine_date,
    cardinality(quality_check_errors['schema_errors']) AS schema_errors,
    cardinality(quality_check_errors['value_errors']) AS value_errors,
    cardinality(quality_check_errors['null_fields']) AS null_errors,
    COUNT(*) AS total_quarantined
FROM quarantine_silver
WHERE quarantine_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY quarantine_date
ORDER BY quarantine_date DESC;
```

### Reprocessing Quarantined Data

```python
"""
After fixing upstream issues (schema changes, data quality improvements),
reprocess quarantined records.
"""

# Read quarantined records
quarantined_df = spark.read.format("delta") \
    .load("/data/quarantine_silver") \
    .filter("NOT reprocessed")

# Re-validate with updated quality checker
quality_checker = DataQualityChecker(layer="silver")
revalidated_df = quality_checker.validate_and_flag(quarantined_df)

# Separate now-valid records
now_valid_df = revalidated_df.filter(col("quality_check_passed"))

# Write back to Silver
now_valid_df.write.format("delta").mode("append").save("/data/silver_api_usage")

# Mark as reprocessed in quarantine
from delta.tables import DeltaTable

quarantine_table = DeltaTable.forPath(spark, "/data/quarantine_silver")

quarantine_table.update(
    condition=expr("event_id IN (SELECT event_id FROM now_valid_df)"),
    set={
        "reprocessed": lit(True),
        "reprocessed_timestamp": current_timestamp()
    }
)

print(f"Reprocessed {now_valid_df.count()} records from quarantine")
```

---

## 6. Integration with Streaming Pipeline

### Bronze Layer Integration

```python
# bronze_ingestion.py - Add quality checks

from data_quality_checks import DataQualityChecker, handle_failed_records

def process_bronze_batch(batch_df: DataFrame, batch_id: int):
    # Initialize quality checker
    quality_checker = DataQualityChecker(layer="bronze", alert_on_failure=True)
    
    # Validate data
    validated_df = quality_checker.validate_and_flag(batch_df)
    
    # Write quality metrics
    quality_checker.write_quality_metrics(
        spark=spark,
        validated_df=validated_df,
        batch_id=batch_id,
        table_path="/data/quality_metrics"
    )
    
    # Handle failed records
    valid_df = handle_failed_records(
        validated_df=validated_df,
        quarantine_path="/data/quarantine_bronze",
        layer="bronze"
    )
    
    # Write valid records to Bronze
    valid_df.write.format("delta").mode("append").save("/data/bronze_api_usage")

# Attach to streaming query
bronze_stream.writeStream \
    .foreachBatch(process_bronze_batch) \
    .option("checkpointLocation", "/checkpoints/bronze_quality") \
    .start()
```

### Silver Layer Integration

```python
# silver_processing.py - Add quality checks after deduplication

def process_silver_batch(batch_df: DataFrame, batch_id: int):
    # Existing deduplication logic
    deduped_df = deduplicator.streaming_dedup(batch_df)
    
    # Add quality validation
    quality_checker = DataQualityChecker(layer="silver", alert_on_failure=True)
    validated_df = quality_checker.validate_and_flag(deduped_df)
    
    # Write quality metrics
    quality_checker.write_quality_metrics(
        spark=spark,
        validated_df=validated_df,
        batch_id=batch_id
    )
    
    # Quarantine failed records
    valid_df = handle_failed_records(
        validated_df=validated_df,
        quarantine_path="/data/quarantine_silver",
        layer="silver"
    )
    
    # Write to Silver
    valid_df.write.format("delta").mode("append").save("/data/silver_api_usage")
```

### Gold Layer Integration

```python
# gold_aggregation.py - Validate aggregated data

def process_gold_batch(batch_df: DataFrame, batch_id: int):
    # Existing aggregation logic
    agg_df = aggregate_1_minute(batch_df)
    
    # Quality validation on aggregated data
    quality_checker = DataQualityChecker(layer="gold", alert_on_failure=True)
    validated_df = quality_checker.validate_and_flag(agg_df)
    
    # Write metrics (should have 100% pass rate in Gold)
    quality_checker.write_quality_metrics(
        spark=spark,
        validated_df=validated_df,
        batch_id=batch_id
    )
    
    # Quarantine anomalies (should be rare)
    valid_df = handle_failed_records(
        validated_df=validated_df,
        quarantine_path="/data/quarantine_gold",
        layer="gold"
    )
    
    # Write to Gold
    valid_df.write.format("delta").mode("overwrite").save("/data/gold_tenant_usage_1min")
```

---

## 7. Monitoring & Alerting

### Quality Metrics Dashboard

```sql
-- Overall quality health (last 24 hours)
SELECT 
    layer,
    COUNT(*) AS total_batches,
    AVG(quality_pass_rate) AS avg_pass_rate,
    MIN(quality_pass_rate) AS min_pass_rate,
    MAX(quality_pass_rate) AS max_pass_rate,
    SUM(invalid_records) AS total_invalid_records,
    SUM(CASE WHEN volume_anomaly_detected THEN 1 ELSE 0 END) AS anomaly_count
FROM quality_metrics
WHERE metric_timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
GROUP BY layer
ORDER BY layer;

-- Quality trend over time (last 7 days, hourly)
SELECT 
    DATE_TRUNC('hour', metric_timestamp) AS hour,
    layer,
    AVG(quality_pass_rate) AS avg_pass_rate,
    SUM(total_records) AS total_records,
    SUM(invalid_records) AS invalid_records
FROM quality_metrics
WHERE metric_timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
GROUP BY hour, layer
ORDER BY hour DESC, layer;

-- Active alerts (unresolved)
SELECT 
    alert_timestamp,
    layer,
    alert_type,
    severity,
    message
FROM quality_alerts
WHERE alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
ORDER BY 
    CASE severity 
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        ELSE 3
    END,
    alert_timestamp DESC;
```

### Grafana Dashboard Queries

```sql
-- Panel 1: Pass Rate by Layer (line chart)
SELECT 
    metric_timestamp AS time,
    layer,
    quality_pass_rate * 100 AS pass_rate_pct
FROM quality_metrics
WHERE metric_timestamp >= $__timeFrom()
    AND metric_timestamp <= $__timeTo()
ORDER BY time;

-- Panel 2: Volume Anomaly Count (stat panel)
SELECT COUNT(*) AS anomaly_count
FROM quality_metrics
WHERE volume_anomaly_detected = true
    AND metric_timestamp >= NOW() - INTERVAL 24 HOURS;

-- Panel 3: Top Error Types (bar chart)
SELECT 
    error_type,
    SUM(error_count) AS total_errors
FROM (
    SELECT 
        explode(top_error_types) AS (error_type, error_count)
    FROM quality_metrics
    WHERE metric_timestamp >= NOW() - INTERVAL 24 HOURS
)
GROUP BY error_type
ORDER BY total_errors DESC
LIMIT 10;

-- Panel 4: Quarantine Growth (area chart)
SELECT 
    DATE(quarantine_timestamp) AS date,
    quarantine_layer,
    COUNT(*) AS quarantined_records
FROM quarantine_silver
WHERE quarantine_timestamp >= NOW() - INTERVAL 30 DAYS
GROUP BY date, quarantine_layer
ORDER BY date;
```

---

## 8. Best Practices

### 1. Layer-Specific Validation

```
Bronze: Focus on schema and ingestion integrity
  - All Kafka metadata present (offset, partition)
  - No parsing errors
  - Timestamps valid

Silver: Focus on business logic and deduplication
  - All critical fields populated
  - Value ranges correct
  - Enum values valid
  - No duplicates

Gold: Focus on aggregation accuracy
  - Window boundaries correct
  - Aggregate calculations accurate
  - Plan limits applied correctly
```

### 2. Tune Anomaly Detection

```python
# Adjust thresholds based on business context

# Low-variability system (stable traffic)
VOLUME_ANOMALY_THRESHOLD = 2.5  # More sensitive

# High-variability system (bursty traffic)
VOLUME_ANOMALY_THRESHOLD = 4.0  # Less sensitive

# Increase history window for more stable baseline
VOLUME_HISTORY_HOURS = 72  # 3 days
```

### 3. Quarantine Retention Policy

```sql
-- Delete old quarantined records after 90 days
DELETE FROM quarantine_silver
WHERE quarantine_date < CURRENT_DATE - INTERVAL 90 DAYS
    AND reprocessed = true;

-- Vacuum to reclaim storage
VACUUM quarantine_silver RETAIN 168 HOURS;
```

### 4. Alert Fatigue Prevention

```python
# Implement alert throttling
# Only alert once per hour for same error type

last_alert_time = get_last_alert_time(alert_type="low_quality_rate", layer="silver")

if datetime.now() - last_alert_time > timedelta(hours=1):
    write_quality_alert(...)  # Only if >1 hour since last alert
```

### 5. Performance Optimization

```sql
-- Z-ORDER quarantine tables for fast error type queries
OPTIMIZE quarantine_silver
ZORDER BY (quarantine_layer, quarantine_date);

-- Partition quality metrics by date
CREATE TABLE quality_metrics (
    ...
)
PARTITIONED BY (metric_date DATE);
```

---

## Summary

| Check Type | Purpose | Failure Impact | Handling |
|------------|---------|----------------|----------|
| **Schema Validation** | Ensure structure matches expected schema | Record unusable | Quarantine |
| **Value Validation** | Ensure values within valid ranges/enums | Business logic errors | Quarantine |
| **Volume Anomaly** | Detect unusual traffic patterns | System/attack detection | Alert only |
| **Null Checks** | Ensure critical fields populated | Missing business data | Quarantine |

**Key Principle:**  
> "Validate early, quarantine failures, monitor continuously. Never let bad data corrupt downstream layers."
