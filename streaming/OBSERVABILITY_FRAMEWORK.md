# Pipeline Observability Framework

## Overview

Comprehensive monitoring and alerting system for the real-time streaming pipeline, tracking performance, data quality, and system health across Bronze → Silver → Gold layers.

**Key Capabilities:**
- **Performance Monitoring**: Throughput, latency, batch duration
- **Lag Tracking**: Processing lag, Kafka consumer lag
- **Data Freshness**: Age of latest processed data
- **Quality Monitoring**: Error rates, validation pass rates
- **Alerting**: Multi-tier alerts (warning, critical) with configurable thresholds

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Streaming Pipeline (Bronze/Silver/Gold)                │
└────────────────────┬────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────┐
│ PipelineObserver.monitor_batch()                        │
│ - Collect metrics (throughput, lag, freshness)         │
│ - Evaluate alert thresholds                            │
│ - Write to monitoring tables                           │
└────────────────────┬────────────────────────────────────┘
                     ↓
          ┌──────────┴──────────┐
          ↓                     ↓
┌──────────────────┐  ┌──────────────────┐
│ observability_   │  │ observability_   │
│ metrics          │  │ alerts           │
└────────┬─────────┘  └────────┬─────────┘
         ↓                     ↓
┌──────────────────┐  ┌──────────────────┐
│ Grafana          │  │ PagerDuty/       │
│ Dashboards       │  │ Slack Alerts     │
└──────────────────┘  └──────────────────┘
```

---

## 1. Metrics to Track

### Performance Metrics

| Metric | Description | Formula | Purpose |
|--------|-------------|---------|---------|
| **Throughput** | Records processed per second | `record_count / batch_duration_sec` | Detect bottlenecks, capacity planning |
| **Batch Duration** | Time to process one micro-batch | `end_time - start_time` | Identify slow batches, optimize processing |
| **Processing Lag** | Delay between event time and processing time | `current_time - event_timestamp` | Measure real-time performance |
| **Data Freshness** | Age of most recent processed event | `current_time - max(event_timestamp)` | Detect pipeline stalls |

### Volume Metrics

| Metric | Description | Purpose |
|--------|-------------|---------|
| **Record Count** | Total records in batch | Track data volume trends |
| **Valid Records** | Records passing quality checks | Monitor data quality |
| **Invalid Records** | Records failing quality checks | Identify data quality issues |

### Quality Metrics

| Metric | Description | Formula | Target |
|--------|-------------|---------|--------|
| **Error Rate** | Percentage of failed records | `(invalid / total) * 100` | <1% |
| **Quality Pass Rate** | Percentage of valid records | `(valid / total) * 100` | >99% |

### Resource Metrics (from Spark UI)

| Metric | Description | Source |
|--------|-------------|--------|
| **Executor Memory** | Memory used by executors | Spark UI |
| **CPU Utilization** | CPU usage per executor | Spark UI |
| **Shuffle Read/Write** | Data shuffled between stages | Spark UI |
| **Task Duration** | Time per task | Spark UI |

---

## 2. Lag Monitoring

### Processing Lag

**Definition:** Time difference between when an event occurred (`event_timestamp`) and when it's processed (`current_timestamp`).

**Calculation:**
```python
processing_lag_sec = current_timestamp() - event_timestamp
```

**Expected Values:**
- **Bronze**: <5 seconds (near real-time ingestion)
- **Silver**: <30 seconds (includes deduplication)
- **Gold**: <60 seconds (includes aggregation)

**Alert Thresholds:**

| Layer | Warning | Critical | Action |
|-------|---------|----------|--------|
| Bronze | 60s | 300s | Check Kafka connectivity |
| Silver | 120s | 600s | Check deduplication performance |
| Gold | 180s | 900s | Check aggregation logic |

**SQL Query:**
```sql
-- Calculate avg processing lag per layer (last hour)
SELECT 
    layer,
    AVG(avg_processing_lag_sec) AS avg_lag_sec,
    MAX(avg_processing_lag_sec) AS max_lag_sec,
    PERCENTILE_APPROX(avg_processing_lag_sec, 0.95) AS p95_lag_sec
FROM observability_metrics
WHERE metric_timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
GROUP BY layer
ORDER BY layer;
```

**Grafana Query:**
```sql
SELECT 
    metric_timestamp AS time,
    layer,
    avg_processing_lag_sec AS lag_seconds
FROM observability_metrics
WHERE metric_timestamp >= $__timeFrom()
    AND metric_timestamp <= $__timeTo()
ORDER BY time;
```

---

### Kafka Consumer Lag

**Definition:** Number of messages behind the latest Kafka offset.

**Calculation:**
```
kafka_lag = latest_kafka_offset - current_consumer_offset
```

**Expected Values:**
- **Healthy**: <1,000 messages
- **Degraded**: 1,000-10,000 messages
- **Unhealthy**: >10,000 messages

**Alert Thresholds:**

| Severity | Threshold | Action |
|----------|-----------|--------|
| Warning | 10,000 messages | Investigate slow processing |
| Critical | 50,000 messages | Scale up Spark executors, check for bottlenecks |

**Monitoring Command:**
```bash
# Check Kafka consumer group lag
kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 \
    --group bronze-consumer-group \
    --describe

# Output:
# GROUP                 TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# bronze-consumer-group api-usage-events.v1  0         12500000        12500100        100
# bronze-consumer-group api-usage-events.v1  1         12499950        12500200        250
```

**SQL Query (from metrics):**
```sql
-- Kafka lag trend (last 24 hours)
SELECT 
    DATE_TRUNC('hour', metric_timestamp) AS hour,
    layer,
    AVG(kafka_lag_messages) AS avg_kafka_lag,
    MAX(kafka_lag_messages) AS max_kafka_lag
FROM observability_metrics
WHERE metric_timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    AND kafka_lag_messages IS NOT NULL
GROUP BY hour, layer
ORDER BY hour DESC, layer;
```

---

## 3. Data Freshness

### Definition

**Data Freshness** = Time since the most recent event was processed.

```
freshness_sec = current_timestamp - MAX(event_timestamp)
```

**Why It Matters:**
- Detects pipeline stalls (no new data arriving)
- Ensures SLAs for real-time data availability
- Critical for rate limiting (stale data = wrong enforcement)

### Expected Values

| Layer | Target Freshness | Max Acceptable |
|-------|------------------|----------------|
| Bronze | <10 seconds | 60 seconds |
| Silver | <30 seconds | 120 seconds |
| Gold | <60 seconds | 300 seconds |

### Alert Thresholds

| Severity | Threshold | Action |
|----------|-----------|--------|
| Warning | 2 minutes | Check for upstream delays |
| Critical | 10 minutes | Page on-call, investigate pipeline health |

### SQL Queries

```sql
-- Current data freshness per layer
SELECT 
    layer,
    MAX(max_event_time) AS latest_event,
    CURRENT_TIMESTAMP AS now,
    TIMESTAMPDIFF(SECOND, MAX(max_event_time), CURRENT_TIMESTAMP) AS freshness_sec
FROM observability_metrics
WHERE metric_timestamp >= CURRENT_TIMESTAMP - INTERVAL 5 MINUTES
GROUP BY layer;

-- Freshness violations (last 24 hours)
SELECT 
    metric_timestamp,
    layer,
    data_freshness_sec,
    CASE 
        WHEN data_freshness_sec >= 600 THEN 'CRITICAL'
        WHEN data_freshness_sec >= 120 THEN 'WARNING'
        ELSE 'OK'
    END AS status
FROM observability_metrics
WHERE metric_timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    AND data_freshness_sec >= 120  -- Only show warnings/critical
ORDER BY data_freshness_sec DESC;
```

### Monitoring Example

```python
# Calculate freshness in real-time
from datetime import datetime

max_event_time = df.agg(max("event_timestamp")).first()[0]
freshness_sec = (datetime.now() - max_event_time).total_seconds()

if freshness_sec > 120:
    print(f"⚠️  WARNING: Data is {freshness_sec:.0f}s old (threshold: 120s)")
    
if freshness_sec > 600:
    print(f"🔴 CRITICAL: Data is {freshness_sec:.0f}s old (threshold: 600s)")
    send_pagerduty_alert(...)
```

---

## 4. Alerting Thresholds

### Alert Configuration

```python
from observability import AlertThresholds

# Default thresholds (suitable for most pipelines)
default_thresholds = AlertThresholds(
    processing_lag_warning=60,       # 1 minute
    processing_lag_critical=300,     # 5 minutes
    kafka_lag_warning=10000,         # 10k messages
    kafka_lag_critical=50000,        # 50k messages
    data_freshness_warning=120,      # 2 minutes
    data_freshness_critical=600,     # 10 minutes
    throughput_min_warning=100.0,    # 100 rec/sec
    throughput_min_critical=10.0,    # 10 rec/sec
    error_rate_warning=1.0,          # 1%
    error_rate_critical=5.0,         # 5%
    quality_pass_rate_warning=95.0,  # 95%
    quality_pass_rate_critical=80.0, # 80%
    batch_duration_warning=30,       # 30 seconds
    batch_duration_critical=120      # 2 minutes
)

# Bronze layer (more lenient throughput)
bronze_thresholds = AlertThresholds(
    throughput_min_warning=500.0,    # 500 rec/sec
    throughput_min_critical=100.0,   # 100 rec/sec
)

# Silver layer (stricter quality requirements)
silver_thresholds = AlertThresholds(
    quality_pass_rate_warning=98.0,  # 98%
    quality_pass_rate_critical=95.0, # 95%
)

# Gold layer (focus on freshness for enforcement)
gold_thresholds = AlertThresholds(
    data_freshness_warning=60,       # 1 minute
    data_freshness_critical=300,     # 5 minutes
)
```

### Alert Types & Severities

| Alert Type | Severity | Threshold | Action |
|------------|----------|-----------|--------|
| **processing_lag_warning** | Warning | >60s avg lag | Monitor, may self-recover |
| **processing_lag_critical** | Critical | >300s avg lag | Page on-call, check Spark executors |
| **data_freshness_warning** | Warning | >120s stale | Check upstream data flow |
| **data_freshness_critical** | Critical | >600s stale | Page on-call, investigate pipeline stall |
| **throughput_warning** | Warning | <100 rec/sec | Monitor for capacity issues |
| **throughput_critical** | Critical | <10 rec/sec | Scale up resources, check for errors |
| **error_rate_warning** | Warning | >1% errors | Review quarantine table |
| **error_rate_critical** | Critical | >5% errors | Stop pipeline, investigate data quality |
| **quality_pass_rate_warning** | Warning | <95% pass | Check validation rules |
| **quality_pass_rate_critical** | Critical | <80% pass | Stop pipeline, fix upstream issues |
| **batch_duration_warning** | Warning | >30s duration | Optimize query logic |
| **batch_duration_critical** | Critical | >120s duration | Investigate bottlenecks, add resources |
| **kafka_lag_warning** | Warning | >10k messages | Monitor, may need scaling |
| **kafka_lag_critical** | Critical | >50k messages | Scale up consumers, check Kafka brokers |

### Alert Routing

```python
# Alert routing configuration
ALERT_ROUTES = {
    "critical": {
        "channels": ["pagerduty", "slack-oncall", "email"],
        "escalation_time": 5  # minutes
    },
    "warning": {
        "channels": ["slack-alerts", "email"],
        "escalation_time": 30  # minutes
    }
}

def send_alert(alert: Dict):
    severity = alert["severity"]
    routes = ALERT_ROUTES.get(severity, {})
    
    for channel in routes.get("channels", []):
        if channel == "pagerduty":
            send_pagerduty_incident(alert)
        elif channel == "slack-oncall":
            send_slack_message("#oncall", alert)
        elif channel == "slack-alerts":
            send_slack_message("#pipeline-alerts", alert)
        elif channel == "email":
            send_email_alert(alert)
```

### Alert Query Examples

```sql
-- Active critical alerts (last hour)
SELECT 
    alert_timestamp,
    layer,
    alert_type,
    message,
    metric_value,
    threshold
FROM observability_alerts
WHERE alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
    AND severity = 'critical'
ORDER BY alert_timestamp DESC;

-- Alert frequency by type (last 24 hours)
SELECT 
    alert_type,
    severity,
    COUNT(*) AS alert_count,
    MIN(alert_timestamp) AS first_occurrence,
    MAX(alert_timestamp) AS last_occurrence
FROM observability_alerts
WHERE alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
GROUP BY alert_type, severity
ORDER BY alert_count DESC;

-- Alerting layers (which layer is noisiest?)
SELECT 
    layer,
    severity,
    COUNT(*) AS alert_count
FROM observability_alerts
WHERE alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
GROUP BY layer, severity
ORDER BY layer, severity;
```

---

## 5. Dashboard & Visualization

### Grafana Dashboard Structure

#### Panel 1: Pipeline Health Overview

```sql
-- Stat panels (single values)
SELECT 
    CASE 
        WHEN AVG(avg_processing_lag_sec) < 60 AND AVG(data_freshness_sec) < 120 THEN 'Healthy'
        WHEN AVG(avg_processing_lag_sec) < 300 AND AVG(data_freshness_sec) < 600 THEN 'Degraded'
        ELSE 'Unhealthy'
    END AS pipeline_status,
    AVG(throughput_rec_per_sec) AS avg_throughput,
    AVG(quality_pass_rate_pct) AS avg_quality
FROM observability_metrics
WHERE metric_timestamp >= NOW() - INTERVAL 5 MINUTES;
```

#### Panel 2: Throughput (Line Chart)

```sql
SELECT 
    metric_timestamp AS time,
    layer,
    throughput_rec_per_sec
FROM observability_metrics
WHERE $__timeFilter(metric_timestamp)
ORDER BY time;
```

#### Panel 3: Processing Lag (Line Chart)

```sql
SELECT 
    metric_timestamp AS time,
    layer,
    avg_processing_lag_sec
FROM observability_metrics
WHERE $__timeFilter(metric_timestamp)
ORDER BY time;
```

#### Panel 4: Data Freshness (Line Chart with Threshold)

```sql
SELECT 
    metric_timestamp AS time,
    layer,
    data_freshness_sec,
    120 AS warning_threshold,  -- Horizontal line
    600 AS critical_threshold  -- Horizontal line
FROM observability_metrics
WHERE $__timeFilter(metric_timestamp)
ORDER BY time;
```

#### Panel 5: Quality Pass Rate (Gauge)

```sql
SELECT 
    layer,
    AVG(quality_pass_rate_pct) AS pass_rate
FROM observability_metrics
WHERE metric_timestamp >= NOW() - INTERVAL 15 MINUTES
GROUP BY layer;
```

#### Panel 6: Active Alerts (Table)

```sql
SELECT 
    alert_timestamp,
    layer,
    alert_type,
    severity,
    message
FROM observability_alerts
WHERE $__timeFilter(alert_timestamp)
ORDER BY alert_timestamp DESC
LIMIT 50;
```

#### Panel 7: Volume Trend (Area Chart)

```sql
SELECT 
    metric_timestamp AS time,
    layer,
    record_count
FROM observability_metrics
WHERE $__timeFilter(metric_timestamp)
ORDER BY time;
```

#### Panel 8: Error Rate (Line Chart)

```sql
SELECT 
    metric_timestamp AS time,
    layer,
    error_rate_pct
FROM observability_metrics
WHERE $__timeFilter(metric_timestamp)
ORDER BY time;
```

---

## 6. Integration Examples

### Bronze Layer Integration

```python
from observability import PipelineObserver, AlertThresholds

# Initialize observer
thresholds = AlertThresholds(
    throughput_min_warning=500.0,
    throughput_min_critical=100.0
)

observer = PipelineObserver(
    layer="bronze",
    app_name="bronze-ingestion",
    alert_thresholds=thresholds
)

# Process batch with monitoring
def process_bronze_batch(batch_df: DataFrame, batch_id: int):
    with observer.track_batch(batch_id):
        # Transform and write
        transformed_df = transform_to_bronze(batch_df)
        transformed_df.write.format("delta").mode("append").save("/data/bronze_api_usage")
    
    # Monitor (collect metrics, evaluate alerts)
    observer.monitor_batch(spark, batch_df, batch_id)

# Start stream
bronze_stream.writeStream \
    .foreachBatch(process_bronze_batch) \
    .option("checkpointLocation", "/checkpoints/bronze_observability") \
    .start()
```

### Silver Layer Integration

```python
from observability import PipelineObserver, AlertThresholds

# Initialize observer with stricter quality thresholds
thresholds = AlertThresholds(
    quality_pass_rate_warning=98.0,
    quality_pass_rate_critical=95.0
)

observer = PipelineObserver(
    layer="silver",
    app_name="silver-processing",
    alert_thresholds=thresholds
)

def process_silver_batch(batch_df: DataFrame, batch_id: int):
    with observer.track_batch(batch_id):
        # Deduplication + quality validation
        deduped_df = deduplicator.streaming_dedup(batch_df)
        validated_df = quality_checker.validate_and_flag(deduped_df)
        valid_df = handle_failed_records(validated_df, "/data/quarantine_silver", "silver")
        
        # Write to Silver
        valid_df.write.format("delta").mode("append").save("/data/silver_api_usage")
    
    # Monitor
    observer.monitor_batch(spark, validated_df, batch_id)

silver_stream.writeStream \
    .foreachBatch(process_silver_batch) \
    .option("checkpointLocation", "/checkpoints/silver_observability") \
    .start()
```

### Gold Layer Integration

```python
from observability import PipelineObserver, AlertThresholds

# Initialize observer with focus on freshness
thresholds = AlertThresholds(
    data_freshness_warning=60,    # 1 minute (strict for enforcement)
    data_freshness_critical=300   # 5 minutes
)

observer = PipelineObserver(
    layer="gold",
    app_name="gold-aggregation",
    alert_thresholds=thresholds
)

def process_gold_batch(batch_df: DataFrame, batch_id: int):
    with observer.track_batch(batch_id):
        # Aggregate and enforce
        agg_1min = aggregate_1_minute(batch_df)
        alerts = generate_enforcement_alerts(agg_1min)
        
        # Write aggregations and alerts
        agg_1min.write.format("delta").mode("append").save("/data/gold_tenant_usage_1min")
        alerts.write.format("delta").mode("append").save("/data/enforcement_alerts")
    
    # Monitor
    observer.monitor_batch(spark, agg_1min, batch_id)

gold_stream.writeStream \
    .foreachBatch(process_gold_batch) \
    .option("checkpointLocation", "/checkpoints/gold_observability") \
    .start()
```

---

## 7. Operational Runbooks

### Alert: Processing Lag Critical

**Symptoms:**
- Avg processing lag >300s
- Events processed significantly later than occurrence time

**Investigation:**
1. Check Spark UI for slow stages
2. Review executor logs for errors
3. Check Kafka broker health
4. Verify network connectivity

**Resolution:**
```bash
# Scale up Spark executors
spark-submit --num-executors 20 --executor-memory 8G ...

# Restart streaming job if stuck
spark-submit --kill <application-id>
spark-submit streaming/bronze_ingestion.py

# Check for data skew
spark-sql -e "
SELECT kafka_partition, COUNT(*) 
FROM bronze_api_usage 
WHERE ingestion_date = CURRENT_DATE 
GROUP BY kafka_partition
"
```

---

### Alert: Data Freshness Critical

**Symptoms:**
- Latest event timestamp >10 minutes old
- Pipeline appears stalled

**Investigation:**
1. Check if new data arriving from Kafka
2. Verify streaming query is running
3. Check for checkpoint corruption

**Resolution:**
```bash
# Check Kafka topic has new messages
kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic api-usage-events.v1 \
    --max-messages 10

# Verify streaming query status
spark-sql -e "SHOW STREAMS"

# If checkpoint corrupted, reset (DATA LOSS RISK)
rm -rf /checkpoints/bronze_processing/*
# Restart from earliest offset or specific timestamp
```

---

### Alert: Quality Pass Rate Critical

**Symptoms:**
- <80% of records passing quality validation
- High quarantine table growth

**Investigation:**
1. Query quarantine table for error patterns
2. Check for upstream schema changes
3. Review quality validation rules

**Resolution:**
```sql
-- Find top error types
SELECT 
    error_type,
    COUNT(*) AS error_count
FROM (
    SELECT explode(quality_check_errors) AS (category, error_type)
    FROM quarantine_silver
    WHERE quarantine_date = CURRENT_DATE
)
GROUP BY error_type
ORDER BY error_count DESC
LIMIT 10;

-- If upstream schema changed, update validation
-- Update data_quality_checks.py VALID_ENUMS or VALUE_CONSTRAINTS
-- Reprocess quarantined records after fix
```

---

## 8. Best Practices

### 1. Layer-Specific Thresholds

Don't use same thresholds for all layers:

```python
# Bronze: High throughput, lenient quality
bronze_thresholds = AlertThresholds(
    throughput_min_warning=1000.0,
    quality_pass_rate_warning=90.0
)

# Silver: Moderate throughput, strict quality
silver_thresholds = AlertThresholds(
    throughput_min_warning=500.0,
    quality_pass_rate_warning=98.0
)

# Gold: Lower throughput (aggregations), focus on freshness
gold_thresholds = AlertThresholds(
    throughput_min_warning=100.0,
    data_freshness_warning=60
)
```

### 2. Alert Fatigue Prevention

```python
# Implement alert suppression (don't alert on same issue repeatedly)
from datetime import datetime, timedelta

def should_send_alert(alert_type: str, layer: str) -> bool:
    # Query last alert of this type
    last_alert = spark.sql(f"""
        SELECT MAX(alert_timestamp) AS last_alert
        FROM observability_alerts
        WHERE alert_type = '{alert_type}'
            AND layer = '{layer}'
    """).first()["last_alert"]
    
    # Only alert if >1 hour since last alert
    if last_alert and (datetime.now() - last_alert) < timedelta(hours=1):
        return False
    
    return True
```

### 3. Baseline Metrics for Anomaly Detection

```sql
-- Establish baseline metrics (median throughput, lag)
CREATE TABLE baseline_metrics AS
SELECT 
    layer,
    PERCENTILE_APPROX(throughput_rec_per_sec, 0.5) AS median_throughput,
    PERCENTILE_APPROX(avg_processing_lag_sec, 0.5) AS median_lag,
    PERCENTILE_APPROX(batch_duration_sec, 0.5) AS median_batch_duration
FROM observability_metrics
WHERE metric_timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
GROUP BY layer;

-- Alert if current metrics deviate >3x from baseline
```

### 4. Metric Retention Policy

```sql
-- Delete old metrics after 90 days
DELETE FROM observability_metrics
WHERE metric_timestamp < CURRENT_TIMESTAMP - INTERVAL 90 DAYS;

-- Delete old alerts after 30 days
DELETE FROM observability_alerts
WHERE alert_timestamp < CURRENT_TIMESTAMP - INTERVAL 30 DAYS;

-- Vacuum to reclaim storage
VACUUM observability_metrics RETAIN 168 HOURS;
VACUUM observability_alerts RETAIN 168 HOURS;
```

---

## Summary

| Aspect | Key Metrics | Alert Threshold | Purpose |
|--------|-------------|-----------------|---------|
| **Lag** | Processing lag, Kafka lag | Warning: 60s, Critical: 300s | Ensure real-time processing |
| **Freshness** | Max event age | Warning: 120s, Critical: 600s | Detect pipeline stalls |
| **Throughput** | Records/second | Warning: <100, Critical: <10 | Capacity planning, bottleneck detection |
| **Quality** | Pass rate, error rate | Warning: 95%, Critical: 80% | Data integrity validation |
| **Duration** | Batch processing time | Warning: 30s, Critical: 120s | Performance optimization |

**Key Principle:**  
> "Monitor early, alert smartly, respond quickly. Observability prevents outages before they happen."
