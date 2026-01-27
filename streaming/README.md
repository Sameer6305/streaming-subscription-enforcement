# Bronze Ingestion Pipeline

Spark Structured Streaming job that ingests API usage events from Kafka to Delta Lake.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Kafka     │────▶│  Spark Streaming │────▶│  Delta Lake     │
│  (Source)   │     │  (Bronze Job)    │     │  (Bronze Table) │
└─────────────┘     └────────┬─────────┘     └─────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Checkpoint     │
                    │  (S3/HDFS)      │
                    └─────────────────┘
```

## Quick Start

```bash
# Submit to Spark cluster
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    bronze_ingestion.py \
    --kafka-brokers kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --kafka-topic api-usage-events.v1 \
    --checkpoint-path s3://data-lake/checkpoints/bronze/api-usage-events \
    --output-path s3://data-lake/bronze/api_usage_events \
    --dlq-path s3://data-lake/bronze/api_usage_events_dlq \
    --trigger-interval "1 minute" \
    --with-metrics
```

## Command Line Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--kafka-brokers` | Yes | - | Comma-separated Kafka brokers |
| `--kafka-topic` | No | `api-usage-events.v1` | Kafka topic to consume |
| `--checkpoint-path` | Yes | - | Checkpoint location (S3/HDFS) |
| `--output-path` | Yes | - | Delta Lake output path |
| `--dlq-path` | No | None | Dead Letter Queue for invalid records |
| `--trigger-interval` | No | `1 minute` | Micro-batch interval |
| `--starting-offsets` | No | `earliest` | Where to start: earliest/latest/JSON |
| `--max-offsets-per-trigger` | No | None | Backpressure control |
| `--with-metrics` | No | False | Enable per-batch logging |

## Fault Tolerance Guarantees

### Exactly-Once Semantics

The pipeline guarantees exactly-once processing through:

1. **Kafka Offsets**: Tracked in checkpoint, not committed to Kafka
2. **Checkpoint**: Stores progress, can recover from any failure
3. **Delta Lake Transactions**: Atomic writes, no partial commits

### Recovery Scenarios

| Failure | Recovery |
|---------|----------|
| Driver crash | Restart from checkpoint, resume at last offset |
| Executor crash | Task retry, idempotent writes |
| Kafka unavailable | Retry with backoff, resume when available |
| Delta write failure | Transaction rollback, retry micro-batch |

### Checkpoint Contents

```
checkpoint/
├── commits/           # Completed micro-batch IDs
├── offsets/           # Kafka offsets per micro-batch
├── sources/           # Source metadata
└── state/             # Streaming state (if any)
```

**CRITICAL**: Never delete checkpoint while pipeline is running!

## Output Schema

### Bronze Table Columns

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING | Unique event identifier |
| `event_version` | STRING | Schema version |
| `event_timestamp` | STRING | When event occurred |
| `tenant_id` | STRING | Tenant identifier |
| `subscription` | STRUCT | Subscription details |
| `identity` | STRUCT | Authentication info |
| `request` | STRUCT | Request details |
| `response` | STRUCT | Response details |
| `client` | STRUCT | Client info |
| `geo` | STRUCT | Geolocation |
| `rate_limiting` | STRUCT | Rate limit state |
| `abuse_signals` | STRUCT | Abuse detection signals |
| `gateway` | STRUCT | Gateway metadata |
| `billing` | STRUCT | Billing info |
| `_raw_value` | STRING | Original JSON (preserved) |
| `_is_valid` | BOOLEAN | JSON parse success |
| `_kafka_topic` | STRING | Source topic |
| `_kafka_partition` | INT | Kafka partition |
| `_kafka_offset` | LONG | Kafka offset |
| `_kafka_timestamp` | TIMESTAMP | Kafka message time |
| `_ingested_at` | TIMESTAMP | Pipeline ingestion time |
| `_row_hash` | LONG | Hash for deduplication |
| `_event_date` | DATE | Partition column |
| `_event_hour` | INT | Partition column |

### Partitioning Strategy

```
s3://data-lake/bronze/api_usage_events/
├── _event_date=2026-01-27/
│   ├── _event_hour=0/
│   ├── _event_hour=1/
│   └── ...
├── _event_date=2026-01-28/
└── ...
```

**Why date + hour?**
- Efficient time-range queries
- Manageable partition sizes
- Good for incremental processing

## Monitoring

### Spark UI Metrics

Access at `http://<driver>:4040/streaming/`

Key metrics:
- Input Rate (records/sec)
- Processing Time (ms)
- Batch Duration (ms)
- Offset Lag

### Custom Logging

With `--with-metrics`:

```
2026-01-27 10:00:00 - Batch 42: total=15000, valid=14985, invalid=15
2026-01-27 10:00:00 - Batch 42 partition distribution: [(0, 234), (1, 241), ...]
```

### Alerts to Configure

```yaml
alerts:
  - name: HighProcessingTime
    condition: batch_duration_ms > 60000  # > 1 minute
    action: Scale up executors

  - name: InvalidRecordSpike
    condition: invalid_count / total_count > 0.01  # > 1%
    action: Investigate upstream

  - name: CheckpointFailure
    condition: checkpoint_write_failed
    action: Page on-call (critical)
```

## Operations

### First-Time Setup

1. Create Delta table (optional, auto-created on first write):
```sql
CREATE TABLE IF NOT EXISTS bronze.api_usage_events (
    event_id STRING,
    ...
) USING DELTA
PARTITIONED BY (_event_date, _event_hour)
LOCATION 's3://data-lake/bronze/api_usage_events';
```

2. Start pipeline with `--starting-offsets earliest`

### Restart from Checkpoint

```bash
# Same command - checkpoint is automatically used
spark-submit ... --checkpoint-path s3://existing/checkpoint
```

### Restart from Scratch

```bash
# 1. Stop the pipeline
# 2. Delete checkpoint
aws s3 rm --recursive s3://data-lake/checkpoints/bronze/api-usage-events

# 3. Restart with desired offset
spark-submit ... --starting-offsets earliest
```

### Backfill Specific Time Range

```bash
# Use JSON offset specification
spark-submit ... \
    --starting-offsets '{"api-usage-events.v1":{"0":1000000,"1":1000000,...}}'
```

### Graceful Shutdown

```bash
# Option 1: Spark REST API
curl -X POST http://<driver>:4040/api/v1/applications/<app-id>/stop

# Option 2: Send SIGTERM (handled in code)
kill -TERM <driver-pid>
```

## Troubleshooting

### Pipeline Stuck / No Progress

1. Check Kafka connectivity:
   ```bash
   kafka-console-consumer --bootstrap-server kafka:9092 --topic api-usage-events.v1 --max-messages 1
   ```

2. Check checkpoint permissions:
   ```bash
   aws s3 ls s3://data-lake/checkpoints/bronze/
   ```

3. Check Spark UI for failed tasks

### High Latency

1. Increase parallelism:
   ```bash
   --conf spark.sql.shuffle.partitions=128
   ```

2. Add more executors:
   ```bash
   --num-executors 20 --executor-cores 4
   ```

3. Reduce trigger interval (more frequent, smaller batches):
   ```bash
   --trigger-interval "10 seconds"
   ```

### OOM Errors

1. Increase executor memory:
   ```bash
   --executor-memory 8g
   ```

2. Enable backpressure:
   ```bash
   --max-offsets-per-trigger 100000
   ```

### Schema Mismatch

If JSON schema changes, invalid records go to DLQ. To handle:

1. Fix upstream producer
2. Update schema in `get_event_schema()`
3. Reprocess DLQ records
