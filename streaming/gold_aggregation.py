"""
Gold Layer Aggregation: Silver → Gold

Creates aggregated tables for entitlement enforcement, billing, and analytics.
Implements windows and schemas defined in DESIGN_ADDENDUM.md.

Enforcement Windows (per DESIGN_ADDENDUM.md):
- 1 minute: Rate limit enforcement (primary)
- 1 hour: Quota tracking (secondary)  
- 1 month: Billing aggregation (tertiary)

Grouping Key: tenant_id (per design - enforcement is per-tenant)

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.1.0 \
        gold_aggregation.py \
        --silver-path s3://data-lake/silver/api_usage_events \
        --gold-path s3://data-lake/gold \
        --checkpoint-path s3://data-lake/checkpoints/gold
"""

import argparse
import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    sum as _sum,
    count,
    avg,
    max as _max,
    min as _min,
    when,
    window,
    current_timestamp,
    date_format,
    to_date,
    expr,
    percentile_approx,
    countDistinct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
)

from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("GoldAggregation")


# =============================================================================
# CONFIGURATION - ALIGNED WITH DESIGN_ADDENDUM.md
# =============================================================================

# Watermark for streaming aggregations
# Per DESIGN_ADDENDUM.md Section 3: "Watermark delay: 1 hour"
WATERMARK_DELAY = "1 hour"

# Time windows per DESIGN_ADDENDUM.md Section 2:
# - Primary (Rate Limit): 1 minute
# - Secondary (Quota): 1 hour
# - Tertiary (Billing): 1 month
WINDOW_1_MINUTE = "1 minute"
WINDOW_1_HOUR = "1 hour"

# Grouping key per DESIGN_ADDENDUM.md Section 5:
# "Rate limiting: tenant_id - Enforcement is per-tenant"
PRIMARY_GROUP_KEY = "tenant_id"

# Plan tier limits per DESIGN_ADDENDUM.md Section 2:
PLAN_LIMITS = {
    "free": {"requests_per_minute": 60, "requests_per_hour": 1000},
    "starter": {"requests_per_minute": 300, "requests_per_hour": 10000},
    "professional": {"requests_per_minute": 1000, "requests_per_hour": 50000},
    "enterprise": {"requests_per_minute": 5000, "requests_per_hour": 200000},
}


# =============================================================================
# GOLD TABLE SCHEMAS - FROM DESIGN_ADDENDUM.md SECTION 6
# =============================================================================

def get_gold_1min_schema() -> StructType:
    """
    Schema for gold.tenant_usage_1min
    
    Per DESIGN_ADDENDUM.md Section 6:
    Purpose: Real-time rate limit enforcement and alerting
    """
    return StructType([
        StructField("tenant_id", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("request_count", LongType(), nullable=False),
        StructField("billing_units", DoubleType(), nullable=True),
        StructField("error_count", LongType(), nullable=True),
        StructField("avg_latency_ms", DoubleType(), nullable=True),
        StructField("p99_latency_ms", DoubleType(), nullable=True),
        StructField("unique_endpoints", LongType(), nullable=True),
        StructField("unique_api_keys", LongType(), nullable=True),
        StructField("rate_limited_count", LongType(), nullable=True),
        StructField("plan_tier", StringType(), nullable=True),
        StructField("limit_requests_per_min", LongType(), nullable=True),
        StructField("usage_percent", DoubleType(), nullable=True),
        StructField("is_over_limit", BooleanType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
    ])


def get_gold_hourly_schema() -> StructType:
    """
    Schema for gold.tenant_usage_hourly
    
    Per DESIGN_ADDENDUM.md Section 6:
    Purpose: Quota tracking, trend analysis, anomaly detection
    """
    return StructType([
        StructField("tenant_id", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("request_count", LongType(), nullable=False),
        StructField("billing_units", DoubleType(), nullable=True),
        StructField("avg_requests_per_min", DoubleType(), nullable=True),
        StructField("peak_requests_per_min", LongType(), nullable=True),
        StructField("quota_used_percent", DoubleType(), nullable=True),
        StructField("anomaly_score", DoubleType(), nullable=True),
        StructField("plan_tier", StringType(), nullable=True),
        StructField("processed_at", TimestampType(), nullable=False),
    ])


# =============================================================================
# SPARK SESSION
# =============================================================================

def create_spark_session(app_name: str = "GoldAggregation") -> SparkSession:
    """Create Spark session with Delta Lake support."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )


# =============================================================================
# READ SILVER STREAM
# =============================================================================

def read_silver_stream(
    spark: SparkSession,
    silver_path: str,
    max_files_per_trigger: int = 100
) -> DataFrame:
    """
    Read Silver table as streaming source.
    
    Per DESIGN_ADDENDUM.md Section 4:
    - Silver is deduplicated on event_id
    - Gold relies on Silver for deduplication
    """
    logger.info(f"Reading Silver stream from: {silver_path}")
    
    return (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(silver_path)
    )


# =============================================================================
# 1-MINUTE AGGREGATION (Rate Limit Enforcement)
# =============================================================================

def aggregate_1_minute(df: DataFrame) -> DataFrame:
    """
    Create 1-minute tumbling window aggregations for rate limit enforcement.
    
    Per DESIGN_ADDENDUM.md Section 5:
    - Window Type: Tumbling (non-overlapping)
    - Rationale: "Simple to reason about, matches customer expectation"
    
    Per DESIGN_ADDENDUM.md Section 2:
    - Primary enforcement window: 1 minute
    - Grouping: tenant_id
    """
    logger.info("Creating 1-minute aggregations for rate limit enforcement")
    
    return (
        df
        # Apply watermark per DESIGN_ADDENDUM.md Section 3
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        
        # Tumbling window per DESIGN_ADDENDUM.md Section 5
        .groupBy(
            col("tenant_id"),
            window(col("event_timestamp"), WINDOW_1_MINUTE)
        )
        .agg(
            # Core metrics
            count("*").alias("request_count"),
            _sum("billing_units").alias("billing_units"),
            
            # Error tracking
            _sum(when(col("status_code") >= 400, 1).otherwise(0)).alias("error_count"),
            
            # Latency metrics
            avg("latency_ms").alias("avg_latency_ms"),
            percentile_approx("latency_ms", 0.99).alias("p99_latency_ms"),
            
            # Cardinality metrics (for abuse detection)
            countDistinct("endpoint_id").alias("unique_endpoints"),
            countDistinct("api_key_id").alias("unique_api_keys"),
            
            # Rate limiting metrics
            _sum(when(col("was_rate_limited") == True, 1).otherwise(0)).alias("rate_limited_count"),
            
            # Plan tier (take first, should be consistent within window)
            expr("first(plan_tier)").alias("plan_tier"),
        )
        # Flatten window struct
        .select(
            col("tenant_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("request_count"),
            col("billing_units"),
            col("error_count"),
            col("avg_latency_ms"),
            col("p99_latency_ms"),
            col("unique_endpoints"),
            col("unique_api_keys"),
            col("rate_limited_count"),
            col("plan_tier"),
        )
        # Add limit based on plan tier (per DESIGN_ADDENDUM.md Section 2)
        .withColumn(
            "limit_requests_per_min",
            when(col("plan_tier") == "free", lit(60))
            .when(col("plan_tier") == "starter", lit(300))
            .when(col("plan_tier") == "professional", lit(1000))
            .when(col("plan_tier") == "enterprise", lit(5000))
            .otherwise(lit(60))  # Default to free tier
        )
        # Calculate usage percentage
        .withColumn(
            "usage_percent",
            (col("request_count") / col("limit_requests_per_min")) * 100
        )
        # Determine over-limit status (per DESIGN_ADDENDUM.md Section 2)
        # "condition: request_count_1min > plan.requests_per_minute"
        .withColumn(
            "is_over_limit",
            col("request_count") > col("limit_requests_per_min")
        )
        # Add processing timestamp
        .withColumn("processed_at", current_timestamp())
    )


# =============================================================================
# HOURLY AGGREGATION (Quota Tracking)
# =============================================================================

def aggregate_hourly(df: DataFrame) -> DataFrame:
    """
    Create 1-hour tumbling window aggregations for quota tracking.
    
    Per DESIGN_ADDENDUM.md Section 2:
    - Secondary (Quota) window: 1 hour
    - Purpose: Sustained usage tracking
    """
    logger.info("Creating hourly aggregations for quota tracking")
    
    return (
        df
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            col("tenant_id"),
            window(col("event_timestamp"), WINDOW_1_HOUR)
        )
        .agg(
            count("*").alias("request_count"),
            _sum("billing_units").alias("billing_units"),
            
            # Derived: average requests per minute within the hour
            (count("*") / 60).alias("avg_requests_per_min"),
            
            # Plan tier
            expr("first(plan_tier)").alias("plan_tier"),
        )
        .select(
            col("tenant_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("request_count"),
            col("billing_units"),
            col("avg_requests_per_min"),
            col("plan_tier"),
        )
        # Add hourly limit based on plan tier
        .withColumn(
            "limit_requests_per_hour",
            when(col("plan_tier") == "free", lit(1000))
            .when(col("plan_tier") == "starter", lit(10000))
            .when(col("plan_tier") == "professional", lit(50000))
            .when(col("plan_tier") == "enterprise", lit(200000))
            .otherwise(lit(1000))
        )
        # Calculate quota usage
        .withColumn(
            "quota_used_percent",
            (col("request_count") / col("limit_requests_per_hour")) * 100
        )
        # Placeholder for anomaly score (computed by separate ML pipeline)
        .withColumn("anomaly_score", lit(None).cast(DoubleType()))
        # Peak requests per minute would come from 1-min aggregates
        # For now, estimate from hourly count
        .withColumn("peak_requests_per_min", lit(None).cast(LongType()))
        .withColumn("processed_at", current_timestamp())
        .drop("limit_requests_per_hour")
    )


# =============================================================================
# WRITE TO GOLD TABLES
# =============================================================================

def write_gold_stream(
    df: DataFrame,
    gold_path: str,
    checkpoint_path: str,
    table_name: str,
    trigger_interval: str = "1 minute"
):
    """
    Write aggregated stream to Gold Delta table.
    
    Per DESIGN_ADDENDUM.md Section 6:
    - Partitioned by: date(window_start)
    """
    output_path = f"{gold_path}/{table_name}"
    
    logger.info(f"Writing Gold stream to: {output_path}")
    
    def process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            logger.info(f"[{table_name}] Batch {batch_id}: Empty, skipping")
            return
        
        count = batch_df.count()
        logger.info(f"[{table_name}] Batch {batch_id}: Processing {count} aggregates")
        
        # Add partition column
        batch_with_partition = batch_df.withColumn(
            "_partition_date",
            to_date(col("window_start"))
        )
        
        # Write to Delta
        (
            batch_with_partition.write
            .format("delta")
            .mode("append")
            .partitionBy("_partition_date")
            .save(output_path)
        )
        
        # Log over-limit events for alerting
        if table_name == "tenant_usage_1min":
            over_limit_count = batch_df.filter(col("is_over_limit") == True).count()
            if over_limit_count > 0:
                logger.warning(f"[{table_name}] Batch {batch_id}: {over_limit_count} tenants over limit!")
    
    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{checkpoint_path}/{table_name}")
        .trigger(processingTime=trigger_interval)
        .start()
    )
    
    return query


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run_gold_pipeline(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    checkpoint_base: str,
    trigger_interval: str = "1 minute"
):
    """
    Run the Gold aggregation pipeline.
    
    Pipeline Architecture (per DESIGN_ADDENDUM.md):
    
    Silver Table (deduplicated)
           │
           ├──────────────────────────────────┐
           │                                   │
           ▼                                   ▼
    ┌──────────────────┐              ┌──────────────────┐
    │ 1-Min Aggregation│              │ Hourly Aggregation│
    │ (Rate Limiting)  │              │ (Quota Tracking)  │
    └────────┬─────────┘              └────────┬──────────┘
             │                                  │
             ├──────────────┐                   │
             ▼              ▼                   ▼
    ┌──────────────┐ ┌─────────────┐   ┌───────────────────┐
    │tenant_usage  │ │ Enforcement │   │ tenant_usage_hourly│
    │_1min         │ │ Alerts      │   └───────────────────┘
    └──────────────┘ └──────┬──────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │ Redis/PagerDuty │
                   └─────────────────┘
    """
    
    logger.info("=" * 60)
    logger.info("Starting Gold Aggregation Pipeline")
    logger.info("=" * 60)
    logger.info(f"Silver path: {silver_path}")
    logger.info(f"Gold path: {gold_path}")
    logger.info(f"Watermark: {WATERMARK_DELAY}")
    logger.info(f"Primary window: {WINDOW_1_MINUTE}")
    logger.info(f"Secondary window: {WINDOW_1_HOUR}")
    logger.info("=" * 60)
    
    # Read Silver stream
    silver_df = read_silver_stream(spark, silver_path)
    
    # Create 1-minute aggregations
    agg_1min = aggregate_1_minute(silver_df)
    query_1min = write_gold_stream(
        agg_1min,
        gold_path,
        checkpoint_base,
        "tenant_usage_1min",
        trigger_interval
    )
    
    # Generate enforcement alerts from 1-minute aggregations
    silver_df_alerts = read_silver_stream(spark, silver_path)
    agg_1min_alerts = aggregate_1_minute(silver_df_alerts)
    alerts_df = generate_enforcement_alerts(agg_1min_alerts)
    query_alerts = write_alerts_stream(
        alerts_df,
        f"{gold_path}/enforcement_alerts",
        f"{checkpoint_base}/enforcement_alerts",
        trigger_interval
    )
    
    # Create hourly aggregations
    # Note: Need separate read for separate watermark tracking
    silver_df_hourly = read_silver_stream(spark, silver_path)
    agg_hourly = aggregate_hourly(silver_df_hourly)
    query_hourly = write_gold_stream(
        agg_hourly,
        gold_path,
        checkpoint_base,
        "tenant_usage_hourly",
        trigger_interval
    )
    
    return query_1min, query_hourly, query_alerts


# =============================================================================
# ENFORCEMENT ALERTS - OUTPUT SCHEMA FOR DOWNSTREAM SYSTEMS
# =============================================================================

def get_enforcement_alert_schema() -> StructType:
    """
    Schema for enforcement alerts emitted to downstream systems.
    
    Consumers:
    - Redis: Update rate limit counters
    - PagerDuty/OpsGenie: Alert on-call for abuse
    - Kafka: Publish to enforcement-alerts topic
    - Gateway: Sync block/throttle decisions
    
    NOT for UI/dashboards - purely for enforcement automation.
    """
    return StructType([
        # Alert identification
        StructField("alert_id", StringType(), nullable=False),
        StructField("alert_type", StringType(), nullable=False),  # over_limit, approaching_limit, abuse_detected
        StructField("severity", StringType(), nullable=False),    # critical, warning, info
        
        # Tenant context
        StructField("tenant_id", StringType(), nullable=False),
        StructField("plan_tier", StringType(), nullable=True),
        
        # Time context
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("detected_at", TimestampType(), nullable=False),
        
        # Enforcement details
        StructField("metric_name", StringType(), nullable=False),  # request_count, billing_units
        StructField("current_value", LongType(), nullable=False),
        StructField("limit_value", LongType(), nullable=False),
        StructField("usage_percent", DoubleType(), nullable=False),
        StructField("overage_amount", LongType(), nullable=True),
        
        # Recommended action
        StructField("recommended_action", StringType(), nullable=False),  # throttle, block, alert_only
        StructField("action_params", StringType(), nullable=True),        # JSON with action details
    ])


# Alert type constants
class AlertType:
    OVER_LIMIT = "over_limit"
    APPROACHING_LIMIT = "approaching_limit"
    SUSTAINED_HIGH_USAGE = "sustained_high_usage"
    ABUSE_DETECTED = "abuse_detected"


class AlertSeverity:
    CRITICAL = "critical"   # Immediate action required
    WARNING = "warning"     # Attention needed soon
    INFO = "info"           # Informational only


class RecommendedAction:
    BLOCK = "block"         # Return 429 for all requests
    THROTTLE = "throttle"   # Slow down requests
    ALERT_ONLY = "alert_only"  # Just notify, no enforcement


def generate_enforcement_alerts(usage_df: DataFrame) -> DataFrame:
    """
    Generate enforcement alerts from usage aggregations.
    
    Alert Triggers (per DESIGN_ADDENDUM.md Section 2):
    
    1. OVER_LIMIT (Critical):
       - request_count > plan.requests_per_minute
       - Action: Block (429)
    
    2. APPROACHING_LIMIT (Warning):
       - usage_percent >= 80%
       - Action: Alert only (add warning header)
    
    3. SUSTAINED_HIGH_USAGE (Warning):
       - usage_percent >= 90% for 3+ consecutive windows
       - Action: Throttle
    
    4. ABUSE_DETECTED (Critical):
       - unique_endpoints > 50 (enumeration)
       - error_rate > 50%
       - Action: Block + Security alert
    """
    from pyspark.sql.functions import when, lit, to_json, struct
    
    # Generate unique alert IDs
    alerts = (
        usage_df
        .withColumn("alert_id", expr("uuid()"))
        .withColumn("detected_at", current_timestamp())
        .withColumn("metric_name", lit("request_count"))
        .withColumn("current_value", col("request_count"))
        .withColumn("limit_value", col("limit_requests_per_min"))
        .withColumn("overage_amount", 
            when(col("is_over_limit"), col("request_count") - col("limit_requests_per_min"))
            .otherwise(lit(0))
        )
    )
    
    # Determine alert type and severity
    alerts = alerts.withColumn(
        "alert_type",
        when(col("is_over_limit"), lit(AlertType.OVER_LIMIT))
        .when(col("usage_percent") >= 80, lit(AlertType.APPROACHING_LIMIT))
        .otherwise(lit(None))
    ).withColumn(
        "severity",
        when(col("is_over_limit"), lit(AlertSeverity.CRITICAL))
        .when(col("usage_percent") >= 90, lit(AlertSeverity.WARNING))
        .when(col("usage_percent") >= 80, lit(AlertSeverity.INFO))
        .otherwise(lit(None))
    ).withColumn(
        "recommended_action",
        when(col("is_over_limit"), lit(RecommendedAction.BLOCK))
        .when(col("usage_percent") >= 90, lit(RecommendedAction.THROTTLE))
        .otherwise(lit(RecommendedAction.ALERT_ONLY))
    ).withColumn(
        "action_params",
        when(col("is_over_limit"), 
            to_json(struct(
                lit(429).alias("status_code"),
                lit(60).alias("retry_after_seconds"),
                col("window_end").alias("reset_at")
            ))
        ).otherwise(lit(None))
    )
    
    # Filter to only rows that need alerts (>= 80% usage)
    alerts = alerts.filter(col("alert_type").isNotNull())
    
    # Select final schema
    return alerts.select(
        "alert_id",
        "alert_type",
        "severity",
        "tenant_id",
        "plan_tier",
        "window_start",
        "window_end",
        "detected_at",
        "metric_name",
        "current_value",
        "limit_value",
        "usage_percent",
        "overage_amount",
        "recommended_action",
        "action_params",
    )


def write_alerts_stream(
    df: DataFrame,
    alerts_path: str,
    checkpoint_path: str,
    trigger_interval: str = "1 minute"
):
    """
    Write enforcement alerts to Delta table.
    
    This table is consumed by:
    - Redis sync job (update counters)
    - Alert dispatcher (send to PagerDuty)
    - Enforcement API (gateway queries for blocked tenants)
    """
    logger.info(f"Writing alerts stream to: {alerts_path}")
    
    def process_alerts_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return
        
        alert_count = batch_df.count()
        critical_count = batch_df.filter(col("severity") == "critical").count()
        
        logger.info(f"[Alerts] Batch {batch_id}: {alert_count} alerts, {critical_count} critical")
        
        # Log critical alerts for immediate visibility
        if critical_count > 0:
            critical_alerts = batch_df.filter(col("severity") == "critical").collect()
            for alert in critical_alerts:
                logger.critical(
                    f"ENFORCEMENT ALERT: tenant={alert.tenant_id} "
                    f"type={alert.alert_type} usage={alert.usage_percent:.1f}% "
                    f"action={alert.recommended_action}"
                )
        
        # Write to Delta
        (
            batch_df
            .withColumn("_alert_date", to_date(col("detected_at")))
            .write
            .format("delta")
            .mode("append")
            .partitionBy("_alert_date", "severity")
            .save(alerts_path)
        )
    
    query = (
        df.writeStream
        .foreachBatch(process_alerts_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
    
    return query


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Gold layer aggregation")
    
    parser.add_argument("--silver-path", required=True, help="Silver table path")
    parser.add_argument("--gold-path", required=True, help="Gold tables base path")
    parser.add_argument("--checkpoint-path", required=True, help="Checkpoint base path")
    parser.add_argument("--trigger-interval", default="1 minute", help="Trigger interval")
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    spark = create_spark_session()
    
    query_1min, query_hourly, query_alerts = run_gold_pipeline(
        spark=spark,
        silver_path=args.silver_path,
        gold_path=args.gold_path,
        checkpoint_base=args.checkpoint_path,
        trigger_interval=args.trigger_interval
    )
    
    logger.info("Gold pipeline started. Awaiting termination...")
    
    try:
        # Wait for all queries
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        query_1min.stop()
        query_hourly.stop()
        query_alerts.stop()


if __name__ == "__main__":
    main()
