"""
Silver Layer Processing: Bronze → Silver

Transforms raw Bronze data into clean, deduplicated, schema-enforced Silver table.
Focus: Data quality, deduplication, late-event handling.

Design Principles:
- Single source of truth for clean data
- Exactly-once semantics via deduplication
- Late-arriving events handled gracefully
- Bad records quarantined, not dropped silently

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.1.0 \
        silver_processing.py \
        --bronze-path s3://data-lake/bronze/api_usage_events \
        --silver-path s3://data-lake/silver/api_usage_events \
        --quarantine-path s3://data-lake/quarantine/api_usage_events \
        --checkpoint-path s3://data-lake/checkpoints/silver/api-usage-events
"""

import argparse
import logging
from datetime import datetime
from typing import Optional, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    current_timestamp,
    to_timestamp,
    window,
    row_number,
    expr,
    sha2,
    concat_ws,
    array,
    size,
    explode,
    from_json,
    to_json,
    struct,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
    ArrayType,
    MapType,
)

from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SilverProcessing")


# =============================================================================
# DESIGN CHOICE: SILVER SCHEMA (Strict & Typed)
# =============================================================================
# 
# WHY STRICT SCHEMA?
# - Bronze stores raw JSON as strings (flexible, preserves everything)
# - Silver enforces types (catches data quality issues early)
# - Downstream consumers get predictable, queryable data
# 
# WHY DIFFERENT FROM BRONZE?
# - Bronze: StringType for timestamps (preserve raw format)
# - Silver: TimestampType (enables time-based queries)
# - Bronze: All fields nullable (accept anything)
# - Silver: Required fields enforced (fail fast on bad data)
# =============================================================================

def get_silver_schema() -> StructType:
    """
    Silver layer schema with strict typing.
    
    Key differences from Bronze:
    1. Timestamps are TimestampType (not StringType)
    2. Required fields are NOT NULL
    3. Nested structures flattened where useful
    4. Derived fields added for analytics
    """
    
    return StructType([
        # =================================================================
        # PRIMARY KEY & DEDUPLICATION
        # =================================================================
        # event_id: Used for deduplication
        # Why NOT NULL: Without event_id, cannot deduplicate
        StructField("event_id", StringType(), nullable=False),
        
        # =================================================================
        # TEMPORAL FIELDS (Typed as Timestamp)
        # =================================================================
        # Why TimestampType: Enables window functions, time-range queries
        # Why NOT NULL: Events without timestamps are useless
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        
        # Derived: Processing time for latency tracking
        StructField("processed_at", TimestampType(), nullable=False),
        
        # =================================================================
        # TENANT & IDENTITY (Core dimensions)
        # =================================================================
        StructField("tenant_id", StringType(), nullable=False),
        StructField("api_key_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("auth_method", StringType(), nullable=True),
        
        # =================================================================
        # REQUEST DETAILS (Flattened for query performance)
        # =================================================================
        StructField("request_id", StringType(), nullable=True),
        StructField("trace_id", StringType(), nullable=True),
        StructField("http_method", StringType(), nullable=True),
        StructField("endpoint_path", StringType(), nullable=True),
        StructField("endpoint_id", StringType(), nullable=True),
        StructField("api_version", StringType(), nullable=True),
        StructField("request_size_bytes", LongType(), nullable=True),
        
        # =================================================================
        # RESPONSE DETAILS
        # =================================================================
        StructField("status_code", IntegerType(), nullable=True),
        StructField("status_category", StringType(), nullable=True),
        StructField("response_size_bytes", LongType(), nullable=True),
        StructField("latency_ms", DoubleType(), nullable=True),
        StructField("upstream_latency_ms", DoubleType(), nullable=True),
        StructField("cache_hit", BooleanType(), nullable=True),
        
        # =================================================================
        # CLIENT & GEO (Flattened)
        # =================================================================
        StructField("client_ip_hash", StringType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("is_bot", BooleanType(), nullable=True),
        StructField("is_datacenter", BooleanType(), nullable=True),
        StructField("is_vpn", BooleanType(), nullable=True),
        
        # =================================================================
        # RATE LIMITING & ABUSE
        # =================================================================
        StructField("was_rate_limited", BooleanType(), nullable=True),
        StructField("rate_limit_remaining", IntegerType(), nullable=True),
        StructField("risk_score", DoubleType(), nullable=True),
        StructField("is_suspicious", BooleanType(), nullable=True),
        
        # =================================================================
        # BILLING
        # =================================================================
        StructField("is_billable", BooleanType(), nullable=True),
        StructField("billing_units", DoubleType(), nullable=True),
        StructField("plan_tier", StringType(), nullable=True),
        
        # =================================================================
        # METADATA
        # =================================================================
        StructField("event_version", StringType(), nullable=True),
        StructField("gateway_id", StringType(), nullable=True),
        StructField("gateway_region", StringType(), nullable=True),
        
        # =================================================================
        # LINEAGE (Link back to Bronze)
        # =================================================================
        StructField("_bronze_kafka_partition", IntegerType(), nullable=True),
        StructField("_bronze_kafka_offset", LongType(), nullable=True),
        StructField("_bronze_ingested_at", TimestampType(), nullable=True),
        
        # =================================================================
        # DATA QUALITY FLAGS
        # =================================================================
        StructField("_is_late_arriving", BooleanType(), nullable=False),
        StructField("_arrival_delay_seconds", LongType(), nullable=True),
        StructField("_quality_flags", ArrayType(StringType()), nullable=True),
        
        # =================================================================
        # PARTITIONING
        # =================================================================
        StructField("_event_date", StringType(), nullable=False),
        StructField("_event_hour", IntegerType(), nullable=False),
    ])


# =============================================================================
# DESIGN CHOICE: DATA QUALITY VALIDATION
# =============================================================================
#
# WHY VALIDATE?
# - Bronze accepts everything (preserve raw data)
# - Silver must be trustworthy (consumers depend on quality)
# - Catch issues early, quarantine bad records
#
# VALIDATION LEVELS:
# 1. Required fields (must exist)
# 2. Type correctness (must parse)
# 3. Business rules (must make sense)
# 4. Referential integrity (must be consistent)
# =============================================================================

class DataQualityValidator:
    """
    Validates Bronze records before Silver processing.
    
    Design: Returns (valid_df, invalid_df, quality_metrics)
    - Valid records → Silver table
    - Invalid records → Quarantine table
    - Metrics → Monitoring dashboard
    """
    
    # Required fields - records missing these go to quarantine
    REQUIRED_FIELDS = [
        "event_id",
        "event_timestamp", 
        "tenant_id",
    ]
    
    # Business rule validations
    BUSINESS_RULES = {
        "valid_status_code": "response.status_code BETWEEN 100 AND 599",
        "valid_latency": "response.latency_ms >= 0 OR response.latency_ms IS NULL",
        "valid_risk_score": "abuse_signals.risk_score BETWEEN 0 AND 100 OR abuse_signals.risk_score IS NULL",
        "valid_timestamp": "event_timestamp <= current_timestamp()",  # No future events
        "valid_billing_units": "billing.billing_units >= 0 OR billing.billing_units IS NULL",
    }
    
    @classmethod
    def validate(cls, df: DataFrame) -> Tuple[DataFrame, DataFrame, dict]:
        """
        Validate DataFrame and split into valid/invalid.
        
        Returns:
            (valid_df, invalid_df, metrics_dict)
        """
        # Track quality flags per record
        quality_checks = []
        
        # =================================================================
        # CHECK 1: Required Fields Present
        # =================================================================
        for field in cls.REQUIRED_FIELDS:
            check_name = f"missing_{field}"
            df = df.withColumn(
                check_name,
                when(col(field).isNull(), lit(check_name)).otherwise(lit(None))
            )
            quality_checks.append(check_name)
        
        # =================================================================
        # CHECK 2: Business Rules
        # =================================================================
        for rule_name, rule_expr in cls.BUSINESS_RULES.items():
            check_name = f"failed_{rule_name}"
            df = df.withColumn(
                check_name,
                when(~expr(rule_expr), lit(check_name)).otherwise(lit(None))
            )
            quality_checks.append(check_name)
        
        # =================================================================
        # CHECK 3: Timestamp Parseability
        # =================================================================
        df = df.withColumn(
            "_parsed_event_ts",
            to_timestamp(col("event_timestamp"))
        ).withColumn(
            "unparseable_timestamp",
            when(col("_parsed_event_ts").isNull(), lit("unparseable_timestamp")).otherwise(lit(None))
        )
        quality_checks.append("unparseable_timestamp")
        
        # =================================================================
        # AGGREGATE QUALITY FLAGS
        # =================================================================
        df = df.withColumn(
            "_quality_flags",
            array(*[col(c) for c in quality_checks])
        ).withColumn(
            "_quality_flags",
            expr("filter(_quality_flags, x -> x IS NOT NULL)")
        ).withColumn(
            "_has_quality_issues",
            size(col("_quality_flags")) > 0
        )
        
        # Drop temporary check columns
        df = df.drop(*quality_checks).drop("_parsed_event_ts")
        
        # =================================================================
        # SPLIT VALID / INVALID
        # =================================================================
        valid_df = df.filter(~col("_has_quality_issues")).drop("_has_quality_issues")
        invalid_df = df.filter(col("_has_quality_issues")).drop("_has_quality_issues")
        
        # =================================================================
        # COMPUTE METRICS
        # =================================================================
        # Note: In streaming, metrics are computed per micro-batch
        # For batch, we can compute here
        metrics = {}
        
        return valid_df, invalid_df, metrics


# =============================================================================
# DESIGN CHOICE: DEDUPLICATION STRATEGY
# =============================================================================
#
# WHY DEDUPLICATE?
# - Kafka producers may retry (at-least-once delivery)
# - Network issues cause duplicate sends
# - Bronze may ingest same event twice (replay scenarios)
#
# DEDUPLICATION APPROACHES:
# 
# 1. STREAMING DROPDUPLICATES (chosen for streaming)
#    - Uses watermark + event_id
#    - State-based, handles late events
#    - Memory-bounded with watermark
#
# 2. MERGE INTO (chosen for batch/micro-batch)
#    - Upsert pattern with Delta Lake
#    - Handles historical dedup
#    - More control over conflict resolution
#
# 3. ROW_NUMBER WINDOW (fallback)
#    - Deterministic pick of "winner" record
#    - Works for batch processing
#    - No state management needed
# =============================================================================

class Deduplicator:
    """
    Handles event deduplication with multiple strategies.
    """
    
    @staticmethod
    def streaming_dedup(
        df: DataFrame,
        watermark_delay: str = "1 hour",
        dedup_columns: List[str] = None
    ) -> DataFrame:
        """
        Streaming deduplication using watermark.
        
        Design Choice: Watermark-based deduplication
        
        WHY WATERMARK?
        - Bounds state size (old duplicates eventually dropped)
        - Allows late events within window
        - Prevents OOM from unbounded state
        
        WHY 1 HOUR DEFAULT?
        - Covers typical retry scenarios (seconds to minutes)
        - Handles network partitions (usually < 30 min)
        - Balances memory vs late-event tolerance
        
        Args:
            df: Input DataFrame with event_timestamp column
            watermark_delay: How late events can arrive
            dedup_columns: Columns to deduplicate on (default: event_id)
        """
        if dedup_columns is None:
            dedup_columns = ["event_id"]
        
        logger.info(f"Applying streaming dedup with watermark={watermark_delay}")
        
        return (
            df
            # First, ensure event_timestamp is TimestampType for watermark
            .withColumn(
                "event_timestamp_ts",
                to_timestamp(col("event_timestamp"))
            )
            # Apply watermark on event time
            # DESIGN: Use event_timestamp (when event occurred), not ingestion time
            # This ensures consistent dedup regardless of when event arrives
            .withWatermark("event_timestamp_ts", watermark_delay)
            # Drop duplicates within watermark window
            .dropDuplicates(dedup_columns)
        )
    
    @staticmethod
    def batch_dedup_with_merge(
        source_df: DataFrame,
        target_table: DeltaTable,
        merge_keys: List[str] = None,
        update_condition: str = None
    ) -> None:
        """
        Batch deduplication using Delta Lake MERGE.
        
        Design Choice: MERGE INTO for batch processing
        
        WHY MERGE?
        - Atomic upsert (no partial updates)
        - Handles conflicts with custom logic
        - Efficient for incremental loads
        - Works with historical data
        
        MERGE STRATEGY:
        - Match on event_id
        - If exists: Keep existing (first-write-wins)
        - If new: Insert
        
        Alternative: Last-write-wins
        - Update if source.event_timestamp > target.event_timestamp
        - Use when later events may have corrections
        """
        if merge_keys is None:
            merge_keys = ["event_id"]
        
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in merge_keys
        ])
        
        logger.info(f"Executing MERGE with keys: {merge_keys}")
        
        (
            target_table.alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition
            )
            # DESIGN CHOICE: First-write-wins
            # If record exists, do nothing (keep original)
            # Rationale: First event is authoritative; duplicates are discarded
            .whenMatchedUpdateAll(condition=update_condition) if update_condition else None
            # Insert new records
            .whenNotMatchedInsertAll()
            .execute()
        )
    
    @staticmethod
    def batch_dedup_with_window(
        df: DataFrame,
        dedup_columns: List[str] = None,
        order_columns: List[Tuple[str, str]] = None
    ) -> DataFrame:
        """
        Batch deduplication using window functions.
        
        Design Choice: ROW_NUMBER for deterministic selection
        
        WHY ROW_NUMBER?
        - No state management
        - Deterministic (same input → same output)
        - Can define custom ordering for "winner" selection
        
        ORDER STRATEGY (default):
        - event_timestamp ASC → earliest event wins
        - _kafka_offset ASC → lower offset wins (tie-breaker)
        
        Alternative strategies:
        - Latest wins: event_timestamp DESC
        - Most complete: non-null field count
        """
        if dedup_columns is None:
            dedup_columns = ["event_id"]
        
        if order_columns is None:
            # Default: earliest event wins
            order_columns = [
                ("event_timestamp", "asc"),
                ("_bronze_kafka_offset", "asc")
            ]
        
        # Build window specification
        window_spec = Window.partitionBy(*dedup_columns)
        for col_name, order in order_columns:
            if order.lower() == "asc":
                window_spec = window_spec.orderBy(col(col_name).asc())
            else:
                window_spec = window_spec.orderBy(col(col_name).desc())
        
        logger.info(f"Applying window dedup on: {dedup_columns}")
        
        return (
            df
            .withColumn("_dedup_rank", row_number().over(window_spec))
            .filter(col("_dedup_rank") == 1)
            .drop("_dedup_rank")
        )


# =============================================================================
# DESIGN CHOICE: LATE-ARRIVING EVENT HANDLING
# =============================================================================
#
# WHY HANDLE LATE EVENTS?
# - Network delays cause events to arrive late
# - Batch producers may send historical data
# - Cross-timezone clients have clock skew
#
# WATERMARK STRATEGY:
#
# WATERMARK = "1 hour" means:
# - Events up to 1 hour late are processed normally
# - Events more than 1 hour late may be dropped (streaming)
# - State for events older than watermark is garbage collected
#
# TRADE-OFFS:
# - Longer watermark → More late events captured, more memory used
# - Shorter watermark → Less memory, more late events dropped
#
# OUR CHOICE: 1 hour with flagging
# - Most legitimate late events are < 30 min
# - 1 hour covers edge cases (deployments, network issues)
# - Flag late events for monitoring (don't drop silently)
# =============================================================================

class LateEventHandler:
    """
    Handles late-arriving events with flagging and metrics.
    
    Thresholds aligned with DESIGN_ADDENDUM.md:
    - Watermark: 1 hour (events beyond this may miss streaming aggregations)
    - Late threshold: 5 minutes (flagged for monitoring)
    - Very late: 1 hour (still processed, flagged, reconciled in batch)
    """
    
    # Events arriving after this threshold are flagged as "late"
    # Per DESIGN_ADDENDUM.md: Most legitimate delays are < 5 min
    LATE_THRESHOLD_SECONDS = 300  # 5 minutes
    
    # Events arriving after this are considered "very late"
    # Per DESIGN_ADDENDUM.md: Watermark is 1 hour
    VERY_LATE_THRESHOLD_SECONDS = 3600  # 1 hour
    
    @classmethod
    def flag_late_events(cls, df: DataFrame) -> DataFrame:
        """
        Flag late-arriving events without dropping them.
        
        Design Choice: Flag, don't drop
        
        WHY NOT DROP?
        - Late events may contain important data
        - Dropping silently causes data loss
        - Better to flag and let downstream decide
        
        WHY FLAG?
        - Enables monitoring/alerting on late events
        - Allows filtering in analytics if needed
        - Preserves information about system health
        """
        return (
            df
            # Calculate arrival delay
            .withColumn(
                "_parsed_event_ts",
                to_timestamp(col("event_timestamp"))
            )
            .withColumn(
                "_parsed_ingested_at",
                to_timestamp(col("_bronze_ingested_at"))
            )
            .withColumn(
                "_arrival_delay_seconds",
                (
                    col("_parsed_ingested_at").cast("long") -
                    col("_parsed_event_ts").cast("long")
                )
            )
            # Flag late events
            .withColumn(
                "_is_late_arriving",
                col("_arrival_delay_seconds") > cls.LATE_THRESHOLD_SECONDS
            )
            # Add severity category
            .withColumn(
                "_late_category",
                when(
                    col("_arrival_delay_seconds") > cls.VERY_LATE_THRESHOLD_SECONDS,
                    lit("very_late")
                ).when(
                    col("_arrival_delay_seconds") > cls.LATE_THRESHOLD_SECONDS,
                    lit("late")
                ).otherwise(lit("on_time"))
            )
            # Clean up temp columns
            .drop("_parsed_event_ts", "_parsed_ingested_at")
        )


# =============================================================================
# TRANSFORMATION: BRONZE → SILVER
# =============================================================================

def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform Bronze records to Silver schema.
    
    Design Choices:
    
    1. FLATTEN NESTED STRUCTURES
       - Bronze: Nested JSON objects (identity.api_key_id)
       - Silver: Flat columns (api_key_id)
       - Why: Better query performance, easier to use in BI tools
    
    2. TYPE COERCION
       - Bronze: Strings preserved from JSON
       - Silver: Proper types (TimestampType, IntegerType)
       - Why: Enables type-safe operations, better compression
    
    3. DERIVED FIELDS
       - Add processing_timestamp (when Silver processed)
       - Add cache_hit (boolean from cache_status)
       - Why: Simplify downstream queries
    
    4. LINEAGE PRESERVATION
       - Keep _bronze_* columns linking to source
       - Why: Full traceability from Silver → Bronze → Kafka
    """
    
    return (
        df
        # =================================================================
        # CORE IDENTIFIERS
        # =================================================================
        .select(
            # Primary key
            col("event_id"),
            
            # Timestamps - coerce to TimestampType
            to_timestamp(col("event_timestamp")).alias("event_timestamp"),
            coalesce(
                to_timestamp(col("_ingested_at")),
                current_timestamp()
            ).alias("ingested_at"),
            current_timestamp().alias("processed_at"),
            
            # Tenant
            col("tenant_id"),
            
            # =================================================================
            # IDENTITY (flattened)
            # =================================================================
            col("identity.api_key_id").alias("api_key_id"),
            col("identity.user_id").alias("user_id"),
            col("identity.auth_method").alias("auth_method"),
            
            # =================================================================
            # REQUEST (flattened)
            # =================================================================
            col("request.request_id").alias("request_id"),
            col("request.trace_id").alias("trace_id"),
            col("request.method").alias("http_method"),
            col("request.path").alias("endpoint_path"),
            col("request.endpoint_id").alias("endpoint_id"),
            col("request.api_version").alias("api_version"),
            col("request.content_length").alias("request_size_bytes"),
            
            # =================================================================
            # RESPONSE (flattened with derived fields)
            # =================================================================
            col("response.status_code").alias("status_code"),
            col("response.status_category").alias("status_category"),
            col("response.content_length").alias("response_size_bytes"),
            col("response.latency_ms").alias("latency_ms"),
            col("response.upstream_latency_ms").alias("upstream_latency_ms"),
            # Derived: cache_hit as boolean
            (col("response.cache_status") == "hit").alias("cache_hit"),
            
            # =================================================================
            # CLIENT & GEO (flattened)
            # =================================================================
            col("client.ip_address_hash").alias("client_ip_hash"),
            col("geo.country_code").alias("country_code"),
            col("geo.region").alias("region"),
            col("client.user_agent_parsed.is_bot").alias("is_bot"),
            col("geo.is_datacenter").alias("is_datacenter"),
            col("geo.is_vpn").alias("is_vpn"),
            
            # =================================================================
            # RATE LIMITING & ABUSE
            # =================================================================
            col("rate_limiting.was_rate_limited").alias("was_rate_limited"),
            col("rate_limiting.remaining").alias("rate_limit_remaining"),
            col("abuse_signals.risk_score").alias("risk_score"),
            col("abuse_signals.is_suspicious").alias("is_suspicious"),
            
            # =================================================================
            # BILLING
            # =================================================================
            col("billing.billable").alias("is_billable"),
            col("billing.billing_units").alias("billing_units"),
            col("subscription.plan_tier").alias("plan_tier"),
            
            # =================================================================
            # METADATA
            # =================================================================
            col("event_version"),
            col("gateway.gateway_id").alias("gateway_id"),
            col("gateway.gateway_region").alias("gateway_region"),
            
            # =================================================================
            # LINEAGE (preserved from Bronze)
            # =================================================================
            col("_kafka_partition").alias("_bronze_kafka_partition"),
            col("_kafka_offset").alias("_bronze_kafka_offset"),
            col("_ingested_at").alias("_bronze_ingested_at"),
            
            # =================================================================
            # QUALITY FLAGS (will be populated by validator)
            # =================================================================
            lit(False).alias("_is_late_arriving"),
            lit(None).cast(LongType()).alias("_arrival_delay_seconds"),
            array().cast(ArrayType(StringType())).alias("_quality_flags"),
            
            # =================================================================
            # PARTITIONING
            # =================================================================
            col("_event_date"),
            col("_event_hour"),
        )
    )


# =============================================================================
# QUARANTINE HANDLING
# =============================================================================

def write_to_quarantine(
    df: DataFrame,
    quarantine_path: str,
    checkpoint_path: str,
    reason: str = "validation_failure"
) -> None:
    """
    Write invalid records to quarantine table.
    
    Design Choice: Quarantine vs Drop
    
    WHY QUARANTINE?
    - Preserve data for investigation
    - Enable manual correction and replay
    - Audit trail of data quality issues
    - Metrics on data quality trends
    
    WHY NOT DROP?
    - Silent data loss is unacceptable
    - May contain valuable data with minor issues
    - Can't debug what you can't see
    
    QUARANTINE SCHEMA:
    - Original Bronze record (complete)
    - Quality flags explaining why quarantined
    - Quarantine timestamp
    - Can be reprocessed after fixes
    """
    quarantine_df = (
        df
        .withColumn("_quarantine_reason", lit(reason))
        .withColumn("_quarantined_at", current_timestamp())
    )
    
    query = (
        quarantine_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("_event_date")
        .trigger(processingTime="1 minute")
        .start(quarantine_path)
    )
    
    return query


# =============================================================================
# MAIN SILVER PIPELINE
# =============================================================================

def create_spark_session(app_name: str = "SilverProcessing") -> SparkSession:
    """Create Spark session with Delta Lake support."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Streaming settings
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        # Adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        # Delta optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )


def read_bronze_stream(
    spark: SparkSession,
    bronze_path: str,
    max_files_per_trigger: int = 100
) -> DataFrame:
    """
    Read Bronze table as streaming source.
    
    Design Choice: Delta Lake as streaming source
    
    WHY DELTA STREAMING?
    - Exactly-once semantics
    - Automatic file discovery
    - Schema enforcement
    - Works with Bronze table directly
    
    TRIGGER SETTINGS:
    - maxFilesPerTrigger: Controls batch size
    - Too high: Memory pressure
    - Too low: High latency
    """
    return (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        # Only read records marked valid in Bronze
        .load(bronze_path)
        .filter(col("_is_valid") == True)
    )


def write_silver_stream(
    df: DataFrame,
    silver_path: str,
    checkpoint_path: str,
    trigger_interval: str = "1 minute"
):
    """
    Write to Silver table with streaming.
    
    Design Choice: Append mode with MERGE for dedup
    
    WHY APPEND + MERGE?
    - Append is efficient for streaming
    - MERGE handles deduplication
    - Delta Lake handles exactly-once
    """
    
    def process_micro_batch(batch_df: DataFrame, batch_id: int):
        """
        Process each micro-batch with deduplication.
        """
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty, skipping")
            return
        
        record_count = batch_df.count()
        logger.info(f"Batch {batch_id}: Processing {record_count} records")
        
        # Check if Silver table exists
        if DeltaTable.isDeltaTable(spark, silver_path):
            # MERGE for deduplication
            silver_table = DeltaTable.forPath(spark, silver_path)
            
            (
                silver_table.alias("target")
                .merge(
                    batch_df.alias("source"),
                    "target.event_id = source.event_id"
                )
                # First-write-wins: don't update existing records
                .whenNotMatchedInsertAll()
                .execute()
            )
            
            # Count actual inserts
            # (In production, use merge metrics)
            logger.info(f"Batch {batch_id}: MERGE completed")
        else:
            # First write - create table
            (
                batch_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("_event_date", "_event_hour")
                .save(silver_path)
            )
            logger.info(f"Batch {batch_id}: Created Silver table")
    
    # Get spark session for DeltaTable check
    spark = df.sparkSession
    
    query = (
        df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
    
    return query


# =============================================================================
# ORCHESTRATION
# =============================================================================

def run_silver_pipeline(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
    checkpoint_base: str,
    watermark_delay: str = "1 hour",
    trigger_interval: str = "1 minute"
):
    """
    Run the complete Silver pipeline.
    
    PIPELINE FLOW:
    
    Bronze Table
        │
        ▼
    ┌─────────────────┐
    │ Read Stream     │
    │ (filter valid)  │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ Transform       │
    │ (Bronze→Silver) │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ Validate        │
    │ (quality rules) │
    └────────┬────────┘
             │
        ┌────┴────┐
        ▼         ▼
    ┌───────┐ ┌───────────┐
    │ Valid │ │ Invalid   │
    │       │ │           │
    └───┬───┘ └─────┬─────┘
        │           │
        ▼           ▼
    ┌───────┐ ┌───────────┐
    │ Dedup │ │ Quarantine│
    └───┬───┘ └───────────┘
        │
        ▼
    ┌───────┐
    │ Flag  │
    │ Late  │
    └───┬───┘
        │
        ▼
    ┌───────┐
    │ MERGE │
    │ Silver│
    └───────┘
    """
    
    logger.info("=" * 60)
    logger.info("Starting Silver Processing Pipeline")
    logger.info("=" * 60)
    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver path: {silver_path}")
    logger.info(f"Quarantine path: {quarantine_path}")
    logger.info(f"Watermark delay: {watermark_delay}")
    logger.info("=" * 60)
    
    # Step 1: Read Bronze stream
    bronze_df = read_bronze_stream(spark, bronze_path)
    
    # Step 2: Transform to Silver schema
    transformed_df = transform_bronze_to_silver(bronze_df)
    
    # Step 3: Validate data quality
    valid_df, invalid_df, _ = DataQualityValidator.validate(transformed_df)
    
    # Step 4: Handle late-arriving events (flag them)
    valid_df = LateEventHandler.flag_late_events(valid_df)
    
    # Step 5: Deduplicate using watermark
    deduped_df = Deduplicator.streaming_dedup(
        valid_df,
        watermark_delay=watermark_delay,
        dedup_columns=["event_id"]
    )
    
    # Step 6: Write to Silver table
    silver_query = write_silver_stream(
        deduped_df,
        silver_path,
        f"{checkpoint_base}/silver",
        trigger_interval
    )
    
    # Step 7: Write invalid records to quarantine
    quarantine_query = write_to_quarantine(
        invalid_df,
        quarantine_path,
        f"{checkpoint_base}/quarantine"
    )
    
    return silver_query, quarantine_query


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Silver layer processing")
    
    parser.add_argument("--bronze-path", required=True, help="Bronze table path")
    parser.add_argument("--silver-path", required=True, help="Silver table path")
    parser.add_argument("--quarantine-path", required=True, help="Quarantine table path")
    parser.add_argument("--checkpoint-path", required=True, help="Checkpoint base path")
    parser.add_argument("--watermark-delay", default="1 hour", help="Watermark delay")
    parser.add_argument("--trigger-interval", default="1 minute", help="Trigger interval")
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    spark = create_spark_session()
    
    silver_query, quarantine_query = run_silver_pipeline(
        spark=spark,
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        quarantine_path=args.quarantine_path,
        checkpoint_base=args.checkpoint_path,
        watermark_delay=args.watermark_delay,
        trigger_interval=args.trigger_interval
    )
    
    logger.info("Pipeline started. Awaiting termination...")
    
    try:
        silver_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        silver_query.stop()
        quarantine_query.stop()


if __name__ == "__main__":
    main()
