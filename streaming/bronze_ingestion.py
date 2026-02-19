"""
Bronze Layer Ingestion: Kafka → Delta Lake

Reads raw API usage events from Kafka and writes to Delta Lake Bronze table.
Focus: Correctness, fault tolerance, raw data preservation.
No business logic - pure ingestion.

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
        bronze_ingestion.py \
        --kafka-brokers kafka-1:9092,kafka-2:9092 \
        --checkpoint-path s3://data-lake/checkpoints/bronze/api-usage-events \
        --output-path s3://data-lake/bronze/api_usage_events
"""

import argparse
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    lit,
    expr,
    xxhash64,
    to_date,
    hour,
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
    ArrayType,
    MapType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("BronzeIngestion")


# =============================================================================
# SCHEMA DEFINITION
# =============================================================================

def get_event_schema() -> StructType:
    """
    Define schema for API usage events.
    
    IMPORTANT: Schema must match the JSON schema in api_usage_event.schema.json
    Using explicit schema instead of schema inference for:
    - Performance (no schema inference overhead)
    - Consistency (schema doesn't change between batches)
    - Fault tolerance (malformed records don't break pipeline)
    """
    
    identity_schema = StructType([
        StructField("api_key_id", StringType(), True),
        StructField("api_key_prefix", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("auth_method", StringType(), True),
        StructField("oauth_client_id", StringType(), True),
        StructField("scopes", ArrayType(StringType()), True),
    ])
    
    request_schema = StructType([
        StructField("request_id", StringType(), True),
        StructField("trace_id", StringType(), True),
        StructField("span_id", StringType(), True),
        StructField("method", StringType(), True),
        StructField("path", StringType(), True),
        StructField("path_raw", StringType(), True),
        StructField("query_params_hash", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("endpoint_id", StringType(), True),
        StructField("received_at", StringType(), True),  # Keep as string, parse later
        StructField("content_length", LongType(), True),
        StructField("content_type", StringType(), True),
    ])
    
    response_schema = StructType([
        StructField("status_code", IntegerType(), True),
        StructField("status_category", StringType(), True),
        StructField("content_length", LongType(), True),
        StructField("latency_ms", DoubleType(), True),
        StructField("upstream_latency_ms", DoubleType(), True),
        StructField("cache_status", StringType(), True),
        StructField("error_code", StringType(), True),
        StructField("error_message", StringType(), True),
    ])
    
    client_schema = StructType([
        StructField("ip_address", StringType(), True),
        StructField("ip_address_hash", StringType(), True),
        StructField("ip_version", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("user_agent_parsed", MapType(StringType(), StringType()), True),
        StructField("sdk_name", StringType(), True),
        StructField("sdk_version", StringType(), True),
    ])
    
    geo_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("asn", IntegerType(), True),
        StructField("as_org", StringType(), True),
        StructField("is_datacenter", BooleanType(), True),
        StructField("is_vpn", BooleanType(), True),
        StructField("is_tor", BooleanType(), True),
        StructField("is_proxy", BooleanType(), True),
    ])
    
    rate_limiting_schema = StructType([
        StructField("limit", IntegerType(), True),
        StructField("remaining", IntegerType(), True),
        StructField("reset_at", StringType(), True),
        StructField("window_size_seconds", IntegerType(), True),
        StructField("was_rate_limited", BooleanType(), True),
        StructField("rate_limit_policy", StringType(), True),
        StructField("quota_used_percent", DoubleType(), True),
    ])
    
    abuse_signals_schema = StructType([
        StructField("risk_score", DoubleType(), True),
        StructField("risk_factors", ArrayType(StringType()), True),
        StructField("velocity_1min", IntegerType(), True),
        StructField("velocity_5min", IntegerType(), True),
        StructField("velocity_1hour", IntegerType(), True),
        StructField("unique_ips_24h", IntegerType(), True),
        StructField("unique_endpoints_1h", IntegerType(), True),
        StructField("is_suspicious", BooleanType(), True),
        StructField("action_taken", StringType(), True),
        StructField("fingerprint_hash", StringType(), True),
    ])
    
    subscription_schema = StructType([
        StructField("subscription_id", StringType(), True),
        StructField("plan_tier", StringType(), True),
        StructField("billing_cycle_id", StringType(), True),
    ])
    
    gateway_schema = StructType([
        StructField("gateway_id", StringType(), True),
        StructField("gateway_region", StringType(), True),
        StructField("gateway_version", StringType(), True),
        StructField("upstream_service", StringType(), True),
        StructField("upstream_instance", StringType(), True),
    ])
    
    billing_schema = StructType([
        StructField("billable", BooleanType(), True),
        StructField("billing_units", DoubleType(), True),
        StructField("pricing_tier", StringType(), True),
        StructField("cost_estimate_usd", DoubleType(), True),
    ])
    
    metadata_schema = StructType([
        StructField("source", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("partition_key", StringType(), True),
        StructField("retention_days", IntegerType(), True),
    ])
    
    # Full event schema
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_version", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("processing_timestamp", StringType(), True),
        StructField("sequence_number", LongType(), True),
        StructField("tenant_id", StringType(), True),
        StructField("subscription", subscription_schema, True),
        StructField("identity", identity_schema, True),
        StructField("request", request_schema, True),
        StructField("response", response_schema, True),
        StructField("client", client_schema, True),
        StructField("geo", geo_schema, True),
        StructField("rate_limiting", rate_limiting_schema, True),
        StructField("abuse_signals", abuse_signals_schema, True),
        StructField("gateway", gateway_schema, True),
        StructField("billing", billing_schema, True),
        StructField("custom_attributes", MapType(StringType(), StringType()), True),
        StructField("metadata", metadata_schema, True),
    ])


# =============================================================================
# SPARK SESSION
# =============================================================================

def create_spark_session(app_name: str = "BronzeIngestion") -> SparkSession:
    """
    Create Spark session with Delta Lake and Kafka support.
    
    Configuration optimized for streaming fault tolerance.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        # Delta Lake configuration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Streaming reliability
        .config("spark.sql.streaming.checkpointLocation.validateRecovery", "true")
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "64")  # Match Kafka partitions
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Delta optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )


# =============================================================================
# KAFKA SOURCE
# =============================================================================

def read_from_kafka(
    spark: SparkSession,
    kafka_brokers: str,
    topic: str = "api-usage-events.v1",
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: Optional[int] = None,
) -> DataFrame:
    """
    Read streaming data from Kafka.
    
    Args:
        spark: SparkSession
        kafka_brokers: Comma-separated list of Kafka brokers
        topic: Kafka topic name
        starting_offsets: Where to start reading ("earliest", "latest", or JSON offset spec)
        max_offsets_per_trigger: Max records per micro-batch (backpressure control)
    
    Returns:
        Streaming DataFrame with Kafka message columns
    
    Kafka columns returned:
        - key: Message key (bytes)
        - value: Message value (bytes) - contains JSON payload
        - topic: Source topic name
        - partition: Kafka partition number
        - offset: Message offset within partition
        - timestamp: Kafka message timestamp
        - timestampType: Type of timestamp (CreateTime or LogAppendTime)
    """
    logger.info(f"Configuring Kafka source: {kafka_brokers} / {topic}")
    
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        # Reliability settings
        .option("failOnDataLoss", "false")  # Don't fail if offsets are unavailable
        .option("kafka.consumer.commit.groupid", "bronze-ingestion")
        # Security (configure as needed)
        # .option("kafka.security.protocol", "SASL_SSL")
        # .option("kafka.sasl.mechanism", "PLAIN")
        # .option("kafka.sasl.jaas.config", "...")
    )
    
    # Apply backpressure if configured
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    
    return reader.load()


# =============================================================================
# TRANSFORMATION (Minimal - Bronze Layer)
# =============================================================================

def transform_to_bronze(df: DataFrame, event_schema: StructType) -> DataFrame:
    """
    Transform Kafka messages to Bronze table format.
    
    Bronze layer principles:
    - Preserve ALL raw data (nothing dropped)
    - Add ingestion metadata
    - Parse JSON but keep original
    - Add partitioning columns
    
    Args:
        df: Raw Kafka DataFrame
        event_schema: Schema for JSON parsing
    
    Returns:
        DataFrame ready for Delta Lake Bronze table
    """
    return (
        df
        # Decode Kafka key and value from bytes to string
        .withColumn("kafka_key", col("key").cast("string"))
        .withColumn("kafka_value", col("value").cast("string"))
        
        # Parse JSON payload (permissive mode - don't fail on bad records)
        .withColumn(
            "parsed",
            from_json(col("kafka_value"), event_schema, {"mode": "PERMISSIVE"})
        )
        
        # Check if parsing succeeded
        .withColumn(
            "is_valid_json",
            col("parsed").isNotNull() & col("parsed.event_id").isNotNull()
        )
        
        # Preserve raw data
        .withColumn("_raw_value", col("kafka_value"))
        
        # Add Kafka metadata
        .withColumn("_kafka_topic", col("topic"))
        .withColumn("_kafka_partition", col("partition"))
        .withColumn("_kafka_offset", col("offset"))
        .withColumn("_kafka_timestamp", col("timestamp"))
        .withColumn("_kafka_timestamp_type", col("timestampType"))
        
        # Add ingestion metadata
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_job", lit("bronze_ingestion"))
        
        # Create row hash for deduplication downstream
        .withColumn("_row_hash", xxhash64(col("kafka_value")))
        
        # Extract partitioning columns (for efficient queries)
        # Using event_timestamp for time-based partitioning
        .withColumn(
            "_event_date",
            to_date(col("parsed.event_timestamp"))
        )
        .withColumn(
            "_event_hour",
            hour(col("parsed.event_timestamp"))
        )
        
        # Extract tenant for partition pruning
        .withColumn("_tenant_id", col("parsed.tenant_id"))
        
        # Flatten parsed fields at top level for easier access
        .select(
            # Event identification
            col("parsed.event_id").alias("event_id"),
            col("parsed.event_version").alias("event_version"),
            col("parsed.event_timestamp").alias("event_timestamp"),
            col("parsed.sequence_number").alias("sequence_number"),
            col("parsed.tenant_id").alias("tenant_id"),
            
            # Nested structures (preserved as-is)
            col("parsed.subscription").alias("subscription"),
            col("parsed.identity").alias("identity"),
            col("parsed.request").alias("request"),
            col("parsed.response").alias("response"),
            col("parsed.client").alias("client"),
            col("parsed.geo").alias("geo"),
            col("parsed.rate_limiting").alias("rate_limiting"),
            col("parsed.abuse_signals").alias("abuse_signals"),
            col("parsed.gateway").alias("gateway"),
            col("parsed.billing").alias("billing"),
            col("parsed.custom_attributes").alias("custom_attributes"),
            col("parsed.metadata").alias("metadata"),
            
            # Raw data preservation
            col("_raw_value"),
            col("is_valid_json").alias("_is_valid"),
            
            # Kafka metadata
            col("_kafka_topic"),
            col("_kafka_partition"),
            col("_kafka_offset"),
            col("_kafka_timestamp"),
            col("_kafka_timestamp_type"),
            
            # Ingestion metadata
            col("_ingested_at"),
            col("_ingestion_job"),
            col("_row_hash"),
            
            # Partitioning columns
            col("_event_date"),
            col("_event_hour"),
            col("_tenant_id"),
        )
    )


# =============================================================================
# DELTA LAKE SINK
# =============================================================================

def write_to_delta(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str = "1 minute",
    partition_columns: list = None,
) -> None:
    """
    Write streaming DataFrame to Delta Lake Bronze table.
    
    Args:
        df: Transformed DataFrame
        output_path: Delta table location
        checkpoint_path: Checkpoint location for fault tolerance
        trigger_interval: Micro-batch trigger interval
        partition_columns: Columns to partition by
    
    Fault tolerance guarantees:
        - Exactly-once semantics via checkpointing
        - Atomic micro-batch commits to Delta
        - Recoverable from checkpoint on failure
    """
    if partition_columns is None:
        partition_columns = ["_event_date", "_event_hour"]
    
    logger.info(f"Starting Delta Lake writer to: {output_path}")
    logger.info(f"Checkpoint location: {checkpoint_path}")
    logger.info(f"Trigger interval: {trigger_interval}")
    logger.info(f"Partition columns: {partition_columns}")
    
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        
        # Checkpoint configuration (CRITICAL for fault tolerance)
        .option("checkpointLocation", checkpoint_path)
        
        # Partitioning for query performance
        .partitionBy(*partition_columns)
        
        # Trigger configuration
        # Options:
        #   - processingTime: Fixed interval micro-batches
        #   - availableNow: Process all available, then stop
        #   - continuous: Low-latency continuous processing (experimental)
        .trigger(processingTime=trigger_interval)
        
        # Delta-specific options
        .option("mergeSchema", "false")  # Fail on schema changes (intentional)
        
        # Start the stream
        .start(output_path)
    )
    
    return query


# =============================================================================
# MONITORING
# =============================================================================

def log_batch_metrics(batch_df: DataFrame, batch_id: int) -> None:
    """
    Log metrics for each micro-batch.
    Called via foreachBatch for observability.
    """
    count = batch_df.count()
    valid_count = batch_df.filter(col("_is_valid") == True).count()
    invalid_count = count - valid_count
    
    logger.info(
        f"Batch {batch_id}: "
        f"total={count}, valid={valid_count}, invalid={invalid_count}"
    )
    
    # Log partition distribution
    if count > 0:
        partition_counts = (
            batch_df
            .groupBy("_kafka_partition")
            .count()
            .collect()
        )
        logger.info(f"Batch {batch_id} partition distribution: {partition_counts}")


def write_to_delta_with_metrics(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str = "1 minute",
):
    """
    Write to Delta with per-batch metrics logging.
    Uses foreachBatch for fine-grained control.
    """
    def process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty batch, skipping")
            return
        
        # Log metrics
        log_batch_metrics(batch_df, batch_id)
        
        # Write to Delta
        (
            batch_df
            .write
            .format("delta")
            .mode("append")
            .partitionBy("_event_date", "_event_hour")
            .save(output_path)
        )
        
        logger.info(f"Batch {batch_id}: Written to {output_path}")
    
    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
    
    return query


# =============================================================================
# DEAD LETTER QUEUE
# =============================================================================

def write_invalid_to_dlq(
    df: DataFrame,
    dlq_path: str,
    checkpoint_path: str,
) -> None:
    """
    Write invalid/malformed records to Dead Letter Queue.
    
    Separates bad records for investigation without blocking pipeline.
    """
    invalid_df = (
        df
        .filter(col("_is_valid") == False)
        .select(
            col("_raw_value"),
            col("_kafka_topic"),
            col("_kafka_partition"),
            col("_kafka_offset"),
            col("_kafka_timestamp"),
            col("_ingested_at"),
            lit("parse_failure").alias("_error_type"),
        )
        # Add partition column before writing (partitionBy requires column name, not expression)
        .withColumn("_dlq_date", to_date(col("_ingested_at")))
    )
    
    query = (
        invalid_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("_dlq_date")
        .trigger(processingTime="1 minute")
        .start(dlq_path)
    )
    
    return query


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Bronze layer Kafka to Delta ingestion")
    
    parser.add_argument(
        "--kafka-brokers",
        required=True,
        help="Comma-separated Kafka broker addresses"
    )
    parser.add_argument(
        "--kafka-topic",
        default="api-usage-events.v1",
        help="Kafka topic to consume"
    )
    parser.add_argument(
        "--checkpoint-path",
        required=True,
        help="Checkpoint location for fault tolerance"
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Delta Lake output path"
    )
    parser.add_argument(
        "--dlq-path",
        default=None,
        help="Dead letter queue path for invalid records"
    )
    parser.add_argument(
        "--trigger-interval",
        default="1 minute",
        help="Micro-batch trigger interval"
    )
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        help="Starting offsets: earliest, latest, or JSON spec"
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=None,
        help="Max offsets per micro-batch (backpressure)"
    )
    parser.add_argument(
        "--with-metrics",
        action="store_true",
        help="Enable per-batch metrics logging"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    logger.info("=" * 60)
    logger.info("Starting Bronze Ingestion Pipeline")
    logger.info("=" * 60)
    logger.info(f"Kafka brokers: {args.kafka_brokers}")
    logger.info(f"Kafka topic: {args.kafka_topic}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Checkpoint path: {args.checkpoint_path}")
    logger.info("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Get event schema
    event_schema = get_event_schema()
    
    # Read from Kafka
    kafka_df = read_from_kafka(
        spark=spark,
        kafka_brokers=args.kafka_brokers,
        topic=args.kafka_topic,
        starting_offsets=args.starting_offsets,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
    )
    
    # Transform to Bronze format
    bronze_df = transform_to_bronze(kafka_df, event_schema)
    
    # Write valid records to Delta Lake
    if args.with_metrics:
        main_query = write_to_delta_with_metrics(
            df=bronze_df.filter(col("_is_valid") == True),
            output_path=args.output_path,
            checkpoint_path=args.checkpoint_path,
            trigger_interval=args.trigger_interval,
        )
    else:
        main_query = write_to_delta(
            df=bronze_df.filter(col("_is_valid") == True),
            output_path=args.output_path,
            checkpoint_path=args.checkpoint_path,
            trigger_interval=args.trigger_interval,
        )
    
    # Optionally write invalid records to DLQ
    dlq_query = None
    if args.dlq_path:
        dlq_query = write_invalid_to_dlq(
            df=bronze_df,
            dlq_path=args.dlq_path,
            checkpoint_path=f"{args.checkpoint_path}_dlq",
        )
    
    # Await termination
    logger.info("Pipeline started. Awaiting termination...")
    
    try:
        main_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        main_query.stop()
        if dlq_query:
            dlq_query.stop()
        logger.info("Pipeline stopped gracefully")


if __name__ == "__main__":
    main()
