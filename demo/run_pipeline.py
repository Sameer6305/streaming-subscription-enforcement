"""
Local Pipeline Runner: Bronze → Silver → Gold on Local Filesystem
=================================================================

Runs the complete Medallion Architecture pipeline locally using PySpark
and Delta Lake. No Kafka, Docker, or cloud infrastructure needed.

Reads JSON events from the landing directory and processes them through:
  Bronze (raw ingestion) → Silver (dedup + validate) → Gold (aggregate + enforce)

The output Delta tables are what the Streamlit dashboard reads.

Usage:
    # Process all events in landing directory
    python demo/run_pipeline.py

    # Custom paths
    python demo/run_pipeline.py --input-dir ./demo/landing_data --output-dir ./demo/delta_tables
"""

import argparse
import os
import sys
import time
import json
import io

# Fix Windows console encoding for Unicode characters
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, PROJECT_ROOT)

# ============================================================================
# Windows Hadoop Setup (winutils.exe)
# ============================================================================
# PySpark on Windows requires HADOOP_HOME with winutils.exe.
# We auto-configure this so users don't need manual setup.

def _setup_hadoop_home():
    """Set up HADOOP_HOME on Windows if not already configured."""
    if os.name != "nt":
        return  # Only needed on Windows

    if os.environ.get("HADOOP_HOME") and os.path.exists(
        os.path.join(os.environ["HADOOP_HOME"], "bin", "winutils.exe")
    ):
        return  # Already configured

    hadoop_dir = os.path.join(PROJECT_ROOT, "hadoop")
    bin_dir = os.path.join(hadoop_dir, "bin")
    winutils_path = os.path.join(bin_dir, "winutils.exe")

    if not os.path.exists(winutils_path):
        print("  Setting up Hadoop for Windows (one-time)...")
        os.makedirs(bin_dir, exist_ok=True)
        try:
            import urllib.request
            base_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
            for fname in ["winutils.exe", "hadoop.dll"]:
                fpath = os.path.join(bin_dir, fname)
                if not os.path.exists(fpath):
                    urllib.request.urlretrieve(f"{base_url}/{fname}", fpath)
                    print(f"  Downloaded {fname}")
        except Exception as e:
            print(f"  WARNING: Could not download hadoop binaries: {e}")
            print(f"  Download manually from: https://github.com/cdarlint/winutils")
            print(f"  Place winutils.exe and hadoop.dll in: {bin_dir}")

    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")

_setup_hadoop_home()

# On Windows, PySpark needs explicit python executable path
if os.name == "nt":
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, TimestampType, ArrayType, MapType,
)
from pyspark.sql.window import Window


# ============================================================================
# Spark Session with Delta Lake
# ============================================================================

def create_spark_session() -> SparkSession:
    """Create local Spark session with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("SubscriptionTracker-LocalDemo")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


# ============================================================================
# Schema Definition (matches our JSON schema)
# ============================================================================

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_version", StringType(), True),
    StructField("event_timestamp", StringType(), False),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("sequence_number", LongType(), True),
    StructField("tenant_id", StringType(), False),
    StructField("subscription", StructType([
        StructField("subscription_id", StringType(), True),
        StructField("plan_tier", StringType(), True),
        StructField("billing_cycle_id", StringType(), True),
    ]), True),
    StructField("identity", StructType([
        StructField("api_key_id", StringType(), True),
        StructField("api_key_prefix", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("auth_method", StringType(), True),
        StructField("oauth_client_id", StringType(), True),
        StructField("scopes", ArrayType(StringType()), True),
    ]), True),
    StructField("request", StructType([
        StructField("request_id", StringType(), True),
        StructField("trace_id", StringType(), True),
        StructField("span_id", StringType(), True),
        StructField("method", StringType(), True),
        StructField("path", StringType(), True),
        StructField("path_raw", StringType(), True),
        StructField("query_params_hash", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("endpoint_id", StringType(), True),
        StructField("received_at", StringType(), True),
        StructField("content_length", IntegerType(), True),
        StructField("content_type", StringType(), True),
    ]), True),
    StructField("response", StructType([
        StructField("status_code", IntegerType(), True),
        StructField("status_category", StringType(), True),
        StructField("content_length", IntegerType(), True),
        StructField("latency_ms", DoubleType(), True),
        StructField("upstream_latency_ms", DoubleType(), True),
        StructField("cache_status", StringType(), True),
        StructField("error_code", StringType(), True),
        StructField("error_message", StringType(), True),
    ]), True),
    StructField("client", StructType([
        StructField("ip_address", StringType(), True),
        StructField("ip_address_hash", StringType(), True),
        StructField("ip_version", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("user_agent_parsed", MapType(StringType(), StringType()), True),
        StructField("sdk_name", StringType(), True),
        StructField("sdk_version", StringType(), True),
    ]), True),
    StructField("geo", StructType([
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
    ]), True),
    StructField("rate_limiting", StructType([
        StructField("limit", IntegerType(), True),
        StructField("remaining", IntegerType(), True),
        StructField("reset_at", StringType(), True),
        StructField("window_size_seconds", IntegerType(), True),
        StructField("was_rate_limited", BooleanType(), True),
        StructField("rate_limit_policy", StringType(), True),
        StructField("quota_used_percent", DoubleType(), True),
    ]), True),
    StructField("abuse_signals", StructType([
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
    ]), True),
    StructField("gateway", StructType([
        StructField("gateway_id", StringType(), True),
        StructField("gateway_region", StringType(), True),
        StructField("gateway_version", StringType(), True),
        StructField("upstream_service", StringType(), True),
        StructField("upstream_instance", StringType(), True),
    ]), True),
    StructField("billing", StructType([
        StructField("billable", BooleanType(), True),
        StructField("billing_units", DoubleType(), True),
        StructField("pricing_tier", StringType(), True),
        StructField("cost_estimate_usd", DoubleType(), True),
    ]), True),
    StructField("custom_attributes", MapType(StringType(), StringType()), True),
    StructField("metadata", StructType([
        StructField("source", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("partition_key", StringType(), True),
        StructField("retention_days", IntegerType(), True),
    ]), True),
])


# ============================================================================
# BRONZE LAYER: Raw Ingestion
# ============================================================================

def run_bronze_layer(spark: SparkSession, input_dir: str, output_dir: str) -> DataFrame:
    """
    Bronze Layer: Ingest raw events with minimal transformation.
    Adds ingestion metadata, partitions by date.
    """
    print("\n" + "=" * 65)
    print("  BRONZE LAYER: Raw Ingestion")
    print("=" * 65)

    bronze_path = os.path.join(output_dir, "bronze")

    # Read all JSON files from landing directory
    raw_df = (
        spark.read
        .schema(EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(input_dir)
    )

    total_raw = raw_df.count()
    print(f"  Raw events read: {total_raw:,}")

    # Add Bronze metadata columns
    bronze_df = (
        raw_df
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_bronze_source_file", F.input_file_name())
        .withColumn("_bronze_batch_id", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")))
        .withColumn("_event_date", F.to_date(F.col("event_timestamp")))
    )

    # Separate valid and invalid records
    valid_df = bronze_df.filter(
        F.col("event_id").isNotNull() &
        F.col("tenant_id").isNotNull() &
        F.col("event_timestamp").isNotNull()
    )
    invalid_df = bronze_df.filter(
        F.col("event_id").isNull() |
        F.col("tenant_id").isNull() |
        F.col("event_timestamp").isNull()
    )

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    # Write Bronze Delta table
    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("_event_date")
        .save(bronze_path)
    )

    # Write DLQ for invalid records
    if invalid_count > 0:
        dlq_path = os.path.join(output_dir, "bronze_dlq")
        invalid_df.write.format("delta").mode("overwrite").save(dlq_path)

    print(f"  Valid events:   {valid_count:,}")
    print(f"  Invalid (DLQ):  {invalid_count:,}")
    print(f"  Output:         {bronze_path}/")
    print(f"  Bronze layer complete\n")

    return valid_df


# ============================================================================
# SILVER LAYER: Deduplication + Validation + Flattening
# ============================================================================

def run_silver_layer(spark: SparkSession, bronze_df: DataFrame, output_dir: str) -> DataFrame:
    """
    Silver Layer: Deduplicate, validate, flatten nested structures.
    Applies business rules and quarantines bad data.
    """
    print("=" * 65)
    print("  SILVER LAYER: Dedup + Validate + Flatten")
    print("=" * 65)

    silver_path = os.path.join(output_dir, "silver")
    quarantine_path = os.path.join(output_dir, "silver_quarantine")

    initial_count = bronze_df.count()

    # ---- Step 1: Deduplication (by event_id, keep earliest) ----
    window_spec = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").asc())
    deduped_df = (
        bronze_df
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    dedup_count = deduped_df.count()
    duplicates = initial_count - dedup_count

    # ---- Step 2: Flatten nested structs ----
    silver_df = (
        deduped_df
        .select(
            # Core fields
            F.col("event_id"),
            F.col("event_timestamp"),
            F.col("ingestion_timestamp"),
            F.col("sequence_number"),
            F.col("tenant_id"),

            # Subscription (flattened)
            F.col("subscription.subscription_id").alias("subscription_id"),
            F.col("subscription.plan_tier").alias("plan_tier"),
            F.col("subscription.billing_cycle_id").alias("billing_cycle_id"),

            # Identity (flattened)
            F.col("identity.api_key_id").alias("api_key_id"),
            F.col("identity.user_id").alias("user_id"),
            F.col("identity.auth_method").alias("auth_method"),

            # Request (flattened)
            F.col("request.request_id").alias("request_id"),
            F.col("request.trace_id").alias("trace_id"),
            F.col("request.method").alias("http_method"),
            F.col("request.path").alias("endpoint_path"),
            F.col("request.endpoint_id").alias("endpoint_id"),
            F.col("request.content_length").alias("request_content_length"),

            # Response (flattened)
            F.col("response.status_code").alias("status_code"),
            F.col("response.status_category").alias("status_category"),
            F.col("response.content_length").alias("response_content_length"),
            F.col("response.latency_ms").alias("latency_ms"),
            F.col("response.cache_status").alias("cache_status"),
            F.col("response.error_code").alias("error_code"),

            # Rate Limiting (flattened)
            F.col("rate_limiting.limit").alias("rate_limit"),
            F.col("rate_limiting.remaining").alias("rate_limit_remaining"),
            F.col("rate_limiting.was_rate_limited").alias("was_rate_limited"),
            F.col("rate_limiting.quota_used_percent").alias("quota_used_percent"),

            # Abuse Signals (flattened)
            F.col("abuse_signals.risk_score").alias("risk_score"),
            F.col("abuse_signals.velocity_1min").alias("velocity_1min"),
            F.col("abuse_signals.is_suspicious").alias("is_suspicious"),
            F.col("abuse_signals.action_taken").alias("action_taken"),

            # Geo
            F.col("geo.country_code").alias("country_code"),
            F.col("geo.gateway_region") if "gateway_region" in [f.name for f in deduped_df.schema["geo"].dataType.fields] else F.lit(None).alias("geo_region"),
            F.col("gateway.gateway_region").alias("gateway_region"),

            # Billing (flattened)
            F.col("billing.billable").alias("is_billable"),
            F.col("billing.billing_units").alias("billing_units"),
            F.col("billing.cost_estimate_usd").alias("cost_estimate_usd"),

            # Metadata
            F.col("metadata.source").alias("event_source"),
            F.col("_event_date"),
            F.col("_bronze_ingested_at"),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # ---- Step 3: Data Quality Validation ----
    silver_df = silver_df.withColumn(
        "_quality_flags",
        F.array(
            F.when(F.col("status_code").isNull(), F.lit("missing_status_code")),
            F.when(F.col("latency_ms") < 0, F.lit("negative_latency")),
            F.when(F.col("latency_ms") > 30000, F.lit("extreme_latency")),
            F.when(F.col("risk_score") > 100, F.lit("invalid_risk_score")),
            F.when(F.col("billing_units") < 0, F.lit("negative_billing")),
        )
    )
    # Remove nulls from quality flags array
    silver_df = silver_df.withColumn(
        "_quality_flags",
        F.expr("filter(_quality_flags, x -> x is not null)")
    )
    silver_df = silver_df.withColumn(
        "_is_valid",
        F.size(F.col("_quality_flags")) == 0
    )

    # Quarantine invalid records
    quarantine_df = silver_df.filter(~F.col("_is_valid"))
    valid_silver_df = silver_df.filter(F.col("_is_valid"))

    quarantine_count = quarantine_df.count()
    valid_silver_count = valid_silver_df.count()

    # Write Silver Delta table
    (
        valid_silver_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("_event_date")
        .save(silver_path)
    )

    # Write Quarantine
    if quarantine_count > 0:
        quarantine_df.write.format("delta").mode("overwrite").save(quarantine_path)

    print(f"  Input from Bronze: {initial_count:,}")
    print(f"  Duplicates removed: {duplicates:,}")
    print(f"  Quality failures:   {quarantine_count:,}")
    print(f"  Valid Silver output: {valid_silver_count:,}")
    print(f"  Output:             {silver_path}/")
    print(f"  Silver layer complete\n")

    return valid_silver_df


# ============================================================================
# GOLD LAYER: Aggregations + Enforcement Alerts
# ============================================================================

def run_gold_layer(spark: SparkSession, silver_df: DataFrame, output_dir: str) -> dict:
    """
    Gold Layer: Business-level aggregations and enforcement alerts.
    Produces 4 Gold tables consumed by the Streamlit dashboard.
    """
    print("=" * 65)
    print("  GOLD LAYER: Aggregate + Enforce")
    print("=" * 65)

    gold_dir = os.path.join(output_dir, "gold")

    # Ensure event_timestamp is properly typed
    silver_ts = silver_df.withColumn(
        "event_ts", F.to_timestamp(F.col("event_timestamp"))
    )

    # ---- Gold Table 1: Tenant Usage Summary ----
    tenant_usage = (
        silver_ts.groupBy("tenant_id", "plan_tier")
        .agg(
            F.count("*").alias("total_requests"),
            F.countDistinct("event_id").alias("unique_events"),
            F.sum(F.when(F.col("status_code") < 400, 1).otherwise(0)).alias("successful_requests"),
            F.sum(F.when(F.col("status_code") >= 400, 1).otherwise(0)).alias("failed_requests"),
            F.sum(F.when(F.col("was_rate_limited"), 1).otherwise(0)).alias("rate_limited_count"),
            F.sum(F.when(F.col("is_suspicious"), 1).otherwise(0)).alias("suspicious_count"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.expr("percentile_approx(latency_ms, 0.5)").alias("p50_latency_ms"),
            F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
            F.expr("percentile_approx(latency_ms, 0.99)").alias("p99_latency_ms"),
            F.sum("billing_units").alias("total_billing_units"),
            F.sum("cost_estimate_usd").alias("total_cost_usd"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.max("risk_score").alias("max_risk_score"),
            F.avg("quota_used_percent").alias("avg_quota_used_pct"),
            F.max("quota_used_percent").alias("max_quota_used_pct"),
            F.countDistinct("api_key_id").alias("unique_api_keys"),
            F.countDistinct("endpoint_path").alias("unique_endpoints"),
            F.min("event_ts").alias("first_event_at"),
            F.max("event_ts").alias("last_event_at"),
        )
        .withColumn("success_rate", F.round(F.col("successful_requests") / F.col("total_requests") * 100, 2))
        .withColumn("rate_limit_rate", F.round(F.col("rate_limited_count") / F.col("total_requests") * 100, 2))
        .withColumn("_gold_computed_at", F.current_timestamp())
    )

    tenant_usage_path = os.path.join(gold_dir, "tenant_usage")
    tenant_usage.write.format("delta").mode("overwrite").save(tenant_usage_path)
    tenant_count = tenant_usage.count()
    print(f"  Tenant Usage Summary:    {tenant_count:,} tenants → {tenant_usage_path}/")

    # ---- Gold Table 2: Endpoint Analytics ----
    endpoint_analytics = (
        silver_ts.groupBy("endpoint_path", "http_method")
        .agg(
            F.count("*").alias("total_calls"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
            F.sum(F.when(F.col("status_code") < 400, 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("status_code") >= 500, 1).otherwise(0)).alias("error_count"),
            F.sum(F.when(F.col("status_code") == 429, 1).otherwise(0)).alias("rate_limited_count"),
            F.avg("billing_units").alias("avg_billing_units"),
            F.countDistinct("tenant_id").alias("unique_tenants"),
        )
        .withColumn("error_rate", F.round(F.col("error_count") / F.col("total_calls") * 100, 2))
        .withColumn("_gold_computed_at", F.current_timestamp())
    )

    endpoint_path = os.path.join(gold_dir, "endpoint_analytics")
    endpoint_analytics.write.format("delta").mode("overwrite").save(endpoint_path)
    endpoint_count = endpoint_analytics.count()
    print(f"  Endpoint Analytics:      {endpoint_count:,} endpoints → {endpoint_path}/")

    # ---- Gold Table 3: Enforcement Alerts ----
    plan_limits = {"free": 60, "starter": 300, "professional": 1000, "enterprise": 5000}

    enforcement_alerts = (
        silver_ts.groupBy("tenant_id", "plan_tier")
        .agg(
            F.max("velocity_1min").alias("peak_velocity_1min"),
            F.max("risk_score").alias("max_risk_score"),
            F.sum(F.when(F.col("was_rate_limited"), 1).otherwise(0)).alias("rate_limited_requests"),
            F.sum(F.when(F.col("is_suspicious"), 1).otherwise(0)).alias("suspicious_requests"),
            F.count("*").alias("total_requests"),
            F.avg("quota_used_percent").alias("avg_quota_pct"),
        )
    )

    # Generate alerts based on thresholds
    enforcement_alerts = (
        enforcement_alerts
        .withColumn("alert_type",
            F.when(F.col("max_risk_score") > 75, F.lit("ABUSE_DETECTED"))
            .when(F.col("rate_limited_requests") > 10, F.lit("RATE_LIMIT_BREACH"))
            .when(F.col("avg_quota_pct") > 80, F.lit("QUOTA_WARNING"))
            .when(F.col("suspicious_requests") > 5, F.lit("SUSPICIOUS_ACTIVITY"))
            .otherwise(F.lit("HEALTHY"))
        )
        .withColumn("severity",
            F.when(F.col("alert_type") == "ABUSE_DETECTED", F.lit("CRITICAL"))
            .when(F.col("alert_type") == "RATE_LIMIT_BREACH", F.lit("HIGH"))
            .when(F.col("alert_type") == "QUOTA_WARNING", F.lit("MEDIUM"))
            .when(F.col("alert_type") == "SUSPICIOUS_ACTIVITY", F.lit("HIGH"))
            .otherwise(F.lit("LOW"))
        )
        .withColumn("recommended_action",
            F.when(F.col("alert_type") == "ABUSE_DETECTED", F.lit("Block API key, investigate"))
            .when(F.col("alert_type") == "RATE_LIMIT_BREACH", F.lit("Enforce rate limits, notify tenant"))
            .when(F.col("alert_type") == "QUOTA_WARNING", F.lit("Send quota warning email"))
            .when(F.col("alert_type") == "SUSPICIOUS_ACTIVITY", F.lit("Enable enhanced monitoring"))
            .otherwise(F.lit("No action needed"))
        )
        .withColumn("_gold_computed_at", F.current_timestamp())
    )

    alerts_path = os.path.join(gold_dir, "enforcement_alerts")
    enforcement_alerts.write.format("delta").mode("overwrite").save(alerts_path)
    alert_count = enforcement_alerts.filter(F.col("alert_type") != "HEALTHY").count()
    total_alert_rows = enforcement_alerts.count()
    print(f"  Enforcement Alerts:      {alert_count:,} active alerts ({total_alert_rows:,} tenants evaluated) → {alerts_path}/")

    # ---- Gold Table 4: Status Code Distribution ----
    status_distribution = (
        silver_ts.groupBy("status_code", "status_category", "plan_tier")
        .agg(
            F.count("*").alias("count"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.countDistinct("tenant_id").alias("tenant_count"),
        )
        .withColumn("_gold_computed_at", F.current_timestamp())
    )

    status_path = os.path.join(gold_dir, "status_distribution")
    status_distribution.write.format("delta").mode("overwrite").save(status_path)
    status_rows = status_distribution.count()
    print(f"  Status Distribution:     {status_rows:,} combos → {status_path}/")

    # ---- Gold Table 5: Geo Distribution ----
    geo_distribution = (
        silver_ts.groupBy("country_code", "gateway_region")
        .agg(
            F.count("*").alias("request_count"),
            F.avg("latency_ms").alias("avg_latency_ms"),
            F.countDistinct("tenant_id").alias("unique_tenants"),
            F.sum(F.when(F.col("is_suspicious"), 1).otherwise(0)).alias("suspicious_count"),
        )
        .withColumn("_gold_computed_at", F.current_timestamp())
    )

    geo_path = os.path.join(gold_dir, "geo_distribution")
    geo_distribution.write.format("delta").mode("overwrite").save(geo_path)
    geo_rows = geo_distribution.count()
    print(f"  Geo Distribution:        {geo_rows:,} regions → {geo_path}/")

    # ---- Gold Table 6: Data Quality Metrics ----
    quality_metrics_data = [
        {
            "layer": "bronze",
            "total_records": silver_df.count(),
            "valid_records": silver_df.count(),
            "invalid_records": 0,
            "duplicate_rate": 0.0,
            "null_rate": 0.0,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }
    ]
    quality_df = spark.createDataFrame(
        [(
            "pipeline_run",
            silver_df.count(),
            tenant_count,
            endpoint_count,
            alert_count,
            datetime.now(timezone.utc).isoformat(),
        )],
        ["run_type", "total_events_processed", "unique_tenants", "unique_endpoints",
         "active_alerts", "computed_at"]
    )

    quality_path = os.path.join(gold_dir, "pipeline_metrics")
    quality_df.write.format("delta").mode("overwrite").save(quality_path)
    print(f"  Pipeline Metrics:        1 summary row → {quality_path}/")

    print(f"\n  Gold layer complete")
    print(f"  All Gold tables written to: {gold_dir}/\n")

    return {
        "tenant_count": tenant_count,
        "endpoint_count": endpoint_count,
        "alert_count": alert_count,
        "status_combos": status_rows,
        "geo_regions": geo_rows,
    }


# ============================================================================
# Pipeline Orchestrator
# ============================================================================

def run_full_pipeline(input_dir: str, output_dir: str):
    """Run the complete Bronze → Silver → Gold pipeline."""
    start_time = time.time()

    print("\n" + "=" * 65)
    print("  SUBSCRIPTION ENFORCEMENT PIPELINE")
    print("  Medallion Architecture: Bronze → Silver → Gold")
    print("=" * 65)
    print(f"  Input:  {input_dir}/")
    print(f"  Output: {output_dir}/")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check input directory
    if not os.path.exists(input_dir):
        print(f"\n  ERROR: Input directory not found: {input_dir}")
        print(f"  Run first: python demo/collect_live_data.py")
        sys.exit(1)

    json_files = [f for f in os.listdir(input_dir) if f.endswith(".json")]
    if not json_files:
        print(f"\n  ERROR: No JSON files in {input_dir}")
        print(f"  Run first: python demo/collect_live_data.py")
        sys.exit(1)

    print(f"  JSON files found: {len(json_files)}")

    # Create Spark session
    print("\n  Initializing Spark + Delta Lake...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("  Spark session created successfully\n")

    try:
        # Bronze
        bronze_df = run_bronze_layer(spark, input_dir, output_dir)

        # Silver
        silver_df = run_silver_layer(spark, bronze_df, output_dir)

        # Gold
        gold_stats = run_gold_layer(spark, silver_df, output_dir)

        # Summary
        elapsed = time.time() - start_time
        total_events = silver_df.count()

        print("=" * 65)
        print("  PIPELINE COMPLETE")
        print("=" * 65)
        print(f"  Total events processed:  {total_events:,}")
        print(f"  Unique tenants:          {gold_stats['tenant_count']:,}")
        print(f"  Unique endpoints:        {gold_stats['endpoint_count']:,}")
        print(f"  Active alerts:           {gold_stats['alert_count']:,}")
        print(f"  Execution time:          {elapsed:.1f} seconds")
        print(f"  Throughput:              {total_events / max(elapsed, 0.1):,.0f} events/sec")
        print(f"")
        print(f"  Delta tables written to: {output_dir}/")
        print(f"")
        print(f"  Next: Launch the dashboard:")
        print(f"    streamlit run demo/dashboard.py")
        print("=" * 65)

    finally:
        spark.stop()


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Run Bronze → Silver → Gold pipeline locally")
    parser.add_argument("--input-dir", default="./demo/landing_data",
                        help="Landing directory with JSON event files")
    parser.add_argument("--output-dir", default="./demo/delta_tables",
                        help="Output directory for Delta tables")

    args = parser.parse_args()
    run_full_pipeline(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
