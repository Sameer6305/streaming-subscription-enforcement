"""
Data Quality Checks for Streaming Pipeline
===========================================

Validates data quality across Bronze → Silver → Gold layers:
- Schema validation (structure, types, constraints)
- Volume anomaly detection (sudden drops/spikes)
- Null checks on critical fields
- Business rule validation
- Metric tracking and alerting

Usage:
    # Bronze layer - schema validation
    quality_checker = DataQualityChecker(layer="bronze")
    validated_df = quality_checker.validate_and_flag(bronze_df)
    
    # Silver layer - full validation
    quality_checker = DataQualityChecker(layer="silver")
    validated_df = quality_checker.validate_and_flag(silver_df)
    
    # Write quality metrics
    quality_checker.write_quality_metrics(spark, validated_df)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, coalesce, count, sum as spark_sum, avg, stddev,
    current_timestamp, lit, struct, array, size, length,
    isnan, isnull, expr, window, lag, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, DoubleType, BooleanType, IntegerType, MapType
)
from pyspark.sql.window import Window
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import json


class DataQualityChecker:
    """
    Comprehensive data quality validation framework for streaming pipeline.
    
    Validation Levels:
    1. Schema Validation (structure, types, required fields)
    2. Value Validation (ranges, formats, business rules)
    3. Volume Validation (anomaly detection)
    4. Referential Integrity (tenant_id, subscription_plan)
    
    Failure Handling:
    - Invalid records flagged with quality_check_failed=true
    - Quality metrics written to monitoring table
    - Alerts triggered for critical failures
    """
    
    # Critical fields that CANNOT be null
    CRITICAL_FIELDS = {
        "bronze": [
            "event_id",
            "event_timestamp",
            "tenant_id",
            "ingestion_timestamp",
            "kafka_offset",
            "kafka_partition"
        ],
        "silver": [
            "event_id",
            "event_timestamp",
            "tenant_id",
            "api_key_id",
            "endpoint",
            "http_method",
            "response_status_code",
            "subscription_plan"
        ],
        "gold": [
            "tenant_id",
            "window_start",
            "window_end",
            "total_requests",
            "subscription_plan"
        ]
    }
    
    # Expected value ranges for validation
    VALUE_CONSTRAINTS = {
        "response_status_code": (100, 599),  # Valid HTTP status codes
        "response_time_ms": (0, 300000),     # 0-5 minutes (300,000 ms)
        "request_size_bytes": (0, 100_000_000),  # 0-100 MB
        "response_size_bytes": (0, 500_000_000),  # 0-500 MB
        "risk_score": (0.0, 1.0),            # Normalized risk score
        "velocity_last_1min": (0, 10000),    # Max 10k req/min per key
        "velocity_last_5min": (0, 50000),    # Max 50k req/5min per key
    }
    
    # Valid enum values
    VALID_ENUMS = {
        "http_method": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
        "subscription_plan": ["free", "starter", "professional", "enterprise"],
        "rate_limit_status": ["allowed", "rate_limited", "quota_exceeded"],
        "abuse_detected": [True, False, None],  # None allowed (no abuse check)
    }
    
    # Volume anomaly thresholds (Z-score based)
    VOLUME_ANOMALY_THRESHOLD = 3.0  # 3 standard deviations
    VOLUME_HISTORY_HOURS = 24       # Look back 24 hours for baseline
    
    def __init__(self, layer: str, alert_on_failure: bool = True):
        """
        Initialize data quality checker.
        
        Args:
            layer: Pipeline layer ("bronze", "silver", "gold")
            alert_on_failure: Whether to write alerts on quality failures
        """
        self.layer = layer.lower()
        self.alert_on_failure = alert_on_failure
        
        if self.layer not in ["bronze", "silver", "gold"]:
            raise ValueError(f"Invalid layer: {layer}. Must be bronze/silver/gold.")
    
    def validate_schema(self, df: DataFrame) -> DataFrame:
        """
        Validate schema structure and add quality check flags.
        
        Checks:
        - Required fields present
        - Correct data types
        - No unexpected nulls in critical fields
        
        Returns DataFrame with added columns:
        - schema_valid: bool
        - schema_errors: array<string>
        """
        schema_errors = []
        
        # Check critical fields exist and are non-null
        for field_name in self.CRITICAL_FIELDS.get(self.layer, []):
            if field_name in df.columns:
                # Add null check for this field
                schema_errors.append(
                    when(col(field_name).isNull(), lit(f"null_{field_name}"))
                )
            else:
                # Field missing entirely
                schema_errors.append(lit(f"missing_{field_name}"))
        
        # Combine all error conditions
        if schema_errors:
            df = df.withColumn(
                "schema_errors",
                array(*schema_errors).cast("array<string>")
            ).withColumn(
                "schema_errors",
                expr("filter(schema_errors, x -> x IS NOT NULL)")
            )
        else:
            df = df.withColumn("schema_errors", array().cast("array<string>"))
        
        # Schema is valid if no errors
        df = df.withColumn(
            "schema_valid",
            size(col("schema_errors")) == 0
        )
        
        return df
    
    def validate_values(self, df: DataFrame) -> DataFrame:
        """
        Validate field values against business rules.
        
        Checks:
        - Numeric ranges (status codes, response times)
        - Enum values (http_method, subscription_plan)
        - String formats (tenant_id, event_id)
        - Timestamp logic (event_timestamp <= ingestion_timestamp)
        
        Returns DataFrame with added columns:
        - value_valid: bool
        - value_errors: array<string>
        """
        value_errors = []
        
        # Check numeric ranges
        for field_name, (min_val, max_val) in self.VALUE_CONSTRAINTS.items():
            if field_name in df.columns:
                value_errors.append(
                    when(
                        col(field_name).isNotNull() & (
                            (col(field_name) < min_val) | 
                            (col(field_name) > max_val)
                        ),
                        lit(f"invalid_range_{field_name}")
                    )
                )
        
        # Check enum values
        for field_name, valid_values in self.VALID_ENUMS.items():
            if field_name in df.columns:
                # Filter out None if it's allowed
                valid_check = col(field_name).isin(valid_values)
                if None in valid_values:
                    valid_check = valid_check | col(field_name).isNull()
                
                value_errors.append(
                    when(~valid_check, lit(f"invalid_enum_{field_name}"))
                )
        
        # Check event timestamp logic (event_timestamp <= ingestion_timestamp)
        if "event_timestamp" in df.columns and "ingestion_timestamp" in df.columns:
            value_errors.append(
                when(
                    col("event_timestamp") > col("ingestion_timestamp"),
                    lit("future_event_timestamp")
                )
            )
        
        # Check string formats (tenant_id should not be empty)
        if "tenant_id" in df.columns:
            value_errors.append(
                when(
                    (col("tenant_id").isNotNull()) & (length(col("tenant_id")) == 0),
                    lit("empty_tenant_id")
                )
            )
        
        # Check event_id format (should be UUID-like: 36 chars with hyphens)
        if "event_id" in df.columns:
            value_errors.append(
                when(
                    (col("event_id").isNotNull()) & (length(col("event_id")) != 36),
                    lit("invalid_event_id_format")
                )
            )
        
        # Combine all value error conditions
        if value_errors:
            df = df.withColumn(
                "value_errors",
                array(*value_errors).cast("array<string>")
            ).withColumn(
                "value_errors",
                expr("filter(value_errors, x -> x IS NOT NULL)")
            )
        else:
            df = df.withColumn("value_errors", array().cast("array<string>"))
        
        # Values are valid if no errors
        df = df.withColumn(
            "value_valid",
            size(col("value_errors")) == 0
        )
        
        return df
    
    def check_null_critical_fields(self, df: DataFrame) -> DataFrame:
        """
        Explicit null checks on critical fields.
        
        Returns DataFrame with added columns:
        - null_check_passed: bool
        - null_fields: array<string> - list of fields that are null
        """
        critical_fields = self.CRITICAL_FIELDS.get(self.layer, [])
        
        # Build list of null field names
        null_checks = []
        for field_name in critical_fields:
            if field_name in df.columns:
                null_checks.append(
                    when(col(field_name).isNull(), lit(field_name))
                )
        
        if null_checks:
            df = df.withColumn(
                "null_fields",
                array(*null_checks).cast("array<string>")
            ).withColumn(
                "null_fields",
                expr("filter(null_fields, x -> x IS NOT NULL)")
            )
        else:
            df = df.withColumn("null_fields", array().cast("array<string>"))
        
        # Null check passes if no null critical fields
        df = df.withColumn(
            "null_check_passed",
            size(col("null_fields")) == 0
        )
        
        return df
    
    def validate_and_flag(self, df: DataFrame) -> DataFrame:
        """
        Run all validation checks and add quality flags.
        
        Returns DataFrame with added columns:
        - quality_check_passed: bool (overall pass/fail)
        - quality_check_errors: map<string, array<string>> (all errors)
        - quality_check_timestamp: timestamp
        """
        # Run all validation checks
        df = self.validate_schema(df)
        df = self.validate_values(df)
        df = self.check_null_critical_fields(df)
        
        # Combine all error types into single map
        df = df.withColumn(
            "quality_check_errors",
            expr("""
                map(
                    'schema_errors', schema_errors,
                    'value_errors', value_errors,
                    'null_fields', null_fields
                )
            """)
        )
        
        # Overall quality check passes if all individual checks pass
        df = df.withColumn(
            "quality_check_passed",
            col("schema_valid") & col("value_valid") & col("null_check_passed")
        )
        
        # Add validation timestamp
        df = df.withColumn("quality_check_timestamp", current_timestamp())
        
        # Drop intermediate columns (keep only final flags)
        df = df.drop(
            "schema_valid", "schema_errors",
            "value_valid", "value_errors",
            "null_check_passed", "null_fields"
        )
        
        return df
    
    def detect_volume_anomalies(
        self, 
        spark: SparkSession,
        current_count: int,
        table_path: str,
        window_minutes: int = 60
    ) -> Tuple[bool, float, Dict]:
        """
        Detect volume anomalies using Z-score analysis.
        
        Compares current batch volume to historical baseline (last 24 hours).
        
        Args:
            spark: SparkSession
            current_count: Number of records in current batch
            table_path: Path to table for historical analysis
            window_minutes: Window size for aggregation (default: 60)
        
        Returns:
            (is_anomaly, z_score, metrics)
            - is_anomaly: True if volume is anomalous
            - z_score: Standard deviations from mean
            - metrics: Dict with baseline stats
        """
        try:
            # Read historical data (last 24 hours)
            historical_df = spark.read.format("delta").load(table_path)
            
            if "quality_check_timestamp" in historical_df.columns:
                timestamp_col = "quality_check_timestamp"
            elif "ingestion_timestamp" in historical_df.columns:
                timestamp_col = "ingestion_timestamp"
            elif "event_timestamp" in historical_df.columns:
                timestamp_col = "event_timestamp"
            else:
                # Cannot perform volume analysis without timestamp
                return False, 0.0, {"error": "no_timestamp_column"}
            
            # Aggregate by window
            lookback = datetime.now() - timedelta(hours=self.VOLUME_HISTORY_HOURS)
            
            volume_df = historical_df.filter(
                col(timestamp_col) >= lit(lookback)
            ).groupBy(
                window(col(timestamp_col), f"{window_minutes} minutes")
            ).agg(
                count("*").alias("record_count")
            )
            
            # Calculate baseline statistics
            stats = volume_df.agg(
                avg("record_count").alias("mean"),
                stddev("record_count").alias("stddev"),
                spark_sum(when(col("record_count") > 0, 1)).alias("non_zero_windows")
            ).first()
            
            mean_volume = stats["mean"] or 0
            stddev_volume = stats["stddev"] or 0
            non_zero_windows = stats["non_zero_windows"] or 0
            
            # Need at least 12 data points for meaningful analysis
            if non_zero_windows < 12:
                return False, 0.0, {
                    "warning": "insufficient_history",
                    "windows": non_zero_windows,
                    "required": 12
                }
            
            # Calculate Z-score
            if stddev_volume > 0:
                z_score = (current_count - mean_volume) / stddev_volume
            else:
                z_score = 0.0
            
            # Anomaly if beyond threshold
            is_anomaly = abs(z_score) > self.VOLUME_ANOMALY_THRESHOLD
            
            metrics = {
                "current_count": current_count,
                "mean_volume": float(mean_volume),
                "stddev_volume": float(stddev_volume),
                "z_score": float(z_score),
                "threshold": self.VOLUME_ANOMALY_THRESHOLD,
                "lookback_hours": self.VOLUME_HISTORY_HOURS,
                "sample_windows": int(non_zero_windows)
            }
            
            return is_anomaly, z_score, metrics
            
        except Exception as e:
            # Cannot perform volume analysis (e.g., table doesn't exist yet)
            return False, 0.0, {"error": str(e)}
    
    def get_quality_metrics_schema(self) -> StructType:
        """Get schema for quality metrics table."""
        return StructType([
            StructField("metric_id", StringType(), False),
            StructField("metric_timestamp", TimestampType(), False),
            StructField("layer", StringType(), False),
            StructField("batch_id", LongType(), True),
            
            # Volume metrics
            StructField("total_records", LongType(), False),
            StructField("valid_records", LongType(), False),
            StructField("invalid_records", LongType(), False),
            StructField("quality_pass_rate", DoubleType(), False),
            
            # Volume anomaly
            StructField("volume_anomaly_detected", BooleanType(), False),
            StructField("volume_z_score", DoubleType(), True),
            StructField("volume_baseline", MapType(StringType(), DoubleType()), True),
            
            # Error breakdown
            StructField("schema_errors", LongType(), False),
            StructField("value_errors", LongType(), False),
            StructField("null_errors", LongType(), False),
            
            # Top error types (for alerting)
            StructField("top_error_types", MapType(StringType(), LongType()), True),
        ])
    
    def write_quality_metrics(
        self, 
        spark: SparkSession,
        validated_df: DataFrame,
        batch_id: Optional[int] = None,
        table_path: str = "/data/quality_metrics"
    ) -> Dict:
        """
        Calculate and write quality metrics to monitoring table.
        
        Args:
            spark: SparkSession
            validated_df: DataFrame with quality_check_passed column
            batch_id: Optional batch ID for streaming queries
            table_path: Path to quality metrics Delta table
        
        Returns:
            Dict with quality metrics
        """
        # Calculate quality metrics
        metrics_row = validated_df.agg(
            count("*").alias("total_records"),
            spark_sum(when(col("quality_check_passed"), 1).otherwise(0)).alias("valid_records"),
            spark_sum(when(~col("quality_check_passed"), 1).otherwise(0)).alias("invalid_records"),
        ).first()
        
        total_records = metrics_row["total_records"]
        valid_records = metrics_row["valid_records"]
        invalid_records = metrics_row["invalid_records"]
        quality_pass_rate = valid_records / total_records if total_records > 0 else 1.0
        
        # Count error types (explode the quality_check_errors map)
        error_counts = validated_df.filter(~col("quality_check_passed")) \
            .select(expr("explode(quality_check_errors) as (error_type, errors)")) \
            .select("error_type", expr("explode(errors) as error")) \
            .groupBy("error") \
            .count() \
            .orderBy(col("count").desc()) \
            .limit(10) \
            .collect()
        
        top_error_types = {row["error"]: int(row["count"]) for row in error_counts}
        
        # Detect volume anomalies
        is_anomaly, z_score, volume_baseline = self.detect_volume_anomalies(
            spark=spark,
            current_count=total_records,
            table_path=table_path.replace("quality_metrics", f"{self.layer}_api_usage")
        )
        
        # Build metrics record
        metrics = {
            "metric_id": f"{self.layer}-{datetime.now().isoformat()}",
            "metric_timestamp": datetime.now(),
            "layer": self.layer,
            "batch_id": batch_id,
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "quality_pass_rate": quality_pass_rate,
            "volume_anomaly_detected": is_anomaly,
            "volume_z_score": z_score,
            "volume_baseline": volume_baseline,
            "schema_errors": sum(1 for e in top_error_types if "schema" in e or "null" in e or "missing" in e),
            "value_errors": sum(1 for e in top_error_types if "invalid" in e or "range" in e or "enum" in e),
            "null_errors": sum(1 for e in top_error_types if "null" in e),
            "top_error_types": top_error_types
        }
        
        # Write to Delta table
        metrics_df = spark.createDataFrame([metrics], schema=self.get_quality_metrics_schema())
        
        metrics_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(table_path)
        
        # Trigger alerts if quality is poor or volume anomaly detected
        if self.alert_on_failure:
            if quality_pass_rate < 0.95:  # Less than 95% pass rate
                self._write_quality_alert(
                    spark=spark,
                    alert_type="low_quality_rate",
                    severity="warning",
                    details=metrics
                )
            
            if quality_pass_rate < 0.80:  # Less than 80% pass rate
                self._write_quality_alert(
                    spark=spark,
                    alert_type="critical_quality_rate",
                    severity="critical",
                    details=metrics
                )
            
            if is_anomaly:
                self._write_quality_alert(
                    spark=spark,
                    alert_type="volume_anomaly",
                    severity="warning" if abs(z_score) < 4 else "critical",
                    details=metrics
                )
        
        return metrics
    
    def _write_quality_alert(
        self,
        spark: SparkSession,
        alert_type: str,
        severity: str,
        details: Dict,
        alert_table_path: str = "/data/quality_alerts"
    ):
        """Write quality alert to monitoring table."""
        alert = {
            "alert_id": f"{alert_type}-{datetime.now().isoformat()}",
            "alert_timestamp": datetime.now(),
            "layer": self.layer,
            "alert_type": alert_type,
            "severity": severity,
            "message": self._get_alert_message(alert_type, details),
            "details": json.dumps(details),
        }
        
        alert_schema = StructType([
            StructField("alert_id", StringType(), False),
            StructField("alert_timestamp", TimestampType(), False),
            StructField("layer", StringType(), False),
            StructField("alert_type", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("message", StringType(), False),
            StructField("details", StringType(), True),
        ])
        
        alert_df = spark.createDataFrame([alert], schema=alert_schema)
        
        alert_df.write \
            .format("delta") \
            .mode("append") \
            .save(alert_table_path)
    
    def _get_alert_message(self, alert_type: str, details: Dict) -> str:
        """Generate human-readable alert message."""
        if alert_type == "low_quality_rate":
            return (
                f"Data quality below 95% in {self.layer} layer. "
                f"Pass rate: {details['quality_pass_rate']:.2%}. "
                f"Invalid records: {details['invalid_records']:,}"
            )
        elif alert_type == "critical_quality_rate":
            return (
                f"CRITICAL: Data quality below 80% in {self.layer} layer. "
                f"Pass rate: {details['quality_pass_rate']:.2%}. "
                f"Invalid records: {details['invalid_records']:,}. "
                f"Top errors: {', '.join(list(details['top_error_types'].keys())[:3])}"
            )
        elif alert_type == "volume_anomaly":
            z_score = details["volume_z_score"]
            direction = "spike" if z_score > 0 else "drop"
            return (
                f"Volume {direction} detected in {self.layer} layer. "
                f"Current: {details['total_records']:,} records. "
                f"Z-score: {z_score:.2f} (threshold: {details['volume_baseline'].get('threshold', 3.0)})"
            )
        else:
            return f"Quality alert: {alert_type}"


def handle_failed_records(
    validated_df: DataFrame,
    quarantine_path: str,
    layer: str
) -> DataFrame:
    """
    Separate failed records into quarantine table.
    
    Args:
        validated_df: DataFrame with quality_check_passed column
        quarantine_path: Path to quarantine Delta table
        layer: Layer name for tracking
    
    Returns:
        DataFrame with only valid records (quality_check_passed=true)
    """
    # Extract failed records
    failed_df = validated_df.filter(~col("quality_check_passed")) \
        .withColumn("quarantine_timestamp", current_timestamp()) \
        .withColumn("quarantine_layer", lit(layer))
    
    # Write to quarantine (append mode)
    failed_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(quarantine_path)
    
    # Return only valid records for downstream processing
    valid_df = validated_df.filter(col("quality_check_passed"))
    
    return valid_df


# Example usage in streaming context
if __name__ == "__main__":
    """
    Example: Integrate data quality checks into Silver processing.
    """
    spark = SparkSession.builder \
        .appName("DataQualityExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Read Bronze stream
    bronze_df = spark.readStream \
        .format("delta") \
        .load("/data/bronze_api_usage")
    
    # Initialize quality checker
    quality_checker = DataQualityChecker(layer="bronze", alert_on_failure=True)
    
    # Validate data
    validated_df = quality_checker.validate_and_flag(bronze_df)
    
    # Define function to process each micro-batch
    def process_batch(batch_df: DataFrame, batch_id: int):
        # Write quality metrics
        metrics = quality_checker.write_quality_metrics(
            spark=spark,
            validated_df=batch_df,
            batch_id=batch_id
        )
        
        print(f"Batch {batch_id} Quality Metrics:")
        print(f"  Total: {metrics['total_records']:,}")
        print(f"  Valid: {metrics['valid_records']:,}")
        print(f"  Invalid: {metrics['invalid_records']:,}")
        print(f"  Pass Rate: {metrics['quality_pass_rate']:.2%}")
        
        if metrics['volume_anomaly_detected']:
            print(f"  ⚠️  Volume Anomaly Detected! Z-score: {metrics['volume_z_score']:.2f}")
        
        # Handle failed records
        valid_df = handle_failed_records(
            validated_df=batch_df,
            quarantine_path="/data/quarantine_bronze",
            layer="bronze"
        )
        
        # Continue processing with valid records only
        valid_df.write \
            .format("delta") \
            .mode("append") \
            .save("/data/bronze_validated")
    
    # Start streaming query with quality checks
    query = validated_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/checkpoints/quality_bronze") \
        .start()
    
    query.awaitTermination()
