"""
Observability Framework for Streaming Pipeline
==============================================

Comprehensive monitoring and alerting for Bronze → Silver → Gold pipeline:
- Performance metrics (throughput, latency, lag)
- Data freshness tracking
- Error rate monitoring
- Resource utilization
- Custom business metrics

Usage:
    # Initialize observer for a layer
    observer = PipelineObserver(
        layer="silver",
        app_name="silver-processing",
        alert_thresholds=AlertThresholds()
    )
    
    # Track batch processing
    with observer.track_batch(batch_id):
        # Process data
        result_df = process_batch(batch_df)
    
    # Write metrics
    observer.write_metrics(spark)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, max as spark_max, min as spark_min,
    count, avg, sum as spark_sum, expr, unix_timestamp, 
    from_unixtime, window, lag, percentile_approx
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, DoubleType, IntegerType, MapType, BooleanType
)
from pyspark.sql.window import Window
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from contextlib import contextmanager
import time
import json


@dataclass
class AlertThresholds:
    """
    Configurable alert thresholds for pipeline monitoring.
    
    Thresholds are defined per-layer to account for different SLAs.
    """
    # Lag thresholds (seconds)
    processing_lag_warning: int = 60       # 1 minute
    processing_lag_critical: int = 300     # 5 minutes
    kafka_lag_warning: int = 10000         # 10k messages
    kafka_lag_critical: int = 50000        # 50k messages
    
    # Freshness thresholds (seconds)
    data_freshness_warning: int = 120      # 2 minutes
    data_freshness_critical: int = 600     # 10 minutes
    
    # Throughput thresholds (records/second)
    throughput_min_warning: float = 100.0   # Below 100 rec/sec
    throughput_min_critical: float = 10.0   # Below 10 rec/sec
    
    # Error rate thresholds (percentage)
    error_rate_warning: float = 1.0        # 1% errors
    error_rate_critical: float = 5.0       # 5% errors
    
    # Quality thresholds (percentage)
    quality_pass_rate_warning: float = 95.0   # Below 95%
    quality_pass_rate_critical: float = 80.0  # Below 80%
    
    # Batch duration thresholds (seconds)
    batch_duration_warning: int = 30       # 30 seconds
    batch_duration_critical: int = 120     # 2 minutes


class PipelineObserver:
    """
    Observability framework for streaming pipeline.
    
    Tracks:
    - Processing lag (current time - event time)
    - Kafka lag (consumer lag behind producer)
    - Data freshness (age of latest processed record)
    - Throughput (records/second)
    - Error rates
    - Batch processing times
    - Resource utilization
    
    Generates alerts when thresholds are breached.
    """
    
    def __init__(
        self,
        layer: str,
        app_name: str,
        alert_thresholds: Optional[AlertThresholds] = None,
        metrics_table_path: str = "/data/observability_metrics",
        alerts_table_path: str = "/data/observability_alerts"
    ):
        """
        Initialize pipeline observer.
        
        Args:
            layer: Pipeline layer ("bronze", "silver", "gold")
            app_name: Spark application name for tracking
            alert_thresholds: Custom alert thresholds (uses defaults if None)
            metrics_table_path: Delta table path for metrics
            alerts_table_path: Delta table path for alerts
        """
        self.layer = layer.lower()
        self.app_name = app_name
        self.thresholds = alert_thresholds or AlertThresholds()
        self.metrics_table_path = metrics_table_path
        self.alerts_table_path = alerts_table_path
        
        # Runtime tracking
        self.current_batch_id = None
        self.batch_start_time = None
        self.batch_metrics = {}
    
    @contextmanager
    def track_batch(self, batch_id: int):
        """
        Context manager for tracking batch processing.
        
        Usage:
            with observer.track_batch(batch_id):
                # Process batch
                process_data(df)
        """
        self.current_batch_id = batch_id
        self.batch_start_time = time.time()
        
        try:
            yield self
        finally:
            batch_duration = time.time() - self.batch_start_time
            self.batch_metrics["batch_duration_sec"] = batch_duration
            self.batch_metrics["batch_id"] = batch_id
    
    def collect_metrics(
        self,
        spark: SparkSession,
        df: DataFrame,
        batch_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Collect comprehensive metrics from a batch DataFrame.
        
        Args:
            spark: SparkSession
            df: Batch DataFrame to analyze
            batch_id: Optional batch ID
        
        Returns:
            Dict with all collected metrics
        """
        batch_id = batch_id or self.current_batch_id
        
        # Basic volume metrics
        record_count = df.count()
        
        # Timestamp analysis (for lag and freshness)
        timestamp_col = self._detect_timestamp_column(df)
        
        if timestamp_col:
            timestamp_stats = df.agg(
                spark_min(col(timestamp_col)).alias("min_event_time"),
                spark_max(col(timestamp_col)).alias("max_event_time"),
                avg(unix_timestamp(current_timestamp()) - unix_timestamp(col(timestamp_col))).alias("avg_lag_sec")
            ).first()
            
            min_event_time = timestamp_stats["min_event_time"]
            max_event_time = timestamp_stats["max_event_time"]
            avg_lag_sec = timestamp_stats["avg_lag_sec"] or 0.0
            
            # Data freshness = current time - max event time
            if max_event_time:
                data_freshness_sec = (datetime.now() - max_event_time).total_seconds()
            else:
                data_freshness_sec = None
        else:
            min_event_time = None
            max_event_time = None
            avg_lag_sec = None
            data_freshness_sec = None
        
        # Throughput calculation
        batch_duration = self.batch_metrics.get("batch_duration_sec", 1.0)
        throughput = record_count / batch_duration if batch_duration > 0 else 0.0
        
        # Error metrics (if quality_check_passed column exists)
        if "quality_check_passed" in df.columns:
            quality_stats = df.agg(
                spark_sum(when(col("quality_check_passed"), 1).otherwise(0)).alias("valid_records"),
                spark_sum(when(~col("quality_check_passed"), 1).otherwise(0)).alias("invalid_records")
            ).first()
            
            valid_records = quality_stats["valid_records"]
            invalid_records = quality_stats["invalid_records"]
            error_rate = (invalid_records / record_count * 100) if record_count > 0 else 0.0
            quality_pass_rate = (valid_records / record_count * 100) if record_count > 0 else 100.0
        else:
            valid_records = record_count
            invalid_records = 0
            error_rate = 0.0
            quality_pass_rate = 100.0
        
        # Kafka lag (if kafka_offset column exists)
        kafka_lag = self._calculate_kafka_lag(spark, df)
        
        # Build metrics dict
        metrics = {
            "metric_timestamp": datetime.now(),
            "layer": self.layer,
            "app_name": self.app_name,
            "batch_id": batch_id,
            
            # Volume metrics
            "record_count": record_count,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            
            # Performance metrics
            "batch_duration_sec": batch_duration,
            "throughput_rec_per_sec": throughput,
            "avg_processing_lag_sec": avg_lag_sec,
            "data_freshness_sec": data_freshness_sec,
            "kafka_lag_messages": kafka_lag,
            
            # Quality metrics
            "error_rate_pct": error_rate,
            "quality_pass_rate_pct": quality_pass_rate,
            
            # Timestamp bounds
            "min_event_time": min_event_time,
            "max_event_time": max_event_time,
        }
        
        # Store for later use
        self.batch_metrics.update(metrics)
        
        return metrics
    
    def _detect_timestamp_column(self, df: DataFrame) -> Optional[str]:
        """Detect which timestamp column to use for lag calculation."""
        if "event_timestamp" in df.columns:
            return "event_timestamp"
        elif "window_start" in df.columns:
            return "window_start"
        elif "ingestion_timestamp" in df.columns:
            return "ingestion_timestamp"
        else:
            return None
    
    def _calculate_kafka_lag(self, spark: SparkSession, df: DataFrame) -> Optional[int]:
        """
        Calculate Kafka consumer lag.
        
        Lag = Latest Offset (from Kafka) - Current Offset (from DataFrame)
        """
        if "kafka_partition" not in df.columns or "kafka_offset" not in df.columns:
            return None
        
        try:
            # Get max offset per partition from current batch
            current_offsets = df.groupBy("kafka_partition").agg(
                spark_max("kafka_offset").alias("current_offset")
            ).collect()
            
            # In production, query Kafka API for latest offsets
            # For now, estimate lag as 0 (simplified)
            # TODO: Integrate with Kafka Consumer API
            total_lag = 0
            
            return total_lag
            
        except Exception as e:
            return None
    
    def evaluate_alerts(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Evaluate metrics against thresholds and generate alerts.
        
        Args:
            metrics: Collected metrics dict
        
        Returns:
            List of alert dicts (empty if no alerts)
        """
        alerts = []
        
        # 1. Processing lag alerts
        avg_lag = metrics.get("avg_processing_lag_sec")
        if avg_lag is not None:
            if avg_lag >= self.thresholds.processing_lag_critical:
                alerts.append({
                    "alert_type": "processing_lag_critical",
                    "severity": "critical",
                    "message": f"Processing lag {avg_lag:.1f}s exceeds critical threshold {self.thresholds.processing_lag_critical}s",
                    "metric_value": avg_lag,
                    "threshold": self.thresholds.processing_lag_critical
                })
            elif avg_lag >= self.thresholds.processing_lag_warning:
                alerts.append({
                    "alert_type": "processing_lag_warning",
                    "severity": "warning",
                    "message": f"Processing lag {avg_lag:.1f}s exceeds warning threshold {self.thresholds.processing_lag_warning}s",
                    "metric_value": avg_lag,
                    "threshold": self.thresholds.processing_lag_warning
                })
        
        # 2. Data freshness alerts
        freshness = metrics.get("data_freshness_sec")
        if freshness is not None:
            if freshness >= self.thresholds.data_freshness_critical:
                alerts.append({
                    "alert_type": "data_freshness_critical",
                    "severity": "critical",
                    "message": f"Data freshness {freshness:.1f}s exceeds critical threshold {self.thresholds.data_freshness_critical}s",
                    "metric_value": freshness,
                    "threshold": self.thresholds.data_freshness_critical
                })
            elif freshness >= self.thresholds.data_freshness_warning:
                alerts.append({
                    "alert_type": "data_freshness_warning",
                    "severity": "warning",
                    "message": f"Data freshness {freshness:.1f}s exceeds warning threshold {self.thresholds.data_freshness_warning}s",
                    "metric_value": freshness,
                    "threshold": self.thresholds.data_freshness_warning
                })
        
        # 3. Throughput alerts
        throughput = metrics.get("throughput_rec_per_sec", 0.0)
        if throughput <= self.thresholds.throughput_min_critical:
            alerts.append({
                "alert_type": "throughput_critical",
                "severity": "critical",
                "message": f"Throughput {throughput:.1f} rec/sec below critical threshold {self.thresholds.throughput_min_critical}",
                "metric_value": throughput,
                "threshold": self.thresholds.throughput_min_critical
            })
        elif throughput <= self.thresholds.throughput_min_warning:
            alerts.append({
                "alert_type": "throughput_warning",
                "severity": "warning",
                "message": f"Throughput {throughput:.1f} rec/sec below warning threshold {self.thresholds.throughput_min_warning}",
                "metric_value": throughput,
                "threshold": self.thresholds.throughput_min_warning
            })
        
        # 4. Error rate alerts
        error_rate = metrics.get("error_rate_pct", 0.0)
        if error_rate >= self.thresholds.error_rate_critical:
            alerts.append({
                "alert_type": "error_rate_critical",
                "severity": "critical",
                "message": f"Error rate {error_rate:.2f}% exceeds critical threshold {self.thresholds.error_rate_critical}%",
                "metric_value": error_rate,
                "threshold": self.thresholds.error_rate_critical
            })
        elif error_rate >= self.thresholds.error_rate_warning:
            alerts.append({
                "alert_type": "error_rate_warning",
                "severity": "warning",
                "message": f"Error rate {error_rate:.2f}% exceeds warning threshold {self.thresholds.error_rate_warning}%",
                "metric_value": error_rate,
                "threshold": self.thresholds.error_rate_warning
            })
        
        # 5. Quality pass rate alerts
        quality_pass_rate = metrics.get("quality_pass_rate_pct", 100.0)
        if quality_pass_rate <= self.thresholds.quality_pass_rate_critical:
            alerts.append({
                "alert_type": "quality_pass_rate_critical",
                "severity": "critical",
                "message": f"Quality pass rate {quality_pass_rate:.2f}% below critical threshold {self.thresholds.quality_pass_rate_critical}%",
                "metric_value": quality_pass_rate,
                "threshold": self.thresholds.quality_pass_rate_critical
            })
        elif quality_pass_rate <= self.thresholds.quality_pass_rate_warning:
            alerts.append({
                "alert_type": "quality_pass_rate_warning",
                "severity": "warning",
                "message": f"Quality pass rate {quality_pass_rate:.2f}% below warning threshold {self.thresholds.quality_pass_rate_warning}%",
                "metric_value": quality_pass_rate,
                "threshold": self.thresholds.quality_pass_rate_warning
            })
        
        # 6. Batch duration alerts
        batch_duration = metrics.get("batch_duration_sec", 0.0)
        if batch_duration >= self.thresholds.batch_duration_critical:
            alerts.append({
                "alert_type": "batch_duration_critical",
                "severity": "critical",
                "message": f"Batch duration {batch_duration:.1f}s exceeds critical threshold {self.thresholds.batch_duration_critical}s",
                "metric_value": batch_duration,
                "threshold": self.thresholds.batch_duration_critical
            })
        elif batch_duration >= self.thresholds.batch_duration_warning:
            alerts.append({
                "alert_type": "batch_duration_warning",
                "severity": "warning",
                "message": f"Batch duration {batch_duration:.1f}s exceeds warning threshold {self.thresholds.batch_duration_warning}s",
                "metric_value": batch_duration,
                "threshold": self.thresholds.batch_duration_warning
            })
        
        # 7. Kafka lag alerts
        kafka_lag = metrics.get("kafka_lag_messages")
        if kafka_lag is not None:
            if kafka_lag >= self.thresholds.kafka_lag_critical:
                alerts.append({
                    "alert_type": "kafka_lag_critical",
                    "severity": "critical",
                    "message": f"Kafka lag {kafka_lag:,} messages exceeds critical threshold {self.thresholds.kafka_lag_critical:,}",
                    "metric_value": kafka_lag,
                    "threshold": self.thresholds.kafka_lag_critical
                })
            elif kafka_lag >= self.thresholds.kafka_lag_warning:
                alerts.append({
                    "alert_type": "kafka_lag_warning",
                    "severity": "warning",
                    "message": f"Kafka lag {kafka_lag:,} messages exceeds warning threshold {self.thresholds.kafka_lag_warning:,}",
                    "metric_value": kafka_lag,
                    "threshold": self.thresholds.kafka_lag_warning
                })
        
        return alerts
    
    def write_metrics(self, spark: SparkSession, metrics: Optional[Dict[str, Any]] = None):
        """
        Write metrics to Delta table.
        
        Args:
            spark: SparkSession
            metrics: Optional metrics dict (uses self.batch_metrics if None)
        """
        metrics = metrics or self.batch_metrics
        
        if not metrics:
            return
        
        # Convert to DataFrame
        metrics_schema = StructType([
            StructField("metric_timestamp", TimestampType(), False),
            StructField("layer", StringType(), False),
            StructField("app_name", StringType(), False),
            StructField("batch_id", LongType(), True),
            
            # Volume
            StructField("record_count", LongType(), False),
            StructField("valid_records", LongType(), False),
            StructField("invalid_records", LongType(), False),
            
            # Performance
            StructField("batch_duration_sec", DoubleType(), False),
            StructField("throughput_rec_per_sec", DoubleType(), False),
            StructField("avg_processing_lag_sec", DoubleType(), True),
            StructField("data_freshness_sec", DoubleType(), True),
            StructField("kafka_lag_messages", LongType(), True),
            
            # Quality
            StructField("error_rate_pct", DoubleType(), False),
            StructField("quality_pass_rate_pct", DoubleType(), False),
            
            # Timestamps
            StructField("min_event_time", TimestampType(), True),
            StructField("max_event_time", TimestampType(), True),
        ])
        
        metrics_df = spark.createDataFrame([metrics], schema=metrics_schema)
        
        metrics_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(self.metrics_table_path)
    
    def write_alerts(self, spark: SparkSession, alerts: List[Dict[str, Any]]):
        """
        Write alerts to Delta table.
        
        Args:
            spark: SparkSession
            alerts: List of alert dicts
        """
        if not alerts:
            return
        
        # Add metadata to each alert
        enriched_alerts = []
        for alert in alerts:
            enriched_alert = {
                "alert_id": f"{alert['alert_type']}-{datetime.now().isoformat()}",
                "alert_timestamp": datetime.now(),
                "layer": self.layer,
                "app_name": self.app_name,
                "batch_id": self.current_batch_id,
                **alert
            }
            enriched_alerts.append(enriched_alert)
        
        # Convert to DataFrame
        alerts_schema = StructType([
            StructField("alert_id", StringType(), False),
            StructField("alert_timestamp", TimestampType(), False),
            StructField("layer", StringType(), False),
            StructField("app_name", StringType(), False),
            StructField("batch_id", LongType(), True),
            StructField("alert_type", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("message", StringType(), False),
            StructField("metric_value", DoubleType(), True),
            StructField("threshold", DoubleType(), True),
        ])
        
        alerts_df = spark.createDataFrame(enriched_alerts, schema=alerts_schema)
        
        alerts_df.write \
            .format("delta") \
            .mode("append") \
            .save(self.alerts_table_path)
    
    def monitor_batch(self, spark: SparkSession, df: DataFrame, batch_id: int):
        """
        Complete monitoring workflow: collect metrics, evaluate alerts, write both.
        
        Args:
            spark: SparkSession
            df: Batch DataFrame
            batch_id: Batch ID
        """
        # Collect metrics
        metrics = self.collect_metrics(spark, df, batch_id)
        
        # Evaluate alerts
        alerts = self.evaluate_alerts(metrics)
        
        # Write metrics
        self.write_metrics(spark, metrics)
        
        # Write alerts
        if alerts:
            self.write_alerts(spark, alerts)
            
            # Print alerts to console
            print(f"\n⚠️  {len(alerts)} Alert(s) Generated:")
            for alert in alerts:
                severity_icon = "🔴" if alert["severity"] == "critical" else "🟡"
                print(f"{severity_icon} [{alert['severity'].upper()}] {alert['message']}")
        
        # Print summary
        print(f"\n📊 Batch {batch_id} Metrics:")
        print(f"  Records: {metrics['record_count']:,}")
        print(f"  Throughput: {metrics['throughput_rec_per_sec']:.1f} rec/sec")
        print(f"  Duration: {metrics['batch_duration_sec']:.2f}s")
        if metrics.get('avg_processing_lag_sec'):
            print(f"  Avg Lag: {metrics['avg_processing_lag_sec']:.1f}s")
        if metrics.get('data_freshness_sec'):
            print(f"  Freshness: {metrics['data_freshness_sec']:.1f}s")
        print(f"  Quality: {metrics['quality_pass_rate_pct']:.2f}%")


def get_pipeline_health_summary(spark: SparkSession, hours: int = 24) -> DataFrame:
    """
    Get pipeline health summary for the last N hours.
    
    Args:
        spark: SparkSession
        hours: Lookback period in hours
    
    Returns:
        DataFrame with health metrics by layer
    """
    lookback = datetime.now() - timedelta(hours=hours)
    
    health_query = f"""
    SELECT 
        layer,
        COUNT(DISTINCT batch_id) AS total_batches,
        SUM(record_count) AS total_records,
        AVG(throughput_rec_per_sec) AS avg_throughput,
        AVG(batch_duration_sec) AS avg_batch_duration,
        AVG(avg_processing_lag_sec) AS avg_processing_lag,
        AVG(data_freshness_sec) AS avg_data_freshness,
        AVG(quality_pass_rate_pct) AS avg_quality_pass_rate,
        MAX(data_freshness_sec) AS max_data_freshness,
        MAX(avg_processing_lag_sec) AS max_processing_lag
    FROM delta.`/data/observability_metrics`
    WHERE metric_timestamp >= '{lookback.isoformat()}'
    GROUP BY layer
    ORDER BY layer
    """
    
    return spark.sql(health_query)


def get_recent_alerts(
    spark: SparkSession, 
    hours: int = 24,
    severity: Optional[str] = None
) -> DataFrame:
    """
    Get recent alerts from the pipeline.
    
    Args:
        spark: SparkSession
        hours: Lookback period in hours
        severity: Filter by severity ("warning", "critical", or None for all)
    
    Returns:
        DataFrame with recent alerts
    """
    lookback = datetime.now() - timedelta(hours=hours)
    
    query = f"""
    SELECT 
        alert_timestamp,
        layer,
        app_name,
        alert_type,
        severity,
        message,
        metric_value,
        threshold
    FROM delta.`/data/observability_alerts`
    WHERE alert_timestamp >= '{lookback.isoformat()}'
    """
    
    if severity:
        query += f" AND severity = '{severity}'"
    
    query += " ORDER BY alert_timestamp DESC"
    
    return spark.sql(query)


# Example usage in streaming pipeline
if __name__ == "__main__":
    """
    Example: Integrate observability into Silver processing.
    """
    spark = SparkSession.builder \
        .appName("ObservabilityExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Initialize observer with custom thresholds
    thresholds = AlertThresholds(
        processing_lag_warning=30,      # 30 seconds
        processing_lag_critical=120,    # 2 minutes
        throughput_min_warning=500.0,   # 500 rec/sec
        throughput_min_critical=100.0   # 100 rec/sec
    )
    
    observer = PipelineObserver(
        layer="silver",
        app_name="silver-processing",
        alert_thresholds=thresholds
    )
    
    # Read stream
    silver_df = spark.readStream \
        .format("delta") \
        .load("/data/bronze_api_usage")
    
    # Define batch processing with observability
    def process_batch_with_monitoring(batch_df: DataFrame, batch_id: int):
        # Track batch processing time
        with observer.track_batch(batch_id):
            # Your actual processing logic here
            processed_df = batch_df.filter(col("quality_check_passed"))
            
            # Write to Silver
            processed_df.write \
                .format("delta") \
                .mode("append") \
                .save("/data/silver_api_usage")
        
        # Monitor batch (collect metrics, evaluate alerts)
        observer.monitor_batch(spark, batch_df, batch_id)
    
    # Start streaming with monitoring
    query = silver_df.writeStream \
        .foreachBatch(process_batch_with_monitoring) \
        .option("checkpointLocation", "/checkpoints/silver_with_observability") \
        .start()
    
    query.awaitTermination()
