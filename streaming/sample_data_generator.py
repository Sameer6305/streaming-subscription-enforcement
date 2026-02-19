"""
Sample Data Generator for Real-Time Subscription Enforcement Platform
=====================================================================

Generates realistic API usage events that match api_usage_event.schema.json.
Events simulate real SaaS traffic patterns including:
- Multi-tenant workloads with varied plan tiers
- Burst traffic and rate limiting scenarios
- Late-arriving events and duplicates
- Abuse patterns and suspicious behavior
- Geographic distribution across regions

Usage:
    # Generate 10,000 events as JSON files (for local Spark testing)
    python sample_data_generator.py --output-dir ./sample_data --count 10000

    # Generate events as JSONL (one JSON per line) to stdout
    python sample_data_generator.py --format jsonl --count 100

    # Generate events to a Kafka topic
    python sample_data_generator.py --kafka-brokers localhost:9092 --count 50000

    # Generate events as a Parquet file (for direct Spark loading)
    python sample_data_generator.py --output-dir ./sample_data --format parquet --count 100000
"""

import argparse
import json
import os
import random
import uuid
import hashlib
import math
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

# ─── Tenant Configuration ──────────────────────────────────────────
# Simulates 50 realistic tenants across plan tiers with varied traffic patterns

TENANTS = [
    # Free tier tenants (30) - low traffic, occasional bursts
    *[{"tenant_id": f"tenant-free-{i:03d}", "plan_tier": "free",
       "api_keys": [f"key-free-{i:03d}-a", f"key-free-{i:03d}-b"],
       "avg_rpm": random.randint(5, 40), "burst_factor": 3.0}
      for i in range(1, 31)],
    # Starter tier tenants (12) - moderate traffic
    *[{"tenant_id": f"tenant-starter-{i:03d}", "plan_tier": "starter",
       "api_keys": [f"key-starter-{i:03d}-{c}" for c in "abcd"],
       "avg_rpm": random.randint(50, 200), "burst_factor": 2.5}
      for i in range(1, 13)],
    # Professional tier tenants (6) - high traffic
    *[{"tenant_id": f"tenant-pro-{i:03d}", "plan_tier": "professional",
       "api_keys": [f"key-pro-{i:03d}-{c}" for c in "abcdefgh"],
       "avg_rpm": random.randint(200, 700), "burst_factor": 2.0}
      for i in range(1, 7)],
    # Enterprise tier tenants (2) - very high traffic
    *[{"tenant_id": f"tenant-ent-{i:03d}", "plan_tier": "enterprise",
       "api_keys": [f"key-ent-{i:03d}-{c}" for c in "abcdefghijklmnop"],
       "avg_rpm": random.randint(1000, 3000), "burst_factor": 1.5}
      for i in range(1, 3)],
]

PLAN_LIMITS = {
    "free": {"rpm": 60, "rph": 1000, "monthly": 10000},
    "starter": {"rpm": 300, "rph": 10000, "monthly": 100000},
    "professional": {"rpm": 1000, "rph": 50000, "monthly": 1000000},
    "enterprise": {"rpm": 5000, "rph": 200000, "monthly": 10000000},
}

# ─── API Endpoints ─────────────────────────────────────────────────

ENDPOINTS = [
    {"path": "/v1/users/{id}", "method": "GET", "weight": 0.25, "cost": 1.0, "latency_base": 45},
    {"path": "/v1/users", "method": "POST", "weight": 0.05, "cost": 2.0, "latency_base": 120},
    {"path": "/v1/search", "method": "POST", "weight": 0.20, "cost": 3.0, "latency_base": 200},
    {"path": "/v1/documents/{id}", "method": "GET", "weight": 0.15, "cost": 1.0, "latency_base": 60},
    {"path": "/v1/documents", "method": "POST", "weight": 0.08, "cost": 5.0, "latency_base": 350},
    {"path": "/v1/analytics/reports", "method": "GET", "weight": 0.10, "cost": 2.0, "latency_base": 150},
    {"path": "/v1/webhooks", "method": "POST", "weight": 0.02, "cost": 1.0, "latency_base": 30},
    {"path": "/v1/health", "method": "GET", "weight": 0.05, "cost": 0.0, "latency_base": 5},
    {"path": "/v1/auth/token", "method": "POST", "weight": 0.05, "cost": 0.5, "latency_base": 80},
    {"path": "/v1/files/{id}/download", "method": "GET", "weight": 0.05, "cost": 4.0, "latency_base": 500},
]

# ─── Geographic Distribution ──────────────────────────────────────

GEO_REGIONS = [
    {"country_code": "US", "country_name": "United States", "region": "California", "city": "San Francisco",
     "lat": 37.7749, "lon": -122.4194, "tz": "America/Los_Angeles", "weight": 0.30},
    {"country_code": "US", "country_name": "United States", "region": "Virginia", "city": "Ashburn",
     "lat": 39.0438, "lon": -77.4874, "tz": "America/New_York", "weight": 0.15},
    {"country_code": "GB", "country_name": "United Kingdom", "region": "England", "city": "London",
     "lat": 51.5074, "lon": -0.1278, "tz": "Europe/London", "weight": 0.12},
    {"country_code": "DE", "country_name": "Germany", "region": "Hesse", "city": "Frankfurt",
     "lat": 50.1109, "lon": 8.6821, "tz": "Europe/Berlin", "weight": 0.10},
    {"country_code": "JP", "country_name": "Japan", "region": "Tokyo", "city": "Tokyo",
     "lat": 35.6762, "lon": 139.6503, "tz": "Asia/Tokyo", "weight": 0.08},
    {"country_code": "SG", "country_name": "Singapore", "region": "Central", "city": "Singapore",
     "lat": 1.3521, "lon": 103.8198, "tz": "Asia/Singapore", "weight": 0.07},
    {"country_code": "IN", "country_name": "India", "region": "Maharashtra", "city": "Mumbai",
     "lat": 19.0760, "lon": 72.8777, "tz": "Asia/Kolkata", "weight": 0.08},
    {"country_code": "BR", "country_name": "Brazil", "region": "São Paulo", "city": "São Paulo",
     "lat": -23.5505, "lon": -46.6333, "tz": "America/Sao_Paulo", "weight": 0.05},
    {"country_code": "AU", "country_name": "Australia", "region": "NSW", "city": "Sydney",
     "lat": -33.8688, "lon": 151.2093, "tz": "Australia/Sydney", "weight": 0.05},
]

GATEWAY_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1", "ap-southeast-1"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "python-requests/2.31.0",
    "axios/1.6.2",
    "Go-http-client/2.0",
    "curl/8.4.0",
    "PostmanRuntime/7.35.0",
    "okhttp/4.12.0",
    "node-fetch/3.3.2",
]

SDK_NAMES = [None, None, None, "acme-python-sdk", "acme-node-sdk", "acme-go-sdk", "acme-java-sdk"]
SDK_VERSIONS = ["1.0.0", "1.1.0", "1.2.3", "2.0.0", "2.1.0"]

AUTH_METHODS = ["api_key", "api_key", "api_key", "oauth2", "jwt"]
CACHE_STATUSES = ["miss", "miss", "miss", "hit", "hit", "bypass", "stale"]
STATUS_CATEGORIES = {
    200: "success", 201: "success", 204: "success",
    301: "success", 304: "success",
    400: "client_error", 401: "unauthorized", 403: "unauthorized",
    404: "client_error", 422: "client_error", 429: "rate_limited",
    500: "server_error", 502: "server_error", 503: "server_error",
}


def weighted_choice(items, weight_key="weight"):
    """Select from a list using weighted probability."""
    weights = [item[weight_key] for item in items]
    return random.choices(items, weights=weights, k=1)[0]


def generate_ip():
    """Generate a realistic-looking IP address."""
    return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def sha256_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def generate_status_code(is_abuse: bool = False) -> int:
    """Generate realistic HTTP status code distribution."""
    if is_abuse:
        return random.choices(
            [200, 400, 401, 403, 404, 429, 500],
            weights=[0.10, 0.15, 0.20, 0.15, 0.15, 0.20, 0.05],
            k=1
        )[0]
    return random.choices(
        [200, 201, 204, 301, 400, 401, 404, 429, 500, 502, 503],
        weights=[0.70, 0.05, 0.03, 0.01, 0.05, 0.02, 0.04, 0.03, 0.03, 0.02, 0.02],
        k=1
    )[0]


def generate_event(
    event_time: datetime,
    tenant: Dict,
    sequence: int,
    is_duplicate: bool = False,
    is_late: bool = False,
    is_abuse: bool = False,
    existing_event_id: str = None,
) -> Dict[str, Any]:
    """
    Generate a single API usage event matching api_usage_event.schema.json.
    """
    event_id = existing_event_id if is_duplicate else str(uuid.uuid4())
    api_key = random.choice(tenant["api_keys"])
    endpoint = weighted_choice(ENDPOINTS)
    geo = weighted_choice(GEO_REGIONS)
    ip_addr = generate_ip()
    status_code = generate_status_code(is_abuse)
    
    # Latency with realistic distribution (log-normal)
    base_latency = endpoint["latency_base"]
    latency_ms = max(1.0, random.lognormvariate(math.log(base_latency), 0.5))
    upstream_latency = latency_ms * random.uniform(0.6, 0.95)
    
    # Request/response sizes
    request_size = random.randint(64, 8192) if endpoint["method"] in ("POST", "PUT", "PATCH") else 0
    response_size = random.randint(256, 65536)
    
    # Rate limiting state
    plan_limit = PLAN_LIMITS[tenant["plan_tier"]]["rpm"]
    current_velocity = tenant.get("_current_velocity", tenant["avg_rpm"])
    remaining = max(0, plan_limit - current_velocity)
    was_rate_limited = status_code == 429 or current_velocity > plan_limit
    
    # Abuse signals
    risk_score = random.uniform(60, 95) if is_abuse else random.uniform(0, 15)
    risk_factors = []
    if is_abuse:
        risk_factors = random.sample(
            ["high_velocity", "unusual_geo", "credential_sharing", "endpoint_enumeration", "known_bad_ip"],
            k=random.randint(1, 3)
        )
    
    # Ingestion timestamp (slightly after event)
    ingestion_delay = timedelta(seconds=random.uniform(0.1, 2.0))
    if is_late:
        ingestion_delay = timedelta(minutes=random.randint(5, 45))
    ingestion_ts = event_time + ingestion_delay
    
    # User agent
    ua = random.choice(USER_AGENTS)
    sdk = random.choice(SDK_NAMES)
    auth_method = random.choice(AUTH_METHODS)
    
    # Billing
    is_billable = endpoint["cost"] > 0 and status_code < 400
    billing_units = endpoint["cost"] if is_billable else 0.0
    cost_estimate = billing_units * 0.001  # $0.001 per unit
    
    gateway_region = random.choice(GATEWAY_REGIONS)
    
    event = {
        "event_id": event_id,
        "event_version": "1.0.0",
        "event_timestamp": event_time.isoformat() + "Z",
        "ingestion_timestamp": ingestion_ts.isoformat() + "Z",
        "processing_timestamp": None,
        "sequence_number": sequence,
        "tenant_id": tenant["tenant_id"],
        "subscription": {
            "subscription_id": f"sub-{tenant['tenant_id']}-001",
            "plan_tier": tenant["plan_tier"],
            "billing_cycle_id": event_time.strftime("%Y-%m"),
        },
        "identity": {
            "api_key_id": sha256_hash(api_key),
            "api_key_prefix": api_key[:8],
            "user_id": f"user-{sha256_hash(api_key + tenant['tenant_id'])}",
            "auth_method": auth_method,
            "oauth_client_id": f"oauth-{tenant['tenant_id']}" if auth_method == "oauth2" else None,
            "scopes": random.sample(["read", "write", "admin", "analytics"], k=random.randint(1, 3)),
        },
        "request": {
            "request_id": str(uuid.uuid4()),
            "trace_id": f"{uuid.uuid4().hex[:32]}",
            "span_id": f"{uuid.uuid4().hex[:16]}",
            "method": endpoint["method"],
            "path": endpoint["path"],
            "path_raw": endpoint["path"].replace("{id}", str(random.randint(1000, 999999))),
            "query_params_hash": sha256_hash(f"q={random.randint(1, 1000)}"),
            "api_version": "v1",
            "endpoint_id": f"ep-{endpoint['path'].replace('/', '-').strip('-')}",
            "received_at": event_time.isoformat() + "Z",
            "content_length": request_size,
            "content_type": "application/json" if request_size > 0 else None,
        },
        "response": {
            "status_code": status_code,
            "status_category": STATUS_CATEGORIES.get(status_code, "client_error"),
            "content_length": response_size,
            "latency_ms": round(latency_ms, 2),
            "upstream_latency_ms": round(upstream_latency, 2),
            "cache_status": random.choice(CACHE_STATUSES),
            "error_code": f"ERR_{status_code}" if status_code >= 400 else None,
            "error_message": f"Request failed with status {status_code}" if status_code >= 400 else None,
        },
        "client": {
            "ip_address": ip_addr,
            "ip_address_hash": sha256_hash(ip_addr),
            "ip_version": "ipv4",
            "user_agent": ua,
            "user_agent_parsed": {
                "browser": "Chrome" if "AppleWebKit" in ua else ua.split("/")[0],
                "browser_version": "120.0",
                "os": "Windows" if "Windows" in ua else ("macOS" if "Mac" in ua else "Linux"),
                "device_type": "desktop",
                "is_bot": "false",
            },
            "sdk_name": sdk,
            "sdk_version": random.choice(SDK_VERSIONS) if sdk else None,
        },
        "geo": {
            "country_code": geo["country_code"],
            "country_name": geo["country_name"],
            "region": geo["region"],
            "city": geo["city"],
            "postal_code": str(random.randint(10000, 99999)),
            "latitude": round(geo["lat"] + random.uniform(-0.1, 0.1), 4),
            "longitude": round(geo["lon"] + random.uniform(-0.1, 0.1), 4),
            "timezone": geo["tz"],
            "asn": random.randint(10000, 65000),
            "as_org": random.choice(["Amazon", "Google Cloud", "Microsoft Azure", "Cloudflare", "Akamai", "DigitalOcean"]),
            "is_datacenter": random.random() < 0.3,
            "is_vpn": random.random() < 0.05,
            "is_tor": random.random() < 0.005,
            "is_proxy": random.random() < 0.02,
        },
        "rate_limiting": {
            "limit": plan_limit,
            "remaining": remaining,
            "reset_at": (event_time + timedelta(seconds=60 - event_time.second)).isoformat() + "Z",
            "window_size_seconds": 60,
            "was_rate_limited": was_rate_limited,
            "rate_limit_policy": f"{tenant['plan_tier']}_default",
            "quota_used_percent": round(min(100.0, (current_velocity / plan_limit) * 100), 2),
        },
        "abuse_signals": {
            "risk_score": round(risk_score, 2),
            "risk_factors": risk_factors,
            "velocity_1min": current_velocity,
            "velocity_5min": current_velocity * 4,
            "velocity_1hour": current_velocity * 40,
            "unique_ips_24h": random.randint(1, 5) if not is_abuse else random.randint(15, 50),
            "unique_endpoints_1h": random.randint(2, 8) if not is_abuse else random.randint(30, 80),
            "is_suspicious": is_abuse,
            "action_taken": "blocked" if is_abuse and was_rate_limited else ("logged" if is_abuse else "allowed"),
            "fingerprint_hash": sha256_hash(ua + ip_addr),
        },
        "gateway": {
            "gateway_id": f"gw-{gateway_region}-{random.randint(1, 10):02d}",
            "gateway_region": gateway_region,
            "gateway_version": "3.2.1",
            "upstream_service": f"api-service-{endpoint['path'].split('/')[1] if len(endpoint['path'].split('/')) > 1 else 'main'}",
            "upstream_instance": f"i-{uuid.uuid4().hex[:8]}",
        },
        "billing": {
            "billable": is_billable,
            "billing_units": billing_units,
            "pricing_tier": tenant["plan_tier"],
            "cost_estimate_usd": round(cost_estimate, 6),
        },
        "custom_attributes": {
            "source_sdk": sdk or "direct",
            "deployment_ring": random.choice(["canary", "stable", "stable", "stable"]),
        },
        "metadata": {
            "source": "api-gateway-prod",
            "environment": "production",
            "partition_key": tenant["tenant_id"],
            "retention_days": 730,
        },
    }
    
    return event


def generate_dataset(
    count: int,
    start_time: Optional[datetime] = None,
    duration_hours: int = 24,
    duplicate_rate: float = 0.02,
    late_rate: float = 0.05,
    abuse_rate: float = 0.03,
) -> List[Dict[str, Any]]:
    """
    Generate a batch of realistic API usage events.
    
    Args:
        count: Number of events to generate
        start_time: Start of time window (default: 24 hours ago)
        duration_hours: Time window duration in hours
        duplicate_rate: Fraction of events that are duplicates (0.02 = 2%)
        late_rate: Fraction of events that arrive late (0.05 = 5%)
        abuse_rate: Fraction of events that simulate abuse (0.03 = 3%)
    
    Returns:
        List of event dicts sorted by event_timestamp
    """
    if start_time is None:
        start_time = datetime.now(timezone.utc) - timedelta(hours=duration_hours)
    
    events = []
    sequence = 0
    recent_event_ids = []
    
    for i in range(count):
        # Progress display
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1:,}/{count:,} events...")
        
        # Pick a tenant (weighted by avg_rpm for realistic distribution)
        tenant = random.choices(TENANTS, weights=[t["avg_rpm"] for t in TENANTS], k=1)[0]
        
        # Generate event time (spread across duration)
        # Add diurnal pattern (more traffic during business hours)
        progress = i / count
        event_time = start_time + timedelta(hours=duration_hours * progress)
        hour_of_day = event_time.hour
        
        # Diurnal multiplier (peak at 10-14 UTC, low at 2-6 UTC)
        diurnal_factor = 0.3 + 0.7 * math.exp(-0.5 * ((hour_of_day - 12) / 4) ** 2)
        
        # Add jitter
        jitter = timedelta(seconds=random.uniform(-30, 30) * diurnal_factor)
        event_time = event_time + jitter
        
        # Determine special event types
        is_duplicate = random.random() < duplicate_rate and len(recent_event_ids) > 0
        is_late = random.random() < late_rate
        is_abuse = random.random() < abuse_rate
        
        existing_event_id = random.choice(recent_event_ids) if is_duplicate else None
        
        # Set current velocity for rate limit simulation
        tenant["_current_velocity"] = int(tenant["avg_rpm"] * diurnal_factor * random.uniform(0.5, 1.5))
        
        event = generate_event(
            event_time=event_time,
            tenant=tenant,
            sequence=sequence,
            is_duplicate=is_duplicate,
            is_late=is_late,
            is_abuse=is_abuse,
            existing_event_id=existing_event_id,
        )
        
        events.append(event)
        sequence += 1
        
        # Track recent IDs for duplicate generation
        if not is_duplicate:
            recent_event_ids.append(event["event_id"])
            if len(recent_event_ids) > 100:
                recent_event_ids.pop(0)
    
    # Sort by event_timestamp
    events.sort(key=lambda e: e["event_timestamp"])
    
    return events


def write_json_files(events: List[Dict], output_dir: str, batch_size: int = 1000):
    """Write events as JSON files (one file per batch, simulating Kafka micro-batches)."""
    os.makedirs(output_dir, exist_ok=True)
    
    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        batch_file = os.path.join(output_dir, f"events_batch_{i // batch_size:05d}.json")
        with open(batch_file, "w") as f:
            for event in batch:
                f.write(json.dumps(event) + "\n")
    
    print(f"Wrote {len(events):,} events to {output_dir}/ ({len(events) // batch_size + 1} files)")


def write_jsonl(events: List[Dict], output_path: Optional[str] = None):
    """Write events as JSONL (one JSON per line)."""
    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        print(f"Wrote {len(events):,} events to {output_path}")
    else:
        for event in events:
            print(json.dumps(event))


def write_parquet(events: List[Dict], output_dir: str):
    """Write events as Parquet using pandas (for direct Spark loading)."""
    try:
        import pandas as pd
    except ImportError:
        print("ERROR: pandas required for Parquet output. Install with: pip install pandas pyarrow")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    df = pd.json_normalize(events, sep="_")
    output_path = os.path.join(output_dir, "events.parquet")
    df.to_parquet(output_path, index=False)
    print(f"Wrote {len(events):,} events to {output_path} ({os.path.getsize(output_path) / 1024 / 1024:.1f} MB)")


def print_dataset_summary(events: List[Dict]):
    """Print summary statistics about the generated dataset."""
    tenants = set(e["tenant_id"] for e in events)
    plans = {}
    for e in events:
        tier = e["subscription"]["plan_tier"]
        plans[tier] = plans.get(tier, 0) + 1
    
    status_codes = {}
    for e in events:
        sc = e["response"]["status_code"]
        status_codes[sc] = status_codes.get(sc, 0) + 1
    
    abuse_count = sum(1 for e in events if e["abuse_signals"]["is_suspicious"])
    rate_limited = sum(1 for e in events if e["rate_limiting"]["was_rate_limited"])
    late_events = sum(1 for e in events if e["abuse_signals"]["velocity_1min"] > 0)
    billable = sum(1 for e in events if e["billing"]["billable"])
    total_billing_units = sum(e["billing"]["billing_units"] for e in events)
    total_cost = sum(e["billing"]["cost_estimate_usd"] for e in events)
    
    # Duplicate detection
    event_ids = [e["event_id"] for e in events]
    unique_ids = set(event_ids)
    duplicates = len(event_ids) - len(unique_ids)
    
    # Time range
    timestamps = sorted(e["event_timestamp"] for e in events)
    
    print("\n" + "=" * 65)
    print("  DATASET SUMMARY")
    print("=" * 65)
    print(f"  Total Events:        {len(events):>12,}")
    print(f"  Unique Events:       {len(unique_ids):>12,}")
    print(f"  Duplicates:          {duplicates:>12,}  ({duplicates/len(events)*100:.1f}%)")
    print(f"  Unique Tenants:      {len(tenants):>12,}")
    print(f"  Abuse Events:        {abuse_count:>12,}  ({abuse_count/len(events)*100:.1f}%)")
    print(f"  Rate Limited:        {rate_limited:>12,}  ({rate_limited/len(events)*100:.1f}%)")
    print(f"  Billable Events:     {billable:>12,}  ({billable/len(events)*100:.1f}%)")
    print(f"  Total Billing Units: {total_billing_units:>12,.1f}")
    print(f"  Estimated Revenue:   ${total_cost:>11,.4f}")
    print()
    print("  Time Range:")
    print(f"    From: {timestamps[0]}")
    print(f"    To:   {timestamps[-1]}")
    print()
    print("  Plan Distribution:")
    for tier in ["free", "starter", "professional", "enterprise"]:
        count = plans.get(tier, 0)
        print(f"    {tier:>15}: {count:>8,} events  ({count/len(events)*100:5.1f}%)")
    print()
    print("  Top Status Codes:")
    for sc, count in sorted(status_codes.items(), key=lambda x: -x[1])[:8]:
        print(f"    HTTP {sc}: {count:>8,}  ({count/len(events)*100:5.1f}%)")
    print("=" * 65)


def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic API usage events for the Subscription Enforcement Platform"
    )
    parser.add_argument("--count", type=int, default=10000, help="Number of events to generate")
    parser.add_argument("--output-dir", type=str, default="./sample_data", help="Output directory")
    parser.add_argument("--format", choices=["json", "jsonl", "parquet"], default="json",
                        help="Output format")
    parser.add_argument("--duration-hours", type=int, default=24, help="Time window in hours")
    parser.add_argument("--duplicate-rate", type=float, default=0.02, help="Duplicate event rate")
    parser.add_argument("--late-rate", type=float, default=0.05, help="Late-arriving event rate")
    parser.add_argument("--abuse-rate", type=float, default=0.03, help="Abuse event rate")
    parser.add_argument("--kafka-brokers", type=str, default=None,
                        help="Kafka brokers (if set, writes to Kafka instead of files)")
    parser.add_argument("--kafka-topic", type=str, default="api-usage-events.v1", help="Kafka topic")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    random.seed(args.seed)
    
    print(f"\nGenerating {args.count:,} realistic API usage events...")
    print(f"  Time window: {args.duration_hours} hours")
    print(f"  Duplicates: {args.duplicate_rate*100:.0f}% | Late: {args.late_rate*100:.0f}% | Abuse: {args.abuse_rate*100:.0f}%")
    
    events = generate_dataset(
        count=args.count,
        duration_hours=args.duration_hours,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        abuse_rate=args.abuse_rate,
    )
    
    if args.kafka_brokers:
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=args.kafka_brokers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            for event in events:
                producer.send(
                    args.kafka_topic,
                    key=event["tenant_id"],
                    value=event,
                )
            producer.flush()
            print(f"\nSent {len(events):,} events to Kafka topic '{args.kafka_topic}'")
        except ImportError:
            print("ERROR: kafka-python required. Install with: pip install kafka-python")
            return
    elif args.format == "json":
        write_json_files(events, args.output_dir)
    elif args.format == "jsonl":
        output_path = os.path.join(args.output_dir, "events.jsonl")
        write_jsonl(events, output_path)
    elif args.format == "parquet":
        write_parquet(events, args.output_dir)
    
    print_dataset_summary(events)


if __name__ == "__main__":
    main()
