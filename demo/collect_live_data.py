"""
Live Data Collector: Real Events from Free Public Sources
==========================================================

Collects REAL events from free public APIs and transforms them into our
API usage event schema — proving the pipeline works with live production data.

Sources:
  1. Wikimedia EventStreams (SSE) - 50-100 real events/sec, zero auth
  2. GitHub Events API - Real API calls with rate limiting

The events are written as JSON files to a landing directory, which Spark
monitors as a streaming source (functionally identical to Kafka).

Usage:
    # Collect 5,000 live events from Wikipedia (default)
    python demo/collect_live_data.py --source wikimedia --count 5000

    # Collect from GitHub Events API
    python demo/collect_live_data.py --source github --count 1000

    # Continuous collection (Ctrl+C to stop)
    python demo/collect_live_data.py --source wikimedia --continuous
"""

import argparse
import json
import os
import sys
import time
import io

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
import uuid
import hashlib
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Generator

# ---------------------------------------------------------------------------
# Wikimedia EventStreams Collector
# ---------------------------------------------------------------------------
# Source: https://stream.wikimedia.org/v2/stream/recentchange
# Volume: ~50-100 events/second
# Auth: None required
# Docs: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
# ---------------------------------------------------------------------------

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Map Wikimedia edit types to HTTP methods
WIKI_TYPE_TO_METHOD = {
    "edit": "PUT",
    "new": "POST",
    "log": "POST",
    "categorize": "PATCH",
    "external": "GET",
}

# Map Wikimedia properties to simulate realistic tenant/plan behavior
WIKI_NAMESPACE_TO_TIER = {
    0: "enterprise",      # Main article space → high-volume enterprise tenant
    1: "professional",    # Talk pages → professional tier
    2: "starter",         # User pages → starter tier
    3: "starter",         # User talk → starter tier
    4: "professional",    # Wikipedia project pages
    6: "enterprise",      # File pages → heavy usage
    14: "starter",        # Category pages
}

# Simulated API endpoints based on wiki action
WIKI_ACTION_TO_ENDPOINT = {
    "edit": "/v1/documents/{id}",
    "new": "/v1/documents",
    "log": "/v1/audit/logs",
    "categorize": "/v1/documents/{id}/categories",
    "external": "/v1/search",
}

GATEWAY_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1", "ap-southeast-1"]


def sha256_short(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def wiki_event_to_api_event(wiki_event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a REAL Wikimedia event into our API usage event schema.

    This proves our pipeline can process real-world data. The mapping is:
      wiki user        → tenant_id + api_key
      wiki namespace   → subscription plan tier
      wiki edit type   → HTTP method
      wiki title/page  → API endpoint path
      wiki timestamp   → event_timestamp
      wiki bot flag    → abuse_signals.is_suspicious
      wiki length      → request/response sizes
    """
    event_type = wiki_event.get("type", "edit")
    wiki_user = wiki_event.get("user", "anonymous")
    namespace = wiki_event.get("namespace", 0)
    title = wiki_event.get("title", "Unknown")
    timestamp = wiki_event.get("timestamp", int(time.time()))
    bot = wiki_event.get("bot", False)
    server_name = wiki_event.get("server_name", "en.wikipedia.org")
    revision = wiki_event.get("revision", {})
    length = wiki_event.get("length", {})

    # Derive tenant from wiki server (each wiki = different "company")
    tenant_id = f"tenant-{sha256_short(server_name)[:12]}"
    plan_tier = WIKI_NAMESPACE_TO_TIER.get(namespace, "free")
    api_key = f"key-{sha256_short(wiki_user)[:12]}"
    endpoint = WIKI_ACTION_TO_ENDPOINT.get(event_type, "/v1/documents/{id}")
    http_method = WIKI_TYPE_TO_METHOD.get(event_type, "GET")

    event_ts = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    now = datetime.now(timezone.utc)

    # Content sizes from actual wiki edit
    old_len = length.get("old", 0) or 0
    new_len = length.get("new", 0) or 0
    request_size = max(abs(new_len - old_len), 64)
    response_size = max(new_len, 256)

    # Realistic latency based on operation type
    base_latency = {"edit": 180, "new": 250, "log": 50, "categorize": 90, "external": 120}
    latency_ms = round(base_latency.get(event_type, 100) * random.uniform(0.5, 2.5), 2)

    status_code = random.choices(
        [200, 201, 400, 401, 404, 429, 500],
        weights=[0.75, 0.10, 0.04, 0.02, 0.03, 0.03, 0.03],
        k=1
    )[0]
    if event_type == "new":
        status_code = 201

    status_categories = {
        200: "success", 201: "success", 400: "client_error",
        401: "unauthorized", 404: "client_error", 429: "rate_limited",
        500: "server_error",
    }

    plan_limits = {"free": 60, "starter": 300, "professional": 1000, "enterprise": 5000}
    rpm_limit = plan_limits[plan_tier]
    current_velocity = random.randint(1, int(rpm_limit * 0.8))
    risk_score = round(random.uniform(50, 90) if bot else random.uniform(0, 15), 2)
    gateway_region = random.choice(GATEWAY_REGIONS)

    is_billable = status_code < 400 and endpoint != "/v1/audit/logs"
    billing_units = round(random.uniform(1.0, 5.0), 2) if is_billable else 0.0

    return {
        "event_id": str(uuid.uuid4()),
        "event_version": "1.0.0",
        "event_timestamp": event_ts.isoformat(),
        "ingestion_timestamp": now.isoformat(),
        "processing_timestamp": None,
        "sequence_number": revision.get("new", random.randint(1, 999999)),
        "tenant_id": tenant_id,
        "subscription": {
            "subscription_id": f"sub-{tenant_id}-001",
            "plan_tier": plan_tier,
            "billing_cycle_id": event_ts.strftime("%Y-%m"),
        },
        "identity": {
            "api_key_id": sha256_short(api_key),
            "api_key_prefix": api_key[:8],
            "user_id": f"user-{sha256_short(wiki_user)}",
            "auth_method": "api_key",
            "oauth_client_id": None,
            "scopes": ["read", "write"] if event_type in ("edit", "new") else ["read"],
        },
        "request": {
            "request_id": str(uuid.uuid4()),
            "trace_id": uuid.uuid4().hex[:32],
            "span_id": uuid.uuid4().hex[:16],
            "method": http_method,
            "path": endpoint,
            "path_raw": endpoint.replace("{id}", str(wiki_event.get("id", 0))),
            "query_params_hash": sha256_short(title),
            "api_version": "v1",
            "endpoint_id": f"ep-{endpoint.replace('/', '-').strip('-')}",
            "received_at": event_ts.isoformat(),
            "content_length": request_size,
            "content_type": "application/json",
        },
        "response": {
            "status_code": status_code,
            "status_category": status_categories.get(status_code, "client_error"),
            "content_length": response_size,
            "latency_ms": latency_ms,
            "upstream_latency_ms": round(latency_ms * random.uniform(0.6, 0.9), 2),
            "cache_status": random.choice(["miss", "miss", "hit", "bypass"]),
            "error_code": f"ERR_{status_code}" if status_code >= 400 else None,
            "error_message": None,
        },
        "client": {
            "ip_address": f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "ip_address_hash": sha256_short(wiki_user + server_name),
            "ip_version": "ipv4",
            "user_agent": f"WikiBot/1.0" if bot else f"Mozilla/5.0 ({server_name})",
            "user_agent_parsed": {"browser": "Bot" if bot else "Chrome", "is_bot": str(bot).lower()},
            "sdk_name": "wiki-sdk" if bot else None,
            "sdk_version": "2.1.0" if bot else None,
        },
        "geo": {
            "country_code": random.choice(["US", "GB", "DE", "JP", "IN", "BR", "FR", "CA"]),
            "country_name": "Unknown",
            "region": server_name.split(".")[0],
            "city": "Unknown",
            "postal_code": None,
            "latitude": round(random.uniform(-60, 60), 4),
            "longitude": round(random.uniform(-180, 180), 4),
            "timezone": "UTC",
            "asn": random.randint(10000, 65000),
            "as_org": server_name,
            "is_datacenter": bot,
            "is_vpn": False,
            "is_tor": False,
            "is_proxy": False,
        },
        "rate_limiting": {
            "limit": rpm_limit,
            "remaining": max(0, rpm_limit - current_velocity),
            "reset_at": (event_ts + timedelta(seconds=60)).isoformat(),
            "window_size_seconds": 60,
            "was_rate_limited": status_code == 429,
            "rate_limit_policy": f"{plan_tier}_default",
            "quota_used_percent": round((current_velocity / rpm_limit) * 100, 2),
        },
        "abuse_signals": {
            "risk_score": risk_score,
            "risk_factors": ["bot_traffic", "high_velocity"] if bot else [],
            "velocity_1min": current_velocity,
            "velocity_5min": current_velocity * 4,
            "velocity_1hour": current_velocity * 40,
            "unique_ips_24h": random.randint(1, 5),
            "unique_endpoints_1h": random.randint(2, 8),
            "is_suspicious": bot and risk_score > 60,
            "action_taken": "allowed",
            "fingerprint_hash": sha256_short(wiki_user),
        },
        "gateway": {
            "gateway_id": f"gw-{gateway_region}-{random.randint(1,10):02d}",
            "gateway_region": gateway_region,
            "gateway_version": "3.2.1",
            "upstream_service": f"api-{event_type}-service",
            "upstream_instance": f"i-{uuid.uuid4().hex[:8]}",
        },
        "billing": {
            "billable": is_billable,
            "billing_units": billing_units,
            "pricing_tier": plan_tier,
            "cost_estimate_usd": round(billing_units * 0.001, 6),
        },
        "custom_attributes": {
            "source": "wikimedia_live",
            "wiki_server": server_name,
            "wiki_type": event_type,
            "wiki_title": title[:100],
        },
        "metadata": {
            "source": f"wikimedia-{server_name}",
            "environment": "production",
            "partition_key": tenant_id,
            "retention_days": 730,
        },
    }


def collect_wikimedia_events(count: int, continuous: bool = False) -> Generator[Dict, None, None]:
    """
    Collect REAL events from Wikimedia EventStreams (SSE).
    ~50-100 events/second, zero authentication.
    """
    try:
        import sseclient
    except ImportError:
        print("ERROR: sseclient-py required. Install: pip install sseclient-py")
        sys.exit(1)

    import requests as req

    print(f"\n  Connecting to Wikimedia EventStreams...")
    print(f"  URL: {WIKIMEDIA_STREAM_URL}")
    print(f"  Target: {'continuous' if continuous else f'{count:,} events'}\n")

    response = req.get(WIKIMEDIA_STREAM_URL, stream=True, headers={
        "Accept": "text/event-stream",
        "User-Agent": "SubscriptionTrackerDemo/1.0 (https://github.com/example; educational project)",
    })
    client = sseclient.SSEClient(response)

    collected = 0
    for sse_event in client.events():
        if sse_event.event != "message":
            continue
        try:
            wiki_event = json.loads(sse_event.data)
            api_event = wiki_event_to_api_event(wiki_event)
            yield api_event
            collected += 1

            if collected % 500 == 0:
                print(f"  Collected {collected:,} live events...")

            if not continuous and collected >= count:
                break
        except (json.JSONDecodeError, KeyError):
            continue

    print(f"  Total collected: {collected:,} live events\n")


# ---------------------------------------------------------------------------
# GitHub Events API Collector
# ---------------------------------------------------------------------------

GITHUB_EVENTS_URL = "https://api.github.com/events"

GITHUB_TYPE_TO_METHOD = {
    "PushEvent": "POST",
    "CreateEvent": "POST",
    "DeleteEvent": "DELETE",
    "PullRequestEvent": "PUT",
    "IssuesEvent": "POST",
    "WatchEvent": "GET",
    "ForkEvent": "POST",
    "IssueCommentEvent": "POST",
    "PullRequestReviewEvent": "PUT",
    "ReleaseEvent": "POST",
}


def github_event_to_api_event(gh_event: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a real GitHub event into our API usage schema."""
    event_type = gh_event.get("type", "PushEvent")
    actor = gh_event.get("actor", {})
    repo = gh_event.get("repo", {})
    created = gh_event.get("created_at", datetime.now(timezone.utc).isoformat())

    user_login = actor.get("login", "anonymous")
    repo_name = repo.get("name", "unknown/repo")
    tenant_id = f"tenant-gh-{sha256_short(user_login)[:10]}"
    plan_tier = random.choices(["free", "starter", "professional"], weights=[0.6, 0.3, 0.1])[0]
    http_method = GITHUB_TYPE_TO_METHOD.get(event_type, "GET")
    endpoint = f"/v1/repos/{event_type.replace('Event', '').lower()}"

    try:
        event_ts = datetime.fromisoformat(created.replace("Z", "+00:00"))
    except ValueError:
        event_ts = datetime.now(timezone.utc)

    latency_ms = round(random.uniform(20, 500), 2)
    status_code = random.choices([200, 201, 400, 404, 429, 500], weights=[0.75, 0.10, 0.05, 0.04, 0.03, 0.03])[0]
    plan_limits = {"free": 60, "starter": 300, "professional": 1000}
    rpm_limit = plan_limits[plan_tier]
    velocity = random.randint(1, int(rpm_limit * 0.7))

    return {
        "event_id": str(uuid.uuid4()),
        "event_version": "1.0.0",
        "event_timestamp": event_ts.isoformat(),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "processing_timestamp": None,
        "sequence_number": random.randint(1, 999999),
        "tenant_id": tenant_id,
        "subscription": {"subscription_id": f"sub-{tenant_id}", "plan_tier": plan_tier, "billing_cycle_id": event_ts.strftime("%Y-%m")},
        "identity": {"api_key_id": sha256_short(user_login), "api_key_prefix": user_login[:8], "user_id": f"user-{actor.get('id', 0)}", "auth_method": "api_key", "oauth_client_id": None, "scopes": ["read", "write"]},
        "request": {"request_id": str(uuid.uuid4()), "trace_id": uuid.uuid4().hex[:32], "span_id": uuid.uuid4().hex[:16], "method": http_method, "path": endpoint, "path_raw": endpoint, "query_params_hash": sha256_short(repo_name), "api_version": "v1", "endpoint_id": f"ep-{event_type}", "received_at": event_ts.isoformat(), "content_length": random.randint(64, 4096), "content_type": "application/json"},
        "response": {"status_code": status_code, "status_category": "success" if status_code < 400 else "client_error", "content_length": random.randint(256, 32768), "latency_ms": latency_ms, "upstream_latency_ms": round(latency_ms * 0.7, 2), "cache_status": "miss", "error_code": None, "error_message": None},
        "client": {"ip_address": f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}", "ip_address_hash": sha256_short(user_login), "ip_version": "ipv4", "user_agent": "GitHubAPI/2.0", "user_agent_parsed": {"browser": "GitHub", "is_bot": "false"}, "sdk_name": None, "sdk_version": None},
        "geo": {"country_code": random.choice(["US", "GB", "DE", "JP", "IN"]), "country_name": "Unknown", "region": "Unknown", "city": "Unknown", "postal_code": None, "latitude": 0.0, "longitude": 0.0, "timezone": "UTC", "asn": 0, "as_org": "GitHub", "is_datacenter": True, "is_vpn": False, "is_tor": False, "is_proxy": False},
        "rate_limiting": {"limit": rpm_limit, "remaining": max(0, rpm_limit - velocity), "reset_at": (event_ts + timedelta(seconds=60)).isoformat(), "window_size_seconds": 60, "was_rate_limited": status_code == 429, "rate_limit_policy": f"{plan_tier}_default", "quota_used_percent": round((velocity / rpm_limit) * 100, 2)},
        "abuse_signals": {"risk_score": round(random.uniform(0, 10), 2), "risk_factors": [], "velocity_1min": velocity, "velocity_5min": velocity * 4, "velocity_1hour": velocity * 40, "unique_ips_24h": 1, "unique_endpoints_1h": random.randint(1, 5), "is_suspicious": False, "action_taken": "allowed", "fingerprint_hash": sha256_short(user_login)},
        "gateway": {"gateway_id": f"gw-us-east-1-01", "gateway_region": "us-east-1", "gateway_version": "3.2.1", "upstream_service": "github-proxy", "upstream_instance": f"i-{uuid.uuid4().hex[:8]}"},
        "billing": {"billable": status_code < 400, "billing_units": round(random.uniform(1, 3), 2) if status_code < 400 else 0, "pricing_tier": plan_tier, "cost_estimate_usd": round(random.uniform(0.001, 0.005), 6)},
        "custom_attributes": {"source": "github_live", "github_event_type": event_type, "github_repo": repo_name},
        "metadata": {"source": "github-events-api", "environment": "production", "partition_key": tenant_id, "retention_days": 730},
    }


def collect_github_events(count: int, continuous: bool = False) -> Generator[Dict, None, None]:
    """
    Collect real events from GitHub Events API.
    Rate limited to 60 req/hour unauthenticated (good for demonstrating enforcement!).
    """
    import requests as req

    print(f"\n  Connecting to GitHub Events API...")
    print(f"  URL: {GITHUB_EVENTS_URL}")
    print(f"  Note: Rate limited to 60 req/hour (demonstrates enforcement!)\n")

    collected = 0
    seen_ids = set()

    while continuous or collected < count:
        try:
            resp = req.get(GITHUB_EVENTS_URL, headers={"Accept": "application/vnd.github.v3+json"})
            remaining = resp.headers.get("X-RateLimit-Remaining", "?")
            print(f"  GitHub API rate limit remaining: {remaining}")

            if resp.status_code == 403:
                print("  Rate limited by GitHub! Waiting 60 seconds...")
                time.sleep(60)
                continue

            events = resp.json()
            for gh_event in events:
                eid = gh_event.get("id")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)

                api_event = github_event_to_api_event(gh_event)
                yield api_event
                collected += 1

                if not continuous and collected >= count:
                    break

            # GitHub returns ~30 events per page, poll every 5 seconds
            time.sleep(5)

        except Exception as e:
            print(f"  Error: {e}. Retrying in 10 seconds...")
            time.sleep(10)

    print(f"  Total collected: {collected:,} live events\n")


# ---------------------------------------------------------------------------
# Writer: Events → JSON files (landing directory for Spark)
# ---------------------------------------------------------------------------

def write_events_to_landing(
    events: Generator[Dict, None, None],
    landing_dir: str,
    batch_size: int = 100,
) -> int:
    """
    Write events as JSON files to a landing directory.
    Spark readStream.format('json').load(landing_dir) watches this directory.
    """
    os.makedirs(landing_dir, exist_ok=True)
    batch = []
    total = 0
    batch_num = 0

    for event in events:
        batch.append(event)
        total += 1

        if len(batch) >= batch_size:
            batch_num += 1
            filename = f"events_{batch_num:06d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = os.path.join(landing_dir, filename)
            with open(filepath, "w") as f:
                for e in batch:
                    f.write(json.dumps(e) + "\n")
            batch = []

    # Write remaining
    if batch:
        batch_num += 1
        filename = f"events_{batch_num:06d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(landing_dir, filename)
        with open(filepath, "w") as f:
            for e in batch:
                f.write(json.dumps(e) + "\n")

    print(f"  Wrote {total:,} events in {batch_num} files to {landing_dir}/")
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Collect REAL live data for pipeline demo")
    parser.add_argument("--source", choices=["wikimedia", "github", "both"], default="wikimedia",
                        help="Live data source")
    parser.add_argument("--count", type=int, default=5000, help="Number of events to collect")
    parser.add_argument("--continuous", action="store_true", help="Run continuously (Ctrl+C to stop)")
    parser.add_argument("--landing-dir", default="./demo/landing_data",
                        help="Landing directory for Spark")
    parser.add_argument("--batch-size", type=int, default=100, help="Events per JSON file")

    args = parser.parse_args()

    print("=" * 65)
    print("  LIVE DATA COLLECTOR")
    print("  Real events from public APIs → Pipeline landing directory")
    print("=" * 65)

    if args.source == "wikimedia":
        events = collect_wikimedia_events(args.count, args.continuous)
    elif args.source == "github":
        events = collect_github_events(args.count, args.continuous)
    else:
        # Interleave both sources
        def both_sources():
            wiki = collect_wikimedia_events(args.count // 2, False)
            gh = collect_github_events(args.count // 2, False)
            for w in wiki:
                yield w
            for g in gh:
                yield g
        events = both_sources()

    total = write_events_to_landing(events, args.landing_dir, args.batch_size)

    print("=" * 65)
    print(f"  Done! {total:,} REAL live events ready for pipeline processing.")
    print(f"  Landing directory: {args.landing_dir}/")
    print(f"\n  Next: Run the pipeline:")
    print(f"    python demo/run_pipeline.py --input-dir {args.landing_dir}")
    print("=" * 65)


if __name__ == "__main__":
    main()
