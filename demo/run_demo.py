"""
One-Command Demo Launcher
==========================

Runs the entire demo end-to-end with a single command:
  1. Collects real live events from Wikimedia EventStreams
  2. Processes them through Bronze → Silver → Gold pipeline
  3. Launches the Streamlit dashboard

Usage:
    python demo/run_demo.py                    # Full demo (5000 events)
    python demo/run_demo.py --quick            # Quick demo (1000 events)
    python demo/run_demo.py --skip-collect     # Skip collection, just run pipeline + dashboard
    python demo/run_demo.py --dashboard-only   # Just launch dashboard (data must exist)
"""

import argparse
import os
import sys
import subprocess
import time
import io

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
DEMO_DIR = os.path.join(PROJECT_ROOT, "demo")
LANDING_DIR = os.path.join(DEMO_DIR, "landing_data")
DELTA_DIR = os.path.join(DEMO_DIR, "delta_tables")


def print_banner():
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║   🔒  SUBSCRIPTION ENFORCEMENT PLATFORM — LIVE DEMO             ║
║                                                                  ║
║   Real events • Real pipeline • Real results                     ║
║   Powered by Apache Spark + Delta Lake + Streamlit               ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """)


def check_dependencies():
    """Check that required packages are installed."""
    print("  Checking dependencies...")
    missing = []

    try:
        import pyspark
        print(f"    ✓ PySpark {pyspark.__version__}")
    except ImportError:
        missing.append("pyspark")

    try:
        import delta
        print(f"    ✓ Delta Lake")
    except ImportError:
        missing.append("delta-spark")

    try:
        import streamlit
        print(f"    ✓ Streamlit {streamlit.__version__}")
    except ImportError:
        missing.append("streamlit")

    try:
        import plotly
        print(f"    ✓ Plotly {plotly.__version__}")
    except ImportError:
        missing.append("plotly")

    try:
        import sseclient
        print(f"    ✓ sseclient-py")
    except ImportError:
        missing.append("sseclient-py")

    try:
        import requests
        print(f"    ✓ requests {requests.__version__}")
    except ImportError:
        missing.append("requests")

    if missing:
        print(f"\n  Missing packages: {', '.join(missing)}")
        print(f"  Install with: pip install -r demo/requirements.txt")
        print(f"  Or: pip install {' '.join(missing)}")
        return False

    print("    All dependencies satisfied!\n")
    return True


def step_collect(count: int, source: str = "wikimedia"):
    """Step 1: Collect live data."""
    print("=" * 65)
    print(f"  STEP 1: Collecting {count:,} live events from {source}")
    print("=" * 65)

    collector_script = os.path.join(DEMO_DIR, "collect_live_data.py")

    result = subprocess.run(
        [sys.executable, collector_script,
         "--source", source,
         "--count", str(count),
         "--landing-dir", LANDING_DIR],
        cwd=PROJECT_ROOT,
    )

    if result.returncode != 0:
        print("  ERROR: Data collection failed!")
        return False

    # Verify files exist
    json_files = [f for f in os.listdir(LANDING_DIR) if f.endswith(".json")] if os.path.exists(LANDING_DIR) else []
    print(f"  Landing directory: {len(json_files)} files ready\n")
    return len(json_files) > 0


def step_pipeline():
    """Step 2: Run Bronze → Silver → Gold pipeline."""
    print("=" * 65)
    print("  STEP 2: Running Medallion Pipeline (Bronze → Silver → Gold)")
    print("=" * 65)

    pipeline_script = os.path.join(DEMO_DIR, "run_pipeline.py")

    result = subprocess.run(
        [sys.executable, pipeline_script,
         "--input-dir", LANDING_DIR,
         "--output-dir", DELTA_DIR],
        cwd=PROJECT_ROOT,
    )

    if result.returncode != 0:
        print("  ERROR: Pipeline execution failed!")
        return False

    # Verify Gold tables exist
    gold_dir = os.path.join(DELTA_DIR, "gold")
    if os.path.exists(gold_dir):
        tables = [d for d in os.listdir(gold_dir) if os.path.isdir(os.path.join(gold_dir, d))]
        print(f"  Gold tables created: {', '.join(tables)}\n")
        return len(tables) > 0

    return False


def step_dashboard():
    """Step 3: Launch Streamlit dashboard."""
    print("=" * 65)
    print("  STEP 3: Launching Dashboard")
    print("=" * 65)

    gold_dir = os.path.join(DELTA_DIR, "gold")
    dashboard_script = os.path.join(DEMO_DIR, "dashboard.py")

    print(f"  Dashboard: {dashboard_script}")
    print(f"  Data source: {gold_dir}")
    print(f"")
    print(f"  Opening in browser at http://localhost:8501")
    print(f"  Press Ctrl+C to stop\n")

    env = os.environ.copy()
    env["DELTA_DIR"] = gold_dir

    try:
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", dashboard_script,
             "--server.headless", "true",
             "--browser.gatherUsageStats", "false"],
            cwd=PROJECT_ROOT,
            env=env,
        )
    except KeyboardInterrupt:
        print("\n  Dashboard stopped.")


def main():
    parser = argparse.ArgumentParser(description="Run the complete demo end-to-end")
    parser.add_argument("--quick", action="store_true", help="Quick demo with 1000 events")
    parser.add_argument("--count", type=int, default=5000, help="Number of events to collect")
    parser.add_argument("--source", choices=["wikimedia", "github"], default="wikimedia",
                        help="Data source")
    parser.add_argument("--skip-collect", action="store_true",
                        help="Skip data collection (use existing data)")
    parser.add_argument("--skip-pipeline", action="store_true",
                        help="Skip pipeline (use existing Delta tables)")
    parser.add_argument("--dashboard-only", action="store_true",
                        help="Only launch dashboard")

    args = parser.parse_args()

    print_banner()

    # Check dependencies
    if not check_dependencies():
        sys.exit(1)

    count = 1000 if args.quick else args.count

    if args.dashboard_only:
        step_dashboard()
        return

    # Step 1: Collect
    if not args.skip_collect and not args.skip_pipeline:
        if not step_collect(count, args.source):
            print("  Falling back to existing data...")
            if not os.path.exists(LANDING_DIR):
                print("  ERROR: No landing data found. Run without --skip-collect")
                sys.exit(1)

    # Step 2: Pipeline
    if not args.skip_pipeline:
        if not step_pipeline():
            print("  ERROR: Pipeline failed. Check Spark/Java installation.")
            sys.exit(1)

    # Step 3: Dashboard
    step_dashboard()


if __name__ == "__main__":
    main()
