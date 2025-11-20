#!/usr/bin/env python3
"""
Fast sample test - skips slow ArcGIS sources
Tests only CSV-based sources (DFT, GHG)
"""

import dlt
from pathlib import Path
import logging

from sources.other_sources import dft_traffic_resource, ghg_emissions_resource

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def test_fast_sample():
    """Test CSV sources only - fast and reliable"""

    print("=" * 80)
    print("FAST SAMPLE TEST - CSV Sources Only")
    print("Skipping ArcGIS (slow) and IMD (may be blocked)")
    print("=" * 80)
    print()

    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="fast_sample",
        destination=dlt.destinations.duckdb("data/fast_sample.duckdb"),
        dataset_name="test_data",
    )

    row_limit = 1000

    # Test DFT
    print("[1/2] Extracting DFT traffic data (1,000 records)...")
    try:
        load_info = pipeline.run(dft_traffic_resource(row_limit=row_limit))
        if load_info.has_failed_jobs:
            print("❌ DFT failed")
        else:
            print("✅ DFT extracted")
    except Exception as e:
        print(f"❌ DFT error: {e}")

    # Test GHG
    print("\n[2/2] Extracting GHG emissions data (1,000 records)...")
    try:
        load_info = pipeline.run(ghg_emissions_resource(row_limit=row_limit))
        if load_info.has_failed_jobs:
            print("❌ GHG failed")
        else:
            print("✅ GHG extracted")
    except Exception as e:
        print(f"❌ GHG error: {e}")

    # Verify
    print("\n" + "=" * 80)
    print("Verification")
    print("=" * 80)

    import duckdb
    con = duckdb.connect("data/fast_sample.duckdb")

    try:
        dft_count = con.execute("SELECT COUNT(*) FROM test_data.dft_traffic").fetchone()[0]
        print(f"✅ DFT Traffic: {dft_count} records")
    except:
        print("⚠️  DFT Traffic: No data")

    try:
        ghg_count = con.execute("SELECT COUNT(*) FROM test_data.ghg_emissions").fetchone()[0]
        print(f"✅ GHG Emissions: {ghg_count} records")
    except:
        print("⚠️  GHG Emissions: No data")

    con.close()

    print("\n" + "=" * 80)
    print("Fast sample test complete!")
    print("=" * 80)


if __name__ == "__main__":
    test_fast_sample()
