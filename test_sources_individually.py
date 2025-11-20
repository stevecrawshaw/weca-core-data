#!/usr/bin/env python3
"""
Test individual data sources to unblock pipeline
Skip ArcGIS due to 403 errors and test CSV-based sources
"""

import dlt
from pathlib import Path

# Import only the working sources
from sources.other_sources import (
    dft_traffic_resource,
    ghg_emissions_resource,
    imd_2025_resource,
)

def test_source(source_name: str, resource_func, row_limit: int = 100):
    """Test a single source with limited data"""
    print(f"\n{'='*80}")
    print(f"Testing: {source_name}")
    print(f"{'='*80}")

    try:
        # Create test pipeline
        pipeline = dlt.pipeline(
            pipeline_name=f"test_{source_name}",
            destination=dlt.destinations.duckdb("data/test_sources.duckdb"),
            dataset_name="test_data",
        )

        print(f"[1/3] Creating resource with row_limit={row_limit}...")
        resource = resource_func(row_limit=row_limit)

        print(f"[2/3] Running extraction...")
        load_info = pipeline.run(resource)

        if load_info.has_failed_jobs:
            print(f"❌ FAILED: {source_name}")
            return False

        print(f"[3/3] Verifying data...")
        import duckdb
        con = duckdb.connect("data/test_sources.duckdb")
        count = con.execute(f"SELECT COUNT(*) FROM test_data.{source_name}").fetchone()[0]
        con.close()

        print(f"✅ SUCCESS: {source_name} - {count} records loaded")
        return True

    except Exception as e:
        print(f"❌ ERROR in {source_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Test all non-ArcGIS sources"""
    print("="*80)
    print("TESTING NON-EPC DATA SOURCES")
    print("Testing with limited data (100 rows per source)")
    print("="*80)

    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)

    # Clean up any existing test database
    test_db = Path("data/test_sources.duckdb")
    if test_db.exists():
        test_db.unlink()
        print("\n🗑️  Removed old test database")

    results = {}

    # Test each source
    results["dft_traffic"] = test_source("dft_traffic", dft_traffic_resource, row_limit=100)
    results["ghg_emissions"] = test_source("ghg_emissions", ghg_emissions_resource, row_limit=100)
    results["imd_2025"] = test_source("imd_2025", imd_2025_resource, row_limit=100)

    # Summary
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    for source, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {source}")

    passed = sum(results.values())
    total = len(results)
    print(f"\nPassed: {passed}/{total}")

    if passed == total:
        print("\n🎉 All sources working! Pipeline is unblocked.")
    else:
        print("\n⚠️  Some sources failed. Check errors above.")


if __name__ == "__main__":
    main()
