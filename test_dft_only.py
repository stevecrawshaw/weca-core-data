#!/usr/bin/env python3
"""
Quick test with only DFT Traffic source (most reliable)
Use this to verify the pipeline works before trying full extraction
"""

import dlt
from pathlib import Path
from sources.other_sources import dft_traffic_resource

def test_dft_only():
    """Test DFT extraction only - should work even in restricted environments"""

    print("=" * 80)
    print("QUICK TEST: DFT Traffic Source Only")
    print("=" * 80)
    print()

    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="test_dft",
        destination=dlt.destinations.duckdb("data/test_dft.duckdb"),
        dataset_name="test_data",
    )

    print("Extracting 100 records from DFT Traffic CSV...")
    print("(Google Cloud Storage - usually accessible)")
    print()

    try:
        # Extract with row limit
        load_info = pipeline.run(dft_traffic_resource(row_limit=100))

        if load_info.has_failed_jobs:
            print("❌ FAILED: Extraction had errors")
            return False

        print("✅ SUCCESS: DFT data extracted")
        print()

        # Verify
        import duckdb
        con = duckdb.connect("data/test_dft.duckdb")
        count = con.execute("SELECT COUNT(*) FROM test_data.dft_traffic").fetchone()[0]

        print(f"Verified: {count} records in database")
        con.close()

        print()
        print("=" * 80)
        print("Pipeline infrastructure is working correctly!")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. The pipeline code works fine")
        print("  2. ArcGIS may be slow - give it 10-15 minutes")
        print("  3. Or skip ArcGIS with --fast-sample (if we add that flag)")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import sys
    success = test_dft_only()
    sys.exit(0 if success else 1)
