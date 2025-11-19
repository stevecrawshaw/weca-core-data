"""
Test script for ArcGIS sources

Validates that the ArcGIS source extracts data correctly.
Tests with full extraction for one small resource.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import dlt
from sources.arcgis_sources import ca_boundaries_source
import duckdb


def test_ca_boundaries():
    """Test CA boundaries extraction - small dataset suitable for testing"""

    print("=" * 80)
    print("TEST: Combined Authority Boundaries Extraction")
    print("=" * 80)

    pipeline = dlt.pipeline(
        pipeline_name="test_arcgis_ca",
        destination="duckdb",
        dataset_name="test_data",
        dev_mode=True,  # Use temporary database
    )

    print("\nExtracting CA boundaries...")
    source = ca_boundaries_source()
    load_info = pipeline.run(source)

    print(f"[OK] Load completed")
    print(f"Load info: {load_info}")
    print(f"Has failed jobs: {load_info.has_failed_jobs}")

    # Verify data - get connection to the pipeline's database
    # Get the actual dataset name (may include timestamp in dev_mode)
    dataset_name = pipeline.dataset_name

    with pipeline.sql_client() as client:
        # Check if table exists and has data
        with client.execute_query(
            f"SELECT COUNT(*) as count FROM {dataset_name}.ca_boundaries_2025"
        ) as cursor:
            result = cursor.fetchone()
            count = result[0] if result else 0

    print(f"[OK] Records in DB: {count}")

    # Show sample data
    with pipeline.sql_client() as client:
        with client.execute_query(
            f"SELECT * FROM {dataset_name}.ca_boundaries_2025 LIMIT 3"
        ) as cursor:
            print("\nSample data:")
            for row in cursor.fetchall():
                print(row)

    print("\n" + "=" * 80)
    if count > 0:
        print("[OK] TEST SUCCESS: CA boundaries extraction works")
        print(f"     Extracted {count} Combined Authority boundaries")
    else:
        print("[FAIL] TEST FAILED: No records extracted")
    print("=" * 80)

    return count > 0


if __name__ == "__main__":
    # Run test
    test_pass = test_ca_boundaries()

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"CA Boundaries: {'PASS' if test_pass else 'FAIL'}")
    print("=" * 80)

    if test_pass:
        print("\n[OK] Test passed!")
        sys.exit(0)
    else:
        print("\n[FAIL] Test failed")
        sys.exit(1)
