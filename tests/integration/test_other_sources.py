"""
Test script for other data sources (DFT, GHG, IMD)

Validates that the CSV-based sources extract data correctly.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import dlt
from sources.other_sources import ghg_emissions_resource


def test_ghg_emissions():
    """Test GHG emissions extraction - medium-sized CSV dataset"""

    print("=" * 80)
    print("TEST: GHG Emissions Data Extraction")
    print("=" * 80)

    pipeline = dlt.pipeline(
        pipeline_name="test_ghg",
        destination="duckdb",
        dataset_name="test_data",
        dev_mode=True,
    )

    print("\nExtracting GHG emissions data...")

    source = ghg_emissions_resource()
    load_info = pipeline.run(source)

    print(f"[OK] Load completed")
    print(f"Load info: {load_info}")
    print(f"Has failed jobs: {load_info.has_failed_jobs}")

    # Verify data
    dataset_name = pipeline.dataset_name

    with pipeline.sql_client() as client:
        with client.execute_query(
            f"SELECT COUNT(*) as count FROM {dataset_name}.ghg_emissions"
        ) as cursor:
            result = cursor.fetchone()
            count = result[0] if result else 0

    print(f"[OK] Records in DB: {count}")

    # Show sample data
    if count > 0:
        with pipeline.sql_client() as client:
            with client.execute_query(
                f"SELECT * FROM {dataset_name}.ghg_emissions LIMIT 5"
            ) as cursor:
                print("\nSample data (first 5 records):")
                columns = [desc[0] for desc in cursor.description]
                print(f"Columns ({len(columns)} total): {', '.join(columns[:5])}...")
                for row in cursor.fetchall():
                    print(f"  Row: {row[:5]}...")

    print("\n" + "=" * 80)
    if count > 0:
        print("[OK] TEST SUCCESS: GHG emissions extraction works")
        print(f"     Extracted {count} emissions records")
    else:
        print("[FAIL] TEST FAILED: No records extracted")
    print("=" * 80)

    return count > 0


if __name__ == "__main__":
    # Run test
    test_pass = test_ghg_emissions()

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"GHG Emissions: {'PASS' if test_pass else 'FAIL'}")
    print("=" * 80)

    if test_pass:
        print("\n[OK] Test passed!")
        sys.exit(0)
    else:
        print("\n[FAIL] Test failed")
        sys.exit(1)
