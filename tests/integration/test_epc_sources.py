"""
Test script for EPC sources

Validates that the EPC source extracts data correctly.
Tests with a small date range (1 month) to keep test quick.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import dlt
from sources.epc_sources import epc_certificates_source


def test_epc_domestic():
    """Test EPC domestic certificates extraction"""

    print("=" * 80)
    print("TEST: EPC Domestic Certificates Extraction")
    print("=" * 80)

    pipeline = dlt.pipeline(
        pipeline_name="test_epc_domestic",
        destination="duckdb",
        dataset_name="test_data",
        dev_mode=True,
    )

    # Test with a small date range (January 2024) for quick validation
    # and a specific local authority to limit results
    print("\nExtracting EPC domestic certificates...")
    print("  Date range: January 2024")
    print("  Local authority: E06000023 (Bristol)")

    source = epc_certificates_source(
        certificate_type="domestic",
        local_authority="E06000023",  # Bristol
        from_month=1,
        from_year=2024,
        to_month=1,
        to_year=2024,
    )

    load_info = pipeline.run(source)

    print(f"[OK] Load completed")
    print(f"Load info: {load_info}")
    print(f"Has failed jobs: {load_info.has_failed_jobs}")

    # Verify data
    dataset_name = pipeline.dataset_name

    with pipeline.sql_client() as client:
        with client.execute_query(
            f"SELECT COUNT(*) as count FROM {dataset_name}.epc_domestic"
        ) as cursor:
            result = cursor.fetchone()
            count = result[0] if result else 0

    print(f"[OK] Records in DB: {count}")

    # Show sample data
    if count > 0:
        with pipeline.sql_client() as client:
            with client.execute_query(
                f"SELECT * FROM {dataset_name}.epc_domestic LIMIT 3"
            ) as cursor:
                print("\nSample data (first 3 records):")
                columns = [desc[0] for desc in cursor.description]
                print(f"Columns: {', '.join(columns[:10])}...")  # Show first 10 cols
                for row in cursor.fetchall():
                    print(f"  Row: {row[:5]}...")  # Show first 5 fields

    print("\n" + "=" * 80)
    if count > 0:
        print("[OK] TEST SUCCESS: EPC domestic extraction works")
        print(f"     Extracted {count} certificates from Bristol in Jan 2024")
    else:
        print("[FAIL] TEST FAILED: No records extracted")
    print("=" * 80)

    return count > 0


if __name__ == "__main__":
    # Run test
    test_pass = test_epc_domestic()

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"EPC Domestic: {'PASS' if test_pass else 'FAIL'}")
    print("=" * 80)

    if test_pass:
        print("\n[OK] Test passed!")
        sys.exit(0)
    else:
        print("\n[FAIL] Test failed")
        sys.exit(1)
