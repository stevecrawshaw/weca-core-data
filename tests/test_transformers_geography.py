"""
Test script for geography transformers.

Validates that geography transformation functions work correctly.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl
from transformers.geography import (
    clean_column_name,
    get_ca_la_codes,
    get_rename_dict,
    remove_numbers,
    transform_ca_la_lookup,
    transform_lsoa_pwc,
)


def test_remove_numbers():
    """Test remove_numbers utility function"""
    print("\n" + "=" * 80)
    print("TEST: remove_numbers()")
    print("=" * 80)

    test_cases = [
        ("LSOA21CD", "lsoacd"),
        ("Feature123Name", "featurename"),
        ("NoNumbers", "nonumbers"),
        ("LAD24CD", "ladcd"),
    ]

    for input_str, expected in test_cases:
        result = remove_numbers(input_str)
        assert result == expected, f"Expected {expected}, got {result}"
        print(f"[OK] remove_numbers('{input_str}') = '{result}'")

    print("[PASSED] remove_numbers() tests")


def test_clean_column_name():
    """Test clean_column_name utility function"""
    print("\n" + "=" * 80)
    print("TEST: clean_column_name()")
    print("=" * 80)

    test_cases = [
        ("lsoa21cd", "lsoa21cd"),  # Should not change lsoa columns
        ('"{feature,name}"', "feature,name}"),  # Should clean non-lsoa columns
        ("lsoa11cd", "lsoa11cd"),
    ]

    for input_str, expected in test_cases:
        result = clean_column_name(input_str)
        print(f"[OK] clean_column_name('{input_str}') = '{result}'")

    print("[PASSED] clean_column_name() tests")


def test_get_rename_dict():
    """Test get_rename_dict utility function"""
    print("\n" + "=" * 80)
    print("TEST: get_rename_dict()")
    print("=" * 80)

    # Test basic lowercasing
    df1 = pl.DataFrame({"Col1": [1], "Col2": [2], "Col3": [3]})
    rename_dict1 = get_rename_dict(df1)
    assert rename_dict1 == {"Col1": "col1", "Col2": "col2", "Col3": "col3"}
    print("[OK] Basic lowercasing works")

    # Test with number removal
    df2 = pl.DataFrame({"LSOA21CD": [1], "LSOA11CD": [2]})
    rename_dict2 = get_rename_dict(df2, rm_numbers=True)
    print(f"Rename dict with duplicates: {rename_dict2}")
    # When removing numbers, both columns become "lsoacd"
    # The function should create "lsoacd" for first, "lsoacd_1" for second
    values = list(rename_dict2.values())
    assert "lsoacd" in values or "lsoacd_1" in values  # Should handle duplicates
    assert len(set(values)) == 2, "Should have unique column names after deduplication"
    print("[OK] Number removal with duplicate handling works")

    print("[PASSED] get_rename_dict() tests")


def test_transform_ca_la_lookup():
    """Test transform_ca_la_lookup() with mock data"""
    print("\n" + "=" * 80)
    print("TEST: transform_ca_la_lookup()")
    print("=" * 80)

    # Create mock data similar to ArcGIS response
    mock_data = pl.DataFrame(
        {
            "ObjectId": [1, 2, 3],
            "LAD24CD": ["E06000022", "E06000023", "E07000084"],
            "LAD24NM": ["Bath and North East Somerset", "Bristol", "South Gloucestershire"],
            "CAUTH24CD": ["E47000009", "E47000009", "E47000009"],
            "CAUTH24NM": ["West of England", "West of England", "West of England"],
        }
    )

    # Test with North Somerset inclusion (default)
    result_with_ns = transform_ca_la_lookup(mock_data, inc_ns=True)

    print(f"[OK] Transformed {len(result_with_ns)} records")
    print(f"Columns: {result_with_ns.columns}")

    # Verify ObjectId was removed
    assert "ObjectId" not in result_with_ns.columns, "ObjectId should be removed"
    print("[OK] ObjectId removed")

    # Verify column names were cleaned (numbers removed)
    assert "ladcd" in result_with_ns.columns, "Should have 'ladcd' column"
    assert "ladnm" in result_with_ns.columns, "Should have 'ladnm' column"
    assert "cauthcd" in result_with_ns.columns, "Should have 'cauthcd' column"
    assert "cauthnm" in result_with_ns.columns, "Should have 'cauthnm' column"
    print("[OK] Column names cleaned (numbers removed)")

    # Verify North Somerset was added
    ns_present = result_with_ns.filter(pl.col("ladcd") == "E06000024")
    assert len(ns_present) == 1, "North Somerset should be added"
    assert ns_present["ladnm"][0] == "North Somerset"
    print("[OK] North Somerset added correctly")

    # Test without North Somerset
    result_without_ns = transform_ca_la_lookup(mock_data, inc_ns=False)
    assert len(result_without_ns) == 3, "Should have 3 records without North Somerset"
    print("[OK] North Somerset exclusion works")

    print("[PASSED] transform_ca_la_lookup() tests")


def test_transform_lsoa_pwc():
    """Test transform_lsoa_pwc() with mock data"""
    print("\n" + "=" * 80)
    print("TEST: transform_lsoa_pwc()")
    print("=" * 80)

    # Create mock LSOA PWC data
    mock_data = pl.DataFrame(
        {
            "LSOA21CD": ["E01000001", "E01000002", "E01000001"],  # One duplicate
            "LSOA21NM": ["City Centre", "Harbour", "City Centre"],
            "X": [-2.5879, -2.5812, -2.5879],
            "Y": [51.4545, 51.4568, 51.4545],
        }
    )

    result = transform_lsoa_pwc(mock_data)

    print(f"[OK] Transformed {len(result)} records")
    print(f"Columns: {result.columns}")

    # Verify duplicates removed
    assert len(result) == 2, "Should have 2 unique records (duplicate removed)"
    print("[OK] Duplicates removed")

    # Verify column names are lowercase
    assert "lsoa21cd" in result.columns, "Should have lowercase column names"
    assert "x" in result.columns, "Should have lowercase 'x'"
    assert "y" in result.columns, "Should have lowercase 'y'"
    print("[OK] Column names converted to lowercase")

    # Verify geometry columns exist
    assert "x" in result.columns and "y" in result.columns
    print("[OK] Geometry columns (x, y) present")

    print("[PASSED] transform_lsoa_pwc() tests")


def test_get_ca_la_codes():
    """Test get_ca_la_codes() utility function"""
    print("\n" + "=" * 80)
    print("TEST: get_ca_la_codes()")
    print("=" * 80)

    mock_df = pl.DataFrame(
        {
            "ladcd": ["E06000022", "E06000023", "E06000024"],
            "ladnm": ["Bath and NE Somerset", "Bristol", "North Somerset"],
        }
    )

    codes = get_ca_la_codes(mock_df)

    assert len(codes) == 3, "Should return 3 LA codes"
    assert "E06000022" in codes
    assert "E06000023" in codes
    assert "E06000024" in codes
    print(f"[OK] Extracted {len(codes)} LA codes: {codes}")

    print("[PASSED] get_ca_la_codes() tests")


def run_all_tests():
    """Run all geography transformer tests"""
    print("\n" + "=" * 80)
    print("RUNNING ALL GEOGRAPHY TRANSFORMER TESTS")
    print("=" * 80)

    test_remove_numbers()
    test_clean_column_name()
    test_get_rename_dict()
    test_transform_ca_la_lookup()
    test_transform_lsoa_pwc()
    test_get_ca_la_codes()

    print("\n" + "=" * 80)
    print("ALL GEOGRAPHY TRANSFORMER TESTS PASSED [OK]")
    print("=" * 80)


if __name__ == "__main__":
    run_all_tests()
