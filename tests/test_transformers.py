"""
Unit tests for transformer functions.

Tests all transformation functions from:
- transformers/emissions.py
- transformers/geography.py
"""

import polars as pl
import pytest

from transformers.emissions import (
    transform_dft_lookup,
    transform_ghg_emissions,
    transform_imd_2025,
)
from transformers.geography import (
    clean_column_name,
    get_ca_la_codes,
    get_rename_dict,
    remove_numbers,
    transform_ca_la_lookup,
    transform_lsoa_pwc,
)


# ============================================================================
# Utility Functions Tests
# ============================================================================


class TestRemoveNumbers:
    """Test the remove_numbers utility function."""

    def test_removes_all_digits(self):
        """Test that all digits are removed."""
        assert remove_numbers("LSOA21CD") == "lsoacd"
        assert remove_numbers("TEST123XYZ") == "testxyz"

    def test_converts_to_lowercase(self):
        """Test that result is lowercase."""
        assert remove_numbers("UPPERCASE") == "uppercase"
        assert remove_numbers("MixedCase123") == "mixedcase"

    def test_handles_no_numbers(self):
        """Test strings without numbers."""
        assert remove_numbers("hello") == "hello"
        assert remove_numbers("ABC") == "abc"

    def test_handles_only_numbers(self):
        """Test strings that are only numbers."""
        assert remove_numbers("12345") == ""

    def test_handles_empty_string(self):
        """Test empty string input."""
        assert remove_numbers("") == ""


class TestCleanColumnName:
    """Test the clean_column_name utility function."""

    def test_cleans_pivoted_column_names(self):
        """Test cleaning of pivot table column names."""
        # Function removes quotes, braces, commas and skips first 2 chars with [2:]
        assert clean_column_name('"{lsoa,feature}"') == "oafeature"
        assert clean_column_name('"{test,value}"') == "stvalue"

    def test_preserves_lsoa_column_names(self):
        """Test that LSOA column names are preserved."""
        assert clean_column_name("lsoa21cd") == "lsoa21cd"
        assert clean_column_name("lsoa_code") == "lsoa_code"

    def test_removes_special_characters(self):
        """Test removal of quotes, braces, commas."""
        assert clean_column_name('"test"') == "st"
        assert clean_column_name("{test}") == "st"


class TestGetRenameDict:
    """Test the get_rename_dict utility function."""

    def test_converts_to_lowercase(self):
        """Test that column names are converted to lowercase."""
        df = pl.DataFrame({"COL1": [1], "COL2": [2]})
        result = get_rename_dict(df, rm_numbers=False)
        assert result == {"COL1": "col1", "COL2": "col2"}

    def test_removes_numbers_when_requested(self):
        """Test number removal from column names."""
        df = pl.DataFrame({"LSOA21CD": [1], "LSOA21NM": [2]})
        result = get_rename_dict(df, rm_numbers=True)
        assert result["LSOA21CD"] == "lsoacd"
        assert result["LSOA21NM"] == "lsoanm"

    def test_handles_duplicate_columns(self):
        """Test handling of duplicate column names after transformation."""
        # Create DataFrame with columns that become duplicates after lowercasing
        df = pl.DataFrame({"Col": [1], "col": [2], "COL": [3]})
        result = get_rename_dict(df, rm_numbers=False)
        # First occurrence stays as 'col', duplicates get suffixes
        assert "col" in result.values()
        assert "col_1" in result.values() or "col_2" in result.values()


# ============================================================================
# Geography Transformations Tests
# ============================================================================


class TestTransformCaLaLookup:
    """Test the transform_ca_la_lookup function."""

    def test_removes_objectid_column(self, sample_ca_la_df):
        """Test that ObjectId column is removed."""
        result = transform_ca_la_lookup(sample_ca_la_df, inc_ns=False)
        assert "ObjectId" not in result.columns
        assert "objectid" not in result.columns

    def test_removes_numbers_from_column_names(self, sample_ca_la_df):
        """Test that numbers are removed from column names."""
        result = transform_ca_la_lookup(sample_ca_la_df, inc_ns=False)
        assert "ladcd" in result.columns
        assert "ladnm" in result.columns
        assert "cauthcd" in result.columns
        assert "cauthnm" in result.columns

    def test_adds_north_somerset_when_inc_ns_true(self, sample_ca_la_df):
        """Test that North Somerset is added when inc_ns=True."""
        result = transform_ca_la_lookup(sample_ca_la_df, inc_ns=True)
        ns_row = result.filter(pl.col("ladcd") == "E06000024")
        assert len(ns_row) == 1
        assert ns_row["ladnm"][0] == "North Somerset"

    def test_excludes_north_somerset_when_inc_ns_false(self, sample_ca_la_df):
        """Test that North Somerset is not added when inc_ns=False."""
        result = transform_ca_la_lookup(sample_ca_la_df, inc_ns=False)
        # Original data has 3 rows; after transformation should still have 3 rows
        # (ObjectId is a column, not a row)
        assert len(result) == len(sample_ca_la_df)
        # Verify North Somerset is not in the result
        assert "E06000024" not in result["ladcd"].to_list()


class TestTransformLsoaPwc:
    """Test the transform_lsoa_pwc function."""

    def test_converts_columns_to_lowercase(self, sample_lsoa_pwc_df):
        """Test that all column names are lowercase."""
        result = transform_lsoa_pwc(sample_lsoa_pwc_df)
        for col in result.columns:
            assert col == col.lower()

    def test_removes_duplicates(self):
        """Test that duplicate rows are removed."""
        df_with_dups = pl.DataFrame(
            {
                "LSOA21CD": ["E01014533", "E01014533", "E01014534"],
                "X": [-2.5879, -2.5879, -2.5895],
                "Y": [51.4545, 51.4545, 51.4560],
            }
        )
        result = transform_lsoa_pwc(df_with_dups)
        assert len(result) == 2  # Duplicates removed

    def test_validates_geometry_columns_exist(self):
        """Test that error is raised if x/y columns missing."""
        df_no_geom = pl.DataFrame(
            {
                "LSOA21CD": ["E01014533"],
                "LSOA21NM": ["Bristol 001A"],
            }
        )
        with pytest.raises(ValueError, match="Required geometry columns"):
            transform_lsoa_pwc(df_no_geom)

    def test_preserves_all_data(self, sample_lsoa_pwc_df):
        """Test that all data is preserved (just transformed)."""
        result = transform_lsoa_pwc(sample_lsoa_pwc_df)
        assert len(result) == len(sample_lsoa_pwc_df)


class TestGetCaLaCodes:
    """Test the get_ca_la_codes function."""

    def test_extracts_la_codes(self, sample_ca_la_df):
        """Test that LA codes are extracted correctly."""
        # Transform first to get clean column names
        clean_df = transform_ca_la_lookup(sample_ca_la_df, inc_ns=False)
        result = get_ca_la_codes(clean_df)
        assert isinstance(result, list)
        assert len(result) == 3
        assert "E06000023" in result

    def test_raises_error_when_ladcd_missing(self):
        """Test error when 'ladcd' column is missing."""
        df_no_ladcd = pl.DataFrame({"other_col": ["A", "B"]})
        with pytest.raises(ValueError, match="'ladcd' column not found"):
            get_ca_la_codes(df_no_ladcd)


# ============================================================================
# Emissions Transformations Tests
# ============================================================================


class TestTransformGhgEmissions:
    """Test the transform_ghg_emissions function."""

    def test_removes_duplicates(self):
        """Test that duplicate rows are removed."""
        df_with_dups = pl.DataFrame(
            {
                "LA Code": ["E06000023", "E06000023", "E06000024"],
                "Year": [2021, 2021, 2021],
                "CO2_kt": [1500.5, 1500.5, 800.3],
            }
        )
        result = transform_ghg_emissions(df_with_dups, la_codes=None)
        assert len(result) == 2  # Duplicates removed

    def test_filters_by_la_codes_when_provided(self, sample_ghg_df):
        """Test filtering by LA codes."""
        la_codes = ["E06000023", "E06000024"]
        result = transform_ghg_emissions(sample_ghg_df, la_codes=la_codes)
        assert len(result) == 2
        assert all(result["LA Code"].is_in(la_codes))

    def test_returns_all_when_la_codes_none(self, sample_ghg_df):
        """Test that all data is returned when la_codes=None."""
        result = transform_ghg_emissions(sample_ghg_df, la_codes=None)
        assert len(result) == len(sample_ghg_df)

    def test_handles_alternative_la_code_column_names(self):
        """Test that function finds LA code column with different names."""
        # Test with "ladcd" column name
        df_alt = pl.DataFrame(
            {
                "ladcd": ["E06000023", "E06000024"],
                "Year": [2021, 2021],
                "CO2_kt": [1500.5, 800.3],
            }
        )
        result = transform_ghg_emissions(df_alt, la_codes=["E06000023"])
        assert len(result) == 1


class TestTransformDftLookup:
    """Test the transform_dft_lookup function."""

    def test_validates_required_columns(self):
        """Test that error is raised when required columns are missing."""
        df_missing = pl.DataFrame({"year": [2023]})
        with pytest.raises(ValueError, match="Missing required columns"):
            transform_dft_lookup(df_missing, la_codes=None)

    def test_extracts_most_recent_year(self, sample_dft_df):
        """Test that only the most recent year's data is returned."""
        result = transform_dft_lookup(sample_dft_df, la_codes=None)
        assert result["year"].unique().to_list() == [2023]

    def test_renames_columns_correctly(self, sample_dft_df):
        """Test that columns are renamed to expected names."""
        result = transform_dft_lookup(sample_dft_df, la_codes=None)
        assert "dft_la_id" in result.columns
        assert "ladcd" in result.columns
        assert "year" in result.columns

    def test_filters_by_la_codes_when_provided(self, sample_dft_df):
        """Test filtering by LA codes."""
        la_codes = ["E06000023", "E06000024"]
        result = transform_dft_lookup(sample_dft_df, la_codes=la_codes)
        assert all(result["ladcd"].is_in(la_codes))

    def test_returns_all_when_la_codes_none(self, sample_dft_df):
        """Test that all LAs are returned when la_codes=None."""
        result = transform_dft_lookup(sample_dft_df, la_codes=None)
        # Should have 3 unique LAs from 2023 data
        assert len(result) == 3

    def test_handles_empty_result_gracefully(self, sample_dft_df):
        """Test that empty result is handled without error."""
        # Filter for non-existent LA code
        result = transform_dft_lookup(sample_dft_df, la_codes=["E99999999"])
        assert len(result) == 0


class TestTransformImd2025:
    """Test the transform_imd_2025 function."""

    def test_validates_lsoa21_code_column(self):
        """Test that error is raised when lsoa21_code column is missing."""
        df_missing = pl.DataFrame({"other_col": [1, 2, 3]})
        with pytest.raises(ValueError, match="'lsoa21_code' column not found"):
            transform_imd_2025(df_missing, lsoa_codes=None)

    def test_removes_duplicates(self):
        """Test that duplicate LSOAs are removed."""
        df_with_dups = pl.DataFrame(
            {
                "lsoa21_code": ["E01014533", "E01014533", "E01014534"],
                "imd_score": [25.5, 25.5, 18.3],
            }
        )
        result = transform_imd_2025(df_with_dups, lsoa_codes=None)
        assert len(result) == 2  # Duplicates removed

    def test_filters_by_lsoa_codes_when_provided(self, sample_imd_df):
        """Test filtering by LSOA codes."""
        lsoa_codes = ["E01014533", "E01014534"]
        result = transform_imd_2025(sample_imd_df, lsoa_codes=lsoa_codes)
        assert len(result) == 2
        assert all(result["lsoa21_code"].is_in(lsoa_codes))

    def test_returns_all_when_lsoa_codes_none(self, sample_imd_df):
        """Test that all data is returned when lsoa_codes=None."""
        result = transform_imd_2025(sample_imd_df, lsoa_codes=None)
        assert len(result) == len(sample_imd_df)

    def test_preserves_all_columns(self, sample_imd_df):
        """Test that all IMD indicator columns are preserved."""
        result = transform_imd_2025(sample_imd_df, lsoa_codes=None)
        # All original columns should be present
        for col in sample_imd_df.columns:
            assert col in result.columns
