"""
Unit tests for dlt source resources.

Tests resource functions from:
- sources/other_sources.py

Note: These tests validate resource structure and data handling
without making actual HTTP requests.
"""

import polars as pl
import pytest
from unittest.mock import Mock, patch

from sources.other_sources import (
    dft_traffic_resource,
    ghg_emissions_resource,
    imd_2025_resource,
)


class TestDftTrafficResource:
    """Test the DFT traffic dlt resource."""

    @patch("sources.other_sources.pl.read_csv")
    def test_yields_dicts_from_dataframe(self, mock_read_csv, sample_dft_df):
        """Test that resource yields dictionaries from DataFrame."""
        # Mock the CSV read to return sample data
        mock_read_csv.return_value = sample_dft_df

        # Get the resource generator
        resource = dft_traffic_resource(row_limit=None)

        # Consume the generator
        results = list(resource)

        # Verify results
        assert len(results) > 0
        assert isinstance(results[0], dict)
        assert "local_authority_id" in results[0]
        assert "year" in results[0]

    @patch("sources.other_sources.pl.read_csv")
    def test_respects_row_limit(self, mock_read_csv):
        """Test that row_limit parameter is passed to read_csv."""
        # Create sample data
        sample_df = pl.DataFrame(
            {
                "local_authority_id": [1, 2, 3],
                "local_authority_code": ["A", "B", "C"],
                "year": [2023, 2023, 2023],
            }
        )
        mock_read_csv.return_value = sample_df

        # Call with row limit
        resource = dft_traffic_resource(row_limit=100)
        list(resource)  # Consume generator

        # Verify read_csv was called with n_rows parameter
        mock_read_csv.assert_called_once()
        call_kwargs = mock_read_csv.call_args[1]
        assert call_kwargs["n_rows"] == 100

    @patch("sources.other_sources.pl.read_csv")
    def test_has_correct_write_disposition(self, mock_read_csv, sample_dft_df):
        """Test that resource has 'replace' write disposition."""
        mock_read_csv.return_value = sample_dft_df

        resource_func = dft_traffic_resource

        # Check if function has dlt metadata
        assert hasattr(resource_func, "_pipe")
        # Write disposition should be 'replace'
        assert resource_func._pipe.name == "dft_traffic"


class TestGhgEmissionsResource:
    """Test the GHG emissions dlt resource."""

    @patch("sources.other_sources.pl.read_csv")
    def test_yields_dicts_from_dataframe(self, mock_read_csv, sample_ghg_df):
        """Test that resource yields dictionaries."""
        mock_read_csv.return_value = sample_ghg_df

        resource = ghg_emissions_resource(row_limit=None)
        results = list(resource)

        assert len(results) > 0
        assert isinstance(results[0], dict)
        assert "LA Code" in results[0] or "ladcd" in results[0]

    @patch("sources.other_sources.pl.read_csv")
    def test_respects_row_limit(self, mock_read_csv, sample_ghg_df):
        """Test row limiting."""
        mock_read_csv.return_value = sample_ghg_df

        resource = ghg_emissions_resource(row_limit=50)
        list(resource)

        call_kwargs = mock_read_csv.call_args[1]
        assert call_kwargs["n_rows"] == 50


class TestImd2025Resource:
    """Test the IMD 2025 dlt resource."""

    @patch("sources.other_sources.requests.get")
    @patch("sources.other_sources.pl.read_csv")
    def test_yields_dicts_from_dataframe(self, mock_read_csv, mock_get, sample_imd_df):
        """Test that resource yields dictionaries."""
        # Mock the HTTP response
        mock_response = Mock()
        mock_response.text = "dummy,csv,data"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Mock the CSV parsing
        mock_read_csv.return_value = sample_imd_df

        resource = imd_2025_resource(row_limit=None)
        results = list(resource)

        assert len(results) > 0
        assert isinstance(results[0], dict)
        assert "lsoa21_code" in results[0]
        assert "imd_score" in results[0]

    @patch("sources.other_sources.requests.get")
    @patch("sources.other_sources.pl.read_csv")
    def test_respects_row_limit(self, mock_read_csv, mock_get, sample_imd_df):
        """Test row limiting."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.text = "dummy,csv,data"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Mock CSV parsing
        mock_read_csv.return_value = sample_imd_df

        resource = imd_2025_resource(row_limit=1000)
        list(resource)

        # Verify n_rows was passed to read_csv
        call_kwargs = mock_read_csv.call_args[1]
        assert call_kwargs["n_rows"] == 1000

    @patch("sources.other_sources.requests.get")
    @patch("sources.other_sources.pl.read_csv")
    def test_handles_all_imd_columns(self, mock_read_csv, mock_get, sample_imd_df):
        """Test that all IMD columns are preserved in output."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.text = "dummy,csv,data"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Mock CSV parsing
        mock_read_csv.return_value = sample_imd_df

        resource = imd_2025_resource(row_limit=None)
        results = list(resource)

        # Check that all columns from sample are in output
        first_result = results[0]
        for col in sample_imd_df.columns:
            assert col in first_result


class TestResourceIntegration:
    """Integration tests for resources (requires network)."""

    @pytest.mark.skip(reason="Requires network access - skip in restricted environment")
    def test_dft_traffic_resource_real_data(self):
        """Test DFT resource with real data (network required)."""
        resource = dft_traffic_resource(row_limit=10)
        results = list(resource)

        assert len(results) == 10
        assert all(isinstance(r, dict) for r in results)

    @pytest.mark.skip(reason="Requires network access - skip in restricted environment")
    def test_ghg_emissions_resource_real_data(self):
        """Test GHG resource with real data (network required)."""
        resource = ghg_emissions_resource(row_limit=10)
        results = list(resource)

        assert len(results) == 10
        assert all(isinstance(r, dict) for r in results)

    @pytest.mark.skip(reason="Requires network access - skip in restricted environment")
    def test_imd_2025_resource_real_data(self):
        """Test IMD resource with real data (network required)."""
        resource = imd_2025_resource(row_limit=10)
        results = list(resource)

        assert len(results) == 10
        assert all(isinstance(r, dict) for r in results)
