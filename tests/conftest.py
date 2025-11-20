"""
Pytest configuration and fixtures for WECA Core Data tests.

Provides reusable fixtures for:
- Sample Polars DataFrames
- In-memory DuckDB connections
- Mock API responses
"""

import duckdb
import polars as pl
import pytest


@pytest.fixture
def sample_dft_df() -> pl.DataFrame:
    """
    Sample DFT (Department for Transport) traffic data.

    Mimics structure from test_network_connectivity.py results.
    """
    return pl.DataFrame(
        {
            "local_authority_id": [801, 802, 803, 801, 802, 803],
            "local_authority_code": ["E06000023", "E06000024", "E06000025", "E06000023", "E06000024", "E06000025"],
            "local_authority_name": ["Bristol", "North Somerset", "South Glos", "Bristol", "North Somerset", "South Glos"],
            "year": [2022, 2022, 2022, 2023, 2023, 2023],
            "count_point_id": [101, 102, 103, 101, 102, 103],
            "direction_of_travel": ["N", "S", "E", "N", "S", "E"],
            "all_motor_vehicles": [10000, 12000, 15000, 10500, 12500, 15500],
        }
    )


@pytest.fixture
def sample_ghg_df() -> pl.DataFrame:
    """
    Sample GHG (Greenhouse Gas) emissions data.
    """
    return pl.DataFrame(
        {
            "LA Code": ["E06000023", "E06000024", "E06000025", "E06000026"],
            "Local Authority": ["Bristol", "North Somerset", "South Glos", "Bath & NE Somerset"],
            "Year": [2021, 2021, 2021, 2021],
            "CO2_kt": [1500.5, 800.3, 900.7, 750.2],
            "Sector": ["Total", "Total", "Total", "Total"],
        }
    )


@pytest.fixture
def sample_imd_df() -> pl.DataFrame:
    """
    Sample IMD 2025 data.

    Based on humaniverse R-universe package structure.
    """
    return pl.DataFrame(
        {
            "lsoa21_code": ["E01014533", "E01014534", "E01014535", "E01014536"],
            "lsoa21_name": ["Bristol 001A", "Bristol 001B", "Bristol 002A", "Bristol 002B"],
            "imd_score": [25.5, 18.3, 32.1, 12.8],
            "imd_rank": [15000, 20000, 10000, 25000],
            "imd_decile": [5, 6, 4, 8],
            "income_score": [0.15, 0.12, 0.20, 0.08],
            "employment_score": [0.10, 0.08, 0.15, 0.06],
            "health_score": [0.50, 0.40, 0.60, 0.30],
        }
    )


@pytest.fixture
def sample_ca_la_df() -> pl.DataFrame:
    """
    Sample Combined Authority / Local Authority lookup data.

    Mimics ArcGIS REST API response structure.
    """
    return pl.DataFrame(
        {
            "ObjectId": [1, 2, 3],
            "LADCD21": ["E06000023", "E06000025", "E06000022"],
            "LADNM21": ["Bristol, City of", "South Gloucestershire", "Bath and North East Somerset"],
            "CAUTHCD21": ["E47000009", "E47000009", "E47000009"],
            "CAUTHNM21": ["West of England", "West of England", "West of England"],
        }
    )


@pytest.fixture
def sample_lsoa_pwc_df() -> pl.DataFrame:
    """
    Sample LSOA population-weighted centroids data.
    """
    return pl.DataFrame(
        {
            "LSOA21CD": ["E01014533", "E01014534", "E01014535"],
            "LSOA21NM": ["Bristol 001A", "Bristol 001B", "Bristol 002A"],
            "X": [-2.5879, -2.5895, -2.5920],
            "Y": [51.4545, 51.4560, 51.4580],
            "GEOMETRY": [
                "POINT (-2.5879 51.4545)",
                "POINT (-2.5895 51.4560)",
                "POINT (-2.5920 51.4580)",
            ],
        }
    )


@pytest.fixture
def in_memory_duckdb() -> duckdb.DuckDBPyConnection:
    """
    In-memory DuckDB connection for testing.

    Automatically closes connection after test completes.
    """
    con = duckdb.connect(":memory:")
    yield con
    con.close()


@pytest.fixture
def la_codes() -> list[str]:
    """
    Sample WECA Local Authority codes.
    """
    return ["E06000023", "E06000024", "E06000025", "E06000022"]


@pytest.fixture
def lsoa_codes() -> list[str]:
    """
    Sample LSOA codes for testing.
    """
    return ["E01014533", "E01014534", "E01014535", "E01014536"]


@pytest.fixture
def spatial_available(in_memory_duckdb) -> bool:
    """
    Check if DuckDB spatial extension is available.

    Returns True if spatial extension can be loaded, False otherwise.
    This is used to skip spatial tests in network-restricted environments.
    """
    try:
        in_memory_duckdb.execute("INSTALL spatial;")
        in_memory_duckdb.execute("LOAD spatial;")
        in_memory_duckdb.execute("SELECT ST_Point(0, 0);")
        return True
    except Exception:
        return False
