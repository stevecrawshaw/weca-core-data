"""
dlt sources for miscellaneous data sources

Replaces various get_* functions from get_ca_data.py for:
- DFT traffic data (CSV)
- GHG emissions (CSV)
- IMD data (CSV)

Note: NOMIS API is complex and may need custom handling.
"""

import dlt
import requests
import polars as pl


@dlt.resource(name="dft_traffic", write_disposition="replace")
def dft_traffic_resource(row_limit: int | None = None):
    """
    Extract DFT traffic data from CSV

    Replaces: get_flat_data() for DFT CSV from get_ca_data.py

    Args:
        row_limit: If provided, limit extraction to this many rows (for testing)

    Yields:
        Dictionary records of DFT traffic data
    """
    url = "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv"

    # Download and parse CSV using Polars
    df = pl.read_csv(url, n_rows=row_limit)

    # Yield as records for dlt
    yield from df.to_dicts()


@dlt.resource(name="ghg_emissions", write_disposition="replace")
def ghg_emissions_resource(row_limit: int | None = None):
    """
    Extract GHG emissions CSV

    Replaces: get_flat_data() for emissions CSV from get_ca_data.py

    Args:
        row_limit: If provided, limit extraction to this many rows (for testing)

    Yields:
        Dictionary records of GHG emissions data
    """
    url = "https://assets.publishing.service.gov.uk/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv"

    df = pl.read_csv(url, n_rows=row_limit)
    yield from df.to_dicts()


@dlt.resource(name="imd_2025", write_disposition="replace")
def imd_2025_resource(row_limit: int | None = None):
    """
    Extract IMD 2025 (Index of Multiple Deprivation) data for England LSOA21

    New data source: humaniverse R-universe package
    URL: https://humaniverse.r-universe.dev/IMD/data/imd2025_england_lsoa21_indicators/csv

    This replaces the old IMD 2019 data source and eliminates the need for
    complex pivoting transformations. Data is already in wide format with
    all IMD indicators as columns.

    Args:
        row_limit: If provided, limit extraction to this many rows (for testing)

    Returns:
        33,755 England LSOAs with 29 IMD indicators including:
        - Income domain
        - Employment domain
        - Health indicators
        - Crime rates
        - Housing affordability
        - Education indicators
        - Connectivity scores

    Yields:
        Dictionary records of IMD data (one per LSOA)
    """
    url = "https://humaniverse.r-universe.dev/IMD/data/imd2025_england_lsoa21_indicators/csv"

    # R-universe blocks basic user agents, so use browser-like header
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/csv",
    }

    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()

    # Parse CSV with Polars
    from io import StringIO

    df = pl.read_csv(StringIO(response.text), n_rows=row_limit)

    # Yield as records for dlt
    yield from df.to_dicts()
