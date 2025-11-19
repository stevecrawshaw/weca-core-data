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
def dft_traffic_resource():
    """
    Extract DFT traffic data from CSV

    Replaces: get_flat_data() for DFT CSV from get_ca_data.py

    Yields:
        Dictionary records of DFT traffic data
    """
    url = "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv"

    # Download and parse CSV using Polars
    df = pl.read_csv(url)

    # Yield as records for dlt
    yield from df.to_dicts()


@dlt.resource(name="ghg_emissions", write_disposition="replace")
def ghg_emissions_resource():
    """
    Extract GHG emissions CSV

    Replaces: get_flat_data() for emissions CSV from get_ca_data.py

    Yields:
        Dictionary records of GHG emissions data
    """
    url = "https://assets.publishing.service.gov.uk/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv"

    df = pl.read_csv(url)
    yield from df.to_dicts()


@dlt.resource(name="imd_data", write_disposition="replace")
def imd_resource():
    """
    Extract IMD (Index of Multiple Deprivation) data

    Replaces: read_process_imd() from get_ca_data.py

    Note: This data requires custom transformation in Phase 2.
    Here we just extract the raw data.

    Yields:
        Dictionary records of IMD data
    """
    url = "https://opendatacommunities.org/downloads/cube-table?uri=http%3A%2F%2Fopendatacommunities.org%2Fdata%2Fsocietal-wellbeing%2Fimd2019%2Findices"

    # Simple extraction - transformation happens in Phase 2
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    # Parse CSV
    from io import StringIO

    df = pl.read_csv(StringIO(response.text))
    yield from df.to_dicts()
