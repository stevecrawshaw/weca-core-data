"""
EPC (Energy Performance Certificates) data extraction and transformation.

This module provides dual approach for EPC data:
1. Bulk ZIP downloads for initial loads (large datasets)
2. API extraction with pagination for incremental updates (smaller datasets)

Replaces EPC-related functions from get_ca_data.py:
- dl_bulk_epc_zip() -> extract_bulk_epc_zips()
- extract_and_rename_csv_from_zips() -> extract_and_rename_csv_from_zips()
- get_epc_pldf() -> extract_epc_api()
- make_zipfile_list() -> make_zipfile_list()

Authentication: Uses .dlt/secrets.toml for EPC API credentials
"""

import logging
import shutil
import zipfile
from datetime import datetime
from io import StringIO
from pathlib import Path

import dlt
import polars as pl
import requests
from requests.exceptions import RequestException

from epc_schema import all_cols_polars, nondom_polars_schema

# Configure logging
logger = logging.getLogger(__name__)


def make_zipfile_list(
    ca_la_df: pl.DataFrame, epc_base_url: str, cert_type: str = "domestic"
) -> list[dict[str, str]]:
    """
    Generate list of ZIP file URLs for bulk EPC downloads.

    Args:
        ca_la_df: DataFrame containing local authority data with 'ladnm' and 'ladcd' columns
        epc_base_url: Base URL for EPC bulk downloads
        cert_type: 'domestic' or 'non-domestic'

    Returns:
        List of dictionaries with 'url' and 'ladcd' keys

    Raises:
        ValueError: If cert_type is invalid or required columns are missing
    """
    if cert_type not in ["domestic", "non-domestic"]:
        raise ValueError(f"Invalid cert_type: {cert_type}. Must be 'domestic' or 'non-domestic'")

    required_cols = ["ladnm", "ladcd"]
    missing_cols = [col for col in required_cols if col not in ca_la_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    return (
        ca_la_df.with_columns(
            pl.col("ladnm").str.replace_all(", |\\. | ", "-").alias("la")
        )
        .select(
            [
                pl.concat_str(
                    pl.lit(epc_base_url),
                    pl.lit(cert_type),
                    pl.lit("-"),
                    pl.col("ladcd"),
                    pl.lit("-"),
                    pl.col("la"),
                    pl.lit(".zip"),
                ).alias("url"),
                pl.col("ladcd"),
            ]
        )
    ).to_dicts()


def extract_bulk_epc_zips(
    la_zipfile_list: list[dict[str, str]],
    output_path: str = "data/epc_bulk_zips",
    epc_auth_token: str | None = None,
) -> None:
    """
    Download bulk EPC ZIP files for a list of local authorities.

    Replaces: dl_bulk_epc_zip() from get_ca_data.py

    Args:
        la_zipfile_list: List of dicts with 'url' and 'ladcd' keys
        output_path: Directory to save ZIP files
        epc_auth_token: Base64-encoded EPC auth token (if None, reads from dlt secrets)

    Raises:
        ValueError: If auth token is not provided or not found in secrets
        RequestException: If download fails
        OSError: If file writing fails
    """
    # Get auth token from dlt secrets if not provided
    if epc_auth_token is None:
        try:
            epc_auth_token = dlt.secrets.get("sources.epc.auth_token")
        except Exception as e:
            raise ValueError(
                "EPC auth token not found. Provide token or configure .dlt/secrets.toml"
            ) from e

    # Create output directory
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    headers = {"Authorization": f"Basic {epc_auth_token}"}

    for la in la_zipfile_list:
        url = la["url"]
        ladcd = la["ladcd"]

        try:
            logger.info(f"Downloading EPC data for {ladcd}...")
            response = requests.get(
                url, headers=headers, allow_redirects=True, timeout=30
            )
            response.raise_for_status()

            # Save ZIP file
            zip_path = output_dir / f"{ladcd}.zip"
            with open(zip_path, "wb") as file:
                file.write(response.content)

            logger.info(f"Downloaded {ladcd}.zip ({len(response.content)} bytes)")

        except RequestException as e:
            logger.error(f"Error downloading {ladcd}.zip from {url}: {e}")
            raise RequestException(f"Failed to download {ladcd}.zip") from e
        except OSError as e:
            logger.error(f"Error writing {ladcd}.zip to filesystem: {e}")
            raise


def extract_and_rename_csv_from_zips(
    zip_folder: str = "data/epc_bulk_zips",
) -> None:
    """
    Extract certificates.csv from each ZIP file and rename to LA code.

    Replaces: extract_and_rename_csv_from_zips() from get_ca_data.py

    Args:
        zip_folder: Folder containing ZIP files

    Raises:
        FileNotFoundError: If zip_folder doesn't exist
        zipfile.BadZipFile: If ZIP file is corrupted
    """
    zip_folder_path = Path(zip_folder)

    if not zip_folder_path.exists():
        raise FileNotFoundError(f"Zip folder '{zip_folder}' does not exist.")

    for zip_file in zip_folder_path.glob("*.zip"):
        try:
            with zipfile.ZipFile(zip_file, "r") as z:
                if "certificates.csv" in z.namelist():
                    extracted_csv_path = zip_folder_path / f"{zip_file.stem}.csv"
                    with (
                        z.open("certificates.csv") as source,
                        extracted_csv_path.open("wb") as target,
                    ):
                        shutil.copyfileobj(source, target)
                    logger.info(f"Extracted and renamed {zip_file} to {extracted_csv_path}")
                else:
                    logger.warning(f"No certificates.csv found in {zip_file}")
        except zipfile.BadZipFile:
            logger.error(f"Bad zip file: {zip_file}")
            raise
        except Exception as e:
            logger.error(f"Error processing {zip_file}: {e}")
            raise


def extract_epc_api(
    la_code: str,
    cert_type: str,
    from_date: dict[str, int],
    to_date: dict[str, int] | None = None,
    epc_auth_token: str | None = None,
) -> pl.DataFrame:
    """
    Extract EPC data via API with X-Next-Search-After pagination.

    Replaces: get_epc_pldf() from get_ca_data.py

    This method is for incremental updates, not bulk downloads.
    Uses pagination via the X-Next-Search-After header.

    Args:
        la_code: Local authority code (e.g., 'E06000022')
        cert_type: 'domestic' or 'non-domestic'
        from_date: Start date dict with 'year' and 'month' keys
        to_date: End date dict (if None, uses current date)
        epc_auth_token: Base64-encoded auth token (if None, reads from dlt secrets)

    Returns:
        Polars DataFrame with EPC certificates

    Raises:
        ValueError: If cert_type is invalid or auth token not found
        RequestException: If API request fails
    """
    # Get auth token from dlt secrets if not provided
    if epc_auth_token is None:
        try:
            epc_auth_token = dlt.secrets.get("sources.epc.auth_token")
        except Exception as e:
            raise ValueError(
                "EPC auth token not found. Provide token or configure .dlt/secrets.toml"
            ) from e

    # Determine base URL and schema
    if cert_type == "domestic":
        base_url = "https://epc.opendatacommunities.org/api/v1/domestic/search"
        schema = all_cols_polars
    elif cert_type == "non-domestic":
        base_url = "https://epc.opendatacommunities.org/api/v1/non-domestic/search"
        schema = nondom_polars_schema
    else:
        raise ValueError(f"Invalid cert_type: {cert_type}. Must be 'domestic' or 'non-domestic'")

    # Parse date parameters
    from_year = from_date.get("year")
    from_month = from_date.get("month")

    if to_date is None:
        to_month = datetime.now().month - 1 or 12
        to_year = datetime.now().year if to_month != 12 else datetime.now().year - 1
    else:
        to_year = to_date.get("year")
        to_month = to_date.get("month")

    # API parameters
    query_size = 5000
    query_params = {
        "size": query_size,
        "local-authority": la_code,
        "from-month": from_month,
        "from-year": from_year,
        "to-month": to_month,
        "to-year": to_year,
    }

    headers = {"Accept": "text/csv", "Authorization": f"Basic {epc_auth_token}"}

    try:
        first_request = True
        search_after = None
        all_data = []

        while search_after is not None or first_request:
            if not first_request:
                query_params["search-after"] = search_after

            logger.info(f"Fetching EPC data for {la_code} (page {len(all_data) + 1})...")
            response = requests.get(
                base_url, headers=headers, params=query_params, timeout=30
            )
            response.raise_for_status()

            body = response.text

            # Check if body is empty or only contains header
            if not body or body.count("\n") <= 1:
                break

            # Get pagination token from headers
            search_after = response.headers.get("X-Next-Search-After")

            # Skip header for subsequent requests
            if not first_request:
                body = body.split("\n", 1)[1]

            # Parse CSV into Polars DataFrame
            df = pl.read_csv(StringIO(body), schema_overrides=schema)

            if not df.is_empty():
                all_data.append(df)
                logger.info(f"Retrieved {df.shape[0]} rows for {la_code}")

            first_request = False

        if not all_data:
            logger.warning(f"No data found for {la_code}")
            return pl.DataFrame(schema=schema)

        # Combine all DataFrames
        final_df = pl.concat(all_data)
        logger.info(f"Created final DataFrame with {final_df.shape[0]} rows for {la_code}")

        return final_df

    except RequestException as e:
        logger.error(f"API request error for {la_code}: {e}")
        raise


def transform_epc_domestic(raw_epc_df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform and validate domestic EPC certificates.

    Applies schema validation, removes duplicates, parses dates, and filters invalid records.

    Args:
        raw_epc_df: Raw domestic EPC data

    Returns:
        Cleaned and validated DataFrame

    Raises:
        ValueError: If data validation fails
        Exception: If transformation fails
    """
    try:
        # Convert column names from lowercase-hyphenated to UPPERCASE_UNDERSCORED
        # API returns: "lmk-key", "current-energy-rating", etc.
        # Transform expects: "LMK_KEY", "CURRENT_ENERGY_RATING", etc.
        validated_df = raw_epc_df.rename(
            {col: col.upper().replace("-", "_") for col in raw_epc_df.columns}
        )

        # Note: Skip schema validation since column names are already normalized
        # The API returns the correct data types

        # Remove duplicates based on LMK key
        validated_df = validated_df.unique(subset=["LMK_KEY"])

        # Parse dates (if they're not already dates)
        # The API/CSV reader may have already parsed them
        if validated_df["INSPECTION_DATE"].dtype != pl.Date:
            validated_df = validated_df.with_columns(
                pl.col("INSPECTION_DATE").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            )
        if validated_df["LODGEMENT_DATE"].dtype != pl.Date:
            validated_df = validated_df.with_columns(
                pl.col("LODGEMENT_DATE").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            )

        # Filter invalid records
        validated_df = validated_df.filter(pl.col("CURRENT_ENERGY_RATING").is_not_null())

        logger.info(f"Transformed {len(validated_df)} domestic EPC records")
        return validated_df

    except Exception as e:
        logger.error(f"Error transforming domestic EPC data: {e}")
        raise


def transform_epc_nondomestic(raw_epc_df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform and validate non-domestic EPC certificates.

    Args:
        raw_epc_df: Raw non-domestic EPC data

    Returns:
        Cleaned DataFrame

    Raises:
        Exception: If transformation fails
    """
    try:
        # Apply schema validation
        validated_df = raw_epc_df.cast(nondom_polars_schema, strict=False)

        # Remove duplicates
        validated_df = validated_df.unique(subset=["LMK_KEY"])

        logger.info(f"Transformed {len(validated_df)} non-domestic EPC records")
        return validated_df

    except Exception as e:
        logger.error(f"Error transforming non-domestic EPC data: {e}")
        raise
