"""
Custom Polars transformations for geographical data.

Replaces geography-related functions from get_ca_data.py:
- get_ca_la_df() -> transform_ca_la_lookup()
- make_lsoa_pwc_df() -> transform_lsoa_pwc()
- clean_colname() -> clean_column_name()
- remove_numbers() -> remove_numbers()
- get_rename_dict() -> get_rename_dict()
- get_ca_la_codes() -> get_ca_la_codes()
"""

import logging
from typing import Callable

import polars as pl

# Configure logging
logger = logging.getLogger(__name__)


def remove_numbers(input_string: str) -> str:
    """
    Remove all numbers from an input string and return as lowercase.

    Args:
        input_string: String to process

    Returns:
        String with all digits removed and converted to lowercase

    Example:
        >>> remove_numbers("LSOA21CD")
        'lsoacd'
    """
    lowercase_string = input_string.lower()
    translation_table = str.maketrans("", "", "0123456789")
    result_string = lowercase_string.translate(translation_table)
    return result_string


def clean_column_name(colnm: str) -> str:
    """
    Clean column names of pivoted LSOA data.

    Used after pyjanitor.clean_names() method for LSOA data processing.
    Removes quotes, braces, commas, and extracts meaningful column names.

    Args:
        colnm: Column name to clean

    Returns:
        Cleaned column name

    Example:
        >>> clean_column_name('"{lsoa,feature}"')
        'feature'
        >>> clean_column_name("lsoa21cd")
        'lsoa21cd'
    """
    if colnm[0:4] != "lsoa":
        return (
            colnm.replace('"', "")
            .replace("{", "")
            .replace("}", "")
            .replace(",", "")[2:]
        )
    else:
        return colnm


def get_rename_dict(
    df: pl.DataFrame,
    remove_numbers_fn: Callable[[str], str] = remove_numbers,
    rm_numbers: bool = False,
) -> dict[str, str]:
    """
    Generate a dictionary for renaming DataFrame columns.

    Handles duplicate column names by appending numeric suffixes.

    Args:
        df: Polars DataFrame to generate rename dict for
        remove_numbers_fn: Function to remove numbers from strings
        rm_numbers: If True, remove numbers from column names

    Returns:
        Dictionary mapping old column names to new column names

    Example:
        >>> df = pl.DataFrame({"Col1": [1], "Col2": [2], "Col1_dup": [3]})
        >>> get_rename_dict(df)
        {'Col1': 'col1', 'Col2': 'col2', 'Col1_dup': 'col1_dup'}
    """
    old = df.columns
    counts: dict[str, int] = {}

    if not rm_numbers:
        new = [colstring.lower() for colstring in df.columns]
    else:
        new = [remove_numbers_fn(colstring).lower() for colstring in df.columns]

    for i, item in enumerate(new):
        if new.count(item) > 1:
            counts[item] = counts.get(item, 0) + 1
            new[i] = f"{item}_{counts[item]}"

    return dict(zip(old, new, strict=False))


def transform_ca_la_lookup(
    raw_ca_la_df: pl.DataFrame,
    inc_ns: bool = True,
) -> pl.DataFrame:
    """
    Transform Combined Authority - Local Authority lookup data.

    Replaces: get_ca_la_df() from get_ca_data.py

    This function:
    1. Removes ObjectId column
    2. Renames columns by removing numbers
    3. Adds North Somerset to WECA by default

    Args:
        raw_ca_la_df: Raw CA/LA lookup data from dlt extraction
        inc_ns: Include North Somerset in WECA (default True per user requirement)

    Returns:
        Transformed lookup DataFrame with cleaned column names

    Raises:
        ValueError: If required columns are missing
        Exception: If transformation fails
    """
    try:
        # Exclude ObjectId if present
        if "ObjectId" in raw_ca_la_df.columns:
            ca_la_df = raw_ca_la_df.select(pl.exclude("ObjectId"))
        else:
            ca_la_df = raw_ca_la_df

        # Rename columns by removing numbers
        old_names = ca_la_df.columns
        new_names = [remove_numbers(colstring) for colstring in old_names]
        rename_dict = dict(zip(old_names, new_names, strict=False))

        clean_ca_la_df = ca_la_df.rename(rename_dict)

        # North Somerset addition (WECA-specific)
        if inc_ns:
            ns_line_df = pl.DataFrame(
                {
                    "ladcd": "E06000024",
                    "ladnm": "North Somerset",
                    "cauthcd": "E47000009",
                    "cauthnm": "West of England",
                }
            )
            result_df = clean_ca_la_df.vstack(ns_line_df)
            logger.info("ca_la_df with North Somerset created")
        else:
            result_df = clean_ca_la_df
            logger.info("ca_la_df without North Somerset created")

        return result_df

    except Exception as e:
        logger.error(f"Error transforming CA/LA lookup data: {e}")
        raise


def transform_lsoa_pwc(
    raw_lsoa_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Transform LSOA population-weighted centroids data.

    Replaces: make_lsoa_pwc_df() from get_ca_data.py

    This function:
    1. Ensures data is unique (removes duplicates)
    2. Converts column names to lowercase
    3. Validates required geometry columns (x, y) exist

    Args:
        raw_lsoa_df: Raw LSOA PWC data from dlt extraction

    Returns:
        Cleaned LSOA DataFrame with lowercase column names

    Raises:
        ValueError: If required columns are missing
        Exception: If transformation fails
    """
    try:
        # Remove duplicates
        lsoa_df = raw_lsoa_df.unique()

        # Convert column names to lowercase
        lsoa_df = lsoa_df.rename(lambda x: x.lower())

        # Validate geometry columns exist
        if "x" not in lsoa_df.columns or "y" not in lsoa_df.columns:
            raise ValueError(
                f"Required geometry columns (x, y) missing. Found columns: {lsoa_df.columns}"
            )

        logger.info(f"Transformed LSOA PWC data: {len(lsoa_df)} records")
        return lsoa_df

    except Exception as e:
        logger.error(f"Error transforming LSOA PWC data: {e}")
        raise


def get_ca_la_codes(ca_la_df: pl.DataFrame) -> list[str]:
    """
    Extract list of Local Authority codes from CA/LA lookup.

    Args:
        ca_la_df: Combined Authority / Local Authority lookup DataFrame

    Returns:
        List of LA codes (ladcd)

    Raises:
        ValueError: If 'ladcd' column is missing
    """
    if "ladcd" not in ca_la_df.columns:
        raise ValueError(f"'ladcd' column not found. Available columns: {ca_la_df.columns}")

    return ca_la_df.select(pl.col("ladcd")).to_series().to_list()
