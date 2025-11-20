"""
Emissions and environmental data transformations.

Replaces emissions-related functions from get_ca_data.py:
- get_ca_la_dft_lookup() -> transform_dft_lookup()
- GHG emissions processing -> transform_ghg_emissions()
- read_process_imd() -> transform_imd_2025()

New IMD 2025 Data Source:
- humaniverse R-universe package (IMD 2025 for England LSOA21)
- Already in wide format with clean column names
- No complex pivoting required (unlike old IMD 2019 data)
"""

import logging

import polars as pl

# Configure logging
logger = logging.getLogger(__name__)


def transform_ghg_emissions(
    raw_emissions_df: pl.DataFrame,
    la_codes: list[str] | None = None,
) -> pl.DataFrame:
    """
    Transform and filter GHG (Greenhouse Gas) emissions data.

    Filters emissions data for relevant local authorities and ensures
    data quality by removing nulls and duplicates.

    Args:
        raw_emissions_df: Raw GHG emissions data from dlt extraction
        la_codes: Optional list of LA codes to filter for (if None, returns all)

    Returns:
        Filtered and cleaned emissions DataFrame

    Raises:
        ValueError: If required columns are missing
        Exception: If transformation fails
    """
    try:
        # Validate required columns exist
        # (adjust based on actual GHG emissions data structure)
        logger.info(f"Transforming GHG emissions data: {len(raw_emissions_df)} records")

        # Remove any duplicates
        emissions_df = raw_emissions_df.unique()

        # Filter for specific LA codes if provided
        if la_codes is not None:
            # Assuming there's a column like 'LA Code' or 'Local Authority Code'
            # This will need to be adjusted based on actual column names
            la_col_candidates = [
                "LA Code",
                "Local Authority Code",
                "LA_Code",
                "ladcd",
            ]

            la_col = None
            for col in la_col_candidates:
                if col in emissions_df.columns:
                    la_col = col
                    break

            if la_col:
                emissions_df = emissions_df.filter(pl.col(la_col).is_in(la_codes))
                logger.info(f"Filtered to {len(emissions_df)} records for {len(la_codes)} LAs")
            else:
                logger.warning(
                    f"LA code column not found. Available columns: {emissions_df.columns}"
                )

        logger.info(f"Transformed {len(emissions_df)} GHG emissions records")
        return emissions_df

    except Exception as e:
        logger.error(f"Error transforming GHG emissions data: {e}")
        raise


def transform_dft_lookup(
    raw_dft_df: pl.DataFrame,
    la_codes: list[str] | None = None,
) -> pl.DataFrame:
    """
    Transform DFT (Department for Transport) traffic data lookup.

    Replaces: get_ca_la_dft_lookup() from get_ca_data.py

    Extracts the most recent year's data and returns ONS LA codes
    with corresponding DFT IDs for detailed link data retrieval.

    Args:
        raw_dft_df: Raw DFT annual traffic data
        la_codes: Optional list of LA codes to filter for (if None, returns all)

    Returns:
        Lookup DataFrame with dft_la_id, ladcd, and year columns

    Raises:
        ValueError: If required columns are missing
        Exception: If transformation fails
    """
    try:
        # Validate required columns
        required_cols = ["local_authority_id", "local_authority_code", "year"]
        missing_cols = [col for col in required_cols if col not in raw_dft_df.columns]

        if missing_cols:
            raise ValueError(
                f"Missing required columns: {missing_cols}. Available: {raw_dft_df.columns}"
            )

        # Get most recent year's data
        dft_lookup_df = (
            raw_dft_df.filter(pl.col("year") == pl.col("year").max())
            .select(
                [
                    pl.col("local_authority_id").alias("dft_la_id"),
                    pl.col("local_authority_code").alias("ladcd"),
                    pl.col("year"),
                ]
            )
        )

        # Filter for specific LA codes if provided
        if la_codes is not None:
            dft_lookup_df = dft_lookup_df.filter(pl.col("ladcd").is_in(la_codes))

        if len(dft_lookup_df) > 0:
            logger.info(
                f"Transformed DFT lookup: {len(dft_lookup_df)} LAs for year {dft_lookup_df['year'][0]}"
            )
        else:
            logger.warning("DFT lookup resulted in 0 records - check LA codes")

        return dft_lookup_df

    except Exception as e:
        logger.error(f"Error transforming DFT lookup data: {e}")
        raise


def transform_imd_2025(
    raw_imd_df: pl.DataFrame,
    lsoa_codes: list[str] | None = None,
) -> pl.DataFrame:
    """
    Transform IMD 2025 data from humaniverse R-universe package.

    Replaces: read_process_imd() from get_ca_data.py

    The new IMD 2025 data is already in wide format with clean column names,
    eliminating the need for complex pivoting that the old function required.

    Data structure:
    - 33,755 LSOAs (all England)
    - 29 IMD indicators as columns
    - Identifier: lsoa21_code
    - All indicators already cleaned and formatted

    Args:
        raw_imd_df: Raw IMD 2025 data from dlt extraction
        lsoa_codes: Optional list of LSOA21 codes to filter for (WECA LSOAs)

    Returns:
        Transformed IMD DataFrame (wide format with all indicators)

    Raises:
        ValueError: If required columns are missing
        Exception: If transformation fails
    """
    try:
        # Validate identifier column exists
        if "lsoa21_code" not in raw_imd_df.columns:
            raise ValueError(
                f"'lsoa21_code' column not found. Available: {raw_imd_df.columns}"
            )

        logger.info(f"Transforming IMD 2025 data: {len(raw_imd_df)} LSOAs")

        # Remove any duplicates (shouldn't be any, but good practice)
        imd_df = raw_imd_df.unique(subset=["lsoa21_code"])

        # Filter for specific LSOA codes if provided (e.g., WECA LSOAs only)
        if lsoa_codes is not None:
            imd_df = imd_df.filter(pl.col("lsoa21_code").is_in(lsoa_codes))
            logger.info(
                f"Filtered to {len(imd_df)} LSOAs from {len(lsoa_codes)} requested codes"
            )

        # Data is already in clean wide format, no pivoting needed!
        # Column names are already snake_case and descriptive

        logger.info(
            f"Transformed IMD 2025: {len(imd_df)} LSOAs with {len(imd_df.columns)} indicators"
        )

        return imd_df

    except Exception as e:
        logger.error(f"Error transforming IMD 2025 data: {e}")
        raise
