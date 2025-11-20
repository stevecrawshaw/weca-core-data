"""
Orchestrate full ETL pipeline: Extract (dlt) → Transform (custom) → Load (DuckDB)

Replaces: cesap-epc-load-duckdb-data.py

This pipeline:
1. EXTRACT: Uses dlt sources to pull data from APIs
2. TRANSFORM: Applies custom Polars transformations
3. LOAD: Writes to DuckDB with spatial extensions and geometry columns
"""

import logging
from pathlib import Path

import dlt
import duckdb
import polars as pl

# dlt extractors (Phase 1)
from sources.arcgis_sources import arcgis_geographies_source, ca_boundaries_source
from sources.other_sources import (
    dft_traffic_resource,
    ghg_emissions_resource,
    imd_2025_resource,
)

# Custom transformers (Phase 2)
from transformers.geography import (
    get_ca_la_codes,
    transform_ca_la_lookup,
    transform_lsoa_pwc,
)
from transformers.emissions import (
    transform_dft_lookup,
    transform_ghg_emissions,
    transform_imd_2025,
)

# EPC transformers (custom extraction + transformation)
from transformers.epc import (
    extract_epc_api,
    transform_epc_domestic,
    transform_epc_nondomestic,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl.log"),
    ],
)
logger = logging.getLogger(__name__)


def run_full_etl(
    db_path: str = "data/ca_epc.duckdb",
    download_epc: bool = False,
    epc_from_date: dict[str, int] | None = None,
    sample_mode: bool = False,
    sample_size: int = 1000,
    skip_arcgis: bool = False,
) -> None:
    """
    Run complete ETL pipeline.

    Args:
        db_path: Path to DuckDB database
        download_epc: If True, extract EPC data via API (incremental)
        epc_from_date: EPC start date (dict with 'year' and 'month' keys)
        sample_mode: If True, use limited data for testing (default: False)
        sample_size: Number of records to extract in sample mode (default: 1000)
        skip_arcgis: If True, skip ArcGIS extraction (very slow, 10-30 min)

    Pipeline stages:
    1. Extract: dlt pulls data from APIs
    2. Transform: Custom Polars transformations
    3. Load: DuckDB storage with spatial extensions
    """

    print("=" * 80)
    print("WECA CORE DATA ETL - HYBRID APPROACH")
    if sample_mode:
        print(f"** SAMPLE MODE: Limited to {sample_size:,} records per source **")
    if skip_arcgis:
        print("** SKIPPING ARCGIS: Geographic sources will not be extracted **")
    print("=" * 80)

    # Ensure data directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # ==========================================================================
    # STAGE 1: EXTRACT (dlt)
    # ==========================================================================
    print("\n" + "=" * 80)
    print("STAGE 1: EXTRACT")
    print("=" * 80)

    pipeline = dlt.pipeline(
        pipeline_name="weca_etl",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw_data",
    )

    # Extract ArcGIS geographies
    if not skip_arcgis:
        print("\n[1/5] Extracting ArcGIS geographical data...")
        logger.info("Starting ArcGIS extraction (LSOA boundaries, lookups, PWC)...")
        logger.info("WARNING: This may take 10-30 minutes due to pagination of ~42K records...")
        logger.info("Use --skip-arcgis flag to bypass this step for faster testing")
        try:
            load_info = pipeline.run(arcgis_geographies_source())
            if load_info.has_failed_jobs:
                logger.error("ArcGIS extraction failed!")
                raise Exception("Failed to extract ArcGIS data")
            logger.info(f"ArcGIS extraction completed: {load_info}")
            print("[OK] ArcGIS data extracted")
        except Exception as e:
            logger.error(f"ArcGIS extraction failed: {e}")
            if not sample_mode:
                raise
            logger.warning("Continuing in sample mode despite ArcGIS failure...")
    else:
        print("\n[1/5] SKIPPED: ArcGIS geographical data")
        logger.info("ArcGIS extraction skipped per --skip-arcgis flag")

    # Extract CA boundaries
    if not skip_arcgis:
        print("\n[2/5] Extracting Combined Authority boundaries...")
        logger.info("Starting CA boundaries extraction...")
        load_info = pipeline.run(ca_boundaries_source())
        if load_info.has_failed_jobs:
            logger.error("CA boundaries extraction failed!")
            raise Exception("Failed to extract CA boundaries")
        logger.info(f"CA boundaries extraction completed: {load_info}")
        print("[OK] CA boundaries extracted")
    else:
        print("\n[2/5] SKIPPED: Combined Authority boundaries")
        logger.info("CA boundaries extraction skipped per --skip-arcgis flag")

    # Determine row limit for sample mode
    row_limit = sample_size if sample_mode else None
    if sample_mode:
        logger.info(f"Sample mode enabled: limiting to {sample_size:,} rows per source")

    # Extract DFT traffic data
    print("\n[3/5] Extracting DFT traffic data...")
    logger.info(f"Starting DFT extraction (row_limit={row_limit})...")
    load_info = pipeline.run(dft_traffic_resource(row_limit=row_limit))
    if load_info.has_failed_jobs:
        logger.error("DFT extraction failed!")
        raise Exception("Failed to extract DFT data")
    logger.info(f"DFT extraction completed: {load_info}")
    print("[OK] DFT traffic data extracted")

    # Extract GHG emissions data
    print("\n[4/5] Extracting GHG emissions data...")
    logger.info(f"Starting GHG emissions extraction (row_limit={row_limit})...")
    load_info = pipeline.run(ghg_emissions_resource(row_limit=row_limit))
    if load_info.has_failed_jobs:
        logger.error("GHG extraction failed!")
        raise Exception("Failed to extract GHG emissions data")
    logger.info(f"GHG emissions extraction completed: {load_info}")
    print("[OK] GHG emissions data extracted")

    # Extract IMD 2025 data
    print("\n[5/5] Extracting IMD 2025 data...")
    logger.info(f"Starting IMD 2025 extraction (row_limit={row_limit})...")
    try:
        load_info = pipeline.run(imd_2025_resource(row_limit=row_limit))
        if load_info.has_failed_jobs:
            logger.error("IMD extraction failed!")
            raise Exception("Failed to extract IMD 2025 data")
        logger.info(f"IMD 2025 extraction completed: {load_info}")
        print("[OK] IMD 2025 data extracted")
    except Exception as e:
        logger.error(f"IMD 2025 extraction failed: {e}")
        logger.warning("IMD data may be blocked by robots.txt or rate limiting")
        if not sample_mode:
            raise
        logger.warning("Continuing in sample mode despite IMD failure...")

    # ==========================================================================
    # STAGE 2: TRANSFORM
    # ==========================================================================
    print("\n" + "=" * 80)
    print("STAGE 2: TRANSFORM")
    print("=" * 80)

    con = duckdb.connect(db_path)

    try:
        # Create transformed_data schema
        con.execute("CREATE SCHEMA IF NOT EXISTS transformed_data")

        # Initialize la_codes for filtering (will be None if ArcGIS skipped)
        la_codes = None

        # ----------------------------------------------------------------------
        # 2.1: Transform CA/LA Lookup (requires ArcGIS data)
        # ----------------------------------------------------------------------
        if not skip_arcgis:
            print("\n[1/6] Transforming CA/LA lookup data...")

            # Get raw data from dlt extraction
            try:
                raw_ca_la = con.sql(
                    "SELECT * FROM raw_data.lsoa_2021_lookups"
                ).pl()
            except Exception as e:
                logger.warning(f"Could not find CA/LA lookup table: {e}")
                logger.info("Attempting to use alternative source...")
                # Alternative: use LSOA boundaries which also contain LA info
                raw_ca_la = con.sql(
                    "SELECT * FROM raw_data.lsoa_2021_boundaries LIMIT 1000"
                ).pl()

            transformed_ca_la = transform_ca_la_lookup(raw_ca_la, inc_ns=True)

            con.execute("DROP TABLE IF EXISTS transformed_data.ca_la_lookup")
            con.execute(
                "CREATE TABLE transformed_data.ca_la_lookup AS SELECT * FROM transformed_ca_la"
            )
            print(f"[OK] CA/LA lookup: {len(transformed_ca_la)} records")

            # Get LA codes for filtering other datasets
            la_codes = get_ca_la_codes(transformed_ca_la)
            logger.info(f"Working with {len(la_codes)} Local Authorities")
        else:
            print("\n[1/6] SKIPPED: CA/LA lookup (requires ArcGIS data)")
            logger.info("CA/LA lookup skipped - ArcGIS data not available")

        # ----------------------------------------------------------------------
        # 2.2: Transform LSOA PWC (requires ArcGIS data)
        # ----------------------------------------------------------------------
        if not skip_arcgis:
            print("\n[2/6] Transforming LSOA population-weighted centroids...")

            raw_lsoa_pwc = con.sql(
                "SELECT * FROM raw_data.lsoa_2021_pwc"
            ).pl()

            transformed_lsoa_pwc = transform_lsoa_pwc(raw_lsoa_pwc)

            con.execute("DROP TABLE IF EXISTS transformed_data.lsoa_2021_pwc")
            con.execute(
                "CREATE TABLE transformed_data.lsoa_2021_pwc AS SELECT * FROM transformed_lsoa_pwc"
            )
            print(f"[OK] LSOA PWC: {len(transformed_lsoa_pwc)} records")
        else:
            print("\n[2/6] SKIPPED: LSOA PWC (requires ArcGIS data)")
            logger.info("LSOA PWC skipped - ArcGIS data not available")

        # ----------------------------------------------------------------------
        # 2.3: Transform GHG Emissions
        # ----------------------------------------------------------------------
        print("\n[3/6] Transforming GHG emissions data...")

        try:
            raw_ghg = con.sql("SELECT * FROM raw_data.ghg_emissions").pl()
            transformed_ghg = transform_ghg_emissions(raw_ghg, la_codes=la_codes)
            con.execute("DROP TABLE IF EXISTS transformed_data.ghg_emissions")
            con.execute(
                "CREATE TABLE transformed_data.ghg_emissions AS SELECT * FROM transformed_ghg"
            )
            print(f"[OK] GHG emissions: {len(transformed_ghg)} records")
            if la_codes is None:
                logger.info("GHG processed without LA filtering (all data)")
        except Exception as e:
            logger.error(f"GHG transformation failed: {e}")
            print("[SKIP] GHG emissions transformation failed")

        # ----------------------------------------------------------------------
        # 2.4: Transform DFT Lookup
        # ----------------------------------------------------------------------
        print("\n[4/6] Transforming DFT traffic lookup...")

        try:
            raw_dft = con.sql("SELECT * FROM raw_data.dft_traffic").pl()
            transformed_dft = transform_dft_lookup(raw_dft, la_codes=la_codes)
            con.execute("DROP TABLE IF EXISTS transformed_data.dft_la_lookup")
            con.execute(
                "CREATE TABLE transformed_data.dft_la_lookup AS SELECT * FROM transformed_dft"
            )
            print(f"[OK] DFT lookup: {len(transformed_dft)} records")
            if la_codes is None:
                logger.info("DFT processed without LA filtering (all data)")
        except Exception as e:
            logger.error(f"DFT transformation failed: {e}")
            print("[SKIP] DFT transformation failed")

        # ----------------------------------------------------------------------
        # 2.5: Transform IMD 2025
        # ----------------------------------------------------------------------
        print("\n[5/7] Transforming IMD 2025 data...")

        try:
            raw_imd = con.sql("SELECT * FROM raw_data.imd_2025").pl()

            # Get LSOA codes from LSOA PWC table for filtering (if available)
            if not skip_arcgis:
                lsoa_codes_df = con.sql("SELECT DISTINCT lsoa21cd FROM transformed_data.lsoa_2021_pwc").pl()
                lsoa_codes_list = lsoa_codes_df["lsoa21cd"].to_list()
            else:
                # No filtering - process all IMD data
                lsoa_codes_list = None
                logger.info("Processing all IMD data without LSOA filtering")

            transformed_imd = transform_imd_2025(raw_imd, lsoa_codes=lsoa_codes_list)

            con.execute("DROP TABLE IF EXISTS transformed_data.imd_2025")
            con.execute(
                "CREATE TABLE transformed_data.imd_2025 AS SELECT * FROM transformed_imd"
            )
            print(f"[OK] IMD 2025: {len(transformed_imd)} LSOAs with {len(transformed_imd.columns)} indicators")
        except Exception as e:
            logger.error(f"IMD transformation failed: {e}")
            print("[SKIP] IMD transformation failed")

        # ----------------------------------------------------------------------
        # 2.6: Extract and Transform EPC Data (if requested)
        # ----------------------------------------------------------------------
        if download_epc and la_codes is not None:
            print("\n[6/7] Extracting and transforming EPC data...")

            if epc_from_date is None:
                # Default to last 3 months
                from datetime import datetime
                epc_from_date = {
                    "year": datetime.now().year,
                    "month": max(1, datetime.now().month - 3),
                }

            all_epc_domestic = []

            for i, la_code in enumerate(la_codes, 1):
                print(f"  [{i}/{len(la_codes)}] Extracting EPC for {la_code}...")

                try:
                    raw_epc = extract_epc_api(
                        la_code=la_code,
                        cert_type="domestic",
                        from_date=epc_from_date,
                    )

                    if not raw_epc.is_empty():
                        transformed_epc = transform_epc_domestic(raw_epc)
                        all_epc_domestic.append(transformed_epc)
                        print(f"  [OK] {len(transformed_epc)} records")
                    else:
                        print(f"  [SKIP] No data for {la_code}")

                except Exception as e:
                    logger.error(f"Error extracting EPC for {la_code}: {e}")
                    # Continue with other LAs

            if all_epc_domestic:
                combined_epc = pl.concat(all_epc_domestic)

                con.execute("DROP TABLE IF EXISTS transformed_data.epc_domestic")
                con.execute(
                    "CREATE TABLE transformed_data.epc_domestic AS SELECT * FROM combined_epc"
                )
                print(f"[OK] EPC domestic: {len(combined_epc)} total records")
            else:
                print("[WARN] No EPC data extracted")
        elif download_epc and la_codes is None:
            print("\n[6/7] Skipping EPC extraction (requires LA codes from ArcGIS data)")
            logger.warning("EPC extraction skipped - ArcGIS data not available")
        else:
            print("\n[6/7] Skipping EPC extraction (download_epc=False)")

        print("\n[7/7] All transformations complete!")

        # ==========================================================================
        # STAGE 3: LOAD (Spatial Setup)
        # ==========================================================================
        if not skip_arcgis:
            print("\n" + "=" * 80)
            print("STAGE 3: LOAD (Spatial Setup)")
            print("=" * 80)

            print("\n[1/2] Installing spatial extension...")
            con.execute("INSTALL spatial;")
            con.execute("LOAD spatial;")
            print("[OK] Spatial extension loaded")

            print("\n[2/2] Adding geometry columns...")
            # Add geometry column to LSOA PWC
            try:
                con.execute(
                    "ALTER TABLE transformed_data.lsoa_2021_pwc ADD COLUMN IF NOT EXISTS geom GEOMETRY;"
                )
                con.execute(
                    "UPDATE transformed_data.lsoa_2021_pwc SET geom = ST_Point(x, y);"
                )
                print("[OK] Geometry column added to LSOA PWC")
            except Exception as e:
                logger.warning(f"Could not add geometry column: {e}")
        else:
            print("\n" + "=" * 80)
            print("STAGE 3: LOAD (Spatial Setup)")
            print("=" * 80)
            print("\nSKIPPED: No spatial data available (ArcGIS was skipped)")
            logger.info("Spatial setup skipped - no geographic data to process")

    finally:
        con.close()

    # ==========================================================================
    # COMPLETE
    # ==========================================================================
    print("\n" + "=" * 80)
    print("ETL PIPELINE COMPLETE")
    print("=" * 80)
    print(f"\nDatabase: {db_path}")
    print("\nTransformed tables:")
    con = duckdb.connect(db_path)
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'transformed_data' ORDER BY table_name"
    ).fetchall()
    for table in tables:
        row_count = con.execute(
            f"SELECT COUNT(*) FROM transformed_data.{table[0]}"
        ).fetchone()[0]
        print(f"  - {table[0]}: {row_count:,} rows")
    con.close()

    print("\n" + "=" * 80)


if __name__ == "__main__":
    import sys

    # Check for command-line arguments
    sample_mode = "--sample" in sys.argv or "--test" in sys.argv
    full_mode = "--full" in sys.argv
    skip_arcgis = "--skip-arcgis" in sys.argv

    # Run ETL pipeline
    # Use --sample or --test for sample mode (limited data)
    # Use --full for full production run
    # Use --skip-arcgis to skip slow ArcGIS extraction (10-30 min)
    # Set download_epc=True to extract EPC data incrementally
    run_full_etl(
        db_path="data/ca_epc.duckdb",
        download_epc=False,  # Set to True to download EPC data
        epc_from_date={"year": 2024, "month": 1},  # Adjust as needed
        sample_mode=sample_mode if not full_mode else False,
        sample_size=1000,  # Limit to 1000 records in sample mode
        skip_arcgis=skip_arcgis,  # Skip ArcGIS if flag provided
    )
