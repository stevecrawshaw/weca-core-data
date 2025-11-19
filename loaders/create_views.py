"""
Create analytical views in DuckDB.

Replaces view definitions from build_tables_queries.py

This module provides functions to create various analytical views for:
- EPC (Energy Performance Certificates) analysis
- GHG emissions reporting
- Geographic lookups
- ODS (Open Data Service) formatted views
"""

import logging

import duckdb

logger = logging.getLogger(__name__)


def create_simple_geog_lookup_view(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create simple geographic lookup view.

    Replaces: create_simple_geog_lookup_vw from build_tables_queries.py

    This view provides a flattened geographic lookup for West of England,
    joining postcodes with LA codes.

    Args:
        con: DuckDB connection
    """
    query = """
    CREATE OR REPLACE VIEW simple_geog_lookup_vw AS
    (WITH woe_la_cte AS
        (SELECT ladcd, ladnm FROM transformed_data.ca_la_lookup
        WHERE cauthnm = 'West of England')
    SELECT COLUMNS('^ls|^ms|^lad|^pcd|^wd|^lad|^imd|lat|long')
    FROM postcodes_tbl pc
    INNER JOIN woe_la_cte ON pc.lad25cd = woe_la_cte.ladcd);
    """

    try:
        con.execute(query)
        logger.info("Created view: simple_geog_lookup_vw")
    except Exception as e:
        logger.error(f"Failed to create simple_geog_lookup_vw: {e}")
        raise


def create_ghg_emissions_view(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create GHG emissions sub-sector view for Combined Authorities.

    Replaces: create_ca_la_ghg_emissions_sub_sector_ods_vw from build_tables_queries.py

    Args:
        con: DuckDB connection
    """
    query = """
    CREATE OR REPLACE VIEW ca_la_ghg_emissions_sub_sector_ods_vw AS
    (
        WITH joined_data AS (
            SELECT *
            FROM transformed_data.ghg_emissions ghg
            INNER JOIN transformed_data.ca_la_lookup ca ON ghg.local_authority_code = ca.ladcd
        )
        SELECT * EXCLUDE (country, country_code, region, ladcd, ladnm, second_tier_authority)
        FROM joined_data
    )
    """

    try:
        con.execute(query)
        logger.info("Created view: ca_la_ghg_emissions_sub_sector_ods_vw")
    except Exception as e:
        logger.error(f"Failed to create ca_la_ghg_emissions_sub_sector_ods_vw: {e}")
        raise


def create_epc_domestic_view(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create comprehensive EPC domestic certificates view.

    Replaces: create_epc_domestic_vw from build_tables_queries.py

    This view:
    - Extracts nominal construction year from age band
    - Cleans tenure values
    - Joins with postcodes, tenure, and IMD data
    - Keeps only latest certificate per UPRN

    Args:
        con: DuckDB connection
    """
    query = r"""
    CREATE OR REPLACE VIEW epc_domestic_vw AS (
    SELECT
        c.*,
        -- Clean the construction age band to produce a nominal construction year
        CASE
            -- Rule 2 (Revised): If range return the rounded mid point as integer
            WHEN regexp_matches(CONSTRUCTION_AGE_BAND, '(\d{4})-(\d{4})')
            THEN CAST(round((
                    CAST(regexp_extract(CONSTRUCTION_AGE_BAND, '(\d{4})-(\d{4})', 1) AS INTEGER)
                    +
                    CAST(regexp_extract(CONSTRUCTION_AGE_BAND, '(\d{4})-(\d{4})', 2) AS INTEGER)
                 ) / 2.0) AS INTEGER)

            -- Handle 'before YYYY'
            WHEN regexp_matches(CONSTRUCTION_AGE_BAND, 'before (\d{4})')
            THEN CAST(regexp_extract(CONSTRUCTION_AGE_BAND, 'before (\d{4})', 1) AS INTEGER)

            -- Handle 'YYYY onwards'
            WHEN regexp_matches(CONSTRUCTION_AGE_BAND, '(\d{4}) onwards')
            THEN CAST(regexp_extract(CONSTRUCTION_AGE_BAND, '(\d{4}) onwards', 1) AS INTEGER)

            -- If a single 4-digit year is present anywhere, return that year
            WHEN regexp_matches(CONSTRUCTION_AGE_BAND, '(\d{4})')
            THEN CAST(regexp_extract(CONSTRUCTION_AGE_BAND, '(\d{4})', 1) AS INTEGER)

            -- If no numerical year value can be extracted, return NULL
            ELSE NULL
        END AS NOMINAL_CONSTRUCTION_YEAR,

        -- Clean the construction age band to produce a construction epoch
        CASE WHEN NOMINAL_CONSTRUCTION_YEAR < 1900
        THEN 'Before 1900'
             WHEN (NOMINAL_CONSTRUCTION_YEAR >= 1900)
             AND (NOMINAL_CONSTRUCTION_YEAR <= 1930)
             THEN '1900 - 1930'
             WHEN NOMINAL_CONSTRUCTION_YEAR > 1930
             THEN '1930 to present'
             ELSE 'Unknown' END AS CONSTRUCTION_EPOCH,

        -- Clean the tenure column
        CASE
            WHEN TENURE = 'owner-occupied' OR TENURE = 'Owner-occupied'
            THEN 'Owner occupied'
            WHEN TENURE = 'Rented (social)' OR TENURE = 'rental (social)'
            THEN 'Social rented'
            WHEN TENURE = 'rental (private)' OR TENURE = 'Rented (private)'
            THEN 'Private rented'
            ELSE NULL
            END AS TENURE_CLEAN,

        -- Extract the day, month, and year from the lodgement datetime
        date_part('day', c.LODGEMENT_DATETIME) AS LODGEMENT_DAY,
        date_part('month', c.LODGEMENT_DATETIME) AS LODGEMENT_MONTH,
        date_part('year', c.LODGEMENT_DATETIME) AS LODGEMENT_YEAR,

        -- Select the postcodes table columns
        postcodes_tbl.lsoa21cd,
        postcodes_tbl.msoa21cd,
        postcodes_tbl.lsoa11cd,
        postcodes_tbl.msoa11cd,
        postcodes_tbl.lat,
        postcodes_tbl.long,
        postcodes_tbl.north1m,
        postcodes_tbl.east1m,
        ST_AsText(postcodes_tbl.geom) as geom_text,
        postcodes_tbl.imd20ind,
        postcodes_tbl.pcds,
        tenure_tbl.*,
        imd_lsoa_tbl.*

    FROM transformed_data.epc_domestic c
    -- Join the certificates table with the latest certificates for each UPRN
    INNER JOIN (
        SELECT UPRN, MAX(LODGEMENT_DATETIME) as max_date
        FROM transformed_data.epc_domestic
        GROUP BY UPRN
    ) latest ON c.UPRN = latest.UPRN
        AND c.LODGEMENT_DATETIME = latest.max_date
    LEFT JOIN postcodes_tbl ON c.POSTCODE = postcodes_tbl.pcds
    LEFT JOIN tenure_tbl ON postcodes_tbl.lsoa21cd = tenure_tbl.lsoa21cd
    LEFT JOIN imd_lsoa_tbl ON postcodes_tbl.lsoa11cd = imd_lsoa_tbl.lsoa11cd);
    """

    try:
        con.execute(query)
        logger.info("Created view: epc_domestic_vw")
    except Exception as e:
        logger.error(f"Failed to create epc_domestic_vw: {e}")
        logger.warning("Some referenced tables may not exist (postcodes_tbl, tenure_tbl, imd_lsoa_tbl)")


def create_epc_domestic_ods_view(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create EPC domestic ODS (Open Data Service) formatted view.

    Replaces: create_epc_domestic_ods_vw from build_tables_queries.py

    This view provides a cleaned, ODS-compatible format for domestic EPC data
    for the West of England region.

    Args:
        con: DuckDB connection
    """
    query = """
    CREATE OR REPLACE VIEW epc_domestic_ods_vw AS (
    SELECT
        LMK_KEY lmk_key,
        UPRN uprn,
        LOCAL_AUTHORITY local_authority,
        PROPERTY_TYPE property_type,
        TRANSACTION_TYPE transaction_type,
        TENURE_CLEAN tenure,
        WALLS_DESCRIPTION walls_description,
        ROOF_DESCRIPTION roof_description,
        WALLS_ENERGY_EFF walls_energy_eff,
        ROOF_ENERGY_EFF roof_energy_eff,
        MAINHEAT_DESCRIPTION mainheat_description,
        MAINHEAT_ENERGY_EFF mainheat_energy_eff,
        MAIN_FUEL main_fuel,
        SOLAR_WATER_HEATING_FLAG solar_water_heating_flag,
        CONSTRUCTION_AGE_BAND construction_age_band,
        CURRENT_ENERGY_RATING current_energy_rating,
        POTENTIAL_ENERGY_RATING potential_energy_rating,
        CO2_EMISSIONS_CURRENT co2_emissions_current,
        CO2_EMISSIONS_POTENTIAL co2_emissions_potential,
        CO2_EMISS_CURR_PER_FLOOR_AREA co2_emiss_curr_per_floor_area,
        NUMBER_HABITABLE_ROOMS number_habitable_rooms,
        NUMBER_HEATED_ROOMS number_heated_rooms,
        PHOTO_SUPPLY photo_supply,
        TOTAL_FLOOR_AREA total_floor_area,
        BUILDING_REFERENCE_NUMBER building_reference_number,
        BUILT_FORM built_form,
        lsoa21cd,
        msoa21cd,
        lat,
        long,
        index_of_multiple_deprivation_imd_rank imd20ind,
        total_all_households total,
        owned,
        social_rented,
        private_rented,
        LODGEMENT_DATE "date",
        LODGEMENT_YEAR "year",
        LODGEMENT_MONTH "month",
        index_of_multiple_deprivation_imd_decile n_imd_decile,
        NOMINAL_CONSTRUCTION_YEAR n_nominal_construction_date,
        CONSTRUCTION_EPOCH construction_epoch,
        LOCAL_AUTHORITY_LABEL ladnm,
        concat('{', lat, ', ', long, '}') as geo_point_2d
    FROM epc_domestic_lep_vw);
    """

    try:
        con.execute(query)
        logger.info("Created view: epc_domestic_ods_vw")
    except Exception as e:
        logger.error(f"Failed to create epc_domestic_ods_vw: {e}")
        logger.warning("epc_domestic_lep_vw may not exist yet")


def create_epc_non_domestic_view(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create EPC non-domestic certificates view.

    Replaces: create_epc_non_domestic_vw from build_tables_queries.py

    Args:
        con: DuckDB connection
    """
    query = """
    CREATE OR REPLACE VIEW epc_non_domestic_vw AS (
    SELECT
        c.*,
        ca_la_tbl.*,
        p.lsoa21cd,
        p.lat,
        p.long,
        date_part('day', c.LODGEMENT_DATETIME) AS LODGEMENT_DAY,
        date_part('month', c.LODGEMENT_DATETIME) AS LODGEMENT_MONTH,
        date_part('year', c.LODGEMENT_DATETIME) AS LODGEMENT_YEAR
    FROM transformed_data.epc_nondomestic c
    -- Join with latest certificates for each UPRN
    INNER JOIN (
        SELECT UPRN, MAX(LODGEMENT_DATETIME) as max_date
        FROM transformed_data.epc_nondomestic
        GROUP BY UPRN
    ) latest ON c.UPRN = latest.UPRN
        AND c.LODGEMENT_DATETIME = latest.max_date
    INNER JOIN transformed_data.ca_la_lookup ca_la_tbl
        ON c.LOCAL_AUTHORITY = ca_la_tbl.ladcd
    INNER JOIN (
        SELECT pcds, lsoa21cd, lat, long
        FROM postcodes_tbl
    ) as p
        ON c.POSTCODE = p.pcds
    WHERE c.LOCAL_AUTHORITY IN (
        SELECT ladcd
        FROM transformed_data.ca_la_lookup
        WHERE cauthnm = 'West of England'
    ));
    """

    try:
        con.execute(query)
        logger.info("Created view: epc_non_domestic_vw")
    except Exception as e:
        logger.error(f"Failed to create epc_non_domestic_vw: {e}")


def create_all_views(con: duckdb.DuckDBPyConnection, skip_errors: bool = True) -> None:
    """
    Create all analytical views.

    Args:
        con: DuckDB connection
        skip_errors: If True, continue creating views even if some fail

    Note:
        Some views depend on tables that may not exist yet (postcodes_tbl, tenure_tbl, etc.).
        Set skip_errors=True to create views that can be created.
    """
    views = [
        ("Simple Geographic Lookup", create_simple_geog_lookup_view),
        ("GHG Emissions", create_ghg_emissions_view),
        ("EPC Domestic", create_epc_domestic_view),
        ("EPC Domestic ODS", create_epc_domestic_ods_view),
        ("EPC Non-Domestic", create_epc_non_domestic_view),
    ]

    created = []
    failed = []

    for view_name, create_func in views:
        try:
            create_func(con)
            created.append(view_name)
        except Exception as e:
            failed.append((view_name, str(e)))
            if not skip_errors:
                raise

    logger.info(f"Successfully created {len(created)} views")
    if failed:
        logger.warning(f"Failed to create {len(failed)} views: {[v[0] for v in failed]}")

    return {"created": created, "failed": failed}
