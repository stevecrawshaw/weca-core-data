# build_tables_queries.py

# These queries are intended to be used within a DuckDB transaction.
# Remember to define the necessary variables (e.g., paths, schemas)
# in your main script before executing these queries.

install_spatial_query = "INSTALL spatial;"
load_spatial_query = "LOAD spatial;"
# install_httpfs_query = "INSTALL HTTPFS;"
# load_httpfs_query = "LOAD HTTPFS;"

# LSOA PWC
create_lsoa_pwc_table_query = """
    CREATE OR REPLACE TABLE lsoa_2021_pwc_tbl AS
    SELECT * FROM read_parquet('data/lsoa_2021_pwc_df.parquet')
"""
add_geom_column_lsoa_pwc_query = (
    "ALTER TABLE lsoa_2021_pwc_tbl ADD COLUMN geom GEOMETRY;"
)
update_geom_lsoa_pwc_query = (
    "UPDATE lsoa_2021_pwc_tbl SET geom = ST_Point(x, y);"
)
create_lsoacd_pwc_index_query = """
    CREATE UNIQUE INDEX lsoacd_pwc_idx ON lsoa_2021_pwc_tbl (lsoa21cd);
"""

# LOOKUPS
create_lsoa_2021_lookup_table_query = """
    CREATE OR REPLACE TABLE lsoa_2021_lookup_tbl AS
    SELECT * FROM read_parquet('data/lookups_2021_pldf.parquet');
"""
create_lsoa21cd_lookup_index_query = """
    CREATE UNIQUE INDEX lsoa21cd_lookup_idx ON lsoa_2021_lookup_tbl (lsoa21cd);
"""
create_lsoa_2011_lookup_table_query = """
    CREATE OR REPLACE TABLE lsoa_2011_lookup_tbl AS
    SELECT * FROM read_parquet('data/lookups_2011_pldf.parquet');
"""

# IMD
create_imd_lsoa_table_query = """
    CREATE OR REPLACE TABLE imd_lsoa_tbl AS
    SELECT * FROM read_parquet('data/lsoa_imd_df.parquet');
"""
create_lsoa11cd_imd_index_query = """
    CREATE UNIQUE INDEX lsoa11cd_imd_idx ON imd_lsoa_tbl (lsoa11cd);
"""

# EPC - Requires epc_schema variable (dict of column schemas)
create_epc_domestic_table_query = """
    CREATE OR REPLACE TABLE epc_domestic_tbl AS
    SELECT * FROM read_csv('data/epc_bulk_zips/*.csv', columns = ?);
"""

create_epc_nondom_table_query = """
    CREATE OR REPLACE TABLE epc_nondom_tbl AS
    SELECT * FROM read_csv('data/epc_bulk_nondom_zips/*.csv', columns = ?)
"""

# LSOA POLYS - Requires path_2021_poly_parquet and path_2011_poly_parquet
# variables (paths to parquet files)
create_lsoa_poly_2021_table_query = """
    CREATE OR REPLACE TABLE lsoa_poly_2021_tbl AS
    SELECT *, ST_GeomFromText(geometry::VARCHAR) as geom
    FROM read_parquet(?);
"""

create_lsoa_poly_2011_table_query = """
    CREATE OR REPLACE TABLE lsoa_poly_2011_tbl AS
    SELECT *, ST_GeomFromText(geometry::VARCHAR) as geom
    FROM read_parquet(?);
"""

# EMISSIONS
create_ghg_emissions_table_query = """
    CREATE OR REPLACE TABLE ghg_emissions_tbl AS
    SELECT * FROM read_parquet('data/ghg_emissions_df.parquet');
"""

create_emissions_table_query = """
    CREATE OR REPLACE TABLE emissions_tbl AS
    SELECT * FROM read_parquet('data/emissions_clean_df.parquet');
"""
# This feed the evidence.dev carbon emissions dashboard
# note the tricksy regex to account for lulucf which is not labelled with _total
# in the spreadsheet but must be accounted for
create_ca_emissions_evidence_long_tbl = r"""
CREATE OR REPLACE TABLE ca_emissions_evidence_long_tbl AS (
SELECT region_country, local_authority, local_authority_code, calendar_year,
       population_000s_mid_year_estimate, per_capita_emissions_t_co2e, area_km2,
       emissions_per_km2_kt_co2e, sector,
       regexp_matches(sector, '_total$|lulucf_net_emissions') total_bool,
       grand_total, emissions_kt_co2e, cauthcd, cauthnm
FROM (
UNPIVOT emissions_tbl
ON COLUMNS(* EXCLUDE (  region_country,
                        second_tier_authority,
                        local_authority,
                        local_authority_code,
                        calendar_year,
                        population_000s_mid_year_estimate,
                        per_capita_emissions_t_co2e,
                        area_km2,
                        grand_total,
                        emissions_per_km2_kt_co2e))
INTO
    NAME sector
    VALUE emissions_kt_co2e)  up
INNER JOIN ca_la_tbl la
ON up.local_authority_code = la.ladcd)
"""

# INDEXES - these cause a byte length error in the transaction
create_lsoa21cd_geom_index = (
    "CREATE INDEX lsoa21cd_geom_idx ON lsoa_poly_2021_tbl (geom)"
)
create_lsoa11cd_geom_index = (
    "CREATE INDEX lsoa11cd_geom_idx ON lsoa_poly_2011_tbl (geom)"
)

create_lsoa21cd_poly_index_query = """
    CREATE UNIQUE INDEX lsoa21cd_poly_idx ON lsoa_poly_2021_tbl (lsoa21cd)
"""
create_lsoa11cd_poly_index_query = """
    CREATE UNIQUE INDEX lsoa11cd_poly_idx ON lsoa_poly_2011_tbl (lsoa11cd)
"""

# TENURE
create_tenure_table_query = """
    CREATE OR REPLACE TABLE tenure_tbl AS
    SELECT * FROM read_parquet('data/tenure_df.parquet')
"""
create_lsoacd_tenure_index_query = """
    CREATE UNIQUE INDEX lsoacd_tenure_idx ON tenure_tbl (lsoa21cd)
"""

# POSTCODES
create_postcodes_table_query = """
    CREATE OR REPLACE TABLE postcodes_tbl AS
    SELECT *
    FROM read_csv_auto(
    'data/postcode_centroids/ONSPD*.csv',
    types={'ruc11ind': 'VARCHAR',
    'ruc21ind': 'VARCHAR',
    'north1m': 'BIGINT',
    'ssr95cd': 'VARCHAR'});
"""
create_postcode_centroids_index_query = """
    CREATE UNIQUE INDEX postcode_centroids_idx ON postcodes_tbl (pcds)
"""
add_geom_column_postcodes_query = (
    "ALTER TABLE postcodes_tbl ADD COLUMN geom GEOMETRY"
)
update_geom_postcodes_query = (
    "UPDATE postcodes_tbl SET geom = ST_Point(long, lat)"
)

create_postcode_lookup_table_query = """
CREATE OR REPLACE TABLE postcode_lookup_tbl AS
FROM read_csv("data/postcode_centroids/PCD*.csv")
"""

# CA LA lookups
create_ca_la_table_query = """CREATE OR REPLACE TABLE ca_la_tbl AS
    SELECT * FROM read_parquet('data/ca_la_df.parquet')"""

# CA Boundaries
create_ca_boundaries_table_query = """
 CREATE OR REPLACE TABLE ca_boundaries_bgc_tbl AS
 SELECT * FROM ST_Read('data/ca_boundaries.geojson')
"""

# LA data from mysociety
create_la_all_mysoc_tbl_query = """
CREATE TABLE la_all_mysoc_tbl AS
SELECT * FROM read_csv('https://raw.githubusercontent.com/mysociety/uk_local_authority_names_and_codes/refs/heads/main/data/uk_local_authorities.csv')
"""

# NEED TO CREATE INDEXES FOR EPC TABLES
create_lmk_key_dom_index_query = """
    CREATE UNIQUE INDEX lmk_key_dom_idx ON epc_domestic_tbl (LMK_KEY)
"""
create_lmk_key_nondom_index_query = """
    CREATE UNIQUE INDEX lmk_key_nondom_idx ON epc_nondom_tbl (LMK_KEY)
"""

create_ca_la_dft_lookup_table_query = """
    CREATE OR REPLACE TABLE ca_la_dft_lookup_tbl AS
    SELECT * FROM read_parquet('data/ca_la_dft_lookup_df.parquet');
"""
create_ca_la_dft_lookup_index_query = """
    CREATE UNIQUE INDEX ca_la_dft_lookup_idx ON ca_la_dft_lookup_tbl (ladcd)
"""

create_simple_geog_lookup_vw = """
    CREATE OR REPLACE VIEW simple_geog_lookup_vw AS
    (WITH woe_la_cte AS
        (SELECT ladcd, ladnm FROM ca_la_tbl
        WHERE cauthnm = 'West of England')
SELECT COLUMNS('^ls|^ms|^lad|^pcd|^wd|^lad|^imd|lat|long') 
FROM postcodes_tbl pc
INNER JOIN woe_la_cte ON pc.lad25cd = woe_la_cte.ladcd);
"""

create_per_cap_emissions_ca_national_vw = r"""
CREATE OR REPLACE VIEW per_cap_emissions_ca_national_vw AS
(SELECT
  "t6"."calendar_year",
  "t6"."cauthnm" AS "area",
  "t6"."gt_sum_ca" / "t6"."pop_sum_ca" AS "per_cap"
FROM (
  SELECT
    "t5"."cauthnm",
    "t5"."calendar_year",
    SUM("t5"."grand_total") AS "gt_sum_ca",
    SUM("t5"."population_000s_mid_year_estimate") AS "pop_sum_ca"
  FROM (
    SELECT
    "t2"."region_country",
    "t2"."second_tier_authority",
    "t2"."local_authority",
    "t2"."local_authority_code",
    "t2"."calendar_year",
    "t2"."industry_electricity",
    "t2"."industry_gas",
    "t2"."large_industrial_installations",
    "t2"."industry_other",
    "t2"."industry_total",
    "t2"."commercial_electricity",
    "t2"."commercial_gas",
    "t2"."commercial_other",
    "t2"."commercial_total",
    "t2"."public_sector_electricity",
    "t2"."public_sector_gas",
    "t2"."public_sector_other",
    "t2"."public_sector_total",
    "t2"."domestic_electricity",
    "t2"."domestic_gas",
    "t2"."domestic_other",
    "t2"."domestic_total",
    "t2"."road_transport_a_roads",
    "t2"."road_transport_motorways",
    "t2"."road_transport_minor_roads",
    "t2"."diesel_railways",
    "t2"."transport_other",
    "t2"."transport_total",
    "t2"."net_emissions_forestry",
    "t2"."net_emissions_cropland_mineral_soils_change",
    "t2"."net_emissions_grassland_mineral_soils_change",
    "t2"."net_emissions_settlements",
    "t2"."net_emissions_peatland",
    "t2"."net_emissions_bioenergy_crops",
    "t2"."net_emissions_other_lulucf",
    "t2"."lulucf_net_emissions",
    "t2"."agriculture_electricity",
    "t2"."agriculture_gas",
    "t2"."agriculture_other",
    "t2"."agriculture_livestock",
    "t2"."agriculture_soils",
    "t2"."agriculture_total",
    "t2"."landfill",
    "t2"."waste_other",
    "t2"."waste_total",
    "t2"."grand_total",
    "t2"."population_000s_mid_year_estimate",
    "t2"."per_capita_emissions_t_co2e",
    "t2"."area_km2",
    "t2"."emissions_per_km2_kt_co2e",
      "t4"."ladcd",
      "t4"."ladnm",
      "t4"."cauthcd",
      "t4"."cauthnm"
    FROM "emissions_tbl" AS "t2"
    INNER JOIN (
      SELECT
        *
      FROM "ca_la_tbl" AS "t1"
      WHERE
        "t1"."ladnm" <> 'North Somerset'
    ) AS "t4"
      ON "t2"."local_authority_code" = "t4"."ladcd"
  ) AS "t5"
  GROUP BY
    1,
    2
) AS "t6");
"""

create_ca_la_ghg_emissions_sub_sector_ods_vw = r"""
CREATE OR REPLACE VIEW ca_la_ghg_emissions_sub_sector_ods_vw AS
(
    WITH joined_data AS (
        SELECT *
        FROM ghg_emissions_tbl ghg
        INNER JOIN ca_la_tbl ca ON ghg.local_authority_code = ca.ladcd
    )
    SELECT * EXCLUDE (country, country_code, region, ladcd, ladnm, second_tier_authority)
    FROM joined_data
)
"""

# create_ca_la_ghg_emissions_sub_sector_ods_vw = r"""
# CREATE OR REPLACE VIEW ca_la_ghg_emissions_sub_sector_ods_vw AS
# (SELECT * EXCLUDE(ghg.country,
#                     ghg.country_code,
#                     ghg.region,
#                     ca.ladcd,
#                     ca.ladnm,
#                     ghg.second_tier_authority)
#   FROM ghg_emissions_tbl ghg
#   INNER JOIN ca_la_tbl ca ON ghg.local_authority_code = ca.ladcd);
# """

create_epc_domestic_vw = r"""
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

        -- New Rule: Handle 'before YYYY'
        WHEN regexp_matches(CONSTRUCTION_AGE_BAND, 'before (\d{4})')
        THEN CAST(regexp_extract(CONSTRUCTION_AGE_BAND, 'before (\d{4})', 1) AS INTEGER)

        -- New Rule: Handle 'YYYY onwards'
        WHEN regexp_matches(CONSTRUCTION_AGE_BAND, '(\d{4}) onwards')
        THEN CAST(regexp_extract(CONSTRUCTION_AGE_BAND, '(\d{4}) onwards', 1) AS INTEGER)

        -- Rule 1 (Revised): If a single 4-digit year is present anywhere, return that year
        WHEN regexp_matches(CONSTRUCTION_AGE_BAND, '(\d{4})')
        THEN CAST(regexp_extract(CONSTRUCTION_AGE_BAND, '(\d{4})', 1) AS INTEGER)

        -- Rule 3: If no numerical year value can be extracted, return NULL
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
    date_part('day', c.LODGEMENT_DATETIME)
        AS LODGEMENT_DAY,
    date_part('month', c.LODGEMENT_DATETIME)
        AS LODGEMENT_MONTH,
    date_part('year', c.LODGEMENT_DATETIME)
        AS LODGEMENT_YEAR,
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
FROM epc_domestic_tbl c
-- Join the certificates table with the latest certificates for each UPRN
-- This is to ensure that we only have the latest certificate for each UPRN
INNER JOIN (
    SELECT UPRN, MAX(LODGEMENT_DATETIME) as max_date
    FROM epc_domestic_tbl
    GROUP BY UPRN
) latest ON c.UPRN = latest.UPRN
    AND c.LODGEMENT_DATETIME = latest.max_date
LEFT JOIN postcodes_tbl ON c.POSTCODE = postcodes_tbl.pcds
LEFT JOIN tenure_tbl ON postcodes_tbl.lsoa21cd = tenure_tbl.lsoa21cd
LEFT JOIN imd_lsoa_tbl ON postcodes_tbl.lsoa11cd = imd_lsoa_tbl.lsoa11cd);
"""
create_epc_domestic_lep_vw = """
CREATE OR REPLACE VIEW epc_domestic_lep_vw AS(
SELECT * FROM epc_domestic_vw
 WHERE local_authority IN
        (SELECT ladcd
        FROM ca_la_tbl
        WHERE cauthnm = \'West of England\')
        AND (long BETWEEN -3.1178 AND -2.25211)
        AND (lat BETWEEN 51.273 AND 51.68239));
"""
create_epc_non_domestic_vw = """
CREATE OR REPLACE VIEW epc_non_domestic_vw AS(
        SELECT
                c.*,
                ca_la_tbl.*,
                p.lsoa21cd,
                p.lat,
                p.long,
    date_part('day', c.LODGEMENT_DATETIME)
        AS LODGEMENT_DAY,
    date_part('month', c.LODGEMENT_DATETIME)
        AS LODGEMENT_MONTH,
    date_part('year', c.LODGEMENT_DATETIME)
        AS LODGEMENT_YEAR
        FROM epc_nondom_tbl c
-- Join the certificates table with the latest certificates for each UPRN
-- This is to ensure that we only have the latest certificate for each UPRN
INNER JOIN (
    SELECT UPRN, MAX(LODGEMENT_DATETIME) as max_date
    FROM epc_nondom_tbl
    GROUP BY UPRN
) latest ON c.UPRN = latest.UPRN
    AND c.LODGEMENT_DATETIME = latest.max_date

INNER JOIN
        ca_la_tbl
        ON c.LOCAL_AUTHORITY = ca_la_tbl.ladcd
INNER JOIN
        (SELECT pcds, lsoa21cd, lat, long
        FROM postcodes_tbl) as p
        ON c.POSTCODE = p.pcds

WHERE c.LOCAL_AUTHORITY IN
        (SELECT ladcd
        FROM ca_la_tbl
        WHERE ca_la_tbl.cauthnm = \'West of England\'));
"""
# This is the view that will be used to create the ODS dataset
create_epc_domestic_ods_vw = """
CREATE OR REPLACE VIEW epc_domestic_ods_vw AS(
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
create_epc_non_domestic_ods_vw = """
CREATE OR REPLACE VIEW epc_non_domestic_ods_vw AS (
SELECT
UPRN uprn,
LMK_KEY lmk_key,
BUILDING_REFERENCE_NUMBER building_reference_number,
ASSET_RATING asset_rating,
ASSET_RATING_BAND asset_rating_band,
PROPERTY_TYPE property_type,
LOCAL_AUTHORITY local_authority,
CONSTITUENCY constituency,
TRANSACTION_TYPE transaction_type,
STANDARD_EMISSIONS standard_emissions,
TYPICAL_EMISSIONS typical_emissions,
TARGET_EMISSIONS target_emissions,
BUILDING_EMISSIONS building_emissions,
BUILDING_LEVEL building_level,
RENEWABLE_SOURCES renewable_sources,
LODGEMENT_DATE "date",
LODGEMENT_YEAR "year",
LODGEMENT_MONTH "month",
ladcd,
ladnm,
cauthcd,
cauthnm,
lsoa21cd,
lat,
long,
concat('{', lat, ', ', long, '}') as geo_point_2d
FROM epc_non_domestic_vw
WHERE (long BETWEEN -3.1178 AND -2.25211)
AND (lat BETWEEN 51.273 AND 51.68239));
"""
