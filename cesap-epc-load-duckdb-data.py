# %%
import glob
import json

import duckdb
import geopandas as gpd
import pandas as pd
import polars as pl
from janitor.polars import clean_names

import build_tables_queries
import epc_schema
import get_ca_data as get_ca  # functions for retrieving CA \ common data

# %%
download_epc = False
download_lsoa = False
download_postcodes = False

# %% [markdown]
# This notebook retrieves all the base data needed for comparison analysis with
# other Combined Authorities and loads it into a duckdb database.
# %%

# tempcon = duckdb.connect()

# %%
data_directory = "data"
postcode_directory = "data/postcode_centroids"
bulk_epc_domestic_directory = "data/epc_bulk_zips"
bulk_epc_non_domestic_directory = "data/epc_bulk_nondom_zips"
db_export_directory = "data/db_export"
# %%
directories = [
    data_directory,
    postcode_directory,
    bulk_epc_domestic_directory,
    bulk_epc_non_domestic_directory,
    db_export_directory,
]
path_la_emissions_sector_xlsx = (
    "data/2005-23-uk-local-authority-ghg-emissions.xlsx"
)
path_2011_poly_parquet = "data/all_cas_lsoa_poly_2011.parquet"
path_2021_poly_parquet = "data/all_cas_lsoa_poly_2021.parquet"
# %%
get_ca.create_directories(directories)
# %%
# Define the base urls for the ArcGIS API and some parameters
# we get the 2011 LSOA data as these match to the IMD lsoa codes

# %%
esri_fs_base_url = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
)
esri_fs_tail_url = "FeatureServer/0/query"
# can't check epc url without creds
epc_base_url = "https://epc.opendatacommunities.org/api/v1/files/"
# %%

url_dict = dict(
    base_url_pc_centroids_zip="https://www.arcgis.com/sharing/rest/content/items/295e076b89b542e497e05632706ab429/data",
    base_url_pc_lookup="https://www.arcgis.com/sharing/rest/content/items/7fc55d71a09d4dcfa1fd6473138aacc3/data",
    base_url_lsoa_2021_centroids=get_ca.make_esri_fs_url(
        esri_fs_base_url,
        "LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/",
        esri_fs_tail_url,
    ),
    base_url_2021_lsoa_polys=get_ca.make_esri_fs_url(
        esri_fs_base_url,
        "Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/",
        esri_fs_tail_url,
    ),
    base_url_2011_lsoa_polys=get_ca.make_esri_fs_url(
        esri_fs_base_url,
        "LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/",
        esri_fs_tail_url,
    ),
    base_url_lsoa_2021_lookups=get_ca.make_esri_fs_url(
        esri_fs_base_url, "LSOA21_WD24_LAD24_EW_LU/", esri_fs_tail_url
    ),
    base_url_lsoa_2011_lookups=get_ca.make_esri_fs_url(
        esri_fs_base_url,
        "LSOA01_LSOA11_LAD11_EW_LU_ddfe1cd1c2784c9b991cded95bc915a9/",
        esri_fs_tail_url,
    ),
    url_ghg_emissions_csv="https://assets.publishing.service.gov.uk/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv",
    imd_data_path=r"https://opendatacommunities.org/downloads/cube-table?uri=http%3A%2F%2Fopendatacommunities.org%2Fdata%2Fsocietal-wellbeing%2Fimd2019%2Findices",
    nomis_ts054_url="https://www.nomisweb.co.uk/api/v01/dataset/NM_2072_1.data.csv",
    dft_csv_path="https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv",
    # old version ca_boundaries_url="https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Combined_Authorities_May_2023_Boundaries_EN_BGC/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
    ca_boundaries_url="https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/CAUTH_MAY_2025_EN_BGC/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
    la_all_mysoc_tbl_url="https://raw.githubusercontent.com/mysociety/uk_local_authority_names_and_codes/refs/heads/main/data/uk_local_authorities.csv",
)


# %%
# check if the source urls are valid
if get_ca.validate_urls(url_dict) == 0:
    print("All URLs are valid.")
else:
    print("Invalid URLs: ", get_ca.validate_urls(url_dict))

# %%

chunk_size = 100  # this is used in a where clause to set lsoa polys per call

# %%
ts054_params = {
    "date": ["latest"],
    "c2021_tenure_9": ["0,1001...1004,8,9996,9997"],
    "measures": ["20100"],
    "geography": ["TYPE151"],
    "select": [
        "GEOGRAPHY_NAME,GEOGRAPHY_CODE,C2021_TENURE_9_NAME,C2021_TENURE_9_SORTORDER,OBS_VALUE"
    ],
}
params_base = {"outFields": "*", "outSR": 4326, "f": "json"}
# %%
nomis_creds = get_ca.load_config("../config.yml").get("nomis")
# %%
ca_la_df = get_ca.get_ca_la_df(
    2025, baseurl=esri_fs_base_url, inc_ns=True
)  # include NS
# %%

duckdb.table("ca_la_df").to_parquet("data/ca_la_df.parquet")
# %%
ca_la_codes = get_ca.get_ca_la_codes(ca_la_df)  # INCLUDES N SOMERSET DEFAULT
print(f"There are {len(ca_la_codes)} LAs in Combined Authorities")
# ca_la_df.glimpse()
# %%
ca_la_dft_lookup_df = get_ca.get_ca_la_dft_lookup(
    url_dict.get("dft_csv_path"),
    la_list=ca_la_codes,
)
duckdb.table("ca_la_dft_lookup_df").to_parquet(
    "data/ca_la_dft_lookup_df.parquet"
)
# %%
# if download_lsoa:
lookups_2021_chunk_list = get_ca.get_chunk_list(
    url_dict.get("base_url_lsoa_2021_lookups"), params_base, max_records=1000
)

# %%
# %%
# list of pl.dataframes of the lookups data in cauths
lookups_2021_pldf_list = [
    get_ca.get_flat_data(
        chunk,
        params_base,
        params_other={"where": "1=1"},
        base_url=url_dict.get("base_url_lsoa_2021_lookups"),
    )
    for chunk in lookups_2021_chunk_list
]

lookups_2021_pldf = (
    pl.concat(lookups_2021_pldf_list, how="vertical_relaxed")
    .rename(lambda x: x.lower())
    .unique()
)
# %%

lookups_2011_chunk_list = get_ca.get_chunk_list(
    url_dict.get("base_url_lsoa_2011_lookups"), params_base, max_records=2000
)

lookups_2011_pldf_list = [
    get_ca.get_flat_data(
        chunk,
        params_base,
        params_other={"where": "1=1"},
        base_url=url_dict.get("base_url_lsoa_2011_lookups"),
    )
    for chunk in lookups_2011_chunk_list
]

lookups_2011_pldf = (
    pl.concat(lookups_2011_pldf_list, how="vertical_relaxed")
    .rename(lambda x: x.lower())
    .unique()
)
# %%
duckdb.table("lookups_2021_pldf").to_parquet("data/lookups_2021_pldf.parquet")
duckdb.table("lookups_2011_pldf").to_parquet("data/lookups_2011_pldf.parquet")
# %%
lsoas_in_cauths_iter = (
    lookups_2021_pldf.filter(pl.col("lad24cd").is_in(ca_la_codes))
    .select(pl.col("lsoa21cd"))
    .to_series()
)

lsoas_in_cauths_chunks = [
    lsoas_in_cauths_iter[i : i + chunk_size]
    for i in range(0, len(lsoas_in_cauths_iter), chunk_size)
]
# %%
lsoas_in_cauths_iter_list = list(lsoas_in_cauths_iter)
with open("data/lsoas_in_cauths_iter.json", "w") as f:
    # indent=2 is not needed but makes the file human-readable
    # if the data is nested
    json.dump(lsoas_in_cauths_iter_list, f, indent=2)
# %%
with open("data/lsoas_in_cauths_iter.json") as f:
    lsoas_in_cauths_iter = json.load(f)
# %%
# list of urls to get the lsoa polygons in the combined authorities
if download_lsoa:
    lsoa_2021_poly_url_list = [
        get_ca.make_poly_url(
            url_dict.get("base_url_2021_lsoa_polys"),
            params_base,
            lsoas,
            lsoa_code_name="lsoa21cd",
        )
        for lsoas in lsoas_in_cauths_chunks
    ]
    # a list of geopandas dataframes to hold all lsoa polygons in the CA's
    lsoa_2021_gdf_list = [
        gpd.read_file(polys_url) for polys_url in lsoa_2021_poly_url_list
    ]

    lsoa_2021_gdf = gpd.GeoDataFrame(
        pd.concat(lsoa_2021_gdf_list, ignore_index=True)
    ).drop_duplicates(subset="LSOA21CD")

    # parquet export to import to duckdb
    lsoa_2021_gdf.to_parquet(path_2021_poly_parquet)
# %%
# Retrieve the 2021 LSOA points (population weighted centroids)
lsoa_2021_pwc_df = get_ca.make_lsoa_pwc_df(
    base_url=url_dict.get("base_url_lsoa_2021_centroids"),
    params_base=params_base,
    params_other={"where": "1=1"},
    max_records=2000,
)
duckdb.table("lsoa_2021_pwc_df").to_parquet("data/lsoa_2021_pwc_df.parquet")
# %%
lsoa_2021_pwc_cauth_df = lsoa_2021_pwc_df.filter(
    pl.col("lsoa21cd").is_in(lsoas_in_cauths_iter)
).rename(lambda x: x.lower())
duckdb.table("lsoa_2021_pwc_cauth_df").to_parquet(
    "data/lsoa_2021_pwc_cauth_df.parquet"
)
# %%
# Retrieve the 2011 LSOA polygon data - for joining with IMD
# The latest IMD data available is for 2019 but uses 2011 geographies

lsoacd_2011_in_cauths_iter = (
    lookups_2011_pldf.filter(pl.col("lad11cd").is_in(ca_la_codes))
    .select(pl.col("lsoa11cd"))
    .unique()
    .to_series()
)

lsoa_2011_in_cauths_chunks = [
    lsoacd_2011_in_cauths_iter[i : i + chunk_size]
    for i in range(0, len(lsoacd_2011_in_cauths_iter), chunk_size)
]
if download_lsoa:
    lsoa_2011_poly_url_list = [
        get_ca.make_poly_url(
            url_dict.get("base_url_2011_lsoa_polys"),
            params_base,
            lsoas,
            lsoa_code_name="LSOA11CD",
        )
        for lsoas in lsoa_2011_in_cauths_chunks
    ]

    lsoa_2011_gdf_list = [
        gpd.read_file(polys_url) for polys_url in lsoa_2011_poly_url_list
    ]

    lsoa_2011_gdf = gpd.GeoDataFrame(
        pd.concat(lsoa_2011_gdf_list, ignore_index=True)
    )
    # parquet export to import to duckdb
    lsoa_2011_gdf.to_parquet(path_2011_poly_parquet)
# %%
# uses the opendatacommunities (full - featured) dataset
lsoa_imd_df = get_ca.read_process_imd(url_dict.get("imd_data_path"))
lsoa_imd_df.write_parquet("data/lsoa_imd_df.parquet")
# %%

get_ca.get_ca_geojson(
    url_dict.get("ca_boundaries_url"), output_path="data/ca_boundaries.geojson"
)

# %%

if download_postcodes:
    # centroids
    centroids_zipped_file_path = get_ca.download_zip(
        url=url_dict.get("base_url_pc_centroids_zip"),
        directory=postcode_directory,
        filename="postcodes.zip",
    )
    postcodes_centroids_csv_file = get_ca.extract_csv_from_zip(
        zip_file_path=centroids_zipped_file_path
    )
    get_ca.delete_zip_file(zip_file_path=centroids_zipped_file_path)
    # lookup - for names of LSOAS etc.
    lookup_zipped_file_path = get_ca.download_zip(
        url=url_dict.get("base_url_pc_lookup"),
        directory=postcode_directory,
        filename="postcodes_lookup.zip",
    )

    postcodes_lookup_csv_file = get_ca.extract_csv_from_zip(
        zip_file_path=lookup_zipped_file_path
    )
    get_ca.delete_zip_file(zip_file_path=lookup_zipped_file_path)

# %%
# make a list of urls, download the zip files and extract the csv files
# for domestic and non - domestic EPC data
# the csv's are ingested directly into the
# duckdb database without significant processing
# the EPC data is cleaned and processed in the database
#  creating views for export to the ODS
if download_epc:
    la_zipfile_nondom_list = get_ca.make_zipfile_list(
        ca_la_df, epc_base_url, type="non-domestic"
    )
    get_ca.dl_bulk_epc_zip(
        la_zipfile_nondom_list, path="data/epc_bulk_nondom_zips"
    )
    get_ca.extract_and_rename_csv_from_zips("data/epc_bulk_nondom_zips")
    la_zipfile_list = get_ca.make_zipfile_list(
        ca_la_df, epc_base_url, type="domestic"
    )
    get_ca.dl_bulk_epc_zip(la_zipfile_list, path="data/epc_bulk_zips")
    get_ca.extract_and_rename_csv_from_zips("data/epc_bulk_zips")

# %%

# %%
# TENURE
# Load all tenure LSOA data into the db and create views for
# subsets e.g. West of England and all cauths
tenure_raw_df = get_ca.get_nomis_data(
    url_dict.get("nomis_ts054_url"), ts054_params, nomis_creds
)

# %%
tenure_df = (
    tenure_raw_df.pivot(
        "C2021_TENURE_9_NAME",
        index=["GEOGRAPHY_NAME", "GEOGRAPHY_CODE"],
        values="OBS_VALUE",
    )
    .clean_names()  # uses pyjanitor.polars
    .rename({"geography_name": "lsoa_name", "geography_code": "lsoa21cd"})
)

tenure_df.write_parquet("data/tenure_df.parquet")

# %% [Markdown]
# Green house gas emissions (not just CO2) by sector and LA (all LA's)
emissions_clean_df = pl.read_excel(
    path_la_emissions_sector_xlsx,
    has_header=True,
    sheet_name="1_1",
    table_name="Table1.1",
).clean_names(strip_underscores=True)
# %%
emissions_clean_df.write_parquet("data/emissions_clean_df.parquet")
# %%
ghg_emissions_df = pl.read_csv(
    url_dict.get("url_ghg_emissions_csv"), ignore_errors=True
).clean_names(strip_underscores=True)
ghg_emissions_df.write_parquet("data/ghg_emissions_df.parquet")

# %%
# con.close()
# %%
con = duckdb.connect("data/ca_epc.duckdb")

# %%
try:
    con.execute("BEGIN TRANSACTION;")

    # Install and load extensions
    con.execute(build_tables_queries.install_spatial_query)
    con.execute(build_tables_queries.load_spatial_query)
    # con.execute(build_tables_queries.install_httpfs_query)
    # con.execute(build_tables_queries.load_httpfs_query)
    print("Extensions installed and loaded")
    # LSOA PWC
    con.execute(build_tables_queries.create_lsoa_pwc_table_query)
    con.execute(build_tables_queries.add_geom_column_lsoa_pwc_query)
    con.execute(build_tables_queries.update_geom_lsoa_pwc_query)
    con.execute(build_tables_queries.create_lsoacd_pwc_index_query)
    print("LSOA PWC table created")
    # LOOKUPS
    con.execute(build_tables_queries.create_lsoa_2021_lookup_table_query)
    con.execute(build_tables_queries.create_lsoa21cd_lookup_index_query)
    con.execute(build_tables_queries.create_lsoa_2011_lookup_table_query)
    print("LOOKUPS tables created")
    # IMD
    con.execute(build_tables_queries.create_imd_lsoa_table_query)
    con.execute(build_tables_queries.create_lsoa11cd_imd_index_query)
    print("IMD table created")
    # EPC (Example of a parameterized query)
    con.execute(
        build_tables_queries.create_epc_domestic_table_query,
        parameters=[epc_schema.cols_schema_domestic],
    )
    con.execute(
        build_tables_queries.create_epc_nondom_table_query,
        parameters=[epc_schema.cols_schema_nondom],
    )
    print("EPC tables created")
    # LSOA POLYS (Example of a parameterized query)
    con.execute(
        build_tables_queries.create_lsoa_poly_2021_table_query,
        parameters=[path_2021_poly_parquet],
    )
    print("create_lsoa_poly_2021_table_query")
    con.execute(
        build_tables_queries.create_lsoa_poly_2011_table_query,
        parameters=[path_2011_poly_parquet],
    )
    print("create_lsoa_poly_2011_table_query")
    # EMISSIONS
    con.execute(build_tables_queries.create_ghg_emissions_table_query)
    con.execute(build_tables_queries.create_emissions_table_query)
    print("Emissions tables created")
    # INDEXES - spatial indexes removed as they exceed the byte length for motherduck
    # con.execute(build_tables_queries.create_lsoa21cd_geom_index)
    # con.execute(build_tables_queries.create_lsoa11cd_geom_index)
    con.execute(build_tables_queries.create_lsoa21cd_poly_index_query)
    con.execute(build_tables_queries.create_lsoa11cd_poly_index_query)
    print("Indexes created")
    # TENURE
    con.execute(build_tables_queries.create_tenure_table_query)
    con.execute(build_tables_queries.create_lsoacd_tenure_index_query)
    print("Tenure table created")
    # POSTCODES
    con.execute(build_tables_queries.create_postcodes_table_query)
    con.execute(build_tables_queries.create_postcode_centroids_index_query)
    con.execute(build_tables_queries.add_geom_column_postcodes_query)
    con.execute(build_tables_queries.update_geom_postcodes_query)
    con.execute(build_tables_queries.create_postcode_lookup_table_query)
    print("Postcodes table created")
    # CA LA lookups
    con.execute(build_tables_queries.create_ca_la_table_query)
    print("CA LA table created")
    # CA Boundaries
    con.execute(build_tables_queries.create_ca_boundaries_table_query)
    print("CA Boundaries table created")
    # LA data mysociety csv
    con.execute(build_tables_queries.create_la_all_mysoc_tbl_query)
    print("LA data mysociety csv table created")
    # INDEXES FOR EPC TABLES
    con.execute(build_tables_queries.create_lmk_key_dom_index_query)
    con.execute(build_tables_queries.create_lmk_key_nondom_index_query)
    con.execute(build_tables_queries.create_ca_la_dft_lookup_table_query)
    con.execute(build_tables_queries.create_ca_la_dft_lookup_index_query)
    print("Indexes created for EPC tables")
    # CREATE VIEWS
    con.execute(build_tables_queries.create_simple_geog_lookup_vw)
    print("geog")
    con.execute(build_tables_queries.create_epc_domestic_vw)
    print("create_epc_domestic_vw")
    con.execute(build_tables_queries.create_epc_non_domestic_vw)
    print("create_epc_non_domestic_vw")
    con.execute(build_tables_queries.create_epc_domestic_lep_vw)
    print("create_epc_domestic_lep_vw")
    con.execute(build_tables_queries.create_epc_domestic_ods_vw)
    print("create_epc_domestic_ods_vw")
    con.execute(build_tables_queries.create_epc_non_domestic_ods_vw)
    print("create_epc_non_domestic_ods_vw")
    con.execute(build_tables_queries.create_per_cap_emissions_ca_national_vw)
    print("create_per_cap_emissions_ca_national_vw")
    con.execute(build_tables_queries.create_ca_emissions_evidence_long_tbl)
    print("create_ca_emissions_evidence_long_tbl")
    con.execute(
        build_tables_queries.create_ca_la_ghg_emissions_sub_sector_ods_vw
    )
    print("create_ca_la_ghg_emissions_sub_sector_ods_vw")
    print("Views created")
    con.execute("COMMIT;")
    print("Transaction committed")
except Exception as e:
    print(f"Transaction rolled back due to an error: {e}")


# %%
# con.sql("ROLLBACK;")
# %%
# Introspect
con.sql("SHOW TABLES;")

# %%
# Clean up the bulk ZIP files

bulk_files = glob.glob("data/epc_bulk_zips/*.zip")

for file in bulk_files:
    get_ca.delete_file(file)
# %%
bulk_files = glob.glob("data/epc_bulk_nondom_zips/*.zip")

for file in bulk_files:
    get_ca.delete_file(file)

# %%
con.sql("SELECT count(*) FROM epc_domestic_ods_vw;")

# %%
con.execute("LOAD SPATIAL;")
con.execute("""
COPY epc_domestic_ods_vw TO 'data/epc_domestic_ods_vw.csv' (FORMAT CSV);
""")
# %%
# %%
con.sql("SELECT count(*) FROM epc_non_domestic_ods_vw;")
# %%

con.execute("""
            COPY epc_non_domestic_ods_vw
             TO 'data/epc_non_domestic_ods_vw.csv' (FORMAT CSV);
            """)

# %%
con.sql("EXPORT DATABASE 'data/db_export' (FORMAT PARQUET);")
# %%

# %%
# temporary files to delete
# get_ca.delete_file(path_2011_poly_parquet)
# get_ca.delete_file(path_2021_poly_parquet)


# %%
con.close()
# %% [markdown]
# %%
