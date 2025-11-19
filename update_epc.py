# %%
import duckdb
import polars as pl

import get_ca_data as get_ca  # functions for retrieving CA \ common data

# %%
esri_fs_base_url = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
)
# %%

ca_la_df = get_ca.get_ca_la_df(
    2024, esri_fs_base_url, inc_ns=True
)  # include NS
la_list = ca_la_df["ladcd"]  # includes north somerset
# %%
from_date_dict_domestic = get_ca.get_epc_from_date(type="domestic")
from_date_dict_non_domestic = get_ca.get_epc_from_date(type="non-domestic")
# from_date_dict = {'year': 2024, 'month': 10}
# la_list_test = la_list[0:2]

# %%

domestic_epc_update_pldf = get_ca.make_epc_update_pldf(
    la_list, from_date_dict_domestic, type="domestic"
)

# %%
non_domestic_epc_update_pldf = get_ca.make_epc_update_pldf(
    la_list, from_date_dict_non_domestic, type="non-domestic"
)

# %%
domestic_epc_update_pldf.glimpse()

# %%
non_domestic_epc_update_pldf.glimpse()
# %%
con = duckdb.connect(database="data/ca_epc.duckdb")

# %%
con.sql("""
        INSERT OR REPLACE INTO
        epc_domestic_tbl
        SELECT * FROM  domestic_epc_update_pldf;
        """)

# %%

con.sql("""
        INSERT OR REPLACE INTO
        epc_nondom_tbl
        SELECT * FROM  non_domestic_epc_update_pldf;
        """)


# %%
con.sql("SELECT COUNT(*) FROM epc_nondom_tbl;")


# %%
con.execute("LOAD SPATIAL;")
# %%

con.table("epc_domestic_ods_vw").to_csv("data/epc_domestic_ods_vw.csv")
# %%
con.table("epc_domestic_ods_vw").count("*")
# %%
con.table("epc_non_domestic_ods_vw").to_csv("data/epc_non_domestic_ods_vw.csv")
# %%
con.table("epc_non_domestic_ods_vw").count("*")

# %%
con.table("epc_non_domestic_ods_vw").select("ladnm").limit(10).execute()

# %%
con.close()

# %%
