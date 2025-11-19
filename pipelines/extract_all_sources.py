"""
Unified extraction script using dlt sources

Replaces the extraction portions of cesap-epc-load-duckdb-data.py

Note: EPC extraction is commented out due to authentication issues with dlt.
      Will be handled with custom code in Phase 2.
"""

import dlt
from sources.arcgis_sources import arcgis_geographies_source, ca_boundaries_source
from sources.other_sources import (
    dft_traffic_resource,
    ghg_emissions_resource,
    imd_2025_resource,
)

# EPC source commented out - needs custom auth handling
# from sources.epc_sources import epc_certificates_source


def extract_all_data(db_path: str = "data/ca_epc.duckdb"):
    """
    Extract all data sources using dlt

    Args:
        db_path: Path to DuckDB database

    Returns:
        dlt pipeline object
    """

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="weca_etl",
        destination="duckdb",
        dataset_name="raw_data",
        destination_config={"credentials": db_path},
    )

    print("=" * 80)
    print("WECA Core Data Extraction")
    print("=" * 80)

    # Extract ArcGIS geographies
    print("\n1. Extracting ArcGIS geographical data...")
    print("   Resources: lsoa_2021_boundaries, lsoa_2011_boundaries,")
    print("             lsoa_2021_pwc, lsoa_2021_lookups, lsoa_2011_lookups")

    source = arcgis_geographies_source()
    load_info = pipeline.run(source)

    print(f"   [OK] Loaded: {load_info.load_id}")
    if load_info.has_failed_jobs:
        print(f"   [WARNING] Some jobs failed!")
        for job in load_info.failed_jobs:
            print(f"      Failed: {job}")

    # Extract CA boundaries
    print("\n2. Extracting Combined Authority boundaries...")
    source = ca_boundaries_source()
    load_info = pipeline.run(source)
    print(f"   [OK] Loaded: {load_info.load_id}")

    # Extract DFT traffic data
    print("\n3. Extracting DFT traffic data...")
    load_info = pipeline.run(dft_traffic_resource())
    print(f"   [OK] Loaded: {load_info.load_id}")

    # Extract GHG emissions data
    print("\n4. Extracting GHG emissions data...")
    load_info = pipeline.run(ghg_emissions_resource())
    print(f"   [OK] Loaded: {load_info.load_id}")

    # Extract IMD 2025 data
    print("\n5. Extracting IMD 2025 data...")
    load_info = pipeline.run(imd_2025_resource())
    print(f"   [OK] Loaded: {load_info.load_id}")

    # EPC extraction - commented out, needs custom handling
    # print("\n6. Extracting EPC domestic certificates...")
    # load_info = pipeline.run(
    #     epc_certificates_source(
    #         certificate_type="domestic",
    #         incremental=True,
    #     )
    # )
    # print(f"   [OK] Loaded: {load_info.load_id}")
    #
    # print("\n7. Extracting EPC non-domestic certificates...")
    # load_info = pipeline.run(
    #     epc_certificates_source(
    #         certificate_type="non-domestic",
    #         incremental=True,
    #     )
    # )
    # print(f"   [OK] Loaded: {load_info.load_id}")

    print("\n" + "=" * 80)
    print("[OK] All extractions completed")
    print(f"  Database: {db_path}")
    print("=" * 80)

    # Print summary of extracted tables
    print("\nExtracted tables:")
    try:
        with pipeline.sql_client() as client:
            with client.execute_query(
                "SELECT table_schema, table_name "
                "FROM information_schema.tables "
                "WHERE table_schema = 'raw_data'"
            ) as cursor:
                for row in cursor.fetchall():
                    schema, table = row
                    # Get row count
                    with client.execute_query(
                        f"SELECT COUNT(*) FROM {schema}.{table}"
                    ) as count_cursor:
                        count = count_cursor.fetchone()[0]
                    print(f"  - {table}: {count:,} records")
    except Exception as e:
        print(f"  Could not list tables: {e}")

    return pipeline


if __name__ == "__main__":
    pipeline = extract_all_data()
    print("\n[OK] Extraction pipeline completed successfully!")
