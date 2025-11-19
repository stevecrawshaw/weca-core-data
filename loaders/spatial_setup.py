"""
Custom DuckDB spatial operations.

Replaces spatial setup functionality from build_tables_queries.py

This module provides functions for:
- Installing and loading DuckDB spatial extension
- Adding geometry columns to tables
- Creating spatial indexes
"""

import logging
from typing import Literal

import duckdb

logger = logging.getLogger(__name__)


def setup_spatial_extension(con: duckdb.DuckDBPyConnection) -> None:
    """
    Install and load DuckDB spatial extension.

    Replaces:
        - install_spatial_query from build_tables_queries.py
        - load_spatial_query from build_tables_queries.py

    Args:
        con: DuckDB connection

    Example:
        >>> con = duckdb.connect("data/ca_epc.duckdb")
        >>> setup_spatial_extension(con)
        >>> con.close()
    """
    try:
        con.execute("INSTALL spatial;")
        logger.info("Spatial extension installed")
    except Exception as e:
        logger.warning(f"Spatial extension already installed: {e}")

    con.execute("LOAD spatial;")
    logger.info("Spatial extension loaded")


def add_geometry_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    x_col: str = "x",
    y_col: str = "y",
    geom_col: str = "geom",
) -> None:
    """
    Add geometry column to table from x/y coordinates.

    Replaces:
        - add_geom_column_* queries from build_tables_queries.py
        - update_geom_* queries from build_tables_queries.py

    Args:
        con: DuckDB connection
        table_name: Fully qualified table name (e.g., 'schema.table')
        x_col: Name of X coordinate column (longitude)
        y_col: Name of Y coordinate column (latitude)
        geom_col: Name of geometry column to create

    Example:
        >>> add_geometry_column(
        ...     con,
        ...     "transformed_data.lsoa_2021_pwc",
        ...     x_col="x",
        ...     y_col="y"
        ... )
    """
    try:
        # Add geometry column if it doesn't exist
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {geom_col} GEOMETRY;")
        logger.info(f"Added geometry column to {table_name}")

        # Populate geometry column from x/y coordinates
        con.execute(f"UPDATE {table_name} SET {geom_col} = ST_Point({x_col}, {y_col});")
        logger.info(f"Updated geometry column in {table_name}")

    except Exception as e:
        logger.error(f"Failed to add geometry column to {table_name}: {e}")
        raise


def add_geometry_column_from_wkt(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    wkt_col: str = "geometry",
    geom_col: str = "geom",
) -> None:
    """
    Add geometry column from Well-Known Text (WKT) representation.

    Replaces:
        - create_lsoa_poly_* table queries from build_tables_queries.py

    Args:
        con: DuckDB connection
        table_name: Fully qualified table name
        wkt_col: Name of WKT column
        geom_col: Name of geometry column to create

    Example:
        >>> add_geometry_column_from_wkt(
        ...     con,
        ...     "transformed_data.lsoa_poly_2021",
        ...     wkt_col="geometry"
        ... )
    """
    try:
        con.execute(
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {geom_col} GEOMETRY;"
        )
        con.execute(
            f"UPDATE {table_name} SET {geom_col} = ST_GeomFromText({wkt_col}::VARCHAR);"
        )
        logger.info(f"Added geometry column from WKT to {table_name}")

    except Exception as e:
        logger.error(f"Failed to add geometry from WKT to {table_name}: {e}")
        raise


def create_spatial_indexes(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    id_col: str,
    geom_col: str = "geom",
) -> None:
    """
    Create spatial indexes on table.

    Creates:
    1. Unique index on ID column
    2. Spatial index on geometry column

    Replaces:
        - create_*_index_query patterns from build_tables_queries.py

    Args:
        con: DuckDB connection
        table_name: Fully qualified table name
        id_col: Name of unique ID column (e.g., 'lsoa21cd')
        geom_col: Name of geometry column

    Example:
        >>> create_spatial_indexes(
        ...     con,
        ...     "transformed_data.lsoa_2021_pwc",
        ...     id_col="lsoa21cd"
        ... )
    """
    # Extract schema and table for index naming
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        index_prefix = f"{table}"
    else:
        index_prefix = table_name

    try:
        # Create unique index on ID column
        id_index_name = f"{index_prefix}_{id_col}_idx"
        con.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {id_index_name} ON {table_name} ({id_col});"
        )
        logger.info(f"Created unique index {id_index_name} on {table_name}")

        # Create spatial index on geometry column
        geom_index_name = f"{index_prefix}_{geom_col}_idx"
        con.execute(
            f"CREATE INDEX IF NOT EXISTS {geom_index_name} ON {table_name} ({geom_col});"
        )
        logger.info(f"Created spatial index {geom_index_name} on {table_name}")

    except Exception as e:
        logger.error(f"Failed to create indexes on {table_name}: {e}")
        raise


def create_standard_indexes(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    unique_cols: list[str] | None = None,
    index_cols: list[str] | None = None,
) -> None:
    """
    Create standard (non-spatial) indexes on table.

    Args:
        con: DuckDB connection
        table_name: Fully qualified table name
        unique_cols: List of columns to create unique indexes on
        index_cols: List of columns to create standard indexes on

    Example:
        >>> create_standard_indexes(
        ...     con,
        ...     "transformed_data.epc_domestic",
        ...     unique_cols=["lmk_key"],
        ...     index_cols=["postcode", "local_authority"]
        ... )
    """
    # Extract table name for index naming
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        index_prefix = f"{table}"
    else:
        index_prefix = table_name

    # Create unique indexes
    if unique_cols:
        for col in unique_cols:
            try:
                index_name = f"{index_prefix}_{col}_idx"
                con.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col});"
                )
                logger.info(f"Created unique index {index_name} on {table_name}")
            except Exception as e:
                logger.warning(f"Could not create unique index on {col}: {e}")

    # Create standard indexes
    if index_cols:
        for col in index_cols:
            try:
                index_name = f"{index_prefix}_{col}_idx"
                con.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col});"
                )
                logger.info(f"Created index {index_name} on {table_name}")
            except Exception as e:
                logger.warning(f"Could not create index on {col}: {e}")
