"""
Unit tests for DuckDB loader operations.

Tests all functions from:
- loaders/spatial_setup.py
"""

import duckdb
import polars as pl
import pytest

from loaders.spatial_setup import (
    add_geometry_column,
    add_geometry_column_from_wkt,
    create_spatial_indexes,
    create_standard_indexes,
    setup_spatial_extension,
)


def check_spatial_available() -> bool:
    """
    Check if DuckDB spatial extension can be installed.

    Returns False in network-restricted environments.
    """
    try:
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        con.execute("SELECT ST_Point(0, 0);")
        con.close()
        return True
    except Exception:
        return False


# Check if spatial is available once at module load time
SPATIAL_AVAILABLE = check_spatial_available()
pytestmark = pytest.mark.skipif(
    not SPATIAL_AVAILABLE,
    reason="DuckDB spatial extension not available (network restricted)",
)


class TestSetupSpatialExtension:
    """Test the setup_spatial_extension function."""

    def test_installs_and_loads_spatial_extension(self, in_memory_duckdb):
        """Test that spatial extension is installed and loaded successfully."""
        setup_spatial_extension(in_memory_duckdb)

        # Verify spatial functions are available
        result = in_memory_duckdb.execute("SELECT ST_Point(0, 0) AS geom;").fetchone()
        assert result is not None

    def test_handles_already_installed_extension(self, in_memory_duckdb):
        """Test that function handles already-installed extension gracefully."""
        # Install once
        setup_spatial_extension(in_memory_duckdb)

        # Install again (should not raise error)
        setup_spatial_extension(in_memory_duckdb)

        # Verify still works
        result = in_memory_duckdb.execute("SELECT ST_Point(0, 0) AS geom;").fetchone()
        assert result is not None


class TestAddGeometryColumn:
    """Test the add_geometry_column function."""

    def test_adds_geometry_column_from_xy(self, in_memory_duckdb):
        """Test that geometry column is created from x/y coordinates."""
        setup_spatial_extension(in_memory_duckdb)

        # Create test table
        in_memory_duckdb.execute(
            """
            CREATE TABLE test_points (
                id INTEGER,
                x DOUBLE,
                y DOUBLE
            );
        """
        )

        # Insert test data
        in_memory_duckdb.execute(
            """
            INSERT INTO test_points VALUES
            (1, -2.5879, 51.4545),
            (2, -2.5895, 51.4560);
        """
        )

        # Add geometry column
        add_geometry_column(in_memory_duckdb, "test_points", x_col="x", y_col="y")

        # Verify geometry column exists
        result = in_memory_duckdb.execute("SELECT geom FROM test_points LIMIT 1;").fetchone()
        assert result is not None
        assert result[0] is not None

    def test_handles_custom_column_names(self, in_memory_duckdb):
        """Test that function works with custom x/y/geom column names."""
        setup_spatial_extension(in_memory_duckdb)

        in_memory_duckdb.execute(
            """
            CREATE TABLE test_custom (
                longitude DOUBLE,
                latitude DOUBLE
            );
        """
        )

        in_memory_duckdb.execute("INSERT INTO test_custom VALUES (-2.5879, 51.4545);")

        # Use custom column names
        add_geometry_column(
            in_memory_duckdb,
            "test_custom",
            x_col="longitude",
            y_col="latitude",
            geom_col="location",
        )

        # Verify custom geometry column exists
        columns = [
            desc[0]
            for desc in in_memory_duckdb.execute("DESCRIBE test_custom;").fetchall()
        ]
        assert "location" in columns

    def test_idempotent_operation(self, in_memory_duckdb):
        """Test that adding geometry column twice doesn't fail."""
        setup_spatial_extension(in_memory_duckdb)

        in_memory_duckdb.execute(
            "CREATE TABLE test_idempotent (id INTEGER, x DOUBLE, y DOUBLE);"
        )
        in_memory_duckdb.execute("INSERT INTO test_idempotent VALUES (1, -2.5, 51.5);")

        # Add geometry column twice
        add_geometry_column(in_memory_duckdb, "test_idempotent")
        add_geometry_column(in_memory_duckdb, "test_idempotent")

        # Verify still works
        result = in_memory_duckdb.execute(
            "SELECT COUNT(*) FROM test_idempotent;"
        ).fetchone()
        assert result[0] == 1


class TestAddGeometryColumnFromWkt:
    """Test the add_geometry_column_from_wkt function."""

    def test_adds_geometry_from_wkt(self, in_memory_duckdb):
        """Test that geometry column is created from WKT."""
        setup_spatial_extension(in_memory_duckdb)

        # Create test table with WKT geometry
        in_memory_duckdb.execute(
            """
            CREATE TABLE test_wkt (
                id INTEGER,
                geometry VARCHAR
            );
        """
        )

        in_memory_duckdb.execute(
            """
            INSERT INTO test_wkt VALUES
            (1, 'POINT(-2.5879 51.4545)'),
            (2, 'POINT(-2.5895 51.4560)');
        """
        )

        # Add geometry column from WKT
        add_geometry_column_from_wkt(in_memory_duckdb, "test_wkt", wkt_col="geometry")

        # Verify geometry column exists and is populated
        result = in_memory_duckdb.execute("SELECT geom FROM test_wkt LIMIT 1;").fetchone()
        assert result is not None
        assert result[0] is not None

    def test_handles_custom_wkt_column_name(self, in_memory_duckdb):
        """Test with custom WKT column name."""
        setup_spatial_extension(in_memory_duckdb)

        in_memory_duckdb.execute(
            """
            CREATE TABLE test_custom_wkt (
                wkt_text VARCHAR
            );
        """
        )

        in_memory_duckdb.execute("INSERT INTO test_custom_wkt VALUES ('POINT(-2.5 51.5)');")

        add_geometry_column_from_wkt(
            in_memory_duckdb, "test_custom_wkt", wkt_col="wkt_text", geom_col="location"
        )

        # Verify custom geometry column exists
        result = in_memory_duckdb.execute(
            "SELECT location FROM test_custom_wkt;"
        ).fetchone()
        assert result is not None


class TestCreateSpatialIndexes:
    """Test the create_spatial_indexes function."""

    def test_creates_id_and_spatial_indexes(self, in_memory_duckdb):
        """Test that both ID and spatial indexes are created."""
        setup_spatial_extension(in_memory_duckdb)

        # Create and populate test table
        in_memory_duckdb.execute(
            """
            CREATE TABLE test_indexes (
                lsoa_code VARCHAR,
                x DOUBLE,
                y DOUBLE
            );
        """
        )

        in_memory_duckdb.execute(
            """
            INSERT INTO test_indexes VALUES
            ('E01014533', -2.5879, 51.4545),
            ('E01014534', -2.5895, 51.4560);
        """
        )

        add_geometry_column(in_memory_duckdb, "test_indexes")

        # Create indexes
        create_spatial_indexes(in_memory_duckdb, "test_indexes", id_col="lsoa_code")

        # Verify indexes exist
        indexes = in_memory_duckdb.execute(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE table_name = 'test_indexes';
        """
        ).fetchall()

        index_names = [idx[0] for idx in indexes]
        assert "test_indexes_lsoa_code_idx" in index_names
        assert "test_indexes_geom_idx" in index_names

    def test_handles_schema_qualified_table_names(self, in_memory_duckdb):
        """Test with schema.table name format."""
        setup_spatial_extension(in_memory_duckdb)

        # Create schema and table
        in_memory_duckdb.execute("CREATE SCHEMA test_schema;")
        in_memory_duckdb.execute(
            """
            CREATE TABLE test_schema.test_table (
                id VARCHAR,
                x DOUBLE,
                y DOUBLE
            );
        """
        )

        in_memory_duckdb.execute(
            "INSERT INTO test_schema.test_table VALUES ('A', -2.5, 51.5);"
        )

        add_geometry_column(in_memory_duckdb, "test_schema.test_table")

        # Create indexes with schema-qualified name
        create_spatial_indexes(in_memory_duckdb, "test_schema.test_table", id_col="id")

        # Verify indexes created (index name should use table name only)
        indexes = in_memory_duckdb.execute(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE table_name = 'test_table';
        """
        ).fetchall()

        index_names = [idx[0] for idx in indexes]
        assert any("test_table_id_idx" in name for name in index_names)
        assert any("test_table_geom_idx" in name for name in index_names)

    def test_idempotent_index_creation(self, in_memory_duckdb):
        """Test that creating indexes twice doesn't fail."""
        setup_spatial_extension(in_memory_duckdb)

        in_memory_duckdb.execute(
            "CREATE TABLE test_idempotent_idx (id VARCHAR, x DOUBLE, y DOUBLE);"
        )
        in_memory_duckdb.execute("INSERT INTO test_idempotent_idx VALUES ('A', -2.5, 51.5);")

        add_geometry_column(in_memory_duckdb, "test_idempotent_idx")

        # Create indexes twice
        create_spatial_indexes(in_memory_duckdb, "test_idempotent_idx", id_col="id")
        create_spatial_indexes(in_memory_duckdb, "test_idempotent_idx", id_col="id")

        # Should not raise error


class TestCreateStandardIndexes:
    """Test the create_standard_indexes function."""

    def test_creates_unique_indexes(self, in_memory_duckdb):
        """Test that unique indexes are created."""
        in_memory_duckdb.execute(
            """
            CREATE TABLE test_unique_idx (
                lmk_key VARCHAR PRIMARY KEY,
                postcode VARCHAR
            );
        """
        )

        create_standard_indexes(
            in_memory_duckdb, "test_unique_idx", unique_cols=["lmk_key"], index_cols=None
        )

        # Verify unique index exists
        indexes = in_memory_duckdb.execute(
            """
            SELECT index_name, is_unique
            FROM duckdb_indexes()
            WHERE table_name = 'test_unique_idx';
        """
        ).fetchall()

        # Should have unique index on lmk_key
        unique_indexes = [idx for idx in indexes if idx[1]]  # is_unique = True
        assert len(unique_indexes) > 0

    def test_creates_standard_indexes(self, in_memory_duckdb):
        """Test that standard (non-unique) indexes are created."""
        in_memory_duckdb.execute(
            """
            CREATE TABLE test_std_idx (
                id INTEGER,
                postcode VARCHAR,
                la_code VARCHAR
            );
        """
        )

        create_standard_indexes(
            in_memory_duckdb,
            "test_std_idx",
            unique_cols=None,
            index_cols=["postcode", "la_code"],
        )

        # Verify indexes exist
        indexes = in_memory_duckdb.execute(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE table_name = 'test_std_idx';
        """
        ).fetchall()

        index_names = [idx[0] for idx in indexes]
        assert "test_std_idx_postcode_idx" in index_names
        assert "test_std_idx_la_code_idx" in index_names

    def test_handles_both_unique_and_standard_indexes(self, in_memory_duckdb):
        """Test creating both unique and standard indexes together."""
        in_memory_duckdb.execute(
            """
            CREATE TABLE test_mixed_idx (
                id VARCHAR PRIMARY KEY,
                code VARCHAR,
                name VARCHAR
            );
        """
        )

        create_standard_indexes(
            in_memory_duckdb,
            "test_mixed_idx",
            unique_cols=["id"],
            index_cols=["code", "name"],
        )

        # Verify all indexes created
        indexes = in_memory_duckdb.execute(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE table_name = 'test_mixed_idx';
        """
        ).fetchall()

        index_names = [idx[0] for idx in indexes]
        assert len(index_names) >= 3  # At least 3 indexes

    def test_handles_empty_column_lists(self, in_memory_duckdb):
        """Test that function handles None/empty column lists gracefully."""
        in_memory_duckdb.execute("CREATE TABLE test_empty_idx (id INTEGER);")

        # Should not raise error with None values
        create_standard_indexes(
            in_memory_duckdb, "test_empty_idx", unique_cols=None, index_cols=None
        )

        # Should not raise error with empty lists
        create_standard_indexes(
            in_memory_duckdb, "test_empty_idx", unique_cols=[], index_cols=[]
        )
