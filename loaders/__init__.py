"""
Loaders module for DuckDB spatial operations and view creation.

This module contains functions for:
- Spatial extension setup
- Geometry column creation
- Spatial indexing
- Analytical view creation
"""

from .spatial_setup import (
    setup_spatial_extension,
    add_geometry_column,
    add_geometry_column_from_wkt,
    create_spatial_indexes,
    create_standard_indexes,
)
from .create_views import (
    create_simple_geog_lookup_view,
    create_ghg_emissions_view,
    create_epc_domestic_view,
    create_epc_domestic_ods_view,
    create_epc_non_domestic_view,
    create_all_views,
)

__all__ = [
    # Spatial operations
    "setup_spatial_extension",
    "add_geometry_column",
    "add_geometry_column_from_wkt",
    "create_spatial_indexes",
    "create_standard_indexes",
    # View creation
    "create_simple_geog_lookup_view",
    "create_ghg_emissions_view",
    "create_epc_domestic_view",
    "create_epc_domestic_ods_view",
    "create_epc_non_domestic_view",
    "create_all_views",
]
