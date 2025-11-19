# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an ETL (Extract, Transform, Load) codebase for the West of England Combined Authority (WECA) Core Data project. It extracts data from various web resources, including spatial data and environmental data, and loads it into a DuckDB database to support environmental assessments at a regional scale.

The code is written in Python and requires comprehensive refactoring to follow best practices (as noted by the developer).

## Key Technologies

- **Database**: DuckDB (with spatial extension)
- **Data Processing**: Polars (primary), Pandas, GeoPandas
- **Data Sources**: EPC (Energy Performance Certificates), ONS geographies, ArcGIS REST APIs, NOMIS, DFT traffic data
- **Logging**: Standard Python logging to console and `etl.log`

## Essential Commands

### Linting and Formatting

PostToolUse hook is set up for Ruff linting and formatting.

Note: Ruff is configured in `ruff.toml` with:
- Line length: 88 characters 
- Target: Python 3.13
- Enabled rules: pycodestyle (E, W), pyflakes (F), pyupgrade (UP), flake8-bandit (S), flake8-bugbear (B), flake8-simplify (SIM), isort (I)

### Type Checking
```bash
mypy .
```

### Running the Main ETL Process

The primary ETL script is `cesap-epc-load-duckdb-data.py`, which is designed to be run as a Jupyter-style script (cells marked with `# %%`). It can be executed directly or in an IDE that supports cell execution.

```bash
python cesap-epc-load-duckdb-data.py
```

## Architecture

### Core Components

1. **get_ca_data.py** - Main utility module containing 30+ functions for:
   - Downloading data from ArcGIS FeatureServer APIs (`make_esri_fs_url`, `get_gis_data`)
   - Fetching EPC bulk data (`dl_bulk_epc_zip`, `get_epc_pldf`)
   - Processing geographical lookups (`get_ca_la_df`, `make_lsoa_pwc_df`)
   - Downloading from NOMIS, DFT, and other APIs
   - URL validation (`validate_urls`)
   - General utilities (directory creation, file deletion, string cleaning)

2. **cesap-epc-load-duckdb-data.py** - Main ETL orchestration script:
   - Sets up directory structure in `data/` folder
   - Validates all source URLs
   - Downloads and processes:
     - Combined Authority boundaries
     - LSOA (Lower Layer Super Output Area) geographies (2011 and 2021)
     - EPC certificates (domestic and non-domestic)
     - IMD (Index of Multiple Deprivation) data
     - GHG emissions data
     - Postcode lookups
   - Creates DuckDB tables with spatial indexing
   - Uses transactions for database operations

3. **build_tables_queries.py** - SQL DDL statements for DuckDB:
   - Extension loading (spatial)
   - Table creation from parquet files
   - Index creation
   - View definitions for EPC data

4. **epc_schema.py** - Schema definitions:
   - `cols_schema_domestic` - Domestic EPC schema (VARCHAR, INTEGER, DATE, DECIMAL types)
   - `nondom_polars_schema` - Non-domestic EPC schema
   - `all_cols_polars` - Polars-compatible schema dictionary

5. **update_epc.py** - Script for updating EPC data incrementally


### Data Flow

```
Web APIs (ArcGIS, EPC ODC, ONS, NOMIS, DFT)
    ↓
get_ca_data.py functions (download & transform to Polars DataFrames)
    ↓
Parquet files in data/ directory
    ↓
build_tables_queries.py (SQL DDL via DuckDB)
    ↓
ca_epc.duckdb (final database with spatial indexes)
```

### DuckDB Database Structure

All table creation happens within a transaction block. Key tables include:

- `lsoa_2021_pwc_tbl` - LSOA 2021 population-weighted centroids with geometry
- `lsoa_2021_lookup_tbl` - Lookups between LSOA21/ward/LAD codes
- `lsoa_2011_lookup_tbl` - Lookups for 2011 geographies
- `imd_lsoa_tbl` - IMD scores by LSOA
- `epc_domestic_table` - Domestic EPC certificates
- `epc_nondomestic_table` - Non-domestic EPC certificates

All geographic tables include spatial indexes and are queryable with DuckDB's spatial extension.

### Data Sources Configuration

URLs for data sources are defined in `cesap-epc-load-duckdb-data.py` in the `url_dict` dictionary:
- ArcGIS REST services for ONS boundaries and lookups
- EPC API at opendatacommunities.org (requires credentials)
- NOMIS API for employment data
- DFT traffic statistics
- mySociety UK local authority codes

### Directory Structure

```
data/                    # All downloaded and processed data
  ├── epc_bulk_zips/            # Domestic EPC bulk downloads
  ├── epc_bulk_nondom_zips/     # Non-domestic EPC downloads
  ├── postcode_centroids/       # Postcode geography data
  └── db_export/                # Database exports
plots/                   # Output visualizations (referenced but not in Python code)
agent-docs/              # Documentation
```

### Logging

All operations in `get_ca_data.py` log to:
- Console (StreamHandler)
- `etl.log` file (FileHandler)
- Level: INFO

## Important Notes

### Current Limitations

- The codebase lacks modularization; functions are all in one large module (`get_ca_data.py`)
- No classes or OOP structure; procedural style
- No test suite currently exists (no pytest or unittest found)
- Download flags in `cesap-epc-load-duckdb-data.py` control whether to re-download data:
  - `download_epc`
  - `download_lsoa`
  - `download_postcodes`
- EPC data requires authentication credentials (not stored in code)
- The codebase uses primarily Polars for data manipulation, with some Pandas/GeoPandas for specific operations
- All spatial operations rely on DuckDB's spatial extension
- The project follows a "download once, process many times" pattern with parquet intermediates

### Guidance

- Code guidelines in @agent-docs/python-code-guidelines,md
- CLI tools for refactoring in @agent-docs/cli-tools-memory.md
