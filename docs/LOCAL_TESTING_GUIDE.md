# Local Testing Guide: WECA Core Data Hybrid Pipeline

This guide explains how to test the hybrid ETL pipeline in your local development environment.

## Prerequisites

### 1. System Requirements

- **Python:** 3.12 or higher
- **uv:** Package manager (recommended) or pip
- **Git:** For version control
- **Network:** Unrestricted internet access to:
  - `services1.arcgis.com` (ArcGIS REST APIs)
  - `assets.publishing.service.gov.uk` (UK Gov GHG emissions)
  - `humaniverse.r-universe.dev` (IMD 2025 data)
  - `storage.googleapis.com` (DFT traffic data)
  - `epc.opendatacommunities.org` (EPC certificates - requires API key)

### 2. Install Dependencies

#### Using uv (Recommended)

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone <your-repo-url>
cd weca-core-data

# Install dependencies
uv sync
```

#### Using pip

```bash
pip install -e .
```

### 3. Configure Secrets (Optional - for EPC data)

If you want to extract EPC data, you'll need API credentials:

```bash
# Copy the example secrets file
cp .dlt/secrets.toml.example .dlt/secrets.toml

# Edit with your credentials
nano .dlt/secrets.toml
```

Add your EPC API credentials:

```toml
[sources.epc]
# Get your credentials from: https://epc.opendatacommunities.org/
auth_token = "your_base64_encoded_token_here"
```

To generate the token:

```python
import base64
email = "your.email@example.com"
api_key = "your_api_key_from_epc_portal"
token = base64.b64encode(f"{email}:{api_key}".encode()).decode()
print(token)
```

---

## Testing Options

### Option 1: Sample Mode (Recommended for First Test)

Test the pipeline with limited data (1,000 records per source):

```bash
# Set PYTHONPATH and run in sample mode
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample
```

**What happens:**
- Extracts only 1,000 records per CSV source
- Full extraction for ArcGIS sources (geometry data)
- Skips EPC data (unless `download_epc=True`)
- Creates database: `data/ca_epc.duckdb`
- Outputs logs to console and `etl.log`

**Expected runtime:** 5-10 minutes

### Option 2: Full Pipeline (Production Mode)

Run the complete ETL with all data:

```bash
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --full
```

**What happens:**
- Extracts all data from all sources
- Full GHG emissions dataset (~559,215 records)
- Full IMD 2025 dataset (~33,755 LSOAs)
- Full ArcGIS geographies
- Skips EPC (requires API key)

**Expected runtime:** 15-30 minutes (depending on network speed)

### Option 3: With EPC Data

To include EPC certificate data:

1. Edit `pipelines/orchestrate_etl.py` line 362:
   ```python
   download_epc=True,  # Enable EPC extraction
   ```

2. Run the pipeline:
   ```bash
   PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample
   ```

**Note:** EPC extraction can be slow (API rate limits). Sample mode is recommended for testing.

---

## Monitoring Progress

### Console Output

The pipeline provides real-time progress updates:

```
================================================================================
WECA CORE DATA ETL - HYBRID APPROACH
⚠️  SAMPLE MODE: Limited to 1,000 records per source
================================================================================

================================================================================
STAGE 1: EXTRACT
================================================================================

[1/5] Extracting ArcGIS geographical data...
[OK] ArcGIS data extracted

[2/5] Extracting Combined Authority boundaries...
[OK] CA boundaries extracted

[3/5] Extracting DFT traffic data...
[OK] DFT traffic data extracted

[4/5] Extracting GHG emissions data...
[OK] GHG emissions data extracted

[5/5] Extracting IMD 2025 data...
[OK] IMD 2025 data extracted

================================================================================
STAGE 2: TRANSFORM
================================================================================

[1/6] Transforming CA/LA lookup data...
[OK] CA/LA lookup: 11 records

[2/6] Transforming LSOA population-weighted centroids...
[OK] LSOA PWC: 2,656 records

[3/6] Transforming GHG emissions data...
[OK] GHG emissions: 1,000 records

...

================================================================================
ETL PIPELINE COMPLETE
================================================================================
```

### Log File

Detailed logs are written to `etl.log`:

```bash
# Watch logs in real-time
tail -f etl.log
```

---

## Verifying Results

### 1. Check Database File

```bash
ls -lh data/ca_epc.duckdb
```

Expected size:
- **Sample mode:** ~50-100 MB
- **Full pipeline:** ~500 MB - 1 GB

### 2. Inspect Database Contents

```bash
# Launch DuckDB CLI
uv run duckdb data/ca_epc.duckdb
```

```sql
-- List all schemas
SHOW SCHEMAS;

-- Expected: raw_data, transformed_data

-- List tables in raw_data
SHOW TABLES FROM raw_data;

-- Expected tables:
-- - lsoa_2021_boundaries
-- - lsoa_2021_pwc
-- - lsoa_2021_lookups
-- - ca_boundaries_2025
-- - dft_traffic
-- - ghg_emissions
-- - imd_2025

-- List tables in transformed_data
SHOW TABLES FROM transformed_data;

-- Expected tables:
-- - ca_la_lookup
-- - lsoa_2021_pwc
-- - ghg_emissions
-- - dft_la_lookup
-- - imd_2025

-- Check record counts
SELECT 'ca_la_lookup' as table_name, COUNT(*) as count FROM transformed_data.ca_la_lookup
UNION ALL
SELECT 'lsoa_2021_pwc', COUNT(*) FROM transformed_data.lsoa_2021_pwc
UNION ALL
SELECT 'ghg_emissions', COUNT(*) FROM transformed_data.ghg_emissions
UNION ALL
SELECT 'imd_2025', COUNT(*) FROM transformed_data.imd_2025;
```

### 3. Verify Spatial Functionality

```sql
-- Check spatial extension is loaded
SELECT * FROM duckdb_extensions() WHERE extension_name = 'spatial';

-- Verify geometry column exists
DESCRIBE transformed_data.lsoa_2021_pwc;

-- Should include: geom GEOMETRY

-- Test spatial query
SELECT
    lsoa21cd,
    ST_AsText(geom) as geometry,
    x, y
FROM transformed_data.lsoa_2021_pwc
LIMIT 5;
```

---

## Troubleshooting

### Issue: Network Errors (403 Forbidden)

**Symptom:**
```
requests.exceptions.HTTPError: 403 Client Error: Forbidden
```

**Cause:** Network restrictions or firewall blocking API access

**Solutions:**
1. **Check internet connection:** Ensure you can access the URLs directly in a browser
2. **VPN/Proxy:** Try disabling VPN or corporate proxy
3. **Firewall:** Check firewall settings allow outbound HTTPS
4. **User-Agent:** Headers are already included in the code

### Issue: Memory Errors

**Symptom:**
```
MemoryError: Unable to allocate...
```

**Cause:** Large datasets exceeding available RAM

**Solutions:**
1. Use **sample mode** first: `--sample`
2. Increase system swap space
3. Close other applications
4. Consider chunked processing (future enhancement)

### Issue: Module Import Errors

**Symptom:**
```
ModuleNotFoundError: No module named 'sources'
```

**Cause:** `PYTHONPATH` not set correctly

**Solutions:**
```bash
# Always run with PYTHONPATH set
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample

# Or add to your shell profile
export PYTHONPATH="${PYTHONPATH}:/path/to/weca-core-data"
```

### Issue: DuckDB Already Open

**Symptom:**
```
duckdb.IOException: Could not set lock on file
```

**Cause:** Database file is open in another process

**Solutions:**
1. Close any DuckDB CLI sessions
2. Kill processes: `lsof data/ca_epc.duckdb`
3. Delete lock file: `rm data/ca_epc.duckdb.wal`

### Issue: Slow EPC Extraction

**Symptom:** EPC extraction takes hours

**Cause:** API rate limiting (by design)

**Solutions:**
1. Use **sample mode** for testing
2. Run overnight for full extraction
3. Consider caching/incremental updates

---

## Performance Expectations

### Sample Mode

| Stage | Duration | Notes |
|-------|----------|-------|
| **Extract** | 2-5 min | Network speed dependent |
| **Transform** | 1-2 min | CPU bound |
| **Load** | 30 sec | Spatial operations |
| **Total** | **3-8 min** | |

### Full Mode (No EPC)

| Stage | Duration | Notes |
|-------|----------|-------|
| **Extract** | 10-15 min | Large CSV downloads |
| **Transform** | 3-5 min | Processing ~600K records |
| **Load** | 1-2 min | Spatial indexing |
| **Total** | **15-25 min** | |

### Full Mode (With EPC)

| Stage | Duration | Notes |
|-------|----------|-------|
| **Extract** | 30-60 min | API rate limited |
| **Transform** | 5-10 min | Large dataset |
| **Load** | 2-3 min | |
| **Total** | **40-75 min** | |

---

## Testing Individual Sources

If you encounter issues, test sources individually:

```bash
PYTHONPATH=. uv run python test_sources_individually.py
```

This script tests each source separately with 100 records:
- ✅ If all pass: Full pipeline should work
- ❌ If any fail: Investigate specific source

---

## Comparing with Legacy Implementation

To validate the hybrid approach produces equivalent output:

### 1. Run Legacy Pipeline

```bash
python cesap-epc-load-duckdb-data.py
```

Creates: `data/ca_epc_legacy.duckdb`

### 2. Run Hybrid Pipeline

```bash
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --full
```

Creates: `data/ca_epc.duckdb`

### 3. Compare Record Counts

```python
import duckdb

# Legacy database
con_legacy = duckdb.connect("data/ca_epc_legacy.duckdb")
legacy_counts = con_legacy.execute("""
    SELECT 'lsoa_2021_pwc' as table, COUNT(*) as count
    FROM lsoa_2021_pwc_tbl
""").fetchall()
con_legacy.close()

# Hybrid database
con_hybrid = duckdb.connect("data/ca_epc.duckdb")
hybrid_counts = con_hybrid.execute("""
    SELECT 'lsoa_2021_pwc' as table, COUNT(*) as count
    FROM transformed_data.lsoa_2021_pwc
""").fetchall()
con_hybrid.close()

print(f"Legacy: {legacy_counts}")
print(f"Hybrid: {hybrid_counts}")
```

---

## Next Steps After Successful Test

1. ✅ **Verify data quality** - Spot-check sample records
2. ✅ **Test transformations** - Ensure calculations are correct
3. ✅ **Test spatial queries** - Verify geometry operations work
4. ✅ **Run full pipeline** - Extract complete datasets
5. ✅ **Performance benchmark** - Compare with legacy implementation
6. ✅ **Document any issues** - Report bugs or unexpected behavior

---

## Getting Help

### Issues with the Pipeline

1. Check `etl.log` for detailed error messages
2. Review [HYBRID_IMPLEMENTATION_PLAN.md](../HYBRID_IMPLEMENTATION_PLAN.md) for known issues
3. Open a GitHub issue with:
   - Error message
   - Log output
   - System details (OS, Python version)

### Network/API Issues

1. **ArcGIS APIs:** Check https://services1.arcgis.com status
2. **UK Gov APIs:** Check https://www.gov.uk/service-manual/technology/monitoring-the-status-of-your-service
3. **EPC API:** Contact opendatacommunities.org support

---

## Appendix: Command Reference

```bash
# Sample mode (recommended first test)
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample

# Full mode
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --full

# Test individual sources
PYTHONPATH=. uv run python test_sources_individually.py

# Inspect database
uv run duckdb data/ca_epc.duckdb

# Watch logs
tail -f etl.log

# Check Python environment
uv run python --version
uv run python -c "import dlt; print(dlt.__version__)"
uv run python -c "import polars; print(polars.__version__)"

# Clean up test data
rm -rf data/ca_epc.duckdb
rm -rf .dlt/*.duckdb
rm -f etl.log
```

---

**Document Version:** 1.0
**Last Updated:** 2025-11-20
**Tested On:** Ubuntu 22.04, macOS 14, Windows 11
