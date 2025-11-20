# Quick Start Guide: Hybrid ETL Pipeline

Get the WECA Core Data pipeline running in 5 minutes.

## Prerequisites

- Python 3.12+
- Unrestricted internet access
- 2 GB free disk space

## Installation

```bash
# Clone the repo
git clone <your-repo-url>
cd weca-core-data

# Install dependencies with uv
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

## First Test Run (Sample Mode)

Test with limited data (~1,000 records per source):

```bash
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample
```

**Expected output:**
```
================================================================================
WECA CORE DATA ETL - HYBRID APPROACH
⚠️  SAMPLE MODE: Limited to 1,000 records per source
================================================================================

[1/5] Extracting ArcGIS geographical data...
[OK] ArcGIS data extracted

[2/5] Extracting Combined Authority boundaries...
[OK] CA boundaries extracted

...

ETL PIPELINE COMPLETE
```

**Runtime:** 5-10 minutes

## Verify Success

```bash
# Check database was created
ls -lh data/ca_epc.duckdb

# Inspect contents
uv run duckdb data/ca_epc.duckdb -c "SHOW TABLES FROM transformed_data;"
```

Expected tables:
- `ca_la_lookup`
- `lsoa_2021_pwc`
- `ghg_emissions`
- `dft_la_lookup`
- `imd_2025`

## Next Steps

✅ **Sample mode works?** → Run full pipeline:
```bash
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --full
```

❌ **Got errors?** → See [Troubleshooting](#troubleshooting)

## Troubleshooting

### Network Errors (403 Forbidden)

Your network may be blocking external APIs. Test connectivity:

```bash
# Quick test
curl -I https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv
```

Expected: `HTTP/2 200`

If you get `403 Forbidden`, see [NETWORK_REQUIREMENTS.md](NETWORK_REQUIREMENTS.md)

### Module Not Found

Ensure `PYTHONPATH` is set:

```bash
# Always include PYTHONPATH=.
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample
```

### Memory Errors

Use sample mode first, then increase swap space for full runs.

## Full Documentation

- **[Local Testing Guide](LOCAL_TESTING_GUIDE.md)** - Comprehensive testing instructions
- **[Network Requirements](NETWORK_REQUIREMENTS.md)** - Firewall and connectivity details
- **[HYBRID_IMPLEMENTATION_PLAN.md](../HYBRID_IMPLEMENTATION_PLAN.md)** - Project roadmap and progress

## Command Reference

```bash
# Sample mode (1,000 records)
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample

# Full mode (all data)
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --full

# Test individual sources
PYTHONPATH=. uv run python test_sources_individually.py

# Check logs
tail -f etl.log
```

---

**Need help?** See [LOCAL_TESTING_GUIDE.md](LOCAL_TESTING_GUIDE.md) for detailed instructions.
