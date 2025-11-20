# Integration Tests

This directory contains integration tests that require network access to external APIs and resources.

## Files

- `test_arcgis_sources.py` - Tests for ArcGIS REST API sources
- `test_epc_sources.py` - Tests for EPC data sources
- `test_other_sources.py` - Tests for DFT, GHG, and IMD sources
- `test_transformers_geography.py` - Integration tests for geography transformers

## Running Integration Tests

These tests require:
- Unrestricted network access to external APIs
- Valid EPC credentials in `.dlt/secrets.toml` (for EPC tests)
- Longer timeout settings (some tests take 30+ minutes)

To run integration tests:

```bash
# Run all integration tests
PYTHONPATH=. uv run pytest tests/integration/ -v

# Run specific integration test file
PYTHONPATH=. uv run pytest tests/integration/test_other_sources.py -v
```

## Note for Restricted Environments

These tests will **FAIL** in network-restricted environments (like Claude Code web).
Use the unit tests in `tests/` instead, which use mock data and don't require network access.

For testing in restricted environments, use:
```bash
# Run only unit tests (no network required)
PYTHONPATH=. uv run pytest tests/ -v --ignore=tests/integration/
```
