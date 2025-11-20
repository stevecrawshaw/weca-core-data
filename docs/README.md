# Documentation Index

Documentation for the WECA Core Data Hybrid ETL Pipeline.

## Quick Links

### Getting Started

- **[QUICKSTART.md](QUICKSTART.md)** - Get running in 5 minutes
  - Installation
  - First test run
  - Verification steps

### Testing & Deployment

- **[LOCAL_TESTING_GUIDE.md](LOCAL_TESTING_GUIDE.md)** - Comprehensive testing guide
  - Prerequisites
  - Testing options (sample vs full mode)
  - Monitoring progress
  - Verifying results
  - Troubleshooting
  - Performance expectations

- **[NETWORK_REQUIREMENTS.md](NETWORK_REQUIREMENTS.md)** - Network connectivity details
  - Required external APIs
  - Firewall configuration
  - Proxy setup
  - SSL/TLS requirements
  - Bandwidth requirements
  - Connectivity testing

### Architecture & Planning

- **[../HYBRID_IMPLEMENTATION_PLAN.md](../HYBRID_IMPLEMENTATION_PLAN.md)** - Project roadmap
  - Phase-by-phase implementation plan
  - Progress tracking (currently 75% complete)
  - Architecture decisions
  - Known issues and resolutions

## Testing Scripts

Located in project root:

- `test_network_connectivity.sh` - Bash script to test API access
- `test_network_connectivity.py` - Python script to test API access
- `test_sources_individually.py` - Test each data source separately

## Directory Structure

```
docs/
├── README.md                    # This file
├── QUICKSTART.md               # 5-minute quick start
├── LOCAL_TESTING_GUIDE.md      # Comprehensive testing guide
└── NETWORK_REQUIREMENTS.md     # Network connectivity details

../
├── HYBRID_IMPLEMENTATION_PLAN.md  # Project roadmap (main doc)
├── test_network_connectivity.sh   # Network test (bash)
├── test_network_connectivity.py   # Network test (python)
└── test_sources_individually.py   # Individual source tests
```

## Common Tasks

### First-Time Setup

```bash
# 1. Install dependencies
uv sync

# 2. Test network connectivity
./test_network_connectivity.sh

# 3. Run sample pipeline
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample
```

See [QUICKSTART.md](QUICKSTART.md) for details.

### Running the Pipeline

```bash
# Sample mode (recommended first test)
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample

# Full mode (all data)
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --full
```

See [LOCAL_TESTING_GUIDE.md](LOCAL_TESTING_GUIDE.md) for all options.

### Troubleshooting Network Issues

```bash
# Quick test
curl -I https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv

# Detailed test (bash)
./test_network_connectivity.sh

# Detailed test (python)
python test_network_connectivity.py
```

See [NETWORK_REQUIREMENTS.md](NETWORK_REQUIREMENTS.md) for details.

## Getting Help

### Documentation Issues

If the documentation is unclear or missing information:

1. Check [HYBRID_IMPLEMENTATION_PLAN.md](../HYBRID_IMPLEMENTATION_PLAN.md) for architectural details
2. Review code comments in `sources/`, `transformers/`, `loaders/`
3. Open a GitHub issue with the "documentation" label

### Pipeline Issues

If you encounter errors running the pipeline:

1. Check `etl.log` for detailed error messages
2. Review [LOCAL_TESTING_GUIDE.md](LOCAL_TESTING_GUIDE.md) troubleshooting section
3. Test network connectivity with the test scripts
4. Open a GitHub issue with:
   - Error message
   - Log excerpt
   - System details (OS, Python version)
   - Output of `./test_network_connectivity.sh`

### Network/Firewall Issues

If APIs are blocked (403 Forbidden):

1. Review [NETWORK_REQUIREMENTS.md](NETWORK_REQUIREMENTS.md)
2. Run `./test_network_connectivity.sh` to identify blocked endpoints
3. Contact your IT/network team with the requirements document
4. Consider running in a less restricted environment (local dev machine, cloud VM)

## Project Status

**Current Phase:** Phase 3 - Integration & Testing (75% complete)

**Last Updated:** 2025-11-20

**Branch:** `claude/unblock-pipeline-01AeSUXbaJhV2Cy3jSLykJJz`

See [../HYBRID_IMPLEMENTATION_PLAN.md](../HYBRID_IMPLEMENTATION_PLAN.md) for detailed progress.

---

**Document Version:** 1.0
**Maintained By:** WECA Data Team
