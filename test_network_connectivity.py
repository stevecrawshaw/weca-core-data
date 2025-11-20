#!/usr/bin/env python3
"""
Test network connectivity for WECA Pipeline APIs

This script tests whether all required external APIs are accessible
from your network. Run this before attempting the full ETL pipeline.

Usage:
    python test_network_connectivity.py
"""

import sys
import time
import requests
from typing import Dict, Tuple

# ANSI color codes
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
RESET = '\033[0m'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

ENDPOINTS: Dict[str, Dict[str, str]] = {
    "ArcGIS CA Boundaries": {
        "url": "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/CAUTH_MAY_2025_EN_BGC/FeatureServer/0/query?where=1%3D1&outFields=*&f=json&resultRecordCount=1",
        "category": "ArcGIS REST API (ONS Geography)",
        "required": True,
    },
    "GHG Emissions CSV": {
        "url": "https://assets.publishing.service.gov.uk/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv",
        "category": "UK Government Open Data",
        "required": True,
    },
    "DFT Traffic CSV": {
        "url": "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv",
        "category": "DFT Traffic Statistics (Google Cloud)",
        "required": True,
    },
    "IMD 2025 CSV": {
        "url": "https://humaniverse.r-universe.dev/IMD/data/imd2025_england_lsoa21_indicators/csv",
        "category": "R-universe (IMD 2025 Data)",
        "required": True,
    },
    "EPC Domestic API": {
        "url": "https://epc.opendatacommunities.org/api/v1/domestic/search",
        "category": "EPC API (Optional - requires credentials)",
        "required": False,
    },
}


def test_endpoint(name: str, url: str, required: bool = True) -> Tuple[bool, str, float]:
    """
    Test connectivity to a single endpoint

    Args:
        name: Human-readable name of the endpoint
        url: URL to test
        required: Whether this endpoint is required for the pipeline

    Returns:
        Tuple of (success, status_message, response_time)
    """
    print(f"  Testing {name}...", end=" ", flush=True)

    start_time = time.time()

    try:
        response = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        elapsed = time.time() - start_time

        if response.status_code == 200:
            print(f"{GREEN}✅ OK{RESET} ({elapsed:.2f}s)")
            return True, f"HTTP 200", elapsed
        elif response.status_code == 403:
            print(f"{RED}❌ BLOCKED{RESET} (HTTP 403 Forbidden)")
            return False, "HTTP 403 Forbidden", elapsed
        elif response.status_code == 401:
            # Expected for EPC API without credentials
            if not required:
                print(f"{YELLOW}⚠️  AUTH REQUIRED{RESET} (HTTP 401 - expected)")
                return True, "HTTP 401 (expected)", elapsed
            else:
                print(f"{YELLOW}⚠️  HTTP 401{RESET}")
                return False, "HTTP 401", elapsed
        else:
            print(f"{YELLOW}⚠️  HTTP {response.status_code}{RESET}")
            return False, f"HTTP {response.status_code}", elapsed

    except requests.exceptions.Timeout:
        elapsed = time.time() - start_time
        print(f"{RED}❌ TIMEOUT{RESET} (>{elapsed:.0f}s)")
        return False, "Connection timeout", elapsed

    except requests.exceptions.ConnectionError as e:
        elapsed = time.time() - start_time
        print(f"{RED}❌ CONNECTION FAILED{RESET}")
        return False, f"Connection error: {str(e)[:50]}", elapsed

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"{RED}❌ ERROR{RESET}: {str(e)[:50]}")
        return False, f"Error: {str(e)[:50]}", elapsed


def main():
    """Run connectivity tests for all endpoints"""
    print("=" * 60)
    print("WECA Pipeline Network Connectivity Test")
    print("=" * 60)
    print()

    results = {}
    failures = 0
    current_category = None

    for name, config in ENDPOINTS.items():
        # Print category header
        if config["category"] != current_category:
            current_category = config["category"]
            print(f"\n{current_category}")
            print("-" * 60)

        # Test endpoint
        success, message, elapsed = test_endpoint(
            name, config["url"], config["required"]
        )

        results[name] = {
            "success": success,
            "message": message,
            "elapsed": elapsed,
            "required": config["required"],
        }

        # Count failures (only for required endpoints)
        if not success and config["required"]:
            failures += 1

    # Print summary
    print()
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)

    total_required = sum(1 for r in results.values() if r["required"])
    passed_required = sum(
        1 for r in results.values() if r["required"] and r["success"]
    )

    if failures == 0:
        print(f"{GREEN}✅ All {total_required} required endpoints accessible!{RESET}")
        print()
        print("The pipeline should work correctly in your environment.")
        print("You can now run:")
        print("  PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample")
        return 0
    else:
        print(f"{RED}❌ {failures}/{total_required} required endpoint(s) failed{RESET}")
        print()
        print("Failed endpoints:")
        for name, result in results.items():
            if not result["success"] and result["required"]:
                print(f"  • {name}: {result['message']}")

        print()
        print("Your network may be blocking some APIs.")
        print()
        print("Next steps:")
        print("  1. Check firewall/proxy settings")
        print("  2. Review docs/NETWORK_REQUIREMENTS.md")
        print("  3. Contact your IT department if on corporate network")
        print()

        # Check if at least DFT works
        if results["DFT Traffic CSV"]["success"]:
            print(f"{YELLOW}Note:{RESET} DFT Traffic (Google Cloud) is accessible.")
            print("      You can test the pipeline with --sample mode,")
            print("      though some data sources will be unavailable.")

        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print(f"\n\n{YELLOW}⚠️  Test interrupted by user{RESET}")
        sys.exit(130)
