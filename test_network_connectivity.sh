#!/bin/bash
# Test network connectivity for WECA Pipeline APIs
# Usage: ./test_network_connectivity.sh

set -e

echo "=========================================="
echo "WECA Pipeline Network Connectivity Test"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

test_endpoint() {
    local name="$1"
    local url="$2"
    local user_agent="${3:-Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36}"

    echo -n "Testing $name... "

    # Test with curl
    http_code=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "User-Agent: $user_agent" \
        -H "Accept: application/json" \
        --connect-timeout 10 \
        --max-time 30 \
        "$url" 2>/dev/null || echo "000")

    # Check result
    if [ "$http_code" = "200" ]; then
        echo -e "${GREEN}✅ OK (HTTP 200)${NC}"
        return 0
    elif [ "$http_code" = "403" ]; then
        echo -e "${RED}❌ BLOCKED (HTTP 403 Forbidden)${NC}"
        return 1
    elif [ "$http_code" = "401" ]; then
        echo -e "${YELLOW}⚠️  AUTH REQUIRED (HTTP 401 - expected for EPC)${NC}"
        return 0
    elif [ "$http_code" = "000" ]; then
        echo -e "${RED}❌ CONNECTION FAILED (timeout or network error)${NC}"
        return 1
    else
        echo -e "${YELLOW}⚠️  HTTP $http_code${NC}"
        return 1
    fi
}

# Track failures
failures=0

echo "1. ArcGIS REST API (ONS Geography)"
echo "-----------------------------------"
if ! test_endpoint "ArcGIS CA Boundaries" \
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/CAUTH_MAY_2025_EN_BGC/FeatureServer/0/query?where=1%3D1&outFields=*&f=json&resultRecordCount=1"; then
    ((failures++))
fi
echo ""

echo "2. UK Government Open Data"
echo "-----------------------------------"
if ! test_endpoint "GHG Emissions CSV" \
    "https://assets.publishing.service.gov.uk/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv"; then
    ((failures++))
fi
echo ""

echo "3. DFT Traffic Statistics (Google Cloud)"
echo "-----------------------------------"
if ! test_endpoint "DFT Traffic CSV" \
    "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv"; then
    ((failures++))
fi
echo ""

echo "4. R-universe (IMD 2025 Data)"
echo "-----------------------------------"
if ! test_endpoint "IMD 2025 CSV" \
    "https://humaniverse.r-universe.dev/IMD/data/imd2025_england_lsoa21_indicators/csv"; then
    ((failures++))
fi
echo ""

echo "5. EPC API (Optional - requires credentials)"
echo "-----------------------------------"
if ! test_endpoint "EPC Domestic API" \
    "https://epc.opendatacommunities.org/api/v1/domestic/search"; then
    # Don't count EPC as failure (requires auth)
    echo "   (This is expected without credentials)"
fi
echo ""

# Summary
echo "=========================================="
echo "Test Summary"
echo "=========================================="
if [ $failures -eq 0 ]; then
    echo -e "${GREEN}✅ All required endpoints accessible!${NC}"
    echo ""
    echo "The pipeline should work correctly in your environment."
    echo "You can now run:"
    echo "  PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample"
    exit 0
else
    echo -e "${RED}❌ $failures endpoint(s) failed${NC}"
    echo ""
    echo "Your network may be blocking some APIs."
    echo ""
    echo "Next steps:"
    echo "  1. Check firewall/proxy settings"
    echo "  2. Review docs/NETWORK_REQUIREMENTS.md"
    echo "  3. Contact your IT department if corporate network"
    echo ""
    echo "Note: Only DFT Traffic (Google Cloud) is strictly required"
    echo "      for testing. Other sources may work with local data."
    exit 1
fi
