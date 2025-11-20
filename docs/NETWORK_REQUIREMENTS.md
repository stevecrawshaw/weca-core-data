# Network Requirements for WECA Core Data Pipeline

This document outlines the network connectivity requirements for running the hybrid ETL pipeline.

## Required External APIs

The pipeline requires outbound HTTPS access to the following endpoints:

### 1. ArcGIS REST APIs (ONS Geography)

**Host:** `services1.arcgis.com`

**Endpoints:**
- `/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query`
- `/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/FeatureServer/0/query`
- `/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query`
- `/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA21_WD24_LAD24_EW_LU/FeatureServer/0/query`
- `/ESMARspQHYMw9BZ9/arcgis/rest/services/CAUTH_MAY_2025_EN_BGC/FeatureServer/0/query`

**Purpose:** Extract LSOA boundaries, lookups, and Combined Authority boundaries

**Data Size:** ~50-100 MB per endpoint

**Required Headers:**
- `User-Agent: Mozilla/5.0...` (prevents bot detection)
- `Accept: application/json`
- `Referer: https://geoportal.statistics.gov.uk/`

**Rate Limiting:** Pagination at 2,000 records per request

---

### 2. UK Government Open Data (GHG Emissions)

**Host:** `assets.publishing.service.gov.uk`

**Endpoint:**
- `/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv`

**Purpose:** Extract greenhouse gas emissions by local authority

**Data Size:** ~50 MB (559,215 records)

**Format:** CSV

**Update Frequency:** Annually

---

### 3. DFT Traffic Statistics

**Host:** `storage.googleapis.com`

**Endpoint:**
- `/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv`

**Purpose:** Extract Department for Transport traffic statistics

**Data Size:** ~10 MB

**Format:** CSV

**Note:** This is hosted on Google Cloud Storage, often accessible even in restricted environments

---

### 4. IMD 2025 Data (R-universe)

**Host:** `humaniverse.r-universe.dev`

**Endpoint:**
- `/IMD/data/imd2025_england_lsoa21_indicators/csv`

**Purpose:** Extract Index of Multiple Deprivation 2025 indicators

**Data Size:** ~5 MB (33,755 LSOAs)

**Format:** CSV

**Required Headers:**
- `User-Agent: Mozilla/5.0...` (R-universe blocks default user agents)
- `Accept: text/csv`

---

### 5. EPC API (Optional - Requires Credentials)

**Host:** `epc.opendatacommunities.org`

**Endpoint:**
- `/api/v1/domestic/search`
- `/api/v1/non-domestic/search`

**Purpose:** Extract Energy Performance Certificates

**Authentication:** Basic Auth (base64 encoded email:api_key)

**Rate Limiting:** Moderate (recommended: sequential requests per LA)

**Data Size:** Variable (depends on date range and LAs)

**Format:** CSV

**Note:** Requires free API key from https://epc.opendatacommunities.org/

---

## Firewall Configuration

### Outbound Rules Required

Allow HTTPS (port 443) to:
```
services1.arcgis.com
assets.publishing.service.gov.uk
storage.googleapis.com
humaniverse.r-universe.dev
epc.opendatacommunities.org  # Optional
```

### IP Ranges (if static IPs required)

Most services use CDNs with dynamic IPs. If your firewall requires static IPs:

- **Google Cloud Storage:** See [Google Cloud IP ranges](https://www.gstatic.com/ipranges/cloud.json)
- **UK Gov APIs:** Contact GDS for CDN IP ranges
- **ArcGIS:** Esri uses Akamai CDN - dynamic IPs

**Recommendation:** Use domain-based firewall rules, not IP-based

---

## Proxy Configuration

If your network requires an HTTP/HTTPS proxy:

### Environment Variables

```bash
export HTTP_PROXY="http://proxy.example.com:8080"
export HTTPS_PROXY="http://proxy.example.com:8080"
export NO_PROXY="localhost,127.0.0.1"
```

### Python Requests

The pipeline uses `requests` and `polars`, which automatically respect proxy environment variables.

To configure explicitly in code (if needed):

```python
import os
os.environ['HTTP_PROXY'] = 'http://proxy.example.com:8080'
os.environ['HTTPS_PROXY'] = 'http://proxy.example.com:8080'
```

---

## SSL/TLS Requirements

### Minimum TLS Version

- **Required:** TLS 1.2 or higher
- **Recommended:** TLS 1.3

### Certificate Validation

The pipeline validates SSL certificates by default. If using a corporate proxy with SSL inspection:

```python
# NOT RECOMMENDED - only use in development
import requests
requests.packages.urllib3.disable_warnings()
```

**Better approach:** Install corporate root CA certificate:

```bash
# Ubuntu/Debian
sudo cp corporate-ca.crt /usr/local/share/ca-certificates/
sudo update-ca-certificates

# macOS
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain corporate-ca.crt
```

---

## Bandwidth Requirements

### Peak Bandwidth

| Operation | Bandwidth | Duration |
|-----------|-----------|----------|
| **Sample Mode** | ~5 Mbps | 2-5 min |
| **Full Pipeline** | ~10 Mbps | 15-25 min |
| **With EPC** | ~2 Mbps | 30-60 min (rate limited) |

### Total Data Transfer

| Mode | Download | Upload | Total |
|------|----------|--------|-------|
| **Sample Mode** | ~10 MB | Negligible | ~10 MB |
| **Full Pipeline** | ~150 MB | Negligible | ~150 MB |
| **With EPC** | ~500 MB | Negligible | ~500 MB |

---

## Testing Network Connectivity

Use this script to test connectivity before running the pipeline:

```bash
#!/bin/bash
# test_network_connectivity.sh

echo "Testing WECA Pipeline Network Connectivity"
echo "==========================================="
echo ""

# Test ArcGIS
echo "1. Testing ArcGIS API..."
curl -s -o /dev/null -w "HTTP %{http_code} - %{time_total}s\n" \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/CAUTH_MAY_2025_EN_BGC/FeatureServer/0/query?where=1%3D1&outFields=*&f=json&resultRecordCount=1"

# Test GHG Emissions
echo "2. Testing UK Gov GHG API..."
curl -s -o /dev/null -w "HTTP %{http_code} - %{time_total}s\n" \
  --head "https://assets.publishing.service.gov.uk/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv"

# Test DFT Traffic
echo "3. Testing DFT Traffic API..."
curl -s -o /dev/null -w "HTTP %{http_code} - %{time_total}s\n" \
  --head "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv"

# Test IMD 2025
echo "4. Testing R-universe IMD API..."
curl -s -o /dev/null -w "HTTP %{http_code} - %{time_total}s\n" \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  --head "https://humaniverse.r-universe.dev/IMD/data/imd2025_england_lsoa21_indicators/csv"

# Test EPC (optional)
echo "5. Testing EPC API (optional - requires auth)..."
curl -s -o /dev/null -w "HTTP %{http_code} - %{time_total}s\n" \
  --head "https://epc.opendatacommunities.org/api/v1/domestic/search"

echo ""
echo "Expected results:"
echo "  - HTTP 200: Success"
echo "  - HTTP 403: Firewall/network restriction"
echo "  - HTTP 401: Authentication required (EPC only)"
echo "  - HTTP 404: Endpoint not found"
echo "  - Connection timeout: Network blocked"
```

Run the test:

```bash
chmod +x test_network_connectivity.sh
./test_network_connectivity.sh
```

---

## Python Connectivity Test

Alternatively, test from Python:

```python
#!/usr/bin/env python3
"""Test network connectivity for WECA pipeline"""

import requests
from urllib.parse import urlencode

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

ENDPOINTS = {
    "ArcGIS CA Boundaries": "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/CAUTH_MAY_2025_EN_BGC/FeatureServer/0/query?where=1%3D1&outFields=*&f=json&resultRecordCount=1",
    "GHG Emissions": "https://assets.publishing.service.gov.uk/media/68653c7ee6c3cc924228943f/2005-23-uk-local-authority-ghg-emissions-CSV-dataset.csv",
    "DFT Traffic": "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv",
    "IMD 2025": "https://humaniverse.r-universe.dev/IMD/data/imd2025_england_lsoa21_indicators/csv",
}

print("Testing WECA Pipeline Network Connectivity")
print("=" * 60)

for name, url in ENDPOINTS.items():
    try:
        response = requests.head(url, headers=HEADERS, timeout=10)
        status = "✅ OK" if response.status_code == 200 else f"⚠️ HTTP {response.status_code}"
        print(f"{name:25} {status}")
    except requests.exceptions.RequestException as e:
        print(f"{name:25} ❌ FAILED: {e}")

print("\nIf all tests pass, the pipeline should work correctly.")
```

---

## Common Network Issues

### Issue: 403 Forbidden

**Symptom:** `HTTPError: 403 Client Error: Forbidden`

**Possible Causes:**
1. Corporate firewall blocking external APIs
2. Missing/incorrect User-Agent header
3. IP rate limiting
4. Geographic restrictions

**Solutions:**
1. Check firewall rules
2. Headers are already configured in code
3. Reduce request frequency
4. Use VPN if geo-restricted

### Issue: Connection Timeout

**Symptom:** `requests.exceptions.ConnectionError: Connection timeout`

**Possible Causes:**
1. Network latency
2. Firewall dropping packets
3. Service outage

**Solutions:**
1. Increase timeout in code
2. Check firewall logs
3. Verify service status

### Issue: SSL Certificate Errors

**Symptom:** `SSLError: certificate verify failed`

**Possible Causes:**
1. Corporate SSL inspection
2. Outdated CA certificates
3. System date/time incorrect

**Solutions:**
1. Install corporate root CA
2. Update CA bundle: `pip install --upgrade certifi`
3. Check system time

---

## Production Deployment Considerations

### 1. Scheduled Runs

If running on a schedule (e.g., nightly):

```bash
# Cron example (daily at 2 AM)
0 2 * * * cd /path/to/weca-core-data && PYTHONPATH=. /path/to/uv run python pipelines/orchestrate_etl.py --full >> /var/log/weca-etl.log 2>&1
```

### 2. Error Notifications

Consider adding notification on network failures:

```python
import smtplib
from email.message import EmailMessage

def notify_admin(error_msg):
    msg = EmailMessage()
    msg['Subject'] = 'WECA ETL Pipeline Network Error'
    msg['From'] = 'etl@example.com'
    msg['To'] = 'admin@example.com'
    msg.set_content(f"Pipeline failed with network error:\n\n{error_msg}")

    with smtplib.SMTP('localhost') as s:
        s.send_message(msg)
```

### 3. Retry Strategy

The pipeline uses `dlt`, which has built-in retry logic. Configure if needed:

```python
# In .dlt/config.toml
[runtime]
request_timeout = 30
request_max_attempts = 5
request_backoff_factor = 2
```

### 4. Monitoring

Monitor API availability:

```bash
# Uptime monitoring
while true; do
  curl -s -o /dev/null -w "%{http_code}" "https://services1.arcgis.com/..." >> uptime.log
  sleep 300  # Check every 5 minutes
done
```

---

## Contact Information

For network/firewall issues specific to your organization, contact:
- **IT Department:** Firewall/proxy configuration
- **Security Team:** SSL inspection certificates
- **Network Team:** Bandwidth/connectivity issues

For API-specific issues:
- **ArcGIS:** Esri support
- **UK Gov APIs:** GDS support
- **EPC API:** opendatacommunities.org support
- **IMD 2025:** humaniverse package maintainers

---

**Document Version:** 1.0
**Last Updated:** 2025-11-20
**Maintained By:** WECA Data Team
