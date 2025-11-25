# Credentials Setup Guide

Complete guide for setting up EPC API credentials for the WECA Core Data ETL pipeline.

## Quick Setup (Recommended)

Your credentials are already configured! Skip to [Running the Pipeline](#running-the-pipeline).

If you need to reconfigure:

```bash
# Verify credentials are working
python verify_credentials.py

# If not working, regenerate from config.yml
python setup_credentials.py --env
```

## Files Created

✓ `.env` - Environment variables (your credentials)
✓ `.env.example` - Template for others
✓ `setup_credentials.py` - Helper to generate credentials
✓ `verify_credentials.py` - Test credentials are valid

## Credential Storage Options

### Option 1: `.env` File (Recommended) ⭐

Best for development and local use. Automatically loaded by the pipeline.

```bash
# File: .env
SOURCES__EPC__API_KEY=c3RldmUuY3Jhd3NoY...  # Base64 encoded email:apikey
```

**Advantages:**
- ✅ Easy to edit
- ✅ Works with all Python tools
- ✅ Already in `.gitignore` (won't be committed)
- ✅ Standard practice

### Option 2: `.dlt/secrets.toml`

Alternative for dlt-specific configuration.

```bash
# Generate secrets.toml
python setup_credentials.py --secrets

# File: .dlt/secrets.toml
[sources.epc]
api_key = "your_base64_token"
```

### Option 3: Environment Variables

For production/CI environments:

```bash
# Set in shell
export SOURCES__EPC__API_KEY="your_base64_token"

# Or pass inline
SOURCES__EPC__API_KEY="..." python pipelines/orchestrate_etl.py
```

## How EPC Credentials Work

### Format

The EPC API requires HTTP Basic Authentication:
```
Authorization: Basic <base64(email:apikey)>
```

### Getting Your API Key

1. Register at: https://epc.opendatacommunities.org/
2. Get your API key from the portal
3. The key is already in your `../config.yml` file

### Encoding Your Credentials

If you need to manually create the token:

```python
import base64

email = "your.email@example.com"
api_key = "your_api_key_from_portal"
token = base64.b64encode(f"{email}:{api_key}".encode()).decode()
print(f"SOURCES__EPC__API_KEY={token}")
```

Or use the helper script:
```bash
python setup_credentials.py --env
```

## Verifying Credentials

```bash
# Check credentials are valid
python verify_credentials.py
```

**Expected output:**
```
✓ Found SOURCES__EPC__API_KEY in environment
✓ Token decoded successfully:
  Email: your.email@example.com
  API Key: abc123...xyz789
✅ Credentials look valid!
```

## Running the Pipeline

Once credentials are set up:

```bash
# Sample mode (fast test with 1,000 records)
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py --sample

# Full pipeline (30+ minutes, millions of records)
PYTHONPATH=. uv run python pipelines/orchestrate_etl.py
```

## Troubleshooting

### Error: "api_key not found in secrets"

**Cause:** `.env` file not loaded or missing

**Solution:**
```bash
# Verify .env exists
ls -la .env

# Regenerate if needed
python setup_credentials.py --env

# Verify it works
python verify_credentials.py
```

### Error: "401 Unauthorized"

**Cause:** Invalid API key or wrong encoding

**Solution:**
```bash
# Check token decodes correctly
python verify_credentials.py

# Check original credentials in config.yml
cat ../config.yml | grep -A3 "epc:"

# Regenerate from config.yml
python setup_credentials.py --env
```

### Error: ".env file not found"

**Cause:** Running from wrong directory

**Solution:**
```bash
# Make sure you're in project root
cd /home/steve/projects/weca-core-data

# Verify
ls -la .env
```

### Error: "Module 'dotenv' not found"

**Cause:** python-dotenv not installed

**Solution:**
```bash
uv add python-dotenv
```

## Security Best Practices

✅ **DO:**
- Keep `.env` in `.gitignore` (already done)
- Use `.env.example` for sharing templates
- Rotate API keys periodically
- Use environment variables in production

❌ **DON'T:**
- Commit `.env` to version control
- Share your `.env` file
- Hardcode credentials in code
- Push secrets to GitHub

## For Team Members

If you're setting up on a new machine:

1. **Get credentials** from team lead or register at EPC portal
2. **Copy template**: `cp .env.example .env`
3. **Add your token**: Edit `.env` and set `SOURCES__EPC__API_KEY`
4. **Verify**: `python verify_credentials.py`
5. **Run pipeline**: `python pipelines/orchestrate_etl.py --sample`

Or use the helper:
```bash
python setup_credentials.py --env
```

## Related Files

- `.env` - Your actual credentials (DO NOT COMMIT)
- `.env.example` - Template for team (safe to commit)
- `.dlt/secrets.toml` - Alternative credential storage
- `.dlt/secrets.toml.example` - Template for dlt secrets
- `setup_credentials.py` - Helper script to generate credentials
- `verify_credentials.py` - Test credentials are valid

---

**Last Updated:** 2025-11-25
**Status:** ✅ Credentials configured and tested
