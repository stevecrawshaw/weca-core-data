#!/usr/bin/env python3
"""
Helper script to set up EPC credentials from config.yml to .env or secrets.toml

Usage:
    python setup_credentials.py --env        # Create .env file
    python setup_credentials.py --secrets    # Create .dlt/secrets.toml
    python setup_credentials.py --both       # Create both files
"""

import argparse
import base64
from pathlib import Path

import yaml


def load_config(config_path: str = "../config.yml") -> dict:
    """Load credentials from config.yml"""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file) as f:
        return yaml.safe_load(f)


def create_env_file(auth_token: str) -> None:
    """Create .env file with EPC credentials"""
    env_content = f"""# WECA Core Data ETL - Environment Variables
# Auto-generated from config.yml

# =============================================================================
# EPC API Configuration
# =============================================================================
SOURCES__EPC__API_KEY={auth_token}

# =============================================================================
# Pipeline Configuration
# =============================================================================
# Control data downloads
DOWNLOAD_EPC=true
DOWNLOAD_LSOA=true
DOWNLOAD_POSTCODES=true
"""

    env_file = Path(".env")
    env_file.write_text(env_content)
    print(f"✓ Created {env_file.absolute()}")


def create_secrets_file(auth_token: str) -> None:
    """Create .dlt/secrets.toml with EPC credentials"""
    secrets_content = f"""# EPC API Authentication Configuration
# Auto-generated from config.yml

[sources.epc]
api_key = "{auth_token}"
"""

    secrets_file = Path(".dlt/secrets.toml")
    secrets_file.write_text(secrets_content)
    print(f"✓ Created {secrets_file.absolute()}")


def verify_credentials(auth_token: str) -> None:
    """Verify the auth token is valid base64 and has email:apikey format"""
    try:
        decoded = base64.b64decode(auth_token).decode("utf-8")
        if ":" in decoded:
            email, api_key = decoded.split(":", 1)
            print(f"\n✓ Credentials verified:")
            print(f"  Email: {email}")
            print(f"  API Key: {api_key[:10]}...{api_key[-10:]}")
        else:
            print("\n⚠ Warning: Decoded token doesn't contain ':' separator")
    except Exception as e:
        print(f"\n⚠ Warning: Could not decode auth token: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Set up EPC credentials from config.yml"
    )
    parser.add_argument(
        "--env", action="store_true", help="Create .env file (recommended)"
    )
    parser.add_argument(
        "--secrets", action="store_true", help="Create .dlt/secrets.toml"
    )
    parser.add_argument("--both", action="store_true", help="Create both files")
    parser.add_argument(
        "--config", default="../config.yml", help="Path to config.yml"
    )

    args = parser.parse_args()

    # Default to --env if no options specified
    if not (args.env or args.secrets or args.both):
        args.env = True

    try:
        # Load config
        print(f"Loading credentials from {args.config}...")
        config = load_config(args.config)

        if "epc" not in config or "auth_token" not in config["epc"]:
            raise ValueError(
                "EPC credentials not found in config.yml. "
                "Expected: epc.auth_token"
            )

        auth_token = config["epc"]["auth_token"]

        # Verify credentials
        verify_credentials(auth_token)

        # Create requested files
        if args.env or args.both:
            create_env_file(auth_token)

        if args.secrets or args.both:
            create_secrets_file(auth_token)

        print("\n✅ Setup complete!")
        print("\nYou can now run the full pipeline:")
        print("  PYTHONPATH=. uv run python pipelines/orchestrate_etl.py")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
