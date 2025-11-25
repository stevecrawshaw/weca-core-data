#!/usr/bin/env python3
"""
Verify EPC credentials are properly configured

Usage:
    python verify_credentials.py
"""

import base64
import os

from dotenv import load_dotenv

# Load .env file
load_dotenv()


def verify_epc_credentials():
    """Verify EPC API credentials from environment variables"""
    print("=" * 70)
    print("EPC CREDENTIALS VERIFICATION")
    print("=" * 70)

    # Check for API key in environment
    api_key = os.getenv("SOURCES__EPC__API_KEY")

    if not api_key:
        print("\n❌ SOURCES__EPC__API_KEY not found in environment")
        print("\nTroubleshooting:")
        print("  1. Make sure .env file exists")
        print("  2. Check .env contains: SOURCES__EPC__API_KEY=your_token")
        print("  3. Run: python setup_credentials.py --env")
        return False

    print(f"\n✓ Found SOURCES__EPC__API_KEY in environment")
    print(f"  Length: {len(api_key)} characters")

    # Try to decode the base64 token
    try:
        decoded = base64.b64decode(api_key).decode("utf-8")
        if ":" not in decoded:
            print("\n⚠ Warning: Token doesn't contain ':' separator")
            print(f"  Decoded: {decoded[:50]}...")
            return False

        email, key = decoded.split(":", 1)
        print(f"\n✓ Token decoded successfully:")
        print(f"  Email: {email}")
        print(f"  API Key: {key[:10]}...{key[-10:]}")

        # Check format
        if "@" not in email:
            print("\n⚠ Warning: Email doesn't look valid (no @ symbol)")
            return False

        if len(key) < 20:
            print("\n⚠ Warning: API key seems too short")
            return False

        print("\n✅ Credentials look valid!")
        print("\nYou can now run:")
        print("  PYTHONPATH=. uv run python pipelines/orchestrate_etl.py")
        return True

    except Exception as e:
        print(f"\n❌ Error decoding token: {e}")
        print("\nThe token should be base64 encoded in format:")
        print("  base64(email:apikey)")
        return False


def check_other_env_vars():
    """Check other environment variables"""
    print("\n" + "=" * 70)
    print("OTHER ENVIRONMENT VARIABLES")
    print("=" * 70)

    env_vars = [
        ("DOWNLOAD_EPC", "Control EPC downloads"),
        ("DOWNLOAD_LSOA", "Control LSOA downloads"),
        ("DOWNLOAD_POSTCODES", "Control postcode downloads"),
    ]

    for var, description in env_vars:
        value = os.getenv(var)
        if value:
            print(f"\n✓ {var}: {value}")
            print(f"  ({description})")
        else:
            print(f"\n- {var}: Not set (will use defaults)")


if __name__ == "__main__":
    # Verify credentials
    valid = verify_epc_credentials()

    # Check other variables
    check_other_env_vars()

    # Exit with appropriate code
    exit(0 if valid else 1)
