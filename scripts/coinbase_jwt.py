#!/usr/bin/env python3
"""
Generate a Coinbase Advanced Trade JWT for manual testing.

Usage:
  python scripts/coinbase_jwt.py --method GET --path /api/v3/brokerage/products
"""

from __future__ import annotations

import argparse
import sys

from coinbase import jwt_generator
from dotenv import load_dotenv

from data_providers.services.credential_store import load_credentials


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a Coinbase JWT.")
    parser.add_argument("--method", default="GET", help="HTTP method, e.g. GET")
    parser.add_argument("--path", required=True, help="Request path, e.g. /api/v3/brokerage/products")
    parser.add_argument(
        "--host",
        default="api.coinbase.com",
        help="API host (default: api.coinbase.com)",
    )
    parser.add_argument(
        "--env-file",
        default="secrets.env",
        help="Path to env file for PG_DSN/QT_SECURITY_PROVIDER_CREDENTIAL_KEY (default: secrets.env)",
    )
    parser.add_argument("--provider", default="COINBASE")
    parser.add_argument("--venue", default="COINBASE_DIRECT")
    parser.add_argument("--credential-ref")
    parser.add_argument("--environment", default="paper")
    args = parser.parse_args()

    load_dotenv(args.env_file)

    stored = load_credentials(
        args.provider,
        args.venue,
        credential_ref=args.credential_ref,
        environment=args.environment,
    )
    if not stored:
        raise ValueError("Coinbase credentials were not found in the provider credential store")

    api_key = str(stored.get("COINBASE_API_KEY") or "").strip()
    if not api_key:
        raise ValueError("Stored Coinbase credentials are missing COINBASE_API_KEY")

    api_secret = str(stored.get("COINBASE_API_SECRET") or "")
    if not api_secret:
        raise ValueError("Stored Coinbase credentials are missing COINBASE_API_SECRET")
    if "\\n" in api_secret:
        api_secret = api_secret.replace("\\n", "\n")
    uri = jwt_generator.format_jwt_uri(args.method.upper(), args.path)
    token = jwt_generator.build_rest_jwt(uri, api_key, api_secret)
    print(token)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
