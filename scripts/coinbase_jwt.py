#!/usr/bin/env python3
"""
Generate a Coinbase Advanced Trade JWT for manual testing.

Usage:
  python scripts/coinbase_jwt.py --method GET --path /api/v3/brokerage/products
"""

from __future__ import annotations

import argparse
import os
import sys

from coinbase import jwt_generator
from dotenv import load_dotenv


def _load_secret() -> str:
    secret_file = os.getenv("COINBASE_API_SECRET_FILE")
    if secret_file:
        secret_file = os.path.abspath(os.path.expanduser(secret_file))
        if not os.path.isfile(secret_file):
            raise FileNotFoundError(f"COINBASE_API_SECRET_FILE not found: {secret_file}")
        with open(secret_file, "r") as handle:
            return handle.read()
    secret = os.getenv("COINBASE_API_SECRET")
    if not secret:
        raise ValueError("COINBASE_API_SECRET or COINBASE_API_SECRET_FILE is required")
    if "\\n" in secret:
        secret = secret.replace("\\n", "\n")
    return secret


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
        help="Path to env file (default: secrets.env)",
    )
    args = parser.parse_args()

    load_dotenv(args.env_file)

    api_key = os.getenv("COINBASE_API_KEY")
    if not api_key:
        raise ValueError("COINBASE_API_KEY is required")

    api_secret = _load_secret()
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
