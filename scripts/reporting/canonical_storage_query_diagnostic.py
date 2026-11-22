#!/usr/bin/env python3
"""Run one bounded, read-only canonical storage SQL diagnostic."""

from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
import sys


os.environ.setdefault("QT_DISABLE_DOTENV", "1")
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portal.backend.db import db  # noqa: E402
from portal.backend.service.storage.repos.query_diagnostic import (  # noqa: E402
    QueryDiagnosticLimits,
    run_live_query_diagnostic,
    write_receipt,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the closed, server-only canonical retention query inventory. "
            "This command accepts no SQL and creates no maintenance authority."
        )
    )
    parser.add_argument("--storage-day", required=True, help="Verified UTC storage day (YYYY-MM-DD).")
    parser.add_argument("--receipt", required=True, help="Private JSON receipt path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        storage_day = date.fromisoformat(args.storage_day)
    except ValueError as exc:
        raise SystemExit("--storage-day must be YYYY-MM-DD") from exc
    receipt = run_live_query_diagnostic(
        database=db,
        storage_day=storage_day,
        limits=QueryDiagnosticLimits(),
    )
    write_receipt(Path(args.receipt), receipt)
    print(json.dumps(receipt, sort_keys=True, default=str))
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
