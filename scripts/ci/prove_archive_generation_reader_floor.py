#!/usr/bin/env python3
"""Exercise one frozen application's real database startup contract."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from portal.backend.db.session import Database


EXPECTED_REFUSAL = (
    "Canonical Fact storage function differs: market.cold_fact_read_required. Run "
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expect", choices=("ready", "refused"), required=True)
    args = parser.parse_args()
    attestation = json.loads(Path("/app/.qt-source-attestation.json").read_text())
    expected_revision = os.environ.get("QT_EXPECTED_SOURCE_REVISION", "")
    if attestation.get("source_revision") != expected_revision:
        raise SystemExit("baseline_application_source_attestation_mismatch")
    database = Database()
    ready = database.ensure_schema()
    error = "" if database.last_error is None else str(database.last_error)
    if args.expect == "ready":
        if not ready:
            raise SystemExit("baseline_application_failed_before_generation_migration: " + error)
    elif ready or EXPECTED_REFUSAL not in error:
        raise SystemExit(
            "baseline_application_did_not_refuse_generation_reader_floor: "
            + ("startup_succeeded" if ready else error)
        )
    print(json.dumps({"expected": args.expect, "ready": ready,
                      "reader_floor_refusal": EXPECTED_REFUSAL in error,
                      "source_revision": attestation["source_revision"]}, sort_keys=True))


if __name__ == "__main__":
    main()
