"""Capture the exact next archive page batch from an existing database, read-only."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date
import hashlib
import json
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from portal.backend.service.storage.repos.fact_archival import (
    FACT_ARCHIVE_VERIFIER_VERSION, prepare_selected_verification_batch,
)
from portal.backend.service.storage.repos.query_diagnostic import write_receipt
from scripts.db.canonical_cold_migration import PostgresMigrationSource


VERSION = "market.canonical_retention_verification_batch.v1"


def _source(engine, identity):
    if (not isinstance(identity, dict)
            or set(identity) != {"system_identifier", "database", "database_oid", "header_oid"}
            or not isinstance(identity["system_identifier"], str)
            or not identity["system_identifier"].isascii()
            or not identity["system_identifier"].isdigit()
            or not isinstance(identity["database"], str) or not identity["database"]
            or any(type(identity[key]) is not int or not 1 <= identity[key] <= 2**32 - 1
                   for key in ("database_oid", "header_oid"))):
        raise ValueError("canonical_archive_prepare_database_identity_invalid")
    return PostgresMigrationSource(
        engine, expected_system_identifier=identity["system_identifier"], statement_timeout_ms=5_000,
    )


def _assert_source(source, conn, expected):
    if source.identity(conn) != expected:
        raise RuntimeError("canonical_archive_prepare_source_changed")


def prepare_verification_batch(engine, *, expected_source, day, first_page_ordinal, page_count, day_page_count):
    if (type(day) is not date or type(first_page_ordinal) is not int or not 0 <= first_page_ordinal < 2**31
            or type(page_count) is not int or not 2 <= page_count <= 128
            or type(day_page_count) is not int or not page_count <= day_page_count <= 100_000):
        raise ValueError("canonical_archive_prepare_scope_invalid")
    source = _source(engine, expected_source)
    with source.snapshot() as conn:
        _assert_source(source, conn, expected_source)
        partition = conn.execute(text("""
            SELECT state,expected_rows FROM market.fact_retention_partitions WHERE storage_day=:day
        """), {"day": day}).mappings().one_or_none()
        if partition is None or partition["state"] != "sealed":
            raise RuntimeError("canonical_archive_prepare_partition_not_sealed")
        if conn.scalar(text("SELECT to_regclass('market.cold_fact_retired_days')")) is not None and conn.scalar(text("""
            SELECT EXISTS(SELECT 1 FROM market.cold_fact_retired_days WHERE storage_day=:day)
        """), {"day": day}):
            raise RuntimeError("canonical_archive_prepare_partition_retired")
        count, rows = conn.execute(text("""
            SELECT count(*),coalesce(sum(row_count),0)
            FROM (SELECT row_count FROM market.fact_archive_manifests
                  WHERE storage_day=:day ORDER BY page_ordinal LIMIT :limit) AS published
        """), {"day": day, "limit": day_page_count + 1}).one()
        if (count, rows) != (day_page_count, partition["expected_rows"]):
            raise RuntimeError("canonical_archive_prepare_day_coverage_changed")
        pins = prepare_selected_verification_batch(
            conn, day, first_page_ordinal=first_page_ordinal, page_count=page_count,
        )
        timestamp = conn.scalar(text("SELECT transaction_timestamp()"))
    return {"schema_version": VERSION, "source": expected_source, "storage_day": day.isoformat(),
            "observed_at": timestamp.isoformat(), "verifier_version": FACT_ARCHIVE_VERIFIER_VERSION,
            "day_page_count": count, "day_expected_rows": int(rows), "pages": [asdict(pin) for pin in pins]}


def require_prepared_source(expected_source):
    """Check the reviewed preparation's identity before the action runner opens."""
    engine = _engine()
    try:
        source = _source(engine, expected_source)
        with source.snapshot() as conn:
            _assert_source(source, conn, expected_source)
    finally:
        engine.dispose()


def _engine():
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        raise ValueError("PG_DSN is required")
    return create_engine(dsn, connect_args={"connect_timeout": 5, "options": "-c default_transaction_read_only=on"},
                         hide_parameters=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-day", required=True, type=date.fromisoformat)
    parser.add_argument("--system-identifier", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--database-oid", type=int, required=True)
    parser.add_argument("--header-oid", type=int, required=True)
    parser.add_argument("--first-page-ordinal", type=int, required=True)
    parser.add_argument("--page-count", type=int, required=True)
    parser.add_argument("--day-page-count", type=int, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    engine = None
    try:
        engine = _engine()
        receipt = prepare_verification_batch(
            engine, expected_source={key: getattr(args, key) for key in (
                "system_identifier", "database", "database_oid", "header_oid",
            )},
            day=args.storage_day, first_page_ordinal=args.first_page_ordinal,
            page_count=args.page_count, day_page_count=args.day_page_count,
        )
        path = args.receipt.expanduser().resolve()
        write_receipt(path, receipt)
        print(json.dumps({"receipt": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                          "first_page_ordinal": args.first_page_ordinal, "page_count": args.page_count}, sort_keys=True))
        return 0
    except SQLAlchemyError as exc:
        raise SystemExit(f"canonical_archive_prepare_database_error: {type(exc).__name__}") from None
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
