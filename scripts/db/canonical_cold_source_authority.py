"""Measure the exact source archive authority against a consolidated migration audit."""

from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
from time import monotonic

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract
from portal.backend.service.storage.repos.query_diagnostic import write_receipt
from scripts.db.canonical_cold_activate import verify_source_authority_inventory
from scripts.db.canonical_cold_migration import PostgresMigrationSource
from scripts.db.canonical_cold_receipts import RECEIPT_SET_VERSION


VERSION = "qt.canonical_cold_source_authority_preflight.v1"
_MAX_SUMMARY_BYTES = 16 * 1024**2


def verify_source_authority_preflight(engine, *, audit, expected_audit_hash, max_seconds):
    """Verify the same archive authority as activation, on one read-only snapshot."""
    if (type(max_seconds) is not int or not 1 <= max_seconds <= MAX_MIGRATION_SECONDS
            or not isinstance(audit, dict) or audit.get("schema") != RECEIPT_SET_VERSION
            or not isinstance(expected_audit_hash, str) or len(expected_audit_hash) != 64
            or audit.get("audit_set_hash") != expected_audit_hash
            or archive_evidence_hash({key: value for key, value in audit.items() if key != "audit_set_hash"})
            != expected_audit_hash):
        raise ValueError("canonical_cold_source_authority_bad_audit")
    identity = audit["database"]
    if not isinstance(identity, dict) or set(identity) != {
        "system_identifier", "database", "database_oid", "header_oid",
    }:
        raise ValueError("canonical_cold_source_authority_bad_database_identity")
    cutoff = date.fromisoformat(audit["prefix"]["cold_before_day"])
    started = monotonic()

    def check_budget():
        if monotonic() - started >= max_seconds:
            raise TimeoutError("canonical_cold_source_authority_deadline_exceeded")

    source = PostgresMigrationSource(
        engine, expected_system_identifier=identity["system_identifier"],
        statement_timeout_ms=min(30_000, max_seconds * 1000),
    )
    with source.snapshot() as conn:
        check_budget()
        if (getattr(conn.connection.driver_connection, "autocommit", None) is not False
                or source.identity(conn) != identity):
            raise RuntimeError("canonical_cold_source_authority_wrong_database_or_session")
        state = conn.execute(text("""
            SELECT layout_version,state,completed_at IS NOT NULL FROM market.fact_storage_state
            ORDER BY layout_version LIMIT 2
        """)).all()
        if state != [(FACT_STORAGE_LAYOUT_VERSION, "ready", True)]:
            raise RuntimeError("canonical_cold_source_authority_original_layout_required")
        check_budget()
        assert_fact_storage_contract(conn)
        observed_at = conn.scalar(text("SELECT transaction_timestamp()")).isoformat()
        inventory_started = monotonic()
        inventory = verify_source_authority_inventory(conn, cutoff, audit, check_budget=check_budget)
        inventory_seconds = monotonic() - inventory_started
    return {
        "schema": VERSION, "audit_set_hash": expected_audit_hash, "database": identity,
        "cold_before_day": cutoff.isoformat(), "observed_at": observed_at, **inventory,
        "inventory_seconds": round(inventory_seconds, 6),
        "elapsed_seconds": round(monotonic() - started, 6), "max_seconds": max_seconds,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-summary", required=True, type=Path)
    parser.add_argument("--audit-set-hash", required=True)
    parser.add_argument("--max-seconds", required=True, type=int)
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args(argv)
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        parser.error("PG_DSN is required")
    if args.audit_summary.resolve() == args.receipt.resolve():
        parser.error("The audit summary and output receipt require separate paths.")
    engine = None
    try:
        with args.audit_summary.open("rb") as handle:
            data = handle.read(_MAX_SUMMARY_BYTES + 1)
        if len(data) > _MAX_SUMMARY_BYTES:
            raise ValueError("canonical_cold_source_authority_audit_summary_too_large")
        engine = create_engine(
            dsn, hide_parameters=True, connect_args={"connect_timeout": 5, "options": "-c default_transaction_read_only=on"},
        )
        result = verify_source_authority_preflight(
            engine, audit=json.loads(data), expected_audit_hash=args.audit_set_hash, max_seconds=args.max_seconds,
        )
        write_receipt(args.receipt, result | {"receipt_hash": archive_evidence_hash(result)})
        print(json.dumps(result, sort_keys=True))
        return 0
    except SQLAlchemyError as exc:
        raise SystemExit(f"canonical_cold_source_authority_database_error: {type(exc).__name__}") from None
    except (OSError, RuntimeError, ValueError, TypeError, KeyError) as exc:
        raise SystemExit(str(exc)) from None
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
