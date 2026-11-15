"""Load verified staged pages into the inactive canonical history catalog.

The operator supplies an exact target database identity. Each transaction
rechecks the activation and placement boundary. This command only prepares cold
lookup rows. The cutover owner verifies whole-inventory parity before activation.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date
from functools import partial
from itertools import islice
import json
from math import isfinite
import os
from pathlib import Path
from time import monotonic

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from core.storage_mounts import configured_archive_root
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import FactArchiveLimits
from portal.backend.db.cold_history_models import guard_cold_runtime_placement
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory, cold_layout_active
from portal.backend.service.storage.repos.fact_storage import ensure_payload_contracts
from scripts.db.canonical_cold_load_pressure import CatalogLoadLimits, LoadPressure, supervise_page
from scripts.db.canonical_cold_migration import INITIAL, MigrationCheckpoint


@dataclass(frozen=True)
class TargetDatabase:
    system_identifier: str
    name: str
    oid: int


def _guard(session, target, *, statement_timeout_ms, history_target_id=None):
    session.execute(text("""
        SELECT set_config('statement_timeout',
            LEAST(CASE WHEN setting::bigint=0 THEN :value ELSE setting::bigint END,:value)::text,true)
        FROM pg_settings WHERE name='statement_timeout'
    """),
                    {"value": statement_timeout_ms})
    session.execute(text("SET LOCAL lock_timeout = '2s'"))
    actual = session.execute(text("""SELECT control.system_identifier::text, db.datname, db.oid::bigint
        FROM pg_catalog.pg_control_system() control CROSS JOIN pg_catalog.pg_database db
        WHERE db.datname=current_database()""")).one()
    if tuple(actual) != (target.system_identifier, target.name, target.oid):
        raise RuntimeError("canonical_cold_load_wrong_database")
    # Blocks state-changing DML/DDL during each admission. A subsequent batch
    # reacquires the guard and sees any intervening activation before writing.
    session.execute(text("LOCK TABLE market.fact_storage_state IN SHARE MODE NOWAIT"))
    if cold_layout_active(session):
        raise RuntimeError("canonical_cold_load_layout_already_active")
    guard_cold_runtime_placement(session, locators=True, locator_target_id=history_target_id)


def _verify_resume_page(session, prior):
    observed = session.execute(text("SELECT manifest_hash,staged_storage_day FROM market.cold_fact_pages WHERE id=:id"),
                               {"id": prior.manifest.manifest_id}).one_or_none()
    if observed != (prior.manifest.manifest_hash, None):
        raise RuntimeError(f"canonical_cold_load_resume_page_not_admitted: id={prior.manifest.manifest_id}")


def load_checkpoint(engine, checkpoint, *, target, object_store, day, after_ordinal=-1,
                    max_pages=1, max_seconds=120, statement_timeout_ms=30_000,
                    pressure_limits=None, pg_controldata=None):
    if ((pressure_limits is None) != (pg_controldata is None)
            or (pressure_limits is not None and (not isinstance(pressure_limits, CatalogLoadLimits)
                                                 or not Path(pg_controldata).is_absolute()))):
        raise ValueError("canonical_cold_load_pressure_invalid_configuration")
    history_target_id = pressure_limits.history_target_id if pressure_limits is not None else None
    supervision = (partial(supervise_page, limits=pressure_limits, pg_controldata=Path(pg_controldata))
                   if pressure_limits is not None else None)
    return _load_checkpoint(engine, checkpoint, target=target, object_store=object_store, day=day,
                            after_ordinal=after_ordinal, max_pages=max_pages, max_seconds=max_seconds,
                            statement_timeout_ms=statement_timeout_ms, history_target_id=history_target_id,
                            supervise=supervision)


def _load_checkpoint(engine, checkpoint, *, target, object_store, day, after_ordinal, max_pages, max_seconds,
                     statement_timeout_ms, history_target_id, supervise):
    if (type(max_pages) is not int or max_pages < 1 or not isfinite(max_seconds) or max_seconds <= 0
            or type(statement_timeout_ms) is not int or statement_timeout_ms < 1):
        raise ValueError("canonical_cold_load_invalid_budget")
    deadline = monotonic() + max_seconds
    binding = checkpoint.binding()
    if Path(binding["staging_root"]).resolve(strict=True) != object_store.root:
        raise RuntimeError("canonical_cold_load_object_root_does_not_match_checkpoint")
    limits = FactArchiveLimits(**binding["limits"])
    complete, cohort_rows, cohort_pages, final_seq, final_id = checkpoint.progress(day)
    if not complete:
        raise RuntimeError(f"canonical_cold_load_cohort_incomplete: day={day}")
    if type(after_ordinal) is not int or not -1 <= after_ordinal < cohort_pages:
        raise ValueError(f"canonical_cold_load_invalid_resume_ordinal: day={day}")
    prior = None
    previous_cursor = INITIAL
    if after_ordinal >= 0:
        cursor = checkpoint.iter_completed_pages(day=day, after_ordinal=after_ordinal - 1)
        try:
            prior = next(cursor, None)
        finally:
            cursor.close()
        if prior is None or prior.ordinal != after_ordinal:
            raise RuntimeError(f"canonical_cold_load_missing_resume_page: day={day} ordinal={after_ordinal}")
        previous_cursor = prior.manifest.last_cursor
    processed = rows = 0
    deferred = None
    last = after_ordinal
    pages = checkpoint.iter_completed_pages(day=day, after_ordinal=after_ordinal)
    try:
        for page in islice(pages, max_pages):
            remaining_ms = int((deadline - monotonic()) * 1000)
            if remaining_ms <= 0:
                break
            if (page.ordinal != last + 1 or page.after != previous_cursor
                    or page.manifest.first_cursor <= previous_cursor):
                raise RuntimeError(f"canonical_cold_load_checkpoint_order_changed: day={day} ordinal={page.ordinal}")
            try:
                with Session(engine) as session, session.begin():
                    _guard(session, target, statement_timeout_ms=min(statement_timeout_ms, remaining_ms),
                           history_target_id=history_target_id)
                    if prior is not None:
                        _verify_resume_page(session, prior)
                        prior = None
                    reader = ColdHistoryReader(
                        object_store, limits=limits,
                        ensure_contracts=lambda contracts: ensure_payload_contracts(session, contracts),
                    )
                    supervision = nullcontext() if supervise is None else supervise(session, deadline=deadline)
                    with supervision:
                        PostgresColdHistory(reader).admit(session, page.manifest)
            except LoadPressure as exc:
                deferred = str(exc)
                break
            last = page.ordinal
            previous_cursor = page.manifest.last_cursor
            processed += 1
            rows += page.manifest.row_count
    finally:
        pages.close()
    exhausted = last + 1 == cohort_pages
    if exhausted and previous_cursor != (final_seq, final_id):
        raise RuntimeError(f"canonical_cold_load_checkpoint_cursor_changed: day={day}")
    if exhausted and not processed:
        with Session(engine) as session, session.begin():
            _guard(session, target, statement_timeout_ms=statement_timeout_ms, history_target_id=history_target_id)
            if prior is not None:
                _verify_resume_page(session, prior)
    if deferred is None and processed < max_pages and not exhausted and monotonic() < deadline:
        raise RuntimeError(f"canonical_cold_load_checkpoint_page_missing: day={day} after_ordinal={last}")
    result = {"storage_day": day.isoformat(), "pages_processed": processed, "rows_processed": rows,
            "last_processed_ordinal": last, "checkpoint_pages": cohort_pages, "checkpoint_rows": cohort_rows,
            "checkpoint_end_reached": exhausted}
    return result if deferred is None else result | {"deferred_reason": deferred}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-system-identifier", required=True)
    parser.add_argument("--target-database", required=True)
    parser.add_argument("--target-database-oid", required=True, type=int)
    parser.add_argument("--checkpoint", required=True, type=Path)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    parser.add_argument("--pressure-limits", required=True, type=Path)
    parser.add_argument("--pg-controldata", type=Path, default=Path("/usr/lib/postgresql/15/bin/pg_controldata"))
    parser.add_argument("--after-ordinal", default=-1, type=int)
    parser.add_argument("--max-pages", default=1, type=int)
    parser.add_argument("--max-seconds", default=120, type=int)
    args = parser.parse_args()
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        parser.error("PG_DSN is required")
    try:
        with args.pressure_limits.open("rb") as config:
            data = config.read(8193)
        if len(data) > 8192:
            raise ValueError("configuration exceeds 8192 bytes")
        pressure_limits = CatalogLoadLimits(**json.loads(data))
    except (OSError, TypeError, ValueError) as exc:
        parser.error(f"invalid pressure limits: {exc}")
    target = TargetDatabase(args.target_system_identifier, args.target_database, args.target_database_oid)
    root = (configured_archive_root() / "objects").resolve(strict=True)
    store = FilesystemRawArchiveObjectStore(root, writable=False)
    engine = create_engine(dsn, future=True)
    try:
        result = load_checkpoint(engine, MigrationCheckpoint(args.checkpoint), target=target,
                                 object_store=store, day=args.day, after_ordinal=args.after_ordinal,
                                 max_pages=args.max_pages, max_seconds=args.max_seconds,
                                 pressure_limits=pressure_limits, pg_controldata=args.pg_controldata)
        print(json.dumps(result, sort_keys=True))
        return 75 if "deferred_reason" in result else 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
