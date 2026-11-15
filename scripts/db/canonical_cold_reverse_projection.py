"""Isolated reverse-projection primitive for one verified lossless Fact page."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date
from hashlib import sha256
from time import monotonic
from typing import Mapping

from sqlalchemy import insert, text

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_namespace import archive_namespace
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import FactArchiveManifest, archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_identity_schema import _assert_header_partition, _partition_bound
from portal.backend.db.fact_storage_schema import (
    FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract, ensure_fact_payload_partition, fact_partition_name,
)
from portal.backend.db.market_data_models import MarketFactVersionRecord
from portal.backend.db.market_storage_models import MarketFactHotPayloadRecord
from portal.backend.service.storage.repos.cold_history import _manifest
from portal.backend.service.storage.repos.fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM, ensure_payload_contracts
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.db.canonical_cold_audit import _locators
from scripts.db.canonical_cold_migration import PostgresMigrationSource
from scripts.db.canonical_cold_restore_references import _require_disposable_fence
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_reverse_projection_fixture.v1"


@dataclass(frozen=True)
class ReversePage:
    manifest_hash: str
    object_sha256: str
    source_commit: int
    expected_database: Mapping[str, str | int]
    original_storage_days: Mapping[str, date] | None = None

    @property
    def plan_hash(self):
        days = self.original_storage_days
        return archive_evidence_hash({"version": VERSION, "manifest_hash": self.manifest_hash,
            "object_sha256": self.object_sha256, "source_commit": self.source_commit, "database": dict(self.expected_database),
            "original_storage_days": {key: day.isoformat() for key, day in days.items()} if days is not None else None})


def _layout_guard(conn, plan, timeout_ms, *, layout=FACT_STORAGE_LAYOUT_VERSION):
    if (not conn.in_transaction() or conn.get_isolation_level() != "READ COMMITTED"
            or conn.get_execution_options().get("isolation_level") == "AUTOCOMMIT"
            or type(timeout_ms) is not int or not 1 <= timeout_ms <= 120_000):
        raise ValueError("reverse projection needs a bounded read-committed transaction")
    expected = plan.expected_database
    if set(expected) != {"system_identifier", "database", "database_oid", "header_oid"}:
        raise ValueError("reverse projection requires the complete destination database identity")
    if not str(expected["database"]).startswith(("qt_bench_", "qt_migration_")):
        raise ValueError("reverse projection requires an isolated fixture database")
    url = conn.engine.url
    if (url.host not in {None, "timescaledb", "127.0.0.1", "::1"}
            or (url.query.get("host") is not None and not str(url.query["host"]).startswith("/"))):
        raise ValueError("reverse projection requires a local isolated fixture endpoint")
    conn.exec_driver_sql("SET LOCAL lock_timeout='500ms'")
    conn.execute(text("SELECT set_config('statement_timeout',:value,true)"), {"value": str(timeout_ms)})
    conn.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
    if conn.exec_driver_sql("SHOW session_replication_role").scalar_one() != "origin":
        raise RuntimeError("reverse projection requires normal PostgreSQL guards")
    actual = PostgresMigrationSource(conn.engine, expected_system_identifier=expected["system_identifier"]).identity(conn)
    if actual != expected:
        raise RuntimeError("reverse projection destination identity differs")
    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"), {"name": _LIFECYCLE_LOCK_NAME}):
        raise RuntimeError("reverse projection is blocked by the lifecycle fence")
    conn.exec_driver_sql("LOCK TABLE market.fact_storage_state IN SHARE MODE NOWAIT")
    state = conn.execute(text("""SELECT layout_version,state,completed_at IS NOT NULL
        FROM market.fact_storage_state ORDER BY layout_version LIMIT 2""")).all()
    if state != [(layout, "ready", True)]:
        raise RuntimeError("reverse projection layout differs from plan")
    assert_fact_storage_contract(conn)


def _existing_destination_day(conn, day):
    _assert_header_partition(conn, day)
    payload = "market." + fact_partition_name(day)
    state = conn.execute(text("""
        SELECT registry.state,c.relkind,pg_get_expr(c.relpartbound,c.oid),parents.inhparent::regclass::text
        FROM market.fact_retention_partitions AS registry
        JOIN pg_class AS c ON c.oid=to_regclass(:payload)
        JOIN pg_inherits AS parents ON parents.inhrelid=c.oid
        WHERE registry.storage_day=:day FOR SHARE OF registry
    """), {"day": day, "payload": payload}).one_or_none()
    if state != ("open", "r", _partition_bound(day), "market.fact_hot_payloads"):
        raise RuntimeError(f"reverse projection requires a pre-created open payload partition for {day}")


def _exact_row(conn, *, day, identity):
    return conn.execute(text(f"""SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM}
        WHERE versions.storage_day=:day AND versions.id=:id"""), {"day": day, "id": identity}).mappings().one_or_none()


def _assert_exact(conn, *, day, row):
    actual = _exact_row(conn, day=day, identity=row["id"])
    if actual is None or dict(actual) != row | {"storage_day": day}:
        raise RuntimeError(f"reverse projection output differs for fact={row['id']}")
    identity = conn.execute(text("""
        SELECT storage_day,series_id,observation_key,revision FROM market.fact_identities WHERE id=:id
    """), {"id": row["id"]}).one_or_none()
    if identity != (day, row["series_id"], row["observation_key"], row["revision"]):
        raise RuntimeError(f"reverse projection identity differs for fact={row['id']}")


def rematerialize_page(conn, *, plan: ReversePage, reader: ColdHistoryReader,
                       manifest: FactArchiveManifest, expected_plan_hash, timeout_ms=30_000):
    """Write one exact verified page into existing open v2 daily partitions.

    The caller owns the isolated clone and transaction. An original storage day
    comes from a separately trusted witness. Candidate-only rows had no v2
    placement, so their destination day comes from the accepted UTC clock.
    This primitive preserves explicit commit values. The full restore owner must
    bind and reconcile the allocated sequence state before allowing new writes.
    """
    placement = plan.original_storage_days
    if placement is not None and (not isinstance(placement, Mapping) or any(type(day) is not date for day in placement.values())):
        raise ValueError("reverse projection original placement requires exact UTC date witnesses")
    if (type(plan.source_commit) is not int or plan.source_commit < manifest.last_cursor[0]
            or manifest.manifest_hash != plan.manifest_hash or manifest.object_sha256 != plan.object_sha256):
        raise RuntimeError("reverse projection page does not match its bounded acceptance plan")
    if plan.plan_hash != expected_plan_hash:
        raise RuntimeError("reverse projection plan hash differs")
    _layout_guard(conn, plan, timeout_ms)
    verified = _verified_page(conn, reader, manifest)
    if placement is not None and {row["id"] for row in verified} != placement.keys():
        raise RuntimeError("reverse projection original placement witness set differs")
    rows = [(placement[row["id"]] if placement is not None else row["accepted_at"].astimezone(UTC).date(), row)
            for row in verified]
    inserted = _write_exact_page(conn, rows)
    receipt = {"version": VERSION, "plan_hash": expected_plan_hash, "manifest_hash": manifest.manifest_hash,
               "inserted": inserted, "verified": len(rows), "last_cursor": list(manifest.last_cursor),
               "placement_mode": "original_storage_day" if placement is not None else "candidate_acceptance_utc_day",
               "stored_rows_sha256": archive_evidence_hash({"ordered_rows": [[day.isoformat(), row] for day, row in rows]})}
    return receipt | {"receipt_sha256": archive_evidence_hash(receipt)}


def _verified_page(conn, reader, manifest):
    verified = ColdHistoryReader(reader.object_store, limits=reader.limits,
        ensure_contracts=lambda contracts: ensure_payload_contracts(conn, contracts)).read_page(manifest)
    ids = {row["id"] for row in verified}
    commits = {row["market_commit_seq"] for row in verified}
    if len(ids) != len(verified) or len(commits) != len(verified):
        raise RuntimeError("reverse projection page contains duplicate identities or commits")
    return verified


def _write_exact_page(conn, rows):
    for day in sorted({day for day, _ in rows}):
        _existing_destination_day(conn, day)

    header, payload = MarketFactVersionRecord.__table__, MarketFactHotPayloadRecord.__table__
    inserted = 0
    with conn.begin_nested():
        conn.exec_driver_sql("SET CONSTRAINTS market.trg_require_fact_identity_header, market.trg_require_fact_hot_payload DEFERRED")
        for day, row in rows:
            actual = _exact_row(conn, day=day, identity=row["id"])
            if actual is not None:
                _assert_exact(conn, day=day, row=row)
                continue
            values = row | {"storage_day": day}
            conn.execute(insert(header), {name: values[name] for name in header.columns.keys()})
            conn.execute(insert(payload), {name: values[name] for name in payload.columns.keys()})
            inserted += 1
        # Run the inherited deferred identity/header and required-payload
        # guards before returning a result; leave the original default timing.
        conn.exec_driver_sql("SET CONSTRAINTS market.trg_require_fact_identity_header, market.trg_require_fact_hot_payload IMMEDIATE")
        for day, row in rows:
            _assert_exact(conn, day=day, row=row)
        conn.exec_driver_sql("SET CONSTRAINTS market.trg_require_fact_identity_header, market.trg_require_fact_hot_payload DEFERRED")
    return inserted


def restore_candidate_only_pages(conn, *, fence, expected_database, expected_execution_hash,
                                 expected_source_commit, reader, max_pages, max_bytes, timeout_seconds=120):
    """Project every post-switch direct-cold page back to SQL before reattaching legacy.

    Use only on the fenced latest rollback clone. SQL and cold placement can
    coexist while isolated. Keep the writer fence and do not reopen until the
    old catalog and authority are completely restored.
    """
    _require_disposable_fence(conn, fence)
    if (type(expected_source_commit) is not int or expected_source_commit < 1
            or type(max_pages) is not int or not 1 <= max_pages <= 16_384
            or type(max_bytes) is not int or not 1 <= max_bytes <= 128 * 1024**3
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_MIGRATION_SECONDS):
        raise ValueError("reverse projection candidate page budget invalid")

    with archive_namespace(reader.object_store.root, exclusive=True) as check_archive, _bounded_step(conn, timeout_seconds) as bound:
        bound(max(0, fence.deadline - monotonic()))
        fence.assert_quiet(conn)
        _layout_guard(conn, ReversePage("", "", expected_source_commit, expected_database), min(timeout_seconds, 120) * 1000,
                      layout=COLD_HISTORY_LAYOUT_VERSION)
        certificate = conn.scalar(text("SELECT evidence FROM market.fact_storage_state"))
        initial = certificate.get("source_commit_seq") if isinstance(certificate, dict) else None
        if (not isinstance(certificate, dict) or certificate.get("execution_hash") != expected_execution_hash
                or type(initial) is not int or not 0 < initial <= expected_source_commit
                or PostgresMarketDataRepository._current_commit_seq_with_session(conn) != expected_source_commit):
            raise RuntimeError("reverse projection candidate authority changed")
        receipt, inserted = _candidate_page_receipt(
            conn, reader=reader, expected_database=expected_database, execution_hash=expected_execution_hash,
            source_commit=expected_source_commit, initial=initial, max_pages=max_pages, max_bytes=max_bytes,
            check=lambda: (fence.check_clock(), check_archive()), write=True,
        )
        check_archive()
        fence.assert_quiet(conn)
    return receipt | {"inserted": inserted}


def verify_candidate_only_pages(conn, *, reader, receipt, initial, check):
    """Re-read the complete current page set and its exact SQL projection."""
    observed, _ = _candidate_page_receipt(
        conn, reader=reader, expected_database=receipt["target_database"], execution_hash=receipt["execution_hash"],
        source_commit=receipt["source_commit"], initial=initial, max_pages=receipt["page_count"],
        max_bytes=receipt["archive_bytes"], check=check, write=False,
    )
    if observed != {key: value for key, value in receipt.items() if key != "inserted"}:
        raise RuntimeError("reverse projection candidate receipts differ from stored pages")
    return observed["receipt_sha256"]


def _candidate_page_receipt(conn, *, reader, expected_database, execution_hash, source_commit, initial,
                            max_pages, max_bytes, check, write):
    pages = conn.execute(text("""
            SELECT * FROM market.cold_fact_pages WHERE staged_storage_day IS NULL AND last_commit_seq>:initial
            ORDER BY first_commit_seq,id COLLATE "C" LIMIT :limit
    """), {"initial": initial, "limit": max_pages + 1}).mappings().all()
    archive_bytes = sum(page["byte_count"] for page in pages)
    if len(pages) > max_pages or archive_bytes > max_bytes:
        raise RuntimeError("reverse projection candidate page budget exceeded")

    digest = archive_evidence_hash({"schema": VERSION, "source_commit": source_commit})
    prior_commit, inserted, verified = initial, 0, 0
    for page in pages:
        check()
        manifest = _manifest(page)
        if not prior_commit < manifest.first_cursor[0] <= manifest.last_cursor[0] <= source_commit:
            raise RuntimeError("reverse projection candidate page commit range changed")
        rows = _verified_page(conn, reader, manifest)
        _assert_candidate_locators(conn, rows, manifest)
        dated = [(row["accepted_at"].astimezone(UTC).date(), row) for row in rows]
        if write:
            for day in sorted({day for day, _ in dated}):
                ensure_fact_payload_partition(conn, day)
            inserted += _write_exact_page(conn, dated)
        else:
            for day, row in dated:
                check()
                _assert_exact(conn, day=day, row=row)
        verified += len(dated)
        prior_commit = manifest.last_cursor[0]
        digest = archive_evidence_hash({"prior": digest, "manifest": manifest.manifest_hash,
                                        "placement": [[day.isoformat(), row["id"]] for day, row in dated]})
    receipt = {"schema": VERSION, "target_database": expected_database, "source_commit": source_commit,
               "execution_hash": execution_hash, "page_count": len(pages), "row_count": verified,
               "ordered_content_hash": digest, "archive_bytes": archive_bytes}
    return receipt | {"receipt_sha256": archive_evidence_hash(receipt)}, inserted


def _assert_candidate_locators(conn, rows, manifest):
    locators = _locators(conn, manifest)
    if len(locators) != len(rows):
        raise RuntimeError("reverse projection candidate locator count differs")
    for ordinal, (row, locator) in enumerate(zip(rows, locators, strict=True)):
        if (locator["id"], locator["ordinal"], locator["series_id"], locator["revision"], locator["market_commit_seq"],
                bytes(locator["key_digest"]), bytes(locator["row_hash"])) != (
                row["id"], ordinal, row["series_id"], row["revision"], row["market_commit_seq"],
                sha256(row["observation_key"].encode()).digest(), bytes.fromhex(row["row_hash"])):
            raise RuntimeError(f"reverse projection candidate locator differs: {row['id']}")
