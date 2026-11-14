"""Isolated reverse-projection primitive for one verified lossless Fact page."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date
from typing import Mapping

from sqlalchemy import insert, text

from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import FactArchiveManifest, archive_evidence_hash
from portal.backend.db.fact_identity_schema import _assert_header_partition, _partition_bound
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract, fact_partition_name
from portal.backend.db.market_data_models import MarketFactVersionRecord
from portal.backend.db.market_storage_models import MarketFactHotPayloadRecord
from portal.backend.service.storage.repos.fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM, ensure_payload_contracts
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db.canonical_cold_migration import PostgresMigrationSource


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


def _layout_guard(conn, plan, timeout_ms):
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
    if state != [(FACT_STORAGE_LAYOUT_VERSION, "ready", True)]:
        raise RuntimeError("reverse projection requires the ready original v2 layout")
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
    # Bind archived contracts to the destination's schema owners; use the
    # actual bounded codec and bypass the optional read-copy cache.
    verified = ColdHistoryReader(reader.object_store, limits=reader.limits,
        ensure_contracts=lambda contracts: ensure_payload_contracts(conn, contracts)).read_page(manifest)
    ids = {row["id"] for row in verified}
    commits = {row["market_commit_seq"] for row in verified}
    if len(ids) != len(verified) or len(commits) != len(verified) or (placement is not None and ids != placement.keys()):
        raise RuntimeError("reverse projection identity or original placement witness set differs")
    rows = [(placement[row["id"]] if placement is not None else row["accepted_at"].astimezone(UTC).date(), row)
            for row in verified]
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
    receipt = {"version": VERSION, "plan_hash": expected_plan_hash, "manifest_hash": manifest.manifest_hash,
               "inserted": inserted, "verified": len(rows), "last_cursor": list(manifest.last_cursor),
               "placement_mode": "original_storage_day" if placement is not None else "candidate_acceptance_utc_day",
               "stored_rows_sha256": archive_evidence_hash({"ordered_rows": [[day.isoformat(), row] for day, row in rows]})}
    return receipt | {"receipt_sha256": archive_evidence_hash(receipt)}
