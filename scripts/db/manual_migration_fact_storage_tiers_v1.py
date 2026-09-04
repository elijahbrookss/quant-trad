#!/usr/bin/env python3
"""Explicit, resumable offline cutover of fact payloads into daily partitions.

Default: read-only inspection. --execute --writers-stopped prepares/copies a
bounded number of pages. Rerun until ready. The immutable old table is retained
for operator rollback; this command never drops the source or enables retention.
Run PR #196/#197's required migrations first. PG_DSN is the only connection input.
On an already-ready older tiered layout, the same explicit execution adds only
missing empty proof tables. It never backfills verification receipts.
"""
from __future__ import annotations

import argparse
import json
import os

from sqlalchemy import create_engine, inspect, text

from portal.backend.db import Base, MarketFactVersionRecord
from portal.backend.db.fact_storage_schema import (
    FACT_STORAGE_LAYOUT_VERSION, FACT_STORAGE_TABLES, FACT_STORAGE_IMMUTABLE_TABLES,
    FACT_BOOK_PREFIX_TABLES, FACT_CANONICAL_DEPENDENCY_TABLES,
    assert_fact_storage_contract, ensure_fact_payload_partition, install_fact_storage_functions,
)
from portal.backend.db.session import Database

SOURCE_SCHEMA = "qt_fact_storage_cutover_v1"
SOURCE = SOURCE_SCHEMA + ".fact_versions"
LOCK_NAME = "quant-trad:fact-storage-cutover:v1"
HEADER_COLUMNS = tuple(column.name for column in MarketFactVersionRecord.__table__.columns)
LEGACY_COLUMNS = tuple(name for name in HEADER_COLUMNS if name != "storage_day") + ("payload", "provenance", "quality")
PAGE_PREDICATE = "(market_commit_seq, id) > (:after_seq, :after_id) AND (market_commit_seq, id) <= (:until_seq, :until_id)"


def _relation(conn, name):
    return conn.execute(text("SELECT to_regclass(:name)"), {"name": name}).scalar_one_or_none()


def _state(conn):
    if _relation(conn, "market.fact_storage_state") is None:
        return None
    row = conn.execute(text(
        "SELECT state, evidence FROM market.fact_storage_state WHERE layout_version=:version"
    ), {"version": FACT_STORAGE_LAYOUT_VERSION}).mappings().one_or_none()
    if row is None:
        raise RuntimeError("fact_storage_cutover_certificate_missing")
    return dict(row)


def _assert_source(conn, name):
    schema, table = name.split(".")
    columns = {column["name"] for column in inspect(conn).get_columns(table, schema=schema)}
    if columns != set(LEGACY_COLUMNS):
        raise RuntimeError("fact_storage_cutover_source_layout_mismatch: run the earlier canonical migrations first")


def _missing_proof_tables(conn):
    prefix = [name for name in FACT_BOOK_PREFIX_TABLES if _relation(conn, "market." + name) is None]
    if prefix and len(prefix) != len(FACT_BOOK_PREFIX_TABLES):
        raise RuntimeError("fact_storage_book_prefix_partial_layout: inspect before retrying")
    canonical = [name for name in FACT_CANONICAL_DEPENDENCY_TABLES if _relation(conn, "market." + name) is None]
    return prefix, canonical


def _assert_prior_ready_layout(conn, prefix, canonical):
    assert_fact_storage_contract(conn, allow_missing_book_prefix_tables=bool(prefix),
                                 allow_missing_canonical_dependency_tables=bool(canonical))


def inspect_cutover(conn):
    """Read-only catalog/preflight report; does not acquire DDL or mutation locks."""
    state = _state(conn)
    source = SOURCE if _relation(conn, SOURCE) else "market.fact_versions"
    if state and state["state"] == "ready":
        prefix, canonical = _missing_proof_tables(conn)
        if prefix or canonical:
            _assert_prior_ready_layout(conn, prefix, canonical)
            status = ("proof_metadata_required" if prefix and canonical else
                      "book_prefix_metadata_required" if prefix else "canonical_dependency_metadata_required")
            return {"status": status, "missing_tables": prefix + canonical,
                    "source_retained": bool(_relation(conn, SOURCE)), "evidence": state["evidence"]}
        assert_fact_storage_contract(conn)
        return {"status": "ready", "source_retained": bool(_relation(conn, SOURCE)),
                "evidence": state["evidence"]}
    if _relation(conn, source) is None:
        raise RuntimeError("fact_storage_cutover_source_missing")
    _assert_source(conn, source)
    counts = conn.execute(text(
        f"SELECT count(*) AS source_rows, pg_total_relation_size('{source}') AS source_bytes FROM {source}"
    )).mappings().one()
    return {"status": "copying" if state else "not_started", **dict(counts),
            "source": source, "evidence": state["evidence"] if state else {},
            "capacity_note": "Requires room for both layouts plus WAL. Source is not automatically removed."}


def _prepare(conn):
    state = _state(conn)
    if state is not None:
        prefix, canonical = _missing_proof_tables(conn)
        missing = prefix + canonical
        if missing:
            if state["state"] != "ready":
                raise RuntimeError("fact_storage_proof_partial_layout: finish the prior cutover before upgrading")
            _assert_prior_ready_layout(conn, prefix, canonical)
            conn.execute(text("SET LOCAL lock_timeout='5s'"))
            for table in Base.metadata.sorted_tables:
                if table.schema == "market" and table.name in missing:
                    table.create(conn)
                    conn.execute(text(
                        f"CREATE TRIGGER trg_reject_mutation_{table.name} BEFORE UPDATE OR DELETE ON market.{table.name} "
                        "FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()"
                    ))
            assert_fact_storage_contract(conn)
        return
    _assert_source(conn, "market.fact_versions")
    if _relation(conn, SOURCE) is not None:
        raise RuntimeError("fact_storage_cutover_unregistered_source")
    for name in FACT_STORAGE_TABLES:
        if _relation(conn, "market." + name) is not None:
            raise RuntimeError(f"fact_storage_cutover_unregistered_target: market.{name}")
    # The old relation must not be rebound underneath foreign keys or SQL views.
    incoming = conn.execute(text(
        "SELECT count(*) FROM pg_constraint WHERE contype='f' AND confrelid='market.fact_versions'::regclass"
    )).scalar_one()
    views = conn.execute(text(
        "SELECT count(*) FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid "
        "WHERE d.refobjid='market.fact_versions'::regclass"
    )).scalar_one()
    if incoming or views:
        raise RuntimeError("fact_storage_cutover_external_dependents: inspect foreign keys/views before cutover")
    conn.execute(text("SET LOCAL lock_timeout='5s'"))
    conn.execute(text("LOCK TABLE market.fact_versions IN ACCESS EXCLUSIVE MODE"))
    # Verify the already-required operational migration before changing layout.
    Database("")._assert_book_operational_rollup_migration(conn)
    count = conn.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one()
    conn.execute(text(f"CREATE SCHEMA {SOURCE_SCHEMA}"))
    conn.execute(text(f"ALTER TABLE market.fact_versions SET SCHEMA {SOURCE_SCHEMA}"))
    conn.execute(text(
        f"CREATE TRIGGER trg_storage_cutover_reject_insert BEFORE INSERT ON {SOURCE} "
        "FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()"
    ))
    conn.execute(text(f"ALTER TABLE {SOURCE} ENABLE ALWAYS TRIGGER trg_storage_cutover_reject_insert"))
    conn.execute(text(f"ALTER TABLE {SOURCE} ENABLE ALWAYS TRIGGER trg_reject_mutation_fact_versions"))
    # Clean definitions only. No ALTER-column/backfill path is installed in runtime.
    MarketFactVersionRecord.__table__.create(conn)
    for table in Base.metadata.sorted_tables:
        if table.schema == "market" and table.name in FACT_STORAGE_TABLES:
            table.create(conn)
    Database("")._ensure_canonical_fact_insert_trigger(conn)
    install_fact_storage_functions(conn)
    for name in ("fact_versions", *FACT_STORAGE_IMMUTABLE_TABLES):
        conn.execute(text(
            f"CREATE TRIGGER trg_reject_mutation_{name} BEFORE UPDATE OR DELETE ON market.{name} "
            "FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()"
        ))
    evidence = {"source_rows": int(count), "copied_rows": 0, "after_seq": 0, "after_id": "",
                "source_relation": SOURCE, "source_retained": True}
    conn.execute(text(
        "INSERT INTO market.fact_storage_state(layout_version,state,evidence) "
        "VALUES (:version,'copying',CAST(:evidence AS jsonb))"
    ), {"version": FACT_STORAGE_LAYOUT_VERSION, "evidence": json.dumps(evidence)})


def _copy_page(conn, batch_rows):
    row = conn.execute(text(
        "SELECT state, evidence FROM market.fact_storage_state WHERE layout_version=:version FOR UPDATE"
    ), {"version": FACT_STORAGE_LAYOUT_VERSION}).mappings().one()
    if row["state"] == "ready":
        return False
    evidence = dict(row["evidence"])
    if evidence.get("source_relation") != SOURCE:
        raise RuntimeError("fact_storage_cutover_source_certificate_mismatch")
    # Old code cannot append after the preparation transaction. A missing guard
    # is a hard failure, not permission to make a moving-source copy.
    enabled = conn.execute(text(
        "SELECT tgname, tgenabled FROM pg_trigger WHERE tgrelid=to_regclass(:source) "
        "AND tgname IN ('trg_storage_cutover_reject_insert','trg_reject_mutation_fact_versions')"
    ), {"source": SOURCE}).all()
    if dict(enabled) != {"trg_storage_cutover_reject_insert": "A", "trg_reject_mutation_fact_versions": "A"}:
        raise RuntimeError("fact_storage_cutover_source_not_fenced")
    params = {"after_seq": evidence["after_seq"], "after_id": evidence["after_id"], "limit": batch_rows}
    boundary = conn.execute(text(
        f"SELECT market_commit_seq, id FROM {SOURCE} WHERE (market_commit_seq,id) > (:after_seq,:after_id) "
        "ORDER BY market_commit_seq,id LIMIT :limit"
    ), params).all()
    if not boundary:
        target_count = conn.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one()
        hot_count = conn.execute(text("SELECT count(*) FROM market.fact_hot_payloads")).scalar_one()
        source_count = conn.execute(text(f"SELECT count(*) FROM {SOURCE}")).scalar_one()
        if not (target_count == hot_count == source_count == evidence["copied_rows"] == evidence["source_rows"]):
            raise RuntimeError("fact_storage_cutover_count_mismatch: source retained; runtime remains disabled")
        evidence["verified_rows"] = evidence["copied_rows"]
        conn.execute(text(
            "UPDATE market.fact_storage_state SET state='ready', completed_at=now(), evidence=CAST(:evidence AS jsonb) "
            "WHERE layout_version=:version"
        ), {"version": FACT_STORAGE_LAYOUT_VERSION, "evidence": json.dumps(evidence)})
        assert_fact_storage_contract(conn)
        return False
    params.update(until_seq=boundary[-1][0], until_id=boundary[-1][1])
    days = conn.execute(text(
        f"SELECT DISTINCT (accepted_at AT TIME ZONE 'UTC')::date FROM {SOURCE} WHERE {PAGE_PREDICATE}"
    ), params).scalars().all()
    for day in sorted(days):
        ensure_fact_payload_partition(conn, day)
    projection = ", ".join(
        "(accepted_at AT TIME ZONE 'UTC')::date" if name == "storage_day" else name for name in HEADER_COLUMNS
    )
    conn.execute(text(
        f"INSERT INTO market.fact_versions ({', '.join(HEADER_COLUMNS)}) SELECT {projection} "
        f"FROM {SOURCE} WHERE {PAGE_PREDICATE} ORDER BY market_commit_seq,id"
    ), params)
    conn.execute(text(
        "INSERT INTO market.fact_hot_payloads "
        "(storage_day,id,series_id,payload_schema_id,observation_time,payload,provenance,quality) "
        f"SELECT (accepted_at AT TIME ZONE 'UTC')::date,id,series_id,payload_schema_id,observation_time,payload,provenance,quality "
        f"FROM {SOURCE} WHERE {PAGE_PREDICATE}"
    ), params)
    # Compare all persisted canonical fields, not merely count/hash metadata.
    # storage_day is placement, and is deliberately excluded from market identity.
    differs = conn.execute(text(
        f"SELECT count(*) FROM (SELECT * FROM {SOURCE} WHERE {PAGE_PREDICATE}) old "
        "LEFT JOIN market.fact_rows new ON new.id=old.id "
        "WHERE to_jsonb(old) IS DISTINCT FROM (to_jsonb(new) - 'storage_day')"
    ), params).scalar_one()
    if differs:
        raise RuntimeError(f"fact_storage_cutover_row_mismatch: rows={differs}; page rolled back")
    evidence.update(after_seq=int(boundary[-1][0]), after_id=boundary[-1][1],
                    copied_rows=int(evidence["copied_rows"]) + len(boundary))
    conn.execute(text(
        "UPDATE market.fact_storage_state SET evidence=CAST(:evidence AS jsonb) WHERE layout_version=:version"
    ), {"version": FACT_STORAGE_LAYOUT_VERSION, "evidence": json.dumps(evidence)})
    return True


def run_cutover(engine, *, execute=False, writers_stopped=False, batch_rows=2000, max_pages=100):
    if type(batch_rows) is not int or not 1 <= batch_rows <= 10000:
        raise ValueError("fact_storage_cutover_batch_rows_out_of_range")
    if type(max_pages) is not int or not 1 <= max_pages <= 10000:
        raise ValueError("fact_storage_cutover_max_pages_out_of_range")
    if not execute:
        with engine.begin() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            return inspect_cutover(conn)
    if not writers_stopped:
        raise ValueError("fact_storage_cutover_requires_writers_stopped_acknowledgement")
    with engine.connect() as conn:
        try:
            locked = conn.execute(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"),
                                  {"name": LOCK_NAME}).scalar_one()
            conn.commit()
        except BaseException:
            conn.invalidate()
            raise
        if not locked:
            raise RuntimeError("fact_storage_cutover_already_running")
        try:
            with conn.begin():
                _prepare(conn)
            for _ in range(max_pages):
                with conn.begin():
                    more = _copy_page(conn, batch_rows)
                if not more:
                    break
            with conn.begin():
                return inspect_cutover(conn)
        finally:
            if not conn.invalidated:
                try:
                    if conn.in_transaction():
                        conn.rollback()
                    released = conn.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"),
                                            {"name": LOCK_NAME}).scalar_one()
                    conn.commit()
                    if not released:
                        raise RuntimeError("fact_storage_cutover_unlock_failed")
                except BaseException:
                    conn.invalidate()
                    raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--writers-stopped", action="store_true")
    parser.add_argument("--batch-rows", type=int, default=2000)
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args()
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        parser.error("PG_DSN is required; no dotenv or alternate connection setting is used")
    engine = create_engine(dsn, future=True, pool_pre_ping=True)
    try:
        result = run_cutover(engine, execute=args.execute, writers_stopped=args.writers_stopped,
                             batch_rows=args.batch_rows, max_pages=args.max_pages)
        print(json.dumps(result, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
