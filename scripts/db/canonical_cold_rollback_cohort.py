"""Prove one original-layout cohort survived isolated physical restore."""
from __future__ import annotations

from datetime import date
from hashlib import sha256
from itertools import islice
from math import isfinite
from time import monotonic

from sqlalchemy import text

from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import FactArchiveLimits, archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_identity_schema import _partition_bound
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract, fact_partition_name
from portal.backend.service.storage.repos.cold_history import _manifest
from portal.backend.service.storage.repos.fact_archival import _partition_manifest_set_hash
from portal.backend.service.storage.repos.fact_storage import ensure_payload_contracts
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.db.canonical_cold_audit import _assert_page_series, _source_rows
from scripts.db.canonical_cold_migration import DOCS, INITIAL, PostgresMigrationSource, _compare_existing
from scripts.db.canonical_cold_paused_donor import PausedRollbackDonor
from scripts.db.canonical_cold_receipts import (
    RECEIPT_SET_VERSION, _forward_day, _identity, _prefix_days, _validate_common, manifest_inventory_hash,
)


VERSION = "qt.canonical_cold_rollback_cohort.v1"


def _require_snapshot(conn, expected, *, paused_donor=None):
    url = conn.engine.url
    if (not _identity(expected) or (paused_donor is None and not expected["database"].startswith(("qt_bench_", "qt_migration_")))
            or url.host not in {None, "timescaledb", "127.0.0.1", "::1"}
            or (url.query.get("host") is not None and not str(url.query["host"]).startswith("/"))):
        raise ValueError("canonical_cold_rollback_requires_isolated_clone")
    if paused_donor is not None:
        if type(paused_donor) is not PausedRollbackDonor:
            raise ValueError("canonical_cold_rollback_invalid_paused_donor_binding")
        paused_donor.assert_snapshot(conn, expected)
    if (not conn.in_transaction() or conn.get_isolation_level() != "REPEATABLE READ"
            or conn.scalar(text("SHOW transaction_read_only")) != "on"):
        raise ValueError("canonical_cold_rollback_requires_read_only_repeatable_snapshot")
    source = PostgresMigrationSource(conn.engine, expected_system_identifier=expected["system_identifier"])
    if source.identity(conn) != expected:
        raise RuntimeError("canonical_cold_rollback_database_identity_changed")
    return source


def _certificate(conn):
    rows = conn.execute(text("""
        SELECT layout_version,state,completed_at,evidence FROM market.fact_storage_state
        ORDER BY layout_version LIMIT 2
    """)).all()
    if len(rows) != 1 or rows[0].state != "ready" or rows[0].completed_at is None:
        raise RuntimeError("canonical_cold_rollback_layout_not_ready")
    return rows[0].layout_version, rows[0].evidence


def inspect_old_cohort(conn, *, day, header, header_oid, state):
    """Return a placement digest stable across physical tablespace remapping."""
    if (type(day) is not date or type(header_oid) is not int or header_oid <= 0
            or state not in {"sealed", "verified", "reclaimed"}):
        raise ValueError("canonical_cold_rollback_old_placement_scope_invalid")
    payload = fact_partition_name(day)
    day_bound = _partition_bound(day)
    legacy_end = conn.scalar(text("SELECT market.fact_header_legacy_end_day()"))
    legacy = header == "fact_versions_legacy"
    if legacy and (legacy_end is None or day >= legacy_end):
        raise RuntimeError("canonical_cold_rollback_legacy_day_outside_bound")
    header_bound = f"FOR VALUES FROM (MINVALUE) TO ('{legacy_end.isoformat()}')" if legacy else day_bound
    if not legacy and not conn.scalar(text("""
        SELECT EXISTS (SELECT FROM market.fact_header_partitions WHERE storage_day=:day)
    """), {"day": day}):
        raise RuntimeError("canonical_cold_rollback_header_registry_missing")
    registered = conn.execute(text("""
        SELECT c.relname,c.oid::bigint, i.inhparent::regclass::text AS parent,
               pg_get_expr(c.relpartbound,c.oid) AS bound,c.relkind,c.relpersistence
        FROM pg_class c LEFT JOIN pg_inherits i ON i.inhrelid=c.oid
        WHERE c.oid IN (to_regclass(:header),to_regclass(:payload)) LIMIT 3
    """), {"header": f"market.{header}", "payload": f"market.{payload}"}).all()
    expected = {header: (header_oid, "market.fact_versions", header_bound, "r", "p")}
    if state != "reclaimed":
        payload_row = next((row for row in registered if row.relname == payload), None)
        if payload_row is None:
            raise RuntimeError("canonical_cold_rollback_payload_relation_missing")
        expected[payload] = (payload_row.oid, "market.fact_hot_payloads", day_bound, "r", "p")
    if len(registered) != len(expected) or {row[0]: tuple(row[1:]) for row in registered} != expected:
        raise RuntimeError("canonical_cold_rollback_old_partition_binding_changed")
    relation_oids = [row[1] for row in registered]
    components = conn.execute(text("""
        WITH heaps AS (
            SELECT c.relname AS owner,c.oid,'heap' AS kind FROM pg_class c WHERE c.oid=ANY(CAST(:oids AS oid[]))
            UNION ALL SELECT c.relname,c.reltoastrelid,'toast' FROM pg_class c
                       WHERE c.oid=ANY(CAST(:oids AS oid[])) AND c.reltoastrelid<>0
        ), components AS (
            SELECT * FROM heaps
            UNION ALL SELECT h.owner,i.indexrelid,'index' FROM heaps h JOIN pg_index i ON i.indrelid=h.oid
        )
        SELECT m.owner,m.kind,c.oid::bigint,c.relname,t.oid::bigint AS tablespace_oid,t.spcname AS tablespace,
               c.relpersistence, COALESCE(i.indisvalid AND i.indisready AND i.indislive,true) AS valid
        FROM components m JOIN pg_class c ON c.oid=m.oid
        LEFT JOIN pg_index i ON i.indexrelid=c.oid
        CROSS JOIN pg_database d
        JOIN pg_tablespace t ON t.oid=COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)
        WHERE d.datname=current_database() ORDER BY m.owner,m.kind,c.oid LIMIT 129
    """), {"oids": relation_oids}).mappings().all()
    if len(components) > 128 or any(row["relpersistence"] != "p" or not row["valid"] for row in components):
        raise RuntimeError("canonical_cold_rollback_invalid_placement_members")
    placement = {"day": day.isoformat(), "state": state, "relations": [dict(row) for row in components]}
    return placement | {"placement_hash": archive_evidence_hash(placement)}


def _approved_pages(audit, receipts, day, max_pages):
    if (audit.get("schema") != RECEIPT_SET_VERSION
            or archive_evidence_hash({key: value for key, value in audit.items() if key != "audit_set_hash"})
            != audit.get("audit_set_hash")):
        raise ValueError("canonical_cold_rollback_audit_hash_mismatch")
    day_id = day.isoformat()
    child = _prefix_days(audit["prefix"]).get(day_id)
    detail = audit["days"].get(day_id)
    if child is None or detail is None or not detail["expected_pages"] or detail["expected_pages"] > max_pages:
        raise ValueError("canonical_cold_rollback_cohort_outside_nonempty_page_budget")
    batches = []
    for receipt in islice(receipts, max_pages + 1):
        common = _validate_common(receipt)
        batch = receipt.get("day_batch")
        if (receipt["receipt_hash"] not in audit["receipt_hashes"]
                or any(audit[key] != value for key, value in common.items())
                or batch is None or batch["storage_day"] != day_id):
            raise ValueError("canonical_cold_rollback_unbound_day_receipt")
        batches.append(batch)
    if not batches or len(batches) > max_pages or sum(len(batch["page_receipts"]) for batch in batches) > max_pages:
        raise ValueError("canonical_cold_rollback_day_receipt_budget")
    if _forward_day(day_id, batches, child["child"], {}) != detail:
        raise ValueError("canonical_cold_rollback_day_summary_changed")
    pages = []
    for batch in batches:
        authorities = {row["ordinal"]: row["authority_hash"] for row in batch["source_archive_authority_receipts"]}
        for page in batch["page_receipts"]:
            pages.append((page, authorities.get(page["ordinal"])))
    return child, detail, pages


def _assert_candidate_inventory(conn, *, source_commit, expected, max_pages, check_budget):
    def entries():
        count, after = 0, ""
        while True:
            check_budget(conn)
            rows = conn.execute(text("""
                SELECT id,manifest_hash,row_count,last_commit_seq,staged_storage_day
                FROM market.cold_fact_pages WHERE id>:after AND first_commit_seq<=:commit
                ORDER BY id LIMIT 512
            """), {"after": after, "commit": source_commit}).all()
            for row in rows:
                count += 1
                if count > max_pages or row.last_commit_seq > source_commit or row.staged_storage_day is not None:
                    raise RuntimeError("canonical_cold_rollback_candidate_inventory_exceeds_initial_scope")
                yield (row.id, row.manifest_hash, row.row_count)
                after = row.id
            if len(rows) < 512:
                break
    if manifest_inventory_hash(entries()) != expected:
        raise RuntimeError("canonical_cold_rollback_candidate_inventory_changed")


def _compare_page(original, candidate, source, old_reader, cold_reader, *, cohort, header_oid, page, authority):
    stored = candidate.execute(text("SELECT * FROM market.cold_fact_pages WHERE id=:id"),
                               {"id": page["manifest_id"]}).mappings().one_or_none()
    if stored is None or stored["staged_storage_day"] is not None:
        raise RuntimeError("canonical_cold_rollback_candidate_page_missing")
    manifest = _manifest(stored)
    if (manifest.manifest_hash, manifest.row_count, manifest.first_cursor, manifest.last_cursor) != (
        page["manifest_hash"], page["row_count"], tuple(page["first_cursor"]), tuple(page["last_cursor"]),
    ):
        raise RuntimeError("canonical_cold_rollback_candidate_page_changed")
    rows = cold_reader.read_page(manifest)
    after = tuple(page["after"])
    old_rows = source.rows(original, cohort.day, after, through=manifest.last_cursor, limits=old_reader.limits)
    headers = _source_rows(original, day=cohort.day, after=after, through=manifest.last_cursor,
                           limit=manifest.row_count + 1)
    locators = candidate.execute(text("""
        SELECT id,ordinal,series_id,revision,market_commit_seq,encode(key_digest,'hex') AS key_digest,
               encode(row_hash,'hex') AS row_hash FROM market.cold_fact_locators
        WHERE page_id=:page ORDER BY ordinal LIMIT :limit
    """), {"page": manifest.manifest_id, "limit": manifest.row_count + 1}).all()
    _compare_existing(old_rows, rows, manifest=manifest, cohort=cohort, after=after, limits=old_reader.limits)
    if len(headers) != len(rows) or len(locators) != len(rows):
        raise RuntimeError("canonical_cold_rollback_page_coverage_changed")
    for ordinal, (row, header, locator, old) in enumerate(zip(rows, headers, locators, old_rows, strict=True)):
        identity = (row["id"], row["series_id"], row["observation_key"], row["revision"], cohort.day)
        if (tuple(header[field] for field in ("identity_id", "identity_series_id", "identity_key",
                                               "identity_revision", "identity_day")) != identity
                or header["child_oid"] != header_oid
                or any((old[key] is None) != (cohort.state == "reclaimed") for key in DOCS)
                or tuple(locator) != (row["id"], ordinal, row["series_id"], row["revision"], row["market_commit_seq"],
                                      sha256(row["observation_key"].encode()).hexdigest(), row["row_hash"])):
            raise RuntimeError(f"canonical_cold_rollback_identity_or_placement_changed: id={row['id']}")
    _assert_page_series(candidate, manifest, rows)
    if authority is None:
        if cohort.state != "sealed":
            raise RuntimeError("canonical_cold_rollback_old_archive_authority_missing")
        return None
    old_page = source.existing(original, cohort.day, page["ordinal"])
    if old_page.manifest != manifest or archive_evidence_hash(old_page.authority) != authority:
        raise RuntimeError("canonical_cold_rollback_old_archive_authority_changed")
    old_file = old_reader.object_store.local_path(old_page.manifest.object_key)
    new_file = cold_reader.object_store.local_path(manifest.object_key)
    if old_file.samefile(new_file):
        raise RuntimeError("canonical_cold_rollback_archive_copies_share_file_identity")
    if old_reader.read_page(old_page.manifest) != rows:
        raise RuntimeError("canonical_cold_rollback_old_archive_rows_changed")
    return old_page.authority["verification_hash"]


def verify_restored_cohort(original, candidate, *, day, original_identity, candidate_identity, audit,
                           forward_receipts, source_commit, old_store, candidate_store, expected_placement_hash,
                           max_pages=1024, max_inventory_pages=100_000, max_archive_bytes=32 * 1024**3, max_seconds=900,
                           limits=FactArchiveLimits(), paused_donor=None):
    """Recheck full forward and inverse membership without writing to either clone."""
    old_root = old_store.root.resolve(strict=True)
    new_root = candidate_store.root.resolve(strict=True)
    if (type(day) is not date or type(source_commit) is not int or source_commit <= 0
            or type(max_pages) is not int or not 1 <= max_pages <= 4096
            or type(max_inventory_pages) is not int or not max_pages <= max_inventory_pages <= 100_000
            or type(max_archive_bytes) is not int or not 1 <= max_archive_bytes <= 1024**4
            or type(max_seconds) not in {int, float} or not isfinite(max_seconds) or not 0 < max_seconds <= 3600
            or old_root.is_relative_to(new_root) or new_root.is_relative_to(old_root)
            or original_identity == candidate_identity):
        raise ValueError("canonical_cold_rollback_invalid_scope")
    deadline = monotonic() + max_seconds

    def check_budget(conn):
        remaining = int((deadline - monotonic()) * 1000)
        if remaining <= 0:
            raise TimeoutError("canonical_cold_rollback_budget_exhausted")
        conn.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                     {"timeout": str(min(30_000, remaining))})

    for conn in (original, candidate):
        check_budget(conn)
        conn.exec_driver_sql("SET LOCAL lock_timeout='500ms'")
        conn.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
    source = _require_snapshot(original, original_identity, paused_donor=paused_donor)
    _require_snapshot(candidate, candidate_identity)
    child, detail, pages = _approved_pages(audit, forward_receipts, day, max_pages)
    if (source_commit < audit["retired_prefix_max_commit_seq"]
            or child["child_oid"] is None or original_identity["header_oid"] != audit["source_database"]["header_oid"]
            or candidate_identity["header_oid"] != audit["database"]["header_oid"]):
        raise RuntimeError("canonical_cold_rollback_boundary_changed")
    old_layout, _ = _certificate(original)
    new_layout, certificate = _certificate(candidate)
    if (old_layout != FACT_STORAGE_LAYOUT_VERSION or new_layout != COLD_HISTORY_LAYOUT_VERSION
            or not isinstance(certificate, dict)
            or (certificate.get("audit_set_hash"), certificate.get("loaded_inventory_hash"), certificate.get("source_commit_seq"))
            != (audit["audit_set_hash"], audit["loaded_inventory_hash"], source_commit)):
        raise RuntimeError("canonical_cold_rollback_authority_changed")
    if (PostgresMarketDataRepository._current_commit_seq_with_session(original) != source_commit
            or PostgresMarketDataRepository._current_commit_seq_with_session(candidate) < source_commit):
        raise RuntimeError("canonical_cold_rollback_source_watermark_changed")
    retirement = candidate.execute(text("""
        SELECT handoff_kind,manifest_set_hash,row_count FROM market.cold_fact_retired_days WHERE storage_day=:day
    """), {"day": day}).one_or_none()
    if retirement != ("initial", audit["audit_set_hash"], detail["expected_rows"]):
        raise RuntimeError("canonical_cold_rollback_candidate_day_not_initial_retirement")
    if candidate.scalar(text("""
        SELECT to_regclass(:header) IS NOT NULL OR to_regclass(:payload) IS NOT NULL
    """), {"header": f"market.{child['child']}", "payload": f"market.{fact_partition_name(day)}"}):
        raise RuntimeError("canonical_cold_rollback_candidate_old_relations_still_present")
    check_budget(original)
    assert_fact_storage_contract(original)
    cohort = source.cohort(original, day)
    if (cohort.state, cohort.sealed_at, cohort.expected_rows, cohort.reuse_prefix_pages) != (
        detail["source_retention_state"], detail["source_sealed_at"], detail["expected_rows"],
        sum(authority is not None for _, authority in pages),
    ):
        raise RuntimeError("canonical_cold_rollback_old_cohort_changed")
    placement = inspect_old_cohort(original, day=day, header=child["child"], header_oid=child["child_oid"], state=cohort.state)
    if placement["placement_hash"] != expected_placement_hash:
        raise RuntimeError("canonical_cold_rollback_old_placement_changed")
    _assert_candidate_inventory(candidate, source_commit=source_commit, expected=audit["loaded_inventory_hash"],
                                max_pages=max_inventory_pages, check_budget=check_budget)
    expected_reads = {page["manifest_id"]: 2 if authority is not None else 1 for page, authority in pages}
    page_ids = list(expected_reads)
    archived_bytes = 0
    for offset in range(0, len(page_ids), 512):
        check_budget(candidate)
        ids = page_ids[offset:offset + 512]
        archive = candidate.execute(text("SELECT id, byte_count FROM market.cold_fact_pages WHERE id=ANY(:ids)"),
                                    {"ids": ids}).all()
        if len(archive) != len(ids):
            raise RuntimeError("canonical_cold_rollback_candidate_page_missing")
        archived_bytes += sum(expected_reads[row.id] * row.byte_count for row in archive)
        if archived_bytes > max_archive_bytes:
            raise RuntimeError("canonical_cold_rollback_archive_byte_budget_exceeded")
    old_reader = ColdHistoryReader(old_store, limits=limits, ensure_contracts=lambda pairs: ensure_payload_contracts(original, pairs))
    cold_reader = ColdHistoryReader(candidate_store, limits=limits, ensure_contracts=lambda pairs: ensure_payload_contracts(candidate, pairs))
    verifications = []
    for page, authority in pages:
        check_budget(original)
        check_budget(candidate)
        verifications.append(_compare_page(original, candidate, source, old_reader, cold_reader,
                                           cohort=cohort, header_oid=child["child_oid"], page=page, authority=authority))
    check_budget(original)
    if _source_rows(original, day=day, after=tuple(detail["last_cursor"]), through=None, limit=1):
        raise RuntimeError("canonical_cold_rollback_old_cohort_has_extra_rows")
    if cohort.state in {"verified", "reclaimed"}:
        stored_hash = original.scalar(text("SELECT manifest_set_hash FROM market.fact_retention_partitions WHERE storage_day=:day"),
                                      {"day": day})
        if any(digest is None for digest in verifications) or stored_hash != _partition_manifest_set_hash(
            day, cohort.expected_rows, verifications,
        ):
            raise RuntimeError("canonical_cold_rollback_old_archive_day_receipt_changed")
    check_budget(candidate)
    result = {"version": VERSION, "storage_day": day.isoformat(), "original": original_identity,
              "candidate": candidate_identity, "audit_set_hash": audit["audit_set_hash"],
              "loaded_inventory_hash": audit["loaded_inventory_hash"], "verified_archive_bytes": archived_bytes,
              "source_commit_seq": source_commit, "old_placement": placement,
              "verified_rows": cohort.expected_rows, "verified_pages": len(pages),
              "last_cursor": detail["last_cursor"], "patched_rows": 0}
    return result | {"receipt_hash": archive_evidence_hash(result)}
