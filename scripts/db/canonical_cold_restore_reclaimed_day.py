"""Rebuild one recurring cold-retired header day on a fenced rollback clone."""

from __future__ import annotations

from datetime import date
from hashlib import sha256
from time import monotonic

from sqlalchemy import insert, text

from market_data.archive_namespace import archive_namespace
from market_data.archive_verification import ArchiveVerificationBatch
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_identity_schema import (
    _assert_header_partition, assert_fact_identity_contract, ensure_fact_header_partition, fact_header_partition_name,
)
from portal.backend.db.fact_storage_schema import assert_fact_storage_contract, fact_partition_name
from portal.backend.db.market_data_models import MarketFactVersionRecord
from portal.backend.service.storage.repos.cold_history import _manifest
from portal.backend.service.storage.repos.fact_archival import _partition_manifest_set_hash
from portal.backend.service.storage.repos.fact_storage import _catalog_manifest, ensure_payload_contracts
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db.canonical_cold_audit import _locators, _source_rows
from scripts.db.canonical_cold_migration import INITIAL, PostgresMigrationSource, _compare_existing
from scripts.db.canonical_cold_restore_references import _database_identity, _ready, _require_disposable_fence
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_restore_reclaimed_day.v1"


def _cohort(conn, day):
    boundary = conn.execute(text("""
        SELECT cold.row_count,cold.manifest_set_hash,old.expected_rows,
               old.manifest_set_hash AS original_manifest_set_hash,old.state
        FROM market.cold_fact_retired_days cold
        JOIN market.fact_retention_partitions old USING(storage_day)
        WHERE cold.storage_day=:day AND cold.handoff_kind='recurring'
    """), {"day": day}).one_or_none()
    if (boundary is None or boundary.row_count != boundary.expected_rows
            or boundary.manifest_set_hash != boundary.original_manifest_set_hash or boundary.state != "reclaimed"
            or conn.scalar(text("SELECT to_regclass(:name) IS NOT NULL"),
                           {"name": "market." + fact_partition_name(day)})):
        raise RuntimeError(f"canonical_cold_recovery_reclaimed_day_changed: {day}")
    retention = conn.scalar(text("SELECT to_jsonb(p) FROM market.fact_retention_partitions p WHERE storage_day=:day"),
                            {"day": day})
    return boundary.row_count, boundary.manifest_set_hash, archive_evidence_hash(retention)


def _page_rows(conn, reader, manifest, day, *, budget):
    loaded = conn.execute(text("""
        SELECT *
        FROM market.cold_fact_pages WHERE id=:id
    """), {"id": manifest.manifest_id}).mappings().one_or_none()
    if loaded is None or loaded["staged_storage_day"] != day or _manifest(loaded) != manifest:
        raise RuntimeError(f"canonical_cold_recovery_reclaimed_manifest_changed: {manifest.manifest_id}")
    budget.verify(manifest.object_key, manifest.object_sha256, expected_bytes=manifest.byte_count)
    rows = reader.read_page(manifest)
    locators = _locators(conn, manifest)
    if len(rows) != manifest.row_count or len(locators) != len(rows):
        raise RuntimeError(f"canonical_cold_recovery_reclaimed_locator_count_changed: {manifest.manifest_id}")
    for ordinal, (row, locator) in enumerate(zip(rows, locators, strict=True)):
        expected = (row["id"], manifest.manifest_id, ordinal, row["series_id"],
                    sha256(row["observation_key"].encode()).digest(), row["revision"],
                    row["market_commit_seq"], bytes.fromhex(row["row_hash"]))
        actual = (locator["id"], locator["page_id"], locator["ordinal"], locator["series_id"],
                  bytes(locator["key_digest"]), locator["revision"], locator["market_commit_seq"], bytes(locator["row_hash"]))
        if actual != expected:
            raise RuntimeError(f"canonical_cold_recovery_reclaimed_locator_changed: {row['id']}")
    return rows


def restore_reclaimed_header_day(conn, *, fence, day, expected_manifest_set_hash,
                                 store, limits, object_limits, timeout_seconds=120):
    """Recover v2 headers and identities while retaining verified Parquet custody.

    Keep the database closed throughout inverse layout recovery. The caller
    must separately verify that raw/checkpoint objects and archive dependencies
    are available and restore every remaining cohort before changing authority.
    """
    _require_disposable_fence(conn, fence)
    if type(day) is not date:
        raise ValueError("canonical_cold_recovery_reclaimed_day_invalid")
    reader = ColdHistoryReader(store, limits=limits,
                               ensure_contracts=lambda pairs: ensure_payload_contracts(conn, pairs))
    budget = ArchiveVerificationBatch(store, limits=object_limits, check_budget=fence.check_clock)
    header = MarketFactVersionRecord.__table__
    name = fact_header_partition_name(day)
    with archive_namespace(store.root, exclusive=True) as check_archive, _bounded_step(conn, timeout_seconds) as bound:
        bound(max(0, fence.deadline - monotonic()))
        fence.assert_quiet(conn)
        check_archive()
        if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                           {"name": _LIFECYCLE_LOCK_NAME}):
            raise RuntimeError("canonical_cold_recovery_reclaimed_lifecycle_busy")
        conn.exec_driver_sql("LOCK TABLE market.fact_storage_state IN SHARE MODE NOWAIT")
        _ready(conn, COLD_HISTORY_LAYOUT_VERSION)
        # Restoring an initial legacy child and switching the certificate are
        # separate fenced steps. Validate the header namespace without asking
        # the normal serving startup to admit that temporary shape.
        if not conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NOT NULL")):
            assert_fact_storage_contract(conn)
        else:
            assert_fact_identity_contract(conn)
        expected_rows, manifest_set_hash, retention_hash = _cohort(conn, day)
        cohort = PostgresMigrationSource.cohort(conn, day)
        if manifest_set_hash != expected_manifest_set_hash:
            raise RuntimeError(f"canonical_cold_recovery_reclaimed_commitment_changed: {day}")
        pages = conn.execute(text("""
            SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day
            ORDER BY page_ordinal LIMIT :limit
        """), {"day": day, "limit": object_limits.max_pages + 1}).mappings().all()
        candidate_page_count = conn.scalar(text("SELECT count(*) FROM market.cold_fact_pages WHERE staged_storage_day=:day"),
                                            {"day": day})
        if (len(pages) > object_limits.max_pages or len(pages) != candidate_page_count
                or sum(page["row_count"] for page in pages) != expected_rows):
            raise RuntimeError(f"canonical_cold_recovery_reclaimed_page_scope_changed: {day}")
        proofs = []
        for ordinal, page in enumerate(pages):
            if page["page_ordinal"] != ordinal:
                raise RuntimeError(f"canonical_cold_recovery_reclaimed_page_order_changed: {day}")
            proof = PostgresMigrationSource.source_authority(conn, page)["verification_hash"]
            if proof is None:
                raise RuntimeError(f"canonical_cold_recovery_reclaimed_receipt_missing: {page['id']}")
            proofs.append(proof)
        if _partition_manifest_set_hash(day, expected_rows, proofs) != manifest_set_hash:
            raise RuntimeError(f"canonical_cold_recovery_reclaimed_receipt_changed: {day}")

        exists = conn.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": "market." + name})
        ensure_fact_header_partition(conn, day)
        if not exists:
            # The verified day deliberately has no SQL payload partition. Only
            # this new leaf's payload guard is disabled while writing its exact
            # recovered headers. Identity, FK and series routing guards stay on.
            conn.exec_driver_sql(f'ALTER TABLE ONLY market."{name}" DISABLE TRIGGER trg_require_fact_hot_payload')
        cursor = INITIAL
        content_hash = archive_evidence_hash({"day": day.isoformat(), "manifest_set_hash": manifest_set_hash})
        for page in pages:
            manifest = _catalog_manifest(page)
            rows = _page_rows(conn, reader, manifest, day, budget=budget)
            if manifest.first_cursor <= cursor:
                raise RuntimeError(f"canonical_cold_recovery_reclaimed_page_overlap: {manifest.manifest_id}")
            if not exists:
                conn.execute(insert(header), [
                    {field: day if field == "storage_day" else row[field] for field in header.columns.keys()}
                    for row in rows
                ])
            restored = PostgresMigrationSource.rows(conn, day, cursor, through=manifest.last_cursor, limits=limits)
            _compare_existing(restored, rows, manifest=manifest,
                              cohort=cohort, after=cursor, limits=limits)
            source = _source_rows(conn, day=day, after=cursor, through=manifest.last_cursor, limit=manifest.row_count + 1)
            for actual, row in zip(source, rows, strict=True):
                if (actual["identity_id"], actual["identity_day"], actual["identity_series_id"],
                        actual["identity_key"], actual["identity_revision"]) != (
                            row["id"], day, row["series_id"], row["observation_key"], row["revision"]):
                    raise RuntimeError(f"canonical_cold_recovery_reclaimed_identity_changed: {row['id']}")
            cursor = manifest.last_cursor
            content_hash = archive_evidence_hash({"previous": content_hash, "manifest": manifest.manifest_hash})
        if _source_rows(conn, day=day, after=cursor, through=None, limit=1):
            raise RuntimeError(f"canonical_cold_recovery_reclaimed_unaccounted_headers: {day}")
        if not exists:
            conn.exec_driver_sql("SET CONSTRAINTS market.trg_require_fact_identity_header IMMEDIATE")
            conn.exec_driver_sql(f'ALTER TABLE ONLY market."{name}" ENABLE ALWAYS TRIGGER trg_require_fact_hot_payload')
            conn.exec_driver_sql("SET CONSTRAINTS market.trg_require_fact_identity_header DEFERRED")
        budget.assert_unchanged()
        check_archive()
        _assert_header_partition(conn, day)
        oid = conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": "market." + name})
        fence.assert_quiet(conn)
    receipt = {"version": VERSION, "target_database": _database_identity(conn), "storage_day": day.isoformat(),
               "manifest_set_hash": manifest_set_hash, "ordered_content_hash": content_hash,
               "row_count": expected_rows, "page_count": len(pages), "retention_sha256": retention_hash,
               "header_oid": oid}
    return receipt | {"receipt_sha256": archive_evidence_hash(receipt), "already_restored": exists}
