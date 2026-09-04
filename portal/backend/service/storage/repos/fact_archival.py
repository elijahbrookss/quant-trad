"""Bounded, resumable publication of sealed canonical payload partitions.

This owner stages pages, persists resumable full-page verification receipts,
and checks whole-source coverage plus current bytes. It never drops a table;
reclamation is a separate lifecycle gate.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import date
import logging
from pathlib import Path

from sqlalchemy import text

from market_data.archive import RawArchiveObjectStore, RawArchiveReadLimits
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.canonical_storage import legacy_material_alias, record_from_storage_row, verify_archived_envelope
from market_data.fact_archive import (
    FactArchiveLimits, publish_canonical_fact_archive, read_canonical_fact_archive,
    verify_canonical_fact_archive_rows, archive_evidence_hash,
)
from portal.backend.db import (
    MarketFactArchiveManifestRecord, MarketFactArchiveSeriesRecord,
    MarketFactArchiveDependencyRecord, MarketFactArchiveMaterialAliasRecord,
    MarketFactArchiveVerificationRecord,
)
from portal.backend.db.fact_storage_schema import fact_partition_name

from .fact_storage import (
    CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM, CANONICAL_ENVELOPE_COLUMNS, CANONICAL_ENVELOPE_FROM,
    _catalog_manifest, ensure_payload_contracts,
)
from .market_lifecycle import MarketStorageLifecycleBusyError, market_storage_lifecycle_repository

logger = logging.getLogger(__name__)
# Increase when deep admission rules change: old receipts must not bypass new
# dependency/lineage requirements during the metadata-only final coverage pass.
FACT_ARCHIVE_VERIFIER_VERSION = "market.canonical_archive_verification.v3"


def _series_catalog(manifest):
    return [{key: value for key, value in asdict(bounds).items()
             if key not in {"source_ids", "payload_contracts"}} for bounds in manifest.series]


def _receipt(page, catalog_hash):
    evidence = {
        "manifest_id": page["id"], "manifest_hash": page["manifest_hash"],
        "storage_day": page["storage_day"].isoformat(), "page_ordinal": page["page_ordinal"],
        "verifier_version": FACT_ARCHIVE_VERIFIER_VERSION, "catalog_hash": catalog_hash,
    }
    return {key: value for key, value in evidence.items() if key not in {"storage_day", "page_ordinal"}} | {
        "verification_hash": archive_evidence_hash(evidence),
    }


def _partition_manifest_set_hash(day, expected_rows, page_verifications):
    return archive_evidence_hash({
        "verifier_version": FACT_ARCHIVE_VERIFIER_VERSION, "storage_day": day.isoformat(),
        "expected_rows": expected_rows, "page_verifications": page_verifications,
    })


class PostgresCanonicalFactArchiveRepository:
    def __init__(self, *, database, object_store: RawArchiveObjectStore,
                 temporary_directory: Path, limits: FactArchiveLimits = FactArchiveLimits(),
                 max_dependency_bytes: int = 1024**3, max_dependency_objects: int = 5000,
                 raw_read_limits: RawArchiveReadLimits = RawArchiveReadLimits(), max_raw_mapping_rows: int = 50_000,
                 statement_timeout_ms: int | None = None, partition_guard=None, check_budget=None):
        if (type(max_dependency_bytes) is not int or max_dependency_bytes <= 0
                or type(max_dependency_objects) is not int or max_dependency_objects <= 0
                or type(max_raw_mapping_rows) is not int or max_raw_mapping_rows <= 0):
            raise ValueError("canonical_archive_dependency_budget_invalid")
        self.database = database
        self.object_store = object_store
        self.temporary_directory = temporary_directory
        self.limits = limits
        self.max_dependency_bytes = max_dependency_bytes
        self.max_dependency_objects = max_dependency_objects
        self.raw_read_limits = raw_read_limits
        self.max_raw_mapping_rows = max_raw_mapping_rows
        if statement_timeout_ms is not None and (type(statement_timeout_ms) is not int or statement_timeout_ms <= 0):
            raise ValueError("canonical_archive_statement_timeout_invalid")
        self.statement_timeout_ms = statement_timeout_ms
        self.partition_guard = partition_guard
        self.check_budget = check_budget

    def _check_budget(self):
        if self.check_budget is not None:
            self.check_budget()

    @staticmethod
    def _day(day):
        if type(day) is not date:
            raise ValueError("canonical_archive_storage_day_invalid")
        return day

    def _lock(self, session, day, *, statement_timeout_ms=None):
        self._day(day)
        self._check_budget()
        timeout = statement_timeout_ms if statement_timeout_ms is not None else self.statement_timeout_ms
        if timeout is not None:
            session.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                            {"timeout": str(timeout)})
        market_storage_lifecycle_repository.acquire_dataset_pin_lock(session)
        acquired = session.execute(text(
            "SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"
        ), {"name": f"quant-trad:canonical-archive:{day.isoformat()}"}).scalar_one()
        if not acquired:
            raise MarketStorageLifecycleBusyError(f"canonical_archive_partition_busy: storage_day={day}")
        row = session.execute(text(
            "SELECT * FROM market.fact_retention_partitions WHERE storage_day=:day FOR UPDATE"
        ), {"day": day}).mappings().one_or_none()
        if row is None:
            raise RuntimeError(f"canonical_archive_partition_unknown: storage_day={day}")
        self._check_budget()
        if self.partition_guard is not None:
            self.partition_guard(session, row)
        return row

    def inspect_partition(self, day: date) -> dict:
        """Read-only progress; does not open/create files or seal the partition."""
        self._day(day)
        with self.database.session() as session:
            row = session.execute(text("""
                SELECT partition.*,
                       (SELECT count(*) FROM market.fact_archive_manifests AS pages
                        WHERE pages.storage_day=partition.storage_day) AS page_count,
                       (SELECT coalesce(sum(row_count),0) FROM market.fact_archive_manifests AS pages
                        WHERE pages.storage_day=partition.storage_day) AS archived_rows
                FROM market.fact_retention_partitions AS partition WHERE storage_day=:day
            """), {"day": day}).mappings().one_or_none()
            if row is None:
                raise RuntimeError(f"canonical_archive_partition_unknown: storage_day={day}")
            return dict(row)

    def seal_partition(self, day: date) -> dict:
        with self.database.session() as session:
            partition = self._lock(session, day)
            if partition["state"] != "open":
                return dict(partition)
            today = session.execute(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date")).scalar_one()
            if day >= today:
                raise RuntimeError(f"canonical_archive_active_day: storage_day={day} database_day={today}")
            relation = "market." + fact_partition_name(day)
            attached = session.execute(text("""
                SELECT EXISTS (SELECT 1 FROM pg_inherits
                    WHERE inhparent='market.fact_hot_payloads'::regclass AND inhrelid=to_regclass(:relation))
            """), {"relation": relation}).scalar_one()
            if not attached:
                raise RuntimeError(f"canonical_archive_partition_not_attached: storage_day={day}")
            count = session.execute(text(
                "SELECT count(*) FROM market.fact_versions WHERE storage_day=:day"
            ), {"day": day}).scalar_one()
            hot_count = session.execute(text(
                "SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"
            ), {"day": day}).scalar_one()
            if count != hot_count:
                raise RuntimeError(f"canonical_archive_source_incomplete: storage_day={day} expected={count} hot={hot_count}")
            source_bytes = session.execute(text("SELECT pg_total_relation_size(to_regclass(:relation))"),
                                           {"relation": relation}).scalar_one()
            result = session.execute(text("""
                UPDATE market.fact_retention_partitions
                SET state='sealed', sealed_at=clock_timestamp(), expected_rows=:count, source_bytes=:bytes
                WHERE storage_day=:day RETURNING *
            """), {"day": day, "count": count, "bytes": source_bytes}).mappings().one()
        logger.info("canonical_archive_partition_sealed | storage_day=%s rows=%s source_bytes=%s", day, count, source_bytes)
        return dict(result)

    def _dependencies(self, session, rows, *, bound_manifest_ids=None):
        from .market_data import _collect_material_archive_refs
        from .fact_lineage import EXACT_RAW_FACT_TYPES, resolve_canonical_raw_archive_refs
        groups = defaultdict(list)
        for row in rows:
            groups[(row["series_id"], row["fact_type"])].append(row)
        references = {}
        objects = ArchiveVerificationBatch(self.object_store, limits=ArchiveVerificationLimits(
            max_objects=self.max_dependency_objects, max_bytes=self.max_dependency_bytes,
        ), check_budget=self.check_budget)
        exact_rows = [row for row in rows if row["fact_type"] in EXACT_RAW_FACT_TYPES]
        references.update(resolve_canonical_raw_archive_refs(
            session, rows=exact_rows, object_store=self.object_store, byte_verifier=objects,
            limits=self.raw_read_limits, max_mapping_rows=self.max_raw_mapping_rows,
            bound_manifest_ids=bound_manifest_ids,
            check_budget=self.check_budget,
        ))
        for (series_id, fact_type), group in groups.items():
            self._check_budget()
            if fact_type in EXACT_RAW_FACT_TYPES:
                continue
            if fact_type.startswith("market.normalized."):
                # A normalized row's window witnesses need a separate recursive
                # admission proof; do not pretend its source dependencies are empty.
                raise RuntimeError(f"canonical_archive_dependency_proof_required: series_id={series_id} fact_type={fact_type}")
            roots = []
            for row in group:
                alias = legacy_material_alias(row)
                roots.append((series_id, alias["material_hash"] if alias else row["material_hash"]))
            found = _collect_material_archive_refs(session, roots=roots)
            for identity, reference in found.items():
                if references.setdefault(identity, reference) != reference:
                    raise RuntimeError(f"canonical_archive_dependency_conflict: target_id={identity}")
        result = []
        if len(references) > self.max_dependency_objects:
            raise RuntimeError("canonical_archive_dependency_object_budget_exceeded: reduce page row limit")
        for identity, reference in sorted(references.items()):
            expired = session.execute(text("""
                SELECT EXISTS (SELECT 1 FROM market.storage_lifecycle_events
                    WHERE action='archive_expire' AND target_kind='raw_manifest'
                      AND target_id=:id AND event_type='completed')
            """), {"id": identity}).scalar_one()
            if expired:
                raise RuntimeError(f"canonical_archive_dependency_expired: target_id={identity}")
            # The lifecycle shared fence remains held while bytes are read and
            # permanent hold edges commit. Expiry cannot race this admission.
            objects.verify(reference["object_key"], reference["object_sha256"])
            result.append({"target_kind": "raw_manifest", "target_id": identity,
                           "object_key": reference["object_key"], "object_sha256": reference["object_sha256"]})
        objects.assert_unchanged()
        return result

    def stage_next_page(self, day: date) -> dict:
        """Publish/read back one complete source page, then atomically acknowledge it.

        An interrupted publication can leave an unreferenced immutable object.
        Retrying reads the same source cursor and safely reuses identical bytes;
        progress advances only in the catalog transaction.
        """
        with self.database.session() as session:
            partition = self._lock(session, day)
            if partition["state"] != "sealed":
                raise RuntimeError(f"canonical_archive_partition_not_sealed: storage_day={day} state={partition['state']}")
            previous = session.execute(text("""
                SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day
                ORDER BY page_ordinal DESC LIMIT 1
            """), {"day": day}).mappings().one_or_none()
            cursor = _catalog_manifest(previous).last_cursor if previous else (0, "")
            ordinal = previous["page_ordinal"] + 1 if previous else 0
            params = {"day": day, "seq": cursor[0], "id": cursor[1], "limit": self.limits.max_rows,
                      "max_bytes": self.limits.max_logical_bytes}
            source = session.execute(text(f"""
                WITH candidates AS MATERIALIZED (
                    SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM}
                    WHERE versions.storage_day=:day AND (versions.market_commit_seq,versions.id) > (:seq,:id)
                    ORDER BY versions.market_commit_seq,versions.id LIMIT :limit
                ), budget AS (
                    SELECT id, sum(6::bigint * octet_length(to_jsonb(candidates)::text))
                        OVER (ORDER BY market_commit_seq,id) AS cumulative_bytes FROM candidates
                )
                SELECT candidates.* FROM candidates JOIN budget USING(id)
                WHERE budget.cumulative_bytes <= :max_bytes
                ORDER BY candidates.market_commit_seq,candidates.id
            """), params).mappings().all()
            if not source:
                # PostgreSQL sizes a conservative ASCII-escape allowance before
                # transferring JSON to Python. Oversized first rows cannot be
                # mistaken for an exhausted source or skipped behind a cursor.
                remaining = session.execute(text(
                    "SELECT EXISTS (SELECT 1 FROM market.fact_versions WHERE storage_day=:day "
                    "AND (market_commit_seq,id) > (:seq,:id))"
                ), params).scalar_one()
                if remaining:
                    raise RuntimeError(f"canonical_archive_source_row_budget_exceeded: storage_day={day}; raise the explicit logical-byte budget")
                count = session.execute(text(
                    "SELECT coalesce(sum(row_count),0) FROM market.fact_archive_manifests WHERE storage_day=:day"
                ), {"day": day}).scalar_one()
                if count != partition["expected_rows"]:
                    raise RuntimeError(f"canonical_archive_source_coverage_mismatch: storage_day={day}")
                return {"storage_day": day, "status": "source_exhausted", "archived_rows": count}
            rows = [{key: value for key, value in row.items() if key != "storage_day"} for row in source]
            ensure_payload_contracts(session, [(row["payload_schema_id"], row["payload_contract_hash"]) for row in rows])
            # Validate canonical payload/provenance hashes before following any
            # claimed source reference or publishing bytes.
            for row in rows:
                self._check_budget()
                record_from_storage_row(row)
            dependencies = self._dependencies(session, rows)
            manifest = publish_canonical_fact_archive(
                rows, object_store=self.object_store, temporary_directory=self.temporary_directory, limits=self.limits,
            )
            read_canonical_fact_archive(self.object_store.local_path(manifest.object_key), expected=manifest, limits=self.limits)
            verify_canonical_fact_archive_rows(rows, expected=manifest, limits=self.limits)
            self._check_budget()
            session.add(MarketFactArchiveManifestRecord(
                id=manifest.manifest_id, storage_day=day, page_ordinal=ordinal,
                object_key=manifest.object_key, object_sha256=manifest.object_sha256,
                manifest_hash=manifest.manifest_hash, row_count=manifest.row_count, byte_count=manifest.byte_count,
                first_commit_seq=manifest.first_cursor[0], first_id=manifest.first_cursor[1],
                last_commit_seq=manifest.last_cursor[0], last_id=manifest.last_cursor[1], descriptor=manifest.to_dict(),
            ))
            session.flush()
            for values in _series_catalog(manifest):
                session.add(MarketFactArchiveSeriesRecord(manifest_id=manifest.manifest_id, **values))
            for row in rows:
                alias = legacy_material_alias(row)
                if alias is not None:
                    session.add(MarketFactArchiveMaterialAliasRecord(manifest_id=manifest.manifest_id, **alias))
            for dependency in dependencies:
                session.add(MarketFactArchiveDependencyRecord(manifest_id=manifest.manifest_id, **dependency))
        logger.info("canonical_archive_page_acknowledged | storage_day=%s page_ordinal=%s manifest_id=%s rows=%s bytes=%s dependencies=%s",
                    day, ordinal, manifest.manifest_id, manifest.row_count, manifest.byte_count, len(dependencies))
        return {"storage_day": day, "status": "page_acknowledged", "page_ordinal": ordinal,
                "manifest_id": manifest.manifest_id, "row_count": manifest.row_count}

    def _page_catalogs(self, session, manifest_id):
        catalogs = {}
        # Explicit projections define the versioned proof. Fetch one extra row
        # to reject excess metadata without transferring an unbounded catalog.
        for name, table, columns, order, limit in (
            ("series", "fact_archive_series",
             "series_id, row_count, first_observation_at, last_observation_at, first_known_at, "
             "last_known_at, first_accepted_at, last_accepted_at", "series_id", self.limits.max_rows),
            ("aliases", "fact_archive_material_aliases",
             "fact_version_id, series_id, evidence_key, material_hash", "fact_version_id, evidence_key", self.limits.max_rows),
            ("dependencies", "fact_archive_dependencies",
             "target_kind, target_id, object_key, object_sha256", "target_kind, target_id", self.max_dependency_objects),
        ):
            rows = session.execute(text(
                f"SELECT {columns} FROM market.{table} WHERE manifest_id=:id ORDER BY {order} LIMIT :limit"
            ), {"id": manifest_id, "limit": limit + 1}).mappings().all()
            if len(rows) > limit:
                raise RuntimeError(f"canonical_archive_catalog_budget_exceeded: manifest_id={manifest_id} catalog={name}")
            catalogs[name] = [dict(row) for row in rows]
        return catalogs

    def restart_partition_verification(self, day: date) -> dict:
        """Withdraw an older verifier's admission, without deleting any evidence.

        Page receipts stay immutable. The sealed partition resumes current-version
        page verification and whole-partition admission before reclamation. This
        is not a repair path for a changed proof from the current verifier.
        """
        with self.database.session() as session:
            partition = self._lock(session, day)
            if partition["state"] == "sealed":
                return {"storage_day": day, "status": "verification_already_restarted"}
            if partition["state"] != "verified":
                raise RuntimeError(f"canonical_archive_partition_not_verified: storage_day={day} state={partition['state']}")
            counts = session.execute(text("""
                SELECT count(*) AS page_count, count(receipts.manifest_id) AS verified_page_count
                FROM market.fact_archive_manifests AS pages
                LEFT JOIN market.fact_archive_verifications AS receipts
                  ON receipts.manifest_id=pages.id AND receipts.verifier_version=:version
                WHERE pages.storage_day=:day
            """), {"day": day, "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one()
            if (counts["page_count"] == counts["verified_page_count"]
                    and (counts["page_count"] or partition["manifest_set_hash"] == _partition_manifest_set_hash(day, 0, []))):
                raise RuntimeError(f"canonical_archive_verification_not_stale: storage_day={day}")
            self._check_budget()
            session.execute(text("""
                UPDATE market.fact_retention_partitions
                SET state='sealed', verified_at=NULL, manifest_set_hash=NULL WHERE storage_day=:day
            """), {"day": day})
        logger.warning("canonical_archive_verification_restarted | storage_day=%s prior_manifest_set_hash=%s verifier_version=%s",
                       day, partition["manifest_set_hash"], FACT_ARCHIVE_VERIFIER_VERSION)
        return {"storage_day": day, "status": "partition_verification_restarted",
                "prior_manifest_set_hash": partition["manifest_set_hash"], "verifier_version": FACT_ARCHIVE_VERIFIER_VERSION}

    def verify_next_page(self, day: date) -> dict:
        """Deep-check one unverified page; commit its receipt only after all checks.

        The codec validates every full-row hash. Comparing every permanent
        envelope then proves document equality without rereading hot JSON.
        This progress survives process restarts; it cannot authorize a DROP.
        """
        with self.database.session() as session:
            partition = self._lock(session, day)
            if partition["state"] != "sealed":
                raise RuntimeError(f"canonical_archive_partition_not_sealed: storage_day={day} state={partition['state']}")
            page = session.execute(text("""
                SELECT pages.* FROM market.fact_archive_manifests AS pages
                LEFT JOIN market.fact_archive_verifications AS proof
                  ON proof.manifest_id=pages.id AND proof.verifier_version=:version
                WHERE pages.storage_day=:day AND proof.manifest_id IS NULL
                ORDER BY pages.page_ordinal LIMIT 1
            """), {"day": day, "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one_or_none()
            if page is None:
                return {"storage_day": day, "status": "no_unverified_pages"}
            manifest = _catalog_manifest(page)
            if manifest.row_count > self.limits.max_rows:
                raise RuntimeError(f"canonical_archive_page_row_budget_exceeded: manifest_id={manifest.manifest_id}")
            ensure_payload_contracts(session, [contract for bounds in manifest.series for contract in bounds.payload_contracts])
            rows = read_canonical_fact_archive(
                self.object_store.local_path(manifest.object_key), expected=manifest, limits=self.limits,
            )
            envelopes = session.execute(text(f"""
                SELECT {CANONICAL_ENVELOPE_COLUMNS} {CANONICAL_ENVELOPE_FROM}
                WHERE versions.storage_day=:day
                  AND (versions.market_commit_seq, versions.id) >= (:first_seq, :first_id)
                  AND (versions.market_commit_seq, versions.id) <= (:last_seq, :last_id)
                ORDER BY versions.market_commit_seq, versions.id LIMIT :limit
            """), {"day": day, "first_seq": manifest.first_cursor[0], "first_id": manifest.first_cursor[1],
                   "last_seq": manifest.last_cursor[0], "last_id": manifest.last_cursor[1],
                   "limit": self.limits.max_rows + 1}).mappings().all()
            if len(envelopes) != len(rows):
                raise RuntimeError(f"canonical_archive_page_source_coverage_mismatch: manifest_id={manifest.manifest_id}")
            for envelope, archived in zip(envelopes, rows):
                self._check_budget()
                verify_archived_envelope(envelope, archived)
            aliases = [alias for row in rows if (alias := legacy_material_alias(row)) is not None]
            catalogs = self._page_catalogs(session, manifest.manifest_id)
            expected = {
                "series": _series_catalog(manifest),
                "aliases": sorted(aliases, key=lambda item: (item["fact_version_id"], item["evidence_key"])),
                "dependencies": sorted(self._dependencies(session, rows, bound_manifest_ids=[
                    item["target_id"] for item in catalogs["dependencies"] if item["target_kind"] == "raw_manifest"
                ]), key=lambda item: (item["target_kind"], item["target_id"])),
            }
            for name, items in expected.items():
                if catalogs[name] != items:
                    raise RuntimeError(f"canonical_archive_catalog_incomplete: manifest_id={manifest.manifest_id} catalog={name}")
            receipt = _receipt(page, archive_evidence_hash(catalogs))
            self._check_budget()
            session.add(MarketFactArchiveVerificationRecord(**receipt))
        logger.info("canonical_archive_page_verified | storage_day=%s page_ordinal=%s manifest_id=%s rows=%s verification_hash=%s",
                    day, page["page_ordinal"], manifest.manifest_id, manifest.row_count, receipt["verification_hash"])
        return {"storage_day": day, "status": "page_verified", "page_ordinal": page["page_ordinal"],
                "manifest_id": manifest.manifest_id, "verification_hash": receipt["verification_hash"]}

    def _partition_evidence(self, session, partition, *, limits, objects=None, check_budget=None):
        """Check bounded catalogs/receipts and exact sealed-source coverage.

        Supplying a byte verifier additionally checks every current cold object.
        With no verifier this returns metadata evidence only, never permission
        to delete. The caller must own the partition and lifecycle fences.
        """
        day = partition["storage_day"]
        check_budget = check_budget or self.check_budget
        last_cursor = (0, "")
        ordinal = 0
        total_rows = 0
        proofs = []
        # Fetch bounded batches of metadata, not all rows or all manifests at
        # once. Each page's aliases/dependencies have their own explicit bounds.
        while True:
            if check_budget is not None:
                check_budget()
            pages = session.execute(text("""
                SELECT * FROM market.fact_archive_manifests
                WHERE storage_day=:day AND page_ordinal>=:ordinal ORDER BY page_ordinal LIMIT :limit
            """), {"day": day, "ordinal": ordinal, "limit": min(100, limits.max_pages - ordinal + 1)}).mappings().all()
            if not pages:
                break
            for page in pages:
                if check_budget is not None:
                    check_budget()
                if ordinal >= limits.max_pages:
                    raise RuntimeError(f"canonical_archive_partition_page_budget_exceeded: storage_day={day}")
                manifest = _catalog_manifest(page)
                if page["page_ordinal"] != ordinal or manifest.first_cursor <= last_cursor:
                    raise RuntimeError(f"canonical_archive_partition_page_order_invalid: storage_day={day} page_ordinal={page['page_ordinal']}")
                catalogs = self._page_catalogs(session, manifest.manifest_id)
                receipt = session.execute(text("""
                    SELECT manifest_id, verifier_version, manifest_hash, catalog_hash, verification_hash
                    FROM market.fact_archive_verifications WHERE manifest_id=:id AND verifier_version=:version
                """), {"id": manifest.manifest_id, "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one_or_none()
                if receipt is None or dict(receipt) != _receipt(page, archive_evidence_hash(catalogs)):
                    raise RuntimeError(f"canonical_archive_verification_missing_or_stale: manifest_id={manifest.manifest_id}")
                if objects is not None:
                    objects.verify(manifest.object_key, manifest.object_sha256, expected_bytes=manifest.byte_count)
                for dependency in catalogs["dependencies"]:
                    if check_budget is not None:
                        check_budget()
                    expired = session.execute(text("""
                        SELECT EXISTS (SELECT 1 FROM market.storage_lifecycle_events
                            WHERE action='archive_expire' AND event_type='completed'
                              AND target_kind=:target_kind AND target_id=:target_id)
                    """), dependency).scalar_one()
                    if expired:
                        raise RuntimeError(f"canonical_archive_dependency_expired: target_id={dependency['target_id']}")
                    if objects is not None:
                        objects.verify(dependency["object_key"], dependency["object_sha256"])
                proofs.append(receipt["verification_hash"])
                total_rows += manifest.row_count
                last_cursor = manifest.last_cursor
                ordinal += 1
        if check_budget is not None:
            check_budget()
        count = session.execute(text(
            "SELECT count(*) FROM market.fact_versions WHERE storage_day=:day"
        ), {"day": day}).scalar_one()
        # Each verified interval equals its complete header range. Nonoverlap
        # plus equal total cardinality proves no headers were skipped between
        # pages, before the first, or after the last page.
        if count != partition["expected_rows"] or total_rows != count:
            raise RuntimeError(f"canonical_archive_source_coverage_mismatch: storage_day={day} source_rows={count} archived_rows={total_rows}")
        return {"storage_day": day, "page_count": ordinal, "row_count": total_rows,
                "manifest_set_hash": _partition_manifest_set_hash(day, count, proofs)}

    def verify_partition(self, day: date, *, limits: ArchiveVerificationLimits = ArchiveVerificationLimits()) -> dict:
        """Admit complete, currently readable cold coverage; retain all hot data.

        Expensive per-page decoding resumes via immutable receipts. This final
        bounded pass still rehashes current files; stale receipts cannot hide
        missing/corrupted bytes. Reclamation must repeat this fresh-byte gate.
        """
        with self.database.session() as session:
            partition = self._lock(session, day)
            if partition["state"] not in {"sealed", "verified"}:
                raise RuntimeError(f"canonical_archive_partition_not_sealed: storage_day={day} state={partition['state']}")
            objects = ArchiveVerificationBatch(self.object_store, limits=limits, check_budget=self.check_budget)
            evidence = self._partition_evidence(session, partition, limits=limits, objects=objects)
            objects.assert_unchanged()
            if partition["state"] == "verified":
                if partition["manifest_set_hash"] != evidence["manifest_set_hash"]:
                    raise RuntimeError(f"canonical_archive_partition_verification_changed: storage_day={day}")
            else:
                session.execute(text("""
                    UPDATE market.fact_retention_partitions
                    SET state='verified', verified_at=clock_timestamp(), manifest_set_hash=:manifest_set_hash
                    WHERE storage_day=:storage_day
                """), evidence)
        logger.info("canonical_archive_partition_verified | storage_day=%s pages=%s rows=%s objects=%s bytes=%s manifest_set_hash=%s",
                    day, evidence["page_count"], evidence["row_count"], len(objects.objects), objects.byte_count,
                    evidence["manifest_set_hash"])
        return {**evidence, "status": "partition_verified", "verified_objects": len(objects.objects),
                "verified_bytes": objects.byte_count}
