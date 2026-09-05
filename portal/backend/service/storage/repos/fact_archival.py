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
    MarketFactArchiveCanonicalDependencyRecord,
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
FACT_ARCHIVE_VERIFIER_VERSION = "market.canonical_archive_verification.v12"


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

    def _book_sources(self, session, rows):
        from .fact_book_admission import resolve_book_source_revisions
        return resolve_book_source_revisions(session, rows=rows, object_store=self.object_store,
            max_rows=self.max_raw_mapping_rows, max_logical_bytes=self.limits.max_logical_bytes,
            max_file_bytes=self.limits.max_file_bytes,
            check_budget=self.check_budget)

    def _source_revisions(self, session, rows):
        from .fact_derived_admission import resolve_derived_source_revisions
        derived = resolve_derived_source_revisions(session, rows=rows, object_store=self.object_store,
            max_rows=self.max_raw_mapping_rows, max_logical_bytes=self.limits.max_logical_bytes,
            max_file_bytes=self.limits.max_file_bytes, check_budget=self.check_budget)
        books = self._book_sources(session, list({row["id"]: row for row in (*rows, *derived)}.values()))
        sources = {row["id"]: row for row in (*derived, *books)}
        if len(sources) > self.max_raw_mapping_rows:
            raise RuntimeError("canonical_archive_source_dependency_budget_exceeded: reduce archive page size")
        return [sources[identity] for identity in sorted(sources)]

    def _dependencies(self, session, rows, *, bound_manifest_ids=None, source_rows=(), bound_checkpoint_ids=None):
        from .market_data import _collect_material_archive_refs
        from .fact_lineage import EXACT_RAW_FACT_TYPES, resolve_canonical_raw_archive_refs
        from .fact_book_prefix import resolve_verified_book_prefixes
        from .fact_book_admission import verify_book_metadata_and_checkpoints
        from .fact_derived_admission import DERIVED_FACT_TYPES
        from .fact_dependencies import SELF_CONTAINED_FACT_TYPES
        if not source_rows and any(row["fact_type"].startswith("market.normalized.") for row in rows):
            raise RuntimeError("canonical_archive_dependency_proof_required: normalized sources were not resolved")
        rows = list({row["id"]: row for row in (*rows, *source_rows)}.values())
        groups = defaultdict(list)
        for row in rows:
            groups[(row["series_id"], row["fact_type"])].append(row)
        references = {}
        objects = ArchiveVerificationBatch(self.object_store, limits=ArchiveVerificationLimits(
            max_objects=self.max_dependency_objects, max_bytes=self.max_dependency_bytes,
        ), check_budget=self.check_budget)
        exact_rows = [row for row in rows if row["fact_type"] in EXACT_RAW_FACT_TYPES]
        prefix_refs, root_bindings = resolve_verified_book_prefixes(
            session, rows=exact_rows, byte_verifier=objects, max_objects=self.max_dependency_objects,
            bound_manifest_ids=bound_manifest_ids, check_budget=self.check_budget,
        )
        references.update(prefix_refs)
        references.update(resolve_canonical_raw_archive_refs(
            session, rows=exact_rows, object_store=self.object_store, byte_verifier=objects,
            limits=self.raw_read_limits, max_mapping_rows=self.max_raw_mapping_rows,
            bound_manifest_ids=bound_manifest_ids,
            check_budget=self.check_budget,
            witness_manifest_ids=root_bindings,
        ))
        for identity, reference in self._flow_references(session, rows, objects, bound_manifest_ids=bound_manifest_ids).items():
            if references.setdefault(identity, reference) != reference:
                raise RuntimeError(f"canonical_flow_dependency_conflict: target_id={identity}")
        for (series_id, fact_type), group in groups.items():
            self._check_budget()
            if (fact_type in EXACT_RAW_FACT_TYPES or fact_type in DERIVED_FACT_TYPES or fact_type in SELF_CONTAINED_FACT_TYPES
                    or fact_type.startswith("market.normalized.")):
                continue
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
        checkpoints, checkpoint_sources = verify_book_metadata_and_checkpoints(session, rows=rows, object_store=self.object_store,
            byte_verifier=objects, max_objects=self.max_dependency_objects,
            max_rows=self.max_raw_mapping_rows, max_logical_bytes=self.limits.max_logical_bytes,
            max_file_bytes=self.limits.max_file_bytes,
            bound_checkpoint_ids=bound_checkpoint_ids, check_budget=self.check_budget)
        exact_ids = {row["id"] for row in exact_rows}
        additional = [row for row in checkpoint_sources if row["id"] not in exact_ids]
        if additional:
            checkpoint_refs, checkpoint_bindings = resolve_verified_book_prefixes(
                session, rows=additional, byte_verifier=objects, max_objects=self.max_dependency_objects,
                bound_manifest_ids=bound_manifest_ids, check_budget=self.check_budget)
            checkpoint_refs.update(resolve_canonical_raw_archive_refs(
                session, rows=additional, object_store=self.object_store, byte_verifier=objects,
                limits=self.raw_read_limits, max_mapping_rows=self.max_raw_mapping_rows,
                bound_manifest_ids=bound_manifest_ids, witness_manifest_ids=checkpoint_bindings,
                check_budget=self.check_budget))
            for identity, reference in sorted(checkpoint_refs.items()):
                if identity not in references:
                    result.append({"target_kind": "raw_manifest", "target_id": identity,
                        "object_key": reference["object_key"], "object_sha256": reference["object_sha256"]})
                elif references[identity] != reference:
                    raise RuntimeError(f"canonical_archive_dependency_conflict: target_id={identity}")
        result.extend(checkpoints)
        if len(result) > self.max_dependency_objects:
            raise RuntimeError("canonical_archive_dependency_object_budget_exceeded: reduce page row limit")
        objects.assert_unchanged()
        sources = {row["id"]: row for row in (*source_rows, *checkpoint_sources)}
        if len(sources) > self.max_raw_mapping_rows:
            raise RuntimeError("canonical_archive_source_dependency_budget_exceeded: reduce page row limit")
        return result, [sources[identity] for identity in sorted(sources)]

    def _prepare_book_prefix(self, session, rows, day, *, bound_manifest_ids=None):
        from .fact_book_prefix import prepare_next_book_prefix, prepare_next_trade_prefix
        from .fact_flow_admission import load_trade_flow_roots, trade_flow_prefix_requirements
        progress = prepare_next_book_prefix(
            session, rows=rows, object_store=self.object_store,
            byte_verifier=ArchiveVerificationBatch(self.object_store, limits=ArchiveVerificationLimits(
                max_objects=self.max_dependency_objects, max_bytes=self.max_dependency_bytes,
            ), check_budget=self.check_budget), limits=self.raw_read_limits,
            max_mapping_rows=self.max_raw_mapping_rows, max_objects=self.max_dependency_objects,
            check_budget=self.check_budget, bound_manifest_ids=bound_manifest_ids,
        )
        if progress is not None:
            # The executor logs completion only after this transaction commits.
            logger.info("canonical_archive_book_prefix_prepared | storage_day=%s chunk_id=%s first_ordinal=%s last_ordinal=%s required_ordinal=%s",
                day, progress["chunk_id"], progress["first_receive_ordinal"], progress["last_receive_ordinal"], progress["required_receive_ordinal"])
            return {"storage_day": day, **progress}
        roots = load_trade_flow_roots(session, rows=rows, max_rows=self.max_raw_mapping_rows,
            max_logical_bytes=self.limits.max_logical_bytes, check_budget=self.check_budget)
        prefixes, _ = trade_flow_prefix_requirements(roots)
        progress = prepare_next_trade_prefix(session, prefixes=prefixes, object_store=self.object_store,
            byte_verifier=ArchiveVerificationBatch(self.object_store, limits=ArchiveVerificationLimits(
                max_objects=self.max_dependency_objects, max_bytes=self.max_dependency_bytes,
            ), check_budget=self.check_budget), limits=self.raw_read_limits,
            max_mapping_rows=self.max_raw_mapping_rows, max_objects=self.max_dependency_objects,
            check_budget=self.check_budget, bound_manifest_ids=bound_manifest_ids)
        if progress is not None:
            logger.info("canonical_archive_trade_prefix_prepared | storage_day=%s chunk_id=%s first_ordinal=%s last_ordinal=%s required_ordinal=%s",
                day, progress["chunk_id"], progress["first_receive_ordinal"], progress["last_receive_ordinal"], progress["required_receive_ordinal"])
            return {"storage_day": day, **progress}
        return None

    def _flow_references(self, session, rows, objects, *, bound_manifest_ids=None):
        from .fact_book_prefix import resolve_verified_trade_prefixes
        from .fact_flow_admission import load_trade_flow_roots, trade_flow_prefix_requirements
        from .fact_lineage import resolve_canonical_raw_archive_refs
        roots = load_trade_flow_roots(session, rows=rows, max_rows=self.max_raw_mapping_rows,
            max_logical_bytes=self.limits.max_logical_bytes, check_budget=self.check_budget)
        prefixes, witnesses = trade_flow_prefix_requirements(roots)
        references, bindings = resolve_verified_trade_prefixes(session, prefixes=prefixes, witnesses=witnesses,
            byte_verifier=objects, max_objects=self.max_dependency_objects,
            bound_manifest_ids=bound_manifest_ids, check_budget=self.check_budget)
        endpoints = resolve_canonical_raw_archive_refs(session, rows=[], object_store=self.object_store,
            byte_verifier=objects, limits=self.raw_read_limits, max_mapping_rows=self.max_raw_mapping_rows,
            bound_manifest_ids=bound_manifest_ids, check_budget=self.check_budget,
            book_prefix_ranges=witnesses, witness_manifest_ids=bindings)
        for identity, reference in endpoints.items():
            if references.setdefault(identity, reference) != reference:
                raise RuntimeError(f"canonical_flow_dependency_conflict: target_id={identity}")
        return references

    def stage_next_page(self, day: date) -> dict:
        """Publish/read back one complete source page, then atomically acknowledge it.

        Missing book/trade prefix evidence advances one committed interval first
        and returns its prefix status; a later call publishes the same hot page.
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
            source_rows = self._source_revisions(session, rows)
            progress = self._prepare_book_prefix(session, (*rows, *source_rows), day)
            if progress is not None:
                return progress
            dependencies, source_rows = self._dependencies(session, rows, source_rows=source_rows)
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
            for source in source_rows:
                session.add(MarketFactArchiveCanonicalDependencyRecord(manifest_id=manifest.manifest_id,
                    fact_version_id=source["id"], row_hash=source["row_hash"]))
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
            ("canonical_dependencies", "fact_archive_canonical_dependencies",
             "fact_version_id, row_hash", "fact_version_id", self.max_raw_mapping_rows),
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
        Older pages can first advance one book_prefix_verified interval using
        their existing dependency bindings, without republishing their bytes.
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
            bound_ids = [item["target_id"] for item in catalogs["dependencies"] if item["target_kind"] == "raw_manifest"]
            source_rows = self._source_revisions(session, rows)
            progress = self._prepare_book_prefix(session, (*rows, *source_rows), day, bound_manifest_ids=bound_ids)
            if progress is not None:
                return progress
            bound_checkpoints = [item["target_id"] for item in catalogs["dependencies"] if item["target_kind"] == "book_checkpoint"]
            dependencies, source_rows = self._dependencies(session, rows, bound_manifest_ids=bound_ids,
                source_rows=source_rows, bound_checkpoint_ids=bound_checkpoints or None)
            expected = {
                "series": _series_catalog(manifest),
                "aliases": sorted(aliases, key=lambda item: (item["fact_version_id"], item["evidence_key"])),
                "dependencies": sorted(dependencies, key=lambda item: (item["target_kind"], item["target_id"])),
                "canonical_dependencies": [{"fact_version_id": row["id"], "row_hash": row["row_hash"]} for row in source_rows],
            }
            # Older pages can gain newly required monotone proof edges under
            # this explicit verification transaction. Never rewrite bytes,
            # existing raw bindings, an existing edge, or a prior receipt.
            additions = 0
            for name, model, identity_fields in (
                ("aliases", MarketFactArchiveMaterialAliasRecord, ("fact_version_id", "evidence_key")),
                ("canonical_dependencies", MarketFactArchiveCanonicalDependencyRecord, ("fact_version_id",)),
                ("dependencies", MarketFactArchiveDependencyRecord, ("target_kind", "target_id")),
            ):
                existing = {tuple(item[key] for key in identity_fields): item for item in catalogs[name]}
                for item in expected[name]:
                    identity = tuple(item[key] for key in identity_fields)
                    if identity not in existing and (name in {"canonical_dependencies", "aliases"} or item["target_kind"] == "book_checkpoint"):
                        session.add(model(manifest_id=manifest.manifest_id, **item))
                        additions += 1
            if additions:
                session.flush()
                catalogs = self._page_catalogs(session, manifest.manifest_id)
                logger.info("canonical_archive_proof_edges_added | manifest_id=%s verifier_version=%s edges=%s",
                            manifest.manifest_id, FACT_ARCHIVE_VERIFIER_VERSION, additions)
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
        source_placements = []
        from .fact_dependencies import verify_canonical_source_placements
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
                source_placements.append(verify_canonical_source_placements(
                    session, catalogs["canonical_dependencies"], objects=objects, check_budget=check_budget))
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
                "manifest_set_hash": _partition_manifest_set_hash(day, count, proofs),
                "source_placement_hash": archive_evidence_hash({"pages": source_placements})}

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
