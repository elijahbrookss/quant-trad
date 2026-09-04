"""Bounded, resumable publication of sealed canonical payload partitions.

This owner stages independently verified pages and their immutable lookup/hold
catalogs. It never marks a partition reclaimable or drops a table; whole-source
coverage and reclamation are separate lifecycle gates.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import date, timedelta
import hashlib
import logging
from pathlib import Path

from sqlalchemy import text

from market_data.archive import RawArchiveObjectStore
from market_data.canonical_storage import legacy_material_alias, record_from_storage_row
from market_data.fact_archive import (
    FactArchiveLimits, publish_canonical_fact_archive, read_canonical_fact_archive,
    verify_canonical_fact_archive_rows,
)
from portal.backend.db import (
    MarketFactArchiveManifestRecord, MarketFactArchiveSeriesRecord,
    MarketFactArchiveDependencyRecord, MarketFactArchiveMaterialAliasRecord,
)
from portal.backend.db.fact_storage_schema import fact_partition_name

from .fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM, _catalog_manifest, ensure_payload_contracts
from .market_lifecycle import market_storage_lifecycle_repository

logger = logging.getLogger(__name__)


class PostgresCanonicalFactArchiveRepository:
    def __init__(self, *, database, object_store: RawArchiveObjectStore,
                 temporary_directory: Path, limits: FactArchiveLimits = FactArchiveLimits(),
                 max_dependency_bytes: int = 1024**3, max_dependency_objects: int = 5000):
        if (type(max_dependency_bytes) is not int or max_dependency_bytes <= 0
                or type(max_dependency_objects) is not int or max_dependency_objects <= 0):
            raise ValueError("canonical_archive_dependency_budget_invalid")
        self.database = database
        self.object_store = object_store
        self.temporary_directory = temporary_directory
        self.limits = limits
        self.max_dependency_bytes = max_dependency_bytes
        self.max_dependency_objects = max_dependency_objects

    @staticmethod
    def _day(day):
        if type(day) is not date:
            raise ValueError("canonical_archive_storage_day_invalid")
        return day

    def _lock(self, session, day):
        self._day(day)
        market_storage_lifecycle_repository.acquire_dataset_pin_lock(session)
        acquired = session.execute(text(
            "SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"
        ), {"name": f"quant-trad:canonical-archive:{day.isoformat()}"}).scalar_one()
        if not acquired:
            raise RuntimeError(f"canonical_archive_partition_busy: storage_day={day}")
        row = session.execute(text(
            "SELECT * FROM market.fact_retention_partitions WHERE storage_day=:day FOR UPDATE"
        ), {"day": day}).mappings().one_or_none()
        if row is None:
            raise RuntimeError(f"canonical_archive_partition_unknown: storage_day={day}")
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

    def _dependencies(self, session, rows):
        from .market_data import _collect_material_archive_refs, _resolve_canonical_book_archive_positions
        groups = defaultdict(list)
        for row in rows:
            groups[(row["series_id"], row["fact_type"])].append(row)
        references = {}
        for (series_id, fact_type), group in groups.items():
            if fact_type.startswith("market.normalized."):
                # A normalized row's window witnesses need a separate recursive
                # admission proof; do not pretend its source dependencies are empty.
                raise RuntimeError(f"canonical_archive_dependency_proof_required: series_id={series_id} fact_type={fact_type}")
            if fact_type == "market.l2_book":
                positions = [row["provenance"].get("_qt_l2_evidence") for row in group]
                found = _resolve_canonical_book_archive_positions(
                    session, positions=positions, series_id=series_id, fact_type=fact_type,
                    start=min(row["observation_time"] for row in group),
                    end=max(row["observation_time"] for row in group) + timedelta(microseconds=1),
                    as_of_commit_seq=max(row["market_commit_seq"] for row in group),
                )
            else:
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
        dependency_bytes = 0
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
            path = self.object_store.local_path(reference["object_key"])
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    dependency_bytes += len(block)
                    if dependency_bytes > self.max_dependency_bytes:
                        raise RuntimeError("canonical_archive_dependency_byte_budget_exceeded: reduce page row limit")
                    digest.update(block)
            if digest.hexdigest() != reference["object_sha256"]:
                raise RuntimeError(f"canonical_archive_dependency_corrupt: target_id={identity}")
            result.append({"target_kind": "raw_manifest", "target_id": identity,
                           "object_key": reference["object_key"], "object_sha256": reference["object_sha256"]})
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
                record_from_storage_row(row)
            dependencies = self._dependencies(session, rows)
            manifest = publish_canonical_fact_archive(
                rows, object_store=self.object_store, temporary_directory=self.temporary_directory, limits=self.limits,
            )
            read_canonical_fact_archive(self.object_store.local_path(manifest.object_key), expected=manifest, limits=self.limits)
            verify_canonical_fact_archive_rows(rows, expected=manifest, limits=self.limits)
            session.add(MarketFactArchiveManifestRecord(
                id=manifest.manifest_id, storage_day=day, page_ordinal=ordinal,
                object_key=manifest.object_key, object_sha256=manifest.object_sha256,
                manifest_hash=manifest.manifest_hash, row_count=manifest.row_count, byte_count=manifest.byte_count,
                first_commit_seq=manifest.first_cursor[0], first_id=manifest.first_cursor[1],
                last_commit_seq=manifest.last_cursor[0], last_id=manifest.last_cursor[1], descriptor=manifest.to_dict(),
            ))
            session.flush()
            for bounds in manifest.series:
                values = {key: value for key, value in asdict(bounds).items() if key not in {"source_ids", "payload_contracts"}}
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
