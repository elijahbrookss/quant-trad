"""Bounded, resumable publication of sealed canonical payload partitions.

This owner stages pages, persists resumable full-page verification receipts,
and checks whole-source coverage plus current bytes. It never drops a table;
reclamation is a separate lifecycle gate.
"""
from __future__ import annotations

from collections import defaultdict
import hashlib
import uuid
from dataclasses import asdict
from datetime import date
import logging
from pathlib import Path
from time import perf_counter

from sqlalchemy import event, text

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
    MarketFactArchiveVerificationRecord, MarketFactArchiveVerificationGenerationRecord,
    MarketFactArchiveCanonicalDependencyRecord,
)
from portal.backend.db.fact_storage_schema import fact_partition_name

from .fact_storage import (
    CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM, CANONICAL_ENVELOPE_COLUMNS, CANONICAL_ENVELOPE_FROM,
    _catalog_manifest, ensure_payload_contracts,
)
from .fact_dependencies import CanonicalSourceByteBudgetExceeded
from .fact_archive_generation import (
    ArchiveGenerationCoordinator, LEGACY_CATALOG_TABLES, PREPARED_CATALOG_TABLES,
)
from .market_lifecycle import MarketStorageLifecycleBusyError, market_storage_lifecycle_repository

logger = logging.getLogger(__name__)
# Increase when deep admission rules change: old receipts must not bypass new
# dependency/lineage requirements during the metadata-only final coverage pass.
FACT_ARCHIVE_VERIFIER_VERSION = "market.canonical_archive_verification.v12"
# Keep cold catalog/FK work below the per-statement ceiling without shrinking
# immutable archive pages or committing partial page metadata.
_CATALOG_WRITE_BATCH_ROWS = 128
# Alias rows maintain more identities than the fixed-width dependency catalog.
# Keep their cold-storage statements below the shared statement timeout while
# preserving the all-or-nothing page transaction.
_ALIAS_CATALOG_WRITE_BATCH_ROWS = 64
# Bound expiration-key SQL independently of the number of archive dependencies.
_EXPIRATION_QUERY_BATCH_ROWS = 128
_ALIAS_CATALOG_INSERT = text("""INSERT INTO market.fact_archive_material_aliases
    (manifest_id, fact_version_id, evidence_key, series_id, material_hash)
SELECT :manifest_id, input.fact_version_id, input.evidence_key,
       input.series_id, input.material_hash
FROM unnest(
    CAST(:fact_version_ids AS text[]), CAST(:evidence_keys AS text[]),
    CAST(:series_ids AS bigint[]), CAST(:material_hashes AS text[])
) WITH ORDINALITY AS input(
    fact_version_id, evidence_key, series_id, material_hash, position
)
ORDER BY input.position""")
_CANONICAL_DEPENDENCY_CATALOG_INSERT = text("""INSERT INTO market.fact_archive_canonical_dependencies
    (manifest_id, fact_version_id, row_hash)
SELECT :manifest_id, input.fact_version_id, input.row_hash
FROM unnest(
    CAST(:fact_version_ids AS text[]), CAST(:row_hashes AS text[])
) WITH ORDINALITY AS input(fact_version_id, row_hash, position)
ORDER BY input.position""")

# PostgreSQL 15 exposes these counters only at cluster scope. The paired
# snapshots help correlate a page with overlapping WAL/checkpoint pressure; they
# are never presented as page-exclusive bytes or device attribution.
_ARCHIVE_STAGE_DATABASE_SNAPSHOT = text("""
    SELECT pg_backend_pid() AS backend_pid,
           current_setting('server_version_num')::integer AS server_version_num,
           current_setting('track_io_timing')::boolean AS track_io_timing,
           wal.stats_reset AS wal_stats_reset,
           wal.wal_records::bigint AS wal_records,
           wal.wal_fpi::bigint AS wal_fpi,
           wal.wal_bytes::bigint AS wal_bytes,
           wal.wal_buffers_full::bigint AS wal_buffers_full,
           writer.stats_reset AS writer_stats_reset,
           writer.checkpoints_timed::bigint AS checkpoints_timed,
           writer.checkpoints_req::bigint AS checkpoints_req,
           writer.checkpoint_write_time::double precision AS checkpoint_write_time_ms,
           writer.checkpoint_sync_time::double precision AS checkpoint_sync_time_ms,
           writer.buffers_checkpoint::bigint AS buffers_checkpoint,
           writer.buffers_clean::bigint AS buffers_clean,
           writer.buffers_backend::bigint AS buffers_backend,
           writer.buffers_backend_fsync::bigint AS buffers_backend_fsync
    FROM pg_stat_wal AS wal CROSS JOIN pg_stat_bgwriter AS writer
""")
_ARCHIVE_STAGE_DATABASE_COUNTERS = (
    "wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
    "checkpoints_timed", "checkpoints_req", "checkpoint_write_time_ms",
    "checkpoint_sync_time_ms", "buffers_checkpoint", "buffers_clean",
    "buffers_backend", "buffers_backend_fsync",
)

_ARCHIVE_LAST_MANIFEST_QUERY = text("""
    SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day
    ORDER BY page_ordinal DESC LIMIT 1
""")

_ARCHIVE_SOURCE_PAGE_QUERY = text(f"""
    WITH candidates AS MATERIALIZED (
        SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM}
        WHERE versions.storage_day=:day AND (versions.market_commit_seq,versions.id) > (:seq,:id)
        ORDER BY versions.market_commit_seq,versions.id LIMIT :limit
    ), documents AS MATERIALIZED (
        SELECT id, market_commit_seq, to_jsonb(candidates)::text AS document
        FROM candidates
    ), budget AS (
        SELECT id, sum(octet_length(document)::bigint + 35 + 11::bigint *
            char_length(regexp_replace(document COLLATE "C", '[ -~]+', '', 'g')))
            OVER (ORDER BY market_commit_seq,id) AS cumulative_bytes FROM documents
    )
    SELECT candidates.* FROM candidates JOIN budget USING(id)
    WHERE budget.cumulative_bytes <= :max_bytes
    ORDER BY candidates.market_commit_seq,candidates.id
""")

_ARCHIVE_UNVERIFIED_PAGE_QUERY = text("""
    SELECT pages.* FROM market.fact_archive_manifests AS pages
    LEFT JOIN market.fact_archive_verifications AS proof
      ON proof.manifest_id=pages.id AND proof.verifier_version=:version
    WHERE pages.storage_day=:day AND proof.manifest_id IS NULL
    ORDER BY pages.page_ordinal LIMIT 1
""")

_ARCHIVE_SOURCE_ENVELOPES_QUERY = text(f"""
    SELECT {CANONICAL_ENVELOPE_COLUMNS} {CANONICAL_ENVELOPE_FROM}
    WHERE versions.storage_day=:day
      AND (versions.market_commit_seq, versions.id) >= (:first_seq, :first_id)
      AND (versions.market_commit_seq, versions.id) <= (:last_seq, :last_id)
    ORDER BY versions.market_commit_seq, versions.id LIMIT :limit
""")

_ARCHIVE_EXPIRED_DEPENDENCY_QUERY = text("""
    SELECT requested.target_id
    FROM unnest(CAST(:kinds AS text[]), CAST(:ids AS text[]))
        WITH ORDINALITY AS requested(target_kind, target_id, ordinal)
    WHERE EXISTS (
        SELECT 1 FROM market.storage_lifecycle_events AS lifecycle
        WHERE lifecycle.action='archive_expire' AND lifecycle.event_type='completed'
          AND lifecycle.target_kind=requested.target_kind
          AND lifecycle.target_id=requested.target_id)
    ORDER BY requested.ordinal LIMIT 1
""")

ARCHIVE_PAGE_CATALOG_SPECS = (
    ("series", "fact_archive_series",
     "series_id, row_count, first_observation_at, last_observation_at, first_known_at, "
     "last_known_at, first_accepted_at, last_accepted_at", "series_id"),
    ("aliases", "fact_archive_material_aliases",
     "fact_version_id, series_id, evidence_key, material_hash", "fact_version_id, evidence_key"),
    ("dependencies", "fact_archive_dependencies",
     "target_kind, target_id, object_key, object_sha256", "target_kind, target_id"),
    ("canonical_dependencies", "fact_archive_canonical_dependencies",
     "fact_version_id, row_hash", "fact_version_id"),
)


def archive_page_catalog_statement(*, table: str, columns: str, order: str,
                                   handoff: bool = False):
    """Return the exact bounded catalog statement used by verification."""
    permitted = {(item[1], item[2], item[3]) for item in ARCHIVE_PAGE_CATALOG_SPECS}
    permitted.update({
        ("fact_archive_prepared_material_aliases", ARCHIVE_PAGE_CATALOG_SPECS[1][2], ARCHIVE_PAGE_CATALOG_SPECS[1][3]),
        ("fact_archive_prepared_canonical_dependencies", ARCHIVE_PAGE_CATALOG_SPECS[3][2], ARCHIVE_PAGE_CATALOG_SPECS[3][3]),
    })
    if (table, columns, order) not in permitted:
        raise ValueError("canonical_archive_catalog_statement_invalid")
    physical = ",ctid::text AS _handoff_tid,tableoid AS _handoff_oid" if handoff else ""
    identity_column = "preparation_id" if table.startswith("fact_archive_prepared_") else "manifest_id"
    return text(
        f"SELECT {columns}{physical} FROM market.{table} "
        f"WHERE {identity_column}=:id ORDER BY {order} LIMIT :limit"
    )


def _record_verification_phase(*, timings, phase, day, page, seconds, outcome):
    timings[phase] = timings.get(phase, 0.0) + seconds
    logger.info(
        "canonical_archive_verification_phase | storage_day=%s page_ordinal=%s "
        "manifest_id=%s phase=%s outcome=%s seconds=%.6f",
        day, page["page_ordinal"], page["id"], phase, outcome, seconds,
    )


def _timed_verification_call(*, timings, phase, day, page, operation):
    started = perf_counter()
    try:
        result = operation()
    except Exception:
        _record_verification_phase(
            timings=timings, phase=phase, day=day, page=page,
            seconds=perf_counter() - started, outcome="failed",
        )
        raise
    _record_verification_phase(
        timings=timings, phase=phase, day=day, page=page,
        seconds=perf_counter() - started, outcome="completed",
    )
    return result


def _record_stage_phase(*, phase, day, page_ordinal, seconds, outcome):
    logger.info(
        "canonical_archive_stage_phase | storage_day=%s page_ordinal=%s "
        "phase=%s outcome=%s seconds=%.6f",
        day, page_ordinal, phase, outcome, seconds,
    )


def _stage_database_snapshot(session):
    """Best-effort small PostgreSQL snapshot isolated by a SQL savepoint."""
    connection = session.connection()
    connection.exec_driver_sql("SAVEPOINT qt_archive_stage_observation")
    try:
        snapshot = dict(connection.execute(_ARCHIVE_STAGE_DATABASE_SNAPSHOT).mappings().one())
        connection.exec_driver_sql("RELEASE SAVEPOINT qt_archive_stage_observation")
        return snapshot
    except Exception as exc:
        connection.exec_driver_sql("ROLLBACK TO SAVEPOINT qt_archive_stage_observation")
        connection.exec_driver_sql("RELEASE SAVEPOINT qt_archive_stage_observation")
        logger.warning(
            "canonical_archive_stage_database | outcome=observation_unavailable error_type=%s",
            type(exc).__name__,
        )
        return None


def _record_stage_database_observation(*, day, page_ordinal, before, after, outcome):
    if before is None:
        logger.info(
            "canonical_archive_stage_database | storage_day=%s page_ordinal=%s outcome=%s "
            "scope=cluster_overlap backend_pid=unavailable server_version_num=unavailable "
            "track_io_timing=unavailable device_attribution=unsupported deltas=unavailable",
            day, page_ordinal, outcome,
        )
        return
    wal_reset = after is not None and before["wal_stats_reset"] == after["wal_stats_reset"]
    writer_reset = after is not None and before["writer_stats_reset"] == after["writer_stats_reset"]
    deltas = {}
    for name in _ARCHIVE_STAGE_DATABASE_COUNTERS:
        comparable = wal_reset if name.startswith("wal_") else writer_reset
        deltas[name] = after[name] - before[name] if comparable else None
    logger.info(
        "canonical_archive_stage_database | storage_day=%s page_ordinal=%s outcome=%s "
        "scope=cluster_overlap backend_pid=%s server_version_num=%s track_io_timing=%s "
        "device_attribution=unsupported wal_records=%s wal_fpi=%s wal_bytes=%s "
        "wal_buffers_full=%s checkpoints_timed=%s checkpoints_req=%s "
        "checkpoint_write_time_ms=%s checkpoint_sync_time_ms=%s "
        "buffers_checkpoint=%s buffers_clean=%s buffers_backend=%s "
        "buffers_backend_fsync=%s",
        day, page_ordinal, outcome, before["backend_pid"], before["server_version_num"],
        before["track_io_timing"], *(deltas[name] for name in _ARCHIVE_STAGE_DATABASE_COUNTERS),
    )


def _timed_stage_call(*, phase, day, page_ordinal, operation):
    started = perf_counter()
    try:
        result = operation()
    except BaseException:
        _record_stage_phase(
            phase=phase, day=day, page_ordinal=page_ordinal,
            seconds=perf_counter() - started, outcome="failed",
        )
        raise
    _record_stage_phase(
        phase=phase, day=day, page_ordinal=page_ordinal,
        seconds=perf_counter() - started, outcome="completed",
    )
    return result


def _observe_stage_commit(session, *, day, page_ordinal, database_before):
    state = {"started": None, "recorded": False, "database_recorded": False}

    def before_commit(_session):
        state["started"] = perf_counter()
        state["database_after"] = _stage_database_snapshot(_session)

    def after_commit(_session):
        if state["started"] is not None:
            _record_stage_phase(
                phase="transaction_commit", day=day, page_ordinal=page_ordinal,
                seconds=perf_counter() - state["started"], outcome="completed",
            )
            _record_stage_database_observation(
                day=day, page_ordinal=page_ordinal, before=database_before,
                after=state["database_after"], outcome="committed",
            )
            state["database_recorded"] = True
            state["recorded"] = True

    def after_rollback(_session):
        if not state["database_recorded"]:
            _record_stage_database_observation(
                day=day, page_ordinal=page_ordinal, before=database_before,
                after=None, outcome="rolled_back_delta_unavailable",
            )
            state["database_recorded"] = True
        if state["started"] is not None and not state["recorded"]:
            _record_stage_phase(
                phase="transaction_commit", day=day, page_ordinal=page_ordinal,
                seconds=perf_counter() - state["started"], outcome="failed",
            )
            state["recorded"] = True

    event.listen(session, "before_commit", before_commit)
    event.listen(session, "after_commit", after_commit)
    event.listen(session, "after_rollback", after_rollback)


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


def _verified_generation_bindings(session, manifest_ids):
    """Bind every active pointer to the exact current-version receipt."""
    manifest_ids = list(manifest_ids)
    rows = session.execute(text("""
        SELECT scope.manifest_id,
               active.preparation_id AS active_preparation_id,
               active.generation AS active_generation,
               active.catalog_commitment AS active_catalog_commitment,
               binding.preparation_id AS binding_preparation_id,
               binding.catalog_commitment AS binding_catalog_commitment
        FROM unnest(CAST(:ids AS text[])) AS scope(manifest_id)
        LEFT JOIN market.fact_archive_active_generations AS active
          ON active.manifest_id=scope.manifest_id
        LEFT JOIN market.fact_archive_verification_generations AS binding
          ON binding.manifest_id=scope.manifest_id
         AND binding.verifier_version=:version
        ORDER BY scope.manifest_id
    """), {"ids": manifest_ids,
           "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().all()
    if len(rows) != len(manifest_ids):
        raise RuntimeError("canonical_archive_verification_generation_scope_changed")
    result = {}
    for row in rows:
        active = row["active_preparation_id"]
        bound = row["binding_preparation_id"]
        if active is None and bound is None:
            result[row["manifest_id"]] = None
            continue
        if (active is None or bound is None or active != bound
                or row["active_catalog_commitment"]
                   != row["binding_catalog_commitment"]):
            raise RuntimeError(
                "canonical_archive_verification_generation_changed: "
                f"manifest_id={row['manifest_id']}"
            )
        result[row["manifest_id"]] = {
            "manifest_id": row["manifest_id"],
            "preparation_id": active,
            "generation": row["active_generation"],
            "catalog_commitment": row["active_catalog_commitment"],
        }
    return result


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
        self._prepared_page = None
        self._current_stage_page_ordinal = None
        self._archive_generation_owner = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        self._generation = ArchiveGenerationCoordinator(
            database=self.database, lock_day=self._lock, check_budget=self.check_budget,
            statement_timeout_ms=self.statement_timeout_ms,
            owner_token_hash=self._archive_generation_owner,
            verifier_version=FACT_ARCHIVE_VERIFIER_VERSION,
        )

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
        self._assert_dependencies_unexpired(session,
            [{"target_kind": "raw_manifest", "target_id": identity} for identity in sorted(references)],
            check_budget=self.check_budget)
        for identity, reference in sorted(references.items()):
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
        This repository may retain one bounded prepared page between those calls;
        the scheduled executor confines its lifetime to one bounded run.
        An interrupted publication can leave an unreferenced immutable object.
        Retrying reads the same source cursor and safely reuses identical bytes;
        progress advances only in the catalog transaction.
        """
        started = perf_counter()
        self._current_stage_page_ordinal = None
        try:
            result = self._stage_next_page(day)
        except BaseException:
            _record_stage_phase(
                phase="stage_call_total", day=day,
                page_ordinal=self._current_stage_page_ordinal,
                seconds=perf_counter() - started, outcome="failed",
            )
            # A failed transaction or cancellation cannot retain prepared input.
            self._prepared_page = None
            raise
        _record_stage_phase(
            phase="stage_call_total", day=day,
            page_ordinal=self._current_stage_page_ordinal,
            seconds=perf_counter() - started, outcome="completed",
        )
        return result

    def _stage_catalog_rows(self, session, record_type, manifest_id, rows):
        """Bound each write and observe cancellation within the page transaction."""
        batch = []
        for values in rows:
            self._check_budget()
            batch.append(record_type(manifest_id=manifest_id, **values))
            if len(batch) == _CATALOG_WRITE_BATCH_ROWS:
                session.add_all(batch)
                session.flush()
                self._check_budget()
                batch = []
        if batch:
            session.add_all(batch)
            session.flush()
        self._check_budget()

    def _stage_canonical_dependency_catalog_rows(self, session, manifest_id, rows):
        """Insert fixed-width source identities in bounded typed-array statements."""
        row_count = 0
        batch_count = 0
        max_batch_seconds = 0.0
        batch = []

        def insert(values):
            nonlocal row_count, batch_count, max_batch_seconds
            started = perf_counter()
            try:
                result = session.execute(_CANONICAL_DEPENDENCY_CATALOG_INSERT, {
                    "manifest_id": manifest_id,
                    "fact_version_ids": [item["fact_version_id"] for item in values],
                    "row_hashes": [item["row_hash"] for item in values],
                })
                elapsed = perf_counter() - started
                max_batch_seconds = max(max_batch_seconds, elapsed)
                if result.rowcount != len(values):
                    raise RuntimeError(
                        "canonical_archive_dependency_insert_count_mismatch: "
                        f"manifest_id={manifest_id} expected={len(values)} actual={result.rowcount}"
                    )
            except BaseException:
                elapsed = perf_counter() - started
                max_batch_seconds = max(max_batch_seconds, elapsed)
                logger.info(
                    "canonical_archive_catalog_batches | catalog=canonical_dependency "
                    "manifest_id=%s outcome=failed rows=%s completed_batches=%s "
                    "attempted_batch_rows=%s failed_batch_seconds=%.6f max_batch_seconds=%.6f",
                    manifest_id, row_count, batch_count, len(values), elapsed, max_batch_seconds,
                )
                raise
            row_count += len(values)
            batch_count += 1

        for values in rows:
            self._check_budget()
            batch.append(values)
            if len(batch) != _CATALOG_WRITE_BATCH_ROWS:
                continue
            insert(batch)
            batch = []
            self._check_budget()
        if batch:
            insert(batch)
        self._check_budget()
        logger.info(
            "canonical_archive_catalog_batches | catalog=canonical_dependency "
            "manifest_id=%s outcome=completed rows=%s batches=%s max_batch_seconds=%.6f",
            manifest_id, row_count, batch_count, max_batch_seconds,
        )
        return {
            "row_count": row_count,
            "batch_count": batch_count,
            "max_batch_seconds": max_batch_seconds,
        }

    def _stage_alias_catalog_batches(self, session, manifest_id, aliases):
        """Insert already ordered aliases through bounded controlled statements."""
        row_count = 0
        batch_count = 0
        max_batch_seconds = 0.0
        batch = []

        def insert(values):
            nonlocal row_count, batch_count, max_batch_seconds
            started = perf_counter()
            try:
                result = session.execute(_ALIAS_CATALOG_INSERT, {
                    "manifest_id": manifest_id,
                    "fact_version_ids": [item["fact_version_id"] for item in values],
                    "evidence_keys": [item["evidence_key"] for item in values],
                    "series_ids": [item["series_id"] for item in values],
                    "material_hashes": [item["material_hash"] for item in values],
                })
                elapsed = perf_counter() - started
                max_batch_seconds = max(max_batch_seconds, elapsed)
                if result.rowcount != len(values):
                    raise RuntimeError(
                        f"canonical_archive_alias_insert_count_mismatch: manifest_id={manifest_id} "
                        f"expected={len(values)} actual={result.rowcount}"
                    )
            except BaseException:
                elapsed = perf_counter() - started
                max_batch_seconds = max(max_batch_seconds, elapsed)
                logger.info(
                    "canonical_archive_catalog_batches | catalog=material_alias "
                    "manifest_id=%s outcome=failed rows=%s completed_batches=%s "
                    "attempted_batch_rows=%s failed_batch_seconds=%.6f max_batch_seconds=%.6f",
                    manifest_id, row_count, batch_count, len(values), elapsed, max_batch_seconds,
                )
                raise
            row_count += len(values)
            batch_count += 1

        for values in aliases:
            self._check_budget()
            batch.append(values)
            if len(batch) != _ALIAS_CATALOG_WRITE_BATCH_ROWS:
                continue
            insert(batch)
            batch = []
            self._check_budget()
        if batch:
            insert(batch)
        self._check_budget()
        logger.info(
            "canonical_archive_catalog_batches | catalog=material_alias "
            "manifest_id=%s outcome=completed rows=%s batches=%s max_batch_seconds=%.6f",
            manifest_id, row_count, batch_count, max_batch_seconds,
        )
        return {
            "row_count": row_count,
            "batch_count": batch_count,
            "max_batch_seconds": max_batch_seconds,
        }

    def _stage_alias_catalog_rows(self, session, manifest_id, rows):
        """Derive one deterministic order for both alias identity indexes."""
        aliases = []
        for values in rows:
            self._check_budget()
            aliases.append(values)
        aliases.sort(key=lambda item: (item["fact_version_id"], item["evidence_key"]))
        self._check_budget()
        return self._stage_alias_catalog_batches(session, manifest_id, aliases)

    def _stage_next_page(self, day: date) -> dict:
        prepared = self._prepare_next_page_plan(day)
        plan = prepared.get("_generation_plan") if isinstance(prepared, dict) else None
        if plan is None:
            return prepared
        claim = _timed_stage_call(
            phase="catalog_generation_claim", day=day, page_ordinal=plan["page_ordinal"],
            operation=lambda: self._generation.claim(
                day=day, page_ordinal=plan["page_ordinal"], manifest=plan["manifest"],
                series_catalog=plan["series"], dependencies=plan["dependencies"],
                aliases=plan["aliases"],
                canonical_dependencies=plan["canonical_dependencies"],
            ),
        )
        ready = _timed_stage_call(
            phase="catalog_generation_stage", day=day, page_ordinal=plan["page_ordinal"],
            operation=lambda: self._generation.stage(
                day=day, claim=claim, alias_batch_rows=_ALIAS_CATALOG_WRITE_BATCH_ROWS,
                dependency_batch_rows=_CATALOG_WRITE_BATCH_ROWS,
            ),
        )
        activated = _timed_stage_call(
            phase="catalog_generation_activate", day=day,
            page_ordinal=plan["page_ordinal"],
            operation=lambda: self._generation.activate(
                day=day, preparation_id=ready["id"], fence=ready["fence"],
            ),
        )
        logger.info(
            "canonical_archive_page_acknowledged | storage_day=%s page_ordinal=%s manifest_id=%s "
            "rows=%s bytes=%s generation=%s preparation_id=%s catalog_commitment=%s",
            day, plan["page_ordinal"], plan["manifest"].manifest_id,
            plan["manifest"].row_count, plan["manifest"].byte_count,
            activated["generation"], activated["preparation_id"], activated["catalog_commitment"],
        )
        return {
            "storage_day": day, "status": "page_acknowledged",
            "page_ordinal": plan["page_ordinal"], "manifest_id": plan["manifest"].manifest_id,
            "row_count": plan["manifest"].row_count,
            "canonical_dependency_count": len(plan["canonical_dependencies"]),
            "canonical_dependency_batch_count": (
                len(plan["canonical_dependencies"]) + _CATALOG_WRITE_BATCH_ROWS - 1
            ) // _CATALOG_WRITE_BATCH_ROWS,
            "catalog_generation": activated["generation"],
        }

    def _prepare_next_page_plan(self, day: date) -> dict:
        with self.database.session() as session:
            admission_started = perf_counter()
            partition = self._lock(session, day)
            if partition["state"] != "sealed":
                raise RuntimeError(f"canonical_archive_partition_not_sealed: storage_day={day} state={partition['state']}")
            previous = session.execute(
                _ARCHIVE_LAST_MANIFEST_QUERY, {"day": day}
            ).mappings().one_or_none()
            cursor = _catalog_manifest(previous).last_cursor if previous else (0, "")
            ordinal = previous["page_ordinal"] + 1 if previous else 0
            self._current_stage_page_ordinal = ordinal
            database_before = _stage_database_snapshot(session)
            _record_stage_phase(
                phase="partition_admission_cursor", day=day, page_ordinal=ordinal,
                seconds=perf_counter() - admission_started, outcome="completed",
            )
            logger.info(
                "canonical_archive_stage_backend | storage_day=%s page_ordinal=%s backend_pid=%s "
                "server_version_num=%s track_io_timing=%s wait_sampling=external_pid "
                "device_attribution=unsupported",
                day, ordinal,
                database_before["backend_pid"] if database_before is not None else "unavailable",
                database_before["server_version_num"] if database_before is not None else "unavailable",
                database_before["track_io_timing"] if database_before is not None else "unavailable",
            )
            _observe_stage_commit(
                session, day=day, page_ordinal=ordinal, database_before=database_before,
            )
            # Source rows are immutable and the sealed day rejects new inserts.
            # Every call still locks/rechecks the partition, policy and committed
            # page cursor. Raw/checkpoint dependencies are resolved afresh below.
            key = (day, partition["sealed_at"], partition["expected_rows"], cursor,
                   ordinal, self.limits, self.max_raw_mapping_rows, FACT_ARCHIVE_VERIFIER_VERSION)
            if self._prepared_page is not None and self._prepared_page[0] == key:
                _, rows, source_rows = self._prepared_page
                logger.debug("canonical_archive_prepared_page_reused | storage_day=%s page_ordinal=%s rows=%s",
                             day, ordinal, len(rows))
            else:
                self._prepared_page = None
                params = {"day": day, "seq": cursor[0], "id": cursor[1], "limit": self.limits.max_rows,
                          "max_bytes": self.limits.max_logical_bytes}
                # Bound the ASCII JSON used by the archive codec before hydration.
                # Printable ASCII already has its JSON escaping in PostgreSQL's
                # text. Every other character needs at most 12 ASCII bytes (a UTF-16
                # surrogate pair); reserve 11 beyond its existing UTF-8 bytes.
                # Five typed timestamps may each gain seven microsecond characters.
                # The C collation keeps the printable range independent of locale.
                source = _timed_stage_call(
                    phase="source_select", day=day, page_ordinal=ordinal,
                    operation=lambda: session.execute(
                        _ARCHIVE_SOURCE_PAGE_QUERY, params
                    ).mappings().all(),
                )
                if not source:
                    def prove_source_exhausted():
                        remaining = session.execute(text(
                            "SELECT EXISTS (SELECT 1 FROM market.fact_versions WHERE storage_day=:day "
                            "AND (market_commit_seq,id) > (:seq,:id))"
                        ), params).scalar_one()
                        if remaining:
                            raise RuntimeError(
                                f"canonical_archive_source_row_budget_exceeded: storage_day={day}; "
                                "raise the explicit logical-byte budget"
                            )
                        count = session.execute(text(
                            "SELECT coalesce(sum(row_count),0) FROM market.fact_archive_manifests "
                            "WHERE storage_day=:day"
                        ), {"day": day}).scalar_one()
                        if count != partition["expected_rows"]:
                            raise RuntimeError(f"canonical_archive_source_coverage_mismatch: storage_day={day}")
                        return count
                    count = _timed_stage_call(
                        phase="source_exhaustion_proof", day=day, page_ordinal=ordinal,
                        operation=prove_source_exhausted,
                    )
                    return {"storage_day": day, "status": "source_exhausted", "archived_rows": count}
                rows = [{key: value for key, value in row.items() if key != "storage_day"} for row in source]

                def validate_payloads():
                    ensure_payload_contracts(
                        session, [(row["payload_schema_id"], row["payload_contract_hash"]) for row in rows]
                    )
                    # Validate canonical payload/provenance hashes before following
                    # any claimed source reference or publishing bytes.
                    for row in rows:
                        self._check_budget()
                        record_from_storage_row(row)
                _timed_stage_call(
                    phase="payload_contract_and_row_validation", day=day, page_ordinal=ordinal,
                    operation=validate_payloads,
                )

                def resolve_source_rows():
                    nonlocal rows
                    # The roots can fit while their complete causal sources do not.
                    # Reduce only this new ordered page; never lower read limits or
                    # trim an individual root's correction/provenance closure.
                    while True:
                        self._check_budget()
                        try:
                            return self._source_revisions(session, rows)
                        except CanonicalSourceByteBudgetExceeded as exc:
                            if len(rows) == 1:
                                exc.add_note(f"storage_day={day} fact_version_id={rows[0]['id']}; "
                                             "one root cannot fit; source cursor unchanged")
                                raise
                            next_count = max(1, len(rows) // 2)
                            logger.warning(
                                "canonical_archive_page_source_budget_reduced | storage_day=%s page_ordinal=%s "
                                "rows_before=%s rows_after=%s logical_bytes=%s max_logical_bytes=%s",
                                day, ordinal, len(rows), next_count, exc.logical_bytes, exc.max_logical_bytes)
                            rows = rows[:next_count]
                source_rows = _timed_stage_call(
                    phase="causal_source_resolution", day=day, page_ordinal=ordinal,
                    operation=resolve_source_rows,
                )
                self._prepared_page = (key, rows, source_rows)

            progress = _timed_stage_call(
                phase="prefix_preparation", day=day, page_ordinal=ordinal,
                operation=lambda: self._prepare_book_prefix(session, (*rows, *source_rows), day),
            )
            if progress is not None:
                return progress
            # Publication/verification never retain a second prepared page.
            self._prepared_page = None
            dependencies, source_rows = _timed_stage_call(
                phase="dependency_proof", day=day, page_ordinal=ordinal,
                operation=lambda: self._dependencies(session, rows, source_rows=source_rows),
            )
            observer = lambda phase, seconds, outcome: _record_stage_phase(
                phase=phase, day=day, page_ordinal=ordinal,
                seconds=seconds, outcome=outcome,
            )
            manifest = _timed_stage_call(
                phase="archive_publish_total", day=day, page_ordinal=ordinal,
                operation=lambda: publish_canonical_fact_archive(
                    rows, object_store=self.object_store,
                    temporary_directory=self.temporary_directory, limits=self.limits,
                    phase_observer=observer,
                ),
            )
            _timed_stage_call(
                phase="archive_page_readback", day=day, page_ordinal=ordinal,
                operation=lambda: read_canonical_fact_archive(
                    self.object_store.local_path(manifest.object_key),
                    expected=manifest, limits=self.limits,
                ),
            )
            _timed_stage_call(
                phase="source_row_verification", day=day, page_ordinal=ordinal,
                operation=lambda: verify_canonical_fact_archive_rows(
                    rows, expected=manifest, limits=self.limits,
                ),
            )
            self._check_budget()

            aliases = tuple(
                alias for row in rows if (alias := legacy_material_alias(row)) is not None
            )
            canonical_dependencies = tuple(
                {"fact_version_id": row["id"], "row_hash": row["row_hash"]}
                for row in source_rows
            )
            return {"_generation_plan": {
                "page_ordinal": ordinal,
                "manifest": manifest,
                "series": tuple(_series_catalog(manifest)),
                "dependencies": tuple(dependencies),
                "aliases": aliases,
                "canonical_dependencies": canonical_dependencies,
            }}

    def _page_catalogs(self, session, manifest_id, *, handoff=None):
        catalogs = {}
        physical = {}
        # Explicit projections define the versioned proof. Fetch one extra row
        # to reject excess metadata without transferring an unbounded catalog.
        limits = {
            "series": self.limits.max_rows,
            "aliases": self.limits.max_rows,
            "dependencies": self.max_dependency_objects,
            "canonical_dependencies": self.max_raw_mapping_rows,
        }
        active_generation = session.execute(text(
            "SELECT manifest_id,preparation_id,generation,catalog_commitment "
            "FROM market.fact_archive_active_generations WHERE manifest_id=:id"
        ), {"id": manifest_id}).mappings().one_or_none()
        active_preparation_id = (active_generation["preparation_id"]
                                 if active_generation is not None else None)
        tables = PREPARED_CATALOG_TABLES if active_generation is not None else LEGACY_CATALOG_TABLES
        for name, legacy_table, columns, order in ARCHIVE_PAGE_CATALOG_SPECS:
            table = tables.get(name, legacy_table)
            catalog_id = active_preparation_id if table.startswith("fact_archive_prepared_") else manifest_id
            limit = limits[name]
            rows = session.execute(archive_page_catalog_statement(
                table=table, columns=columns, order=order, handoff=handoff is not None,
            ), {"id": catalog_id, "limit": limit + 1}).mappings().all()
            if len(rows) > limit:
                raise RuntimeError(f"canonical_archive_catalog_budget_exceeded: manifest_id={manifest_id} catalog={name}")
            if handoff is not None:
                physical[name] = handoff.catalog_rows(session, name, manifest_id, rows, table=table, relation_identity=catalog_id)
                catalogs[name] = [{key: value for key, value in row.items()
                                   if key not in {"_handoff_tid", "_handoff_oid"}} for row in rows]
            else:
                catalogs[name] = [dict(row) for row in rows]
        return ((catalogs, physical, dict(active_generation) if active_generation is not None else None)
                if handoff is not None else catalogs)

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
        transaction_started = perf_counter()
        supersession_plan = None
        with self.database.session() as session:
            partition = self._lock(session, day)
            if partition["state"] != "sealed":
                raise RuntimeError(f"canonical_archive_partition_not_sealed: storage_day={day} state={partition['state']}")
            page = session.execute(_ARCHIVE_UNVERIFIED_PAGE_QUERY, {
                "day": day, "version": FACT_ARCHIVE_VERIFIER_VERSION,
            }).mappings().one_or_none()
            if page is None:
                return {"storage_day": day, "status": "no_unverified_pages"}
            manifest = _catalog_manifest(page)
            if manifest.row_count > self.limits.max_rows:
                raise RuntimeError(f"canonical_archive_page_row_budget_exceeded: manifest_id={manifest.manifest_id}")
            timings = {}
            _timed_verification_call(
                timings=timings, phase="payload_contracts", day=day, page=page,
                operation=lambda: ensure_payload_contracts(
                    session, [contract for bounds in manifest.series for contract in bounds.payload_contracts]
                ),
            )
            rows = _timed_verification_call(
                timings=timings, phase="archive_read", day=day, page=page,
                operation=lambda: read_canonical_fact_archive(
                    self.object_store.local_path(manifest.object_key), expected=manifest, limits=self.limits,
                ),
            )
            envelopes = _timed_verification_call(
                timings=timings, phase="source_envelopes", day=day, page=page,
                operation=lambda: session.execute(_ARCHIVE_SOURCE_ENVELOPES_QUERY, {
                       "day": day, "first_seq": manifest.first_cursor[0], "first_id": manifest.first_cursor[1],
                       "last_seq": manifest.last_cursor[0], "last_id": manifest.last_cursor[1],
                       "limit": self.limits.max_rows + 1}).mappings().all(),
            )

            def validate_envelopes():
                if len(envelopes) != len(rows):
                    raise RuntimeError(
                        f"canonical_archive_page_source_coverage_mismatch: manifest_id={manifest.manifest_id}"
                    )
                for envelope, archived in zip(envelopes, rows):
                    self._check_budget()
                    verify_archived_envelope(envelope, archived)
            _timed_verification_call(
                timings=timings, phase="envelope_validation", day=day, page=page,
                operation=validate_envelopes,
            )
            aliases = [alias for row in rows if (alias := legacy_material_alias(row)) is not None]
            catalogs = _timed_verification_call(
                timings=timings, phase="catalog_read", day=day, page=page,
                operation=lambda: self._page_catalogs(session, manifest.manifest_id),
            )
            bound_ids = [item["target_id"] for item in catalogs["dependencies"] if item["target_kind"] == "raw_manifest"]
            source_rows = _timed_verification_call(
                timings=timings, phase="causal_source_resolution", day=day, page=page,
                operation=lambda: self._source_revisions(session, rows),
            )
            progress = _timed_verification_call(
                timings=timings, phase="book_prefix", day=day, page=page,
                operation=lambda: self._prepare_book_prefix(
                    session, (*rows, *source_rows), day, bound_manifest_ids=bound_ids
                ),
            )
            if progress is not None:
                return progress
            bound_checkpoints = [item["target_id"] for item in catalogs["dependencies"] if item["target_kind"] == "book_checkpoint"]
            dependencies, source_rows = _timed_verification_call(
                timings=timings, phase="dependency_proof", day=day, page=page,
                operation=lambda: self._dependencies(
                    session, rows, bound_manifest_ids=bound_ids, source_rows=source_rows,
                    bound_checkpoint_ids=bound_checkpoints or None,
                ),
            )
            expected = {
                "series": _series_catalog(manifest),
                "aliases": sorted(aliases, key=lambda item: (item["fact_version_id"], item["evidence_key"])),
                "dependencies": sorted(dependencies, key=lambda item: (item["target_kind"], item["target_id"])),
                "canonical_dependencies": [{"fact_version_id": row["id"], "row_hash": row["row_hash"]} for row in source_rows],
            }
            # Older pages can gain newly required monotone proof edges under
            # this explicit verification transaction. Never rewrite bytes,
            # existing raw bindings, an existing edge, or a prior receipt.
            active_generation = session.execute(text(
                "SELECT preparation_id,generation,catalog_commitment "
                "FROM market.fact_archive_active_generations WHERE manifest_id=:id"
            ), {"id": manifest.manifest_id}).mappings().one_or_none()
            if active_generation is not None and any(
                    catalogs[name] != expected[name] for name in expected):
                # Do not mutate an active immutable generation. Persist the
                # replacement in independently committed hidden batches after
                # this proof transaction releases its locks. Supersession
                # rechecks monotonicity, expiry, ownership, and the exact
                # receipt before switching authority atomically.
                receipt = _receipt(page, archive_evidence_hash(expected))
                supersession_plan = {
                    "page": dict(page), "manifest": manifest,
                    "series": tuple(expected["series"]),
                    "aliases": tuple(expected["aliases"]),
                    "dependencies": tuple(expected["dependencies"]),
                    "canonical_dependencies": tuple(expected["canonical_dependencies"]),
                    "receipt": receipt,
                }
            else:
                additions = 0
                for name, model, identity_fields in (
                    ("aliases", MarketFactArchiveMaterialAliasRecord,
                     ("fact_version_id", "evidence_key")),
                    ("canonical_dependencies", MarketFactArchiveCanonicalDependencyRecord,
                     ("fact_version_id",)),
                    ("dependencies", MarketFactArchiveDependencyRecord,
                     ("target_kind", "target_id")),
                ):
                    existing = {
                        tuple(item[key] for key in identity_fields): item
                        for item in catalogs[name]
                    }
                    for item in expected[name]:
                        identity = tuple(item[key] for key in identity_fields)
                        if (identity not in existing
                                and (name in {"canonical_dependencies", "aliases"}
                                     or item["target_kind"] == "book_checkpoint")):
                            session.add(model(manifest_id=manifest.manifest_id, **item))
                            additions += 1
                if additions:
                    session.flush()
                    catalogs = self._page_catalogs(session, manifest.manifest_id)
                    logger.info(
                        "canonical_archive_proof_edges_added | manifest_id=%s "
                        "verifier_version=%s edges=%s",
                        manifest.manifest_id, FACT_ARCHIVE_VERIFIER_VERSION, additions,
                    )
                for name, items in expected.items():
                    if catalogs[name] != items:
                        raise RuntimeError(
                            "canonical_archive_catalog_incomplete: "
                            f"manifest_id={manifest.manifest_id} catalog={name}"
                        )
                receipt = _receipt(page, archive_evidence_hash(catalogs))
                self._check_budget()
                session.add(MarketFactArchiveVerificationRecord(**receipt))
                if active_generation is not None:
                    session.add(MarketFactArchiveVerificationGenerationRecord(
                        manifest_id=manifest.manifest_id,
                        verifier_version=FACT_ARCHIVE_VERIFIER_VERSION,
                        preparation_id=active_generation["preparation_id"],
                        catalog_commitment=active_generation["catalog_commitment"],
                    ))
        if supersession_plan is not None:
            plan = supersession_plan
            claim = _timed_verification_call(
                timings=timings, phase="catalog_supersession_claim", day=day,
                page=plan["page"], operation=lambda: self._generation.claim(
                    day=day, page_ordinal=plan["page"]["page_ordinal"],
                    manifest=plan["manifest"], series_catalog=plan["series"],
                    dependencies=plan["dependencies"], aliases=plan["aliases"],
                    canonical_dependencies=plan["canonical_dependencies"],
                ),
            )
            ready = _timed_verification_call(
                timings=timings, phase="catalog_supersession_stage", day=day,
                page=plan["page"], operation=lambda: self._generation.stage(
                    day=day, claim=claim,
                    alias_batch_rows=_ALIAS_CATALOG_WRITE_BATCH_ROWS,
                    dependency_batch_rows=_CATALOG_WRITE_BATCH_ROWS,
                ),
            )
            _timed_verification_call(
                timings=timings, phase="catalog_supersession_activate", day=day,
                page=plan["page"], operation=lambda: self._generation.supersede_verified(
                    day=day, preparation=ready,
                    verification_receipt=plan["receipt"],
                ),
            )
            receipt = plan["receipt"]
        _record_verification_phase(
            timings=timings, phase="transaction_total", day=day, page=page,
            seconds=perf_counter() - transaction_started, outcome="completed",
        )
        logger.info("canonical_archive_page_verified | storage_day=%s page_ordinal=%s manifest_id=%s rows=%s verification_hash=%s",
                    day, page["page_ordinal"], manifest.manifest_id, manifest.row_count, receipt["verification_hash"])
        return {"storage_day": day, "status": "page_verified", "page_ordinal": page["page_ordinal"],
                "manifest_id": manifest.manifest_id, "verification_hash": receipt["verification_hash"],
                "phase_seconds": {name: round(seconds, 6) for name, seconds in timings.items()}}

    @staticmethod
    def _assert_dependencies_unexpired(session, dependencies, *, check_budget=None):
        # The caller holds the lifecycle fence. Each bounded SQL statement checks
        # exact kind/id pairs; results are never cached across pages or passes.
        for offset in range(0, len(dependencies), _EXPIRATION_QUERY_BATCH_ROWS):
            if check_budget is not None:
                check_budget()
            batch = dependencies[offset:offset + _EXPIRATION_QUERY_BATCH_ROWS]
            expired = session.execute(_ARCHIVE_EXPIRED_DEPENDENCY_QUERY, {
                   "kinds": [row["target_kind"] for row in batch],
                   "ids": [row["target_id"] for row in batch]}).scalar_one_or_none()
            if expired is not None:
                raise RuntimeError(f"canonical_archive_dependency_expired: target_id={expired}")

    def _partition_evidence(self, session, partition, *, limits, objects=None, check_budget=None, handoff=None):
        """Check bounded catalogs/receipts and exact sealed-source coverage.

        Supplying a byte verifier additionally checks every current cold object.
        With no verifier this returns metadata evidence only, never permission
        to delete. The caller must own the partition and lifecycle fences.
        """
        day = partition["storage_day"]
        check_budget = check_budget or self.check_budget
        final_handoff = handoff is not None and objects is None
        if final_handoff:
            final_started = perf_counter()
            handoff.admit(session, partition, limits, check_budget=check_budget)
            logger.info("canonical_reclaim_handoff_progress | storage_day=%s stage=admitted seconds=%s",
                        day, perf_counter() - final_started)
            handoff.verify_headers(session, check_budget)
            logger.info("canonical_reclaim_handoff_progress | storage_day=%s stage=headers_current seconds=%s",
                        day, perf_counter() - final_started)
            check_budget()
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
            verified_generations = _verified_generation_bindings(
                session, [page["id"] for page in pages],
            )
            if final_handoff:
                # Same bounded metadata page, same final transaction. Read
                # receipts once per batch rather than once per manifest.
                receipts = session.execute(text("""
                    SELECT manifest_id, verifier_version, manifest_hash, catalog_hash, verification_hash
                    FROM market.fact_archive_verifications
                    WHERE manifest_id=ANY(CAST(:ids AS text[])) AND verifier_version=:version
                """), {"ids": [page["id"] for page in pages],
                        "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().all()
                receipt_by_id = {row["manifest_id"]: row for row in receipts}
                if len(receipts) != len(pages) or set(receipt_by_id) != {page["id"] for page in pages}:
                    raise RuntimeError("canonical_archive_verification_missing_or_stale: handoff receipt scope")
                handoff.verify_catalog_batch(session,
                    [(page, receipt_by_id[page["id"]]) for page in pages], check_budget)
                logger.info("canonical_reclaim_handoff_progress | storage_day=%s stage=catalogs_current first_page=%s pages=%s seconds=%s",
                            day, ordinal, len(pages), perf_counter() - final_started)
            for page in pages:
                if check_budget is not None:
                    check_budget()
                if ordinal >= limits.max_pages:
                    raise RuntimeError(f"canonical_archive_partition_page_budget_exceeded: storage_day={day}")
                manifest = _catalog_manifest(page)
                if page["page_ordinal"] != ordinal or manifest.first_cursor <= last_cursor:
                    raise RuntimeError(f"canonical_archive_partition_page_order_invalid: storage_day={day} page_ordinal={page['page_ordinal']}")
                if final_handoff:
                    catalogs = None
                elif handoff is not None:
                    catalogs, physical, generation = self._page_catalogs(
                        session, manifest.manifest_id, handoff=handoff,
                    )
                    if generation != verified_generations[manifest.manifest_id]:
                        raise RuntimeError(
                            "canonical_archive_verification_generation_changed: "
                            f"manifest_id={manifest.manifest_id}"
                        )
                else:
                    catalogs = self._page_catalogs(session, manifest.manifest_id)
                receipt = (receipt_by_id[manifest.manifest_id] if final_handoff else session.execute(text("""
                    SELECT manifest_id, verifier_version, manifest_hash, catalog_hash, verification_hash
                    FROM market.fact_archive_verifications WHERE manifest_id=:id AND verifier_version=:version
                """), {"id": manifest.manifest_id, "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one_or_none())
                if final_handoff:
                    handoff.verify_sources(session, manifest.manifest_id, check_budget)
                    source_placements.append(handoff.source_hash(manifest.manifest_id))
                else:
                    if receipt is None or dict(receipt) != _receipt(page, archive_evidence_hash(catalogs)):
                        raise RuntimeError(f"canonical_archive_verification_missing_or_stale: manifest_id={manifest.manifest_id}")
                    if handoff is not None:
                        handoff.capture_page(
                            page, receipt, catalogs, physical, generation=generation,
                        )
                    if objects is not None:
                        objects.verify(manifest.object_key, manifest.object_sha256, expected_bytes=manifest.byte_count)
                    placement_hash = verify_canonical_source_placements(
                        session, catalogs["canonical_dependencies"], objects=objects, check_budget=check_budget,
                        handoff=handoff, manifest_id=manifest.manifest_id)
                    source_placements.append(placement_hash)
                    if handoff is not None:
                        handoff.record_source_hash(manifest.manifest_id, placement_hash)
                    self._assert_dependencies_unexpired(
                        session, catalogs["dependencies"], check_budget=check_budget)
                    for dependency in catalogs["dependencies"]:
                        if check_budget is not None:
                            check_budget()
                        if objects is not None:
                            objects.verify(dependency["object_key"], dependency["object_sha256"])
                proofs.append(receipt["verification_hash"])
                total_rows += manifest.row_count
                last_cursor = manifest.last_cursor
                ordinal += 1
        if final_handoff:
            # All catalog contents were freshly matched above. Check each exact
            # target once, now, under the exclusive lifecycle fence; no expiry
            # result survives this transaction or comes from the first pass.
            self._assert_dependencies_unexpired(session,
                [{"target_kind": kind, "target_id": identity} for kind, identity in sorted(handoff.targets)],
                check_budget=check_budget)
            if ordinal != len(handoff.pages):
                raise RuntimeError("canonical_handoff_page_scope_changed")
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
        if final_handoff:
            logger.info("canonical_reclaim_handoff_progress | storage_day=%s stage=evidence_current pages=%s seconds=%s",
                        day, ordinal, perf_counter() - final_started)
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
            with objects.distinct_references():
                evidence = self._partition_evidence(session, partition, limits=limits, objects=objects)
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
