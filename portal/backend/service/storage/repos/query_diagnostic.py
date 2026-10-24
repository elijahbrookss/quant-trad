"""Bounded operator-only diagnostics for canonical retention SQL contracts.

This module executes a closed inventory of production SELECT builders. It does
not accept SQL, mutate production, decode archive objects, or grant maintenance
authority. Write builders remain qualified in the disposable PostgreSQL suite.
"""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import threading
from time import monotonic, perf_counter
from typing import Any, Callable, Mapping

from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.service.provenance import evidence_source_revision

from .fact_archival import (
    ARCHIVE_PAGE_CATALOG_SPECS,
    FACT_ARCHIVE_VERIFIER_VERSION,
    _ALIAS_CATALOG_INSERT,
    _ARCHIVE_EXPIRED_DEPENDENCY_QUERY,
    _ARCHIVE_LAST_MANIFEST_QUERY,
    _ARCHIVE_SOURCE_ENVELOPES_QUERY,
    _ARCHIVE_SOURCE_PAGE_QUERY,
    _ARCHIVE_UNVERIFIED_PAGE_QUERY,
    _CANONICAL_DEPENDENCY_CATALOG_INSERT,
    archive_page_catalog_statement,
)
from .fact_archive_generation import PREPARED_CATALOG_TABLES
from .fact_lineage import raw_archive_mapping_lookup_statement
from .fact_reclamation import (
    _PINNED_RANGES_QUERY,
    _RECLAIM_ELIGIBILITY_FAMILIES_QUERY,
    _RECLAIM_RELATION_QUERY,
)
from .fact_retention import (
    FACT_STORAGE_FAMILIES_SQL,
    RETENTION_PAGE_COUNTS_QUERY,
    RETENTION_PARTITION_TREE_QUERY,
    RETENTION_PHYSICAL_CHILDREN_QUERY,
    RETENTION_PROGRESS_QUERY,
    RETENTION_SIZE_INVENTORY_QUERY,
)
from .market_lifecycle import _LIFECYCLE_LOCK_NAME


SCHEMA_VERSION = "market.canonical_query_diagnostic.v1"
OUTCOMES = frozenset({
    "passed", "timed_out", "lock_refused", "changed", "refused",
    "unsupported", "untested",
})
_WRITE_TESTS = (
    "tests/test_market_data/test_fact_archival_db.py::"
    "test_catalogue_writes_finish_under_per_statement_limit_with_slow_rows",
    "tests/test_market_data/test_fact_archival_db.py::"
    "test_canonical_dependency_writes_finish_under_per_statement_limit_with_slow_rows",
    "tests/test_market_data/test_fact_archival_db.py::"
    "test_catalogue_mid_write_stop_rolls_back_page_and_reuses_immutable_object",
)
_CLEANUP_RESERVE_MS = 1_500
_CONNECT_TIMEOUT_SECONDS = 1
_SAMPLER_STATEMENT_TIMEOUT_MS = 250


@dataclass(frozen=True)
class QueryDiagnosticLimits:
    statement_timeout_ms: int = 5_000
    lock_timeout_ms: int = 250
    total_seconds: int = 60
    max_inventory_partitions: int = 10_000
    max_page_rows: int = 100_000
    max_page_logical_bytes: int = 1024**3
    max_dependency_rows: int = 100_000
    max_raw_mapping_rows: int = 50_000
    max_wait_samples: int = 64

    def __post_init__(self) -> None:
        if (
            type(self.statement_timeout_ms) is not int
            or not 1 <= self.statement_timeout_ms <= 5_000
            or type(self.lock_timeout_ms) is not int
            or not 1 <= self.lock_timeout_ms <= 250
            or type(self.total_seconds) is not int
            or not 1 <= self.total_seconds <= 60
            or type(self.max_inventory_partitions) is not int
            or not 1 <= self.max_inventory_partitions <= 10_000
            or type(self.max_page_rows) is not int
            or not 1 <= self.max_page_rows <= 100_000
            or type(self.max_page_logical_bytes) is not int
            or not 1 <= self.max_page_logical_bytes <= 1024**3
            or type(self.max_dependency_rows) is not int
            or not 1 <= self.max_dependency_rows <= 100_000
            or type(self.max_raw_mapping_rows) is not int
            or not 1 <= self.max_raw_mapping_rows <= 50_000
            or type(self.max_wait_samples) is not int
            or not 1 <= self.max_wait_samples <= 64
        ):
            raise ValueError("canonical_query_diagnostic_limits_invalid")


@dataclass(frozen=True)
class QueryCase:
    case_id: str
    boundary: str
    builder: str
    statement: Any
    params: Mapping[str, Any]
    requested_cardinality: int
    designation: str
    execution: str
    sample_waits: bool = False


class _StrictReadOnlyDatabase:
    """Small session boundary whose first server-side default forbids writes."""

    def __init__(self, engine):
        self.engine = engine

    @contextmanager
    def session(self):
        with self.engine.connect() as connection:
            with Session(bind=connection, expire_on_commit=False, autoflush=False,
                         future=True) as session:
                try:
                    yield session
                finally:
                    session.rollback()


@contextmanager
def _strict_read_only_database(source, limits: QueryDiagnosticLimits, *, deadline: float):
    """Open an isolated engine that is read-only before its first statement.

    The normal portal Database.session() bootstraps and validates schema before
    yielding. That is correct for application startup but is a mutation-capable
    boundary, so this diagnostic must not call it even on a healthy database.
    """
    remaining_ms = _remaining_milliseconds(deadline, reserve_ms=_CLEANUP_RESERVE_MS)
    dsn = source.dsn or source._resolve_dsn()
    options = dict(source._engine_options())
    connect_args = dict(options.get("connect_args") or {})
    existing = str(connect_args.get("options") or "").strip()
    readonly = (
        "-c default_transaction_read_only=on "
        f"-c statement_timeout={limits.statement_timeout_ms} "
        f"-c lock_timeout={limits.lock_timeout_ms}"
    )
    connect_args["options"] = " ".join(part for part in (existing, readonly) if part)
    connect_args["application_name"] = "qt_canonical_query_diagnostic"
    connect_args["connect_timeout"] = min(
        _CONNECT_TIMEOUT_SECONDS,
        max(1, remaining_ms // 1_000),
    )
    options["connect_args"] = connect_args
    options["pool_timeout"] = min(
        float(options.get("pool_timeout", _CONNECT_TIMEOUT_SECONDS)),
        float(_CONNECT_TIMEOUT_SECONDS),
        remaining_ms / 1_000,
    )
    engine = create_engine(dsn, **options)
    try:
        yield _StrictReadOnlyDatabase(engine)
    finally:
        engine.dispose()


def _sql_fingerprint(statement: Any) -> str:
    normalized = " ".join(str(statement).split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _plan_summary(plan: Mapping[str, Any]) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    nodes: Counter[str] = Counter()
    buffers: Counter[str] = Counter()
    wal: Counter[str] = Counter()
    pending = [plan]
    while pending:
        node = pending.pop()
        nodes[str(node.get("Node Type") or "unknown")] += 1
        for key in (
            "Shared Hit Blocks", "Shared Read Blocks", "Shared Dirtied Blocks",
            "Shared Written Blocks", "Local Hit Blocks", "Local Read Blocks",
            "Temp Read Blocks", "Temp Written Blocks",
        ):
            if type(node.get(key)) is int:
                buffers[key] += int(node[key])
        for key in ("WAL Records", "WAL FPI", "WAL Bytes"):
            if type(node.get(key)) in {int, float}:
                wal[key] += int(node[key])
        pending.extend(item for item in node.get("Plans", ()) if isinstance(item, Mapping))
    return dict(sorted(nodes.items())), dict(sorted(buffers.items())), dict(sorted(wal.items()))


def _db_code(exc: DBAPIError) -> str | None:
    return str(getattr(exc.orig, "pgcode", None) or getattr(exc.orig, "sqlstate", None) or "") or None


def _readonly(session, *, statement_timeout_ms: int, lock_timeout_ms: int) -> None:
    session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
    session.execute(text("SELECT set_config('statement_timeout', :value, true)"),
                    {"value": str(statement_timeout_ms)})
    session.execute(text("SELECT set_config('lock_timeout', :value, true)"),
                    {"value": str(lock_timeout_ms)})
    if session.execute(text("SHOW transaction_read_only")).scalar_one() != "on":
        raise RuntimeError("canonical_query_diagnostic_read_only_required")


def _remaining_milliseconds(deadline: float, *, reserve_ms: int = 0) -> int:
    remaining = int((deadline - monotonic()) * 1000) - reserve_ms
    if remaining <= 0:
        raise TimeoutError("canonical_query_diagnostic_total_budget_exhausted")
    return remaining


class _WaitSampler:
    def __init__(self, database, pid: int, *, maximum: int, deadline: float):
        self.database = database
        self.pid = pid
        self.maximum = maximum
        self.deadline = deadline
        self.samples: list[dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.error: str | None = None

    def _run(self) -> None:
        try:
            with self.database.session() as session:
                sampler_ms = min(
                    _SAMPLER_STATEMENT_TIMEOUT_MS,
                    _remaining_milliseconds(self.deadline, reserve_ms=_CLEANUP_RESERVE_MS),
                )
                _readonly(session, statement_timeout_ms=sampler_ms, lock_timeout_ms=100)
                while (
                    not self._stop.is_set()
                    and len(self.samples) < self.maximum
                    and monotonic() < self.deadline
                ):
                    row = session.execute(text("""
                        SELECT state,wait_event_type,wait_event,
                               extract(epoch FROM clock_timestamp()-query_start) AS active_age_seconds
                        FROM pg_stat_activity WHERE pid=:pid
                    """), {"pid": self.pid}).mappings().one_or_none()
                    if row is not None:
                        self.samples.append({
                            "state": str(row["state"] or "unknown"),
                            "wait_event_type": str(row["wait_event_type"] or "none"),
                            "wait_event": str(row["wait_event"] or "none"),
                            "active_age_seconds": round(float(row["active_age_seconds"] or 0.0), 6),
                        })
                    self._stop.wait(0.1)
        except Exception as exc:  # diagnostic sampling cannot change query outcome
            self.error = type(exc).__name__

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_args):
        self._stop.set()
        remaining = max(0.0, self.deadline - monotonic())
        self._thread.join(timeout=min(0.3, remaining))
        if self._thread.is_alive() and self.error is None:
            self.error = "sampler_cleanup_incomplete"

    def summary(self) -> dict[str, Any]:
        categories = Counter(
            f"{row['wait_event_type']}:{row['wait_event']}" for row in self.samples
        )
        return {
            "sample_count": len(self.samples),
            "categories": dict(sorted(categories.items())),
            "max_active_age_seconds": max(
                (row["active_age_seconds"] for row in self.samples), default=None
            ),
            "unavailable_reason": self.error if not self.samples else None,
        }


def _select_scope(database, storage_day: date, limits: QueryDiagnosticLimits,
                  *, deadline: float) -> dict[str, Any]:
    with database.session() as session:
        setup_ms = min(
            limits.statement_timeout_ms,
            _remaining_milliseconds(deadline, reserve_ms=_CLEANUP_RESERVE_MS),
        )
        _readonly(session, statement_timeout_ms=setup_ms,
                  lock_timeout_ms=limits.lock_timeout_ms)

        def execute(statement, params=None):
            statement_ms = min(
                limits.statement_timeout_ms,
                _remaining_milliseconds(deadline, reserve_ms=_CLEANUP_RESERVE_MS),
            )
            session.execute(
                text("SELECT set_config('statement_timeout', :value, true)"),
                {"value": str(statement_ms)},
            )
            return session.execute(statement, params or {})

        owner_count = execute(text("""
            WITH key AS (SELECT hashtextextended(:name,0) AS value)
            SELECT count(*) FROM pg_locks,key WHERE locktype='advisory'
              AND mode IN ('ExclusiveLock','ShareLock') AND granted AND objsubid=1
              AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
              AND classid::bigint=((key.value>>32)&4294967295)
              AND objid::bigint=(key.value&4294967295)
        """), {"name": _LIFECYCLE_LOCK_NAME}).scalar_one()
        if owner_count:
            raise RuntimeError("canonical_query_diagnostic_lifecycle_owner_active")
        partition = execute(text("""
            SELECT storage_day,state,expected_rows,manifest_set_hash
            FROM market.fact_retention_partitions WHERE storage_day=:day
        """), {"day": storage_day}).mappings().one_or_none()
        if partition is None or partition["state"] != "verified":
            state = None if partition is None else partition["state"]
            raise RuntimeError(
                f"canonical_query_diagnostic_partition_not_verified: storage_day={storage_day} state={state}"
            )
        page = execute(text("""
            SELECT pages.* FROM market.fact_archive_manifests AS pages
            JOIN market.fact_archive_verifications AS proof
              ON proof.manifest_id=pages.id AND proof.verifier_version=:version
            WHERE pages.storage_day=:day ORDER BY pages.page_ordinal LIMIT 1
        """), {"day": storage_day, "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one_or_none()
        if page is None:
            raise RuntimeError("canonical_query_diagnostic_committed_page_required")
        if int(page["row_count"]) > limits.max_page_rows:
            raise RuntimeError("canonical_query_diagnostic_page_row_budget_exceeded")
        raw = execute(text("""
            SELECT mappings.raw_record_id,mappings.session_id,mappings.connection_epoch,
                   mappings.receive_ordinal,manifests.id AS manifest_id,
                   manifests.definition_id,manifests.first_receive_ordinal,
                   manifests.last_receive_ordinal
            FROM market.fact_archive_dependencies AS dependencies
            JOIN market.raw_archive_manifests AS manifests
              ON dependencies.target_kind='raw_manifest'
             AND dependencies.target_id=manifests.id
            JOIN market.raw_archive_record_mappings AS mappings
              ON mappings.manifest_id=manifests.id
            WHERE dependencies.manifest_id=:page_id
              AND NOT EXISTS (
                SELECT 1 FROM market.storage_lifecycle_events AS expired
                WHERE expired.action='archive_expire' AND expired.event_type='completed'
                  AND expired.target_kind='raw_manifest' AND expired.target_id=manifests.id)
            ORDER BY manifests.acknowledged_at DESC,mappings.raw_record_id LIMIT 1
        """), {"page_id": page["id"]}).mappings().one_or_none()
        dependency = execute(text("""
            SELECT target_kind,target_id FROM market.fact_archive_dependencies
            WHERE manifest_id=:page_id ORDER BY target_kind,target_id LIMIT 1
        """), {"page_id": page["id"]}).mappings().one_or_none()
        generation = execute(text("""
            SELECT preparation_id,generation,catalog_commitment
            FROM market.fact_archive_active_generations WHERE manifest_id=:page_id
        """), {"page_id": page["id"]}).mappings().one_or_none()
        version = execute(text("SHOW server_version")).scalar_one()
        return {
            "partition": dict(partition), "page": dict(page),
            "raw": None if raw is None else dict(raw),
            "dependency": None if dependency is None else dict(dependency),
            "generation": None if generation is None else dict(generation),
            "postgresql_version": str(version),
        }


def _cases(scope: Mapping[str, Any], limits: QueryDiagnosticLimits) -> list[QueryCase]:
    day = scope["partition"]["storage_day"]
    page = scope["page"]
    relation = fact_partition_name(day)
    cases = [
        QueryCase("retention.size_inventory", "retention_planning",
                  "fact_retention.RETENTION_SIZE_INVENTORY_QUERY",
                  RETENTION_SIZE_INVENTORY_QUERY, {}, 1, "representative", "analyze"),
        QueryCase("retention.partition_tree", "retention_planning",
                  "fact_retention.RETENTION_PARTITION_TREE_QUERY",
                  RETENTION_PARTITION_TREE_QUERY,
                  {"limit": limits.max_inventory_partitions + 2},
                  limits.max_inventory_partitions + 2,
                  "maximum_supported", "plan"),
        QueryCase("retention.progress", "retention_planning",
                  "fact_retention.RETENTION_PROGRESS_QUERY",
                  RETENTION_PROGRESS_QUERY,
                  {"limit": limits.max_inventory_partitions + 1},
                  limits.max_inventory_partitions + 1,
                  "maximum_supported", "plan"),
        QueryCase("retention.physical_children", "retention_planning",
                  "fact_retention.RETENTION_PHYSICAL_CHILDREN_QUERY",
                  RETENTION_PHYSICAL_CHILDREN_QUERY,
                  {"limit": limits.max_inventory_partitions + 1},
                  limits.max_inventory_partitions + 1,
                  "maximum_supported", "plan"),
        QueryCase("retention.fact_families", "retention_planning",
                  "fact_retention.FACT_STORAGE_FAMILIES_SQL", text(FACT_STORAGE_FAMILIES_SQL),
                  {"day": day}, 257, "representative", "analyze"),
        QueryCase("retention.page_counts", "retention_planning",
                  "fact_retention.RETENTION_PAGE_COUNTS_QUERY", RETENTION_PAGE_COUNTS_QUERY,
                  {"day": day, "version": FACT_ARCHIVE_VERIFIER_VERSION}, 1,
                  "representative", "analyze"),
        QueryCase("archive.page_cursor", "archive_preparation",
                  "fact_archival._ARCHIVE_LAST_MANIFEST_QUERY", _ARCHIVE_LAST_MANIFEST_QUERY,
                  {"day": day}, 1, "representative", "analyze"),
        QueryCase("archive.source_page", "archive_preparation",
                  "fact_archival._ARCHIVE_SOURCE_PAGE_QUERY", _ARCHIVE_SOURCE_PAGE_QUERY,
                  {"day": day, "seq": 0, "id": "", "limit": limits.max_page_rows,
                   "max_bytes": limits.max_page_logical_bytes}, limits.max_page_rows,
                  "maximum_supported", "plan"),
        QueryCase("verification.next_page", "verification",
                  "fact_archival._ARCHIVE_UNVERIFIED_PAGE_QUERY", _ARCHIVE_UNVERIFIED_PAGE_QUERY,
                  {"day": day, "version": FACT_ARCHIVE_VERIFIER_VERSION}, 1,
                  "representative", "analyze"),
        QueryCase("verification.source_envelopes", "verification",
                  "fact_archival._ARCHIVE_SOURCE_ENVELOPES_QUERY", _ARCHIVE_SOURCE_ENVELOPES_QUERY,
                  {"day": day, "first_seq": page["first_commit_seq"], "first_id": page["first_id"],
                   "last_seq": page["last_commit_seq"], "last_id": page["last_id"],
                   "limit": limits.max_page_rows + 1}, limits.max_page_rows + 1,
                  "maximum_supported", "plan"),
        QueryCase("reclamation.eligibility_families", "reclamation_admission",
                  "fact_reclamation._RECLAIM_ELIGIBILITY_FAMILIES_QUERY",
                  _RECLAIM_ELIGIBILITY_FAMILIES_QUERY, {"day": day}, 65,
                  "representative", "analyze"),
        QueryCase("reclamation.relation_identity", "reclamation_admission",
                  "fact_reclamation._RECLAIM_RELATION_QUERY", _RECLAIM_RELATION_QUERY,
                  {"name": relation}, 1, "representative", "analyze"),
        QueryCase("reclamation.relation_size", "reclamation_admission",
                  "postgresql.pg_total_relation_size",
                  text("SELECT pg_total_relation_size(to_regclass(:relation)) AS bytes"),
                  {"relation": f"market.{relation}"}, 1, "representative", "analyze"),
        QueryCase("reclamation.pinned_ranges", "reclamation_admission",
                  "fact_reclamation._PINNED_RANGES_QUERY", _PINNED_RANGES_QUERY,
                  {"day": day}, 1, "representative", "analyze", sample_waits=True),
    ]
    limits_by_name = {
        "series": limits.max_page_rows,
        "aliases": limits.max_page_rows,
        "dependencies": limits.max_dependency_rows,
        "canonical_dependencies": limits.max_raw_mapping_rows,
    }
    generation = scope.get("generation")
    for name, legacy_table, columns, order in ARCHIVE_PAGE_CATALOG_SPECS:
        table = PREPARED_CATALOG_TABLES.get(name, legacy_table) if generation else legacy_table
        identity = generation["preparation_id"] if table.startswith(
            "fact_archive_prepared_"
        ) else page["id"]
        cases.append(QueryCase(
            f"verification.catalog.{name}", "verification",
            "fact_archival.archive_page_catalog_statement",
            archive_page_catalog_statement(table=table, columns=columns, order=order),
            {"id": identity, "limit": limits_by_name[name] + 1},
            limits_by_name[name] + 1, "maximum_supported", "plan",
        ))
    dependency = scope["dependency"]
    if dependency is not None:
        cases.append(QueryCase(
            "verification.dependency_expiry", "verification",
            "fact_archival._ARCHIVE_EXPIRED_DEPENDENCY_QUERY",
            _ARCHIVE_EXPIRED_DEPENDENCY_QUERY,
            {"kinds": [dependency["target_kind"]], "ids": [dependency["target_id"]]},
            1, "representative", "analyze",
        ))
    raw = scope["raw"]
    if raw is not None:
        bound = [raw["manifest_id"]]
        position = {
            "definition_id": raw["definition_id"], "session_id": raw["session_id"],
            "connection_epoch": raw["connection_epoch"],
            "receive_ordinal": raw["receive_ordinal"],
        }
        position_predicate = (
            "manifests.definition_id = ANY(:position_definitions) "
            "AND manifests.session_id = ANY(:position_sessions) "
            "AND manifests.first_receive_ordinal<=:last_position "
            "AND manifests.last_receive_ordinal>=:first_position"
        )
        maximum_record_ids = [raw["raw_record_id"]] + [
            f"diagnostic-record-{index}"
            for index in range(1, limits.max_raw_mapping_rows)
        ]
        maximum_positions = [position | {
            "receive_ordinal": int(position["receive_ordinal"]) + index,
        } for index in range(limits.max_raw_mapping_rows)]
        cases.extend([
            QueryCase(
                "lineage.record_id", "archive_preparation",
                "fact_lineage.raw_archive_mapping_lookup_statement(record)",
                raw_archive_mapping_lookup_statement(mode="record", bound=True),
                {"ids": [raw["raw_record_id"]], "bound_ids": bound, "limit": 2},
                1, "representative", "analyze", sample_waits=True,
            ),
            QueryCase(
                "lineage.exact_position", "archive_preparation",
                "fact_lineage.raw_archive_mapping_lookup_statement(position)",
                raw_archive_mapping_lookup_statement(
                    mode="position", bound=True,
                    predicate=position_predicate,
                ),
                {
                    "positions": json.dumps([position]),
                    "position_definitions": [raw["definition_id"]],
                    "position_sessions": [raw["session_id"]],
                    "first_position": raw["receive_ordinal"],
                    "last_position": raw["receive_ordinal"],
                    "bound_ids": bound, "limit": 2,
                },
                1, "representative", "analyze", sample_waits=True,
            ),
            QueryCase(
                "lineage.record_id.maximum", "archive_preparation",
                "fact_lineage.raw_archive_mapping_lookup_statement(record)",
                raw_archive_mapping_lookup_statement(mode="record", bound=True),
                {"ids": maximum_record_ids, "bound_ids": bound,
                 "limit": limits.max_raw_mapping_rows + 1},
                limits.max_raw_mapping_rows, "maximum_supported", "plan",
            ),
            QueryCase(
                "lineage.exact_position.maximum", "archive_preparation",
                "fact_lineage.raw_archive_mapping_lookup_statement(position)",
                raw_archive_mapping_lookup_statement(
                    mode="position", bound=True, predicate=position_predicate,
                ),
                {
                    "positions": json.dumps(maximum_positions),
                    "position_definitions": [raw["definition_id"]],
                    "position_sessions": [raw["session_id"]],
                    "first_position": raw["receive_ordinal"],
                    "last_position": int(raw["receive_ordinal"]) + limits.max_raw_mapping_rows - 1,
                    "bound_ids": bound, "limit": limits.max_raw_mapping_rows + 1,
                },
                limits.max_raw_mapping_rows, "maximum_supported", "plan",
            ),
        ])
    return cases


def _execute_case(database, case: QueryCase, *, limits: QueryDiagnosticLimits,
                  deadline: float) -> dict[str, Any]:
    base = {
        "case_id": case.case_id, "boundary": case.boundary,
        "builder": case.builder, "sql_fingerprint": _sql_fingerprint(case.statement),
        "requested_cardinality": case.requested_cardinality,
        "designation": case.designation, "execution": case.execution,
    }
    try:
        remaining_ms = _remaining_milliseconds(
            deadline, reserve_ms=_CLEANUP_RESERVE_MS,
        )
    except TimeoutError:
        return {**base, "outcome": "untested", "reason": "total_budget_exhausted"}
    started = perf_counter()
    sampler = None
    try:
        with database.session() as session:
            remaining_ms = _remaining_milliseconds(
                deadline, reserve_ms=_CLEANUP_RESERVE_MS,
            )
            _readonly(session, statement_timeout_ms=min(limits.statement_timeout_ms, remaining_ms),
                      lock_timeout_ms=limits.lock_timeout_ms)
            pid = int(session.execute(text("SELECT pg_backend_pid()")).scalar_one())
            statement_ms = min(
                limits.statement_timeout_ms,
                _remaining_milliseconds(deadline, reserve_ms=_CLEANUP_RESERVE_MS),
            )
            session.execute(
                text("SELECT set_config('statement_timeout', :value, true)"),
                {"value": str(statement_ms)},
            )
            prefix = "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON) " if case.execution == "analyze" else "EXPLAIN (SETTINGS, FORMAT JSON) "
            sampler = _WaitSampler(
                database, pid, maximum=limits.max_wait_samples, deadline=deadline,
            )
            context = sampler if case.sample_waits else _NullContext()
            with context:
                document = session.execute(
                    text(prefix + str(case.statement)), dict(case.params)
                ).scalar_one()[0]
            plan = document["Plan"]
            nodes, buffers, wal = _plan_summary(plan)
            if monotonic() >= deadline:
                return {
                    **base, "outcome": "refused", "reason": "total_budget_exhausted",
                    "elapsed_seconds": round(perf_counter() - started, 6),
                    "wait_samples": sampler.summary() if case.sample_waits else None,
                }
            return {
                **base, "outcome": "passed", "elapsed_seconds": round(perf_counter() - started, 6),
                "planning_time_ms": document.get("Planning Time"),
                "execution_time_ms": document.get("Execution Time"),
                "observed_rows": plan.get("Actual Rows") if case.execution == "analyze" else None,
                "plan_nodes": nodes, "buffers": buffers,
                "wal": wal if case.execution == "analyze" else {},
                "settings": document.get("Settings", {}),
                "wait_samples": sampler.summary() if case.sample_waits else {
                    "sample_count": 0, "categories": {}, "max_active_age_seconds": None,
                    "unavailable_reason": "not_sampled_for_plan_or_short_metadata_case",
                },
            }
    except DBAPIError as exc:
        code = _db_code(exc)
        outcome = "timed_out" if code == "57014" else "lock_refused" if code == "55P03" else "refused"
        return {
            **base, "outcome": outcome, "reason": f"postgresql_sqlstate_{code or 'unknown'}",
            "elapsed_seconds": round(perf_counter() - started, 6),
            "wait_samples": sampler.summary() if sampler is not None and case.sample_waits else None,
        }
    except Exception as exc:
        return {
            **base, "outcome": "refused", "reason": type(exc).__name__,
            "elapsed_seconds": round(perf_counter() - started, 6),
            "wait_samples": sampler.summary() if sampler is not None and case.sample_waits else None,
        }


class _NullContext:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _write_path_coverage() -> list[dict[str, Any]]:
    return [
        {
            "case_id": "retention.partition_lock_and_hot_window_recheck",
            "boundary": "retention_execution",
            "builder": "fact_archival._lock and fact_retention.require_hot_window_elapsed",
            "sql_fingerprint": None, "requested_cardinality": 1,
            "designation": "locking_context", "execution": "not_executed_live",
            "outcome": "unsupported",
            "reason": "read_only_diagnostic_does_not_take_production_row_or_advisory_ownership",
            "durable_ci_coverage": [
                "tests/test_market_data/test_fact_archival_db.py",
                "tests/test_market_data/test_canonical_retention_executor.py",
            ],
        },
        {
            "case_id": "archive.causal_source_closure",
            "boundary": "archive_preparation",
            "builder": "derived, book, trade-flow, response and normalized admission resolvers",
            "sql_fingerprint": None, "requested_cardinality": None,
            "designation": "typed_payload_bound", "execution": "not_executed_live",
            "outcome": "untested",
            "reason": "requires_typed_payload_bound_fixture_and_can_open_archive_objects",
            "durable_ci_coverage": [
                "tests/test_market_data/test_fact_archival_db.py",
                "tests/test_market_data/test_fact_source_placements_db.py",
                "tests/test_market_data/test_fact_raw_lineage_db.py",
            ],
        },
        {
            "case_id": "lineage.book_prefix_range",
            "boundary": "archive_preparation",
            "builder": "fact_lineage.raw_archive_mapping_lookup_statement(prefix)",
            "sql_fingerprint": None, "requested_cardinality": None,
            "designation": "dynamic_bounded_values", "execution": "not_executed_live",
            "outcome": "untested", "reason": "no_prefix_request_is_invented_from_catalog_rows",
            "durable_ci_coverage": ["tests/test_market_data/test_fact_raw_lineage_db.py"],
        },
        {
            "case_id": "archive.source_exhaustion_and_coverage",
            "boundary": "archive_preparation",
            "builder": "fact_archival._stage_next_page exhaustion proof",
            "sql_fingerprint": None, "requested_cardinality": None,
            "designation": "cursor_dependent", "execution": "not_executed_live",
            "outcome": "untested", "reason": "diagnostic_does_not_invent_a_terminal_archive_cursor",
            "durable_ci_coverage": ["tests/test_market_data/test_fact_archival_db.py"],
        },
        {
            "case_id": "publication.manifest_series_dependency_writes",
            "boundary": "catalog_publication", "builder": "SQLAlchemy ORM insert builders",
            "sql_fingerprint": None, "requested_cardinality": None,
            "designation": "disposable_only", "execution": "not_executed_live",
            "outcome": "untested", "reason": "production_write_forbidden",
            "durable_ci_coverage": list(_WRITE_TESTS),
        },
        {
            "case_id": "publication.alias_write", "boundary": "catalog_publication",
            "builder": "fact_archival._ALIAS_CATALOG_INSERT",
            "sql_fingerprint": _sql_fingerprint(_ALIAS_CATALOG_INSERT),
            "requested_cardinality": 128, "designation": "maximum_supported",
            "execution": "not_executed_live", "outcome": "untested",
            "reason": "production_write_forbidden", "durable_ci_coverage": list(_WRITE_TESTS),
        },
        {
            "case_id": "publication.canonical_dependency_write",
            "boundary": "catalog_publication",
            "builder": "fact_archival._CANONICAL_DEPENDENCY_CATALOG_INSERT",
            "sql_fingerprint": _sql_fingerprint(_CANONICAL_DEPENDENCY_CATALOG_INSERT),
            "requested_cardinality": 128, "designation": "maximum_supported",
            "execution": "not_executed_live", "outcome": "untested",
            "reason": "production_write_forbidden", "durable_ci_coverage": list(_WRITE_TESTS),
        },
        {
            "case_id": "verification.receipt_manifest_set_and_current_bytes",
            "boundary": "verification",
            "builder": "fact_archival._partition_evidence and ArchiveVerificationBatch",
            "sql_fingerprint": None, "requested_cardinality": None,
            "designation": "full_partition_and_filesystem", "execution": "not_executed_live",
            "outcome": "untested",
            "reason": "bounded_sql_diagnostic_does_not_reread_all_archive_objects",
            "durable_ci_coverage": [
                "tests/test_market_data/test_fact_archival_db.py",
                "tests/test_market_data/test_fact_reclamation_db.py",
            ],
        },
        {
            "case_id": "reclamation.pinned_ranges_exclusive_recheck",
            "boundary": "reclamation_execution",
            "builder": "fact_reclamation._PINNED_RANGES_QUERY",
            "sql_fingerprint": _sql_fingerprint(_PINNED_RANGES_QUERY),
            "requested_cardinality": 1, "designation": "exclusive_lock_context",
            "execution": "not_executed_live", "outcome": "unsupported",
            "reason": "same_builder_is_tested_read_only_but_exact_exclusive_lock_context_is_not_acquired",
            "durable_ci_coverage": ["tests/test_market_data/test_fact_reclamation_db.py"],
        },
        {
            "case_id": "reclamation.exclusive_handoff_and_drop",
            "boundary": "reclamation_execution", "builder": "FactHandoffProof and DROP TABLE",
            "sql_fingerprint": None, "requested_cardinality": None,
            "designation": "destructive_context", "execution": "not_executed_live",
            "outcome": "unsupported",
            "reason": "safe_read_only_analog_cannot_validate_exact_exclusive_lock_or_drop_context",
            "durable_ci_coverage": [
                "tests/test_market_data/test_fact_reclamation_db.py",
                "tests/test_market_data/test_fact_handoff_db.py",
            ],
        },
        {
            "case_id": "archive.object_decode_compress_and_byte_compare",
            "boundary": "archive_filesystem", "builder": "non_sql_archive_path",
            "sql_fingerprint": None, "requested_cardinality": None,
            "designation": "non_sql", "execution": "not_executed_live",
            "outcome": "unsupported", "reason": "outside_sql_diagnostic_contract",
            "durable_ci_coverage": ["tests/test_market_data/test_canonical_fact_archive.py"],
        },
    ]


def run_live_query_diagnostic(*, database, storage_day: date,
                              limits: QueryDiagnosticLimits = QueryDiagnosticLimits(),
                              source_revision: str | None = None,
                              source_tree_hash: str | None = None,
                              cancel_requested: Callable[[], bool] | None = None) -> dict[str, Any]:
    if type(storage_day) is not date:
        raise ValueError("canonical_query_diagnostic_storage_day_invalid")
    cancel_requested = cancel_requested or (lambda: False)
    started_at = datetime.now(UTC)
    started_clock = monotonic()
    deadline = started_clock + limits.total_seconds
    with _strict_read_only_database(database, limits, deadline=deadline) as readonly_database:
        scope = _select_scope(readonly_database, storage_day, limits, deadline=deadline)
        revision = source_revision or evidence_source_revision()
        tree_hash = source_tree_hash or str(os.getenv("QT_IMAGE_SOURCE_TREE_HASH") or "").strip()
        if not re.fullmatch(r"[0-9a-f]{40}", revision) or not re.fullmatch(r"[0-9a-f]{64}", tree_hash):
            raise RuntimeError("canonical_query_diagnostic_source_attestation_invalid")
        results: list[dict[str, Any]] = []
        for case in _cases(scope, limits):
            if cancel_requested():
                results.append({
                    "case_id": case.case_id, "boundary": case.boundary, "builder": case.builder,
                    "sql_fingerprint": _sql_fingerprint(case.statement),
                    "requested_cardinality": case.requested_cardinality,
                    "designation": case.designation, "execution": case.execution,
                    "outcome": "untested", "reason": "cancelled",
                })
                continue
            results.append(_execute_case(
                readonly_database, case, limits=limits, deadline=deadline,
            ))
    if scope["raw"] is None:
        for name in ("record_id", "exact_position"):
            results.append({
                "case_id": f"lineage.{name}", "boundary": "archive_preparation",
                "builder": "fact_lineage.raw_archive_mapping_lookup_statement",
                "sql_fingerprint": None, "requested_cardinality": 0,
                "designation": "representative", "execution": "not_executed_live",
                "outcome": "unsupported", "reason": "no_committed_unexpired_raw_mapping_available",
            })
    if scope["dependency"] is None:
        results.append({
            "case_id": "verification.dependency_expiry", "boundary": "verification",
            "builder": "fact_archival._ARCHIVE_EXPIRED_DEPENDENCY_QUERY",
            "sql_fingerprint": _sql_fingerprint(_ARCHIVE_EXPIRED_DEPENDENCY_QUERY),
            "requested_cardinality": 0, "designation": "representative",
            "execution": "not_executed_live", "outcome": "unsupported",
            "reason": "no_committed_archive_dependency_available",
        })
    results.extend(_write_path_coverage())
    if any(item["outcome"] not in OUTCOMES for item in results):
        raise RuntimeError("canonical_query_diagnostic_outcome_invalid")
    required = [item for item in results if item["execution"] in {"plan", "analyze"}]
    elapsed_clock = monotonic() - started_clock
    budget_outcome = "within_limit" if elapsed_clock <= limits.total_seconds else "overrun"
    status = (
        "passed"
        if budget_outcome == "within_limit"
        and all(item["outcome"] == "passed" for item in required)
        else "findings"
    )
    finished_at = datetime.now(UTC)
    return {
        "schema_version": SCHEMA_VERSION, "status": status,
        "mode": "live_read_only", "source_revision": revision,
        "source_tree_hash": tree_hash, "postgresql_version": scope["postgresql_version"],
        "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat(),
        "elapsed_seconds": round(elapsed_clock, 6),
        "budget_outcome": budget_outcome,
        "storage_day": storage_day.isoformat(),
        "representative_scope": {
            "partition_state": scope["partition"]["state"],
            "expected_rows": int(scope["partition"]["expected_rows"]),
            "manifest_id": scope["page"]["id"],
            "page_ordinal": int(scope["page"]["page_ordinal"]),
            "page_rows": int(scope["page"]["row_count"]),
        },
        "limits": {
            "statement_timeout_ms": limits.statement_timeout_ms,
            "lock_timeout_ms": limits.lock_timeout_ms,
            "total_seconds": limits.total_seconds,
            "max_inventory_partitions": limits.max_inventory_partitions,
            "max_page_rows": limits.max_page_rows,
            "max_page_logical_bytes": limits.max_page_logical_bytes,
            "max_dependency_rows": limits.max_dependency_rows,
            "max_raw_mapping_rows": limits.max_raw_mapping_rows,
        },
        "read_only": True, "maintenance_authority": False,
        "coverage": {
            "case_count": len(results),
            "outcomes": dict(sorted(Counter(item["outcome"] for item in results).items())),
            "complete": all(item["outcome"] != "untested" or item["execution"] == "not_executed_live"
                            for item in results),
            "claims_every_query": False,
        },
        "cases": results,
        "caveats": [
            "The closed inventory covers named canonical-retention SQL builders, not every SQL statement or contention state.",
            "Maximum live shapes are plan-only unless the case is explicitly designated representative and read-only.",
            "Disposable CI covers write semantics; live rollback is not permission to benchmark writes.",
            "Read-only plans do not validate archive decoding, byte comparison, exclusive handoff, or DROP execution.",
            "A warm-cache result is not a worst-case guarantee.",
        ],
    }


def write_receipt(path: Path, receipt: Mapping[str, Any]) -> None:
    path = Path(path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True, default=str) + "\n",
                         encoding="utf-8")
    os.chmod(temporary, 0o600)
    temporary.replace(path)
