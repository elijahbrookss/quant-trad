"""PostgreSQL authority for market-data storage lifecycle operations."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Optional

from sqlalchemy import text

from core.market_storage_lifecycle import (
    DEFAULT_HOT_TABLE_POLICIES,
    MARKET_STORAGE_LIFECYCLE_POLICY_VERSION,
)

from ._shared import db


_LIFECYCLE_LOCK_NAME = "quant-trad:market-storage-lifecycle:v1"
_ALLOWED_TABLES = {
    policy.table_name: policy.time_column for policy in DEFAULT_HOT_TABLE_POLICIES
}
for _policy in DEFAULT_HOT_TABLE_POLICIES:
    _ALLOWED_TABLES.update(dict(_policy.dependent_tables))


class MarketStorageLifecycleBusyError(RuntimeError):
    """Raised when another process owns the global lifecycle fence."""


def lifecycle_operation_id(
    *, action: str, target_kind: str, target_id: str, policy_version: str
) -> str:
    payload = json.dumps(
        {
            "action": str(action),
            "policy_version": str(policy_version),
            "target_id": str(target_id),
            "target_kind": str(target_kind),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "mslo_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _json(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _relation(table_name: str) -> tuple[str, str]:
    table = str(table_name or "").strip()
    try:
        time_column = _ALLOWED_TABLES[table]
    except KeyError as exc:
        raise ValueError(
            f"market_storage_lifecycle_table_invalid: table={table}"
        ) from exc
    return f"market.{table}", time_column


class PostgresMarketStorageLifecycleRepository:
    """Fenced planning and evidence for object and Timescale lifecycle work."""

    @staticmethod
    def dataset_snapshot_session(*, database=db):
        return database.locked_snapshot_session(shared_lock_name=_LIFECYCLE_LOCK_NAME)

    @contextmanager
    def lifecycle_lock(self, *, owner_id: str) -> Iterator[None]:
        owner = str(owner_id or "").strip()
        if not owner:
            raise ValueError("market_storage_lifecycle_owner_required")
        with db.session() as session:
            acquired = bool(
                session.execute(
                    text("SELECT pg_try_advisory_lock(hashtextextended(:name, 0))"),
                    {"name": _LIFECYCLE_LOCK_NAME},
                ).scalar_one()
            )
            if not acquired:
                raise MarketStorageLifecycleBusyError(
                    f"market_storage_lifecycle_busy: owner_id={owner}"
                )
            try:
                yield
            finally:
                released = bool(
                    session.execute(
                        text("SELECT pg_advisory_unlock(hashtextextended(:name, 0))"),
                        {"name": _LIFECYCLE_LOCK_NAME},
                    ).scalar_one()
                )
                if not released:
                    raise RuntimeError(
                        f"market_storage_lifecycle_unlock_failed: owner_id={owner}"
                    )

    @staticmethod
    def canonical_dependency_count(session, *, target_kind: str, target_id: str) -> int:
        """Cold Fact evidence holds survive release of user and dataset pins."""
        return int(session.execute(text(
            "SELECT count(*) FROM market.fact_archive_dependencies "
            "WHERE target_kind=:target_kind AND target_id=:target_id"
        ), {"target_kind": str(target_kind), "target_id": str(target_id)}).scalar_one())

    @staticmethod
    def acquire_dataset_pin_lock(session) -> None:
        """Block lifecycle expiry while a frozen or explicit pin is committed."""

        session.execute(
            text(
                "SELECT pg_advisory_xact_lock_shared(hashtextextended(:name, 0))"
            ),
            {"name": _LIFECYCLE_LOCK_NAME},
        )

    def append_event(
        self,
        *,
        operation_id: str,
        action: str,
        event_type: str,
        target_kind: str,
        target_id: str,
        evidence: Mapping[str, Any],
        cutoff_at: Optional[datetime] = None,
        reason: Optional[str] = None,
        occurred_at: Optional[datetime] = None,
        policy_version: str = MARKET_STORAGE_LIFECYCLE_POLICY_VERSION,
    ) -> dict[str, Any]:
        with db.session() as session:
            return self._append_event_with_session(
                session,
                operation_id=operation_id,
                action=action,
                event_type=event_type,
                target_kind=target_kind,
                target_id=target_id,
                evidence=evidence,
                cutoff_at=cutoff_at,
                reason=reason,
                occurred_at=occurred_at,
                policy_version=policy_version,
            )

    def _append_event_with_session(
        self,
        session,
        *,
        operation_id: str,
        action: str,
        event_type: str,
        target_kind: str,
        target_id: str,
        evidence: Mapping[str, Any],
        cutoff_at: Optional[datetime] = None,
        reason: Optional[str] = None,
        occurred_at: Optional[datetime] = None,
        policy_version: str = MARKET_STORAGE_LIFECYCLE_POLICY_VERSION,
    ) -> dict[str, Any]:
        operation = str(operation_id or "").strip()
        normalized_evidence = dict(evidence or {})
        evidence_hash = _hash(normalized_evidence)
        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:operation_id, 0))"),
            {"operation_id": operation},
        )
        identical = session.execute(
            text(
                """
                SELECT * FROM market.storage_lifecycle_events
                WHERE operation_id = :operation_id
                  AND event_type = :event_type
                  AND evidence_hash = :evidence_hash
                ORDER BY event_ordinal DESC
                LIMIT 1
                """
            ),
            {
                "operation_id": operation,
                "event_type": str(event_type),
                "evidence_hash": evidence_hash,
            },
        ).mappings().first()
        if identical is not None:
            return dict(identical)
        event_ordinal = int(
            session.execute(
                text(
                    "SELECT COALESCE(max(event_ordinal), -1) + 1 "
                    "FROM market.storage_lifecycle_events "
                    "WHERE operation_id = :operation_id"
                ),
                {"operation_id": operation},
            ).scalar_one()
        )
        now = occurred_at or datetime.now(UTC)
        event_id = "msle_" + _hash(
            {
                "operation_id": operation,
                "event_ordinal": event_ordinal,
                "event_type": str(event_type),
                "evidence_hash": evidence_hash,
            }
        )
        row = session.execute(
            text(
                """
                INSERT INTO market.storage_lifecycle_events (
                    id, operation_id, event_ordinal, policy_version, action,
                    event_type, target_kind, target_id, cutoff_at, occurred_at,
                    known_at, reason, evidence_hash, evidence
                ) VALUES (
                    :id, :operation_id, :event_ordinal, :policy_version, :action,
                    :event_type, :target_kind, :target_id, :cutoff_at, :occurred_at,
                    :known_at, :reason, :evidence_hash, CAST(:evidence AS jsonb)
                )
                RETURNING *
                """
            ),
            {
                "id": event_id,
                "operation_id": operation,
                "event_ordinal": event_ordinal,
                "policy_version": str(policy_version),
                "action": str(action),
                "event_type": str(event_type),
                "target_kind": str(target_kind),
                "target_id": str(target_id),
                "cutoff_at": cutoff_at,
                "occurred_at": now,
                "known_at": datetime.now(UTC),
                "reason": str(reason) if reason else None,
                "evidence_hash": evidence_hash,
                "evidence": _json(normalized_evidence),
            },
        ).mappings().one()
        return dict(row)

    def operation_completed(self, *, operation_id: str) -> bool:
        with db.session() as session:
            return bool(
                session.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM market.storage_lifecycle_events "
                        "WHERE operation_id = :operation_id AND event_type = 'completed')"
                    ),
                    {"operation_id": str(operation_id)},
                ).scalar_one()
            )

    def operation_started(self, *, operation_id: str) -> bool:
        with db.session() as session:
            return bool(
                session.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM market.storage_lifecycle_events "
                        "WHERE operation_id = :operation_id AND event_type = 'planned')"
                    ),
                    {"operation_id": str(operation_id)},
                ).scalar_one()
            )

    def list_recent_events(self, *, limit: int = 200) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), 1000))
        with db.session() as session:
            rows = session.execute(
                text(
                    "SELECT * FROM market.storage_lifecycle_events "
                    "ORDER BY occurred_at DESC, operation_id, event_ordinal DESC "
                    "LIMIT :limit"
                ),
                {"limit": bounded},
            ).mappings().all()
        return [dict(row) for row in rows]

    def list_compaction_manifests(
        self, *, older_than: datetime, limit: int = 5000
    ) -> list[dict[str, Any]]:
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT manifests.*, definitions.provider_product_id,
                           definitions.channels
                    FROM market.raw_archive_manifests AS manifests
                    JOIN market.stream_definitions AS definitions
                      ON definitions.id = manifests.definition_id
                    WHERE manifests.last_received_at < :older_than
                      AND NOT EXISTS (
                          SELECT 1
                          FROM market.raw_archive_compaction_sources AS sources
                          WHERE sources.source_manifest_id = manifests.id
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM market.storage_lifecycle_events AS lifecycle
                          WHERE lifecycle.action = 'archive_expire'
                            AND lifecycle.target_kind = 'raw_manifest'
                            AND lifecycle.target_id = manifests.id
                            AND lifecycle.event_type = 'completed'
                      )
                    ORDER BY manifests.definition_id, manifests.session_id,
                             manifests.connection_epoch,
                             manifests.first_receive_ordinal, manifests.id
                    LIMIT :limit
                    """
                ),
                {"older_than": older_than, "limit": max(2, min(int(limit), 20000))},
            ).mappings().all()
        return [dict(row) for row in rows]

    def list_archive_expiration_candidates(
        self,
        *,
        raw_trade_cutoff: datetime,
        raw_l2_cutoff: datetime,
        checkpoint_cutoff: datetime,
        compacted_source_cutoff: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), 5000))
        with db.session() as session:
            raw_rows = session.execute(
                text(
                    """
                    WITH latest_pin AS (
                        SELECT DISTINCT ON (pin_id)
                               pin_id, target_kind, target_id, status
                        FROM market.archive_retention_pin_versions
                        ORDER BY pin_id, revision DESC
                    ), explicit_pins AS (
                        SELECT target_kind, target_id, count(*) AS pin_count
                        FROM latest_pin WHERE status = 'active'
                        GROUP BY target_kind, target_id
                    ), replacements AS (
                        SELECT source_manifest_id, replacement_manifest_id,
                               compacted_at
                        FROM market.raw_archive_compaction_sources
                    )
                    SELECT manifests.id AS target_id,
                           'raw_manifest' AS target_kind,
                           manifests.object_key, manifests.object_uri,
                           manifests.object_sha256, manifests.byte_count,
                           manifests.last_received_at AS effective_at,
                           definitions.channels,
                           replacements.replacement_manifest_id,
                           replacements.compacted_at,
                           COALESCE(explicit_pins.pin_count, 0) AS explicit_pin_count,
                           (SELECT count(*) FROM market.dataset_archive_refs AS refs
                            WHERE refs.raw_archive_manifest_id = manifests.id) AS dataset_pin_count,
                           (SELECT count(*) FROM market.fact_archive_dependencies AS dependencies
                            WHERE dependencies.target_kind='raw_manifest'
                              AND dependencies.target_id=manifests.id) AS canonical_dependency_count
                    FROM market.raw_archive_manifests AS manifests
                    JOIN market.stream_definitions AS definitions
                      ON definitions.id = manifests.definition_id
                    LEFT JOIN replacements
                      ON replacements.source_manifest_id = manifests.id
                    LEFT JOIN explicit_pins
                      ON explicit_pins.target_kind = 'raw_manifest'
                     AND explicit_pins.target_id = manifests.id
                    WHERE (
                        (definitions.channels ? 'level2'
                         AND manifests.last_received_at < :raw_l2_cutoff)
                        OR (NOT (definitions.channels ? 'level2')
                            AND manifests.last_received_at < :raw_trade_cutoff)
                        OR replacements.compacted_at < :compacted_source_cutoff
                    )
                      AND NOT EXISTS (
                          SELECT 1 FROM market.storage_lifecycle_events AS lifecycle
                          WHERE lifecycle.action = 'archive_expire'
                            AND lifecycle.target_kind = 'raw_manifest'
                            AND lifecycle.target_id = manifests.id
                            AND lifecycle.event_type = 'completed'
                      )
                    ORDER BY manifests.last_received_at, manifests.id
                    LIMIT :limit
                    """
                ),
                {
                    "raw_trade_cutoff": raw_trade_cutoff,
                    "raw_l2_cutoff": raw_l2_cutoff,
                    "compacted_source_cutoff": compacted_source_cutoff,
                    "limit": bounded,
                },
            ).mappings().all()
            remaining = max(0, bounded - len(raw_rows))
            checkpoint_rows = []
            if remaining:
                checkpoint_rows = session.execute(
                    text(
                        """
                        WITH latest_pin AS (
                            SELECT DISTINCT ON (pin_id)
                                   pin_id, target_kind, target_id, status
                            FROM market.archive_retention_pin_versions
                            ORDER BY pin_id, revision DESC
                        ), explicit_pins AS (
                            SELECT target_kind, target_id, count(*) AS pin_count
                            FROM latest_pin WHERE status = 'active'
                            GROUP BY target_kind, target_id
                        )
                        SELECT checkpoints.id AS target_id,
                               'book_checkpoint' AS target_kind,
                               checkpoints.object_key, checkpoints.object_uri,
                               checkpoints.object_sha256, checkpoints.byte_count,
                               checkpoints.effective_at,
                               NULL::jsonb AS channels,
                               NULL::text AS replacement_manifest_id,
                               NULL::timestamptz AS compacted_at,
                               COALESCE(explicit_pins.pin_count, 0) AS explicit_pin_count,
                               0::bigint AS dataset_pin_count,
                               (SELECT count(*) FROM market.fact_archive_dependencies AS dependencies
                                WHERE dependencies.target_kind='book_checkpoint'
                                  AND dependencies.target_id=checkpoints.id) AS canonical_dependency_count
                        FROM market.book_checkpoint_manifests AS checkpoints
                        LEFT JOIN explicit_pins
                          ON explicit_pins.target_kind = 'book_checkpoint'
                         AND explicit_pins.target_id = checkpoints.id
                        WHERE checkpoints.effective_at < :checkpoint_cutoff
                          AND NOT EXISTS (
                              SELECT 1 FROM market.storage_lifecycle_events AS lifecycle
                              WHERE lifecycle.action = 'archive_expire'
                                AND lifecycle.target_kind = 'book_checkpoint'
                                AND lifecycle.target_id = checkpoints.id
                                AND lifecycle.event_type = 'completed'
                          )
                        ORDER BY checkpoints.effective_at, checkpoints.id
                        LIMIT :limit
                        """
                    ),
                    {"checkpoint_cutoff": checkpoint_cutoff, "limit": remaining},
                ).mappings().all()
        return [dict(row) for row in [*raw_rows, *checkpoint_rows]]

    def archive_target_status(
        self, *, target_kind: str, target_id: str
    ) -> dict[str, Any]:
        kind = str(target_kind)
        table = {
            "raw_manifest": "market.raw_archive_manifests",
            "book_checkpoint": "market.book_checkpoint_manifests",
        }.get(kind)
        if table is None:
            raise ValueError(
                f"market_storage_lifecycle_target_invalid: kind={kind}"
            )
        with db.session() as session:
            target = session.execute(
                text(
                    f"SELECT id, object_key, object_uri, object_sha256, byte_count "
                    f"FROM {table} WHERE id = :target_id"
                ),
                {"target_id": str(target_id)},
            ).mappings().first()
            if target is None:
                raise ValueError(
                    f"market_storage_lifecycle_target_missing: kind={kind} id={target_id}"
                )
            explicit_pin_count = int(
                session.execute(
                    text(
                        """
                        SELECT count(*) FROM (
                            SELECT DISTINCT ON (pin_id) pin_id, status
                            FROM market.archive_retention_pin_versions
                            WHERE target_kind = :target_kind
                              AND target_id = :target_id
                            ORDER BY pin_id, revision DESC
                        ) AS latest WHERE status = 'active'
                        """
                    ),
                    {"target_kind": kind, "target_id": str(target_id)},
                ).scalar_one()
            )
            dataset_pin_count = 0
            if kind == "raw_manifest":
                dataset_pin_count = int(
                    session.execute(
                        text(
                            "SELECT count(*) FROM market.dataset_archive_refs "
                            "WHERE raw_archive_manifest_id = :target_id"
                        ),
                        {"target_id": str(target_id)},
                    ).scalar_one()
                )
            expired = bool(
                session.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM market.storage_lifecycle_events "
                        "WHERE action = 'archive_expire' "
                        "AND target_kind = :target_kind AND target_id = :target_id "
                        "AND event_type = 'completed')"
                    ),
                    {"target_kind": kind, "target_id": str(target_id)},
                ).scalar_one()
            )
            canonical_dependency_count = self.canonical_dependency_count(
                session, target_kind=kind, target_id=target_id,
            )
        return {
            **dict(target),
            "target_kind": kind,
            "explicit_pin_count": explicit_pin_count,
            "dataset_pin_count": dataset_pin_count,
            "canonical_dependency_count": canonical_dependency_count,
            "pinned": bool(explicit_pin_count or dataset_pin_count or canonical_dependency_count),
            "expired": expired,
        }

    def list_hot_chunks(self, *, table_name: str) -> list[dict[str, Any]]:
        relation, _ = _relation(table_name)
        schema, table = relation.split(".", 1)
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT chunks.chunk_schema, chunks.chunk_name,
                           chunks.range_start, chunks.range_end,
                           chunks.is_compressed,
                           hypertables.compression_enabled,
                           pg_total_relation_size(
                               format('%I.%I', chunks.chunk_schema, chunks.chunk_name)::regclass
                           ) AS byte_count
                    FROM timescaledb_information.chunks AS chunks
                    JOIN timescaledb_information.hypertables AS hypertables
                      ON hypertables.hypertable_schema = chunks.hypertable_schema
                     AND hypertables.hypertable_name = chunks.hypertable_name
                    WHERE chunks.hypertable_schema = :schema
                      AND chunks.hypertable_name = :table
                    ORDER BY chunks.range_start, chunks.chunk_name
                    """
                ),
                {"schema": schema, "table": table},
            ).mappings().all()
        return [dict(row) for row in rows]

    def dependent_chunk_status(
        self,
        *,
        table_name: str,
        range_start: datetime,
        range_end: datetime,
    ) -> Optional[dict[str, Any]]:
        relation, _ = _relation(table_name)
        schema, table = relation.split(".", 1)
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    SELECT chunks.chunk_schema, chunks.chunk_name,
                           chunks.range_start, chunks.range_end,
                           chunks.is_compressed,
                           hypertables.compression_enabled
                    FROM timescaledb_information.chunks AS chunks
                    JOIN timescaledb_information.hypertables AS hypertables
                      ON hypertables.hypertable_schema = chunks.hypertable_schema
                     AND hypertables.hypertable_name = chunks.hypertable_name
                    WHERE chunks.hypertable_schema = :schema
                      AND chunks.hypertable_name = :table
                      AND chunks.range_start = :range_start
                      AND chunks.range_end = :range_end
                    """
                ),
                {
                    "schema": schema,
                    "table": table,
                    "range_start": range_start,
                    "range_end": range_end,
                },
            ).mappings().first()
        return dict(row) if row is not None else None

    def list_dataset_chunk_pins(
        self,
        *,
        table_name: str,
        range_start: datetime,
        range_end: datetime,
    ) -> list[dict[str, Any]]:
        relation, time_column = _relation(table_name)
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT dataset_series.dataset_id, dataset_series.series_id,
                           dataset_series.range_start, dataset_series.range_end
                    FROM market.dataset_series AS dataset_series
                    WHERE dataset_series.range_start < :range_end
                      AND dataset_series.range_end > :range_start
                      AND EXISTS (
                          SELECT 1 FROM {relation} AS facts
                          WHERE facts.series_id = dataset_series.series_id
                            AND facts.{time_column} >= :range_start
                            AND facts.{time_column} < :range_end
                      )
                    ORDER BY dataset_series.dataset_id, dataset_series.series_id
                    """
                ),
                {"range_start": range_start, "range_end": range_end},
            ).mappings().all()
        return [dict(row) for row in rows]

    def compress_chunk_group(
        self,
        *,
        table_names: Sequence[str],
        range_start: datetime,
        range_end: datetime,
        operation_id: str,
        target_id: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        compressed: list[str] = []
        with db.session() as session:
            for table_name in table_names:
                relation, _ = _relation(table_name)
                chunk = self._chunk_with_session(
                    session,
                    table_name=table_name,
                    range_start=range_start,
                    range_end=range_end,
                )
                if chunk is None:
                    raise RuntimeError(
                        "market_storage_lifecycle_layout_missing: "
                        f"table={relation} range={range_start.isoformat()}..{range_end.isoformat()}"
                    )
                if not bool(chunk["compression_enabled"]):
                    raise RuntimeError(
                        f"market_storage_lifecycle_compression_not_configured: table={relation}"
                    )
                chunk_ref = f"{chunk['chunk_schema']}.{chunk['chunk_name']}"
                session.execute(
                    text(
                        "SELECT compress_chunk(CAST(:chunk_ref AS regclass), "
                        "if_not_compressed => TRUE)"
                    ),
                    {"chunk_ref": chunk_ref},
                ).scalar_one_or_none()
                compressed.append(chunk_ref)
            completed_evidence = {**dict(evidence), "compressed_chunks": compressed}
            self._append_event_with_session(
                session,
                operation_id=operation_id,
                action="chunk_compress",
                event_type="completed",
                target_kind="hypertable_chunk",
                target_id=target_id,
                evidence=completed_evidence,
                cutoff_at=range_end,
            )
        return {"status": "completed", "compressed_chunks": compressed}

    def expire_chunk_group(
        self,
        *,
        table_names: Sequence[str],
        pin_table_name: str,
        range_start: datetime,
        range_end: datetime,
        operation_id: str,
        target_id: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        pin_relation, pin_time_column = _relation(pin_table_name)
        dropped: list[str] = []
        with db.session() as session:
            pins = session.execute(
                text(
                    f"""
                    SELECT dataset_series.dataset_id, dataset_series.series_id
                    FROM market.dataset_series AS dataset_series
                    WHERE dataset_series.range_start < :range_end
                      AND dataset_series.range_end > :range_start
                      AND EXISTS (
                          SELECT 1 FROM {pin_relation} AS facts
                          WHERE facts.series_id = dataset_series.series_id
                            AND facts.{pin_time_column} >= :range_start
                            AND facts.{pin_time_column} < :range_end
                      )
                    LIMIT 100
                    """
                ),
                {"range_start": range_start, "range_end": range_end},
            ).mappings().all()
            if pins:
                skipped_evidence = {
                    **dict(evidence),
                    "dataset_pins": [dict(row) for row in pins],
                }
                self._append_event_with_session(
                    session,
                    operation_id=operation_id,
                    action="chunk_expire",
                    event_type="skipped",
                    target_kind="hypertable_chunk",
                    target_id=target_id,
                    evidence=skipped_evidence,
                    cutoff_at=range_end,
                    reason="frozen dataset overlaps chunk",
                )
                return {"status": "skipped", "dataset_pins": len(pins)}
            for table_name in reversed(tuple(table_names)):
                relation, _ = _relation(table_name)
                chunk = self._chunk_with_session(
                    session,
                    table_name=table_name,
                    range_start=range_start,
                    range_end=range_end,
                )
                if chunk is None:
                    raise RuntimeError(
                        "market_storage_lifecycle_layout_missing: "
                        f"table={relation} range={range_start.isoformat()}..{range_end.isoformat()}"
                    )
                removed = [
                    str(value)
                    for value in session.execute(
                        text(
                            "SELECT drop_chunks(CAST(:relation AS regclass), "
                            "older_than => :range_end, newer_than => :range_start)"
                        ),
                        {
                            "relation": relation,
                            "range_start": range_start,
                            "range_end": range_end,
                        },
                    ).scalars()
                ]
                expected = f"{chunk['chunk_schema']}.{chunk['chunk_name']}"
                if expected not in removed:
                    raise RuntimeError(
                        "market_storage_lifecycle_chunk_drop_mismatch: "
                        f"table={relation} expected={expected} removed={removed}"
                    )
                dropped.extend(removed)
            completed_evidence = {**dict(evidence), "dropped_chunks": dropped}
            self._append_event_with_session(
                session,
                operation_id=operation_id,
                action="chunk_expire",
                event_type="completed",
                target_kind="hypertable_chunk",
                target_id=target_id,
                evidence=completed_evidence,
                cutoff_at=range_end,
            )
        return {"status": "completed", "dropped_chunks": dropped}

    @staticmethod
    def _chunk_with_session(
        session,
        *,
        table_name: str,
        range_start: datetime,
        range_end: datetime,
    ) -> Optional[Mapping[str, Any]]:
        relation, _ = _relation(table_name)
        schema, table = relation.split(".", 1)
        return session.execute(
            text(
                """
                SELECT chunks.chunk_schema, chunks.chunk_name,
                       chunks.range_start, chunks.range_end,
                       chunks.is_compressed,
                       hypertables.compression_enabled
                FROM timescaledb_information.chunks AS chunks
                JOIN timescaledb_information.hypertables AS hypertables
                  ON hypertables.hypertable_schema = chunks.hypertable_schema
                 AND hypertables.hypertable_name = chunks.hypertable_name
                WHERE chunks.hypertable_schema = :schema
                  AND chunks.hypertable_name = :table
                  AND chunks.range_start = :range_start
                  AND chunks.range_end = :range_end
                """
            ),
            {
                "schema": schema,
                "table": table,
                "range_start": range_start,
                "range_end": range_end,
            },
        ).mappings().first()


market_storage_lifecycle_repository = PostgresMarketStorageLifecycleRepository()


__all__ = [
    "MarketStorageLifecycleBusyError",
    "PostgresMarketStorageLifecycleRepository",
    "lifecycle_operation_id",
    "market_storage_lifecycle_repository",
]
