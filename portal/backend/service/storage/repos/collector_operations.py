"""Transactional collector desired-state control and immutable audit evidence."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any, Mapping

from sqlalchemy import text

from market_data.collector_operations import (
    CollectorAction,
    CollectorDesiredState,
    CollectorKind,
    CollectorOperationStatus,
)

from ....db import db


class CollectorOperationRequestConflict(RuntimeError):
    """Raised when an idempotency key is reused for different intent."""


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _json(value: Mapping[str, Any] | None) -> str:
    return json.dumps(
        dict(value or {}),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _public_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            payload[str(key)] = _as_utc(value).isoformat()
        else:
            payload[str(key)] = value
    return payload


def _prior_state(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "configured_enabled": bool(row["enabled"]),
        "desired_state": str(row["desired_state"]),
        "control_generation": int(row["control_generation"]),
        "control_requested_at": (
            _as_utc(row["control_requested_at"]).isoformat()
            if row.get("control_requested_at") is not None
            else None
        ),
        "control_requested_by": row.get("control_requested_by"),
        "control_request_id": row.get("control_request_id"),
    }


class PostgresCollectorOperationsRepository:
    """Own one audited command path for both durable collector families."""

    _TABLES = {
        CollectorKind.SCHEDULED_FACT: "market.collection_definitions",
        CollectorKind.CONTINUOUS_STREAM: "market.stream_definitions",
    }

    @staticmethod
    def _normalize_intent(
        *,
        collector_kind: CollectorKind | str,
        action: CollectorAction | str,
    ) -> tuple[CollectorKind, CollectorAction]:
        kind = CollectorKind(collector_kind)
        normalized_action = CollectorAction(action)
        if not normalized_action.mutates:
            raise ValueError(
                "collector_operation_invalid: read-only actions do not enter the mutation ledger"
            )
        return kind, normalized_action

    @staticmethod
    def _transition(
        *,
        action: CollectorAction,
        configured_enabled: bool,
        desired_state: CollectorDesiredState,
    ) -> tuple[CollectorDesiredState, bool, str | None]:
        if action in {
            CollectorAction.START,
            CollectorAction.RESTART,
            CollectorAction.RESUME,
        } and not configured_enabled:
            return desired_state, False, "collector_configured_disabled"
        if action == CollectorAction.START:
            if desired_state == CollectorDesiredState.PAUSED:
                return desired_state, False, "collector_paused_use_resume"
            return CollectorDesiredState.RUNNING, False, None
        if action == CollectorAction.STOP:
            return CollectorDesiredState.STOPPED, False, None
        if action == CollectorAction.PAUSE:
            if desired_state == CollectorDesiredState.STOPPED:
                return desired_state, False, "collector_stopped_use_start"
            return CollectorDesiredState.PAUSED, False, None
        if action == CollectorAction.RESUME:
            if desired_state == CollectorDesiredState.STOPPED:
                return desired_state, False, "collector_stopped_use_start"
            return CollectorDesiredState.RUNNING, False, None
        if action == CollectorAction.RESTART:
            if desired_state != CollectorDesiredState.RUNNING:
                return desired_state, False, "collector_not_running"
            return CollectorDesiredState.RUNNING, True, None
        if action == CollectorAction.RECOVER:
            return desired_state, False, "collector_recovery_requires_capability_handler"
        raise AssertionError(f"unhandled collector action: {action.value}")

    @staticmethod
    def _same_request(
        row: Mapping[str, Any],
        *,
        collector_id: str,
        collector_kind: CollectorKind,
        action: CollectorAction,
        actor_id: str,
        context: Mapping[str, Any],
    ) -> bool:
        return (
            str(row["collector_id"]) == collector_id
            and str(row["collector_kind"]) == collector_kind.value
            and str(row["action"]) == action.value
            and str(row["actor_id"]) == actor_id
            and dict(row["context"] or {}) == dict(context)
        )

    def apply_lifecycle_action(
        self,
        *,
        request_id: str,
        collector_id: str,
        collector_kind: CollectorKind | str,
        action: CollectorAction | str,
        requested_at: datetime,
        actor_id: str,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Apply one desired-state transition and append its final result atomically."""

        kind, normalized_action = self._normalize_intent(
            collector_kind=collector_kind,
            action=action,
        )
        normalized_request = str(request_id or "").strip()
        normalized_collector = str(collector_id or "").strip()
        normalized_actor = str(actor_id or "").strip()
        normalized_context = dict(context or {})
        if not normalized_request or not normalized_collector or not normalized_actor:
            raise ValueError(
                "collector_operation_invalid: request_id, collector_id, and actor_id are required"
            )
        requested = _as_utc(requested_at)
        table_name = self._TABLES[kind]

        with db.session() as session:
            session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:request_id, 0))"),
                {"request_id": normalized_request},
            )
            prior_operation = session.execute(
                text(
                    """
                    SELECT * FROM market.collector_operation_events
                    WHERE request_id = :request_id
                    """
                ),
                {"request_id": normalized_request},
            ).mappings().first()
            if prior_operation is not None:
                if not self._same_request(
                    prior_operation,
                    collector_id=normalized_collector,
                    collector_kind=kind,
                    action=normalized_action,
                    actor_id=normalized_actor,
                    context=normalized_context,
                ):
                    raise CollectorOperationRequestConflict(
                        "collector_operation_request_conflict: request_id was already used for different intent"
                    )
                result = _public_row(prior_operation)
                result["idempotent_replay"] = True
                return result

            definition = session.execute(
                text(f"SELECT * FROM {table_name} WHERE id = :collector_id FOR UPDATE"),
                {"collector_id": normalized_collector},
            ).mappings().first()
            prior_state = _prior_state(definition) if definition is not None else {}
            error: str | None = None
            resulting_state = dict(prior_state)

            if definition is None:
                error = "collector_unknown"
            else:
                target, force_generation, error = self._transition(
                    action=normalized_action,
                    configured_enabled=bool(definition["enabled"]),
                    desired_state=CollectorDesiredState(definition["desired_state"]),
                )
                if error is None:
                    generation_delta = int(
                        force_generation
                        or target.value != str(definition["desired_state"])
                    )
                    schedule_clause = (
                        ", next_scheduled_at = LEAST(next_scheduled_at, now())"
                        ", available_at = LEAST(available_at, now())"
                        if kind == CollectorKind.SCHEDULED_FACT
                        and target == CollectorDesiredState.RUNNING
                        else ""
                    )
                    definition = session.execute(
                        text(
                            f"""
                            UPDATE {table_name}
                            SET desired_state = :desired_state,
                                control_generation = control_generation + :generation_delta,
                                control_requested_at = :requested_at,
                                control_requested_by = :actor_id,
                                control_request_id = :request_id,
                                updated_at = now()
                                {schedule_clause}
                            WHERE id = :collector_id
                            RETURNING *
                            """
                        ),
                        {
                            "collector_id": normalized_collector,
                            "desired_state": target.value,
                            "generation_delta": generation_delta,
                            "requested_at": requested,
                            "actor_id": normalized_actor,
                            "request_id": normalized_request,
                        },
                    ).mappings().one()
                    resulting_state = _prior_state(definition)

            status = (
                CollectorOperationStatus.FAILED
                if error is not None
                else CollectorOperationStatus.SUCCEEDED
            )
            operation_id = f"mco_{uuid.uuid4().hex}"
            evidence = {
                "schema_version": "market.collector_operation_evidence.v1",
                "transition_applied": error is None,
                "generation_changed": (
                    prior_state.get("control_generation")
                    != resulting_state.get("control_generation")
                ),
            }
            operation = session.execute(
                text(
                    """
                    INSERT INTO market.collector_operation_events (
                        id, request_id, collector_id, collector_kind, action,
                        status, requested_at, actor_id, context, prior_state,
                        resulting_state, evidence, error
                    ) VALUES (
                        :id, :request_id, :collector_id, :collector_kind, :action,
                        :status, :requested_at, :actor_id, CAST(:context AS jsonb),
                        CAST(:prior_state AS jsonb), CAST(:resulting_state AS jsonb),
                        CAST(:evidence AS jsonb), :error
                    )
                    RETURNING *
                    """
                ),
                {
                    "id": operation_id,
                    "request_id": normalized_request,
                    "collector_id": normalized_collector,
                    "collector_kind": kind.value,
                    "action": normalized_action.value,
                    "status": status.value,
                    "requested_at": requested,
                    "actor_id": normalized_actor,
                    "context": _json(normalized_context),
                    "prior_state": _json(prior_state),
                    "resulting_state": _json(resulting_state),
                    "evidence": _json(evidence),
                    "error": error,
                },
            ).mappings().one()
        result = _public_row(operation)
        result["idempotent_replay"] = False
        return result

    def list_operations(
        self,
        *,
        collector_id: str | None = None,
        collector_kind: CollectorKind | str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 1000))
        predicates: list[str] = []
        params: dict[str, Any] = {"limit": bounded_limit}
        if collector_id:
            predicates.append("collector_id = :collector_id")
            params["collector_id"] = str(collector_id)
        if collector_kind is not None:
            predicates.append("collector_kind = :collector_kind")
            params["collector_kind"] = CollectorKind(collector_kind).value
        predicate = "WHERE " + " AND ".join(predicates) if predicates else ""
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT * FROM market.collector_operation_events
                    {predicate}
                    ORDER BY requested_at DESC, recorded_at DESC
                    LIMIT :limit
                    """
                ),
                params,
            ).mappings().all()
        return [_public_row(row) for row in rows]

    def fact_series_telemetry(
        self, *, series_ids: list[int]
    ) -> dict[int, dict[str, Any]]:
        normalized_ids = sorted({int(value) for value in series_ids})
        if not normalized_ids:
            return {}
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT series_id,
                           max(observation_time) FILTER (WHERE state = 'active')
                               AS last_observation_time,
                           max(accepted_at) FILTER (WHERE state = 'active')
                               AS last_accepted_at,
                           count(*) FILTER (
                               WHERE state = 'active'
                                 AND accepted_at >= now() - interval '1 minute'
                           ) AS accepted_last_minute,
                           count(*) FILTER (
                               WHERE state = 'active'
                                 AND accepted_at >= now() - interval '5 minutes'
                           ) AS accepted_last_five_minutes
                    FROM market.fact_versions
                    WHERE series_id = ANY(:series_ids)
                    GROUP BY series_id
                    """
                ),
                {"series_ids": normalized_ids},
            ).mappings().all()
        return {int(row["series_id"]): _public_row(row) for row in rows}

    def recent_facts(
        self,
        *,
        series_ids: list[int],
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_ids = sorted({int(value) for value in series_ids})
        if not normalized_ids:
            return []
        bounded_limit = max(1, min(int(limit), 500))
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    WITH latest AS (
                        SELECT DISTINCT ON (series_id, observation_key)
                               id, series_id, observation_key, revision,
                               market_commit_seq, source_id, ingestion_run_id,
                               fact_type, payload_schema_id, observation_time,
                               source_published_at, received_at, accepted_at,
                               known_at, transformation_id, external_event_key,
                               external_event_group_key, state, payload,
                               provenance_schema_id, provenance,
                               quality_schema_id, quality
                        FROM market.fact_versions
                        WHERE series_id = ANY(:series_ids)
                        ORDER BY series_id, observation_key, revision DESC
                    )
                    SELECT latest.*, sources.provider, sources.venue,
                           sources.source_kind, sources.adapter_version,
                           series.instrument_id
                    FROM latest
                    JOIN market.sources AS sources ON sources.id = latest.source_id
                    JOIN market.series AS series ON series.id = latest.series_id
                    WHERE latest.state = 'active'
                    ORDER BY latest.accepted_at DESC, latest.market_commit_seq DESC
                    LIMIT :limit
                    """
                ),
                {"series_ids": normalized_ids, "limit": bounded_limit},
            ).mappings().all()
        return [_public_row(row) for row in rows]

    def list_gap_evidence(
        self,
        *,
        series_ids: list[int],
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_ids = sorted({int(value) for value in series_ids})
        if not normalized_ids:
            return []
        bounded_limit = max(1, min(int(limit), 500))
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT gaps.*, sources.provider, sources.venue,
                           sources.adapter_version
                    FROM market.gap_evidence AS gaps
                    LEFT JOIN market.sources AS sources ON sources.id = gaps.source_id
                    WHERE gaps.series_id = ANY(:series_ids)
                    ORDER BY gaps.created_at DESC, gaps.id DESC
                    LIMIT :limit
                    """
                ),
                {"series_ids": normalized_ids, "limit": bounded_limit},
            ).mappings().all()
        return [_public_row(row) for row in rows]

    def list_stream_events(
        self,
        *,
        definition_id: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 500))
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT * FROM market.stream_session_events
                    WHERE definition_id = :definition_id
                    ORDER BY occurred_at DESC, session_id, event_ordinal DESC
                    LIMIT :limit
                    """
                ),
                {"definition_id": str(definition_id), "limit": bounded_limit},
            ).mappings().all()
        return [_public_row(row) for row in rows]

    def list_stream_quality_events(
        self,
        *,
        definition_id: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 500))
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT * FROM market.stream_quality_events
                    WHERE definition_id = :definition_id
                    ORDER BY detected_at DESC, id DESC
                    LIMIT :limit
                    """
                ),
                {"definition_id": str(definition_id), "limit": bounded_limit},
            ).mappings().all()
        return [_public_row(row) for row in rows]


collector_operations_repository = PostgresCollectorOperationsRepository()


__all__ = [
    "CollectorOperationRequestConflict",
    "PostgresCollectorOperationsRepository",
    "collector_operations_repository",
]
