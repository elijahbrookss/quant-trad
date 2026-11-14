"""Transactional collector desired-state control and immutable audit evidence."""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

from sqlalchemy import text

from core.execution_control import execution_checkpoint
from market_data.collector_operations import (
    CollectorAction,
    CollectorDesiredState,
    CollectorKind,
    CollectorOperationStatus,
)

from ....db import db
from .cold_history import cold_layout_active, _manifest
from .fact_storage import canonical_fact_storage_repository
from .market_lifecycle import market_storage_lifecycle_repository


_COLD_COLLECTOR_MAX_PAGES = 32
_COLD_COLLECTOR_MAX_BYTES = 64 * 1024**2
_COLD_COLLECTOR_MAX_ROWS = 250_000


RECENT_FACTS_SQL = """
WITH latest AS (
    SELECT candidate.*
    FROM unnest(CAST(:series_ids AS bigint[])) AS requested(series_id)
    CROSS JOIN LATERAL (
        SELECT id, series_id, observation_key, revision,
               market_commit_seq, source_id, ingestion_run_id,
               fact_type, payload_schema_id, observation_time,
               source_published_at, received_at, accepted_at,
               known_at, transformation_id, external_event_key,
               external_event_group_key, state,
               provenance_schema_id, quality_schema_id
        FROM market.fact_versions AS versions
        WHERE versions.series_id = requested.series_id
          AND versions.state = 'active'
          AND NOT EXISTS (
              SELECT 1 FROM market.fact_versions AS newer
              WHERE newer.series_id = versions.series_id
                AND newer.observation_key = versions.observation_key
                AND newer.revision > versions.revision
          )
        ORDER BY accepted_at DESC, market_commit_seq DESC
        LIMIT :limit
    ) AS candidate
    ORDER BY candidate.accepted_at DESC,
             candidate.market_commit_seq DESC
    LIMIT :limit
)
SELECT latest.*, sources.provider, sources.venue,
       sources.source_kind, sources.adapter_version,
       series.instrument_id
FROM latest
JOIN market.sources AS sources ON sources.id = latest.source_id
JOIN market.series AS series ON series.id = latest.series_id
ORDER BY latest.accepted_at DESC, latest.market_commit_seq DESC
"""


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


# Keep one statement snapshot and independent observation/acceptance clocks.
# Telemetry counts active revisions, including superseded active revisions.
FACT_SERIES_TELEMETRY_SQL = """
SELECT wanted.series_id,
       observation.observation_time AS last_observation_time,
       acceptance.accepted_at AS last_accepted_at,
       recent.accepted_last_minute,
       recent.accepted_last_five_minutes
FROM unnest(CAST(:series_ids AS bigint[])) AS wanted(series_id)
LEFT JOIN LATERAL (
    SELECT observation_time
    FROM market.fact_versions
    WHERE series_id = wanted.series_id AND state = 'active'
    ORDER BY observation_time DESC
    LIMIT 1
) observation ON true
LEFT JOIN LATERAL (
    SELECT accepted_at
    FROM market.fact_versions
    WHERE series_id = wanted.series_id AND state = 'active'
    ORDER BY accepted_at DESC
    LIMIT 1
) acceptance ON true
CROSS JOIN LATERAL (
    SELECT count(*) FILTER (
               WHERE accepted_at >= now() - interval '1 minute'
           ) AS accepted_last_minute,
           count(*) AS accepted_last_five_minutes
    FROM market.fact_versions
    WHERE series_id = wanted.series_id AND state = 'active'
      AND accepted_at >= now() - interval '5 minutes'
) recent
WHERE EXISTS (
    SELECT 1 FROM market.fact_versions WHERE series_id = wanted.series_id
)
"""


def _collector_pages(session, *, series_ids, accepted_at=None):
    rows = session.execute(text("""
        SELECT page.*, max(bounds.last_active_accepted_at) AS last_accepted_at
        FROM market.cold_fact_page_series AS bounds
        JOIN market.cold_fact_pages AS page ON page.id=bounds.page_id
        WHERE bounds.series_id=ANY(:series_ids) AND bounds.last_active_accepted_at IS NOT NULL
          AND (CAST(:accepted_at AS timestamptz) IS NULL OR bounds.last_active_accepted_at>=:accepted_at)
        GROUP BY page.id
        ORDER BY last_accepted_at DESC, page.id
        LIMIT :limit
    """), {"series_ids": series_ids, "accepted_at": accepted_at,
           "limit": _COLD_COLLECTOR_MAX_PAGES + 1}).mappings()
    for row in rows:
        execution_checkpoint()
        yield row["last_accepted_at"], _manifest(row)


class _CollectorReadBudget:
    def __init__(self):
        self.pages = self.bytes = self.rows = 0

    def admit(self, manifest):
        self.pages += 1
        self.bytes += manifest.byte_count
        self.rows += manifest.row_count
        if (self.pages > _COLD_COLLECTOR_MAX_PAGES or self.bytes > _COLD_COLLECTOR_MAX_BYTES
                or self.rows > _COLD_COLLECTOR_MAX_ROWS):
            raise RuntimeError("collector_cold_fact_budget_exceeded: narrow the selected collector scope")


def _hot_fact_ids(session, identities):
    found = set()
    for offset in range(0, len(identities), 1000):
        found.update(session.execute(text("""
            SELECT id FROM market.fact_identities WHERE id=ANY(:ids)
        """), {"ids": identities[offset:offset + 1000]}).scalars())
    return found


def _merge_cold_telemetry(session, hot, series_ids):
    bounds = session.execute(text("""
        SELECT series_id, max(last_active_observation_at) AS last_observation_time,
               max(last_active_accepted_at) AS last_accepted_at
        FROM market.cold_fact_page_series
        WHERE series_id=ANY(:series_ids)
        GROUP BY series_id
    """), {"series_ids": series_ids}).mappings()
    telemetry = {row["series_id"]: dict(row) for row in hot}
    for row in bounds:
        state = telemetry.setdefault(row["series_id"], {
            "series_id": row["series_id"], "last_observation_time": None,
            "last_accepted_at": None, "accepted_last_minute": 0,
            "accepted_last_five_minutes": 0,
        })
        for name in ("last_observation_time", "last_accepted_at"):
            state[name] = max((value for value in (state[name], row[name]) if value is not None), default=None)
    now = session.execute(text("SELECT transaction_timestamp()")).scalar_one()
    since_minute = now - timedelta(minutes=1)
    since_five = now - timedelta(minutes=5)
    cold = canonical_fact_storage_repository.cold_history(session)
    budget = _CollectorReadBudget()
    for _, manifest in _collector_pages(session, series_ids=series_ids, accepted_at=since_five):
        budget.admit(manifest)
        page = cold.reader.read_page(manifest)
        candidates = [row for row in page if row["series_id"] in telemetry
                      and row["state"] == "active" and row["accepted_at"] >= since_five]
        hot_ids = _hot_fact_ids(session, [row["id"] for row in candidates])
        for row in candidates:
            if row["id"] not in hot_ids:
                state = telemetry[row["series_id"]]
                state["accepted_last_five_minutes"] += 1
                if row["accepted_at"] >= since_minute:
                    state["accepted_last_minute"] += 1
    return telemetry.values()


_RECENT_PROVIDER_FIELDS = {
    "provider": "source_provider", "venue": "source_venue",
    "source_kind": "source_kind", "adapter_version": "source_adapter_version",
}


def _cold_recent_row(row, instrument_id):
    return {name: row[name] for name in (
        "id", "series_id", "observation_key", "revision", "market_commit_seq", "source_id",
        "ingestion_run_id", "fact_type", "payload_schema_id", "observation_time",
        "source_published_at", "received_at", "accepted_at", "known_at", "transformation_id",
        "external_event_key", "external_event_group_key", "state", "provenance_schema_id",
        "quality_schema_id", "payload", "provenance", "quality",
    )} | {name: row[field] for name, field in _RECENT_PROVIDER_FIELDS.items()} | {"instrument_id": instrument_id}


def _hot_keys(session, series_id, keys):
    result = set()
    keys = sorted(keys)
    for offset in range(0, len(keys), 1000):
        result.update(session.execute(text("""
            SELECT DISTINCT observation_key
            FROM market.fact_identities
            WHERE series_id=:series_id AND observation_key=ANY(:keys)
        """), {"series_id": series_id, "keys": keys[offset:offset + 1000]}).scalars())
    return result


def _merge_cold_recent(session, hot_rows, series_ids, limit):
    cold = canonical_fact_storage_repository.cold_history(session)
    series = dict(session.execute(text("""
        SELECT id, instrument_id FROM market.series WHERE id=ANY(:series_ids)
    """), {"series_ids": series_ids}).all())
    top = {row["id"]: row for row in hot_rows}
    threshold = min((row["accepted_at"] for row in hot_rows), default=None) if len(top) >= limit else None
    budget = _CollectorReadBudget()
    for last_accepted, manifest in _collector_pages(session, series_ids=series_ids, accepted_at=threshold):
        if threshold is not None and last_accepted < threshold:
            break
        budget.admit(manifest)
        page = cold.reader.read_page(manifest)
        keys = defaultdict(set)
        candidates = []
        for row in page:
            if (row["series_id"] in series and row["state"] == "active"
                    and (threshold is None or row["accepted_at"] >= threshold)):
                candidates.append(row)
                keys[row["series_id"]].add(row["observation_key"])
        for series_id, page_keys in keys.items():
            # Newer archive revisions only exist before a hot head or entirely
            # on cold storage. Any authoritative hot head resolves the key.
            keys[series_id] -= _hot_keys(session, series_id, page_keys)
        for series_id, page_keys in keys.items():
            if not page_keys:
                continue
            latest = cold.latest_for_keys(session, series_id=series_id, keys=sorted(page_keys),
                                          page_admission=budget.admit)
            if latest.keys() != page_keys:
                raise RuntimeError("collector_cold_fact_locator_coverage_invalid")
            for row in candidates:
                if row["series_id"] != series_id or row["observation_key"] not in latest:
                    continue
                if row["id"] == latest[row["observation_key"]]["id"]:
                    top[row["id"]] = _cold_recent_row(row, series[series_id])
        best = sorted(top.values(), key=lambda row: (row["accepted_at"], row["market_commit_seq"]), reverse=True)[:limit]
        top = {row["id"]: row for row in best}
        if len(best) >= limit:
            threshold = best[-1]["accepted_at"]
    return sorted(top.values(), key=lambda row: (row["accepted_at"], row["market_commit_seq"]), reverse=True)[:limit]


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
        precondition_error: str | None = None,
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
            elif precondition_error:
                error = str(precondition_error)
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
        with market_storage_lifecycle_repository.dataset_snapshot_session(database=db) as session:
            rows = session.execute(
                text(FACT_SERIES_TELEMETRY_SQL),
                {"series_ids": normalized_ids},
            ).mappings().all()
            if cold_layout_active(session):
                rows = list(_merge_cold_telemetry(session, rows, normalized_ids))
        return {int(row["series_id"]): _public_row(row) for row in rows}

    def continuous_stream_telemetry(
        self, *, definition_ids: list[str]
    ) -> dict[str, dict[str, Any]]:
        """Return provider activity without treating Fact flow as liveness."""

        normalized_ids = sorted(
            {str(value).strip() for value in definition_ids if str(value).strip()}
        )
        if not normalized_ids:
            return {}
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT definitions.id AS definition_id,
                           latest.last_provider_activity_at,
                           book_state.lifecycle AS projection_lifecycle,
                           book_state.updated_at AS projection_updated_at
                    FROM market.stream_definitions AS definitions
                    LEFT JOIN LATERAL (
                        SELECT ranges.max_received_at AS last_provider_activity_at
                        FROM market.raw_archive_manifests AS manifests
                        JOIN market.raw_archive_ranges AS ranges
                          ON ranges.manifest_id = manifests.id
                        WHERE manifests.definition_id = definitions.id
                          AND ranges.channel <> 'subscriptions'
                        ORDER BY manifests.first_received_at DESC,
                                 ranges.max_received_at DESC
                        LIMIT 1
                    ) AS latest ON TRUE
                    LEFT JOIN market.book_reconstruction_state AS book_state
                      ON book_state.series_id = definitions.series_id
                     AND book_state.definition_id = definitions.id
                    WHERE definitions.id = ANY(:definition_ids)
                    """
                ),
                {"definition_ids": normalized_ids},
            ).mappings().all()
        return {
            str(row["definition_id"]): _public_row(row)
            for row in rows
        }

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
        with market_storage_lifecycle_repository.dataset_snapshot_session(database=db) as session:
            rows = session.execute(
                text(RECENT_FACTS_SQL),
                {"series_ids": normalized_ids, "limit": bounded_limit},
            ).mappings().all()
            payloads = canonical_fact_storage_repository.read_rows_by_ids(session, [row["id"] for row in rows])
            rows = [{**row, **{name: payloads[row["id"]][name] for name in ("payload", "provenance", "quality")}}
                    for row in rows]
            if cold_layout_active(session):
                rows = _merge_cold_recent(session, rows, normalized_ids, bounded_limit)
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
