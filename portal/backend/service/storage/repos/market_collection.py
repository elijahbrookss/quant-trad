"""Fenced scheduler state for provider-neutral market-data collectors."""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Optional

from sqlalchemy import text

from ....db import db


class MarketCollectionOwnershipError(RuntimeError):
    """Raised when a worker attempts to mutate a claim it no longer owns."""


def _token_hash(token: str) -> str:
    normalized = str(token or "").strip()
    if not normalized:
        raise ValueError("market_collection_claim_invalid: lease token is required")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _json(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), sort_keys=True, separators=(",", ":"), default=str)


@dataclass(frozen=True)
class CollectionClaim:
    definition_id: str
    attempt_id: str
    scheduled_for: datetime
    attempt_number: int
    max_attempts: int
    poll_interval_seconds: int
    source_id: int
    series_id: int
    provider: str
    venue: str
    source_kind: str
    adapter_version: str
    instrument_id: str
    fact_type: str
    contract_version: str
    config: Mapping[str, Any]
    owner_id: str
    lease_token: str
    lease_generation: int
    lease_expires_at: datetime
    missed_start: Optional[datetime] = None
    missed_count: int = 0

    def fence(self) -> dict[str, Any]:
        return {
            "definition_id": self.definition_id,
            "source_id": self.source_id,
            "owner_id": self.owner_id,
            "lease_token": self.lease_token,
            "lease_generation": self.lease_generation,
        }

    def public_dict(self) -> dict[str, Any]:
        return {
            "definition_id": self.definition_id,
            "attempt_id": self.attempt_id,
            "scheduled_for": self.scheduled_for.isoformat(),
            "attempt_number": self.attempt_number,
            "max_attempts": self.max_attempts,
            "poll_interval_seconds": self.poll_interval_seconds,
            "source_id": self.source_id,
            "series_id": self.series_id,
            "provider": self.provider,
            "venue": self.venue,
            "instrument_id": self.instrument_id,
            "fact_type": self.fact_type,
            "contract_version": self.contract_version,
            "owner_id": self.owner_id,
            "lease_generation": self.lease_generation,
            "lease_expires_at": self.lease_expires_at.isoformat(),
            "missed_start": self.missed_start.isoformat() if self.missed_start else None,
            "missed_count": self.missed_count,
        }


class PostgresMarketCollectionRepository:
    """Coordinates schedules, retries, provider budgets, and worker fencing."""

    def upsert_definition(
        self,
        *,
        definition_id: str,
        source_id: int,
        series_id: int,
        poll_interval_seconds: int,
        max_attempts: int,
        enabled: bool,
        config: Mapping[str, Any],
        next_scheduled_at: datetime,
    ) -> dict[str, Any]:
        definition_id = str(definition_id or "").strip()
        interval = int(poll_interval_seconds)
        attempts = int(max_attempts)
        if not definition_id or interval <= 0 or attempts <= 0:
            raise ValueError("market_collection_definition_invalid")
        scheduled = _as_utc(next_scheduled_at)
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    INSERT INTO market.collection_definitions (
                        id, source_id, series_id, enabled, poll_interval_seconds,
                        max_attempts, next_scheduled_at, available_at, config
                    ) VALUES (
                        :id, :source_id, :series_id, :enabled, :interval,
                        :max_attempts, :scheduled, :scheduled, CAST(:config AS jsonb)
                    )
                    ON CONFLICT (source_id, series_id) DO UPDATE
                    SET enabled = EXCLUDED.enabled,
                        poll_interval_seconds = EXCLUDED.poll_interval_seconds,
                        max_attempts = EXCLUDED.max_attempts,
                        config = EXCLUDED.config,
                        updated_at = now()
                    RETURNING *
                    """
                ),
                {
                    "id": definition_id,
                    "source_id": int(source_id),
                    "series_id": int(series_id),
                    "enabled": bool(enabled),
                    "interval": interval,
                    "max_attempts": attempts,
                    "scheduled": scheduled,
                    "config": _json(config),
                },
            ).mappings().one()
        return dict(row)

    def list_definitions(
        self, *, definition_id: Optional[str] = None
    ) -> list[dict[str, Any]]:
        predicate = "WHERE definitions.id = :definition_id" if definition_id else ""
        params = {"definition_id": str(definition_id)} if definition_id else {}
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT definitions.*, sources.provider, sources.venue,
                           sources.source_kind, sources.adapter_version,
                           series.instrument_id, series.fact_type,
                           series.timeframe_seconds, series.contract_version,
                           instruments.symbol AS instrument_symbol,
                           instruments.instrument_type AS instrument_type,
                           (definitions.lease_expires_at > now()) AS lease_current
                    FROM market.collection_definitions AS definitions
                    JOIN market.sources AS sources ON sources.id = definitions.source_id
                    JOIN market.series AS series ON series.id = definitions.series_id
                    JOIN portal_instruments AS instruments ON instruments.id = series.instrument_id
                    {predicate}
                    ORDER BY definitions.id
                    """
                ),
                params,
            ).mappings().all()
        return [dict(row) for row in rows]

    def list_attempts(
        self, *, definition_id: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        normalized = str(definition_id or "").strip()
        if not normalized:
            raise ValueError("market_collection_definition_invalid: id is required")
        bounded_limit = max(1, min(int(limit), 1000))
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, definition_id, scheduled_for, attempt_number,
                           lease_generation, owner_id, status, started_at,
                           finished_at, ingestion_run_id, error, evidence
                    FROM market.collection_attempts
                    WHERE definition_id = :definition_id
                    ORDER BY scheduled_for DESC, attempt_number DESC
                    LIMIT :limit
                    """
                ),
                {"definition_id": normalized, "limit": bounded_limit},
            ).mappings().all()
        return [dict(row) for row in rows]

    def set_enabled(self, definition_id: str, *, enabled: bool) -> dict[str, Any]:
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    UPDATE market.collection_definitions
                    SET enabled = :enabled,
                        next_scheduled_at = CASE
                            WHEN :enabled THEN LEAST(next_scheduled_at, now())
                            ELSE next_scheduled_at
                        END,
                        available_at = CASE
                            WHEN :enabled THEN LEAST(available_at, now())
                            ELSE available_at
                        END,
                        updated_at = now()
                    WHERE id = :definition_id
                    RETURNING *
                    """
                ),
                {"definition_id": str(definition_id), "enabled": bool(enabled)},
            ).mappings().first()
        if row is None:
            raise ValueError(
                f"market_collection_definition_unknown: definition_id={definition_id}"
            )
        return dict(row)

    def claim_due(
        self,
        *,
        owner_id: str,
        lease_seconds: float = 90.0,
        definition_id: Optional[str] = None,
    ) -> Optional[CollectionClaim]:
        owner = str(owner_id or "").strip()
        lease_ttl = float(lease_seconds)
        if not owner or lease_ttl <= 0:
            raise ValueError("market_collection_claim_invalid: owner and lease are required")
        token = secrets.token_urlsafe(32)
        token_hash = _token_hash(token)
        selected_definition_id = str(definition_id or "").strip() or None
        with db.session() as session:
            now = _as_utc(session.execute(text("SELECT now()" )).scalar_one())
            row = session.execute(
                text(
                    """
                    SELECT definitions.*, sources.provider, sources.venue,
                           sources.source_kind, sources.adapter_version,
                           series.instrument_id, series.fact_type,
                           series.contract_version
                    FROM market.collection_definitions AS definitions
                    JOIN market.sources AS sources ON sources.id = definitions.source_id
                    JOIN market.series AS series ON series.id = definitions.series_id
                    WHERE definitions.enabled IS TRUE
                      AND (:definition_id IS NULL OR definitions.id = :definition_id)
                      AND definitions.next_scheduled_at <= :now
                      AND definitions.available_at <= :now
                      AND (
                          definitions.lease_owner IS NULL
                          OR definitions.lease_expires_at <= :now
                      )
                    ORDER BY definitions.next_scheduled_at, definitions.id
                    FOR UPDATE OF definitions SKIP LOCKED
                    LIMIT 1
                    """
                ),
                {"now": now, "definition_id": selected_definition_id},
            ).mappings().first()
            if row is None:
                return None

            interval = int(row["poll_interval_seconds"])
            original_scheduled = _as_utc(row["next_scheduled_at"])
            missed_count = max(
                0, int((now - original_scheduled).total_seconds() // interval)
            )
            scheduled_for = original_scheduled + timedelta(
                seconds=missed_count * interval
            )
            if missed_count:
                session.execute(
                    text(
                        """
                        INSERT INTO market.collection_attempts (
                            id, definition_id, scheduled_for, attempt_number,
                            lease_generation, owner_id, status, started_at,
                            finished_at, evidence
                        ) VALUES (
                            :id, :definition_id, :scheduled_for, 1,
                            :generation, :owner, 'missed', :now, :now,
                            CAST(:evidence AS jsonb)
                        )
                        ON CONFLICT (definition_id, scheduled_for, attempt_number)
                        DO NOTHING
                        """
                    ),
                    {
                        "id": f"mca_{uuid.uuid4().hex}",
                        "definition_id": str(row["id"]),
                        "scheduled_for": original_scheduled,
                        "generation": int(row["lease_generation"]),
                        "owner": owner,
                        "now": now,
                        "evidence": _json(
                            {
                                "schema_version": "market_collection_missed.v1",
                                "missed_count": missed_count,
                                "missed_start": original_scheduled.isoformat(),
                                "resumed_at": scheduled_for.isoformat(),
                            }
                        ),
                    },
                )

            attempt_number = int(
                session.execute(
                    text(
                        """
                        SELECT COALESCE(MAX(attempt_number), 0) + 1
                        FROM market.collection_attempts
                        WHERE definition_id = :definition_id
                          AND scheduled_for = :scheduled_for
                        """
                    ),
                    {
                        "definition_id": str(row["id"]),
                        "scheduled_for": scheduled_for,
                    },
                ).scalar_one()
            )
            generation = int(row["lease_generation"]) + 1
            expires_at = now + timedelta(seconds=lease_ttl)
            attempt_id = f"mca_{uuid.uuid4().hex}"
            session.execute(
                text(
                    """
                    UPDATE market.collection_definitions
                    SET next_scheduled_at = :scheduled_for,
                        lease_owner = :owner,
                        lease_token_hash = :token_hash,
                        lease_generation = :generation,
                        lease_expires_at = :expires_at,
                        last_attempt_at = :now,
                        updated_at = :now
                    WHERE id = :definition_id
                    """
                ),
                {
                    "definition_id": str(row["id"]),
                    "scheduled_for": scheduled_for,
                    "owner": owner,
                    "token_hash": token_hash,
                    "generation": generation,
                    "expires_at": expires_at,
                    "now": now,
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO market.collection_attempts (
                        id, definition_id, scheduled_for, attempt_number,
                        lease_generation, owner_id, status, started_at, evidence
                    ) VALUES (
                        :id, :definition_id, :scheduled_for, :attempt_number,
                        :generation, :owner, 'running', :now, '{}'::jsonb
                    )
                    """
                ),
                {
                    "id": attempt_id,
                    "definition_id": str(row["id"]),
                    "scheduled_for": scheduled_for,
                    "attempt_number": attempt_number,
                    "generation": generation,
                    "owner": owner,
                    "now": now,
                },
            )
        return CollectionClaim(
            definition_id=str(row["id"]),
            attempt_id=attempt_id,
            scheduled_for=scheduled_for,
            attempt_number=attempt_number,
            max_attempts=int(row["max_attempts"]),
            poll_interval_seconds=interval,
            source_id=int(row["source_id"]),
            series_id=int(row["series_id"]),
            provider=str(row["provider"]),
            venue=str(row["venue"]),
            source_kind=str(row["source_kind"]),
            adapter_version=str(row["adapter_version"]),
            instrument_id=str(row["instrument_id"]),
            fact_type=str(row["fact_type"]),
            contract_version=str(row["contract_version"]),
            config=dict(row["config"] or {}),
            owner_id=owner,
            lease_token=token,
            lease_generation=generation,
            lease_expires_at=expires_at,
            missed_start=original_scheduled if missed_count else None,
            missed_count=missed_count,
        )

    @staticmethod
    def _require_owned(session, claim: CollectionClaim) -> Mapping[str, Any]:
        row = session.execute(
            text(
                """
                SELECT *, lease_expires_at > now() AS lease_current
                FROM market.collection_definitions
                WHERE id = :definition_id
                FOR UPDATE
                """
            ),
            {"definition_id": claim.definition_id},
        ).mappings().first()
        if (
            row is None
            or str(row["lease_owner"] or "") != claim.owner_id
            or str(row["lease_token_hash"] or "") != _token_hash(claim.lease_token)
            or int(row["lease_generation"]) != claim.lease_generation
            or not bool(row["lease_current"])
        ):
            raise MarketCollectionOwnershipError(
                "market_collection_ownership_lost: stale worker mutation rejected"
            )
        return row

    def heartbeat(self, claim: CollectionClaim, *, lease_seconds: float = 90.0) -> None:
        with db.session() as session:
            self._require_owned(session, claim)
            session.execute(
                text(
                    """
                    UPDATE market.collection_definitions
                    SET lease_expires_at = now() + (:lease_seconds * interval '1 second'),
                        updated_at = now()
                    WHERE id = :definition_id
                    """
                ),
                {
                    "definition_id": claim.definition_id,
                    "lease_seconds": float(lease_seconds),
                },
            )

    def complete(
        self,
        claim: CollectionClaim,
        *,
        ingestion_run_id: str,
        evidence: Mapping[str, Any],
    ) -> None:
        with db.session() as session:
            row = self._require_owned(session, claim)
            session.execute(
                text(
                    """
                    UPDATE market.collection_attempts
                    SET status = 'succeeded', finished_at = now(),
                        ingestion_run_id = :ingestion_run_id,
                        evidence = CAST(:evidence AS jsonb)
                    WHERE id = :attempt_id AND status = 'running'
                    """
                ),
                {
                    "attempt_id": claim.attempt_id,
                    "ingestion_run_id": str(ingestion_run_id),
                    "evidence": _json(evidence),
                },
            )
            session.execute(
                text(
                    """
                    UPDATE market.collection_definitions
                    SET next_scheduled_at = :scheduled_for
                          + (poll_interval_seconds * interval '1 second'),
                        available_at = :scheduled_for
                          + (poll_interval_seconds * interval '1 second'),
                        consecutive_failures = 0,
                        lease_owner = NULL, lease_token_hash = NULL,
                        lease_expires_at = NULL, last_success_at = now(),
                        last_error = NULL, updated_at = now()
                    WHERE id = :definition_id
                    """
                ),
                {
                    "definition_id": str(row["id"]),
                    "scheduled_for": claim.scheduled_for,
                },
            )

    def fail(
        self,
        claim: CollectionClaim,
        *,
        error: BaseException,
        retry_base_seconds: float = 2.0,
        evidence: Mapping[str, Any] | None = None,
    ) -> bool:
        """Fail one attempt and return whether its scheduled poll is exhausted."""

        message = str(error)[:4000]
        failure_evidence = {
            "schema_version": "market_collection_failure.v2",
            "exhausted": claim.attempt_number >= claim.max_attempts,
            **dict(evidence or {}),
        }
        exhausted = claim.attempt_number >= claim.max_attempts
        backoff = min(300.0, max(0.1, float(retry_base_seconds)) * (2 ** (claim.attempt_number - 1)))
        with db.session() as session:
            self._require_owned(session, claim)
            session.execute(
                text(
                    """
                    UPDATE market.collection_attempts
                    SET status = 'failed', finished_at = now(), error = :error,
                        evidence = CAST(:evidence AS jsonb)
                    WHERE id = :attempt_id AND status = 'running'
                    """
                ),
                {
                    "attempt_id": claim.attempt_id,
                    "error": message,
                    "evidence": _json(
                        {
                            **failure_evidence,
                            "retry_backoff_seconds": None if exhausted else backoff,
                        }
                    ),
                },
            )
            session.execute(
                text(
                    """
                    UPDATE market.collection_definitions
                    SET next_scheduled_at = CASE
                            WHEN :exhausted THEN :scheduled_for
                              + (poll_interval_seconds * interval '1 second')
                            ELSE :scheduled_for
                        END,
                        available_at = CASE
                            WHEN :exhausted THEN :scheduled_for
                              + (poll_interval_seconds * interval '1 second')
                            ELSE now() + (:backoff * interval '1 second')
                        END,
                        consecutive_failures = consecutive_failures + 1,
                        lease_owner = NULL, lease_token_hash = NULL,
                        lease_expires_at = NULL, last_error = :error,
                        updated_at = now()
                    WHERE id = :definition_id
                    """
                ),
                {
                    "definition_id": claim.definition_id,
                    "exhausted": exhausted,
                    "scheduled_for": claim.scheduled_for,
                    "backoff": backoff,
                    "error": message,
                },
            )
        return exhausted

    def list_recent_attempts(self, *, limit_per_definition: int = 5) -> list[dict[str, Any]]:
        """Read one bounded recent-attempt window for every collector definition."""

        bounded_limit = max(1, min(int(limit_per_definition), 100))
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    WITH ranked AS (
                        SELECT id, definition_id, scheduled_for, attempt_number,
                               lease_generation, owner_id, status, started_at,
                               finished_at, ingestion_run_id, error, evidence,
                               row_number() OVER (
                                   PARTITION BY definition_id
                                   ORDER BY scheduled_for DESC, attempt_number DESC
                               ) AS attempt_rank
                        FROM market.collection_attempts
                    )
                    SELECT id, definition_id, scheduled_for, attempt_number,
                           lease_generation, owner_id, status, started_at,
                           finished_at, ingestion_run_id, error, evidence
                    FROM ranked
                    WHERE attempt_rank <= :limit
                    ORDER BY definition_id, scheduled_for DESC, attempt_number DESC
                    """
                ),
                {"limit": bounded_limit},
            ).mappings().all()
        return [dict(row) for row in rows]

    def register_worker(
        self,
        *,
        worker_id: str,
        worker_role: str,
        worker_version: str,
        ttl_seconds: float,
        state: str = "starting",
        capabilities: Mapping[str, Any] | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        worker = str(worker_id or "").strip()
        role = str(worker_role or "").strip()
        version = str(worker_version or "").strip()
        ttl = float(ttl_seconds)
        if not worker or not role or not version or ttl <= 0:
            raise ValueError("market_collector_worker_invalid")
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    INSERT INTO market.collector_worker_state (
                        worker_id, worker_role, worker_version, state,
                        started_at, heartbeat_at, expires_at, last_loop_at,
                        active_definition_id, active_attempt_id, last_error,
                        capabilities, context, updated_at
                    ) VALUES (
                        :worker_id, :worker_role, :worker_version, :state,
                        now(), now(), now() + (:ttl * interval '1 second'), now(),
                        NULL, NULL, NULL, CAST(:capabilities AS jsonb),
                        CAST(:context AS jsonb), now()
                    )
                    ON CONFLICT (worker_id) DO UPDATE
                    SET worker_role = EXCLUDED.worker_role,
                        worker_version = EXCLUDED.worker_version,
                        state = EXCLUDED.state,
                        started_at = EXCLUDED.started_at,
                        heartbeat_at = EXCLUDED.heartbeat_at,
                        expires_at = EXCLUDED.expires_at,
                        last_loop_at = EXCLUDED.last_loop_at,
                        active_definition_id = NULL,
                        active_attempt_id = NULL,
                        last_error = NULL,
                        capabilities = EXCLUDED.capabilities,
                        context = EXCLUDED.context,
                        updated_at = now()
                    RETURNING *
                    """
                ),
                {
                    "worker_id": worker,
                    "worker_role": role,
                    "worker_version": version,
                    "state": str(state),
                    "ttl": ttl,
                    "capabilities": _json(capabilities),
                    "context": _json(context),
                },
            ).mappings().one()
        return dict(row)

    def heartbeat_worker(
        self,
        *,
        worker_id: str,
        ttl_seconds: float,
        state: str,
        active_definition_id: Optional[str] = None,
        active_attempt_id: Optional[str] = None,
        last_error: Optional[str] = None,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        worker = str(worker_id or "").strip()
        ttl = float(ttl_seconds)
        if not worker or ttl <= 0:
            raise ValueError("market_collector_worker_invalid")
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    UPDATE market.collector_worker_state
                    SET state = :state, heartbeat_at = now(),
                        expires_at = now() + (:ttl * interval '1 second'),
                        last_loop_at = now(),
                        active_definition_id = :active_definition_id,
                        active_attempt_id = :active_attempt_id,
                        last_error = :last_error,
                        context = context || CAST(:context AS jsonb),
                        updated_at = now()
                    WHERE worker_id = :worker_id
                    RETURNING *
                    """
                ),
                {
                    "worker_id": worker,
                    "ttl": ttl,
                    "state": str(state),
                    "active_definition_id": str(active_definition_id) if active_definition_id else None,
                    "active_attempt_id": str(active_attempt_id) if active_attempt_id else None,
                    "last_error": str(last_error)[:4000] if last_error else None,
                    "context": _json(context),
                },
            ).mappings().first()
        if row is None:
            raise ValueError(f"market_collector_worker_unknown: worker_id={worker}")
        return dict(row)

    def stop_worker(self, *, worker_id: str, state: str = "stopped") -> None:
        worker = str(worker_id or "").strip()
        if not worker:
            return
        with db.session() as session:
            session.execute(
                text(
                    """
                    UPDATE market.collector_worker_state
                    SET state = :state, heartbeat_at = now(), expires_at = now(),
                        active_definition_id = NULL, active_attempt_id = NULL,
                        updated_at = now()
                    WHERE worker_id = :worker_id
                    """
                ),
                {"worker_id": worker, "state": str(state)},
            )

    def list_worker_states(self, *, limit: int = 100) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 1000))
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT workers.*,
                           (workers.expires_at > now()
                            AND workers.state NOT IN ('stopping', 'stopped')) AS alive,
                           now() AS observed_at
                    FROM market.collector_worker_state AS workers
                    ORDER BY alive DESC, workers.heartbeat_at DESC, workers.worker_id
                    LIMIT :limit
                    """
                ),
                {"limit": bounded_limit},
            ).mappings().all()
        return [dict(row) for row in rows]

    def reserve_provider_request(
        self, *, provider: str, minimum_spacing_seconds: float
    ) -> float:
        provider_id = str(provider or "").strip().upper()
        spacing = max(0.0, float(minimum_spacing_seconds))
        if not provider_id:
            raise ValueError("market_collection_rate_budget_invalid: provider is required")
        with db.session() as session:
            now = _as_utc(session.execute(text("SELECT now()" )).scalar_one())
            session.execute(
                text(
                    """
                    INSERT INTO market.provider_rate_budgets (provider, next_request_at)
                    VALUES (:provider, :now)
                    ON CONFLICT (provider) DO NOTHING
                    """
                ),
                {"provider": provider_id, "now": now},
            )
            row = session.execute(
                text(
                    """
                    SELECT next_request_at
                    FROM market.provider_rate_budgets
                    WHERE provider = :provider
                    FOR UPDATE
                    """
                ),
                {"provider": provider_id},
            ).mappings().one()
            next_request = _as_utc(row["next_request_at"])
            slot = max(now, next_request)
            session.execute(
                text(
                    """
                    UPDATE market.provider_rate_budgets
                    SET next_request_at = :next_request_at, updated_at = :now
                    WHERE provider = :provider
                    """
                ),
                {
                    "provider": provider_id,
                    "next_request_at": slot + timedelta(seconds=spacing),
                    "now": now,
                },
            )
        return max(0.0, (slot - now).total_seconds())


market_collection_repo = PostgresMarketCollectionRepository()


__all__ = [
    "CollectionClaim",
    "MarketCollectionOwnershipError",
    "PostgresMarketCollectionRepository",
    "market_collection_repo",
]
