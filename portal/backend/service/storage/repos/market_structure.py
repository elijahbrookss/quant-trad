"""PostgreSQL authority for the bounded market-structure stream plane."""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import text

from market_data.archive import (
    ArchiveObjectAcknowledgement,
    EncodedRawArchive,
    RAW_ARCHIVE_COMPRESSION,
    RAW_ARCHIVE_FORMAT,
    RAW_ARCHIVE_SCHEMA_VERSION,
)
from market_data.book_archive import (
    BOOK_CHECKPOINT_COMPRESSION,
    BOOK_CHECKPOINT_FORMAT,
    EncodedBookCheckpoint,
)
from market_data.contracts import TypedFeatureRecord
from market_data.market_state import (
    BasisFeatureFact,
    BboFeatureFact,
    DepthFeatureFact,
    DerivativeStateFeatureFact,
    ResponseFeatureFact,
    TradeFlowFeatureFact,
)
from market_data.order_book import (
    BOOK_CHECKPOINT_SCHEMA_VERSION,
    BOOK_RECONSTRUCTION_VERSION,
    BookCheckpointFact,
    BookLifecycle,
    BookSourcePosition,
    BookValidityIntervalVersion,
    L2MutationBatchFact,
    L2SnapshotFact,
)
from market_data.structure import (
    MarketSide,
    MarketTradeFact,
    MarketTradeRecord,
    ProductContract,
    RawStreamRecord,
    TradeCoverageIntervalVersion,
    TradeDeliveryKind,
    TradeFlowAggregateFact,
    TradeFlowAggregateRecord,
)

from ._shared import db

from .market_lifecycle import market_storage_lifecycle_repository


class MarketStructureOwnershipError(RuntimeError):
    """Raised when a stale or expired stream worker tries to mutate state."""


class MarketTradeConflictError(RuntimeError):
    """Raised when one provider trade ID arrives with divergent material."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _json(value: Mapping[str, Any] | Sequence[Any] | None) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, separators=(",", ":"), default=str)


def _stable_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _token_hash(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _version_id(prefix: str, payload: Mapping[str, Any]) -> str:
    return f"{prefix}_{_stable_hash(payload)}"


@dataclass(frozen=True)
class StreamClaim:
    definition_id: str
    definition_generation: int
    source_id: int
    series_id: int
    provider: str
    venue: str
    provider_product_id: str
    channels: tuple[str, ...]
    auth_mode: str
    contract_version: str
    max_spool_bytes: int
    max_segment_bytes: int
    config: Mapping[str, Any]
    owner_id: str
    lease_token: str
    lease_generation: int
    lease_expires_at: datetime
    session_id: str

    def public_dict(self) -> dict[str, Any]:
        return {
            "definition_id": self.definition_id,
            "definition_generation": self.definition_generation,
            "source_id": self.source_id,
            "series_id": self.series_id,
            "provider": self.provider,
            "venue": self.venue,
            "provider_product_id": self.provider_product_id,
            "channels": list(self.channels),
            "auth_mode": self.auth_mode,
            "contract_version": self.contract_version,
            "max_spool_bytes": self.max_spool_bytes,
            "max_segment_bytes": self.max_segment_bytes,
            "owner_id": self.owner_id,
            "lease_generation": self.lease_generation,
            "lease_expires_at": self.lease_expires_at.isoformat(),
            "session_id": self.session_id,
        }


@dataclass(frozen=True)
class ArchiveCommitResult:
    manifest_id: str
    inserted_manifest: bool
    inserted_mapping_count: int
    mapped_record_count: int


@dataclass(frozen=True)
class TradeIngestionOutcome:
    requested_count: int
    inserted_count: int
    noop_count: int
    max_commit_seq: int
    records: tuple[MarketTradeRecord, ...]


@dataclass(frozen=True)
class AggregateIngestionOutcome:
    inserted_count: int
    noop_count: int
    max_commit_seq: int
    records: tuple[TradeFlowAggregateRecord, ...]


@dataclass(frozen=True)
class BookIngestionOutcome:
    inserted_snapshot_count: int
    noop_snapshot_count: int
    inserted_batch_count: int
    noop_batch_count: int
    inserted_validity_count: int
    max_commit_seq: int

@dataclass(frozen=True)
class FeatureIngestionOutcome:
    inserted_count: int
    noop_count: int
    max_commit_seq: int
    material_hashes: tuple[str, ...]



def _persist_feature_revision(
    session,
    *,
    table_name: str,
    time_column: str,
    prefix: str,
    identity: Mapping[str, Any],
    values: Mapping[str, Any],
    json_columns: Sequence[str] = (),
) -> tuple[bool, int]:
    """Append one typed revision or prove exact material idempotency."""

    identifiers = (table_name, time_column, *identity.keys(), *values.keys())
    if any(not str(name).replace("_", "").isalnum() for name in identifiers):
        raise ValueError("market_feature_storage_invalid: unsafe SQL identifier")
    if "series_id" not in identity or "material_hash" not in values:
        raise ValueError("market_feature_storage_invalid: identity/material hash required")
    session.execute(
        text("SELECT pg_advisory_xact_lock(:series_id)"),
        {"series_id": int(identity["series_id"])},
    )
    where_sql = " AND ".join(f"{name} = :identity_{name}" for name in identity)
    identity_params = {f"identity_{name}": value for name, value in identity.items()}
    existing = session.execute(
        text(
            f"SELECT revision, material_hash FROM market.{table_name} "
            f"WHERE {where_sql} ORDER BY revision DESC LIMIT 1 FOR UPDATE"
        ),
        identity_params,
    ).mappings().first()
    material_hash = str(values["material_hash"])
    if existing is not None and str(existing["material_hash"]) == material_hash:
        return False, 0
    revision = int(existing["revision"]) + 1 if existing is not None else 1
    version_id = _version_id(
        prefix,
        {
            "schema_version": "market.typed_feature_revision.v1",
            "table_name": table_name,
            "identity": dict(identity),
            "revision": revision,
            "material_hash": material_hash,
        },
    )
    provenance_hash = _stable_hash(
        {
            "schema_version": "market.typed_feature_provenance.v1",
            "table_name": table_name,
            "identity": dict(identity),
            "material_hash": material_hash,
            "input_fingerprint": values.get("input_fingerprint"),
        }
    )
    row = {
        "id": version_id,
        **dict(identity),
        "revision": revision,
        **dict(values),
        "provenance_hash": provenance_hash,
        "quality": _json({}),
    }
    columns = tuple(row)
    json_names = set(json_columns) | {"quality"}
    placeholders = ", ".join(
        f"CAST(:{name} AS jsonb)" if name in json_names else f":{name}"
        for name in columns
    )
    commit_seq = int(
        session.execute(
            text(
                f"INSERT INTO market.{table_name} ({', '.join(columns)}) "
                f"VALUES ({placeholders}) RETURNING market_commit_seq"
            ),
            row,
        ).scalar_one()
    )
    return True, commit_seq


def _require_book_state_source(
    session,
    *,
    series_id: int,
    position: Mapping[str, Any],
    validity_interval_id: str,
    state_hash: str,
) -> None:
    """Require a canonical archive-acknowledged snapshot or mutation state."""

    found = session.execute(
        text(
            """
            SELECT 1
            FROM (
                SELECT series_id, connection_epoch, receive_ordinal,
                       event_ordinal, validity_interval_id, state_hash
                FROM market.l2_snapshot_versions
                UNION ALL
                SELECT series_id, connection_epoch, receive_ordinal,
                       event_ordinal, validity_interval_id,
                       after_state_hash AS state_hash
                FROM market.l2_mutation_batches
            ) AS states
            WHERE series_id = :series_id
              AND connection_epoch = :connection_epoch
              AND receive_ordinal = :receive_ordinal
              AND event_ordinal = :event_ordinal
              AND validity_interval_id = :validity_interval_id
              AND state_hash = :state_hash
            LIMIT 1
            """
        ),
        {
            "series_id": int(series_id),
            "connection_epoch": int(position["connection_epoch"]),
            "receive_ordinal": int(position["receive_ordinal"]),
            "event_ordinal": int(position["event_ordinal"]),
            "validity_interval_id": str(validity_interval_id),
            "state_hash": str(state_hash),
        },
    ).scalar_one_or_none()
    if found is None:
        raise ValueError(
            "market_feature_source_incomplete: canonical acknowledged book state is missing"
        )


def _require_material_source(
    session,
    *,
    table_name: str,
    series_id: int,
    material_hash: str,
) -> None:
    if not str(table_name).replace("_", "").isalnum():
        raise ValueError("market_feature_storage_invalid: unsafe source table")
    found = session.execute(
        text(
            f"SELECT 1 FROM market.{table_name} "
            "WHERE series_id = :series_id AND material_hash = :material_hash LIMIT 1"
        ),
        {"series_id": int(series_id), "material_hash": str(material_hash)},
    ).scalar_one_or_none()
    if found is None:
        raise ValueError(
            f"market_feature_source_incomplete: {table_name} source material is missing"
        )

def _read_feature_rows(
    session,
    *,
    table_name: str,
    time_column: str,
    partition_columns: Sequence[str],
    series_id: int,
    start: datetime,
    end: datetime,
    known_at: datetime,
    as_of_commit_seq: Optional[int],
) -> list[Mapping[str, Any]]:
    identifiers = (table_name, time_column, *partition_columns)
    if any(not str(name).replace("_", "").isalnum() for name in identifiers):
        raise ValueError("market_feature_storage_invalid: unsafe read identifier")
    if end <= start:
        raise ValueError("market_feature_read_invalid: end must follow start")
    partition_sql = ", ".join(partition_columns)
    params: dict[str, Any] = {
        "series_id": int(series_id),
        "start": _utc(start),
        "end": _utc(end),
        "known_at": _utc(known_at),
        "as_of_commit_seq": (
            int(as_of_commit_seq) if as_of_commit_seq is not None else None
        ),
    }
    return list(
        session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT rows.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY {partition_sql}
                               ORDER BY revision DESC, market_commit_seq DESC
                           ) AS selected_revision
                    FROM market.{table_name} AS rows
                    WHERE series_id = :series_id
                      AND {time_column} >= :start
                      AND {time_column} < :end
                      AND known_at <= :known_at
                      AND (
                          :as_of_commit_seq IS NULL
                          OR market_commit_seq <= :as_of_commit_seq
                      )
                )
                SELECT * FROM visible
                WHERE selected_revision = 1
                ORDER BY {time_column}
                """
            ),
            params,
        ).mappings().all()
    )


def _book_position_from_material(value: Mapping[str, Any]) -> BookSourcePosition:
    return BookSourcePosition(
        definition_id=str(value["definition_id"]),
        session_id=str(value["session_id"]),
        connection_epoch=int(value["connection_epoch"]),
        provider_product_id=str(value["provider_product_id"]),
        provider_sequence_num=(
            int(value["provider_sequence_num"])
            if value.get("provider_sequence_num") is not None
            else None
        ),
        receive_ordinal=int(value["receive_ordinal"]),
        event_ordinal=int(value["event_ordinal"]),
    )
class PostgresMarketStructureRepository:
    """One transactional authority for stream facts and projections."""

    def upsert_stream_definition(
        self,
        *,
        definition_id: str,
        source_id: int,
        series_id: int,
        provider: str,
        venue: str,
        provider_product_id: str,
        channels: Sequence[str],
        auth_mode: str,
        contract_version: str,
        max_spool_bytes: int,
        max_segment_bytes: int,
        enabled: Optional[bool] = None,
        config: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        normalized_channels = tuple(
            dict.fromkeys(str(channel).strip().lower() for channel in channels if str(channel).strip())
        )
        if normalized_channels not in {
            ("market_trades", "heartbeats"),
            ("level2", "heartbeats"),
        }:
            raise ValueError(
                "market_stream_definition_invalid: supported ordered channels are market_trades,heartbeats or level2,heartbeats"
            )
        normalized_auth = str(auth_mode or "").strip().lower()
        if normalized_auth not in {"public", "authenticated"}:
            raise ValueError("market_stream_definition_invalid: unsupported auth mode")
        spool_bytes = int(max_spool_bytes)
        segment_bytes = int(max_segment_bytes)
        if spool_bytes <= 0 or segment_bytes <= 0 or segment_bytes > spool_bytes:
            raise ValueError("market_stream_definition_invalid: invalid spool bounds")
        identity = {
            "schema_version": "market.stream_definition_identity.v1",
            "source_id": int(source_id),
            "series_id": int(series_id),
            "provider": str(provider).upper(),
            "venue": str(venue).upper(),
            "provider_product_id": str(provider_product_id),
            "channels": list(normalized_channels),
            "contract_version": str(contract_version),
        }
        identity_key = _stable_hash(identity)
        insert_enabled = bool(enabled) if enabled is not None else False
        material = {
            "auth_mode": normalized_auth,
            "max_spool_bytes": spool_bytes,
            "max_segment_bytes": segment_bytes,
            "enabled": insert_enabled,
            "config": dict(config or {}),
        }
        with db.session() as session:
            existing = session.execute(
                text(
                    """
                    SELECT * FROM market.stream_definitions
                    WHERE identity_key = :identity_key
                    FOR UPDATE
                    """
                ),
                {"identity_key": identity_key},
            ).mappings().first()
            if existing is None:
                session.execute(
                    text(
                        """
                        INSERT INTO market.stream_definitions (
                            id, identity_key, source_id, series_id, provider, venue,
                            provider_product_id, channels, auth_mode, contract_version,
                            enabled, max_spool_bytes,
                            max_segment_bytes, generation, config
                        ) VALUES (
                            :id, :identity_key, :source_id, :series_id, :provider,
                            :venue, :product_id, CAST(:channels AS jsonb), :auth_mode,
                            :contract_version, :enabled,
                            :max_spool_bytes, :max_segment_bytes, 1, CAST(:config AS jsonb)
                        )
                        """
                    ),
                    {
                        "id": str(definition_id),
                        "identity_key": identity_key,
                        "source_id": int(source_id),
                        "series_id": int(series_id),
                        "provider": str(provider).upper(),
                        "venue": str(venue).upper(),
                        "product_id": str(provider_product_id),
                        "channels": _json(list(normalized_channels)),
                        "auth_mode": normalized_auth,
                        "contract_version": str(contract_version),
                        "enabled": insert_enabled,
                        "max_spool_bytes": spool_bytes,
                        "max_segment_bytes": segment_bytes,
                        "config": _json(config),
                    },
                )
            else:
                next_config = dict(config or {})
                existing_config = dict(existing["config"] or {})
                for operational_key in ("collector_runtime",):
                    if (
                        operational_key in existing_config
                        and operational_key not in next_config
                    ):
                        next_config[operational_key] = existing_config[operational_key]
                next_enabled = (
                    bool(existing["enabled"]) if enabled is None else bool(enabled)
                )
                material = {
                    **material,
                    "enabled": next_enabled,
                    "config": next_config,
                }
                existing_material = {
                    "auth_mode": existing["auth_mode"],
                    "max_spool_bytes": int(existing["max_spool_bytes"]),
                    "max_segment_bytes": int(existing["max_segment_bytes"]),
                    "enabled": bool(existing["enabled"]),
                    "config": dict(existing["config"] or {}),
                }
                generation = int(existing["generation"]) + (existing_material != material)
                session.execute(
                    text(
                        """
                        UPDATE market.stream_definitions
                        SET auth_mode = :auth_mode, enabled = :enabled,
                            max_spool_bytes = :max_spool_bytes,
                            max_segment_bytes = :max_segment_bytes,
                            generation = :generation, config = CAST(:config AS jsonb),
                            updated_at = now()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": str(existing["id"]),
                        "auth_mode": normalized_auth,
                        "enabled": next_enabled,
                        "max_spool_bytes": spool_bytes,
                        "max_segment_bytes": segment_bytes,
                        "generation": generation,
                        "config": _json(next_config),
                    },
                )
            row = session.execute(
                text("SELECT * FROM market.stream_definitions WHERE identity_key = :identity_key"),
                {"identity_key": identity_key},
            ).mappings().one()
        return dict(row)

    def list_stream_definitions(
        self, *, definition_id: Optional[str] = None
    ) -> list[dict[str, Any]]:
        predicate = "WHERE definitions.id = :definition_id" if definition_id else ""
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT definitions.*, series.instrument_id,
                           series.fact_type AS series_fact_type,
                           leases.owner_id, leases.lease_generation,
                           leases.heartbeat_at, leases.expires_at,
                           CASE WHEN leases.expires_at > now() THEN true ELSE false END AS lease_current
                    FROM market.stream_definitions AS definitions
                    JOIN market.series AS series ON series.id = definitions.series_id
                    LEFT JOIN market.stream_lease_state AS leases
                      ON leases.definition_id = definitions.id
                    {predicate}
                    ORDER BY definitions.id
                    """
                ),
                {"definition_id": definition_id} if definition_id else {},
            ).mappings().all()
        return [dict(row) for row in rows]

    def record_safety_event(
        self,
        *,
        request_id: str,
        scope_type: str,
        scope_id: str,
        event_type: str,
        severity: str,
        actor_id: str,
        reason: str,
        policy_hash: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Append idempotent safety evidence and update the persistent latch."""

        request = str(request_id or "").strip()
        scope = str(scope_type or "").strip().lower()
        scoped_id = str(scope_id or "").strip()
        event = str(event_type or "").strip().lower()
        level = str(severity or "").strip().lower()
        actor = str(actor_id or "").strip()
        explanation = str(reason or "").strip()
        policy = str(policy_hash or "").strip()
        if not all((request, scoped_id, actor, explanation, policy)):
            raise ValueError("collector_safety_event_invalid: required field is empty")
        if scope not in {"global", "fleet", "stream"}:
            raise ValueError("collector_safety_event_invalid: unsupported scope")
        if event not in {"warning", "halted", "acknowledged"}:
            raise ValueError("collector_safety_event_invalid: unsupported event type")
        if level not in {"warning", "critical", "operator"}:
            raise ValueError("collector_safety_event_invalid: unsupported severity")
        occurred_at = datetime.now(UTC)
        evidence_payload = dict(evidence or {})
        evidence_hash = _stable_hash(evidence_payload)
        event_id = _version_id(
            "cse",
            {
                "schema_version": "market.collector_safety_event.v1",
                "request_id": request,
                "scope_type": scope,
                "scope_id": scoped_id,
                "event_type": event,
                "severity": level,
                "actor_id": actor,
                "reason": explanation,
                "policy_hash": policy,
                "evidence_hash": evidence_hash,
            },
        )
        with db.session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO market.collector_safety_events (
                        id, request_id, scope_type, scope_id, event_type,
                        severity, occurred_at, actor_id, reason, policy_hash,
                        evidence_hash, evidence
                    ) VALUES (
                        :id, :request_id, :scope_type, :scope_id, :event_type,
                        :severity, :occurred_at, :actor_id, :reason, :policy_hash,
                        :evidence_hash, CAST(:evidence AS jsonb)
                    ) ON CONFLICT (request_id) DO NOTHING
                    """
                ),
                {
                    "id": event_id,
                    "request_id": request,
                    "scope_type": scope,
                    "scope_id": scoped_id,
                    "event_type": event,
                    "severity": level,
                    "occurred_at": occurred_at,
                    "actor_id": actor,
                    "reason": explanation,
                    "policy_hash": policy,
                    "evidence_hash": evidence_hash,
                    "evidence": _json(evidence_payload),
                },
            )
            stored = session.execute(
                text(
                    """
                    SELECT * FROM market.collector_safety_events
                    WHERE request_id = :request_id
                    """
                ),
                {"request_id": request},
            ).mappings().one()
            stored_identity = {
                "scope_type": str(stored["scope_type"]),
                "scope_id": str(stored["scope_id"]),
                "event_type": str(stored["event_type"]),
                "severity": str(stored["severity"]),
                "actor_id": str(stored["actor_id"]),
                "reason": str(stored["reason"]),
                "policy_hash": str(stored["policy_hash"]),
                "evidence_hash": str(stored["evidence_hash"]),
            }
            requested_identity = {
                "scope_type": scope,
                "scope_id": scoped_id,
                "event_type": event,
                "severity": level,
                "actor_id": actor,
                "reason": explanation,
                "policy_hash": policy,
                "evidence_hash": evidence_hash,
            }
            if stored_identity != requested_identity:
                raise ValueError("collector_safety_request_conflict")
            if event in {"halted", "acknowledged"}:
                active = event == "halted"
                session.execute(
                    text(
                        """
                        INSERT INTO market.collector_safety_state (
                            scope_type, scope_id, active, halt_event_id,
                            acknowledged_event_id, reason, updated_at
                        ) VALUES (
                            :scope_type, :scope_id, :active, :halt_event_id,
                            :acknowledged_event_id, :reason, :updated_at
                        ) ON CONFLICT (scope_type, scope_id) DO UPDATE
                        SET active = EXCLUDED.active,
                            halt_event_id = CASE
                                WHEN EXCLUDED.active THEN EXCLUDED.halt_event_id
                                ELSE market.collector_safety_state.halt_event_id
                            END,
                            acknowledged_event_id = CASE
                                WHEN EXCLUDED.active THEN NULL
                                ELSE EXCLUDED.acknowledged_event_id
                            END,
                            reason = EXCLUDED.reason,
                            updated_at = EXCLUDED.updated_at
                        """
                    ),
                    {
                        "scope_type": scope,
                        "scope_id": scoped_id,
                        "active": active,
                        "halt_event_id": str(stored["id"]) if active else None,
                        "acknowledged_event_id": (
                            None if active else str(stored["id"])
                        ),
                        "reason": explanation,
                        "updated_at": occurred_at,
                    },
                )
            result = session.execute(
                text(
                    """
                    SELECT events.*, states.active
                    FROM market.collector_safety_events AS events
                    LEFT JOIN market.collector_safety_state AS states
                      ON states.scope_type = events.scope_type
                     AND states.scope_id = events.scope_id
                    WHERE events.request_id = :request_id
                    """
                ),
                {"request_id": request},
            ).mappings().one()
        return dict(result)

    def active_safety_halts(
        self,
        *,
        fleet_id: Optional[str] = None,
        definition_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        scopes: list[tuple[str, str]] = [("global", "*")]
        if fleet_id:
            scopes.append(("fleet", str(fleet_id)))
        if definition_id:
            scopes.append(("stream", str(definition_id)))
        clauses = " OR ".join(
            f"(scope_type = :scope_type_{index} AND scope_id = :scope_id_{index})"
            for index in range(len(scopes))
        )
        params: dict[str, Any] = {}
        for index, (scope, scoped_id) in enumerate(scopes):
            params[f"scope_type_{index}"] = scope
            params[f"scope_id_{index}"] = scoped_id
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT * FROM market.collector_safety_state
                    WHERE active AND ({clauses})
                    ORDER BY scope_type, scope_id
                    """
                ),
                params,
            ).mappings().all()
        return [dict(row) for row in rows]

    def list_safety_events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT events.*, states.active
                    FROM market.collector_safety_events AS events
                    LEFT JOIN market.collector_safety_state AS states
                      ON states.scope_type = events.scope_type
                     AND states.scope_id = events.scope_id
                    ORDER BY events.occurred_at DESC, events.id DESC
                    LIMIT :limit
                    """
                ),
                {"limit": max(1, min(int(limit), 1000))},
            ).mappings().all()
        return [dict(row) for row in rows]

    def stream_storage_growth(self, *, definition_id: str) -> dict[str, Any]:
        """Derive recent canonical archive growth without operator estimates."""

        with db.session() as session:
            row = session.execute(
                text(
                    """
                    SELECT COALESCE(sum(byte_count), 0)::bigint AS byte_count,
                           min(acknowledged_at) AS first_at,
                           max(acknowledged_at) AS last_at,
                           now() AS observed_at
                    FROM market.raw_archive_manifests
                    WHERE definition_id = :definition_id
                      AND acknowledged_at >= now() - interval '24 hours'
                    """
                ),
                {"definition_id": str(definition_id)},
            ).mappings().one()
        byte_count = int(row["byte_count"])
        first_at = row["first_at"]
        observed_at = row["observed_at"]
        window_seconds = (
            max((_utc(observed_at) - _utc(first_at)).total_seconds(), 1.0)
            if first_at is not None
            else 0.0
        )
        bytes_per_hour = (
            byte_count * 3600.0 / window_seconds if window_seconds > 0 else 0.0
        )
        return {
            "schema_version": "market.stream_storage_growth.v1",
            "definition_id": str(definition_id),
            "byte_count": byte_count,
            "window_seconds": window_seconds,
            "bytes_per_hour": bytes_per_hour,
            "first_at": first_at,
            "last_at": row["last_at"],
            "observed_at": observed_at,
        }

    def configure_continuous_runtime(
        self,
        *,
        definition_id: str,
        enabled: bool,
        mode: str,
        requested_by: str,
        policy: Mapping[str, Any],
        stop_at: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """Configure worker-owned stream execution."""

        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {
            "validation",
            "continuous",
            "stopped",
            "safety_halted",
        }:
            raise ValueError("market_stream_runtime_invalid: unsupported mode")
        requester = str(requested_by or "").strip()
        if not requester:
            raise ValueError("market_stream_runtime_invalid: requested_by is required")
        stop_time = _utc(stop_at) if stop_at is not None else None
        if normalized_mode == "validation" and stop_time is None:
            raise ValueError(
                "market_stream_runtime_invalid: validation mode requires stop_at"
            )
        if normalized_mode != "validation" and stop_time is not None:
            raise ValueError(
                "market_stream_runtime_invalid: stop_at is validation-only"
            )
        with db.session() as session:
            definition = session.execute(
                text(
                    """
                    SELECT * FROM market.stream_definitions
                    WHERE id = :definition_id
                    FOR UPDATE
                    """
                ),
                {"definition_id": str(definition_id)},
            ).mappings().first()
            if definition is None:
                raise ValueError(
                    f"market_stream_definition_unknown: definition_id={definition_id}"
                )
            config = dict(definition["config"] or {})
            config["collector_runtime"] = {
                "schema_version": "market.continuous_collector_runtime.v1",
                "mode": normalized_mode,
                "requested_by": requester,
                "requested_at": datetime.now(UTC).isoformat(),
                "stop_at": stop_time.isoformat() if stop_time is not None else None,
                "policy": dict(policy),
            }
            row = session.execute(
                text(
                    """
                    UPDATE market.stream_definitions
                    SET enabled = :enabled,
                        config = CAST(:config AS jsonb), updated_at = now()
                    WHERE id = :definition_id
                    RETURNING *
                    """
                ),
                {
                    "definition_id": str(definition_id),
                    "enabled": bool(enabled),
                    "config": _json(config),
                },
            ).mappings().one()
        return dict(row)

    def claim_stream(
        self,
        *,
        definition_id: str,
        owner_id: str,
        lease_seconds: float,
        bounded: bool,
        resume_session_id: Optional[str] = None,
    ) -> StreamClaim:
        owner = str(owner_id or "").strip()
        ttl = float(lease_seconds)
        if not owner or ttl <= 0:
            raise ValueError("market_stream_claim_invalid: owner and positive lease required")
        token = secrets.token_urlsafe(32)
        token_hash = _token_hash(token)
        requested_session_id = str(resume_session_id or "").strip() or None
        session_id = requested_session_id or f"mss_{uuid.uuid4().hex}"
        with db.session() as session:
            definition = session.execute(
                text(
                    """
                    SELECT * FROM market.stream_definitions
                    WHERE id = :definition_id
                    FOR UPDATE
                    """
                ),
                {"definition_id": str(definition_id)},
            ).mappings().first()
            if definition is None:
                raise ValueError(
                    f"market_stream_definition_unknown: definition_id={definition_id}"
                )
            if not bounded and not bool(definition["enabled"]):
                raise ValueError(
                    "market_stream_not_enabled: continuous collection requires an enabled definition"
                )
            if requested_session_id is not None:
                resumable = session.execute(
                    text(
                        """
                        SELECT 1
                        FROM market.stream_session_events
                        WHERE definition_id = :definition_id
                          AND session_id = :session_id
                        LIMIT 1
                        """
                    ),
                    {
                        "definition_id": str(definition_id),
                        "session_id": requested_session_id,
                    },
                ).first()
                if resumable is None:
                    raise ValueError(
                        "market_stream_resume_invalid: session does not belong to definition"
                    )
            prior = session.execute(
                text(
                    """
                    SELECT * FROM market.stream_lease_state
                    WHERE definition_id = :definition_id
                    FOR UPDATE
                    """
                ),
                {"definition_id": str(definition_id)},
            ).mappings().first()
            now = _utc(session.execute(text("SELECT now()")).scalar_one())
            if prior is not None and _utc(prior["expires_at"]) > now:
                raise MarketStructureOwnershipError(
                    "market_stream_claim_conflict: current lease has not expired"
                )
            lease_generation = int(prior["lease_generation"] if prior else 0) + 1
            expires_at = now + timedelta(seconds=ttl)
            session.execute(
                text(
                    """
                    INSERT INTO market.stream_lease_state (
                        definition_id, owner_id, token_hash, lease_generation,
                        claimed_at, heartbeat_at, expires_at
                    ) VALUES (
                        :definition_id, :owner_id, :token_hash, :lease_generation,
                        :now, :now, :expires_at
                    )
                    ON CONFLICT (definition_id) DO UPDATE
                    SET owner_id = EXCLUDED.owner_id,
                        token_hash = EXCLUDED.token_hash,
                        lease_generation = EXCLUDED.lease_generation,
                        claimed_at = EXCLUDED.claimed_at,
                        heartbeat_at = EXCLUDED.heartbeat_at,
                        expires_at = EXCLUDED.expires_at
                    """
                ),
                {
                    "definition_id": str(definition_id),
                    "owner_id": owner,
                    "token_hash": token_hash,
                    "lease_generation": lease_generation,
                    "now": now,
                    "expires_at": expires_at,
                },
            )
        channels = tuple(str(value) for value in definition["channels"])
        return StreamClaim(
            definition_id=str(definition["id"]),
            definition_generation=int(definition["generation"]),
            source_id=int(definition["source_id"]),
            series_id=int(definition["series_id"]),
            provider=str(definition["provider"]),
            venue=str(definition["venue"]),
            provider_product_id=str(definition["provider_product_id"]),
            channels=channels,
            auth_mode=str(definition["auth_mode"]),
            contract_version=str(definition["contract_version"]),
            max_spool_bytes=int(definition["max_spool_bytes"]),
            max_segment_bytes=int(definition["max_segment_bytes"]),
            config=dict(definition["config"] or {}),
            owner_id=owner,
            lease_token=token,
            lease_generation=lease_generation,
            lease_expires_at=expires_at,
            session_id=session_id,
        )

    def next_session_event_ordinal(self, claim: StreamClaim) -> int:
        """Return the next append-only event ordinal for a claimed session."""

        with db.session() as session:
            self._require_fence(session, claim)
            value = session.execute(
                text(
                    """
                    SELECT COALESCE(max(event_ordinal), -1) + 1
                    FROM market.stream_session_events
                    WHERE definition_id = :definition_id
                      AND session_id = :session_id
                    """
                ),
                {
                    "definition_id": claim.definition_id,
                    "session_id": claim.session_id,
                },
            ).scalar_one()
        return int(value)

    @staticmethod
    def _require_fence(session, claim: StreamClaim) -> Mapping[str, Any]:
        row = session.execute(
            text(
                """
                SELECT definitions.generation AS definition_generation,
                       leases.*, leases.expires_at > now() AS lease_current
                FROM market.stream_definitions AS definitions
                JOIN market.stream_lease_state AS leases
                  ON leases.definition_id = definitions.id
                WHERE definitions.id = :definition_id
                FOR UPDATE OF leases
                """
            ),
            {"definition_id": claim.definition_id},
        ).mappings().first()
        if (
            row is None
            or int(row["definition_generation"]) != claim.definition_generation
            or str(row["owner_id"]) != claim.owner_id
            or str(row["token_hash"]) != _token_hash(claim.lease_token)
            or int(row["lease_generation"]) != claim.lease_generation
            or not bool(row["lease_current"])
        ):
            raise MarketStructureOwnershipError(
                "market_stream_ownership_lost: stale worker mutation rejected"
            )
        return row

    def heartbeat(self, claim: StreamClaim, *, lease_seconds: float) -> datetime:
        ttl = float(lease_seconds)
        if ttl <= 0:
            raise ValueError("market_stream_heartbeat_invalid: lease must be positive")
        with db.session() as session:
            self._require_fence(session, claim)
            row = session.execute(
                text(
                    """
                    UPDATE market.stream_lease_state
                    SET heartbeat_at = now(),
                        expires_at = now() + (:ttl * interval '1 second')
                    WHERE definition_id = :definition_id
                    RETURNING expires_at
                    """
                ),
                {"definition_id": claim.definition_id, "ttl": ttl},
            ).one()
        return _utc(row[0])

    def release(self, claim: StreamClaim) -> None:
        with db.session() as session:
            self._require_fence(session, claim)
            session.execute(
                text("DELETE FROM market.stream_lease_state WHERE definition_id = :definition_id"),
                {"definition_id": claim.definition_id},
            )

    def append_session_event(
        self,
        claim: StreamClaim,
        *,
        event_ordinal: int,
        connection_epoch: int,
        event_type: str,
        occurred_at: datetime,
        received_at: Optional[datetime] = None,
        reason: Optional[str] = None,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> str:
        occurred = _utc(occurred_at)
        received = _utc(received_at or occurred)
        known_at = max(occurred, received)
        material = {
            "schema_version": "market.stream_session_event.v1",
            "session_id": claim.session_id,
            "event_ordinal": int(event_ordinal),
            "connection_epoch": int(connection_epoch),
            "event_type": str(event_type),
            "occurred_at": occurred.isoformat(),
            "reason": reason,
            "evidence": dict(evidence or {}),
        }
        evidence_hash = _stable_hash(material)
        event_id = _version_id("mse", material)
        with db.session() as session:
            self._require_fence(session, claim)
            session.execute(
                text(
                    """
                    INSERT INTO market.stream_session_events (
                        id, definition_id, session_id, event_ordinal,
                        connection_epoch, owner_id, lease_generation, event_type,
                        occurred_at, received_at, known_at, reason,
                        evidence_hash, evidence
                    ) VALUES (
                        :id, :definition_id, :session_id, :event_ordinal,
                        :connection_epoch, :owner_id, :lease_generation,
                        :event_type, :occurred_at, :received_at, :known_at,
                        :reason, :evidence_hash, CAST(:evidence AS jsonb)
                    ) ON CONFLICT (session_id, event_ordinal) DO NOTHING
                    """
                ),
                {
                    "id": event_id,
                    "definition_id": claim.definition_id,
                    "session_id": claim.session_id,
                    "event_ordinal": int(event_ordinal),
                    "connection_epoch": int(connection_epoch),
                    "owner_id": claim.owner_id,
                    "lease_generation": claim.lease_generation,
                    "event_type": str(event_type),
                    "occurred_at": occurred,
                    "received_at": received,
                    "known_at": known_at,
                    "reason": reason,
                    "evidence_hash": evidence_hash,
                    "evidence": _json(evidence),
                },
            )
            stored = session.execute(
                text(
                    """
                    SELECT id, evidence_hash FROM market.stream_session_events
                    WHERE session_id = :session_id AND event_ordinal = :event_ordinal
                    """
                ),
                {"session_id": claim.session_id, "event_ordinal": int(event_ordinal)},
            ).mappings().one()
            if stored["evidence_hash"] != evidence_hash:
                raise RuntimeError("market_stream_session_event_conflict")
        return str(stored["id"])

    def register_product_definition(
        self,
        *,
        definition_version_id: str,
        source_id: int,
        instrument_id: str,
        provider_product_id: str,
        product_type: str,
        venue: str,
        status: str,
        base_currency: str,
        quote_currency: str,
        provider_size_unit: str,
        contract_size: Optional[Decimal],
        price_increment: Optional[Decimal],
        base_increment: Optional[Decimal],
        effective_at: datetime,
        received_at: datetime,
        provenance: Mapping[str, Any],
    ) -> str:
        effective = _utc(effective_at)
        received = _utc(received_at)
        material = {
            "schema_version": "market.product_definition.v1",
            "provider_product_id": provider_product_id,
            "product_type": product_type,
            "venue": venue,
            "status": status,
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "provider_size_unit": provider_size_unit,
            "contract_size": str(contract_size) if contract_size is not None else None,
            "price_increment": str(price_increment) if price_increment is not None else None,
            "base_increment": str(base_increment) if base_increment is not None else None,
            "effective_at": effective.isoformat(),
        }
        material_hash = _stable_hash(material)
        provenance_hash = _stable_hash(dict(provenance))
        with db.session() as session:
            existing = session.execute(
                text("SELECT material_hash FROM market.product_definition_versions WHERE id = :id"),
                {"id": definition_version_id},
            ).scalar_one_or_none()
            if existing is not None and existing != material_hash:
                raise RuntimeError("market_product_definition_conflict")
            session.execute(
                text(
                    """
                    INSERT INTO market.product_definition_versions (
                        id, source_id, instrument_id, provider_product_id,
                        product_type, venue, status, base_currency, quote_currency,
                        provider_size_unit, price_increment, base_increment,
                        contract_size, effective_at, received_at, known_at,
                        revision, material_hash, provenance_hash, provenance
                    ) VALUES (
                        :id, :source_id, :instrument_id, :provider_product_id,
                        :product_type, :venue, :status, :base_currency,
                        :quote_currency, :provider_size_unit, :price_increment,
                        :base_increment, :contract_size, :effective_at,
                        :received_at, :known_at, 1, :material_hash,
                        :provenance_hash, CAST(:provenance AS jsonb)
                    ) ON CONFLICT (id) DO NOTHING
                    """
                ),
                {
                    "id": definition_version_id,
                    "source_id": int(source_id),
                    "instrument_id": instrument_id,
                    "provider_product_id": provider_product_id,
                    "product_type": product_type,
                    "venue": venue,
                    "status": status,
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "provider_size_unit": provider_size_unit,
                    "price_increment": price_increment,
                    "base_increment": base_increment,
                    "contract_size": contract_size,
                    "effective_at": effective,
                    "received_at": received,
                    "known_at": received,
                    "material_hash": material_hash,
                    "provenance_hash": provenance_hash,
                    "provenance": _json(provenance),
                },
            )
        return definition_version_id

    def get_product_contract(self, definition_version_id: str) -> ProductContract:
        """Resolve the immutable provider translation contract by exact version."""

        with db.session() as session:
            row = session.execute(
                text(
                    """
                    SELECT id, provider_product_id, provider_size_unit,
                           base_currency, quote_currency, contract_size
                    FROM market.product_definition_versions
                    WHERE id = :id
                    """
                ),
                {"id": str(definition_version_id)},
            ).mappings().first()
        if row is None:
            raise ValueError(
                "market_product_contract_unknown: "
                f"definition_version_id={definition_version_id}"
            )
        return ProductContract(
            provider_product_id=str(row["provider_product_id"]),
            provider_size_unit=str(row["provider_size_unit"]),
            base_currency=str(row["base_currency"]),
            quote_currency=str(row["quote_currency"]),
            product_definition_version_id=str(row["id"]),
            contract_size=(
                Decimal(str(row["contract_size"]))
                if row["contract_size"] is not None
                else None
            ),
        )

    def register_instrument_mapping(
        self,
        *,
        primary_instrument_id: str,
        related_instrument_id: str,
        role: str,
        effective_from: datetime,
        mapping_reason: str,
        mapping_source: str,
    ) -> str:
        effective = _utc(effective_from)
        material = {
            "schema_version": "market.instrument_role_mapping.v1",
            "primary_instrument_id": primary_instrument_id,
            "related_instrument_id": related_instrument_id,
            "role": role,
            "effective_from": effective.isoformat(),
            "mapping_reason": mapping_reason,
            "mapping_source": mapping_source,
        }
        mapping_id = _version_id("mirm", material)
        material_hash = _stable_hash(material)
        with db.session() as session:
            session.execute(
                text(
                    """
                    INSERT INTO market.instrument_role_mapping_versions (
                        id, primary_instrument_id, related_instrument_id, role,
                        mapping_reason, mapping_source, effective_from,
                        received_at, known_at, revision, material_hash,
                        provenance_hash
                    ) VALUES (
                        :id, :primary, :related, :role, :reason, :source,
                        :effective, now(), now(), 1, :material_hash,
                        :provenance_hash
                    ) ON CONFLICT (id) DO NOTHING
                    """
                ),
                {
                    "id": mapping_id,
                    "primary": primary_instrument_id,
                    "related": related_instrument_id,
                    "role": role,
                    "reason": mapping_reason,
                    "source": mapping_source,
                    "effective": effective,
                    "material_hash": material_hash,
                    "provenance_hash": _stable_hash({"mapping_source": mapping_source}),
                },
            )
        return mapping_id

    def commit_archive(
        self,
        claim: StreamClaim,
        *,
        encoded: EncodedRawArchive,
        acknowledgement: ArchiveObjectAcknowledgement,
        records: Sequence[RawStreamRecord],
        uploaded_at: Optional[datetime] = None,
        compaction_source_manifest_ids: Sequence[str] = (),
        _lifecycle_compaction: bool = False,
    ) -> ArchiveCommitResult:
        if not records or encoded.record_count != len(records):
            raise ValueError("market_archive_commit_invalid: record count mismatch")
        if encoded.sha256 != acknowledgement.sha256 or encoded.byte_count != acknowledgement.byte_count:
            raise ValueError("market_archive_commit_invalid: object acknowledgement mismatch")
        source_manifest_ids = tuple(
            dict.fromkeys(str(value) for value in compaction_source_manifest_ids)
        )
        if _lifecycle_compaction and not source_manifest_ids:
            raise ValueError(
                "market_archive_compaction_commit_invalid: lifecycle source set required"
            )
        if source_manifest_ids and len(source_manifest_ids) < 2:
            raise ValueError(
                "market_archive_compaction_commit_invalid: at least two source manifests required"
            )
        if not source_manifest_ids and any(
            record.spool_segment_id != encoded.spool_segment_id for record in records
        ):
            raise ValueError("market_archive_commit_invalid: segment mismatch")
        if any(
            record.definition_id != claim.definition_id
            or record.session_id != records[0].session_id
            or record.connection_epoch != records[0].connection_epoch
            for record in records
        ):
            raise ValueError("market_archive_commit_invalid: record scope mismatch")
        if not source_manifest_ids and records[0].session_id != claim.session_id:
            raise ValueError("market_archive_commit_invalid: claim session mismatch")
        manifest_material = {
            "schema_version": RAW_ARCHIVE_SCHEMA_VERSION,
            "object_uri": acknowledgement.object_uri,
            "object_sha256": acknowledgement.sha256,
            "content_fingerprint": encoded.content_fingerprint,
        }
        manifest_id = _version_id("ram", manifest_material)
        uploaded = _utc(uploaded_at or acknowledgement.acknowledged_at)
        grouped: dict[str, list[RawStreamRecord]] = defaultdict(list)
        for record in records:
            grouped[record.observed_channel].append(record)
        with db.session() as session:
            if _lifecycle_compaction:
                session.execute(
                    text(
                        "SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"
                    ),
                    {"scope": f"market-archive-compaction:{claim.definition_id}"},
                )
            else:
                self._require_fence(session, claim)
            ordered_sources: list[Mapping[str, Any]] = []
            if source_manifest_ids:
                ordered_sources = list(
                    session.execute(
                        text(
                            """
                            SELECT manifests.id, manifests.definition_id,
                                   manifests.session_id, manifests.connection_epoch,
                                   manifests.first_receive_ordinal,
                                   manifests.last_receive_ordinal,
                                   replacements.replacement_manifest_id
                            FROM market.raw_archive_manifests AS manifests
                            LEFT JOIN market.raw_archive_compaction_sources AS replacements
                              ON replacements.source_manifest_id = manifests.id
                            WHERE manifests.id = ANY(:manifest_ids)
                            ORDER BY manifests.first_receive_ordinal, manifests.id
                            """
                        ),
                        {"manifest_ids": list(source_manifest_ids)},
                    ).mappings()
                )
                if len(ordered_sources) != len(source_manifest_ids):
                    raise ValueError(
                        "market_archive_compaction_commit_invalid: source manifest missing"
                    )
                for index, source in enumerate(ordered_sources):
                    if (
                        str(source["definition_id"]) != claim.definition_id
                        or str(source["session_id"]) != records[0].session_id
                        or int(source["connection_epoch"])
                        != records[0].connection_epoch
                    ):
                        raise ValueError(
                            "market_archive_compaction_commit_invalid: source scope mismatch"
                        )
                    prior_replacement = source["replacement_manifest_id"]
                    if prior_replacement is not None and str(prior_replacement) != manifest_id:
                        raise ValueError(
                            "market_archive_compaction_commit_invalid: source already replaced"
                        )
                    if index and (
                        int(ordered_sources[index - 1]["last_receive_ordinal"]) + 1
                        != int(source["first_receive_ordinal"])
                    ):
                        raise ValueError(
                            "market_archive_compaction_commit_invalid: source ranges are not contiguous"
                        )
                source_mappings = session.execute(
                    text(
                        """
                        SELECT mappings.raw_record_id,
                               mappings.raw_frame_sha256,
                               mappings.receive_ordinal
                        FROM market.raw_archive_record_mappings AS mappings
                        WHERE mappings.manifest_id = ANY(:manifest_ids)
                        ORDER BY mappings.receive_ordinal
                        """
                    ),
                    {"manifest_ids": [str(row["id"]) for row in ordered_sources]},
                ).mappings().all()
                expected_source = [
                    (
                        str(row["raw_record_id"]),
                        str(row["raw_frame_sha256"]),
                        int(row["receive_ordinal"]),
                    )
                    for row in source_mappings
                ]
                replacement = [
                    (
                        row.raw_record_id,
                        row.raw_frame_sha256,
                        row.receive_ordinal,
                    )
                    for row in records
                ]
                if expected_source != replacement:
                    raise ValueError(
                        "market_archive_compaction_commit_invalid: replacement evidence differs"
                    )
            inserted_manifest = bool(
                session.execute(
                    text(
                        """
                        INSERT INTO market.raw_archive_manifests (
                            id, definition_id, session_id, connection_epoch,
                            spool_segment_id, object_uri, object_key, format,
                            schema_version, compression, byte_count, record_count,
                            first_receive_ordinal, last_receive_ordinal,
                            first_received_at, last_received_at, uploaded_at,
                            acknowledged_at, object_sha256, content_fingerprint
                        ) VALUES (
                            :id, :definition_id, :session_id, :epoch,
                            :segment_id, :object_uri, :object_key, :format,
                            :schema_version, :compression, :byte_count,
                            :record_count, :first_ordinal, :last_ordinal,
                            :first_received, :last_received, :uploaded,
                            :acknowledged, :sha256, :fingerprint
                        ) ON CONFLICT (id) DO NOTHING
                        RETURNING id
                        """
                    ),
                    {
                        "id": manifest_id,
                        "definition_id": claim.definition_id,
                        "session_id": records[0].session_id,
                        "epoch": records[0].connection_epoch,
                        "segment_id": encoded.spool_segment_id,
                        "object_uri": acknowledgement.object_uri,
                        "object_key": acknowledgement.object_key,
                        "format": RAW_ARCHIVE_FORMAT,
                        "schema_version": RAW_ARCHIVE_SCHEMA_VERSION,
                        "compression": RAW_ARCHIVE_COMPRESSION,
                        "byte_count": encoded.byte_count,
                        "record_count": encoded.record_count,
                        "first_ordinal": encoded.first_receive_ordinal,
                        "last_ordinal": encoded.last_receive_ordinal,
                        "first_received": encoded.first_received_at,
                        "last_received": encoded.last_received_at,
                        "uploaded": uploaded,
                        "acknowledged": acknowledgement.acknowledged_at,
                        "sha256": acknowledgement.sha256,
                        "fingerprint": encoded.content_fingerprint,
                    },
                ).first()
            )
            stored = session.execute(
                text(
                    """
                    SELECT object_sha256, content_fingerprint, record_count
                    FROM market.raw_archive_manifests WHERE id = :id
                    """
                ),
                {"id": manifest_id},
            ).mappings().one()
            if (
                stored["object_sha256"] != acknowledgement.sha256
                or stored["content_fingerprint"] != encoded.content_fingerprint
                or int(stored["record_count"]) != len(records)
            ):
                raise RuntimeError("market_archive_manifest_conflict")
            for channel, channel_records in sorted(grouped.items()):
                sequences: list[int] = []
                message_times: list[datetime] = []
                for record in channel_records:
                    try:
                        payload = json.loads(record.raw_frame)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    sequence = payload.get("sequence_num") if isinstance(payload, Mapping) else None
                    if isinstance(sequence, int):
                        sequences.append(sequence)
                    raw_time = payload.get("timestamp") if isinstance(payload, Mapping) else None
                    if raw_time:
                        try:
                            normalized = str(raw_time).replace("Z", "+00:00")
                            message_times.append(_utc(datetime.fromisoformat(normalized)))
                        except ValueError:
                            pass
                session.execute(
                    text(
                        """
                        INSERT INTO market.raw_archive_ranges (
                            manifest_id, provider_product_id, channel,
                            first_provider_sequence_num, last_provider_sequence_num,
                            min_provider_message_at, max_provider_message_at,
                            min_received_at, max_received_at, record_count, gap_count
                        ) VALUES (
                            :manifest_id, :product_id, :channel, :first_sequence,
                            :last_sequence, :min_message, :max_message,
                            :min_received, :max_received, :record_count, 0
                        ) ON CONFLICT (manifest_id, provider_product_id, channel) DO NOTHING
                        """
                    ),
                    {
                        "manifest_id": manifest_id,
                        "product_id": claim.provider_product_id,
                        "channel": channel,
                        "first_sequence": min(sequences) if sequences else None,
                        "last_sequence": max(sequences) if sequences else None,
                        "min_message": min(message_times) if message_times else None,
                        "max_message": max(message_times) if message_times else None,
                        "min_received": min(item.received_at for item in channel_records),
                        "max_received": max(item.received_at for item in channel_records),
                        "record_count": len(channel_records),
                    },
                )
            inserted_mappings = 0
            for index, record in enumerate(records):
                inserted_mappings += int(
                    session.execute(
                        text(
                            """
                            INSERT INTO market.raw_archive_record_mappings (
                                raw_record_id, manifest_id, spool_segment_id,
                                session_id, connection_epoch, receive_ordinal,
                                object_row_group, object_row_index,
                                raw_frame_sha256, mapped_at, known_at
                            ) VALUES (
                                :raw_record_id, :manifest_id, :segment_id,
                                :session_id, :epoch, :receive_ordinal, 0,
                                :row_index, :sha256, :mapped_at, :known_at
                            ) ON CONFLICT (raw_record_id, manifest_id) DO NOTHING
                            RETURNING raw_record_id
                            """
                        ),
                        {
                            "raw_record_id": record.raw_record_id,
                            "manifest_id": manifest_id,
                            "segment_id": record.spool_segment_id,
                            "session_id": record.session_id,
                            "epoch": record.connection_epoch,
                            "receive_ordinal": record.receive_ordinal,
                            "row_index": index,
                            "sha256": record.raw_frame_sha256,
                            "mapped_at": acknowledgement.acknowledged_at,
                            "known_at": acknowledgement.acknowledged_at,
                        },
                    ).first()
                    is not None
                )
            mapping_rows = session.execute(
                text(
                    """
                    SELECT raw_record_id, raw_frame_sha256
                    FROM market.raw_archive_record_mappings
                    WHERE manifest_id = :manifest_id
                    """
                ),
                {"manifest_id": manifest_id},
            ).mappings().all()
            expected = {record.raw_record_id: record.raw_frame_sha256 for record in records}
            observed = {str(row["raw_record_id"]): str(row["raw_frame_sha256"]) for row in mapping_rows}
            if observed != expected:
                raise RuntimeError("market_archive_mapping_conflict")
            for source_ordinal, source in enumerate(ordered_sources):
                session.execute(
                    text(
                        """
                        INSERT INTO market.raw_archive_compaction_sources (
                            replacement_manifest_id, source_manifest_id,
                            source_ordinal, replacement_content_fingerprint,
                            compacted_at, known_at
                        ) VALUES (
                            :replacement_id, :source_id, :source_ordinal,
                            :fingerprint, :compacted_at, :known_at
                        ) ON CONFLICT (
                            replacement_manifest_id, source_manifest_id
                        ) DO NOTHING
                        """
                    ),
                    {
                        "replacement_id": manifest_id,
                        "source_id": str(source["id"]),
                        "source_ordinal": source_ordinal,
                        "fingerprint": encoded.content_fingerprint,
                        "compacted_at": acknowledgement.acknowledged_at,
                        "known_at": acknowledgement.acknowledged_at,
                    },
                )
        return ArchiveCommitResult(
            manifest_id=manifest_id,
            inserted_manifest=inserted_manifest,
            inserted_mapping_count=inserted_mappings,
            mapped_record_count=len(records),
        )

    def commit_compacted_archive(
        self,
        *,
        definition_id: str,
        encoded: EncodedRawArchive,
        acknowledgement: ArchiveObjectAcknowledgement,
        records: Sequence[RawStreamRecord],
        source_manifest_ids: Sequence[str],
    ) -> ArchiveCommitResult:
        """Commit verified replacement lineage without taking the live stream lease."""

        definitions = self.list_stream_definitions(definition_id=definition_id)
        if len(definitions) != 1:
            raise ValueError(
                "market_archive_compaction_commit_invalid: "
                f"definition_id={definition_id} is unavailable"
            )
        if not records:
            raise ValueError(
                "market_archive_compaction_commit_invalid: replacement is empty"
            )
        definition = definitions[0]
        now = datetime.now(UTC)
        scope = StreamClaim(
            definition_id=str(definition["id"]),
            definition_generation=int(definition["generation"]),
            source_id=int(definition["source_id"]),
            series_id=int(definition["series_id"]),
            provider=str(definition["provider"]),
            venue=str(definition["venue"]),
            provider_product_id=str(definition["provider_product_id"]),
            channels=tuple(definition["channels"]),
            auth_mode=str(definition["auth_mode"]),
            contract_version=str(definition["contract_version"]),
            max_spool_bytes=int(definition["max_spool_bytes"]),
            max_segment_bytes=int(definition["max_segment_bytes"]),
            config=dict(definition.get("config") or {}),
            owner_id="market-storage-lifecycle",
            lease_token="not-a-stream-lease",
            lease_generation=0,
            lease_expires_at=now,
            session_id=records[0].session_id,
        )
        return self.commit_archive(
            scope,
            encoded=encoded,
            acknowledgement=acknowledgement,
            records=records,
            compaction_source_manifest_ids=source_manifest_ids,
            _lifecycle_compaction=True,
        )

    def append_coverage_version(
        self,
        claim: StreamClaim,
        *,
        coverage: TradeCoverageIntervalVersion,
        opening_session_event_id: str,
        closing_session_event_id: Optional[str] = None,
    ) -> str:
        if (
            coverage.definition_id != claim.definition_id
            or coverage.session_id != claim.session_id
            or coverage.provider_product_id != claim.provider_product_id
        ):
            raise ValueError("market_stream_coverage_invalid: claim scope mismatch")
        version_id = _version_id(
            "mscv",
            {"interval_id": coverage.interval_id, "revision": coverage.revision, "material_hash": coverage.material_hash},
        )
        with db.session() as session:
            self._require_fence(session, claim)
            session.execute(
                text(
                    """
                    INSERT INTO market.stream_coverage_interval_versions (
                        id, interval_id, revision, definition_id, session_id,
                        connection_epoch, provider_product_id, channel, status,
                        ordering_assurance, archive_status,
                        opening_session_event_id, opening_raw_record_id,
                        opening_receive_ordinal, opening_effective_at,
                        last_raw_record_id, last_receive_ordinal, last_effective_at,
                        closing_session_event_id, closing_raw_record_id,
                        closing_receive_ordinal, closing_effective_at,
                        first_provider_sequence_num, last_provider_sequence_num,
                        canonicalization_watermark_ordinal,
                        archive_complete_through_ordinal, gap_quality_event_ids,
                        opening_evidence, closing_evidence, material_hash, known_at
                    ) VALUES (
                        :id, :interval_id, :revision, :definition_id, :session_id,
                        :epoch, :product_id, :channel, :status, :assurance,
                        :archive_status, :opening_event, :opening_raw,
                        :opening_ordinal, :opening_effective, :last_raw,
                        :last_ordinal, :last_effective, :closing_event,
                        :closing_raw, :closing_ordinal, :closing_effective,
                        :first_sequence, :last_sequence, :canonical_watermark,
                        :archive_watermark, CAST(:gaps AS jsonb),
                        CAST(:opening_evidence AS jsonb),
                        CAST(:closing_evidence AS jsonb), :material_hash, :known_at
                    ) ON CONFLICT (interval_id, revision) DO NOTHING
                    """
                ),
                {
                    "id": version_id,
                    "interval_id": coverage.interval_id,
                    "revision": coverage.revision,
                    "definition_id": coverage.definition_id,
                    "session_id": coverage.session_id,
                    "epoch": coverage.connection_epoch,
                    "product_id": coverage.provider_product_id,
                    "channel": coverage.channel,
                    "status": coverage.status.value,
                    "assurance": coverage.ordering_assurance.value,
                    "archive_status": coverage.archive_status.value,
                    "opening_event": opening_session_event_id,
                    "opening_raw": coverage.opening_raw_record_id,
                    "opening_ordinal": coverage.opening_receive_ordinal,
                    "opening_effective": coverage.opening_effective_at,
                    "last_raw": coverage.last_raw_record_id,
                    "last_ordinal": coverage.last_receive_ordinal,
                    "last_effective": coverage.last_effective_at,
                    "closing_event": closing_session_event_id,
                    "closing_raw": coverage.closing_raw_record_id,
                    "closing_ordinal": coverage.closing_receive_ordinal,
                    "closing_effective": coverage.closing_effective_at,
                    "first_sequence": coverage.first_provider_sequence_num,
                    "last_sequence": coverage.last_provider_sequence_num,
                    "canonical_watermark": coverage.canonicalization_watermark_ordinal,
                    "archive_watermark": coverage.archive_complete_through_ordinal,
                    "gaps": _json(list(coverage.gap_quality_event_ids)),
                    "opening_evidence": _json(coverage.opening_evidence),
                    "closing_evidence": _json(coverage.closing_evidence),
                    "material_hash": coverage.material_hash,
                    "known_at": coverage.known_at,
                },
            )
            stored = session.execute(
                text(
                    """
                    SELECT id, material_hash FROM market.stream_coverage_interval_versions
                    WHERE interval_id = :interval_id AND revision = :revision
                    """
                ),
                {"interval_id": coverage.interval_id, "revision": coverage.revision},
            ).mappings().one()
            if stored["material_hash"] != coverage.material_hash:
                raise RuntimeError("market_stream_coverage_conflict")
        return str(stored["id"])

    def get_coverage_version(
        self,
        *,
        interval_id: str,
        revision: int,
    ) -> TradeCoverageIntervalVersion:
        """Read one exact immutable coverage revision for frozen replay binding."""

        normalized_id = str(interval_id or "").strip()
        normalized_revision = int(revision)
        if not normalized_id or normalized_revision <= 0:
            raise ValueError("market_stream_coverage_invalid: exact identity is required")
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    SELECT *
                    FROM market.stream_coverage_interval_versions
                    WHERE interval_id = :interval_id
                      AND revision = :revision
                    """
                ),
                {
                    "interval_id": normalized_id,
                    "revision": normalized_revision,
                },
            ).mappings().first()
        if row is None:
            raise ValueError(
                "market_stream_coverage_unknown: "
                f"interval_id={normalized_id} revision={normalized_revision}"
            )
        return _coverage_version(row)

    def close_open_session_coverages(
        self,
        claim: StreamClaim,
        *,
        closing_session_event_id: str,
        reason: str,
    ) -> int:
        """Conservatively close coverage left open by an interrupted worker.

        Recovery never extends the proven interval across collector downtime.  It
        closes each latest open revision at its last already-canonicalized event;
        a later connection must establish a new coverage interval.
        """

        with db.session() as session:
            self._require_fence(session, claim)
            rows = session.execute(
                text(
                    """
                    SELECT DISTINCT ON (interval_id) *
                    FROM market.stream_coverage_interval_versions
                    WHERE definition_id = :definition_id
                      AND session_id = :session_id
                    ORDER BY interval_id, revision DESC
                    """
                ),
                {
                    "definition_id": claim.definition_id,
                    "session_id": claim.session_id,
                },
            ).mappings().all()
        closed = 0
        for row in rows:
            if str(row["status"]) != "open_valid":
                continue
            coverage = TradeCoverageIntervalVersion(
                interval_id=str(row["interval_id"]),
                revision=int(row["revision"]) + 1,
                definition_id=str(row["definition_id"]),
                session_id=str(row["session_id"]),
                connection_epoch=int(row["connection_epoch"]),
                provider_product_id=str(row["provider_product_id"]),
                channel=str(row["channel"]),
                status="closed_valid",
                ordering_assurance=str(row["ordering_assurance"]),
                archive_status=str(row["archive_status"]),
                opening_raw_record_id=str(row["opening_raw_record_id"]),
                opening_receive_ordinal=int(row["opening_receive_ordinal"]),
                opening_effective_at=_utc(row["opening_effective_at"]),
                last_raw_record_id=str(row["last_raw_record_id"]),
                last_receive_ordinal=int(row["last_receive_ordinal"]),
                last_effective_at=_utc(row["last_effective_at"]),
                closing_raw_record_id=str(row["last_raw_record_id"]),
                closing_receive_ordinal=int(row["last_receive_ordinal"]),
                closing_effective_at=_utc(row["last_effective_at"]),
                canonicalization_watermark_ordinal=int(
                    row["canonicalization_watermark_ordinal"]
                ),
                archive_complete_through_ordinal=int(
                    row["archive_complete_through_ordinal"]
                ),
                known_at=datetime.now(UTC),
                first_provider_sequence_num=row["first_provider_sequence_num"],
                last_provider_sequence_num=row["last_provider_sequence_num"],
                gap_quality_event_ids=tuple(row["gap_quality_event_ids"] or ()),
                opening_evidence=dict(row["opening_evidence"] or {}),
                closing_evidence={
                    "reason": str(reason),
                    "collector_restart_recovery": True,
                    "closed_at_last_proven_event": True,
                },
            )
            self.append_coverage_version(
                claim,
                coverage=coverage,
                opening_session_event_id=str(row["opening_session_event_id"]),
                closing_session_event_id=str(closing_session_event_id),
            )
            closed += 1
        return closed

    def record_quality_event(
        self,
        claim: StreamClaim,
        *,
        connection_epoch: int,
        receive_ordinal: int,
        channel: str,
        classification: str,
        reason: str,
        detected_at: datetime,
        raw_record_id: Optional[str] = None,
        coverage_interval_id: Optional[str] = None,
        sequence_before: Optional[int] = None,
        sequence_after: Optional[int] = None,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> str:
        allowed = {
            "sequence_gap", "out_of_order", "duplicate", "divergent_duplicate",
            "heartbeat_gap", "disconnect", "decode_error", "archive_loss",
            "provider_trade_conflict", "canonicalization_lag", "backpressure_stop",
            "book_invalid", "unknown_zero_delete", "update_before_snapshot",
            "resync_snapshot_accepted",
        }
        normalized = str(classification).strip().lower()
        if normalized not in allowed:
            raise ValueError("market_stream_quality_invalid: unsupported classification")
        detected = _utc(detected_at)
        material = {
            "schema_version": "market.stream_quality_event.v1",
            "session_id": claim.session_id,
            "product_id": claim.provider_product_id,
            "channel": channel,
            "receive_ordinal": int(receive_ordinal),
            "classification": normalized,
            "reason": reason,
            "sequence_before": sequence_before,
            "sequence_after": sequence_after,
            "raw_record_id": raw_record_id,
            "evidence": dict(evidence or {}),
        }
        evidence_hash = _stable_hash(material)
        event_id = _version_id("msq", material)
        with db.session() as session:
            self._require_fence(session, claim)
            session.execute(
                text(
                    """
                    INSERT INTO market.stream_quality_events (
                        id, definition_id, session_id, connection_epoch,
                        provider_product_id, channel, receive_ordinal,
                        classification, sequence_before, sequence_after,
                        reason, detected_at, known_at, raw_record_id,
                        coverage_interval_id, series_id, evidence_hash, evidence
                    ) VALUES (
                        :id, :definition_id, :session_id, :epoch, :product_id,
                        :channel, :receive_ordinal, :classification,
                        :sequence_before, :sequence_after, :reason, :detected_at,
                        :known_at, :raw_record_id, :coverage_interval_id,
                        :series_id, :evidence_hash, CAST(:evidence AS jsonb)
                    ) ON CONFLICT (
                        session_id, provider_product_id, channel,
                        receive_ordinal, classification, evidence_hash
                    ) DO NOTHING
                    """
                ),
                {
                    "id": event_id,
                    "definition_id": claim.definition_id,
                    "session_id": claim.session_id,
                    "epoch": int(connection_epoch),
                    "product_id": claim.provider_product_id,
                    "channel": str(channel),
                    "receive_ordinal": int(receive_ordinal),
                    "classification": normalized,
                    "sequence_before": sequence_before,
                    "sequence_after": sequence_after,
                    "reason": str(reason),
                    "detected_at": detected,
                    "known_at": detected,
                    "raw_record_id": raw_record_id,
                    "coverage_interval_id": coverage_interval_id,
                    "series_id": claim.series_id,
                    "evidence_hash": evidence_hash,
                    "evidence": _json(evidence),
                },
            )
        return event_id

    def list_session_quality_events(
        self, *, definition_id: str, session_id: str
    ) -> list[dict[str, Any]]:
        """Return the immutable quality timeline used by deterministic replay."""

        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT *
                    FROM market.stream_quality_events
                    WHERE definition_id = :definition_id
                      AND session_id = :session_id
                    ORDER BY connection_epoch, receive_ordinal,
                             detected_at, classification, id
                    """
                ),
                {
                    "definition_id": str(definition_id),
                    "session_id": str(session_id),
                },
            ).mappings().all()
        return [dict(row) for row in rows]

    def ingest_trades(
        self,
        claim: StreamClaim,
        *,
        facts: Iterable[MarketTradeFact],
        require_archive_mapping: bool = True,
    ) -> TradeIngestionOutcome:
        rows = sorted(
            facts,
            key=lambda item: (
                item.provider_event_time,
                item.provider_sequence_num if item.provider_sequence_num is not None else 2**63,
                item.receive_ordinal,
                item.event_ordinal,
                item.trade_ordinal,
                item.provider_trade_id,
            ),
        )
        inserted: list[MarketTradeRecord] = []
        noop_count = 0
        max_commit_seq = 0
        with db.session() as session:
            self._require_fence(session, claim)
            for fact in rows:
                if fact.provider_product_id != claim.provider_product_id:
                    raise ValueError("market_trade_ingest_invalid: product scope mismatch")
                if require_archive_mapping:
                    mapping = session.execute(
                        text(
                            """
                            SELECT 1 FROM market.raw_archive_record_mappings
                            WHERE raw_record_id = :raw_record_id LIMIT 1
                            """
                        ),
                        {"raw_record_id": fact.raw_record_id},
                    ).first()
                    if mapping is None:
                        raise ValueError(
                            "market_trade_archive_pending: canonical publication requires acknowledged mapping"
                        )
                identity_text = f"{claim.source_id}:{fact.provider_product_id}:{fact.provider_trade_id}"
                session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
                    {"identity": identity_text},
                )
                identity = session.execute(
                    text(
                        """
                        SELECT * FROM market.market_trade_identities
                        WHERE source_id = :source_id
                          AND provider_product_id = :product_id
                          AND provider_trade_id = :trade_id
                        """
                    ),
                    {
                        "source_id": claim.source_id,
                        "product_id": fact.provider_product_id,
                        "trade_id": fact.provider_trade_id,
                    },
                ).mappings().first()
                if identity is not None:
                    if identity["first_material_hash"] != fact.material_hash:
                        raise MarketTradeConflictError(
                            "market_trade_conflict: same provider trade ID has divergent material "
                            f"product_id={fact.provider_product_id} trade_id={fact.provider_trade_id}"
                        )
                    noop_count += 1
                    continue
                version_material = {
                    "source_id": claim.source_id,
                    "provider_product_id": fact.provider_product_id,
                    "provider_trade_id": fact.provider_trade_id,
                    "revision": 1,
                    "material_hash": fact.material_hash,
                }
                version_id = _version_id("mtv", version_material)
                provenance_hash = _stable_hash(
                    {
                        "schema_version": "market.trade_provenance.v1",
                        "raw_record_id": fact.raw_record_id,
                        "coverage_interval_id": fact.coverage_interval_id,
                        "connection_epoch": fact.connection_epoch,
                        "receive_ordinal": fact.receive_ordinal,
                    }
                )
                result = session.execute(
                    text(
                        """
                        INSERT INTO market.market_trade_versions (
                            id, source_id, series_id, revision,
                            provider_product_id, provider_trade_id, delivery_kind,
                            price, provider_size, provider_size_unit, maker_side,
                            aggressor_side, aggressor_transform_version,
                            contract_quantity, base_quantity, quote_notional,
                            base_currency, quote_currency,
                            product_definition_version_id, provider_event_time,
                            provider_message_time, received_at, accepted_at,
                            known_at, provider_sequence_num, connection_epoch,
                            receive_ordinal, event_ordinal, trade_ordinal,
                            raw_record_id, coverage_interval_id, material_hash,
                            row_hash, provenance_hash, quality
                        ) VALUES (
                            :id, :source_id, :series_id, 1, :product_id,
                            :trade_id, :delivery_kind, :price, :provider_size,
                            :size_unit, :maker_side, :aggressor_side,
                            :transform_version, :contract_quantity,
                            :base_quantity, :quote_notional, :base_currency,
                            :quote_currency, :product_definition_id,
                            :event_time, :message_time, :received_at,
                            :accepted_at, :known_at, :sequence_num, :epoch,
                            :receive_ordinal, :event_ordinal, :trade_ordinal,
                            :raw_record_id, :coverage_interval_id, :material_hash,
                            :row_hash, :provenance_hash, '{}'::jsonb
                        ) RETURNING market_commit_seq
                        """
                    ),
                    {
                        "id": version_id,
                        "source_id": claim.source_id,
                        "series_id": claim.series_id,
                        "product_id": fact.provider_product_id,
                        "trade_id": fact.provider_trade_id,
                        "delivery_kind": fact.delivery_kind.value,
                        "price": fact.price,
                        "provider_size": fact.provider_size,
                        "size_unit": fact.provider_size_unit.value,
                        "maker_side": fact.maker_side.value,
                        "aggressor_side": fact.aggressor_side.value if fact.aggressor_side else None,
                        "transform_version": fact.aggressor_transform_version,
                        "contract_quantity": fact.contract_quantity,
                        "base_quantity": fact.base_quantity,
                        "quote_notional": fact.quote_notional,
                        "base_currency": fact.base_currency,
                        "quote_currency": fact.quote_currency,
                        "product_definition_id": fact.product_definition_version_id,
                        "event_time": fact.provider_event_time,
                        "message_time": fact.provider_message_time,
                        "received_at": fact.received_at,
                        "accepted_at": fact.accepted_at,
                        "known_at": fact.known_at,
                        "sequence_num": fact.provider_sequence_num,
                        "epoch": fact.connection_epoch,
                        "receive_ordinal": fact.receive_ordinal,
                        "event_ordinal": fact.event_ordinal,
                        "trade_ordinal": fact.trade_ordinal,
                        "raw_record_id": fact.raw_record_id,
                        "coverage_interval_id": fact.coverage_interval_id,
                        "material_hash": fact.material_hash,
                        "row_hash": fact.row_hash,
                        "provenance_hash": provenance_hash,
                    },
                ).one()
                commit_seq = int(result[0])
                session.execute(
                    text(
                        """
                        INSERT INTO market.market_trade_identities (
                            id, source_id, provider_product_id, provider_trade_id,
                            first_material_hash, first_version_id, created_at
                        ) VALUES (
                            :id, :source_id, :product_id, :trade_id,
                            :material_hash, :version_id, now()
                        )
                        """
                    ),
                    {
                        "id": _version_id("mti", {"identity": identity_text}),
                        "source_id": claim.source_id,
                        "product_id": fact.provider_product_id,
                        "trade_id": fact.provider_trade_id,
                        "material_hash": fact.material_hash,
                        "version_id": version_id,
                    },
                )
                max_commit_seq = max(max_commit_seq, commit_seq)
                inserted.append(
                    MarketTradeRecord(
                        version_id=version_id,
                        series_id=claim.series_id,
                        source_id=claim.source_id,
                        revision=1,
                        market_commit_seq=commit_seq,
                        provenance_hash=provenance_hash,
                        quality={},
                        fact=fact,
                    )
                )
        return TradeIngestionOutcome(
            requested_count=len(rows),
            inserted_count=len(inserted),
            noop_count=noop_count,
            max_commit_seq=max_commit_seq,
            records=tuple(inserted),
        )

    def read_trades(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
    ) -> list[MarketTradeRecord]:
        predicates = [
            "series_id = :series_id",
            "provider_event_time >= :start",
            "provider_event_time < :end",
        ]
        params: dict[str, Any] = {
            "series_id": int(series_id),
            "start": _utc(start),
            "end": _utc(end),
        }
        if as_of_commit_seq is not None:
            predicates.append("market_commit_seq <= :commit_seq")
            params["commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("known_at <= :known_at")
            params["known_at"] = _utc(known_at_lte)
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT * FROM (
                        SELECT versions.*,
                               row_number() OVER (
                                   PARTITION BY source_id, provider_product_id, provider_trade_id
                                   ORDER BY revision DESC, market_commit_seq DESC
                               ) AS selected_revision
                        FROM market.market_trade_versions AS versions
                        WHERE {' AND '.join(predicates)}
                    ) AS selected
                    WHERE selected_revision = 1
                    ORDER BY provider_event_time, provider_sequence_num NULLS LAST,
                             receive_ordinal, event_ordinal, trade_ordinal,
                             provider_trade_id
                    """
                ),
                params,
            ).mappings().all()
        return [_trade_record(row) for row in rows]

    def ingest_aggregates(
        self,
        *,
        series_id: int,
        facts: Iterable[TradeFlowAggregateFact],
        aggregation_version: str = "market.trade_flow.v1",
    ) -> AggregateIngestionOutcome:
        inserted: list[TradeFlowAggregateRecord] = []
        noop_count = 0
        max_commit_seq = 0
        ordered = sorted(facts, key=lambda fact: (fact.bucket_start, fact.interval_seconds, fact.known_at))
        with db.session() as session:
            for fact in ordered:
                session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
                    {"identity": f"{series_id}:{fact.interval_seconds}:{fact.bucket_start.isoformat()}:{aggregation_version}"},
                )
                prior = session.execute(
                    text(
                        """
                        SELECT revision, material_hash
                        FROM market.trade_flow_aggregate_versions
                        WHERE series_id = :series_id
                          AND interval_seconds = :interval
                          AND bucket_start = :bucket_start
                          AND aggregation_version = :version
                        ORDER BY revision DESC LIMIT 1
                        """
                    ),
                    {
                        "series_id": int(series_id),
                        "interval": fact.interval_seconds,
                        "bucket_start": fact.bucket_start,
                        "version": aggregation_version,
                    },
                ).mappings().first()
                if prior is not None and prior["material_hash"] == fact.material_hash:
                    noop_count += 1
                    continue
                revision = int(prior["revision"] if prior else 0) + 1
                version_id = _version_id(
                    "tfav",
                    {
                        "series_id": int(series_id),
                        "interval": fact.interval_seconds,
                        "bucket_start": fact.bucket_start.isoformat(),
                        "version": aggregation_version,
                        "revision": revision,
                        "material_hash": fact.material_hash,
                    },
                )
                provenance_hash = _stable_hash(
                    {
                        "schema_version": "market.trade_flow_provenance.v1",
                        "input_fingerprint": fact.input_fingerprint,
                        "coverage_interval_id": fact.coverage_interval_id,
                        "coverage_revision": fact.coverage_revision,
                    }
                )
                result = session.execute(
                    text(
                        """
                        INSERT INTO market.trade_flow_aggregate_versions (
                            id, series_id, interval_seconds, bucket_start,
                            bucket_end, aggregation_version, revision,
                            trade_count, maker_buy_count, maker_sell_count,
                            aggressor_buy_count, aggressor_sell_count,
                            contract_volume, base_volume, quote_notional,
                            maker_buy_base_volume, maker_sell_base_volume,
                            aggressor_buy_base_volume, aggressor_sell_base_volume,
                            cvd_delta, cvd_unit, open_price, high_price, low_price,
                            close_price, first_trade_id, last_trade_id,
                            first_receive_ordinal, last_receive_ordinal,
                            coverage_interval_id, coverage_revision,
                            aggregate_complete, archive_complete,
                            canonicalization_complete, late_trade_count, known_at,
                            input_fingerprint, material_hash, provenance_hash, quality
                        ) VALUES (
                            :id, :series_id, :interval, :bucket_start, :bucket_end,
                            :version, :revision, :trade_count, :maker_buy_count,
                            :maker_sell_count, :aggressor_buy_count,
                            :aggressor_sell_count, :contract_volume, :base_volume,
                            :quote_notional, :maker_buy_base_volume,
                            :maker_sell_base_volume, :aggressor_buy_base_volume,
                            :aggressor_sell_base_volume, :cvd_delta, :cvd_unit,
                            :open_price, :high_price, :low_price, :close_price,
                            :first_trade_id, :last_trade_id,
                            :first_receive_ordinal, :last_receive_ordinal,
                            :coverage_interval_id, :coverage_revision,
                            :aggregate_complete, :archive_complete,
                            :canonicalization_complete, :late_trade_count,
                            :known_at, :input_fingerprint, :material_hash,
                            :provenance_hash, '{}'::jsonb
                        ) RETURNING market_commit_seq
                        """
                    ),
                    {
                        "id": version_id,
                        "series_id": int(series_id),
                        "interval": fact.interval_seconds,
                        "bucket_start": fact.bucket_start,
                        "bucket_end": fact.bucket_end,
                        "version": aggregation_version,
                        "revision": revision,
                        "trade_count": fact.trade_count,
                        "maker_buy_count": fact.maker_buy_count,
                        "maker_sell_count": fact.maker_sell_count,
                        "aggressor_buy_count": fact.aggressor_buy_count,
                        "aggressor_sell_count": fact.aggressor_sell_count,
                        "contract_volume": fact.contract_volume,
                        "base_volume": fact.base_volume,
                        "quote_notional": fact.quote_notional,
                        "maker_buy_base_volume": fact.maker_buy_base_volume,
                        "maker_sell_base_volume": fact.maker_sell_base_volume,
                        "aggressor_buy_base_volume": fact.aggressor_buy_base_volume,
                        "aggressor_sell_base_volume": fact.aggressor_sell_base_volume,
                        "cvd_delta": fact.cvd_delta,
                        "cvd_unit": fact.cvd_unit,
                        "open_price": fact.open_price,
                        "high_price": fact.high_price,
                        "low_price": fact.low_price,
                        "close_price": fact.close_price,
                        "first_trade_id": fact.first_trade_id,
                        "last_trade_id": fact.last_trade_id,
                        "first_receive_ordinal": fact.first_receive_ordinal,
                        "last_receive_ordinal": fact.last_receive_ordinal,
                        "coverage_interval_id": fact.coverage_interval_id,
                        "coverage_revision": fact.coverage_revision,
                        "aggregate_complete": fact.aggregate_complete,
                        "archive_complete": fact.archive_complete,
                        "canonicalization_complete": fact.canonicalization_complete,
                        "late_trade_count": fact.late_trade_count,
                        "known_at": fact.known_at,
                        "input_fingerprint": fact.input_fingerprint,
                        "material_hash": fact.material_hash,
                        "provenance_hash": provenance_hash,
                    },
                ).one()
                commit_seq = int(result[0])
                max_commit_seq = max(max_commit_seq, commit_seq)
                inserted.append(
                    TradeFlowAggregateRecord(
                        version_id=version_id,
                        series_id=int(series_id),
                        revision=revision,
                        market_commit_seq=commit_seq,
                        aggregation_version=aggregation_version,
                        provenance_hash=provenance_hash,
                        quality={},
                        fact=fact,
                    )
                )
        return AggregateIngestionOutcome(
            inserted_count=len(inserted),
            noop_count=noop_count,
            max_commit_seq=max_commit_seq,
            records=tuple(inserted),
        )

    def read_aggregates(
        self,
        *,
        series_id: int,
        interval_seconds: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
    ) -> list[TradeFlowAggregateRecord]:
        predicates = [
            "series_id = :series_id", "interval_seconds = :interval",
            "bucket_start >= :start", "bucket_start < :end",
        ]
        params: dict[str, Any] = {
            "series_id": int(series_id), "interval": int(interval_seconds),
            "start": _utc(start), "end": _utc(end),
        }
        if as_of_commit_seq is not None:
            predicates.append("market_commit_seq <= :commit_seq")
            params["commit_seq"] = int(as_of_commit_seq)
        if known_at_lte is not None:
            predicates.append("known_at <= :known_at")
            params["known_at"] = _utc(known_at_lte)
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT * FROM (
                        SELECT versions.*,
                               row_number() OVER (
                                   PARTITION BY series_id, interval_seconds,
                                                bucket_start, aggregation_version
                                   ORDER BY revision DESC, market_commit_seq DESC
                               ) AS selected_revision
                        FROM market.trade_flow_aggregate_versions AS versions
                        WHERE {' AND '.join(predicates)}
                    ) AS selected
                    WHERE selected_revision = 1
                    ORDER BY bucket_start
                    """
                ),
                params,
            ).mappings().all()
        return [_aggregate_record(row) for row in rows]

    def ingest_book_facts(
        self,
        claim: StreamClaim,
        *,
        snapshots: Iterable[L2SnapshotFact],
        batches: Iterable[L2MutationBatchFact],
        validity_versions: Iterable[BookValidityIntervalVersion],
        lifecycle: BookLifecycle,
        final_validity_interval_id: Optional[str],
        checkpoint_id: Optional[str],
        final_state_hash: Optional[str],
        final_connection_epoch: int,
        final_receive_ordinal: int,
        final_event_ordinal: int,
        final_sequence_num: Optional[int],
    ) -> BookIngestionOutcome:
        """Persist accepted typed book evidence only after raw archive mapping."""

        snapshot_rows = sorted(
            snapshots,
            key=lambda row: (
                row.event.position.receive_ordinal,
                row.event.position.event_ordinal,
            ),
        )
        batch_rows = sorted(
            batches,
            key=lambda row: (
                row.event.position.receive_ordinal,
                row.event.position.event_ordinal,
            ),
        )
        validity_rows = sorted(
            validity_versions, key=lambda row: (row.interval_id, row.revision)
        )
        inserted_snapshots = 0
        noop_snapshots = 0
        inserted_batches = 0
        noop_batches = 0
        inserted_validity = 0
        max_commit_seq = 0
        with db.session() as session:
            self._require_fence(session, claim)
            for fact in snapshot_rows:
                if fact.series_id != claim.series_id:
                    raise ValueError("market_l2_ingest_invalid: snapshot series mismatch")
                mapped = session.execute(
                    text(
                        "SELECT 1 FROM market.raw_archive_record_mappings "
                        "WHERE raw_record_id = :raw_record_id LIMIT 1"
                    ),
                    {"raw_record_id": fact.event.raw_record_id},
                ).scalar_one_or_none()
                if mapped is None:
                    raise ValueError(
                        "market_l2_archive_incomplete: snapshot raw record is not acknowledged"
                    )
                existing = session.execute(
                    text(
                        "SELECT event_material_hash, state_hash "
                        "FROM market.l2_snapshot_versions "
                        "WHERE id = :id AND effective_at = :effective_at"
                    ),
                    {"id": fact.snapshot_id, "effective_at": fact.event.effective_at},
                ).mappings().first()
                if existing is not None:
                    if (
                        str(existing["event_material_hash"]) != fact.event.material_hash
                        or str(existing["state_hash"]) != fact.state_hash
                    ):
                        raise RuntimeError("market_l2_snapshot_conflict")
                    noop_snapshots += 1
                    continue
                provenance_hash = _stable_hash(
                    {
                        "schema_version": "market.l2_snapshot_provenance.v1",
                        "raw_record_id": fact.event.raw_record_id,
                        "event_material_hash": fact.event.material_hash,
                        "validity_interval_id": fact.validity_interval_id,
                    }
                )
                commit_seq = int(
                    session.execute(
                        text(
                            """
                            INSERT INTO market.l2_snapshot_versions (
                                id, series_id, definition_id, session_id,
                                connection_epoch, provider_product_id,
                                product_definition_version_id,
                                provider_sequence_num, receive_ordinal,
                                event_ordinal, effective_at, provider_message_time,
                                received_at, accepted_at, known_at, level_count,
                                state_hash, event_material_hash, raw_record_id,
                                validity_interval_id, provenance_hash, quality
                            ) VALUES (
                                :id, :series_id, :definition_id, :session_id,
                                :epoch, :product_id, :product_definition_id,
                                :sequence_num, :receive_ordinal, :event_ordinal,
                                :effective_at, :message_time, :received_at,
                                :accepted_at, :known_at, :level_count,
                                :state_hash, :event_hash, :raw_record_id,
                                :validity_interval_id, :provenance_hash,
                                '{}'::jsonb
                            ) RETURNING market_commit_seq
                            """
                        ),
                        {
                            "id": fact.snapshot_id,
                            "series_id": fact.series_id,
                            "definition_id": fact.event.position.definition_id,
                            "session_id": fact.event.position.session_id,
                            "epoch": fact.event.position.connection_epoch,
                            "product_id": fact.event.position.provider_product_id,
                            "product_definition_id": fact.event.product_definition_version_id,
                            "sequence_num": fact.event.position.provider_sequence_num,
                            "receive_ordinal": fact.event.position.receive_ordinal,
                            "event_ordinal": fact.event.position.event_ordinal,
                            "effective_at": fact.event.effective_at,
                            "message_time": fact.event.provider_message_time,
                            "received_at": fact.event.received_at,
                            "accepted_at": fact.event.accepted_at,
                            "known_at": fact.event.known_at,
                            "level_count": len(fact.bids) + len(fact.asks),
                            "state_hash": fact.state_hash,
                            "event_hash": fact.event.material_hash,
                            "raw_record_id": fact.event.raw_record_id,
                            "validity_interval_id": fact.validity_interval_id,
                            "provenance_hash": provenance_hash,
                        },
                    ).scalar_one()
                )
                mutation_by_level = {
                    (row.side.value, row.price): row for row in fact.event.mutations
                }
                level_ordinal = 0
                level_parameters: list[dict[str, Any]] = []
                for side, levels in (("bid", fact.bids), ("ask", fact.asks)):
                    for price, quantity in levels:
                        mutation = mutation_by_level[(side, price)]
                        level_parameters.append(
                            {
                                "side": side,
                                "price": str(price),
                                "quantity": str(quantity),
                                "size_unit": mutation.provider_size_unit.value,
                                "event_time": mutation.provider_event_time.isoformat(),
                                "level_ordinal": level_ordinal,
                            },
                        )
                        level_ordinal += 1
                session.execute(
                    text(
                        """
                        INSERT INTO market.l2_snapshot_levels (
                            snapshot_version_id, snapshot_effective_at,
                            side, price, quantity, provider_size_unit,
                            provider_event_time, level_ordinal
                        )
                        SELECT :snapshot_id, :effective_at, levels.side,
                               levels.price, levels.quantity, levels.size_unit,
                               levels.event_time, levels.level_ordinal
                        FROM jsonb_to_recordset(CAST(:levels AS jsonb)) AS levels(
                            side text,
                            price numeric,
                            quantity numeric,
                            size_unit text,
                            event_time timestamptz,
                            level_ordinal bigint
                        )
                        """
                    ),
                    {
                        "snapshot_id": fact.snapshot_id,
                        "effective_at": fact.event.effective_at,
                        "levels": _json(level_parameters),
                    },
                )
                inserted_snapshots += 1
                max_commit_seq = max(max_commit_seq, commit_seq)

            for fact in batch_rows:
                if fact.series_id != claim.series_id:
                    raise ValueError("market_l2_ingest_invalid: batch series mismatch")
                mapped = session.execute(
                    text(
                        "SELECT 1 FROM market.raw_archive_record_mappings "
                        "WHERE raw_record_id = :raw_record_id LIMIT 1"
                    ),
                    {"raw_record_id": fact.event.raw_record_id},
                ).scalar_one_or_none()
                if mapped is None:
                    raise ValueError(
                        "market_l2_archive_incomplete: batch raw record is not acknowledged"
                    )
                existing = session.execute(
                    text(
                        "SELECT event_material_hash, before_state_hash, after_state_hash "
                        "FROM market.l2_mutation_batches "
                        "WHERE id = :id AND effective_at = :effective_at"
                    ),
                    {"id": fact.batch_id, "effective_at": fact.event.effective_at},
                ).mappings().first()
                if existing is not None:
                    if (
                        str(existing["event_material_hash"]) != fact.event.material_hash
                        or str(existing["before_state_hash"]) != fact.before_state_hash
                        or str(existing["after_state_hash"]) != fact.after_state_hash
                    ):
                        raise RuntimeError("market_l2_batch_conflict")
                    noop_batches += 1
                    continue
                provenance_hash = _stable_hash(
                    {
                        "schema_version": "market.l2_mutation_provenance.v1",
                        "raw_record_id": fact.event.raw_record_id,
                        "event_material_hash": fact.event.material_hash,
                        "validity_interval_id": fact.validity_interval_id,
                    }
                )
                commit_seq = int(
                    session.execute(
                        text(
                            """
                            INSERT INTO market.l2_mutation_batches (
                                id, series_id, definition_id, session_id,
                                connection_epoch, provider_product_id,
                                product_definition_version_id,
                                provider_sequence_num, receive_ordinal,
                                event_ordinal, effective_at, provider_message_time,
                                received_at, accepted_at, known_at,
                                mutation_count, before_state_hash,
                                after_state_hash, event_material_hash,
                                raw_record_id, validity_interval_id,
                                unknown_zero_delete_count, provenance_hash,
                                quality
                            ) VALUES (
                                :id, :series_id, :definition_id, :session_id,
                                :epoch, :product_id, :product_definition_id,
                                :sequence_num, :receive_ordinal, :event_ordinal,
                                :effective_at, :message_time, :received_at,
                                :accepted_at, :known_at, :mutation_count,
                                :before_hash, :after_hash, :event_hash,
                                :raw_record_id, :validity_interval_id,
                                :unknown_delete_count, :provenance_hash,
                                '{}'::jsonb
                            ) RETURNING market_commit_seq
                            """
                        ),
                        {
                            "id": fact.batch_id,
                            "series_id": fact.series_id,
                            "definition_id": fact.event.position.definition_id,
                            "session_id": fact.event.position.session_id,
                            "epoch": fact.event.position.connection_epoch,
                            "product_id": fact.event.position.provider_product_id,
                            "product_definition_id": fact.event.product_definition_version_id,
                            "sequence_num": fact.event.position.provider_sequence_num,
                            "receive_ordinal": fact.event.position.receive_ordinal,
                            "event_ordinal": fact.event.position.event_ordinal,
                            "effective_at": fact.event.effective_at,
                            "message_time": fact.event.provider_message_time,
                            "received_at": fact.event.received_at,
                            "accepted_at": fact.event.accepted_at,
                            "known_at": fact.event.known_at,
                            "mutation_count": len(fact.event.mutations),
                            "before_hash": fact.before_state_hash,
                            "after_hash": fact.after_state_hash,
                            "event_hash": fact.event.material_hash,
                            "raw_record_id": fact.event.raw_record_id,
                            "validity_interval_id": fact.validity_interval_id,
                            "unknown_delete_count": fact.unknown_zero_delete_count,
                            "provenance_hash": provenance_hash,
                        },
                    ).scalar_one()
                )
                mutation_parameters = [
                    {
                        "ordinal": mutation.mutation_ordinal,
                        "side": mutation.side.value,
                        "price": str(mutation.price),
                        "quantity": str(mutation.new_quantity),
                        "size_unit": mutation.provider_size_unit.value,
                        "event_time": mutation.provider_event_time.isoformat(),
                    }
                    for mutation in fact.event.mutations
                ]
                session.execute(
                    text(
                        """
                        INSERT INTO market.l2_mutations (
                            batch_id, batch_effective_at, mutation_ordinal,
                            side, price, new_quantity, provider_size_unit,
                            provider_event_time
                        )
                        SELECT :batch_id, :effective_at, mutations.ordinal,
                               mutations.side, mutations.price,
                               mutations.quantity, mutations.size_unit,
                               mutations.event_time
                        FROM jsonb_to_recordset(CAST(:mutations AS jsonb)) AS mutations(
                            ordinal integer,
                            side text,
                            price numeric,
                            quantity numeric,
                            size_unit text,
                            event_time timestamptz
                        )
                        """
                    ),
                    {
                        "batch_id": fact.batch_id,
                        "effective_at": fact.event.effective_at,
                        "mutations": _json(mutation_parameters),
                    },
                )
                inserted_batches += 1
                max_commit_seq = max(max_commit_seq, commit_seq)

            for validity in validity_rows:
                inserted_validity += int(
                    bool(
                        session.execute(
                            text(
                                """
                                INSERT INTO market.book_validity_interval_versions (
                                    id, interval_id, revision, series_id, status,
                                    ordering_assurance, reconstruction_version,
                                    opening_snapshot_id, opening_session_id,
                                    opening_connection_epoch, opening_sequence_num,
                                    opening_receive_ordinal, opening_event_ordinal,
                                    opening_effective_at, opening_known_at,
                                    last_session_id, last_connection_epoch,
                                    last_sequence_num, last_receive_ordinal,
                                    last_event_ordinal, last_valid_effective_at,
                                    last_state_hash, closing_session_id,
                                    closing_connection_epoch, closing_sequence_num,
                                    closing_receive_ordinal, closing_event_ordinal,
                                    closing_effective_at, closing_quality_hash,
                                    reason, known_at
                                ) VALUES (
                                    :id, :interval_id, :revision, :series_id,
                                    :status, :ordering_assurance,
                                    :reconstruction_version, :opening_snapshot_id,
                                    :opening_session_id, :opening_epoch,
                                    :opening_sequence, :opening_receive,
                                    :opening_event, :opening_effective,
                                    :opening_known, :last_session_id, :last_epoch,
                                    :last_sequence, :last_receive, :last_event,
                                    :last_effective, :last_state_hash,
                                    :closing_session_id, :closing_epoch,
                                    :closing_sequence, :closing_receive,
                                    :closing_event, :closing_effective,
                                    :closing_quality_hash, :reason, :known_at
                                ) ON CONFLICT (id) DO NOTHING RETURNING id
                                """
                            ),
                            {
                                "id": validity.version_id,
                                "interval_id": validity.interval_id,
                                "revision": validity.revision,
                                "series_id": validity.series_id,
                                "status": validity.status.value,
                                "ordering_assurance": validity.ordering_assurance.value,
                                "reconstruction_version": BOOK_RECONSTRUCTION_VERSION,
                                "opening_snapshot_id": validity.opening_snapshot_id,
                                "opening_session_id": validity.opening_position.session_id,
                                "opening_epoch": validity.opening_position.connection_epoch,
                                "opening_sequence": validity.opening_position.provider_sequence_num,
                                "opening_receive": validity.opening_position.receive_ordinal,
                                "opening_event": validity.opening_position.event_ordinal,
                                "opening_effective": validity.opening_effective_at,
                                "opening_known": validity.opening_known_at,
                                "last_session_id": validity.last_valid_position.session_id,
                                "last_epoch": validity.last_valid_position.connection_epoch,
                                "last_sequence": validity.last_valid_position.provider_sequence_num,
                                "last_receive": validity.last_valid_position.receive_ordinal,
                                "last_event": validity.last_valid_position.event_ordinal,
                                "last_effective": validity.last_valid_effective_at,
                                "last_state_hash": validity.last_state_hash,
                                "closing_session_id": (
                                    validity.closing_position.session_id
                                    if validity.closing_position else None
                                ),
                                "closing_epoch": (
                                    validity.closing_position.connection_epoch
                                    if validity.closing_position else None
                                ),
                                "closing_sequence": (
                                    validity.closing_position.provider_sequence_num
                                    if validity.closing_position else None
                                ),
                                "closing_receive": (
                                    validity.closing_position.receive_ordinal
                                    if validity.closing_position else None
                                ),
                                "closing_event": (
                                    validity.closing_position.event_ordinal
                                    if validity.closing_position else None
                                ),
                                "closing_effective": validity.closing_effective_at,
                                "closing_quality_hash": validity.closing_quality_hash,
                                "reason": validity.reason,
                                "known_at": validity.known_at,
                            },
                        ).scalar_one_or_none()
                    )
                )

            session.execute(
                text(
                    """
                    INSERT INTO market.book_reconstruction_state (
                        series_id, definition_id, session_id, connection_epoch,
                        lifecycle, validity_interval_id, checkpoint_id,
                        provider_sequence_num, receive_ordinal, event_ordinal,
                        state_hash, lease_generation, updated_at
                    ) VALUES (
                        :series_id, :definition_id, :session_id, :connection_epoch,
                        :lifecycle, :validity_interval_id, :checkpoint_id,
                        :sequence_num, :receive_ordinal, :event_ordinal,
                        :state_hash, :lease_generation, now()
                    ) ON CONFLICT (series_id) DO UPDATE SET
                        definition_id = EXCLUDED.definition_id,
                        session_id = EXCLUDED.session_id,
                        connection_epoch = EXCLUDED.connection_epoch,
                        lifecycle = EXCLUDED.lifecycle,
                        validity_interval_id = EXCLUDED.validity_interval_id,
                        checkpoint_id = EXCLUDED.checkpoint_id,
                        provider_sequence_num = EXCLUDED.provider_sequence_num,
                        receive_ordinal = EXCLUDED.receive_ordinal,
                        event_ordinal = EXCLUDED.event_ordinal,
                        state_hash = EXCLUDED.state_hash,
                        lease_generation = EXCLUDED.lease_generation,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {
                    "series_id": claim.series_id,
                    "definition_id": claim.definition_id,
                    "session_id": claim.session_id,
                    "connection_epoch": int(final_connection_epoch),
                    "lifecycle": lifecycle.value,
                    "validity_interval_id": final_validity_interval_id,
                    "checkpoint_id": checkpoint_id,
                    "sequence_num": final_sequence_num,
                    "receive_ordinal": int(final_receive_ordinal),
                    "event_ordinal": int(final_event_ordinal),
                    "state_hash": final_state_hash,
                    "lease_generation": claim.lease_generation,
                },
            )
        return BookIngestionOutcome(
            inserted_snapshot_count=inserted_snapshots,
            noop_snapshot_count=noop_snapshots,
            inserted_batch_count=inserted_batches,
            noop_batch_count=noop_batches,
            inserted_validity_count=inserted_validity,
            max_commit_seq=max_commit_seq,
        )

    def commit_book_checkpoint(
        self,
        claim: StreamClaim,
        *,
        checkpoint: BookCheckpointFact,
        encoded: EncodedBookCheckpoint,
        acknowledgement: ArchiveObjectAcknowledgement,
        source_manifest_ids: Sequence[str],
    ) -> bool:
        if (
            checkpoint.checkpoint_id != encoded.checkpoint_id
            or encoded.sha256 != acknowledgement.sha256
            or encoded.byte_count != acknowledgement.byte_count
            or encoded.content_fingerprint != checkpoint.content_fingerprint
        ):
            raise ValueError("market_book_checkpoint_commit_invalid: acknowledgement mismatch")
        manifests = tuple(dict.fromkeys(str(value) for value in source_manifest_ids))
        if not manifests:
            raise ValueError("market_book_checkpoint_commit_invalid: source manifests required")
        with db.session() as session:
            self._require_fence(session, claim)
            source_count = int(
                session.execute(
                    text(
                        "SELECT count(*) FROM market.raw_archive_manifests "
                        "WHERE definition_id = :definition_id AND id = ANY(:manifest_ids)"
                    ),
                    {
                        "definition_id": claim.definition_id,
                        "manifest_ids": list(manifests),
                    },
                ).scalar_one()
            )
            if source_count != len(manifests):
                raise ValueError(
                    "market_book_checkpoint_archive_incomplete: source manifest is not acknowledged"
                )
            inserted = session.execute(
                text(
                    """
                    INSERT INTO market.book_checkpoint_manifests (
                        id, series_id, validity_interval_id,
                        reconstruction_version, product_definition_version_id,
                        provider_size_unit, session_id, connection_epoch,
                        provider_sequence_num, receive_ordinal, event_ordinal,
                        effective_at, known_at, state_hash, object_uri,
                        object_key, object_sha256, content_fingerprint, format,
                        compression, schema_version, byte_count, level_count,
                        bid_level_count, ask_level_count,
                        mutation_count_since_prior, source_manifest_ids,
                        acknowledged_at
                    ) VALUES (
                        :id, :series_id, :validity_interval_id,
                        :reconstruction_version, :product_definition_id,
                        :size_unit, :session_id, :epoch, :sequence_num,
                        :receive_ordinal, :event_ordinal, :effective_at,
                        :known_at, :state_hash, :object_uri, :object_key,
                        :object_sha256, :content_fingerprint, :format,
                        :compression, :schema_version, :byte_count,
                        :level_count, :bid_count, :ask_count, :mutation_count,
                        CAST(:manifest_ids AS jsonb), :acknowledged_at
                    ) ON CONFLICT (id) DO NOTHING RETURNING id
                    """
                ),
                {
                    "id": checkpoint.checkpoint_id,
                    "series_id": checkpoint.series_id,
                    "validity_interval_id": checkpoint.validity_interval_id,
                    "reconstruction_version": BOOK_RECONSTRUCTION_VERSION,
                    "product_definition_id": checkpoint.product_definition_version_id,
                    "size_unit": checkpoint.provider_size_unit.value,
                    "session_id": checkpoint.source_position.session_id,
                    "epoch": checkpoint.source_position.connection_epoch,
                    "sequence_num": checkpoint.source_position.provider_sequence_num,
                    "receive_ordinal": checkpoint.source_position.receive_ordinal,
                    "event_ordinal": checkpoint.source_position.event_ordinal,
                    "effective_at": checkpoint.effective_at,
                    "known_at": checkpoint.known_at,
                    "state_hash": checkpoint.state_hash,
                    "object_uri": acknowledgement.object_uri,
                    "object_key": acknowledgement.object_key,
                    "object_sha256": acknowledgement.sha256,
                    "content_fingerprint": encoded.content_fingerprint,
                    "format": BOOK_CHECKPOINT_FORMAT,
                    "compression": BOOK_CHECKPOINT_COMPRESSION,
                    "schema_version": BOOK_CHECKPOINT_SCHEMA_VERSION,
                    "byte_count": encoded.byte_count,
                    "level_count": encoded.level_count,
                    "bid_count": len(checkpoint.bids),
                    "ask_count": len(checkpoint.asks),
                    "mutation_count": checkpoint.mutation_count_since_prior,
                    "manifest_ids": _json(list(manifests)),
                    "acknowledged_at": acknowledgement.acknowledged_at,
                },
            ).scalar_one_or_none()
        return inserted is not None

    def link_book_quality_event(
        self,
        claim: StreamClaim,
        *,
        quality_event_id: str,
        validity_interval_id: str,
        link_role: str,
        known_at: datetime,
    ) -> None:
        with db.session() as session:
            self._require_fence(session, claim)
            session.execute(
                text(
                    """
                    INSERT INTO market.book_quality_event_links (
                        quality_event_id, validity_interval_id, link_role,
                        known_at
                    ) VALUES (
                        :quality_event_id, :interval_id, :link_role, :known_at
                    ) ON CONFLICT DO NOTHING
                    """
                ),
                {
                    "quality_event_id": quality_event_id,
                    "interval_id": validity_interval_id,
                    "link_role": str(link_role),
                    "known_at": _utc(known_at),
                },
            )

    def reconcile_book_replay(
        self,
        *,
        definition_id: str,
        session_id: str,
        snapshot_ids: Sequence[str],
        batch_ids: Sequence[str],
        final_state_hash: Optional[str],
    ) -> dict[str, Any]:
        with db.session() as session:
            stored_snapshots = tuple(
                str(value)
                for value in session.execute(
                    text(
                        "SELECT id FROM market.l2_snapshot_versions "
                        "WHERE definition_id = :definition_id AND session_id = :session_id "
                        "ORDER BY receive_ordinal, event_ordinal"
                    ),
                    {"definition_id": definition_id, "session_id": session_id},
                ).scalars()
            )
            stored_batches = tuple(
                str(value)
                for value in session.execute(
                    text(
                        "SELECT id FROM market.l2_mutation_batches "
                        "WHERE definition_id = :definition_id AND session_id = :session_id "
                        "ORDER BY receive_ordinal, event_ordinal"
                    ),
                    {"definition_id": definition_id, "session_id": session_id},
                ).scalars()
            )
            stored_final_hash = session.execute(
                text(
                    "SELECT state_hash FROM market.book_reconstruction_state "
                    "WHERE definition_id = :definition_id AND session_id = :session_id"
                ),
                {"definition_id": definition_id, "session_id": session_id},
            ).scalar_one_or_none()
        requested_snapshots = tuple(snapshot_ids)
        requested_batches = tuple(batch_ids)
        equal = (
            requested_snapshots == stored_snapshots
            and requested_batches == stored_batches
            and final_state_hash == stored_final_hash
        )
        if not equal:
            raise RuntimeError("market_book_replay_reconciliation_failed")
        return {
            "schema_version": "market.book_replay_reconciliation.v1",
            "snapshot_count": len(stored_snapshots),
            "batch_count": len(stored_batches),
            "final_state_hash": stored_final_hash,
            "equal": True,
        }

    def list_sessions(
        self, *, definition_id: Optional[str] = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        predicate = "WHERE definition_id = :definition_id" if definition_id else ""
        params = {"definition_id": definition_id, "limit": max(1, min(int(limit), 1000))}
        with db.session() as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT * FROM market.stream_session_events
                    {predicate}
                    ORDER BY occurred_at DESC, session_id, event_ordinal DESC
                    LIMIT :limit
                    """
                ),
                params,
            ).mappings().all()
        return [dict(row) for row in rows]

    def continuous_validation_evidence(
        self, *, definition_id: str, session_id: str
    ) -> dict[str, Any]:
        """Derive admission evidence from canonical session/archive state."""

        with db.session() as session:
            lifecycle = session.execute(
                text(
                    """
                    SELECT
                        min(occurred_at) FILTER (
                            WHERE event_type IN ('connected', 'reconnected')
                        ) AS started_at,
                        max(occurred_at) FILTER (
                            WHERE event_type = 'continuous_capture_stopped'
                        ) AS stopped_at,
                        count(*) FILTER (
                            WHERE event_type = 'continuous_capture_stopped'
                        ) AS stopped_count,
                        count(*) FILTER (
                            WHERE event_type IN ('failed', 'interrupted')
                        ) AS failure_count,
                        count(*) FILTER (
                            WHERE event_type = 'provider_disconnected'
                        ) AS disconnect_count,
                        count(*) FILTER (
                            WHERE event_type = 'reconnected'
                        ) AS reconnect_count,
                        count(*) FILTER (
                            WHERE event_type = 'segment_canonicalized'
                        ) AS canonicalized_segment_count,
                        count(*) FILTER (
                            WHERE event_type = 'collector_restart_recovery_completed'
                        ) AS recovery_count
                    FROM market.stream_session_events
                    WHERE definition_id = :definition_id
                      AND session_id = :session_id
                    """
                ),
                {
                    "definition_id": str(definition_id),
                    "session_id": str(session_id),
                },
            ).mappings().one()
            archive = session.execute(
                text(
                    """
                    SELECT count(*) AS manifest_count,
                           COALESCE(sum(byte_count), 0) AS archive_bytes,
                           COALESCE(sum(record_count), 0) AS archived_records,
                           COALESCE(sum(
                               record_count - (
                                   SELECT count(*)
                                   FROM market.raw_archive_record_mappings mappings
                                   WHERE mappings.manifest_id = manifests.id
                               )
                           ), 0) AS mapping_lag_records
                    FROM market.raw_archive_manifests manifests
                    WHERE definition_id = :definition_id
                      AND session_id = :session_id
                    """
                ),
                {
                    "definition_id": str(definition_id),
                    "session_id": str(session_id),
                },
            ).mappings().one()
            coverage = session.execute(
                text(
                    """
                    SELECT count(*) AS interval_count,
                           count(*) FILTER (WHERE status = 'open_valid') AS open_count,
                           count(*) FILTER (WHERE status = 'invalid') AS invalid_count
                    FROM (
                        SELECT DISTINCT ON (interval_id) interval_id, status
                        FROM market.stream_coverage_interval_versions
                        WHERE definition_id = :definition_id
                          AND session_id = :session_id
                        ORDER BY interval_id, revision DESC
                    ) latest
                    """
                ),
                {
                    "definition_id": str(definition_id),
                    "session_id": str(session_id),
                },
            ).mappings().one()
            quality_rows = session.execute(
                text(
                    """
                    SELECT classification, count(*) AS count
                    FROM market.stream_quality_events
                    WHERE definition_id = :definition_id
                      AND session_id = :session_id
                    GROUP BY classification
                    ORDER BY classification
                    """
                ),
                {
                    "definition_id": str(definition_id),
                    "session_id": str(session_id),
                },
            ).mappings().all()
        started_at = lifecycle["started_at"]
        stopped_at = lifecycle["stopped_at"]
        derived_at = datetime.now(UTC)
        session_active = started_at is not None and stopped_at is None
        duration_end = stopped_at or (derived_at if session_active else None)
        duration_seconds = (
            max(0.0, (_utc(duration_end) - _utc(started_at)).total_seconds())
            if started_at is not None and duration_end is not None
            else 0.0
        )
        blockers: list[str] = []
        if duration_seconds < 24 * 3600:
            blockers.append("continuous_duration_below_24_hours")
        if session_active:
            blockers.append("validation_session_still_active")
        elif int(lifecycle["stopped_count"] or 0) != 1:
            blockers.append("graceful_terminal_event_missing_or_duplicated")
        if int(lifecycle["failure_count"] or 0):
            blockers.append("failed_or_interrupted_event_present")
        if int(lifecycle["disconnect_count"] or 0) > int(
            lifecycle["reconnect_count"] or 0
        ):
            blockers.append("disconnect_without_reconnect")
        if int(archive["manifest_count"] or 0) <= 0:
            blockers.append("archive_manifest_missing")
        if int(archive["mapping_lag_records"] or 0):
            blockers.append("archive_mapping_lag_present")
        if int(coverage["interval_count"] or 0) <= 0:
            blockers.append("coverage_evidence_missing")
        if not session_active and int(coverage["open_count"] or 0):
            blockers.append("coverage_interval_still_open")
        if int(coverage["invalid_count"] or 0):
            blockers.append("invalid_coverage_interval_present")
        return {
            "schema_version": "market.continuous_validation_evidence.v1",
            "definition_id": str(definition_id),
            "validation_session_id": str(session_id),
            "started_at": _utc(started_at).isoformat() if started_at else None,
            "stopped_at": _utc(stopped_at).isoformat() if stopped_at else None,
            "session_active": session_active,
            "duration_seconds": duration_seconds,
            "continuous_capture_completed": not blockers,
            "blockers": blockers,
            "disconnect_count": int(lifecycle["disconnect_count"] or 0),
            "reconnect_count": int(lifecycle["reconnect_count"] or 0),
            "canonicalized_segment_count": int(
                lifecycle["canonicalized_segment_count"] or 0
            ),
            "recovery_count": int(lifecycle["recovery_count"] or 0),
            "manifest_count": int(archive["manifest_count"] or 0),
            "archive_bytes": int(archive["archive_bytes"] or 0),
            "archived_records": int(archive["archived_records"] or 0),
            "mapping_lag_records": int(archive["mapping_lag_records"] or 0),
            "coverage_interval_count": int(coverage["interval_count"] or 0),
            "quality_counts": {
                str(row["classification"]): int(row["count"])
                for row in quality_rows
            },
            "derived_at": derived_at.isoformat(),
        }

    def list_archive_status_summaries(self) -> dict[str, dict[str, Any]]:
        """Return operator-list status for every definition in one DB round trip."""

        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT definitions.id AS definition_id,
                           COALESCE(manifests.manifest_count, 0) AS manifest_count,
                           COALESCE(manifests.archive_bytes, 0) AS archive_bytes,
                           COALESCE(manifests.archived_records, 0) AS archived_records,
                           COALESCE(manifests.mapping_lag_records, 0) AS archive_mapping_lag_records,
                           manifests.last_acknowledged_at,
                           COALESCE(quality.counts, '{}'::jsonb) AS quality_counts,
                           COALESCE(coverage.intervals, '[]'::jsonb) AS coverage_intervals,
                           COALESCE(book.intervals, '[]'::jsonb) AS book_validity_intervals,
                           COALESCE(datasets.coverage, '[]'::jsonb) AS dataset_coverage
                    FROM market.stream_definitions AS definitions
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS manifest_count,
                               COALESCE(sum(manifest.byte_count), 0) AS archive_bytes,
                               COALESCE(sum(manifest.record_count), 0) AS archived_records,
                               max(manifest.acknowledged_at) AS last_acknowledged_at,
                               COALESCE(sum(
                                   manifest.record_count - (
                                       SELECT count(*)
                                       FROM market.raw_archive_record_mappings AS mapping
                                       WHERE mapping.manifest_id = manifest.id
                                   )
                               ), 0) AS mapping_lag_records
                        FROM market.raw_archive_manifests AS manifest
                        WHERE manifest.definition_id = definitions.id
                    ) AS manifests ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT jsonb_object_agg(grouped.classification, grouped.count) AS counts
                        FROM (
                            SELECT classification, count(*) AS count
                            FROM market.stream_quality_events
                            WHERE definition_id = definitions.id
                            GROUP BY classification
                        ) AS grouped
                    ) AS quality ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT jsonb_agg(to_jsonb(selected) ORDER BY selected.known_at DESC) AS intervals
                        FROM (
                            SELECT DISTINCT ON (interval_id)
                                   interval_id, revision, status, ordering_assurance,
                                   archive_status, opening_effective_at,
                                   closing_effective_at, known_at
                            FROM market.stream_coverage_interval_versions
                            WHERE definition_id = definitions.id
                            ORDER BY interval_id, revision DESC
                        ) AS selected
                    ) AS coverage ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT jsonb_agg(to_jsonb(selected) ORDER BY selected.known_at DESC) AS intervals
                        FROM (
                            SELECT DISTINCT ON (interval_id)
                                   interval_id, revision, status, ordering_assurance,
                                   opening_effective_at, last_valid_effective_at,
                                   closing_effective_at, last_state_hash, known_at
                            FROM market.book_validity_interval_versions
                            WHERE series_id = definitions.series_id
                            ORDER BY interval_id, revision DESC
                        ) AS selected
                    ) AS book ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT jsonb_agg(to_jsonb(scoped) ORDER BY scoped.range_end DESC) AS coverage
                        FROM (
                            SELECT dataset_series.dataset_id, dataset_series.series_id,
                                   dataset_series.range_start, dataset_series.range_end,
                                   dataset_series.row_count, dataset_series.material_hash,
                                   dataset_series.quality_summary
                            FROM market.dataset_series AS dataset_series
                            WHERE dataset_series.series_id = definitions.series_id
                            ORDER BY dataset_series.range_end DESC
                            LIMIT 25
                        ) AS scoped
                    ) AS datasets ON TRUE
                    ORDER BY definitions.id
                    """
                )
            ).mappings().all()
        return {
            str(row["definition_id"]): {
                "schema_version": "market.stream_archive_status_summary.v1",
                **dict(row),
            }
            for row in rows
        }

    def archive_status(self, *, definition_id: str) -> dict[str, Any]:
        with db.session() as session:
            definition = session.execute(
                text(
                    """
                    SELECT id, series_id, provider_product_id, enabled,
                           max_spool_bytes,
                           max_segment_bytes, config
                    FROM market.stream_definitions
                    WHERE id = :definition_id
                    """
                ),
                {"definition_id": definition_id},
            ).mappings().first()
            if definition is None:
                raise ValueError(
                    f"market_stream_definition_unknown: definition_id={definition_id}"
                )
            aggregate_series_ids = sorted(
                int(value)
                for value in dict(
                    (definition["config"] or {}).get("aggregate_series_ids") or {}
                ).values()
            )
            scoped_series_ids = [int(definition["series_id"]), *aggregate_series_ids]
            manifest = session.execute(
                text(
                    """
                    SELECT count(*) AS manifest_count,
                           COALESCE(sum(byte_count), 0) AS archive_bytes,
                           COALESCE(sum(record_count), 0) AS archived_records,
                           max(acknowledged_at) AS last_acknowledged_at,
                           COALESCE(sum(record_count), 0) - COALESCE(sum(mapped_count), 0)
                             AS mapping_lag_records
                    FROM market.raw_archive_manifests
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS mapped_count
                        FROM market.raw_archive_record_mappings
                        WHERE manifest_id = raw_archive_manifests.id
                    ) AS mappings ON TRUE
                    WHERE definition_id = :definition_id
                    """
                ),
                {"definition_id": definition_id},
            ).mappings().one()
            quality = session.execute(
                text(
                    """
                    SELECT classification, count(*) AS count
                    FROM market.stream_quality_events
                    WHERE definition_id = :definition_id
                    GROUP BY classification ORDER BY classification
                    """
                ),
                {"definition_id": definition_id},
            ).mappings().all()
            coverage = session.execute(
                text(
                    """
                    SELECT DISTINCT ON (interval_id)
                           interval_id, revision, status, ordering_assurance,
                           archive_status, opening_effective_at,
                           closing_effective_at, known_at
                    FROM market.stream_coverage_interval_versions
                    WHERE definition_id = :definition_id
                    ORDER BY interval_id, revision DESC
                    """
                ),
                {"definition_id": definition_id},
            ).mappings().all()
            trade_count = int(
                session.execute(
                    text(
                        """
                        SELECT count(*) FROM market.market_trade_identities
                        WHERE source_id = (
                            SELECT source_id FROM market.stream_definitions
                            WHERE id = :definition_id
                        )
                          AND provider_product_id = :product_id
                        """
                    ),
                    {
                        "definition_id": definition_id,
                        "product_id": str(definition["provider_product_id"]),
                    },
                ).scalar_one()
            )
            aggregates = session.execute(
                text(
                    """
                    SELECT series_id, interval_seconds,
                           count(*) FILTER (WHERE selected_revision = 1) AS bucket_count,
                           count(*) FILTER (
                               WHERE selected_revision = 1 AND aggregate_complete
                           ) AS complete_bucket_count,
                           count(*) FILTER (
                               WHERE selected_revision = 1 AND NOT aggregate_complete
                           ) AS incomplete_bucket_count,
                           max(bucket_end) FILTER (WHERE selected_revision = 1)
                             AS latest_bucket_end
                    FROM (
                        SELECT versions.*,
                               row_number() OVER (
                                   PARTITION BY series_id, interval_seconds,
                                                bucket_start, aggregation_version
                                   ORDER BY revision DESC, market_commit_seq DESC
                               ) AS selected_revision
                        FROM market.trade_flow_aggregate_versions AS versions
                        WHERE series_id = ANY(:series_ids)
                    ) AS selected
                    GROUP BY series_id, interval_seconds
                    ORDER BY interval_seconds, series_id
                    """
                ),
                {"series_ids": aggregate_series_ids or [-1]},
            ).mappings().all()
            book_state = session.execute(
                text(
                    """
                    SELECT state.*,
                           COALESCE(snapshots.snapshot_count, 0) AS snapshot_count,
                           COALESCE(batches.batch_count, 0) AS batch_count,
                           COALESCE(batches.mutation_count, 0) AS mutation_count,
                           COALESCE(checkpoints.checkpoint_count, 0) AS checkpoint_count
                    FROM (SELECT CAST(:series_id AS bigint) AS requested_series_id) AS scope
                    LEFT JOIN market.book_reconstruction_state AS state
                      ON state.series_id = scope.requested_series_id
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS snapshot_count
                        FROM market.l2_snapshot_versions
                        WHERE series_id = scope.requested_series_id
                    ) AS snapshots ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS batch_count,
                               COALESCE(sum(mutation_count), 0) AS mutation_count
                        FROM market.l2_mutation_batches
                        WHERE series_id = scope.requested_series_id
                    ) AS batches ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS checkpoint_count
                        FROM market.book_checkpoint_manifests
                        WHERE series_id = scope.requested_series_id
                    ) AS checkpoints ON TRUE
                    """
                ),
                {"series_id": int(definition["series_id"])},
            ).mappings().one()
            book_validity = session.execute(
                text(
                    """
                    SELECT DISTINCT ON (interval_id)
                           interval_id, revision, status, ordering_assurance,
                           opening_effective_at, last_valid_effective_at,
                           closing_effective_at, last_state_hash, known_at
                    FROM market.book_validity_interval_versions
                    WHERE series_id = :series_id
                    ORDER BY interval_id, revision DESC
                    """
                ),
                {"series_id": int(definition["series_id"])},
            ).mappings().all()
            datasets = session.execute(
                text(
                    """
                    SELECT dataset_series.dataset_id, dataset_series.series_id,
                           dataset_series.range_start, dataset_series.range_end,
                           dataset_series.row_count, dataset_series.material_hash,
                           dataset_series.quality_summary,
                           datasets.dataset_hash, datasets.purpose
                    FROM market.dataset_series AS dataset_series
                    JOIN market.datasets AS datasets
                      ON datasets.id = dataset_series.dataset_id
                    WHERE dataset_series.series_id = ANY(:series_ids)
                    ORDER BY dataset_series.range_end DESC,
                             dataset_series.dataset_id, dataset_series.series_id
                    LIMIT 100
                    """
                ),
                {"series_ids": scoped_series_ids},
            ).mappings().all()
        return {
            "schema_version": "market.stream_archive_status.v1",
            "definition_id": definition_id,
            "manifest_count": int(manifest["manifest_count"]),
            "archive_bytes": int(manifest["archive_bytes"]),
            "archived_records": int(manifest["archived_records"]),
            "archive_mapping_lag_records": int(manifest["mapping_lag_records"]),
            "last_acknowledged_at": manifest["last_acknowledged_at"],
            "canonical_trade_count": trade_count,
            "trade_flow_aggregates": [dict(row) for row in aggregates],
            "book_reconstruction": dict(book_state),
            "book_validity_intervals": [dict(row) for row in book_validity],
            "quality_counts": {str(row["classification"]): int(row["count"]) for row in quality},
            "coverage_intervals": [dict(row) for row in coverage],
            "dataset_coverage": [dict(row) for row in datasets],
            "capacity": {
                "max_spool_bytes": int(definition["max_spool_bytes"]),
                "max_segment_bytes": int(definition["max_segment_bytes"]),
            },
            "continuous_enabled": bool(definition["enabled"]),
        }

    def get_manifest(self, manifest_id: str) -> dict[str, Any]:
        with db.session() as session:
            row = session.execute(
                text("SELECT * FROM market.raw_archive_manifests WHERE id = :id"),
                {"id": manifest_id},
            ).mappings().first()
        if row is None:
            raise ValueError(f"market_archive_manifest_unknown: manifest_id={manifest_id}")
        return dict(row)

    def list_session_manifests(
        self, *, definition_id: str, session_id: str
    ) -> list[dict[str, Any]]:
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT * FROM market.raw_archive_manifests
                    WHERE definition_id = :definition_id
                      AND session_id = :session_id
                      AND NOT EXISTS (
                          SELECT 1
                          FROM market.raw_archive_compaction_sources AS compacted
                          WHERE compacted.source_manifest_id = raw_archive_manifests.id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM market.storage_lifecycle_events AS lifecycle
                          WHERE lifecycle.action = 'archive_expire'
                            AND lifecycle.target_kind = 'raw_manifest'
                            AND lifecycle.target_id = raw_archive_manifests.id
                            AND lifecycle.event_type = 'completed'
                      )
                    ORDER BY connection_epoch, first_receive_ordinal, id
                    """
                ),
                {"definition_id": definition_id, "session_id": session_id},
            ).mappings().all()
        return [dict(row) for row in rows]

    def append_archive_retention_pin_version(
        self,
        *,
        target_kind: str,
        target_id: str,
        owner_kind: str,
        owner_id: str,
        active: bool,
        reason: str,
        effective_at: Optional[datetime] = None,
    ) -> str:
        """Append one explicit pin/release revision; never mutate retention history."""

        normalized_target = str(target_kind or "").strip().lower()
        normalized_target_id = str(target_id or "").strip()
        normalized_owner_kind = str(owner_kind or "").strip().lower()
        normalized_owner_id = str(owner_id or "").strip()
        normalized_reason = str(reason or "").strip()
        if normalized_target not in {"raw_manifest", "book_checkpoint"}:
            raise ValueError("market_archive_retention_pin_invalid: unsupported target")
        if not all(
            (normalized_target_id, normalized_owner_kind, normalized_owner_id, normalized_reason)
        ):
            raise ValueError("market_archive_retention_pin_invalid: identity and reason required")
        pin_id = _version_id(
            "arp",
            {
                "schema_version": "market.archive_retention_pin_identity.v1",
                "target_kind": normalized_target,
                "target_id": normalized_target_id,
                "owner_kind": normalized_owner_kind,
                "owner_id": normalized_owner_id,
            },
        )
        status = "active" if active else "released"
        effective = _utc(effective_at or datetime.now(UTC))
        with db.session() as session:
            market_storage_lifecycle_repository.acquire_dataset_pin_lock(session)
            session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:pin_id, 0))"),
                {"pin_id": pin_id},
            )
            target_table = (
                "market.raw_archive_manifests"
                if normalized_target == "raw_manifest"
                else "market.book_checkpoint_manifests"
            )
            target_exists = session.execute(
                text(f"SELECT 1 FROM {target_table} WHERE id = :target_id"),
                {"target_id": normalized_target_id},
            ).scalar_one_or_none()
            if target_exists is None:
                raise ValueError("market_archive_retention_pin_invalid: target missing")
            expired = bool(
                session.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 "
                        "FROM market.storage_lifecycle_events "
                        "WHERE action = 'archive_expire' "
                        "AND target_kind = :target_kind "
                        "AND target_id = :target_id "
                        "AND event_type = 'completed')"
                    ),
                    {"target_kind": normalized_target, "target_id": normalized_target_id},
                ).scalar_one()
            )
            if active and expired:
                raise ValueError(
                    "market_archive_retention_pin_invalid: target already expired"
                )
            prior = session.execute(
                text(
                    """
                    SELECT id, revision, status, reason
                    FROM market.archive_retention_pin_versions
                    WHERE pin_id = :pin_id
                    ORDER BY revision DESC LIMIT 1
                    """
                ),
                {"pin_id": pin_id},
            ).mappings().first()
            if (
                prior is not None
                and str(prior["status"]) == status
                and str(prior["reason"]) == normalized_reason
            ):
                return str(prior["id"])
            revision = int(prior["revision"] if prior else 0) + 1
            version_id = _version_id(
                "arpv",
                {
                    "pin_id": pin_id,
                    "revision": revision,
                    "status": status,
                    "reason": normalized_reason,
                    "effective_at": effective.isoformat(),
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO market.archive_retention_pin_versions (
                        id, pin_id, revision, target_kind, target_id,
                        owner_kind, owner_id, status, reason, effective_at,
                        known_at
                    ) VALUES (
                        :id, :pin_id, :revision, :target_kind, :target_id,
                        :owner_kind, :owner_id, :status, :reason,
                        :effective_at, :known_at
                    )
                    """
                ),
                {
                    "id": version_id,
                    "pin_id": pin_id,
                    "revision": revision,
                    "target_kind": normalized_target,
                    "target_id": normalized_target_id,
                    "owner_kind": normalized_owner_kind,
                    "owner_id": normalized_owner_id,
                    "status": status,
                    "reason": normalized_reason,
                    "effective_at": effective,
                    "known_at": datetime.now(UTC),
                },
            )
        return version_id

    def archive_retention_status(
        self, *, target_kind: str, target_id: str
    ) -> dict[str, Any]:
        normalized_target = str(target_kind or "").strip().lower()
        normalized_target_id = str(target_id or "").strip()
        if normalized_target not in {"raw_manifest", "book_checkpoint"}:
            raise ValueError("market_archive_retention_status_invalid: unsupported target")
        target_table = (
            "market.raw_archive_manifests"
            if normalized_target == "raw_manifest"
            else "market.book_checkpoint_manifests"
        )
        with db.session() as session:
            target = session.execute(
                text(
                    f"SELECT id, object_uri, object_sha256, content_fingerprint "
                    f"FROM {target_table} WHERE id = :target_id"
                ),
                {"target_id": normalized_target_id},
            ).mappings().first()
            if target is None:
                raise ValueError("market_archive_retention_status_invalid: target missing")
            active_pins = session.execute(
                text(
                    """
                    SELECT latest.pin_id, latest.owner_kind, latest.owner_id,
                           latest.reason, latest.effective_at, latest.known_at
                    FROM (
                        SELECT versions.*,
                               row_number() OVER (
                                   PARTITION BY pin_id ORDER BY revision DESC
                               ) AS selected_revision
                        FROM market.archive_retention_pin_versions AS versions
                        WHERE target_kind = :target_kind
                          AND target_id = :target_id
                    ) AS latest
                    WHERE latest.selected_revision = 1
                      AND latest.status = 'active'
                    ORDER BY latest.pin_id
                    """
                ),
                {
                    "target_kind": normalized_target,
                    "target_id": normalized_target_id,
                },
            ).mappings().all()
            dataset_pin_count = 0
            replacement_ids: list[str] = []
            source_ids: list[str] = []
            if normalized_target == "raw_manifest":
                dataset_pin_count = int(
                    session.execute(
                        text(
                            "SELECT count(*) FROM market.dataset_archive_refs "
                            "WHERE raw_archive_manifest_id = :target_id"
                        ),
                        {"target_id": normalized_target_id},
                    ).scalar_one()
                )
                replacement_ids = [
                    str(value)
                    for value in session.execute(
                        text(
                            "SELECT replacement_manifest_id "
                            "FROM market.raw_archive_compaction_sources "
                            "WHERE source_manifest_id = :target_id "
                            "ORDER BY replacement_manifest_id"
                        ),
                        {"target_id": normalized_target_id},
                    ).scalars()
                ]
                source_ids = [
                    str(value)
                    for value in session.execute(
                        text(
                            "SELECT source_manifest_id "
                            "FROM market.raw_archive_compaction_sources "
                            "WHERE replacement_manifest_id = :target_id "
                            "ORDER BY source_ordinal"
                        ),
                        {"target_id": normalized_target_id},
                    ).scalars()
                ]
            expired_event = session.execute(
                text(
                    """
                    SELECT occurred_at, reason, evidence
                    FROM market.storage_lifecycle_events
                    WHERE action = 'archive_expire'
                      AND target_kind = :target_kind
                      AND target_id = :target_id
                      AND event_type = 'completed'
                    ORDER BY occurred_at DESC, event_ordinal DESC
                    LIMIT 1
                    """
                ),
                {
                    "target_kind": normalized_target,
                    "target_id": normalized_target_id,
                },
            ).mappings().first()
        explicit_pin_count = len(active_pins)
        pinned = bool(dataset_pin_count or explicit_pin_count)
        expired = expired_event is not None
        return {
            "schema_version": "market.archive_retention_status.v2",
            "target_kind": normalized_target,
            "target_id": normalized_target_id,
            "object_uri": str(target["object_uri"]),
            "object_sha256": str(target["object_sha256"]),
            "content_fingerprint": str(target["content_fingerprint"]),
            "expired": expired,
            "expiration": dict(expired_event) if expired_event is not None else None,
            "dataset_pin_count": dataset_pin_count,
            "explicit_active_pins": [dict(row) for row in active_pins],
            "replacement_manifest_ids": replacement_ids,
            "source_manifest_ids": source_ids,
            "pinned": pinned,
            "ordinary_retention_eligible": not pinned and not expired,
            "object_retention_state": (
                "expired" if expired else "pinned" if pinned else "active_unpinned"
            ),
        }

    def list_book_checkpoints(
        self, *, definition_id: str, session_id: str
    ) -> list[dict[str, Any]]:
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT checkpoints.*, definitions.provider_product_id
                    FROM market.book_checkpoint_manifests AS checkpoints
                    JOIN market.stream_definitions AS definitions
                      ON definitions.series_id = checkpoints.series_id
                     AND definitions.id = :definition_id
                    WHERE checkpoints.session_id = :session_id
                    ORDER BY checkpoints.connection_epoch,
                             checkpoints.receive_ordinal,
                             checkpoints.event_ordinal
                    """
                ),
                {"definition_id": definition_id, "session_id": session_id},
            ).mappings().all()
        return [dict(row) for row in rows]

    def reconcile_manifest_trade_ids(
        self,
        *,
        manifest_id: str,
        provider_product_id: str,
        provider_trade_ids: Sequence[str],
    ) -> dict[str, Any]:
        """Compare replayed provider identities with mappings and canonical facts."""

        trade_ids = sorted(
            {str(value).strip() for value in provider_trade_ids if str(value).strip()}
        )
        with db.session() as session:
            manifest = session.execute(
                text(
                    """
                    SELECT manifests.record_count, definitions.source_id,
                           definitions.provider_product_id
                    FROM market.raw_archive_manifests AS manifests
                    JOIN market.stream_definitions AS definitions
                      ON definitions.id = manifests.definition_id
                    WHERE manifests.id = :manifest_id
                    """
                ),
                {"manifest_id": manifest_id},
            ).mappings().first()
            if manifest is None:
                raise ValueError(
                    f"market_archive_manifest_unknown: manifest_id={manifest_id}"
                )
            if str(manifest["provider_product_id"]) != str(provider_product_id):
                raise ValueError(
                    "market_archive_reconciliation_invalid: product scope mismatch"
                )
            mapped_count = int(
                session.execute(
                    text(
                        """
                        SELECT count(*) FROM market.raw_archive_record_mappings
                        WHERE manifest_id = :manifest_id
                        """
                    ),
                    {"manifest_id": manifest_id},
                ).scalar_one()
            )
            found = set(
                str(value)
                for value in session.execute(
                    text(
                        """
                        SELECT provider_trade_id
                        FROM market.market_trade_identities
                        WHERE source_id = :source_id
                          AND provider_product_id = :product_id
                          AND provider_trade_id = ANY(:trade_ids)
                        """
                    ),
                    {
                        "source_id": int(manifest["source_id"]),
                        "product_id": str(provider_product_id),
                        "trade_ids": trade_ids,
                    },
                ).scalars().all()
            )
        missing = sorted(set(trade_ids) - found)
        return {
            "schema_version": "market.archive_trade_reconciliation.v1",
            "manifest_id": manifest_id,
            "raw_record_count": int(manifest["record_count"]),
            "mapped_raw_record_count": mapped_count,
            "raw_mapping_complete": mapped_count == int(manifest["record_count"]),
            "replayed_unique_trade_count": len(trade_ids),
            "canonical_trade_count": len(found),
            "missing_provider_trade_ids": missing,
            "canonical_reconciliation_complete": not missing,
        }

    def recent_trade_id_overlap(
        self, *, definition_id: str, provider_trade_ids: Sequence[str]
    ) -> dict[str, Any]:
        """Return bounded canonical overlap without asserting REST completeness."""

        trade_ids = sorted(
            {str(value).strip() for value in provider_trade_ids if str(value).strip()}
        )
        with db.session() as session:
            definition = session.execute(
                text(
                    """
                    SELECT source_id, provider_product_id
                    FROM market.stream_definitions
                    WHERE id = :definition_id
                    """
                ),
                {"definition_id": definition_id},
            ).mappings().first()
            if definition is None:
                raise ValueError(
                    f"market_stream_definition_unknown: definition_id={definition_id}"
                )
            found = sorted(
                str(value)
                for value in session.execute(
                    text(
                        """
                        SELECT provider_trade_id
                        FROM market.market_trade_identities
                        WHERE source_id = :source_id
                          AND provider_product_id = :product_id
                          AND provider_trade_id = ANY(:trade_ids)
                        """
                    ),
                    {
                        "source_id": int(definition["source_id"]),
                        "product_id": str(definition["provider_product_id"]),
                        "trade_ids": trade_ids,
                    },
                ).scalars().all()
            )
        return {
            "provider_product_id": str(definition["provider_product_id"]),
            "requested_trade_ids": trade_ids,
            "canonical_overlap_trade_ids": found,
            "rest_only_trade_ids": sorted(set(trade_ids) - set(found)),
        }



    def cross_stream_input_commit_seq(
        self,
        *,
        futures_bbo_series_id: int,
        spot_bbo_series_id: int,
        oi_series_id: Optional[int],
        funding_series_id: Optional[int],
        start: datetime,
        end: datetime,
        known_at: datetime,
    ) -> int:
        """Return a bounded watermark from inputs only, never derived outputs."""

        start_at = _utc(start)
        end_at = _utc(end)
        decision_time = _utc(known_at)
        if end_at <= start_at:
            raise ValueError("market_feature_watermark_invalid: end must follow start")
        futures_bbo = int(futures_bbo_series_id)
        spot_bbo = int(spot_bbo_series_id)
        if futures_bbo <= 0 or spot_bbo <= 0:
            raise ValueError(
                "market_feature_watermark_invalid: BBO series identities are required"
            )
        oi_series = int(oi_series_id) if oi_series_id is not None else -1
        funding_series = (
            int(funding_series_id) if funding_series_id is not None else -1
        )
        with db.session() as session:
            value = session.execute(
                text(
                    """
                    SELECT GREATEST(
                        COALESCE((
                            SELECT MAX(market_commit_seq)
                            FROM market.bbo_feature_versions
                            WHERE series_id IN (:futures_bbo, :spot_bbo)
                              AND bucket_start >= :start
                              AND bucket_start < :end
                              AND known_at <= :known_at
                        ), 0),
                        COALESCE((
                            SELECT MAX(market_commit_seq)
                            FROM market.open_interest_versions
                            WHERE series_id = :oi_series
                              AND sample_time >= :start
                              AND sample_time < :end
                              AND known_at <= :known_at
                        ), 0),
                        COALESCE((
                            SELECT MAX(market_commit_seq)
                            FROM market.funding_rate_versions
                            WHERE series_id = :funding_series
                              AND sample_time >= :start
                              AND sample_time < :end
                              AND known_at <= :known_at
                        ), 0),
                        COALESCE((
                            SELECT MAX(detected_as_of_commit_seq)
                            FROM market.gap_evidence
                            WHERE series_id IN (:oi_series, :funding_series)
                              AND end_time > :start
                              AND start_time < :end
                        ), 0)
                    )
                    """
                ),
                {
                    "futures_bbo": futures_bbo,
                    "spot_bbo": spot_bbo,
                    "oi_series": oi_series,
                    "funding_series": funding_series,
                    "start": start_at,
                    "end": end_at,
                    "known_at": decision_time,
                },
            ).scalar_one()
        return int(value)

    def ingest_market_state_features(
        self,
        *,
        bbo_facts: Iterable[BboFeatureFact] = (),
        depth_facts: Iterable[DepthFeatureFact] = (),
        flow_facts: Iterable[TradeFlowFeatureFact] = (),
        basis_facts: Iterable[BasisFeatureFact] = (),
        derivative_facts: Iterable[DerivativeStateFeatureFact] = (),
        response_facts: Iterable[ResponseFeatureFact] = (),
    ) -> FeatureIngestionOutcome:
        """Persist one deterministic append-only market-state feature batch."""

        bbo_rows = tuple(bbo_facts)
        depth_rows = tuple(depth_facts)
        flow_rows = tuple(flow_facts)
        basis_rows = tuple(basis_facts)
        derivative_rows = tuple(derivative_facts)
        response_rows = tuple(response_facts)
        inserted = 0
        noop = 0
        max_commit_seq = 0
        material_hashes: list[str] = []

        def record(outcome: tuple[bool, int], material_hash: str) -> None:
            nonlocal inserted, noop, max_commit_seq
            was_inserted, commit_seq = outcome
            inserted += int(was_inserted)
            noop += int(not was_inserted)
            max_commit_seq = max(max_commit_seq, int(commit_seq))
            material_hashes.append(str(material_hash))

        with db.session() as session:
            for fact in bbo_rows:
                position = fact.source_position.material()
                _require_book_state_source(
                    session,
                    series_id=fact.source_l2_series_id,
                    position=position,
                    validity_interval_id=fact.validity_interval_id,
                    state_hash=fact.source_state_hash,
                )
                record(
                    _persist_feature_revision(
                        session,
                        table_name="bbo_feature_versions",
                        time_column="bucket_start",
                        prefix="bbo",
                        identity={
                            "series_id": fact.series_id,
                            "bucket_start": fact.bucket_start,
                        },
                        values={
                            "source_l2_series_id": fact.source_l2_series_id,
                            "bucket_end": fact.bucket_end,
                            "source_effective_at": fact.source_effective_at,
                            "known_at": fact.known_at,
                            "source_position": _json(position),
                            "validity_interval_id": fact.validity_interval_id,
                            "product_definition_version_id": fact.product_definition_version_id,
                            "provider_size_unit": fact.provider_size_unit.value,
                            "source_state_hash": fact.source_state_hash,
                            "bid_price": fact.bid_price,
                            "bid_quantity": fact.bid_quantity,
                            "bid_base_quantity": fact.bid_base_quantity,
                            "ask_price": fact.ask_price,
                            "ask_quantity": fact.ask_quantity,
                            "ask_base_quantity": fact.ask_base_quantity,
                            "mid_price": fact.mid_price,
                            "spread": fact.spread,
                            "spread_bps": fact.spread_bps,
                            "input_fingerprint": fact.input_fingerprint,
                            "material_hash": fact.material_hash,
                        },
                        json_columns=("source_position",),
                    ),
                    fact.material_hash,
                )

            for fact in depth_rows:
                position = fact.source_position.material()
                _require_book_state_source(
                    session,
                    series_id=fact.source_l2_series_id,
                    position=position,
                    validity_interval_id=fact.validity_interval_id,
                    state_hash=fact.source_state_hash,
                )
                record(
                    _persist_feature_revision(
                        session,
                        table_name="depth_feature_versions",
                        time_column="bucket_start",
                        prefix="depth",
                        identity={
                            "series_id": fact.series_id,
                            "bucket_start": fact.bucket_start,
                            "band_bps": fact.band_bps,
                        },
                        values={
                            "source_l2_series_id": fact.source_l2_series_id,
                            "bucket_end": fact.bucket_end,
                            "source_effective_at": fact.source_effective_at,
                            "known_at": fact.known_at,
                            "source_position": _json(position),
                            "validity_interval_id": fact.validity_interval_id,
                            "source_state_hash": fact.source_state_hash,
                            "bbo_input_fingerprint": fact.bbo_input_fingerprint,
                            "provider_size_unit": fact.provider_size_unit.value,
                            "mid_price": fact.mid_price,
                            "bid_quantity": fact.bid_quantity,
                            "ask_quantity": fact.ask_quantity,
                            "bid_base_quantity": fact.bid_base_quantity,
                            "ask_base_quantity": fact.ask_base_quantity,
                            "bid_notional": fact.bid_notional,
                            "ask_notional": fact.ask_notional,
                            "imbalance": fact.imbalance,
                            "input_fingerprint": fact.input_fingerprint,
                            "material_hash": fact.material_hash,
                        },
                        json_columns=("source_position",),
                    ),
                    fact.material_hash,
                )

            for fact in flow_rows:
                _require_material_source(
                    session,
                    table_name="trade_flow_aggregate_versions",
                    series_id=fact.source_trade_flow_series_id,
                    material_hash=fact.aggregate_material_hash,
                )
                record(
                    _persist_feature_revision(
                        session,
                        table_name="trade_flow_feature_versions",
                        time_column="bucket_start",
                        prefix="flow",
                        identity={
                            "series_id": fact.series_id,
                            "interval_seconds": fact.interval_seconds,
                            "bucket_start": fact.bucket_start,
                        },
                        values={
                            "source_trade_flow_series_id": fact.source_trade_flow_series_id,
                            "bucket_end": fact.bucket_end,
                            "known_at": fact.known_at,
                            "aggregate_material_hash": fact.aggregate_material_hash,
                            "aggregate_input_fingerprint": fact.aggregate_input_fingerprint,
                            "trade_count": fact.trade_count,
                            "quote_notional": fact.quote_notional,
                            "aggressor_buy_base_volume": fact.aggressor_buy_base_volume,
                            "aggressor_sell_base_volume": fact.aggressor_sell_base_volume,
                            "aggressor_buy_notional": fact.aggressor_buy_notional,
                            "aggressor_sell_notional": fact.aggressor_sell_notional,
                            "cvd_base": fact.cvd_base,
                            "cvd_notional": fact.cvd_notional,
                            "cvd_volume_share": fact.cvd_volume_share,
                            "input_fingerprint": fact.input_fingerprint,
                            "material_hash": fact.material_hash,
                        },
                    ),
                    fact.material_hash,
                )

            for fact in basis_rows:
                _require_material_source(
                    session,
                    table_name="bbo_feature_versions",
                    series_id=fact.futures_series_id,
                    material_hash=fact.futures_bbo_material_hash,
                )
                _require_material_source(
                    session,
                    table_name="bbo_feature_versions",
                    series_id=fact.spot_series_id,
                    material_hash=fact.spot_bbo_material_hash,
                )
                record(
                    _persist_feature_revision(
                        session,
                        table_name="futures_spot_relationship_versions",
                        time_column="effective_at",
                        prefix="basis",
                        identity={
                            "series_id": fact.series_id,
                            "effective_at": fact.effective_at,
                        },
                        values={
                            "mapping_id": fact.mapping_id,
                            "futures_series_id": fact.futures_series_id,
                            "spot_series_id": fact.spot_series_id,
                            "known_at": fact.known_at,
                            "futures_bbo_material_hash": fact.futures_bbo_material_hash,
                            "spot_bbo_material_hash": fact.spot_bbo_material_hash,
                            "futures_mid": fact.futures_mid,
                            "spot_mid": fact.spot_mid,
                            "futures_staleness_seconds": fact.futures_staleness_seconds,
                            "spot_staleness_seconds": fact.spot_staleness_seconds,
                            "basis": fact.basis,
                            "basis_bps": fact.basis_bps,
                            "input_fingerprint": fact.input_fingerprint,
                            "material_hash": fact.material_hash,
                        },
                    ),
                    fact.material_hash,
                )

            for fact in derivative_rows:
                for table_name, series_id, commit_seq in (
                    (
                        "open_interest_versions",
                        fact.oi_series_id,
                        fact.oi_market_commit_seq,
                    ),
                    (
                        "funding_rate_versions",
                        fact.funding_series_id,
                        fact.funding_market_commit_seq,
                    ),
                ):
                    if series_id is None:
                        continue
                    found = session.execute(
                        text(
                            f"SELECT 1 FROM market.{table_name} "
                            "WHERE series_id = :series_id "
                            "AND market_commit_seq = :commit_seq LIMIT 1"
                        ),
                        {
                            "series_id": int(series_id),
                            "commit_seq": int(commit_seq),
                        },
                    ).scalar_one_or_none()
                    if found is None:
                        raise ValueError(
                            f"market_feature_source_incomplete: {table_name} source is missing"
                        )
                record(
                    _persist_feature_revision(
                        session,
                        table_name="derivative_state_versions",
                        time_column="effective_at",
                        prefix="derivative",
                        identity={
                            "series_id": fact.series_id,
                            "effective_at": fact.effective_at,
                        },
                        values={
                            "instrument_id": fact.instrument_id,
                            "known_at": fact.known_at,
                            "oi_series_id": fact.oi_series_id,
                            "oi_sample_time": fact.oi_sample_time,
                            "oi_market_commit_seq": fact.oi_market_commit_seq,
                            "oi_value": fact.oi_value,
                            "oi_previous_value": fact.oi_previous_value,
                            "oi_log_change": fact.oi_log_change,
                            "funding_series_id": fact.funding_series_id,
                            "funding_sample_time": fact.funding_sample_time,
                            "funding_market_commit_seq": fact.funding_market_commit_seq,
                            "funding_rate": fact.funding_rate,
                            "funding_time": fact.funding_time,
                            "funding_interval_seconds": fact.funding_interval_seconds,
                            "funding_semantics": fact.funding_semantics,
                            "input_fingerprint": fact.input_fingerprint,
                            "material_hash": fact.material_hash,
                        },
                    ),
                    fact.material_hash,
                )

            for fact in response_rows:
                _require_material_source(
                    session,
                    table_name="trade_flow_feature_versions",
                    series_id=fact.source_flow_feature_series_id,
                    material_hash=fact.source_flow_material_hash,
                )
                for position, state_hash in (
                    (fact.pre_book_source_position, fact.pre_state_hash),
                    (fact.trough_book_source_position, fact.trough_state_hash),
                    (fact.post_book_source_position, fact.post_state_hash),
                ):
                    _require_book_state_source(
                        session,
                        series_id=fact.source_l2_series_id,
                        position=position.material(),
                        validity_interval_id=fact.validity_interval_id,
                        state_hash=state_hash,
                    )
                positions = {
                    "first_trade": dict(fact.first_trade_source_position),
                    "last_trade": dict(fact.last_trade_source_position),
                    "pre_book": fact.pre_book_source_position.material(),
                    "trough_book": fact.trough_book_source_position.material(),
                    "post_book": fact.post_book_source_position.material(),
                }
                record(
                    _persist_feature_revision(
                        session,
                        table_name="market_response_feature_versions",
                        time_column="bucket_start",
                        prefix="response",
                        identity={
                            "series_id": fact.series_id,
                            "bucket_start": fact.bucket_start,
                            "direction": fact.direction.value,
                        },
                        values={
                            "source_flow_feature_series_id": fact.source_flow_feature_series_id,
                            "source_l2_series_id": fact.source_l2_series_id,
                            "source_flow_material_hash": fact.source_flow_material_hash,
                            "pre_state_hash": fact.pre_state_hash,
                            "trough_state_hash": fact.trough_state_hash,
                            "post_state_hash": fact.post_state_hash,
                            "bucket_end": fact.bucket_end,
                            "effective_at": fact.effective_at,
                            "known_at": fact.known_at,
                            "first_trade_id": fact.first_trade_id,
                            "last_trade_id": fact.last_trade_id,
                            "source_positions": _json(positions),
                            "validity_interval_id": fact.validity_interval_id,
                            "aggressive_notional": fact.aggressive_notional,
                            "signed_aggressive_notional": fact.signed_aggressive_notional,
                            "response_bps": fact.response_bps,
                            "pre_depth_notional": fact.pre_depth_notional,
                            "consumed_depth_notional": fact.consumed_depth_notional,
                            "replenished_depth_notional": fact.replenished_depth_notional,
                            "depth_replenishment": fact.depth_replenishment,
                            "liquidity_adjusted_impact": fact.liquidity_adjusted_impact,
                            "price_response_per_flow": fact.price_response_per_flow,
                            "input_fingerprint": fact.input_fingerprint,
                            "material_hash": fact.material_hash,
                        },
                        json_columns=("source_positions",),
                    ),
                    fact.material_hash,
                )

        return FeatureIngestionOutcome(
            inserted_count=inserted,
            noop_count=noop,
            max_commit_seq=max_commit_seq,
            material_hashes=tuple(material_hashes),
        )

    def read_bbo_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[BboFeatureFact, ...]:
        with db.session() as session:
            rows = _read_feature_rows(
                session,
                table_name="bbo_feature_versions",
                time_column="bucket_start",
                partition_columns=("series_id", "bucket_start"),
                series_id=series_id,
                start=start,
                end=end,
                known_at=known_at,
                as_of_commit_seq=as_of_commit_seq,
            )
        facts = tuple(
            BboFeatureFact(
                series_id=int(row["series_id"]),
                source_l2_series_id=int(row["source_l2_series_id"]),
                bucket_start=row["bucket_start"],
                bucket_end=row["bucket_end"],
                source_effective_at=row["source_effective_at"],
                known_at=row["known_at"],
                source_position=_book_position_from_material(row["source_position"]),
                validity_interval_id=str(row["validity_interval_id"]),
                product_definition_version_id=str(
                    row["product_definition_version_id"]
                ),
                provider_size_unit=str(row["provider_size_unit"]),
                source_state_hash=str(row["source_state_hash"]),
                bid_price=Decimal(row["bid_price"]),
                bid_quantity=Decimal(row["bid_quantity"]),
                bid_base_quantity=Decimal(row["bid_base_quantity"]),
                ask_price=Decimal(row["ask_price"]),
                ask_quantity=Decimal(row["ask_quantity"]),
                ask_base_quantity=Decimal(row["ask_base_quantity"]),
                mid_price=Decimal(row["mid_price"]),
                spread=Decimal(row["spread"]),
                spread_bps=Decimal(row["spread_bps"]),
                input_fingerprint=str(row["input_fingerprint"]),
            )
            for row in rows
        )
        for fact, row in zip(facts, rows):
            if fact.material_hash != str(row["material_hash"]):
                raise RuntimeError("market_bbo_feature_storage_corrupt: hash mismatch")
        return facts

    def read_depth_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[DepthFeatureFact, ...]:
        with db.session() as session:
            rows = _read_feature_rows(
                session,
                table_name="depth_feature_versions",
                time_column="bucket_start",
                partition_columns=("series_id", "bucket_start", "band_bps"),
                series_id=series_id,
                start=start,
                end=end,
                known_at=known_at,
                as_of_commit_seq=as_of_commit_seq,
            )
        facts = tuple(
            DepthFeatureFact(
                series_id=int(row["series_id"]),
                source_l2_series_id=int(row["source_l2_series_id"]),
                bucket_start=row["bucket_start"],
                bucket_end=row["bucket_end"],
                source_effective_at=row["source_effective_at"],
                known_at=row["known_at"],
                source_position=_book_position_from_material(row["source_position"]),
                validity_interval_id=str(row["validity_interval_id"]),
                source_state_hash=str(row["source_state_hash"]),
                bbo_input_fingerprint=str(row["bbo_input_fingerprint"]),
                provider_size_unit=str(row["provider_size_unit"]),
                band_bps=int(row["band_bps"]),
                mid_price=Decimal(row["mid_price"]),
                bid_quantity=Decimal(row["bid_quantity"]),
                ask_quantity=Decimal(row["ask_quantity"]),
                bid_base_quantity=Decimal(row["bid_base_quantity"]),
                ask_base_quantity=Decimal(row["ask_base_quantity"]),
                bid_notional=(
                    Decimal(row["bid_notional"])
                    if row["bid_notional"] is not None
                    else None
                ),
                ask_notional=(
                    Decimal(row["ask_notional"])
                    if row["ask_notional"] is not None
                    else None
                ),
                imbalance=(
                    Decimal(row["imbalance"])
                    if row["imbalance"] is not None
                    else None
                ),
                input_fingerprint=str(row["input_fingerprint"]),
            )
            for row in rows
        )
        for fact, row in zip(facts, rows):
            if fact.material_hash != str(row["material_hash"]):
                raise RuntimeError("market_depth_feature_storage_corrupt: hash mismatch")
        return facts

    def read_trade_flow_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[TradeFlowFeatureFact, ...]:
        with db.session() as session:
            rows = _read_feature_rows(
                session,
                table_name="trade_flow_feature_versions",
                time_column="bucket_start",
                partition_columns=(
                    "series_id",
                    "interval_seconds",
                    "bucket_start",
                ),
                series_id=series_id,
                start=start,
                end=end,
                known_at=known_at,
                as_of_commit_seq=as_of_commit_seq,
            )
        facts = tuple(
            TradeFlowFeatureFact(
                series_id=int(row["series_id"]),
                source_trade_flow_series_id=int(
                    row["source_trade_flow_series_id"]
                ),
                interval_seconds=int(row["interval_seconds"]),
                bucket_start=row["bucket_start"],
                bucket_end=row["bucket_end"],
                known_at=row["known_at"],
                aggregate_material_hash=str(row["aggregate_material_hash"]),
                aggregate_input_fingerprint=str(
                    row["aggregate_input_fingerprint"]
                ),
                trade_count=int(row["trade_count"]),
                quote_notional=Decimal(row["quote_notional"]),
                aggressor_buy_base_volume=Decimal(
                    row["aggressor_buy_base_volume"]
                ),
                aggressor_sell_base_volume=Decimal(
                    row["aggressor_sell_base_volume"]
                ),
                aggressor_buy_notional=Decimal(row["aggressor_buy_notional"]),
                aggressor_sell_notional=Decimal(
                    row["aggressor_sell_notional"]
                ),
                cvd_base=Decimal(row["cvd_base"]),
                cvd_notional=Decimal(row["cvd_notional"]),
                cvd_volume_share=(
                    Decimal(row["cvd_volume_share"])
                    if row["cvd_volume_share"] is not None
                    else None
                ),
                input_fingerprint=str(row["input_fingerprint"]),
            )
            for row in rows
        )
        for fact, row in zip(facts, rows):
            if fact.material_hash != str(row["material_hash"]):
                raise RuntimeError(
                    "market_trade_flow_feature_storage_corrupt: hash mismatch"
                )
        return facts

    def read_basis_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[BasisFeatureFact, ...]:
        with db.session() as session:
            rows = _read_feature_rows(
                session,
                table_name="futures_spot_relationship_versions",
                time_column="effective_at",
                partition_columns=("series_id", "effective_at"),
                series_id=series_id,
                start=start,
                end=end,
                known_at=known_at,
                as_of_commit_seq=as_of_commit_seq,
            )
        facts = tuple(
            BasisFeatureFact(
                series_id=int(row["series_id"]),
                mapping_id=str(row["mapping_id"]),
                futures_series_id=int(row["futures_series_id"]),
                spot_series_id=int(row["spot_series_id"]),
                effective_at=row["effective_at"],
                known_at=row["known_at"],
                futures_bbo_material_hash=str(row["futures_bbo_material_hash"]),
                spot_bbo_material_hash=str(row["spot_bbo_material_hash"]),
                futures_mid=Decimal(row["futures_mid"]),
                spot_mid=Decimal(row["spot_mid"]),
                futures_staleness_seconds=Decimal(
                    row["futures_staleness_seconds"]
                ),
                spot_staleness_seconds=Decimal(row["spot_staleness_seconds"]),
                basis=Decimal(row["basis"]),
                basis_bps=Decimal(row["basis_bps"]),
                input_fingerprint=str(row["input_fingerprint"]),
            )
            for row in rows
        )
        for fact, row in zip(facts, rows):
            if fact.material_hash != str(row["material_hash"]):
                raise RuntimeError("market_basis_feature_storage_corrupt: hash mismatch")
        return facts

    def read_derivative_state_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[DerivativeStateFeatureFact, ...]:
        with db.session() as session:
            rows = _read_feature_rows(
                session,
                table_name="derivative_state_versions",
                time_column="effective_at",
                partition_columns=("series_id", "effective_at"),
                series_id=series_id,
                start=start,
                end=end,
                known_at=known_at,
                as_of_commit_seq=as_of_commit_seq,
            )
        facts = tuple(
            DerivativeStateFeatureFact(
                series_id=int(row["series_id"]),
                instrument_id=str(row["instrument_id"]),
                effective_at=row["effective_at"],
                known_at=row["known_at"],
                oi_series_id=(
                    int(row["oi_series_id"])
                    if row["oi_series_id"] is not None
                    else None
                ),
                oi_sample_time=row["oi_sample_time"],
                oi_market_commit_seq=(
                    int(row["oi_market_commit_seq"])
                    if row["oi_market_commit_seq"] is not None
                    else None
                ),
                oi_value=(
                    Decimal(row["oi_value"]) if row["oi_value"] is not None else None
                ),
                oi_previous_value=(
                    Decimal(row["oi_previous_value"])
                    if row["oi_previous_value"] is not None
                    else None
                ),
                oi_log_change=(
                    Decimal(row["oi_log_change"])
                    if row["oi_log_change"] is not None
                    else None
                ),
                funding_series_id=(
                    int(row["funding_series_id"])
                    if row["funding_series_id"] is not None
                    else None
                ),
                funding_sample_time=row["funding_sample_time"],
                funding_market_commit_seq=(
                    int(row["funding_market_commit_seq"])
                    if row["funding_market_commit_seq"] is not None
                    else None
                ),
                funding_rate=(
                    Decimal(row["funding_rate"])
                    if row["funding_rate"] is not None
                    else None
                ),
                funding_time=row["funding_time"],
                funding_interval_seconds=(
                    int(row["funding_interval_seconds"])
                    if row["funding_interval_seconds"] is not None
                    else None
                ),
                funding_semantics=row["funding_semantics"],
                input_fingerprint=str(row["input_fingerprint"]),
            )
            for row in rows
        )
        for fact, row in zip(facts, rows):
            if fact.material_hash != str(row["material_hash"]):
                raise RuntimeError(
                    "market_derivative_state_storage_corrupt: hash mismatch"
                )
        return facts

    def read_response_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[ResponseFeatureFact, ...]:
        with db.session() as session:
            rows = _read_feature_rows(
                session,
                table_name="market_response_feature_versions",
                time_column="bucket_start",
                partition_columns=("series_id", "bucket_start", "direction"),
                series_id=series_id,
                start=start,
                end=end,
                known_at=known_at,
                as_of_commit_seq=as_of_commit_seq,
            )
        facts_list: list[ResponseFeatureFact] = []
        for row in rows:
            positions = dict(row["source_positions"])
            fact = ResponseFeatureFact(
                series_id=int(row["series_id"]),
                source_flow_feature_series_id=int(
                    row["source_flow_feature_series_id"]
                ),
                source_l2_series_id=int(row["source_l2_series_id"]),
                source_flow_material_hash=str(row["source_flow_material_hash"]),
                pre_state_hash=str(row["pre_state_hash"]),
                trough_state_hash=str(row["trough_state_hash"]),
                post_state_hash=str(row["post_state_hash"]),
                bucket_start=row["bucket_start"],
                bucket_end=row["bucket_end"],
                effective_at=row["effective_at"],
                known_at=row["known_at"],
                direction=MarketSide(str(row["direction"])),
                first_trade_id=str(row["first_trade_id"]),
                last_trade_id=str(row["last_trade_id"]),
                first_trade_source_position=dict(positions["first_trade"]),
                last_trade_source_position=dict(positions["last_trade"]),
                pre_book_source_position=_book_position_from_material(
                    positions["pre_book"]
                ),
                trough_book_source_position=_book_position_from_material(
                    positions["trough_book"]
                ),
                post_book_source_position=_book_position_from_material(
                    positions["post_book"]
                ),
                validity_interval_id=str(row["validity_interval_id"]),
                aggressive_notional=Decimal(row["aggressive_notional"]),
                signed_aggressive_notional=Decimal(
                    row["signed_aggressive_notional"]
                ),
                response_bps=Decimal(row["response_bps"]),
                pre_depth_notional=Decimal(row["pre_depth_notional"]),
                consumed_depth_notional=Decimal(row["consumed_depth_notional"]),
                replenished_depth_notional=Decimal(
                    row["replenished_depth_notional"]
                ),
                depth_replenishment=Decimal(row["depth_replenishment"]),
                liquidity_adjusted_impact=Decimal(
                    row["liquidity_adjusted_impact"]
                ),
                price_response_per_flow=Decimal(
                    row["price_response_per_flow"]
                ),
                input_fingerprint=str(row["input_fingerprint"]),
            )
            if fact.material_hash != str(row["material_hash"]):
                raise RuntimeError(
                    "market_response_feature_storage_corrupt: hash mismatch"
                )
            facts_list.append(fact)
        return tuple(facts_list)

    def read_feature_records(
        self,
        *,
        fact_type: str,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: Optional[datetime] = None,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[TypedFeatureRecord, ...]:
        """Read one registered market-state fact as storage-backed typed revisions."""

        definitions = {
            "market.bbo": (
                "bbo_feature_versions",
                "bucket_start",
                ("series_id", "bucket_start"),
                self.read_bbo_features,
            ),
            "market.depth_observation": (
                "depth_feature_versions",
                "bucket_start",
                ("series_id", "bucket_start", "band_bps"),
                self.read_depth_features,
            ),
            "market.trade_flow_feature": (
                "trade_flow_feature_versions",
                "bucket_start",
                ("series_id", "interval_seconds", "bucket_start"),
                self.read_trade_flow_features,
            ),
            "market.futures_spot_relationship": (
                "futures_spot_relationship_versions",
                "effective_at",
                ("series_id", "effective_at"),
                self.read_basis_features,
            ),
            "market.derivative_state": (
                "derivative_state_versions",
                "effective_at",
                ("series_id", "effective_at"),
                self.read_derivative_state_features,
            ),
            "market.market_response": (
                "market_response_feature_versions",
                "bucket_start",
                ("series_id", "bucket_start", "direction"),
                self.read_response_features,
            ),
        }
        normalized_type = str(fact_type or "").strip().lower()
        definition = definitions.get(normalized_type)
        if definition is None:
            raise ValueError(
                f"market_feature_read_unsupported: fact_type={normalized_type or '<missing>'}"
            )
        table_name, time_column, partition_columns, reader = definition
        decision_time = known_at or datetime.max.replace(tzinfo=UTC)
        facts = reader(
            series_id=int(series_id),
            start=start,
            end=end,
            known_at=decision_time,
            as_of_commit_seq=as_of_commit_seq,
        )
        with db.session() as session:
            rows = _read_feature_rows(
                session,
                table_name=table_name,
                time_column=time_column,
                partition_columns=partition_columns,
                series_id=int(series_id),
                start=start,
                end=end,
                known_at=decision_time,
                as_of_commit_seq=as_of_commit_seq,
            )
        by_material = {str(row["material_hash"]): row for row in rows}
        if len(by_material) != len(rows):
            raise RuntimeError("market_feature_storage_corrupt: duplicate material hash")
        records: list[TypedFeatureRecord] = []
        for fact in facts:
            row = by_material.get(fact.material_hash)
            if row is None:
                raise RuntimeError("market_feature_storage_corrupt: fact envelope missing")
            records.append(
                TypedFeatureRecord(
                    version_id=str(row["id"]),
                    series_id=int(row["series_id"]),
                    revision=int(row["revision"]),
                    market_commit_seq=int(row["market_commit_seq"]),
                    provenance_hash=str(row["provenance_hash"]),
                    quality=dict(row["quality"] or {}),
                    fact=fact,
                )
            )
        return tuple(records)


def _coverage_version(row: Mapping[str, Any]) -> TradeCoverageIntervalVersion:
    coverage = TradeCoverageIntervalVersion(
        interval_id=str(row["interval_id"]),
        revision=int(row["revision"]),
        definition_id=str(row["definition_id"]),
        session_id=str(row["session_id"]),
        connection_epoch=int(row["connection_epoch"]),
        provider_product_id=str(row["provider_product_id"]),
        channel=str(row["channel"]),
        status=str(row["status"]),
        ordering_assurance=str(row["ordering_assurance"]),
        archive_status=str(row["archive_status"]),
        opening_raw_record_id=str(row["opening_raw_record_id"]),
        opening_receive_ordinal=int(row["opening_receive_ordinal"]),
        opening_effective_at=row["opening_effective_at"],
        last_raw_record_id=str(row["last_raw_record_id"]),
        last_receive_ordinal=int(row["last_receive_ordinal"]),
        last_effective_at=row["last_effective_at"],
        canonicalization_watermark_ordinal=int(
            row["canonicalization_watermark_ordinal"]
        ),
        archive_complete_through_ordinal=int(
            row["archive_complete_through_ordinal"]
        ),
        known_at=row["known_at"],
        closing_raw_record_id=row["closing_raw_record_id"],
        closing_receive_ordinal=(
            int(row["closing_receive_ordinal"])
            if row["closing_receive_ordinal"] is not None
            else None
        ),
        closing_effective_at=row["closing_effective_at"],
        first_provider_sequence_num=row["first_provider_sequence_num"],
        last_provider_sequence_num=row["last_provider_sequence_num"],
        gap_quality_event_ids=tuple(row["gap_quality_event_ids"] or ()),
        opening_evidence=dict(row["opening_evidence"] or {}),
        closing_evidence=dict(row["closing_evidence"] or {}),
    )
    if coverage.material_hash != str(row["material_hash"]):
        raise RuntimeError("market_stream_coverage_storage_corrupt: hash mismatch")
    return coverage


def _trade_record(row: Mapping[str, Any]) -> MarketTradeRecord:
    fact = MarketTradeFact(
        provider_product_id=row["provider_product_id"],
        provider_trade_id=row["provider_trade_id"],
        delivery_kind=TradeDeliveryKind(row["delivery_kind"]),
        price=Decimal(row["price"]),
        provider_size=Decimal(row["provider_size"]),
        provider_size_unit=row["provider_size_unit"],
        maker_side=MarketSide(row["maker_side"]),
        aggressor_side=MarketSide(row["aggressor_side"]) if row["aggressor_side"] else None,
        aggressor_transform_version=row["aggressor_transform_version"],
        contract_quantity=Decimal(row["contract_quantity"]) if row["contract_quantity"] is not None else None,
        base_quantity=Decimal(row["base_quantity"]) if row["base_quantity"] is not None else None,
        quote_notional=Decimal(row["quote_notional"]) if row["quote_notional"] is not None else None,
        base_currency=row["base_currency"],
        quote_currency=row["quote_currency"],
        product_definition_version_id=row["product_definition_version_id"],
        provider_event_time=row["provider_event_time"],
        provider_message_time=row["provider_message_time"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        provider_sequence_num=row["provider_sequence_num"],
        connection_epoch=row["connection_epoch"],
        receive_ordinal=row["receive_ordinal"],
        event_ordinal=row["event_ordinal"],
        trade_ordinal=row["trade_ordinal"],
        raw_record_id=row["raw_record_id"],
        coverage_interval_id=row["coverage_interval_id"],
    )
    if fact.material_hash != row["material_hash"] or fact.row_hash != row["row_hash"]:
        raise RuntimeError("market_trade_storage_corrupt: hash mismatch")
    return MarketTradeRecord(
        version_id=str(row["id"]),
        series_id=int(row["series_id"]),
        source_id=int(row["source_id"]),
        revision=int(row["revision"]),
        market_commit_seq=int(row["market_commit_seq"]),
        provenance_hash=str(row["provenance_hash"]),
        quality=dict(row["quality"] or {}),
        fact=fact,
    )


def _aggregate_record(row: Mapping[str, Any]) -> TradeFlowAggregateRecord:
    fact = TradeFlowAggregateFact(
        interval_seconds=int(row["interval_seconds"]),
        bucket_start=row["bucket_start"],
        bucket_end=row["bucket_end"],
        trade_count=int(row["trade_count"]),
        maker_buy_count=int(row["maker_buy_count"]),
        maker_sell_count=int(row["maker_sell_count"]),
        aggressor_buy_count=int(row["aggressor_buy_count"]) if row["aggressor_buy_count"] is not None else None,
        aggressor_sell_count=int(row["aggressor_sell_count"]) if row["aggressor_sell_count"] is not None else None,
        contract_volume=Decimal(row["contract_volume"]) if row["contract_volume"] is not None else None,
        base_volume=Decimal(row["base_volume"]) if row["base_volume"] is not None else None,
        quote_notional=Decimal(row["quote_notional"]) if row["quote_notional"] is not None else None,
        maker_buy_base_volume=Decimal(row["maker_buy_base_volume"]) if row["maker_buy_base_volume"] is not None else None,
        maker_sell_base_volume=Decimal(row["maker_sell_base_volume"]) if row["maker_sell_base_volume"] is not None else None,
        aggressor_buy_base_volume=Decimal(row["aggressor_buy_base_volume"]) if row["aggressor_buy_base_volume"] is not None else None,
        aggressor_sell_base_volume=Decimal(row["aggressor_sell_base_volume"]) if row["aggressor_sell_base_volume"] is not None else None,
        cvd_delta=Decimal(row["cvd_delta"]) if row["cvd_delta"] is not None else None,
        cvd_unit=row["cvd_unit"],
        open_price=Decimal(row["open_price"]) if row["open_price"] is not None else None,
        high_price=Decimal(row["high_price"]) if row["high_price"] is not None else None,
        low_price=Decimal(row["low_price"]) if row["low_price"] is not None else None,
        close_price=Decimal(row["close_price"]) if row["close_price"] is not None else None,
        first_trade_id=row["first_trade_id"],
        last_trade_id=row["last_trade_id"],
        first_receive_ordinal=int(row["first_receive_ordinal"]) if row["first_receive_ordinal"] is not None else None,
        last_receive_ordinal=int(row["last_receive_ordinal"]) if row["last_receive_ordinal"] is not None else None,
        coverage_interval_id=row["coverage_interval_id"],
        coverage_revision=int(row["coverage_revision"]) if row["coverage_revision"] is not None else None,
        aggregate_complete=bool(row["aggregate_complete"]),
        archive_complete=bool(row["archive_complete"]),
        canonicalization_complete=bool(row["canonicalization_complete"]),
        late_trade_count=int(row["late_trade_count"]),
        known_at=row["known_at"],
        input_fingerprint=row["input_fingerprint"],
    )
    if fact.material_hash != row["material_hash"]:
        raise RuntimeError("market_trade_flow_storage_corrupt: hash mismatch")
    return TradeFlowAggregateRecord(
        version_id=str(row["id"]),
        series_id=int(row["series_id"]),
        revision=int(row["revision"]),
        market_commit_seq=int(row["market_commit_seq"]),
        aggregation_version=str(row["aggregation_version"]),
        provenance_hash=str(row["provenance_hash"]),
        quality=dict(row["quality"] or {}),
        fact=fact,
    )


market_structure_repository = PostgresMarketStructureRepository()


__all__ = [
    "AggregateIngestionOutcome",
    "ArchiveCommitResult",
    "BookIngestionOutcome",
    "FeatureIngestionOutcome",
    "MarketStructureOwnershipError",
    "MarketTradeConflictError",
    "PostgresMarketStructureRepository",
    "StreamClaim",
    "TradeIngestionOutcome",
    "market_structure_repository",
]
