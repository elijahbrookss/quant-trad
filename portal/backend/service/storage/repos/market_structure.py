"""PostgreSQL authority for continuous stream evidence and projections."""

from __future__ import annotations

import hashlib
import json
import re
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
from market_data.canonical_adapters import (
    DERIVED_MARKET_STATE_SOURCE,
    canonicalize_basis_feature,
    canonicalize_bbo_feature,
    canonicalize_depth_feature,
    canonicalize_derivative_state_feature,
    canonicalize_l2_mutation_batch,
    canonicalize_l2_snapshot,
    canonicalize_market_trade,
    canonicalize_response_feature,
    canonicalize_trade_flow,
    canonicalize_trade_flow_feature,
    decode_basis_feature_record,
    decode_bbo_feature_record,
    decode_depth_feature_record,
    decode_derivative_state_feature_record,
    decode_response_feature_record,
    decode_trade_flow_feature_record,
    decode_market_trade_record,
    decode_trade_flow_record,
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
    L2_BOOK_FACT_VERSION,
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
from market_data.stream_quality import normalize_stream_quality_classification

from ._shared import db
from .market_data import market_data_repo
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


def _book_observation_key(position: Mapping[str, Any]) -> str:
    return (
        f"{position['definition_id']}:{position['session_id']}:"
        f"{int(position['connection_epoch'])}:{int(position['receive_ordinal'])}:"
        f"{int(position['event_ordinal'])}"
    )


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


def _require_book_operational_rollup(
    session,
    *,
    series_id: int,
    phase: str,
) -> None:
    present = session.execute(
        text(
            "SELECT 1 FROM market.book_operational_rollups "
            "WHERE series_id = :series_id"
        ),
        {"series_id": int(series_id)},
    ).scalar_one_or_none()
    if present is None:
        raise RuntimeError(
            "market_book_operational_rollup_missing: existing canonical book "
            f"material has no seeded counters series_id={int(series_id)} "
            f"phase={phase}; run "
            "scripts/db/manual_migration_book_operational_rollups_v1.sql "
            "with writers stopped"
        )


def _increment_book_operational_rollup(
    session,
    *,
    series_id: int,
    snapshot_delta: int = 0,
    batch_delta: int = 0,
    mutation_delta: int = 0,
    fact_high_water_commit_seq: int = 0,
) -> None:
    counters = {
        "snapshot_delta": int(snapshot_delta),
        "batch_delta": int(batch_delta),
        "mutation_delta": int(mutation_delta),
        "fact_high_water": int(fact_high_water_commit_seq),
    }
    if any(value < 0 for value in counters.values()):
        raise ValueError(
            "market_book_operational_rollup_invalid: counter deltas and high-water "
            f"must be non-negative series_id={int(series_id)} counters={counters}"
        )
    updated = session.execute(
        text(
            """
            UPDATE market.book_operational_rollups
            SET snapshot_count = snapshot_count + :snapshot_delta,
                batch_count = batch_count + :batch_delta,
                mutation_count = mutation_count + :mutation_delta,
                fact_high_water_commit_seq = GREATEST(
                    fact_high_water_commit_seq,
                    :fact_high_water
                ),
                updated_at = now()
            WHERE series_id = :series_id
            RETURNING series_id
            """
        ),
        {"series_id": int(series_id), **counters},
    ).scalar_one_or_none()
    if updated is None:
        _require_book_operational_rollup(
            session,
            series_id=series_id,
            phase="counter_increment",
        )


def _advance_book_fact_rollup(
    session,
    *,
    series_id: int,
    expected_new_fact_count: int,
) -> None:
    high_water = session.execute(
        text(
            """
            SELECT fact_high_water_commit_seq
            FROM market.book_operational_rollups
            WHERE series_id = :series_id
            FOR UPDATE
            """
        ),
        {"series_id": int(series_id)},
    ).scalar_one_or_none()
    if high_water is None:
        _require_book_operational_rollup(
            session,
            series_id=series_id,
            phase="canonical_fact_fold",
        )
    folded = session.execute(
        text(
            """
            SELECT count(*) AS fact_count,
                   count(*) FILTER (
                       WHERE payload ->> 'event_type' = 'snapshot'
                   ) AS snapshot_count,
                   count(*) FILTER (
                       WHERE payload ->> 'event_type' = 'update'
                   ) AS batch_count,
                   COALESCE(sum(
                       CASE WHEN payload ->> 'event_type' = 'update'
                            THEN CAST(payload ->> 'entry_count' AS bigint)
                            ELSE 0
                       END
                   ), 0) AS mutation_count,
                   COALESCE(max(market_commit_seq), :high_water) AS high_water
            FROM market.fact_rows
            WHERE series_id = :series_id
              AND market_commit_seq > :high_water
              AND payload_schema_id = 'market.l2_book.v1'
            """
        ),
        {
            "series_id": int(series_id),
            "high_water": int(high_water),
        },
    ).mappings().one()
    fact_count = int(folded["fact_count"])
    if fact_count != int(expected_new_fact_count):
        raise RuntimeError(
            "market_book_operational_rollup_stale: canonical L2 Fact suffix "
            "does not match the current fenced ingest "
            f"series_id={int(series_id)} expected_new_fact_count="
            f"{int(expected_new_fact_count)} actual_new_fact_count={fact_count}; "
            "stop writers and rerun "
            "scripts/db/manual_migration_book_operational_rollups_v1.sql"
        )
    snapshot_count = int(folded["snapshot_count"])
    batch_count = int(folded["batch_count"])
    if fact_count != snapshot_count + batch_count:
        raise RuntimeError(
            "market_book_operational_rollup_corrupt: unsupported canonical L2 "
            f"event type series_id={int(series_id)} fact_count={fact_count} "
            f"snapshot_count={snapshot_count} batch_count={batch_count}"
        )
    _increment_book_operational_rollup(
        session,
        series_id=series_id,
        snapshot_delta=snapshot_count,
        batch_delta=batch_count,
        mutation_delta=int(folded["mutation_count"]),
        fact_high_water_commit_seq=int(folded["high_water"]),
    )



def _require_book_state_source(
    session,
    *,
    series_id: int,
    position: Mapping[str, Any],
    validity_interval_id: str,
    state_hash: str,
) -> None:
    """Require a canonical archive-acknowledged snapshot or mutation state."""

    observation_key = _book_observation_key(position)
    found = session.execute(
        text(
            """
            SELECT 1
            FROM market.fact_rows
            WHERE series_id = :series_id
              AND observation_key = :observation_key
              AND payload_schema_id = 'market.l2_book.v1'
              AND provenance -> '_qt_l2_evidence' ->> 'connection_epoch'
                    = CAST(:connection_epoch AS text)
              AND provenance -> '_qt_l2_evidence' ->> 'receive_ordinal'
                    = CAST(:receive_ordinal AS text)
              AND provenance -> '_qt_l2_evidence' ->> 'event_ordinal'
                    = CAST(:event_ordinal AS text)
              AND payload ->> 'validity_interval_id' = :validity_interval_id
              AND payload ->> 'after_state_hash' = :state_hash
            LIMIT 1
            """
        ),
        {
            "series_id": int(series_id),
            "observation_key": observation_key,
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


def _require_canonical_typed_material_source(
    session,
    *,
    series_id: int,
    evidence_key: str,
    material_hash: str,
) -> None:
    if not str(evidence_key).startswith("_qt_"):
        raise ValueError("market_feature_storage_invalid: unsafe evidence key")
    found = session.execute(
        text(
            """
            SELECT 1
            FROM market.fact_rows
            WHERE series_id = :series_id
              AND provenance -> :evidence_key ->> 'legacy_material_hash'
                    = :material_hash
            LIMIT 1
            """
        ),
        {
            "series_id": int(series_id),
            "evidence_key": str(evidence_key),
            "material_hash": str(material_hash),
        },
    ).scalar_one_or_none()
    if found is None:
        raise ValueError(
            "market_feature_source_incomplete: canonical typed source material "
            "is missing"
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
            dict.fromkeys(
                str(channel).strip().lower()
                for channel in channels
                if str(channel).strip()
            )
        )
        if not normalized_channels or len(normalized_channels) > 16:
            raise ValueError(
                "market_stream_definition_invalid: one to sixteen channels are required"
            )
        invalid_channels = [
            channel
            for channel in normalized_channels
            if len(channel) > 64
            or not re.fullmatch(r"[a-z0-9][a-z0-9_.-]*", channel)
        ]
        if invalid_channels:
            raise ValueError(
                "market_stream_definition_invalid: channels must use bounded "
                "lowercase identifiers"
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
            series_contract_version = session.execute(
                text(
                    """
                    SELECT contract_version
                    FROM market.series
                    WHERE id = :series_id
                    FOR SHARE
                    """
                ),
                {"series_id": int(series_id)},
            ).scalar_one_or_none()
            if series_contract_version is None:
                raise ValueError(
                    "market_stream_definition_invalid: unknown "
                    f"series_id={int(series_id)}"
                )
            if str(series_contract_version) != str(contract_version):
                raise ValueError(
                    "market_stream_definition_invalid: contract_version "
                    "disagrees with series "
                    f"series_id={int(series_id)} "
                    f"series_contract_version={series_contract_version} "
                    f"definition_contract_version={contract_version}"
                )
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
                            max_segment_bytes, generation, desired_state, config
                        ) VALUES (
                            :id, :identity_key, :source_id, :series_id, :provider,
                            :venue, :product_id, CAST(:channels AS jsonb), :auth_mode,
                            :contract_version, :enabled,
                            :max_spool_bytes, :max_segment_bytes, 1, :desired_state,
                            CAST(:config AS jsonb)
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
                        "desired_state": "running" if insert_enabled else "stopped",
                        "config": _json(config),
                    },
                )
            else:
                next_config = dict(config or {})
                existing_config = dict(existing["config"] or {})
                for operational_key in ("runtime_policy",):
                    if (
                        operational_key in existing_config
                        and operational_key not in next_config
                    ):
                        next_config[operational_key] = existing_config[operational_key]
                # Enrollment owns initial state and reviewed configuration.
                # Once installed, audited lifecycle actions are the only
                # authority allowed to start, stop, pause, or resume a stream.
                next_enabled = bool(existing["enabled"])
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
            if str(contract_version) == L2_BOOK_FACT_VERSION:
                _require_book_operational_rollup(
                    session,
                    series_id=series_id,
                    phase=(
                        "stream_definition_create"
                        if existing is None
                        else "stream_definition_update"
                    ),
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
                           series.timeframe_seconds,
                           sources.source_kind,
                           sources.adapter_version,
                           instruments.symbol AS instrument_symbol,
                           instruments.instrument_type AS instrument_type,
                           leases.owner_id, leases.lease_generation,
                           leases.heartbeat_at, leases.expires_at,
                           CASE WHEN leases.expires_at > now() THEN true ELSE false END AS lease_current
                    FROM market.stream_definitions AS definitions
                    JOIN market.series AS series ON series.id = definitions.series_id
                    JOIN market.sources AS sources ON sources.id = definitions.source_id
                    JOIN portal_instruments AS instruments
                      ON instruments.id = series.instrument_id
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
            if not bounded and str(definition["desired_state"]) != "running":
                raise ValueError(
                    "market_stream_not_desired_running: continuous collection requires desired_state=running"
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
                       definitions.source_id AS definition_source_id,
                       definitions.series_id AS definition_series_id,
                       definitions.provider AS definition_provider,
                       definitions.venue AS definition_venue,
                       definitions.provider_product_id
                           AS definition_provider_product_id,
                       definitions.channels AS definition_channels,
                       definitions.auth_mode AS definition_auth_mode,
                       definitions.contract_version
                           AS definition_contract_version,
                       definitions.max_spool_bytes
                           AS definition_max_spool_bytes,
                       definitions.max_segment_bytes
                           AS definition_max_segment_bytes,
                       definitions.config AS definition_config,
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
            or int(row["definition_source_id"]) != claim.source_id
            or int(row["definition_series_id"]) != claim.series_id
            or str(row["definition_provider"]) != claim.provider
            or str(row["definition_venue"]) != claim.venue
            or str(row["definition_provider_product_id"])
            != claim.provider_product_id
            or tuple(str(value) for value in row["definition_channels"])
            != claim.channels
            or str(row["definition_auth_mode"]) != claim.auth_mode
            or str(row["definition_contract_version"])
            != claim.contract_version
            or int(row["definition_max_spool_bytes"]) != claim.max_spool_bytes
            or int(row["definition_max_segment_bytes"])
            != claim.max_segment_bytes
            or dict(row["definition_config"] or {}) != claim.config
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
        normalized = normalize_stream_quality_classification(classification)
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

    @staticmethod
    def _collection_fence(claim: StreamClaim) -> dict[str, Any]:
        return {
            "fence_kind": "stream",
            "definition_id": claim.definition_id,
            "definition_generation": claim.definition_generation,
            "source_id": claim.source_id,
            "owner_id": claim.owner_id,
            "lease_token": claim.lease_token,
            "lease_generation": claim.lease_generation,
        }

    @staticmethod
    def _canonical_heads(
        *,
        series_id: int,
        observation_keys: Sequence[str],
    ) -> dict[str, Mapping[str, Any]]:
        if not observation_keys:
            return {}
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT DISTINCT ON (observation_key)
                           observation_key, material_hash, row_hash,
                           market_commit_seq
                    FROM market.fact_versions
                    WHERE series_id = :series_id
                      AND observation_key = ANY(:observation_keys)
                    ORDER BY observation_key, revision DESC
                    """
                ),
                {
                    "series_id": int(series_id),
                    "observation_keys": list(observation_keys),
                },
            ).mappings().all()
        return {str(row["observation_key"]): dict(row) for row in rows}

    @staticmethod
    def _resolve_derived_source_id(series_id: int) -> int:
        with db.session() as session:
            row = session.execute(
                text(
                    """
                    SELECT min(source_bounds.min_source_id) AS min_source_id,
                           max(source_bounds.max_source_id) AS max_source_id
                    FROM market.series AS derived_series
                    JOIN market.series AS trade_series
                      ON trade_series.instrument_id = derived_series.instrument_id
                     AND trade_series.fact_type = 'market.trade'
                    JOIN LATERAL (
                        SELECT min(facts.source_id) AS min_source_id,
                               max(facts.source_id) AS max_source_id
                        FROM market.fact_versions AS facts
                        WHERE facts.series_id = trade_series.id
                    ) AS source_bounds
                      ON source_bounds.min_source_id IS NOT NULL
                    WHERE derived_series.id = :series_id
                    """
                ),
                {"series_id": int(series_id)},
            ).mappings().one()
        min_source_id = row["min_source_id"]
        max_source_id = row["max_source_id"]
        if min_source_id is None or min_source_id != max_source_id:
            source_count = "0" if min_source_id is None else ">1"
            raise RuntimeError(
                "market_trade_flow_source_invalid: expected exactly one canonical "
                f"upstream trade source series_id={int(series_id)} "
                f"source_count={source_count}"
            )
        return int(min_source_id)

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
                item.provider_sequence_num
                if item.provider_sequence_num is not None
                else 2**63,
                item.receive_ordinal,
                item.event_ordinal,
                item.trade_ordinal,
                item.provider_trade_id,
            ),
        )
        if not rows:
            return TradeIngestionOutcome(
                requested_count=0,
                inserted_count=0,
                noop_count=0,
                max_commit_seq=0,
                records=(),
            )

        with db.session() as session:
            self._require_fence(session, claim)
            for fact in rows:
                if fact.provider_product_id != claim.provider_product_id:
                    raise ValueError(
                        "market_trade_ingest_invalid: product scope mismatch"
                    )
                if require_archive_mapping:
                    mapped = session.execute(
                        text(
                            """
                            SELECT 1
                            FROM market.raw_archive_record_mappings
                            WHERE raw_record_id = :raw_record_id
                            LIMIT 1
                            """
                        ),
                        {"raw_record_id": fact.raw_record_id},
                    ).first()
                    if mapped is None:
                        raise ValueError(
                            "market_trade_archive_pending: canonical publication "
                            "requires acknowledged mapping"
                        )

        source = market_data_repo.get_source_identity(claim.source_id)
        canonical = [
            canonicalize_market_trade(
                fact,
                source=source,
                provenance={
                    "stream_definition_id": claim.definition_id,
                    "stream_session_id": claim.session_id,
                },
            )
            for fact in rows
        ]
        canonical_by_key: dict[str, Any] = {}
        batch_noop_count = 0
        for fact in canonical:
            prior = canonical_by_key.get(fact.observation_key)
            if prior is None:
                canonical_by_key[fact.observation_key] = fact
                continue
            if prior.material_hash != fact.material_hash:
                raise MarketTradeConflictError(
                    "market_trade_conflict: same provider trade ID has divergent "
                    f"material product_id={fact.external_event_group_key} "
                    f"trade_id={fact.external_event_key}"
                )
            batch_noop_count += 1
        canonical = list(canonical_by_key.values())
        keys = [fact.observation_key for fact in canonical]
        heads = self._canonical_heads(
            series_id=claim.series_id,
            observation_keys=keys,
        )
        pending = []
        noop_count = batch_noop_count
        for fact in canonical:
            head = heads.get(fact.observation_key)
            if head is None:
                pending.append(fact)
                continue
            if str(head["material_hash"]) != fact.material_hash:
                raise MarketTradeConflictError(
                    "market_trade_conflict: same provider trade ID has divergent "
                    f"material product_id={fact.external_event_group_key} "
                    f"trade_id={fact.external_event_key}"
                )
            noop_count += 1

        if not pending:
            return TradeIngestionOutcome(
                requested_count=len(rows),
                inserted_count=0,
                noop_count=noop_count,
                max_commit_seq=max(
                    (int(row["market_commit_seq"]) for row in heads.values()),
                    default=0,
                ),
                records=(),
            )

        outcome = market_data_repo.ingest_facts(
            series_id=claim.series_id,
            source_id=claim.source_id,
            facts=pending,
            request={
                "operation": "market_trade_stream_canonicalization",
                "definition_id": claim.definition_id,
                "session_id": claim.session_id,
            },
            source_revision=claim.contract_version,
            allow_corrections=False,
            collection_fence=self._collection_fence(claim),
        )
        pending_keys = {fact.observation_key for fact in pending}
        stored = market_data_repo.read_facts(
            series_id=claim.series_id,
            start=min(fact.observation_time for fact in pending),
            end=max(fact.observation_time for fact in pending)
            + timedelta(microseconds=1),
            as_of_commit_seq=outcome.max_commit_seq,
        )
        records = tuple(
            decode_market_trade_record(record)
            for record in stored
            if record.fact.observation_key in pending_keys
        )
        if len(records) != outcome.inserted_count:
            raise RuntimeError(
                "market_trade_ingest_corrupt: canonical write/read count mismatch "
                f"expected={outcome.inserted_count} actual={len(records)}"
            )
        return TradeIngestionOutcome(
            requested_count=len(rows),
            inserted_count=outcome.inserted_count,
            noop_count=noop_count + outcome.noop_count,
            max_commit_seq=outcome.max_commit_seq,
            records=records,
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
        records = [
            decode_market_trade_record(record)
            for record in market_data_repo.read_facts(
                series_id=int(series_id),
                start=_utc(start),
                end=_utc(end),
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
            )
        ]
        return sorted(
            records,
            key=lambda record: (
                record.fact.provider_event_time,
                record.fact.provider_sequence_num
                if record.fact.provider_sequence_num is not None
                else 2**63,
                record.fact.receive_ordinal,
                record.fact.event_ordinal,
                record.fact.trade_ordinal,
                record.fact.provider_trade_id,
            ),
        )

    def ingest_aggregates(
        self,
        *,
        series_id: int,
        facts: Iterable[TradeFlowAggregateFact],
        aggregation_version: str = "market.trade_flow.v1",
    ) -> AggregateIngestionOutcome:
        ordered = sorted(
            facts,
            key=lambda fact: (
                fact.bucket_start,
                fact.interval_seconds,
                fact.known_at,
            ),
        )
        if not ordered:
            return AggregateIngestionOutcome(
                inserted_count=0,
                noop_count=0,
                max_commit_seq=0,
                records=(),
            )
        source_id = self._resolve_derived_source_id(int(series_id))
        source = market_data_repo.get_source_identity(source_id)
        canonical = [
            canonicalize_trade_flow(
                fact,
                source=source,
                aggregation_version=aggregation_version,
                provenance={"source_kind": "canonical_market_trade_aggregation"},
            )
            for fact in ordered
        ]
        keys = [fact.observation_key for fact in canonical]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "market_trade_flow_ingest_invalid: duplicate canonical "
                "observation key"
            )
        heads = self._canonical_heads(
            series_id=int(series_id),
            observation_keys=keys,
        )
        pending = [
            fact
            for fact in canonical
            if (
                fact.observation_key not in heads
                or str(heads[fact.observation_key]["material_hash"])
                != fact.material_hash
            )
        ]
        noop_count = len(canonical) - len(pending)
        if not pending:
            return AggregateIngestionOutcome(
                inserted_count=0,
                noop_count=noop_count,
                max_commit_seq=max(
                    (int(row["market_commit_seq"]) for row in heads.values()),
                    default=0,
                ),
                records=(),
            )

        outcome = market_data_repo.ingest_facts(
            series_id=int(series_id),
            source_id=source_id,
            facts=pending,
            request={
                "operation": "market_trade_flow_materialization",
                "aggregation_version": str(aggregation_version),
            },
            source_revision=str(aggregation_version),
            allow_corrections=True,
        )
        pending_keys = {fact.observation_key for fact in pending}
        stored = market_data_repo.read_facts(
            series_id=int(series_id),
            start=min(fact.observation_time for fact in pending),
            end=max(fact.observation_time for fact in pending)
            + timedelta(microseconds=1),
            as_of_commit_seq=outcome.max_commit_seq,
        )
        records = tuple(
            decode_trade_flow_record(record)
            for record in stored
            if record.fact.observation_key in pending_keys
        )
        written_count = outcome.inserted_count + outcome.corrected_count
        if len(records) != written_count:
            raise RuntimeError(
                "market_trade_flow_ingest_corrupt: canonical write/read count "
                f"mismatch expected={written_count} actual={len(records)}"
            )
        return AggregateIngestionOutcome(
            inserted_count=written_count,
            noop_count=noop_count + outcome.noop_count,
            max_commit_seq=outcome.max_commit_seq,
            records=records,
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
        records = [
            decode_trade_flow_record(record)
            for record in market_data_repo.read_facts(
                series_id=int(series_id),
                start=_utc(start),
                end=_utc(end),
                as_of_commit_seq=as_of_commit_seq,
                known_at_lte=known_at_lte,
            )
        ]
        invalid = [
            record.fact.interval_seconds
            for record in records
            if record.fact.interval_seconds != int(interval_seconds)
        ]
        if invalid:
            raise RuntimeError(
                "market_trade_flow_read_corrupt: interval disagrees with series "
                f"series_id={int(series_id)} requested={int(interval_seconds)} "
                f"actual={invalid[0]}"
            )
        return sorted(records, key=lambda record: record.fact.bucket_start)

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
        """Persist canonical book Facts and operational state atomically."""

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
        source = market_data_repo.get_source_identity(claim.source_id)
        canonical_provenance = {
            "stream_definition_id": claim.definition_id,
            "stream_session_id": claim.session_id,
        }
        canonical_snapshots = [
            canonicalize_l2_snapshot(
                fact,
                source=source,
                provenance=canonical_provenance,
            )
            for fact in snapshot_rows
        ]
        canonical_batches = [
            canonicalize_l2_mutation_batch(
                fact,
                source=source,
                provenance=canonical_provenance,
            )
            for fact in batch_rows
        ]
        observation_keys = [
            fact.observation_key
            for fact in (*canonical_snapshots, *canonical_batches)
        ]
        if len(observation_keys) != len(set(observation_keys)):
            raise ValueError(
                "market_l2_ingest_invalid: duplicate canonical observation key"
            )

        inserted_snapshots = 0
        noop_snapshots = 0
        inserted_batches = 0
        noop_batches = 0
        inserted_validity = 0
        max_commit_seq = 0
        with db.session() as session:
            self._require_fence(session, claim)
            for fact in (*snapshot_rows, *batch_rows):
                if fact.series_id != claim.series_id:
                    raise ValueError(
                        "market_l2_ingest_invalid: canonical Fact series mismatch"
                    )
                mapped = session.execute(
                    text(
                        "SELECT 1 FROM market.raw_archive_record_mappings "
                        "WHERE raw_record_id = :raw_record_id LIMIT 1"
                    ),
                    {"raw_record_id": fact.event.raw_record_id},
                ).scalar_one_or_none()
                if mapped is None:
                    raise ValueError(
                        "market_l2_archive_incomplete: raw record is not acknowledged"
                    )

            if canonical_snapshots:
                outcome = market_data_repo.ingest_l2_book_facts_in_session(
                    session,
                    series_id=claim.series_id,
                    source_id=claim.source_id,
                    facts=canonical_snapshots,
                    request={
                        "operation": "market_l2_snapshot_stream_canonicalization",
                        "definition_id": claim.definition_id,
                        "session_id": claim.session_id,
                    },
                    source_revision=claim.contract_version,
                    allow_corrections=False,
                    collection_fence=self._collection_fence(claim),
                )
                if (
                    outcome.corrected_count != 0
                    or outcome.inserted_count + outcome.noop_count
                    != len(canonical_snapshots)
                ):
                    raise RuntimeError(
                        "market_l2_ingest_corrupt: canonical snapshot outcome mismatch"
                    )
                inserted_snapshots = outcome.inserted_count
                noop_snapshots = outcome.noop_count
                max_commit_seq = max(max_commit_seq, outcome.max_commit_seq)

            if canonical_batches:
                batch_outcome = market_data_repo.ingest_l2_book_facts_in_session(
                    session,
                    series_id=claim.series_id,
                    source_id=claim.source_id,
                    facts=canonical_batches,
                    request={
                        "operation": "market_l2_mutation_stream_canonicalization",
                        "definition_id": claim.definition_id,
                        "session_id": claim.session_id,
                    },
                    source_revision=claim.contract_version,
                    allow_corrections=False,
                    collection_fence=self._collection_fence(claim),
                )
                if (
                    batch_outcome.corrected_count != 0
                    or batch_outcome.inserted_count + batch_outcome.noop_count
                    != len(canonical_batches)
                ):
                    raise RuntimeError(
                        "market_l2_ingest_corrupt: canonical mutation outcome mismatch"
                    )
                inserted_batches = batch_outcome.inserted_count
                noop_batches = batch_outcome.noop_count
                max_commit_seq = max(
                    max_commit_seq,
                    batch_outcome.max_commit_seq,
                )

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

            _advance_book_fact_rollup(
                session,
                series_id=claim.series_id,
                expected_new_fact_count=(
                    inserted_snapshots + inserted_batches
                ),
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
            claim.contract_version != L2_BOOK_FACT_VERSION
            or int(checkpoint.series_id) != int(claim.series_id)
            or checkpoint.source_position.definition_id != claim.definition_id
            or checkpoint.source_position.session_id != claim.session_id
            or checkpoint.source_position.provider_product_id
            != claim.provider_product_id
        ):
            raise ValueError(
                "market_book_checkpoint_commit_invalid: checkpoint scope "
                "disagrees with stream claim"
            )
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
            _require_book_operational_rollup(
                session,
                series_id=checkpoint.series_id,
                phase="checkpoint_commit",
            )
            source_count = int(
                session.execute(
                    text(
                        """
                        SELECT count(*)
                        FROM market.raw_archive_manifests AS manifests
                        WHERE manifests.definition_id = :definition_id
                          AND manifests.session_id = :session_id
                          AND manifests.connection_epoch = :connection_epoch
                          AND manifests.first_receive_ordinal <= :receive_ordinal
                          AND manifests.last_receive_ordinal >= :receive_ordinal
                          AND manifests.id = ANY(:manifest_ids)
                          AND EXISTS (
                              SELECT 1
                              FROM market.raw_archive_ranges AS ranges
                              WHERE ranges.manifest_id = manifests.id
                                AND ranges.provider_product_id =
                                    :provider_product_id
                          )
                        """
                    ),
                    {
                        "definition_id": claim.definition_id,
                        "session_id": claim.session_id,
                        "connection_epoch": (
                            checkpoint.source_position.connection_epoch
                        ),
                        "receive_ordinal": (
                            checkpoint.source_position.receive_ordinal
                        ),
                        "provider_product_id": claim.provider_product_id,
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
                        "SELECT external_event_component_key "
                        "FROM market.fact_rows "
                        "WHERE payload_schema_id = 'market.l2_book.v1' "
                        "AND payload ->> 'event_type' = 'snapshot' "
                        "AND provenance -> '_qt_l2_evidence' ->> 'definition_id' "
                        "= :definition_id "
                        "AND provenance -> '_qt_l2_evidence' ->> 'session_id' "
                        "= :session_id "
                        "ORDER BY "
                        "CAST(provenance -> '_qt_l2_evidence' ->> "
                        "'connection_epoch' AS bigint), "
                        "CAST(provenance -> '_qt_l2_evidence' ->> "
                        "'receive_ordinal' AS bigint), "
                        "CAST(provenance -> '_qt_l2_evidence' ->> "
                        "'event_ordinal' AS bigint)"
                    ),
                    {"definition_id": definition_id, "session_id": session_id},
                ).scalars()
            )
            stored_batches = tuple(
                str(value)
                for value in session.execute(
                    text(
                        "SELECT external_event_component_key "
                        "FROM market.fact_rows "
                        "WHERE payload_schema_id = 'market.l2_book.v1' "
                        "AND payload ->> 'event_type' = 'update' "
                        "AND provenance -> '_qt_l2_evidence' ->> 'definition_id' "
                        "= :definition_id "
                        "AND provenance -> '_qt_l2_evidence' ->> 'session_id' "
                        "= :session_id "
                        "ORDER BY "
                        "CAST(provenance -> '_qt_l2_evidence' ->> "
                        "'connection_epoch' AS bigint), "
                        "CAST(provenance -> '_qt_l2_evidence' ->> "
                        "'receive_ordinal' AS bigint), "
                        "CAST(provenance -> '_qt_l2_evidence' ->> "
                        "'event_ordinal' AS bigint)"
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
                    SELECT id, series_id, provider_product_id, contract_version,
                           enabled,
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
                           count(*) AS bucket_count,
                           count(*) FILTER (
                               WHERE aggregate_complete
                           ) AS complete_bucket_count,
                           count(*) FILTER (
                               WHERE NOT aggregate_complete
                           ) AS incomplete_bucket_count,
                           max(bucket_end) AS latest_bucket_end
                    FROM (
                        SELECT DISTINCT ON (series_id, observation_key)
                               series_id,
                               (
                                   provenance -> '_qt_trade_flow_evidence'
                                   ->> 'interval_seconds'
                               )::integer AS interval_seconds,
                               COALESCE(
                                   (
                                       quality -> '_qt_trade_flow_quality'
                                       ->> 'aggregate_complete'
                                   )::boolean,
                                   false
                               ) AS aggregate_complete,
                               market.canonical_fact_utc_timestamp(
                                   payload ->> 'bucket_end'
                               ) AS bucket_end
                        FROM market.fact_rows
                        WHERE series_id = ANY(:series_ids)
                          AND fact_type = 'market.trade_flow'
                        ORDER BY series_id, observation_key, revision DESC,
                                 market_commit_seq DESC
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
                           rollup.series_id AS rollup_series_id,
                           COALESCE(rollup.snapshot_count, 0) AS snapshot_count,
                           COALESCE(rollup.batch_count, 0) AS batch_count,
                           COALESCE(rollup.mutation_count, 0) AS mutation_count,
                           COALESCE(rollup.checkpoint_count, 0) AS checkpoint_count,
                           rollup.checkpoint_high_water_acknowledged_at,
                           rollup.checkpoint_high_water_id,
                           COALESCE(
                               rollup.fact_high_water_commit_seq,
                               0
                           ) AS counter_fact_high_water_commit_seq,
                           rollup.updated_at AS counter_updated_at,
                           latest_fact.market_commit_seq AS latest_fact_commit_seq,
                           latest_checkpoint.id AS latest_checkpoint_id,
                           latest_checkpoint.acknowledged_at
                               AS latest_checkpoint_acknowledged_at
                    FROM (SELECT CAST(:series_id AS bigint) AS requested_series_id) AS scope
                    LEFT JOIN market.book_reconstruction_state AS state
                      ON state.series_id = scope.requested_series_id
                    LEFT JOIN market.book_operational_rollups AS rollup
                      ON rollup.series_id = scope.requested_series_id
                    LEFT JOIN LATERAL (
                        SELECT facts.market_commit_seq
                        FROM market.fact_versions AS facts
                        WHERE CAST(:is_l2 AS boolean)
                          AND facts.series_id = scope.requested_series_id
                          AND facts.payload_schema_id = 'market.l2_book.v1'
                        ORDER BY facts.market_commit_seq DESC
                        LIMIT 1
                    ) AS latest_fact ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT checkpoints.id, checkpoints.acknowledged_at
                        FROM market.book_checkpoint_manifests AS checkpoints
                        WHERE CAST(:is_l2 AS boolean)
                          AND checkpoints.series_id = scope.requested_series_id
                        ORDER BY checkpoints.acknowledged_at DESC,
                                 checkpoints.id DESC
                        LIMIT 1
                    ) AS latest_checkpoint ON TRUE
                    """
                ),
                {
                    "series_id": int(definition["series_id"]),
                    "is_l2": (
                        str(definition["contract_version"])
                        == L2_BOOK_FACT_VERSION
                    ),
                },
            ).mappings().one()
            book_reconstruction = dict(book_state)
            rollup_series_id = book_reconstruction.pop("rollup_series_id")
            latest_fact_commit_seq = int(
                book_reconstruction.pop("latest_fact_commit_seq") or 0
            )
            latest_checkpoint_id = book_reconstruction.pop("latest_checkpoint_id")
            latest_checkpoint_acknowledged_at = book_reconstruction.pop(
                "latest_checkpoint_acknowledged_at"
            )
            if (
                str(definition["contract_version"]) == L2_BOOK_FACT_VERSION
                and rollup_series_id is None
            ):
                raise RuntimeError(
                    "market_book_operational_rollup_missing: Level 2 definition "
                    f"has no seeded counters series_id={int(definition['series_id'])}; "
                    "run scripts/db/manual_migration_book_operational_rollups_v1.sql "
                    "with writers stopped"
                )
            counter_high_water = int(
                book_reconstruction["counter_fact_high_water_commit_seq"]
            )
            if (
                str(definition["contract_version"]) == L2_BOOK_FACT_VERSION
                and latest_fact_commit_seq != counter_high_water
            ):
                raise RuntimeError(
                    "market_book_operational_rollup_stale: latest canonical "
                    "Level 2 Fact is not represented by the bounded status "
                    f"projection series_id={int(definition['series_id'])} "
                    f"latest_fact_commit_seq={latest_fact_commit_seq} "
                    f"counter_fact_high_water_commit_seq={counter_high_water}; "
                    "stop writers and rerun "
                    "scripts/db/manual_migration_book_operational_rollups_v1.sql"
                )
            if str(definition["contract_version"]) == L2_BOOK_FACT_VERSION and (
                latest_checkpoint_id
                != book_reconstruction["checkpoint_high_water_id"]
                or latest_checkpoint_acknowledged_at
                != book_reconstruction[
                    "checkpoint_high_water_acknowledged_at"
                ]
            ):
                raise RuntimeError(
                    "market_book_operational_rollup_stale: latest book "
                    "checkpoint is not represented by the bounded status "
                    f"projection series_id={int(definition['series_id'])} "
                    f"latest_checkpoint_id={latest_checkpoint_id} "
                    "counter_checkpoint_high_water_id="
                    f"{book_reconstruction['checkpoint_high_water_id']}; "
                    "stop writers and rerun "
                    "scripts/db/manual_migration_book_operational_rollups_v1.sql"
                )
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
            "book_reconstruction": book_reconstruction,
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

    def get_book_reconstruction_state(
        self,
        *,
        definition_id: str,
        session_id: str,
        connection_epoch: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """Return the disposable book watermark for one resumable epoch."""

        epoch_filter = (
            "AND state.connection_epoch = :connection_epoch"
            if connection_epoch is not None
            else ""
        )
        with db.session() as session:
            row = session.execute(
                text(
                    f"""
                    SELECT state.*
                    FROM market.book_reconstruction_state AS state
                    JOIN market.stream_definitions AS definitions
                      ON definitions.series_id = state.series_id
                     AND definitions.id = :definition_id
                    WHERE state.definition_id = :definition_id
                      AND state.session_id = :session_id
                      {epoch_filter}
                    LIMIT 1
                    """
                ),
                {
                    "definition_id": str(definition_id),
                    "session_id": str(session_id),
                    "connection_epoch": (
                        int(connection_epoch)
                        if connection_epoch is not None
                        else None
                    ),
                },
            ).mappings().first()
        return dict(row) if row is not None else None

    def get_book_fact_accepted_at(
        self,
        *,
        series_id: int,
        position: Mapping[str, Any],
    ) -> Optional[datetime]:
        """Return the immutable acceptance clock for an already-persisted event."""

        with db.session() as session:
            value = session.execute(
                text(
                    """
                    SELECT accepted_at
                    FROM market.fact_versions
                    WHERE series_id = :series_id
                      AND observation_key = :observation_key
                      AND payload_schema_id = 'market.l2_book.v1'
                    ORDER BY revision DESC
                    LIMIT 1
                    """
                ),
                {
                    "series_id": int(series_id),
                    "observation_key": _book_observation_key(position),
                },
            ).scalar_one_or_none()
        return _utc(value) if value is not None else None

    def get_book_validity_opening(
        self, *, series_id: int, interval_id: str
    ) -> Optional[dict[str, Any]]:
        """Load the immutable opening revision needed for checkpoint restore."""

        with db.session() as session:
            row = session.execute(
                text(
                    """
                    SELECT *
                    FROM market.book_validity_interval_versions
                    WHERE series_id = :series_id
                      AND interval_id = :interval_id
                      AND revision = 1
                    LIMIT 1
                    """
                ),
                {
                    "series_id": int(series_id),
                    "interval_id": str(interval_id),
                },
            ).mappings().first()
        return dict(row) if row is not None else None

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
                            FROM market.fact_versions
                            WHERE series_id IN (:futures_bbo, :spot_bbo)
                              AND payload_schema_id = 'market.bbo.v1'
                              AND observation_time >= :start
                              AND observation_time < :end
                              AND known_at <= :known_at
                        ), 0),
                        COALESCE((
                            SELECT MAX(market_commit_seq)
                            FROM market.fact_versions
                            WHERE series_id = :oi_series
                              AND fact_type = 'derivatives.open_interest'
                              AND observation_time >= :start
                              AND observation_time < :end
                              AND known_at <= :known_at
                        ), 0),
                        COALESCE((
                            SELECT MAX(market_commit_seq)
                            FROM market.fact_versions
                            WHERE series_id = :funding_series
                              AND fact_type = 'derivatives.funding_rate'
                              AND observation_time >= :start
                              AND observation_time < :end
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
        """Persist one deterministic canonical market-state feature batch."""

        typed_families = (
            (
                "market_bbo_feature_canonicalization",
                tuple(bbo_facts),
                canonicalize_bbo_feature,
            ),
            (
                "market_depth_feature_canonicalization",
                tuple(depth_facts),
                canonicalize_depth_feature,
            ),
            (
                "market_trade_flow_feature_canonicalization",
                tuple(flow_facts),
                canonicalize_trade_flow_feature,
            ),
            (
                "market_futures_spot_basis_canonicalization",
                tuple(basis_facts),
                canonicalize_basis_feature,
            ),
            (
                "market_derivative_state_canonicalization",
                tuple(derivative_facts),
                canonicalize_derivative_state_feature,
            ),
            (
                "market_response_feature_canonicalization",
                tuple(response_facts),
                canonicalize_response_feature,
            ),
        )
        requested_count = sum(
            len(rows) for _operation, rows, _adapter in typed_families
        )
        if requested_count == 0:
            return FeatureIngestionOutcome(
                inserted_count=0,
                noop_count=0,
                max_commit_seq=0,
                material_hashes=(),
            )
        derived_source_id = market_data_repo.register_source(
            DERIVED_MARKET_STATE_SOURCE,
            lineage={
                "schema_version": "market.derived_source_lineage.v1",
                "authority": "QT deterministic market-state transforms",
            },
        )
        canonical_families = tuple(
            (
                operation,
                tuple(
                    (fact, adapter(fact, source=DERIVED_MARKET_STATE_SOURCE))
                    for fact in rows
                ),
            )
            for operation, rows, adapter in typed_families
        )
        material_hashes = tuple(
            fact.material_hash
            for _operation, rows, _adapter in typed_families
            for fact in rows
        )
        inserted = 0
        noop = 0
        max_commit_seq = 0

        with db.session() as session:
            bbo_rows = typed_families[0][1]
            depth_rows = typed_families[1][1]
            flow_rows = typed_families[2][1]
            basis_rows = typed_families[3][1]
            derivative_rows = typed_families[4][1]
            response_rows = typed_families[5][1]

            for fact in (*bbo_rows, *depth_rows):
                _require_book_state_source(
                    session,
                    series_id=fact.source_l2_series_id,
                    position=fact.source_position.material(),
                    validity_interval_id=fact.validity_interval_id,
                    state_hash=fact.source_state_hash,
                )
            for fact in flow_rows:
                _require_material_source(
                    session,
                    table_name="fact_versions",
                    series_id=fact.source_trade_flow_series_id,
                    material_hash=fact.aggregate_material_hash,
                )
            for fact in basis_rows:
                _require_canonical_typed_material_source(
                    session,
                    series_id=fact.futures_series_id,
                    evidence_key="_qt_bbo_evidence",
                    material_hash=fact.futures_bbo_material_hash,
                )
                _require_canonical_typed_material_source(
                    session,
                    series_id=fact.spot_series_id,
                    evidence_key="_qt_bbo_evidence",
                    material_hash=fact.spot_bbo_material_hash,
                )
            for fact in derivative_rows:
                for series_id, commit_seq in (
                    (fact.oi_series_id, fact.oi_market_commit_seq),
                    (fact.funding_series_id, fact.funding_market_commit_seq),
                ):
                    if series_id is None:
                        continue
                    found = session.execute(
                        text(
                            "SELECT 1 FROM market.fact_versions "
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
                            "market_feature_source_incomplete: canonical "
                            "derivative source is missing"
                        )
            for fact in response_rows:
                _require_canonical_typed_material_source(
                    session,
                    series_id=fact.source_flow_feature_series_id,
                    evidence_key="_qt_trade_flow_feature_evidence",
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

            for operation, paired_rows in canonical_families:
                by_series: dict[int, list[Any]] = defaultdict(list)
                for typed, canonical in paired_rows:
                    by_series[typed.series_id].append(canonical)
                for feature_series_id, facts_for_series in sorted(by_series.items()):
                    outcome = market_data_repo.ingest_facts_in_session(
                        session,
                        series_id=feature_series_id,
                        source_id=derived_source_id,
                        facts=facts_for_series,
                        request={
                            "operation": operation,
                            "source_series_kind": "canonical_market_state",
                        },
                        source_revision=DERIVED_MARKET_STATE_SOURCE.adapter_version,
                        allow_corrections=True,
                    )
                    if (
                        outcome.inserted_count
                        + outcome.corrected_count
                        + outcome.noop_count
                        != len(facts_for_series)
                    ):
                        raise RuntimeError(
                            "market_feature_ingest_corrupt: canonical writer "
                            "outcome mismatch"
                        )
                    inserted += outcome.inserted_count + outcome.corrected_count
                    noop += outcome.noop_count
                    max_commit_seq = max(max_commit_seq, outcome.max_commit_seq)

        return FeatureIngestionOutcome(
            inserted_count=inserted,
            noop_count=noop,
            max_commit_seq=max_commit_seq,
            material_hashes=material_hashes,
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
        records = market_data_repo.read_facts(
            series_id=int(series_id),
            start=_utc(start),
            end=_utc(end),
            known_at_lte=_utc(known_at),
            as_of_commit_seq=as_of_commit_seq,
        )
        return tuple(
            decode_bbo_feature_record(record).fact for record in records
        )

    def read_depth_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[DepthFeatureFact, ...]:
        records = market_data_repo.read_facts(
            series_id=int(series_id),
            start=_utc(start),
            end=_utc(end),
            known_at_lte=_utc(known_at),
            as_of_commit_seq=as_of_commit_seq,
        )
        return tuple(
            decode_depth_feature_record(record).fact for record in records
        )

    def read_trade_flow_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[TradeFlowFeatureFact, ...]:
        return tuple(
            decode_trade_flow_feature_record(record).fact
            for record in market_data_repo.read_facts(
                series_id=int(series_id),
                start=_utc(start),
                end=_utc(end),
                known_at_lte=_utc(known_at),
                as_of_commit_seq=as_of_commit_seq,
            )
        )

    def read_basis_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[BasisFeatureFact, ...]:
        return tuple(
            decode_basis_feature_record(record).fact
            for record in market_data_repo.read_facts(
                series_id=int(series_id),
                start=_utc(start),
                end=_utc(end),
                known_at_lte=_utc(known_at),
                as_of_commit_seq=as_of_commit_seq,
            )
        )

    def read_derivative_state_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[DerivativeStateFeatureFact, ...]:
        return tuple(
            decode_derivative_state_feature_record(record).fact
            for record in market_data_repo.read_facts(
                series_id=int(series_id),
                start=_utc(start),
                end=_utc(end),
                known_at_lte=_utc(known_at),
                as_of_commit_seq=as_of_commit_seq,
            )
        )

    def read_response_features(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at: datetime,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[ResponseFeatureFact, ...]:
        return tuple(
            decode_response_feature_record(record).fact
            for record in market_data_repo.read_facts(
                series_id=int(series_id),
                start=_utc(start),
                end=_utc(end),
                known_at_lte=_utc(known_at),
                as_of_commit_seq=as_of_commit_seq,
            )
        )

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
        """Read a canonical market-state family as typed projections."""

        normalized_type = str(fact_type or "").strip().lower()
        decoders = {
            "market.bbo": decode_bbo_feature_record,
            "market.depth_observation": decode_depth_feature_record,
            "market.trade_flow_feature": decode_trade_flow_feature_record,
            "market.futures_spot_relationship": decode_basis_feature_record,
            "market.derivative_state": decode_derivative_state_feature_record,
            "market.market_response": decode_response_feature_record,
        }
        decoder = decoders.get(normalized_type)
        if decoder is None:
            raise ValueError(
                f"market_feature_read_unsupported: fact_type={normalized_type or '<missing>'}"
            )
        decision_time = known_at or datetime.max.replace(tzinfo=UTC)
        return tuple(
            decoder(record)
            for record in market_data_repo.read_facts(
                series_id=int(series_id),
                start=_utc(start),
                end=_utc(end),
                known_at_lte=_utc(decision_time),
                as_of_commit_seq=as_of_commit_seq,
            )
        )


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
