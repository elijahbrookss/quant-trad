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
from market_data.order_book import (
    BOOK_CHECKPOINT_SCHEMA_VERSION,
    BOOK_RECONSTRUCTION_VERSION,
    BookCheckpointFact,
    BookLifecycle,
    BookValidityIntervalVersion,
    L2MutationBatchFact,
    L2SnapshotFact,
)
from market_data.structure import (
    MarketSide,
    MarketTradeFact,
    MarketTradeRecord,
    RawStreamRecord,
    TradeCoverageIntervalVersion,
    TradeDeliveryKind,
    TradeFlowAggregateFact,
    TradeFlowAggregateRecord,
)

from ._shared import db


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


class PostgresMarketStructureRepository:
    """One transactional authority for Phase 1 stream facts and projections."""

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
        enabled: bool = False,
        production_admitted: bool = False,
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
        material = {
            "auth_mode": normalized_auth,
            "max_spool_bytes": spool_bytes,
            "max_segment_bytes": segment_bytes,
            "enabled": bool(enabled),
            "production_admitted": bool(production_admitted),
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
                            enabled, production_admitted, max_spool_bytes,
                            max_segment_bytes, generation, config
                        ) VALUES (
                            :id, :identity_key, :source_id, :series_id, :provider,
                            :venue, :product_id, CAST(:channels AS jsonb), :auth_mode,
                            :contract_version, :enabled, :production_admitted,
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
                        "enabled": bool(enabled),
                        "production_admitted": bool(production_admitted),
                        "max_spool_bytes": spool_bytes,
                        "max_segment_bytes": segment_bytes,
                        "config": _json(config),
                    },
                )
            else:
                existing_material = {
                    "auth_mode": existing["auth_mode"],
                    "max_spool_bytes": int(existing["max_spool_bytes"]),
                    "max_segment_bytes": int(existing["max_segment_bytes"]),
                    "enabled": bool(existing["enabled"]),
                    "production_admitted": bool(existing["production_admitted"]),
                    "config": dict(existing["config"] or {}),
                }
                generation = int(existing["generation"]) + (existing_material != material)
                session.execute(
                    text(
                        """
                        UPDATE market.stream_definitions
                        SET auth_mode = :auth_mode, enabled = :enabled,
                            production_admitted = :production_admitted,
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
                        "enabled": bool(enabled),
                        "production_admitted": bool(production_admitted),
                        "max_spool_bytes": spool_bytes,
                        "max_segment_bytes": segment_bytes,
                        "generation": generation,
                        "config": _json(config),
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
                    SELECT definitions.*, leases.owner_id, leases.lease_generation,
                           leases.heartbeat_at, leases.expires_at,
                           CASE WHEN leases.expires_at > now() THEN true ELSE false END AS lease_current
                    FROM market.stream_definitions AS definitions
                    LEFT JOIN market.stream_lease_state AS leases
                      ON leases.definition_id = definitions.id
                    {predicate}
                    ORDER BY definitions.id
                    """
                ),
                {"definition_id": definition_id} if definition_id else {},
            ).mappings().all()
        return [dict(row) for row in rows]

    def claim_stream(
        self,
        *,
        definition_id: str,
        owner_id: str,
        lease_seconds: float,
        bounded: bool,
    ) -> StreamClaim:
        owner = str(owner_id or "").strip()
        ttl = float(lease_seconds)
        if not owner or ttl <= 0:
            raise ValueError("market_stream_claim_invalid: owner and positive lease required")
        token = secrets.token_urlsafe(32)
        token_hash = _token_hash(token)
        session_id = f"mss_{uuid.uuid4().hex}"
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
            if not bounded and (
                not bool(definition["enabled"])
                or not bool(definition["production_admitted"])
            ):
                raise ValueError(
                    "market_stream_production_not_admitted: enabled and post-Phase-4 capacity admission are required"
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
    ) -> ArchiveCommitResult:
        if not records or encoded.record_count != len(records):
            raise ValueError("market_archive_commit_invalid: record count mismatch")
        if encoded.sha256 != acknowledgement.sha256 or encoded.byte_count != acknowledgement.byte_count:
            raise ValueError("market_archive_commit_invalid: object acknowledgement mismatch")
        source_manifest_ids = tuple(
            dict.fromkeys(str(value) for value in compaction_source_manifest_ids)
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

    def archive_status(self, *, definition_id: str) -> dict[str, Any]:
        with db.session() as session:
            definition = session.execute(
                text(
                    """
                    SELECT id, series_id, provider_product_id, enabled,
                           production_admitted, max_spool_bytes,
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
            "production_admitted": bool(definition["production_admitted"]),
            "production_enabled": bool(definition["enabled"]),
            "production_blockers": [
                "post_phase4_24h_implemented_path_capture",
                "explicit_storage_and_cost_budget",
            ],
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
        explicit_pin_count = len(active_pins)
        pinned = bool(dataset_pin_count or explicit_pin_count)
        return {
            "schema_version": "market.archive_retention_status.v1",
            "target_kind": normalized_target,
            "target_id": normalized_target_id,
            "object_uri": str(target["object_uri"]),
            "object_sha256": str(target["object_sha256"]),
            "content_fingerprint": str(target["content_fingerprint"]),
            "dataset_pin_count": dataset_pin_count,
            "explicit_active_pins": [dict(row) for row in active_pins],
            "replacement_manifest_ids": replacement_ids,
            "source_manifest_ids": source_ids,
            "pinned": pinned,
            "ordinary_retention_eligible": not pinned,
            "object_retention_state": "pinned" if pinned else "active_unpinned",
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
    "MarketStructureOwnershipError",
    "MarketTradeConflictError",
    "PostgresMarketStructureRepository",
    "StreamClaim",
    "TradeIngestionOutcome",
    "market_structure_repository",
]
