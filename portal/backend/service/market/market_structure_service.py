"""Bounded Coinbase market-trade acquisition and replay orchestration."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import socket
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Optional

from data_providers.providers.factory import get_provider
from data_providers.streams.coinbase import (
    CoinbaseAdvancedTradeStream,
    CoinbaseMessageParser,
)
from data_providers.streams.contracts import (
    CanonicalMarketEvent,
    MarketSubscription,
    ProviderRawMessage,
)
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
    SpoolBackpressureError,
    publish_spool_archive,
    read_raw_archive_parquet,
    require_spool_capacity,
    spool_backlog_bytes,
)
from market_data.contracts import SourceIdentity
from market_data.structure import (
    ArchiveStatus,
    CoverageStatus,
    MARKET_TRADE_FACT_TYPE,
    MARKET_TRADE_FACT_VERSION,
    OrderingAssurance,
    PHASE1_COINBASE_TRADE_CONTRACTS,
    RawStreamRecord,
    TRADE_FLOW_FACT_TYPE,
    TRADE_FLOW_FACT_VERSION,
    TradeCoverageIntervalVersion,
    aggregate_trade_bucket,
    bucket_start_for,
    translate_coinbase_market_trade,
)

from ..storage.repos.market_data import market_data_repo
from ..storage.repos.market_structure import (
    MarketTradeConflictError,
    PostgresMarketStructureRepository,
    StreamClaim,
    market_structure_repository,
)
from .instrument_service import get_instrument_record, resolve_or_create_instrument


logger = logging.getLogger(__name__)

PHASE1_PROOF_EFFECTIVE_AT = datetime(2026, 8, 2, tzinfo=UTC)
PHASE1_AUTHENTICATED_PROOF_SHA256 = (
    "81fe668956a794aada0821ee83d202938c5b8ed3c0d1ffb3e83219e83cead032"
)
DEFAULT_SPOOL_BYTES = 8 * 1024**3
DEFAULT_SEGMENT_BYTES = 128 * 1024**2
DEFAULT_STORAGE_ROOT = Path("logs/market-structure")


@dataclass(frozen=True)
class PairDefinition:
    pair_id: str
    futures_instrument_id: str
    futures_product_id: str
    spot_product_id: str


PHASE1_PAIRS: Mapping[str, PairDefinition] = {
    "bip_btc": PairDefinition(
        pair_id="bip_btc",
        futures_instrument_id="b2deb0a0-f292-408a-876d-3dadd8e3819b",
        futures_product_id="BIP-20DEC30-CDE",
        spot_product_id="BTC-USD",
    ),
    "etp_eth": PairDefinition(
        pair_id="etp_eth",
        futures_instrument_id="44226144-fb38-4566-92c4-580734d76d3c",
        futures_product_id="ETP-20DEC30-CDE",
        spot_product_id="ETH-USD",
    ),
    "slp_sol": PairDefinition(
        pair_id="slp_sol",
        futures_instrument_id="bead556e-22e2-4ac0-8ee0-0d8c5310e9a0",
        futures_product_id="SLP-20DEC30-CDE",
        spot_product_id="SOL-USD",
    ),
}


@dataclass(frozen=True)
class CapturedEvent:
    event: CanonicalMarketEvent
    raw_record: RawStreamRecord


@dataclass(frozen=True)
class CaptureAnalysis:
    raw_record_count: int
    raw_bytes: int
    trade_event_count: int
    snapshot_trade_count: int
    update_trade_count: int
    first_sequence_num: Optional[int]
    last_sequence_num: Optional[int]
    subscription_acknowledged: bool
    heartbeat_healthy: bool
    snapshot_accepted: bool
    coverage_opening_ordinal: Optional[int]
    coverage_opening_effective_at: Optional[datetime]
    coverage_first_sequence_num: Optional[int]
    coverage_last_ordinal: Optional[int]
    coverage_last_effective_at: Optional[datetime]
    coverage_last_sequence_num: Optional[int]
    quality_events: tuple[Mapping[str, Any], ...]


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _observed_channel(raw_frame: bytes) -> str:
    try:
        payload = json.loads(raw_frame)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "decode_error"
    if not isinstance(payload, Mapping):
        return "decode_error"
    return str(payload.get("channel") or payload.get("type") or "unknown").strip().lower()


def _instrument_decimal(record: Mapping[str, Any], key: str) -> Optional[Decimal]:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else {}
    fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
    raw = fields.get(key)
    if raw in (None, ""):
        return None
    return Decimal(str(raw))


class MarketStructureService:
    """Coordinates Phase 1 without creating a second credential or data plane."""

    def __init__(
        self,
        *,
        repository: PostgresMarketStructureRepository = market_structure_repository,
        stream_factory: Callable[..., CoinbaseAdvancedTradeStream] = CoinbaseAdvancedTradeStream,
    ) -> None:
        self.repository = repository
        self.stream_factory = stream_factory

    def configure_pair(
        self,
        *,
        pair_id: str,
        auth_mode: str = "authenticated",
        max_spool_bytes: int = DEFAULT_SPOOL_BYTES,
        max_segment_bytes: int = DEFAULT_SEGMENT_BYTES,
        enable_production: bool = False,
    ) -> dict[str, Any]:
        normalized_pair = str(pair_id or "").strip().lower()
        pair = PHASE1_PAIRS.get(normalized_pair)
        if pair is None:
            raise ValueError(
                f"market_structure_pair_invalid: allowed={','.join(PHASE1_PAIRS)}"
            )
        if enable_production:
            raise ValueError(
                "market_stream_production_not_admitted: the implemented-path 24-hour proof and explicit budget are deferred until after Phase 4"
            )
        futures = get_instrument_record(pair.futures_instrument_id)
        if str(futures.get("symbol")) != pair.futures_product_id:
            raise RuntimeError(
                "market_structure_catalog_conflict: futures instrument/product mismatch"
            )
        spot, error = resolve_or_create_instrument(
            "COINBASE",
            "COINBASE_DIRECT",
            pair.spot_product_id,
            provider_id="COINBASE",
            venue_id="COINBASE_DIRECT",
        )
        if error or not spot:
            raise RuntimeError(
                f"market_structure_spot_registration_failed: product_id={pair.spot_product_id} error={error}"
            )
        source = SourceIdentity(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            source_kind="stream",
            adapter_version="coinbase_advanced_trade.market_trades.v1",
        )
        source_id = market_data_repo.register_source(
            source,
            lineage={
                "schema_version": "market_structure_source_lineage.v1",
                "provider_surface": "Coinbase Advanced Trade WebSocket",
                "channels": ["market_trades", "heartbeats"],
                "phase0_proof_sha256": PHASE1_AUTHENTICATED_PROOF_SHA256,
            },
        )
        instruments = (
            (futures, pair.futures_product_id, "future"),
            (spot, pair.spot_product_id, "spot"),
        )
        definitions: list[dict[str, Any]] = []
        series_catalog: list[dict[str, Any]] = []
        for instrument, product_id, product_type in instruments:
            instrument_id = str(instrument["id"])
            trade_series_id = market_data_repo.register_series(
                instrument_id=instrument_id,
                fact_type=MARKET_TRADE_FACT_TYPE,
                timeframe_seconds=None,
                contract_version=MARKET_TRADE_FACT_VERSION,
            )
            aggregate_series_ids = {
                interval: market_data_repo.register_series(
                    instrument_id=instrument_id,
                    fact_type=TRADE_FLOW_FACT_TYPE,
                    timeframe_seconds=interval,
                    contract_version=TRADE_FLOW_FACT_VERSION,
                )
                for interval in (1, 60)
            }
            contract = PHASE1_COINBASE_TRADE_CONTRACTS[product_id]
            self.repository.register_product_definition(
                definition_version_id=contract.product_definition_version_id,
                source_id=source_id,
                instrument_id=instrument_id,
                provider_product_id=product_id,
                product_type=product_type,
                venue="COINBASE_DIRECT",
                status="online_phase0_proven",
                base_currency=contract.base_currency,
                quote_currency=contract.quote_currency,
                provider_size_unit=contract.provider_size_unit.value,
                contract_size=contract.contract_size,
                price_increment=_instrument_decimal(instrument, "tick_size"),
                base_increment=_instrument_decimal(instrument, "qty_step"),
                effective_at=PHASE1_PROOF_EFFECTIVE_AT,
                received_at=datetime.now(UTC),
                provenance={
                    "phase0_proof_sha256": PHASE1_AUTHENTICATED_PROOF_SHA256,
                    "proof_contract": "market_structure_phase0_proof.v3",
                    "quantity_semantics": "phase0_proven",
                },
            )
            definition_id = f"ms_coinbase_{product_id.lower().replace('-', '_')}"
            definition = self.repository.upsert_stream_definition(
                definition_id=definition_id,
                source_id=source_id,
                series_id=trade_series_id,
                provider="COINBASE",
                venue="COINBASE_DIRECT",
                provider_product_id=product_id,
                channels=("market_trades", "heartbeats"),
                auth_mode=auth_mode,
                contract_version=MARKET_TRADE_FACT_VERSION,
                max_spool_bytes=max_spool_bytes,
                max_segment_bytes=max_segment_bytes,
                enabled=False,
                production_admitted=False,
                config={
                    "schema_version": "market_structure_stream_config.v1",
                    "pair_id": normalized_pair,
                    "aggregate_series_ids": {
                        str(key): value for key, value in aggregate_series_ids.items()
                    },
                    "product_definition_version_id": contract.product_definition_version_id,
                    "production_blocker": "post_phase4_24h_capacity_and_budget_gate",
                },
            )
            definitions.append(definition)
            series_catalog.append(
                {
                    "instrument_id": instrument_id,
                    "product_id": product_id,
                    "trade_series_id": trade_series_id,
                    "aggregate_series_ids": aggregate_series_ids,
                }
            )
        mapping_id = self.repository.register_instrument_mapping(
            primary_instrument_id=str(futures["id"]),
            related_instrument_id=str(spot["id"]),
            role="spot_reference",
            effective_from=PHASE1_PROOF_EFFECTIVE_AT,
            mapping_reason="operator-approved Phase 1 futures/spot pair",
            mapping_source="market_structure_phase0_proof.v3",
        )
        return {
            "schema_version": "market_structure_pair_configuration.v1",
            "pair_id": normalized_pair,
            "mapping_id": mapping_id,
            "definitions": definitions,
            "series": series_catalog,
            "production_admitted": False,
            "production_blockers": [
                "post_phase4_24h_implemented_path_capture",
                "explicit_storage_and_cost_budget",
            ],
        }

    async def capture_bounded(
        self,
        *,
        definition_id: str,
        duration_seconds: float,
        storage_root: Path = DEFAULT_STORAGE_ROOT,
        owner_id: Optional[str] = None,
    ) -> dict[str, Any]:
        duration = float(duration_seconds)
        if not 1 <= duration <= 3600:
            raise ValueError("market_structure_capture_invalid: duration must be 1..3600 seconds")
        owner = str(owner_id or f"qt:{socket.gethostname()}:{os.getpid()}")
        lease_seconds = max(90.0, duration + 300.0)
        claim = self.repository.claim_stream(
            definition_id=definition_id,
            owner_id=owner,
            lease_seconds=lease_seconds,
            bounded=True,
        )
        if claim.provider != "COINBASE" or claim.venue != "COINBASE_DIRECT":
            with contextlib.suppress(Exception):
                self.repository.release(claim)
            raise ValueError("market_structure_capture_invalid: unsupported provider/venue")
        storage = Path(storage_root).expanduser().resolve()
        spool_root = storage / "spool"
        object_store = FilesystemRawArchiveObjectStore(storage / "objects")
        temporary_root = storage / "tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        started_monotonic = time.monotonic()
        started_at = datetime.now(UTC)
        session_event_ordinal = 0
        stream: CoinbaseAdvancedTradeStream | None = None
        pending: asyncio.Task[ProviderRawMessage] | None = None
        segments: list[DurableRawSpoolSegment] = []
        current_segment: DurableRawSpoolSegment | None = None
        captured: list[CapturedEvent] = []
        raw_records: list[RawStreamRecord] = []
        analysis_state = _CaptureAnalyzer(claim)
        heartbeat_deadline = time.monotonic() + min(30.0, lease_seconds / 3.0)
        try:
            provider = get_provider("COINBASE", venue="COINBASE_DIRECT")
            jwt_factory = (
                provider.build_websocket_jwt
                if claim.auth_mode == "authenticated"
                else None
            )
            stream = self.stream_factory(
                jwt_factory=jwt_factory,
                stream_session_id=claim.session_id,
            )
            await stream.connect()
            self.repository.append_session_event(
                claim,
                event_ordinal=session_event_ordinal,
                connection_epoch=0,
                event_type="connected",
                occurred_at=datetime.now(UTC),
                evidence={"bounded": True, "duration_seconds": duration},
            )
            session_event_ordinal += 1
            await stream.subscribe(
                [
                    MarketSubscription.from_values(
                        provider="COINBASE",
                        venue="COINBASE_DIRECT",
                        symbol=claim.provider_product_id,
                        product_id=claim.provider_product_id,
                        channels=claim.channels,
                        auth_mode=claim.auth_mode,
                    )
                ]
            )
            self.repository.append_session_event(
                claim,
                event_ordinal=session_event_ordinal,
                connection_epoch=0,
                event_type="subscription_sent",
                occurred_at=datetime.now(UTC),
                evidence={
                    "product_id": claim.provider_product_id,
                    "channels": list(claim.channels),
                    "auth_mode": claim.auth_mode,
                },
            )
            session_event_ordinal += 1
            parser = CoinbaseMessageParser(
                symbol_by_product_id={claim.provider_product_id: claim.provider_product_id}
            )
            iterator = stream.raw_messages().__aiter__()
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                if pending is None:
                    pending = asyncio.create_task(anext(iterator))
                done, _ = await asyncio.wait({pending}, timeout=min(remaining, 1.0))
                if not done:
                    if time.monotonic() >= heartbeat_deadline:
                        self.repository.heartbeat(claim, lease_seconds=lease_seconds)
                        heartbeat_deadline = time.monotonic() + min(30.0, lease_seconds / 3.0)
                    analysis_state.observe_idle(datetime.now(UTC))
                    continue
                try:
                    message = pending.result()
                except StopAsyncIteration as exc:
                    analysis_state.unexpected_disconnect(datetime.now(UTC))
                    raise RuntimeError("market_stream_disconnected_before_bounded_deadline") from exc
                finally:
                    pending = None
                observed_channel = _observed_channel(message.raw_frame)
                estimated_spool_bytes = len(message.raw_frame) * 2 + 4096
                require_spool_capacity(
                    root=spool_root,
                    max_backlog_bytes=claim.max_spool_bytes,
                    next_frame_bytes=estimated_spool_bytes,
                    definition_id=claim.definition_id,
                )
                if (
                    current_segment is None
                    or (
                        current_segment.record_count > 0
                        and current_segment.current_bytes + estimated_spool_bytes
                        > claim.max_segment_bytes
                    )
                ):
                    if current_segment is not None:
                        current_segment.seal()
                    current_segment = DurableRawSpoolSegment(
                        root=spool_root,
                        definition_id=claim.definition_id,
                        session_id=claim.session_id,
                        connection_epoch=message.connection_epoch,
                        segment_ordinal=len(segments),
                    )
                    segments.append(current_segment)
                record = RawStreamRecord.from_provider_message(
                    message,
                    definition_id=claim.definition_id,
                    spool_segment_id=current_segment.spool_segment_id,
                    provider_product_id=claim.provider_product_id,
                    requested_channel="market_trades",
                    observed_channel=observed_channel,
                )
                current_segment.append(record)
                raw_records.append(record)
                events = parser.parse_raw(
                    message.raw_frame,
                    received_at=message.received_at,
                    raw_ref={
                        **message.evidence_ref(),
                        "raw_record_id": record.raw_record_id,
                        "spool_segment_id": record.spool_segment_id,
                    },
                )
                analysis_state.observe(record, events)
                captured.extend(CapturedEvent(event=event, raw_record=record) for event in events)
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                    await pending
                pending = None
            await stream.close()
            stream = None
            stopped_at = datetime.now(UTC)
            analysis_state.observe_idle(stopped_at)
            bounded_stop_event_id = self.repository.append_session_event(
                claim,
                event_ordinal=session_event_ordinal,
                connection_epoch=0,
                event_type="bounded_capture_stopped",
                occurred_at=stopped_at,
                reason="requested_duration_elapsed",
                evidence={"raw_record_count": len(raw_records)},
            )
            session_event_ordinal += 1
            if not raw_records:
                raise RuntimeError("market_structure_capture_empty: no provider frames received")
            if current_segment is not None and current_segment.record_count:
                current_segment.seal()
            self.repository.heartbeat(claim, lease_seconds=lease_seconds)
            manifest_ids: list[str] = []
            for segment in segments:
                if segment.record_count <= 0:
                    continue
                encoded, acknowledgement, segment_records = publish_spool_archive(
                    segment,
                    object_store=object_store,
                    temporary_directory=temporary_root,
                )
                commit = self.repository.commit_archive(
                    claim,
                    encoded=encoded,
                    acknowledgement=acknowledgement,
                    records=segment_records,
                )
                segment.mark_database_acknowledged(
                    manifest_id=commit.manifest_id,
                    object_key=acknowledgement.object_key,
                    object_sha256=acknowledgement.sha256,
                )
                segment.discard_acknowledged_spool()
                manifest_ids.append(commit.manifest_id)
                self.repository.heartbeat(claim, lease_seconds=lease_seconds)

            analysis = analysis_state.finalize()
            coverage_interval_id = _stable_hash(
                {
                    "schema_version": "market.trade_coverage_interval_id.v1",
                    "definition_id": claim.definition_id,
                    "session_id": claim.session_id,
                    "connection_epoch": 0,
                    "product_id": claim.provider_product_id,
                    "channel": "market_trades",
                }
            )
            quality_event_ids: list[str] = []
            invalidating_quality_ids: list[str] = []
            for quality in analysis.quality_events:
                quality_id = self.repository.record_quality_event(
                    claim,
                    connection_epoch=0,
                    receive_ordinal=int(quality["receive_ordinal"]),
                    channel=str(quality["channel"]),
                    classification=str(quality["classification"]),
                    reason=str(quality["reason"]),
                    detected_at=quality["detected_at"],
                    raw_record_id=quality.get("raw_record_id"),
                    coverage_interval_id=coverage_interval_id,
                    sequence_before=quality.get("sequence_before"),
                    sequence_after=quality.get("sequence_after"),
                    evidence=quality.get("evidence"),
                )
                quality_event_ids.append(quality_id)
                if bool(quality.get("invalidating")):
                    invalidating_quality_ids.append(quality_id)

            coverage_opened = (
                analysis.subscription_acknowledged
                and analysis.heartbeat_healthy
                and analysis.snapshot_accepted
                and analysis.coverage_opening_ordinal is not None
                and analysis.coverage_opening_effective_at is not None
                and analysis.coverage_last_ordinal is not None
                and analysis.coverage_last_effective_at is not None
            )
            opening_event_id: Optional[str] = None
            coverage: Optional[TradeCoverageIntervalVersion] = None
            if coverage_opened:
                opening_record = next(
                    record
                    for record in raw_records
                    if record.receive_ordinal == analysis.coverage_opening_ordinal
                )
                last_record = next(
                    record
                    for record in reversed(raw_records)
                    if record.receive_ordinal == analysis.coverage_last_ordinal
                )
                opening_event_id = self.repository.append_session_event(
                    claim,
                    event_ordinal=session_event_ordinal,
                    connection_epoch=0,
                    event_type="trade_coverage_opened",
                    occurred_at=analysis.coverage_opening_effective_at,
                    received_at=datetime.now(UTC),
                    evidence={
                        "raw_record_id": opening_record.raw_record_id,
                        "receive_ordinal": opening_record.receive_ordinal,
                        "ordering_assurance": OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS.value,
                    },
                )
                session_event_ordinal += 1
                status = (
                    CoverageStatus.INVALID
                    if invalidating_quality_ids
                    else CoverageStatus.CLOSED_VALID
                )
                known_at = datetime.now(UTC)
                coverage = TradeCoverageIntervalVersion(
                    interval_id=coverage_interval_id,
                    revision=1,
                    definition_id=claim.definition_id,
                    session_id=claim.session_id,
                    connection_epoch=0,
                    provider_product_id=claim.provider_product_id,
                    channel="market_trades",
                    status=status,
                    ordering_assurance=OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS,
                    archive_status=ArchiveStatus.COMPLETE,
                    opening_raw_record_id=opening_record.raw_record_id,
                    opening_receive_ordinal=opening_record.receive_ordinal,
                    opening_effective_at=analysis.coverage_opening_effective_at,
                    last_raw_record_id=last_record.raw_record_id,
                    last_receive_ordinal=last_record.receive_ordinal,
                    last_effective_at=analysis.coverage_last_effective_at,
                    closing_raw_record_id=last_record.raw_record_id,
                    closing_receive_ordinal=last_record.receive_ordinal,
                    closing_effective_at=analysis.coverage_last_effective_at,
                    canonicalization_watermark_ordinal=raw_records[-1].receive_ordinal,
                    archive_complete_through_ordinal=raw_records[-1].receive_ordinal,
                    known_at=known_at,
                    first_provider_sequence_num=analysis.coverage_first_sequence_num,
                    last_provider_sequence_num=analysis.coverage_last_sequence_num,
                    gap_quality_event_ids=tuple(invalidating_quality_ids),
                    opening_evidence={
                        "subscription_acknowledged": True,
                        "heartbeat_healthy": True,
                        "snapshot_accepted": True,
                    },
                    closing_evidence={
                        "bounded_stop_session_event_id": bounded_stop_event_id,
                        "all_raw_records_mapped": True,
                    },
                )

            translated = []
            for item in captured:
                if item.event.event_kind != "market_trade":
                    continue
                delivery_kind = str(item.event.payload.get("type") or "").lower()
                coverage_ref = (
                    coverage_interval_id
                    if coverage is not None
                    and delivery_kind == "update"
                    and item.raw_record.receive_ordinal >= coverage.opening_receive_ordinal
                    else None
                )
                translated.append(
                    translate_coinbase_market_trade(
                        item.event,
                        contract=PHASE1_COINBASE_TRADE_CONTRACTS[claim.provider_product_id],
                        raw_record_id=item.raw_record.raw_record_id,
                        connection_epoch=item.raw_record.connection_epoch,
                        receive_ordinal=item.raw_record.receive_ordinal,
                        accepted_at=datetime.now(UTC),
                        coverage_interval_id=coverage_ref,
                    )
                )
            try:
                trade_outcome = self.repository.ingest_trades(
                    claim,
                    facts=translated,
                    require_archive_mapping=True,
                )
            except MarketTradeConflictError as exc:
                conflict = translated[0] if translated else None
                self.repository.record_quality_event(
                    claim,
                    connection_epoch=0,
                    receive_ordinal=conflict.receive_ordinal if conflict else raw_records[-1].receive_ordinal,
                    channel="market_trades",
                    classification="provider_trade_conflict",
                    reason=str(exc),
                    detected_at=datetime.now(UTC),
                    raw_record_id=conflict.raw_record_id if conflict else raw_records[-1].raw_record_id,
                )
                raise
            if coverage is not None and opening_event_id is not None:
                self.repository.append_coverage_version(
                    claim,
                    coverage=coverage,
                    opening_session_event_id=opening_event_id,
                    closing_session_event_id=bounded_stop_event_id,
                )

            aggregate_counts: dict[str, dict[str, int]] = {}
            all_trade_facts = []
            if coverage is not None:
                # Initial snapshot trades are valuable canonical evidence, but
                # they predate proven live delivery and must never be projected
                # as complete flow. Keep current-session update deliveries so a
                # trade first seen in a snapshot and then redelivered live is
                # still represented; aggregate_trade_bucket deduplicates its
                # provider identity deterministically. Membership is based on
                # the delivery's typed coverage reference rather than comparing
                # provider event time to receipt time at the interval edge.
                all_trade_facts = [
                    fact
                    for fact in translated
                    if fact.coverage_interval_id == coverage.interval_id
                ]
            aggregate_series_ids = {
                int(key): int(value)
                for key, value in dict(claim.config.get("aggregate_series_ids") or {}).items()
            }
            for interval in (1, 60):
                bucket_starts = {
                    bucket_start_for(fact.provider_event_time, interval_seconds=interval)
                    for fact in all_trade_facts
                }
                if coverage is not None:
                    cursor = bucket_start_for(
                        coverage.opening_effective_at, interval_seconds=interval
                    )
                    last = bucket_start_for(
                        coverage.closing_effective_at or coverage.last_effective_at,
                        interval_seconds=interval,
                    )
                    while cursor <= last:
                        if coverage.complete_for_bucket(
                            bucket_start=cursor,
                            bucket_end=cursor + timedelta(seconds=interval),
                        ):
                            bucket_starts.add(cursor)
                        cursor += timedelta(seconds=interval)
                aggregate_facts = []
                for bucket_start in sorted(bucket_starts):
                    bucket_rows = [
                        fact
                        for fact in all_trade_facts
                        if bucket_start
                        <= fact.provider_event_time
                        < bucket_start + timedelta(seconds=interval)
                    ]
                    try:
                        aggregate_facts.append(
                            aggregate_trade_bucket(
                                bucket_rows,
                                interval_seconds=interval,
                                bucket_start=bucket_start,
                                coverage=coverage,
                                computed_at=max(
                                    datetime.now(UTC),
                                    bucket_start + timedelta(seconds=interval),
                                ),
                            )
                        )
                    except ValueError as exc:
                        if "incomplete_zero_forbidden" not in str(exc):
                            raise
                aggregate_outcome = self.repository.ingest_aggregates(
                    series_id=aggregate_series_ids[interval],
                    facts=aggregate_facts,
                )
                aggregate_counts[str(interval)] = {
                    "requested": len(aggregate_facts),
                    "inserted": aggregate_outcome.inserted_count,
                    "noop": aggregate_outcome.noop_count,
                }
            self.repository.append_session_event(
                claim,
                event_ordinal=session_event_ordinal,
                connection_epoch=0,
                event_type="canonicalization_completed",
                occurred_at=datetime.now(UTC),
                evidence={
                    "trade_requested": trade_outcome.requested_count,
                    "trade_inserted": trade_outcome.inserted_count,
                    "trade_noop": trade_outcome.noop_count,
                    "aggregate_counts": aggregate_counts,
                    "manifest_ids": manifest_ids,
                },
            )
            status = self.repository.archive_status(definition_id=claim.definition_id)
            result = {
                "schema_version": "market_structure_bounded_capture.v1",
                "status": "completed",
                "definition_id": claim.definition_id,
                "session_id": claim.session_id,
                "product_id": claim.provider_product_id,
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
                "elapsed_seconds": time.monotonic() - started_monotonic,
                "raw_record_count": analysis.raw_record_count,
                "raw_bytes": analysis.raw_bytes,
                "spool_segment_count": len(segments),
                "spool_backlog_bytes": spool_backlog_bytes(
                    spool_root, definition_id=claim.definition_id
                ),
                "manifest_ids": manifest_ids,
                "trade_events": analysis.trade_event_count,
                "snapshot_trades": analysis.snapshot_trade_count,
                "update_trades": analysis.update_trade_count,
                "trade_ingestion": {
                    "requested": trade_outcome.requested_count,
                    "inserted": trade_outcome.inserted_count,
                    "noop": trade_outcome.noop_count,
                    "max_commit_seq": trade_outcome.max_commit_seq,
                },
                "aggregates": aggregate_counts,
                "coverage": {
                    "opened": coverage is not None,
                    "status": coverage.status.value if coverage else "unproven",
                    "interval_id": coverage.interval_id if coverage else None,
                    "complete_bucket_eligible": bool(
                        coverage and coverage.status is CoverageStatus.CLOSED_VALID
                    ),
                },
                "quality_event_ids": quality_event_ids,
                "archive_status": status,
                "production_admitted": False,
                "production_blockers": [
                    "post_phase4_24h_implemented_path_capture",
                    "explicit_storage_and_cost_budget",
                ],
            }
            logger.info(
                "market_structure_capture_completed | definition_id=%s session_id=%s product_id=%s raw_records=%s trades=%s manifests=%s elapsed_seconds=%.3f",
                claim.definition_id,
                claim.session_id,
                claim.provider_product_id,
                analysis.raw_record_count,
                analysis.trade_event_count,
                len(manifest_ids),
                result["elapsed_seconds"],
            )
            return result
        except SpoolBackpressureError as exc:
            with contextlib.suppress(Exception):
                self.repository.record_quality_event(
                    claim,
                    connection_epoch=0,
                    receive_ordinal=raw_records[-1].receive_ordinal if raw_records else 0,
                    channel="market_trades",
                    classification="backpressure_stop",
                    reason=str(exc),
                    detected_at=datetime.now(UTC),
                    raw_record_id=raw_records[-1].raw_record_id if raw_records else None,
                )
            raise
        except Exception as exc:
            with contextlib.suppress(Exception):
                self.repository.append_session_event(
                    claim,
                    event_ordinal=session_event_ordinal,
                    connection_epoch=0,
                    event_type="failed",
                    occurred_at=datetime.now(UTC),
                    reason=f"{type(exc).__name__}: {exc}",
                    evidence={"raw_record_count": len(raw_records)},
                )
            logger.exception(
                "market_structure_capture_failed | definition_id=%s session_id=%s product_id=%s",
                claim.definition_id,
                claim.session_id,
                claim.provider_product_id,
            )
            raise
        finally:
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                    await pending
            if stream is not None:
                with contextlib.suppress(Exception):
                    await stream.close()
            if current_segment is not None:
                with contextlib.suppress(Exception):
                    current_segment.close()
            with contextlib.suppress(Exception):
                self.repository.release(claim)

    def replay_manifest(
        self, *, manifest_id: str, storage_root: Path = DEFAULT_STORAGE_ROOT
    ) -> dict[str, Any]:
        manifest = self.repository.get_manifest(manifest_id)
        store = FilesystemRawArchiveObjectStore(
            Path(storage_root).expanduser().resolve() / "objects"
        )
        path = store.local_path(str(manifest["object_key"]))
        if not path.exists():
            raise RuntimeError(
                f"market_archive_object_missing: manifest_id={manifest_id} object_key={manifest['object_key']}"
            )
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != manifest["object_sha256"]:
            raise RuntimeError("market_archive_replay_invalid: object checksum mismatch")
        records = read_raw_archive_parquet(path)
        parser = CoinbaseMessageParser()
        trade_ids: list[str] = []
        for record in records:
            for event in parser.parse_raw(
                record.raw_frame,
                received_at=record.received_at.isoformat(),
                raw_ref={"raw_record_id": record.raw_record_id},
            ):
                if event.event_kind == "market_trade":
                    trade_ids.append(str(event.payload.get("trade_id")))
        fingerprint = _stable_hash(
            {
                "schema_version": "market_structure_trade_replay.v1",
                "raw_record_ids": [record.raw_record_id for record in records],
                "trade_ids": trade_ids,
            }
        )
        reconciliation = self.repository.reconcile_manifest_trade_ids(
            manifest_id=manifest_id,
            provider_product_id=(
                records[0].provider_product_id if records else ""
            ),
            provider_trade_ids=trade_ids,
        )
        return {
            "schema_version": "market_structure_manifest_replay.v1",
            "manifest_id": manifest_id,
            "object_sha256": digest,
            "raw_record_count": len(records),
            "trade_count": len(trade_ids),
            "first_receive_ordinal": records[0].receive_ordinal if records else None,
            "last_receive_ordinal": records[-1].receive_ordinal if records else None,
            "replay_fingerprint": fingerprint,
            "reconciliation": reconciliation,
        }

    def reconcile_recent_trades(
        self, *, definition_id: str, limit: int = 100
    ) -> dict[str, Any]:
        """Compare one bounded REST window with stored IDs; never claim backfill."""

        bounded_limit = max(1, min(int(limit), 1000))
        definitions = self.repository.list_stream_definitions(
            definition_id=definition_id
        )
        if len(definitions) != 1:
            raise ValueError(
                f"market_stream_definition_unknown: definition_id={definition_id}"
            )
        definition = definitions[0]
        provider = get_provider("COINBASE", venue="COINBASE_DIRECT")
        payload = provider.fetch_recent_market_trades_proof(
            str(definition["provider_product_id"]),
            auth_mode=str(definition["auth_mode"]),
            limit=bounded_limit,
        )
        raw_trades = payload.get("trades")
        if not isinstance(raw_trades, Sequence) or isinstance(
            raw_trades, (str, bytes)
        ):
            raise RuntimeError(
                "market_recent_trade_reconciliation_invalid: Coinbase response lacks trades"
            )
        trade_ids = [
            str(row.get("trade_id") or "").strip()
            for row in raw_trades
            if isinstance(row, Mapping) and str(row.get("trade_id") or "").strip()
        ]
        overlap = self.repository.recent_trade_id_overlap(
            definition_id=definition_id,
            provider_trade_ids=trade_ids,
        )
        return {
            "schema_version": "market.recent_trade_reconciliation.v1",
            "definition_id": definition_id,
            "provider_product_id": overlap["provider_product_id"],
            "auth_mode": str(definition["auth_mode"]),
            "rest_limit": bounded_limit,
            "rest_trade_count": len(trade_ids),
            "rest_unique_trade_count": len(set(trade_ids)),
            "canonical_overlap_count": len(
                overlap["canonical_overlap_trade_ids"]
            ),
            "rest_only_trade_ids": overlap["rest_only_trade_ids"],
            "historical_completeness_claim": "none",
            "interpretation": (
                "bounded diagnostic only; REST-only IDs may have occurred after the latest capture"
            ),
        }


class _CaptureAnalyzer:
    def __init__(self, claim: StreamClaim) -> None:
        self.claim = claim
        self.raw_count = 0
        self.raw_bytes = 0
        self.trade_events = 0
        self.snapshot_trades = 0
        self.update_trades = 0
        self.first_sequence: Optional[int] = None
        self.last_sequence: Optional[int] = None
        self.subscription_ack = False
        self.first_heartbeat_at: Optional[datetime] = None
        self.last_heartbeat_at: Optional[datetime] = None
        self.snapshot = False
        self.baseline_ready_ordinal: Optional[int] = None
        self.coverage_opening_ordinal: Optional[int] = None
        self.coverage_opening_effective_at: Optional[datetime] = None
        self.coverage_first_sequence_num: Optional[int] = None
        self.coverage_last_ordinal: Optional[int] = None
        self.coverage_last_effective_at: Optional[datetime] = None
        self.coverage_last_sequence_num: Optional[int] = None
        self.sequence_hashes: dict[int, str] = {}
        self.quality: list[dict[str, Any]] = []
        self.last_record: Optional[RawStreamRecord] = None

    def observe(
        self, record: RawStreamRecord, events: Sequence[CanonicalMarketEvent]
    ) -> None:
        self.raw_count += 1
        self.raw_bytes += len(record.raw_frame)
        self.last_record = record
        message_sequence: Optional[int] = None
        try:
            payload = json.loads(record.raw_frame)
            if isinstance(payload, Mapping) and isinstance(payload.get("sequence_num"), int):
                message_sequence = int(payload["sequence_num"])
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        if message_sequence is not None:
            self.first_sequence = (
                message_sequence if self.first_sequence is None else self.first_sequence
            )
            self.last_sequence = max(message_sequence, self.last_sequence or message_sequence)
            prior_hash = self.sequence_hashes.get(message_sequence)
            if prior_hash is None:
                self.sequence_hashes[message_sequence] = record.raw_frame_sha256
            elif prior_hash != record.raw_frame_sha256:
                self._quality(
                    record,
                    classification="divergent_duplicate",
                    reason="same connection sequence carried different raw bytes",
                    invalidating=True,
                    sequence_before=message_sequence,
                    sequence_after=message_sequence,
                )
        for event in events:
            if event.event_kind == "provider_subscription_ack":
                subscriptions = json.dumps(dict(event.payload or {})).lower()
                if "market_trades" in subscriptions:
                    self.subscription_ack = True
            elif event.event_kind == "provider_heartbeat":
                now = record.received_at
                self.first_heartbeat_at = self.first_heartbeat_at or now
                self.last_heartbeat_at = now
            elif event.event_kind == "provider_sequence_gap":
                status = str(event.payload.get("status") or "")
                classification = {
                    "gap": "sequence_gap",
                    "out_of_order": "out_of_order",
                    "duplicate": "duplicate",
                }.get(status, "sequence_gap")
                # A divergent duplicate is already classified from exact bytes.
                invalidating = classification in {"sequence_gap", "out_of_order"}
                self._quality(
                    record,
                    classification=classification,
                    reason=f"provider connection sequence status={status}",
                    invalidating=invalidating,
                    sequence_before=event.payload.get("previous_sequence_num"),
                    sequence_after=event.payload.get("current_sequence_num"),
                )
            elif event.event_kind == "provider_malformed_message":
                self._quality(
                    record,
                    classification="decode_error",
                    reason=str(event.payload.get("error") or "provider frame decode failed"),
                    invalidating=True,
                )
            elif event.event_kind == "market_trade":
                self.trade_events += 1
                delivery = str(event.payload.get("type") or "").lower()
                if delivery == "snapshot":
                    self.snapshot = True
                    self.snapshot_trades += 1
                elif delivery == "update":
                    self.update_trades += 1
        if self._baseline_ready(record.receive_ordinal) and self.baseline_ready_ordinal is None:
            self.baseline_ready_ordinal = record.receive_ordinal
        # Trade coverage is a connection-delivery assertion, not an assertion
        # that a trade occurred. Once subscription, heartbeat, and initial
        # snapshot evidence are all present, every subsequently received frame
        # advances the healthy interval. This is what makes an explicitly empty
        # bucket distinguishable from a missed or unhealthy stream.
        if self.baseline_ready_ordinal is not None:
            if self.coverage_opening_ordinal is None:
                self.coverage_opening_ordinal = record.receive_ordinal
                self.coverage_opening_effective_at = record.received_at
                self.coverage_first_sequence_num = message_sequence
            self.coverage_last_ordinal = record.receive_ordinal
            self.coverage_last_effective_at = record.received_at
            self.coverage_last_sequence_num = message_sequence

    def _baseline_ready(self, current_ordinal: int) -> bool:
        return bool(
            self.subscription_ack
            and self.first_heartbeat_at is not None
            and self.snapshot
            and (self.baseline_ready_ordinal is None or current_ordinal >= self.baseline_ready_ordinal)
        )

    def observe_idle(self, now: datetime) -> None:
        if (
            self.last_heartbeat_at is not None
            and (now - self.last_heartbeat_at).total_seconds() > 5
            and self.last_record is not None
            and not any(item["classification"] == "heartbeat_gap" for item in self.quality)
        ):
            self._quality(
                self.last_record,
                classification="heartbeat_gap",
                reason="no heartbeat received for more than five seconds",
                invalidating=True,
            )

    def unexpected_disconnect(self, now: datetime) -> None:
        if self.last_record is None:
            return
        self._quality(
            self.last_record,
            classification="disconnect",
            reason=f"stream ended before bounded deadline at {now.isoformat()}",
            invalidating=True,
        )

    def _quality(
        self,
        record: RawStreamRecord,
        *,
        classification: str,
        reason: str,
        invalidating: bool,
        sequence_before: Optional[int] = None,
        sequence_after: Optional[int] = None,
    ) -> None:
        material = {
            "classification": classification,
            "receive_ordinal": record.receive_ordinal,
            "raw_record_id": record.raw_record_id,
            "sequence_before": sequence_before,
            "sequence_after": sequence_after,
        }
        if any(
            item.get("dedupe_hash") == _stable_hash(material) for item in self.quality
        ):
            return
        self.quality.append(
            {
                "dedupe_hash": _stable_hash(material),
                "receive_ordinal": record.receive_ordinal,
                "channel": record.observed_channel,
                "classification": classification,
                "reason": reason,
                "detected_at": record.received_at,
                "raw_record_id": record.raw_record_id,
                "sequence_before": sequence_before,
                "sequence_after": sequence_after,
                "invalidating": invalidating,
                "evidence": {"raw_frame_sha256": record.raw_frame_sha256},
            }
        )

    def finalize(self) -> CaptureAnalysis:
        heartbeat_healthy = bool(
            self.first_heartbeat_at is not None
            and not any(item["classification"] == "heartbeat_gap" for item in self.quality)
        )
        return CaptureAnalysis(
            raw_record_count=self.raw_count,
            raw_bytes=self.raw_bytes,
            trade_event_count=self.trade_events,
            snapshot_trade_count=self.snapshot_trades,
            update_trade_count=self.update_trades,
            first_sequence_num=self.first_sequence,
            last_sequence_num=self.last_sequence,
            subscription_acknowledged=self.subscription_ack,
            heartbeat_healthy=heartbeat_healthy,
            snapshot_accepted=self.snapshot,
            coverage_opening_ordinal=self.coverage_opening_ordinal,
            coverage_opening_effective_at=self.coverage_opening_effective_at,
            coverage_first_sequence_num=self.coverage_first_sequence_num,
            coverage_last_ordinal=self.coverage_last_ordinal,
            coverage_last_effective_at=self.coverage_last_effective_at,
            coverage_last_sequence_num=self.coverage_last_sequence_num,
            quality_events=tuple(self.quality),
        )


market_structure_service = MarketStructureService()


__all__ = [
    "DEFAULT_SEGMENT_BYTES",
    "DEFAULT_SPOOL_BYTES",
    "DEFAULT_STORAGE_ROOT",
    "MarketStructureService",
    "PHASE1_PAIRS",
    "market_structure_service",
]
