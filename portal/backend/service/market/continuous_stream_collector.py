"""Supervised, bounded-memory market-structure stream collection.

The bounded proof command intentionally keeps its short diagnostic lifecycle.
This module owns the long-lived path: one fenced session, rotating durable
segments, asynchronous canonicalization, reconnect epochs, and an explicit
stop predicate supplied by a generic worker supervisor.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence

from data_providers.providers.factory import get_provider
from data_providers.streams.coinbase import (
    CoinbaseAdvancedTradeStream,
    CoinbaseMessageParser,
)
from data_providers.streams.contracts import MarketSubscription
from data_providers.streams.runtime import ContinuousStreamPolicy
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
    SpoolBackpressureError,
    discover_spool_segments,
    publish_spool_archive,
    read_raw_archive_parquet,
    require_spool_capacity,
    spool_backlog_bytes,
)
from market_data.book_archive import (
    BOOK_CHECKPOINT_COMPRESSION,
    BOOK_CHECKPOINT_FORMAT,
    publish_book_checkpoint,
    read_book_checkpoint_parquet,
)
from market_data.market_state import (
    MarketStateValuationContract,
    derive_book_features,
    derive_response_features,
    derive_trade_flow_feature,
)
from market_data.order_book import (
    BOOK_CHECKPOINT_SCHEMA_VERSION,
    BOOK_RECONSTRUCTION_VERSION,
    BookCheckpointFact,
    BookLifecycle,
    BookQualityEvidence,
    BookSide,
    BookSourcePosition,
    BookValidityIntervalVersion,
    BookValidityStatus,
    L2ProductContract,
    Level2BookReconstructor,
    translate_coinbase_l2_event,
)
from market_data.structure import (
    ArchiveStatus,
    CoverageStatus,
    OrderingAssurance,
    ProviderSizeUnit,
    RawStreamRecord,
    TradeCoverageIntervalVersion,
    aggregate_trade_bucket,
    bucket_start_for,
    translate_coinbase_market_trade,
)

from ..storage.repos.market_structure import (
    MarketTradeConflictError,
    PostgresMarketStructureRepository,
    StreamClaim,
    market_structure_repository,
)
from .market_structure_service import (
    DEFAULT_STORAGE_ROOT,
    _CaptureAnalyzer,
    _observed_channel,
    _stable_hash,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SegmentCheckpoint:
    segment: DurableRawSpoolSegment
    analysis: Any
    terminal: bool
    terminal_reason: Optional[str]
    closing_session_event_id: Optional[str]
    external_quality: tuple[Mapping[str, Any], ...] = ()


@dataclass
class _EpochProjectionState:
    opening_session_event_id: Optional[str] = None
    opening_raw_record_id: Optional[str] = None
    opening_receive_ordinal: Optional[int] = None
    opening_effective_at: Optional[datetime] = None
    coverage_revision: int = 0
    persisted_quality_hashes: set[str] = field(default_factory=set)
    invalidating_quality_ids: list[str] = field(default_factory=list)
    next_bucket_start: dict[int, datetime] = field(default_factory=dict)
    book_reducer: Optional[Level2BookReconstructor] = None
    book_checkpoint_id: Optional[str] = None
    book_last_position: Optional[BookSourcePosition] = None


class _SessionEventWriter:
    """Serialize event ordinals across acquisition and finalization threads."""

    def __init__(
        self,
        repository: PostgresMarketStructureRepository,
        claim: StreamClaim,
        *,
        initial_ordinal: int = 0,
    ) -> None:
        self.repository = repository
        self.claim = claim
        self._lock = threading.Lock()
        self._ordinal = int(initial_ordinal)

    def append(
        self,
        *,
        connection_epoch: int,
        event_type: str,
        occurred_at: datetime,
        reason: Optional[str] = None,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> str:
        with self._lock:
            ordinal = self._ordinal
            self._ordinal += 1
            return self.repository.append_session_event(
                self.claim,
                event_ordinal=ordinal,
                connection_epoch=connection_epoch,
                event_type=event_type,
                occurred_at=occurred_at,
                reason=reason,
                evidence=evidence,
            )


class ContinuousTransportAdapter(Protocol):
    """Provider transport plugged into the generic lease/WAL runtime."""

    transport_id: str

    def supports(self, definition: Mapping[str, Any]) -> bool:
        ...

    def create_stream(self, claim: StreamClaim) -> Any:
        ...

    def create_parser(self, claim: StreamClaim) -> Any:
        ...

    def subscription(self, claim: StreamClaim) -> MarketSubscription:
        ...

    def observed_channel(self, raw_frame: bytes) -> str:
        ...


class CoinbaseContinuousTransportAdapter:
    """Coinbase WebSocket/JWT/parser binding; no projection semantics."""

    transport_id = "coinbase.advanced_trade_websocket.v1"

    def __init__(
        self,
        *,
        stream_factory: Callable[..., CoinbaseAdvancedTradeStream] = (
            CoinbaseAdvancedTradeStream
        ),
    ) -> None:
        self.stream_factory = stream_factory

    def supports(self, definition: Mapping[str, Any]) -> bool:
        return (
            str(definition.get("provider") or "").upper() == "COINBASE"
            and str(definition.get("venue") or "").upper()
            == "COINBASE_DIRECT"
        )

    def create_stream(self, claim: StreamClaim) -> CoinbaseAdvancedTradeStream:
        provider = get_provider(claim.provider, venue=claim.venue)
        jwt_factory = (
            provider.build_websocket_jwt
            if claim.auth_mode == "authenticated"
            else None
        )
        return self.stream_factory(
            jwt_factory=jwt_factory,
            stream_session_id=claim.session_id,
        )

    def create_parser(self, claim: StreamClaim) -> CoinbaseMessageParser:
        return CoinbaseMessageParser(
            symbol_by_product_id={
                claim.provider_product_id: claim.provider_product_id
            }
        )

    def subscription(self, claim: StreamClaim) -> MarketSubscription:
        return MarketSubscription.from_values(
            provider=claim.provider,
            venue=claim.venue,
            symbol=claim.provider_product_id,
            product_id=claim.provider_product_id,
            channels=claim.channels,
            auth_mode=claim.auth_mode,
        )

    def observed_channel(self, raw_frame: bytes) -> str:
        observed = _observed_channel(raw_frame)
        # Coinbase currently emits ``l2_data`` for the subscribed ``level2``
        # channel. The transport owns that provider alias so projection and
        # persistence retain the canonical subscription identity.
        return "level2" if observed == "l2_data" else observed


class ContinuousCaptureAnalyzer(Protocol):
    """Projection-owned epoch analysis consumed by the generic runtime."""

    raw_count: int
    last_heartbeat_at: Optional[datetime]
    last_record: Optional[RawStreamRecord]
    last_sequence: Optional[int]
    quality: list[dict[str, Any]]

    def observe(self, record: RawStreamRecord, events: Sequence[Any]) -> None:
        ...

    def observe_idle(self, now: datetime) -> None:
        ...

    def finalize(self) -> Any:
        ...


class ContinuousProjectionAdapter(Protocol):
    """Domain projection plugged into the provider-neutral stream runtime."""

    projection_id: str
    primary_channel: str
    channels: tuple[str, ...]
    disconnect_invalidates: bool

    def create_analyzer(self, claim: StreamClaim) -> ContinuousCaptureAnalyzer:
        """Build the projection-owned analyzer for one connection epoch."""

        ...

    def counters(self) -> dict[str, int]:
        ...

    def finalize_segment(
        self,
        runtime: "ContinuousStreamRuntime",
        **kwargs: Any,
    ) -> None:
        ...

    def begin_recovery(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        claim: StreamClaim,
        entries: Sequence[tuple[Path, DurableRawSpoolSegment]],
        object_store: FilesystemRawArchiveObjectStore,
    ) -> Any:
        ...

    def recover_segment(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        context: Any,
        claim: StreamClaim,
        segment: DurableRawSpoolSegment,
        terminal: bool,
        truncated_tail_bytes: int,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        event_writer: _SessionEventWriter,
        recovery_event_id: str,
    ) -> int:
        ...

    def complete_recovery(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        context: Any,
        claim: StreamClaim,
        recovery_event_id: str,
    ) -> Mapping[str, Any]:
        ...


class CoinbaseMarketTradeProjectionAdapter:
    projection_id = "market.trade_projection.v1"
    primary_channel = "market_trades"
    channels = ("market_trades", "heartbeats")
    disconnect_invalidates = False

    def create_analyzer(self, claim: StreamClaim) -> _CaptureAnalyzer:
        return _CaptureAnalyzer(claim, primary_channel=self.primary_channel)

    def counters(self) -> dict[str, int]:
        return {"trade_inserted": 0, "trade_noop": 0}

    def finalize_segment(
        self,
        runtime: "ContinuousStreamRuntime",
        **kwargs: Any,
    ) -> None:
        runtime._finalize_trade_segment(**kwargs)

    def begin_recovery(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        claim: StreamClaim,
        entries: Sequence[tuple[Path, DurableRawSpoolSegment]],
        object_store: FilesystemRawArchiveObjectStore,
    ) -> None:
        del runtime, claim, entries, object_store
        return None

    def recover_segment(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        context: Any,
        claim: StreamClaim,
        segment: DurableRawSpoolSegment,
        terminal: bool,
        truncated_tail_bytes: int,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        event_writer: _SessionEventWriter,
        recovery_event_id: str,
    ) -> int:
        del context, terminal, truncated_tail_bytes, event_writer, recovery_event_id
        return runtime._recover_trade_segment(
            claim=claim,
            segment=segment,
            object_store=object_store,
            temporary_root=temporary_root,
        )

    def complete_recovery(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        context: Any,
        claim: StreamClaim,
        recovery_event_id: str,
    ) -> Mapping[str, Any]:
        del context
        closed = runtime.repository.close_open_session_coverages(
            claim,
            closing_session_event_id=recovery_event_id,
            reason="collector_restart_closed_at_last_proven_event",
        )
        return {"closed_open_coverage_intervals": int(closed)}


class CoinbaseLevel2BookProjectionAdapter:
    projection_id = "market.level2_book_projection.v1"
    primary_channel = "level2"
    channels = ("level2", "heartbeats")
    disconnect_invalidates = True

    def create_analyzer(self, claim: StreamClaim) -> _CaptureAnalyzer:
        return _CaptureAnalyzer(claim, primary_channel=self.primary_channel)

    def counters(self) -> dict[str, int]:
        return {
            "book_snapshots": 0,
            "book_batches": 0,
            "book_mutations": 0,
            "book_checkpoints": 0,
            "book_features": 0,
        }

    def finalize_segment(
        self,
        runtime: "ContinuousStreamRuntime",
        **kwargs: Any,
    ) -> None:
        runtime._finalize_level2_segment(**kwargs)

    def begin_recovery(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        claim: StreamClaim,
        entries: Sequence[tuple[Path, DurableRawSpoolSegment]],
        object_store: FilesystemRawArchiveObjectStore,
    ) -> dict[str, Any]:
        del runtime, claim, object_store
        return {
            "states": {},
            "parsers": {},
            "analyzers": {},
            "excluded_spool_ids": {
                probe.spool_segment_id for _path, probe in entries
            },
            "counters": {"manifests": 0, "quality_events": 0, **self.counters()},
        }

    def recover_segment(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        context: dict[str, Any],
        claim: StreamClaim,
        segment: DurableRawSpoolSegment,
        terminal: bool,
        truncated_tail_bytes: int,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        event_writer: _SessionEventWriter,
        recovery_event_id: str,
    ) -> int:
        epoch = segment.connection_epoch
        states = context["states"]
        parsers = context["parsers"]
        analyzers = context["analyzers"]
        if epoch not in states:
            states[epoch], parsers[epoch], analyzers[epoch] = (
                runtime._restore_level2_projection(
                    claim=claim,
                    connection_epoch=epoch,
                    object_store=object_store,
                    excluded_spool_segment_ids=context["excluded_spool_ids"],
                )
            )
        records = tuple(segment.records())
        parser = parsers[epoch]
        analyzer = analyzers[epoch]
        for record in records:
            events = parser.parse_raw(
                record.raw_frame,
                received_at=record.received_at.isoformat(),
                raw_ref={
                    "raw_record_id": record.raw_record_id,
                    "spool_segment_id": record.spool_segment_id,
                    "connection_epoch": record.connection_epoch,
                    "receive_ordinal": record.receive_ordinal,
                },
            )
            analyzer.observe(record, events)
        external_quality: tuple[Mapping[str, Any], ...] = ()
        if terminal:
            last_record = records[-1]
            external_quality = (
                {
                    "dedupe_hash": _stable_hash(
                        {
                            "classification": "collector_restart_gap",
                            "connection_epoch": epoch,
                            "receive_ordinal": last_record.receive_ordinal,
                            "spool_segment_id": segment.spool_segment_id,
                        }
                    ),
                    "connection_epoch": epoch,
                    "receive_ordinal": last_record.receive_ordinal,
                    "channel": self.primary_channel,
                    "classification": "collector_restart_gap",
                    "reason": (
                        "worker restart ended transport continuity at the last "
                        "durable raw record"
                    ),
                    "detected_at": datetime.now(UTC),
                    "raw_record_id": last_record.raw_record_id,
                    "sequence_before": analyzer.last_sequence,
                    "sequence_after": None,
                    "invalidating": True,
                    "evidence": {
                        "restart_recovery": True,
                        "truncated_tail_bytes": truncated_tail_bytes,
                    },
                },
            )
        analysis = analyzer.finalize()
        runtime._finalize_level2_segment(
            claim=claim,
            checkpoint=_SegmentCheckpoint(
                segment=segment,
                analysis=analysis,
                terminal=terminal,
                terminal_reason="collector_restart_recovery",
                closing_session_event_id=recovery_event_id,
                external_quality=external_quality,
            ),
            object_store=object_store,
            temporary_root=temporary_root,
            event_writer=event_writer,
            states=states,
            counters=context["counters"],
        )
        analyzer.quality.clear()
        return len(records)

    def complete_recovery(
        self,
        runtime: "ContinuousStreamRuntime",
        *,
        context: dict[str, Any],
        claim: StreamClaim,
        recovery_event_id: str,
    ) -> Mapping[str, Any]:
        del runtime, claim, recovery_event_id
        return {
            "closed_open_coverage_intervals": 0,
            "projection_recovery": dict(context["counters"]),
        }


class ContinuousStreamRuntime:
    """Provider-neutral long-lived lease, WAL, archive, and reconnect runtime."""

    def __init__(
        self,
        *,
        repository: PostgresMarketStructureRepository = market_structure_repository,
    ) -> None:
        self.repository = repository

    async def run(
        self,
        *,
        definition_id: str,
        owner_id: str,
        stop_requested: Callable[[], bool],
        bounded_validation: bool,
        storage_root: Path = DEFAULT_STORAGE_ROOT,
        projection: Optional[ContinuousProjectionAdapter] = None,
        transport: Optional[ContinuousTransportAdapter] = None,
    ) -> dict[str, Any]:
        """Collect until the supervisor requests stop or a fatal invariant fails."""

        definitions = self.repository.list_stream_definitions(
            definition_id=definition_id
        )
        if len(definitions) != 1:
            raise ValueError(
                f"market_stream_definition_unknown: definition_id={definition_id}"
            )
        definition = definitions[0]
        if projection is None or transport is None:
            raise ValueError(
                "continuous_collector_adapter_required: both transport and "
                "projection adapters must be selected by the supervisor"
            )
        active_projection = projection
        active_transport = transport
        if not active_transport.supports(definition):
            raise ValueError(
                "continuous_collector_transport_mismatch: "
                f"definition_id={definition_id} "
                f"transport={active_transport.transport_id}"
            )
        definition_channels = tuple(definition.get("channels") or ())
        if definition_channels != active_projection.channels:
            raise ValueError(
                "continuous_collector_projection_mismatch: "
                f"definition_id={definition_id} "
                f"projection={active_projection.projection_id} "
                f"channels={definition_channels}"
            )
        primary = active_projection.primary_channel
        policy_payload = dict(definition.get("config") or {}).get("runtime_policy")
        policy = ContinuousStreamPolicy.from_mapping(
            policy_payload if isinstance(policy_payload, Mapping) else None
        )
        storage = Path(storage_root).expanduser().resolve()
        spool_root = storage / "spool"
        object_store = FilesystemRawArchiveObjectStore(storage / "objects")
        temporary_root = storage / "tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        await self._recover_orphaned_spools(
            definition=definition,
            owner_id=owner_id,
            lease_seconds=policy.lease_seconds,
            spool_root=spool_root,
            object_store=object_store,
            temporary_root=temporary_root,
            projection=active_projection,
        )
        claim = self.repository.claim_stream(
            definition_id=definition_id,
            owner_id=owner_id,
            lease_seconds=policy.lease_seconds,
            bounded=bounded_validation,
        )
        event_writer = _SessionEventWriter(self.repository, claim)
        queue: asyncio.Queue[_SegmentCheckpoint | None] = asyncio.Queue(
            maxsize=policy.max_inflight_segments
        )
        projection_states: dict[int, Any] = {}
        counters: dict[str, int] = {
            "raw_records": 0,
            "raw_bytes": 0,
            "segments": 0,
            "manifests": 0,
            "reconnects": 0,
            "quality_events": 0,
            **active_projection.counters(),
        }
        finalizer = asyncio.create_task(
            self._finalizer_loop(
                claim=claim,
                queue=queue,
                object_store=object_store,
                temporary_root=temporary_root,
                event_writer=event_writer,
                states=projection_states,
                counters=counters,
                projection=active_projection,
            )
        )
        started_at = datetime.now(UTC)
        stream = active_transport.create_stream(claim)
        current_segment: DurableRawSpoolSegment | None = None
        pending: asyncio.Task | None = None
        connection_epoch = -1
        disconnect_started: float | None = None
        backoff = policy.reconnect.initial_backoff_seconds
        last_lease_heartbeat = time.monotonic()
        try:
            while not stop_requested():
                self._raise_if_finalizer_failed(finalizer, definition_id)
                connection_epoch += 1
                analyzer = active_projection.create_analyzer(claim)
                parser = active_transport.create_parser(claim)
                segment_ordinal = 0
                segment_started = time.monotonic()
                disconnect_reason: Optional[str] = None
                invalid_stream_state = False
                try:
                    await stream.connect()
                    await asyncio.to_thread(
                        event_writer.append,
                        connection_epoch=connection_epoch,
                        event_type=(
                            "connected" if connection_epoch == 0 else "reconnected"
                        ),
                        occurred_at=datetime.now(UTC),
                        evidence={
                            "bounded_validation": bounded_validation,
                            "policy": policy.to_dict(),
                        },
                    )
                    await stream.subscribe([active_transport.subscription(claim)])
                    await asyncio.to_thread(
                        event_writer.append,
                        connection_epoch=connection_epoch,
                        event_type="subscription_sent",
                        occurred_at=datetime.now(UTC),
                        evidence={
                            "product_id": claim.provider_product_id,
                            "channels": list(claim.channels),
                            "auth_mode": claim.auth_mode,
                        },
                    )
                    iterator = stream.raw_messages().__aiter__()
                    while not stop_requested():
                        self._raise_if_finalizer_failed(finalizer, definition_id)
                        if pending is None:
                            pending = asyncio.create_task(anext(iterator))
                        done, _ = await asyncio.wait({pending}, timeout=1.0)
                        now_monotonic = time.monotonic()
                        if (
                            now_monotonic - last_lease_heartbeat
                            >= policy.heartbeat_seconds
                    ):
                            await asyncio.to_thread(
                                self.repository.heartbeat,
                                claim,
                                lease_seconds=policy.lease_seconds,
                            )
                            last_lease_heartbeat = now_monotonic
                        if not done:
                            if (
                                analyzer.last_heartbeat_at is not None
                                and (
                                    datetime.now(UTC) - analyzer.last_heartbeat_at
                                ).total_seconds()
                                > policy.reconnect.heartbeat_stale_seconds
                            ):
                                analyzer.observe_idle(datetime.now(UTC))
                                disconnect_reason = "heartbeat_stale"
                                invalid_stream_state = True
                                break
                            continue
                        try:
                            message = pending.result()
                        except StopAsyncIteration:
                            disconnect_reason = "provider_stream_ended"
                            break
                        finally:
                            pending = None
                        if message.connection_epoch != connection_epoch:
                            raise RuntimeError(
                                "continuous_collector_epoch_mismatch: "
                                f"definition_id={definition_id} "
                                f"expected={connection_epoch} "
                                f"actual={message.connection_epoch}"
                            )
                        # A TCP/WebSocket handshake alone is not recovery.  Reset
                        # the continuous-disconnect budget only after provider
                        # data is actually delivered on the new epoch.
                        disconnect_started = None
                        backoff = policy.reconnect.initial_backoff_seconds
                        estimated_spool_bytes = len(message.raw_frame) * 2 + 4096
                        require_spool_capacity(
                            root=spool_root,
                            max_backlog_bytes=claim.max_spool_bytes,
                            next_frame_bytes=estimated_spool_bytes,
                            definition_id=claim.definition_id,
                        )
                        if current_segment is None:
                            current_segment = DurableRawSpoolSegment(
                                root=spool_root,
                                definition_id=claim.definition_id,
                                session_id=claim.session_id,
                                connection_epoch=connection_epoch,
                                segment_ordinal=segment_ordinal,
                            )
                            segment_started = now_monotonic
                        elif (
                            current_segment.record_count > 0
                            and (
                                current_segment.current_bytes
                                + estimated_spool_bytes
                                > claim.max_segment_bytes
                                or now_monotonic - segment_started
                                >= policy.segment_max_seconds
                            )
                        ):
                            checkpoint_analysis = analyzer.finalize()
                            self._enqueue_checkpoint(
                                queue,
                                segment=current_segment,
                                analysis=checkpoint_analysis,
                                definition_id=definition_id,
                            )
                            analyzer.quality.clear()
                            counters["segments"] += 1
                            current_segment = DurableRawSpoolSegment(
                                root=spool_root,
                                definition_id=claim.definition_id,
                                session_id=claim.session_id,
                                connection_epoch=connection_epoch,
                                segment_ordinal=segment_ordinal + 1,
                            )
                            segment_ordinal += 1
                            segment_started = now_monotonic
                        record = RawStreamRecord.from_provider_message(
                            message,
                            definition_id=claim.definition_id,
                            spool_segment_id=current_segment.spool_segment_id,
                            provider_product_id=claim.provider_product_id,
                            requested_channel=primary,
                            observed_channel=active_transport.observed_channel(
                                message.raw_frame
                            ),
                        )
                        current_segment.append(record)
                        events = parser.parse_raw(
                            message.raw_frame,
                            received_at=message.received_at,
                            raw_ref={
                                **message.evidence_ref(),
                                "raw_record_id": record.raw_record_id,
                                "spool_segment_id": record.spool_segment_id,
                            },
                        )
                        analyzer.observe(record, events)
                        counters["raw_records"] += 1
                        counters["raw_bytes"] += len(record.raw_frame)
                        if any(
                            bool(item.get("invalidating"))
                            for item in analyzer.quality
                        ):
                            disconnect_reason = "invalid_stream_evidence"
                            invalid_stream_state = True
                            break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    disconnect_reason = f"{type(exc).__name__}: {exc}"
                    logger.warning(
                        "continuous_collector_connection_failed | "
                        "definition_id=%s session_id=%s connection_epoch=%s "
                        "error=%s",
                        definition_id,
                        claim.session_id,
                        connection_epoch,
                        exc,
                    )
                finally:
                    if pending is not None:
                        pending.cancel()
                        with contextlib.suppress(
                            asyncio.CancelledError, StopAsyncIteration
                        ):
                            await pending
                        pending = None
                    with contextlib.suppress(Exception):
                        await stream.close()

                stopping = stop_requested()
                closing_event_id = await asyncio.to_thread(
                    event_writer.append,
                    connection_epoch=connection_epoch,
                    event_type=(
                        "continuous_capture_stopped"
                        if stopping
                        else "provider_disconnected"
                    ),
                    occurred_at=datetime.now(UTC),
                    reason=(
                        "supervisor_stop_requested" if stopping else disconnect_reason
                    ),
                    evidence={
                        "raw_record_count": analyzer.raw_count,
                        "invalid_stream_state": invalid_stream_state,
                    },
                )
                if current_segment is not None and current_segment.record_count > 0:
                    external_quality: tuple[Mapping[str, Any], ...] = ()
                    if not stopping and analyzer.last_record is not None:
                        external_quality = (
                            {
                                "dedupe_hash": _stable_hash(
                                    {
                                        "classification": "disconnect",
                                        "connection_epoch": connection_epoch,
                                        "receive_ordinal": (
                                            analyzer.last_record.receive_ordinal
                                        ),
                                        "reason": disconnect_reason,
                                    }
                                ),
                                "connection_epoch": connection_epoch,
                                "receive_ordinal": analyzer.last_record.receive_ordinal,
                                "channel": primary,
                                "classification": "disconnect",
                                "reason": (
                                    disconnect_reason or "provider disconnected"
                                ),
                                "detected_at": datetime.now(UTC),
                                "raw_record_id": analyzer.last_record.raw_record_id,
                                "sequence_before": analyzer.last_sequence,
                                "sequence_after": None,
                                "invalidating": (
                                    active_projection.disconnect_invalidates
                                ),
                                "evidence": {"reconnect_planned": True},
                            },
                        )
                    self._enqueue_checkpoint(
                        queue,
                        segment=current_segment,
                        analysis=analyzer.finalize(),
                        definition_id=definition_id,
                        terminal=True,
                        terminal_reason=(
                            "supervisor_stop_requested"
                            if stopping
                            else disconnect_reason
                        ),
                        closing_session_event_id=closing_event_id,
                        external_quality=external_quality,
                    )
                    counters["segments"] += 1
                    current_segment = None
                elif current_segment is not None:
                    current_segment.close()
                    with contextlib.suppress(FileNotFoundError):
                        current_segment.open_path.unlink()
                    current_segment = None
                if stopping:
                    break
                if not policy.reconnect.enabled:
                    raise RuntimeError(
                        "continuous_collector_reconnect_disabled: "
                        f"definition_id={definition_id} reason={disconnect_reason}"
                    )
                now_monotonic = time.monotonic()
                disconnect_started = disconnect_started or now_monotonic
                disconnected_for = now_monotonic - disconnect_started
                if (
                    disconnected_for
                    > policy.reconnect.continuous_disconnect_budget_seconds
                ):
                    raise RuntimeError(
                        "continuous_collector_disconnect_budget_exhausted: "
                        f"definition_id={definition_id} "
                        f"seconds={disconnected_for:.3f} "
                        f"reason={disconnect_reason}"
                    )
                counters["reconnects"] += 1
                await self._sleep_until_reconnect(
                    backoff_seconds=backoff,
                    stop_requested=stop_requested,
                )
                backoff = min(
                    policy.reconnect.max_backoff_seconds,
                    max(policy.reconnect.initial_backoff_seconds, backoff * 2),
                )
            await self._drain_finalizer(
                queue,
                finalizer,
                claim=claim,
                heartbeat_seconds=policy.heartbeat_seconds,
                lease_seconds=policy.lease_seconds,
            )
            result = {
                "schema_version": "market.continuous_collector_result.v1",
                "status": "stopped",
                "definition_id": claim.definition_id,
                "session_id": claim.session_id,
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
                "bounded_validation": bounded_validation,
                "policy": policy.to_dict(),
                "spool_backlog_bytes": spool_backlog_bytes(
                    spool_root, definition_id=claim.definition_id
                ),
                **counters,
            }
            logger.info(
                "continuous_collector_stopped | definition_id=%s session_id=%s "
                "raw_records=%s segments=%s manifests=%s reconnects=%s",
                claim.definition_id,
                claim.session_id,
                counters["raw_records"],
                counters["segments"],
                counters["manifests"],
                counters["reconnects"],
            )
            return result
        except BaseException as exc:
            try:
                await asyncio.shield(
                    self._drain_finalizer(
                        queue,
                        finalizer,
                        claim=claim,
                        heartbeat_seconds=policy.heartbeat_seconds,
                        lease_seconds=policy.lease_seconds,
                    )
                )
            except BaseException:
                logger.exception(
                    "continuous_collector_finalizer_drain_failed | "
                    "definition_id=%s session_id=%s",
                    definition_id,
                    claim.session_id,
                )
            with contextlib.suppress(Exception):
                await asyncio.to_thread(
                    event_writer.append,
                    connection_epoch=max(connection_epoch, 0),
                    event_type=(
                        "interrupted"
                        if isinstance(exc, asyncio.CancelledError)
                        else "failed"
                    ),
                    occurred_at=datetime.now(UTC),
                    reason=(
                        "continuous collector interrupted; durable spool retained"
                        if isinstance(exc, asyncio.CancelledError)
                        else "continuous collector failed; inspect correlated exception log"
                    ),
                    evidence={
                        "spool_backlog_bytes": spool_backlog_bytes(
                            spool_root, definition_id=definition_id
                        )
                    },
                )
            if not isinstance(exc, asyncio.CancelledError):
                logger.exception(
                    "continuous_collector_failed | definition_id=%s session_id=%s",
                    definition_id,
                    claim.session_id,
                )
            raise
        finally:
            if current_segment is not None:
                with contextlib.suppress(Exception):
                    current_segment.close()
            with contextlib.suppress(Exception):
                await stream.close()
            try:
                await asyncio.to_thread(self.repository.release, claim)
            except Exception as exc:
                logger.exception(
                    "continuous_collector_lease_release_failed | "
                    "definition_id=%s session_id=%s lease_generation=%s",
                    definition_id,
                    claim.session_id,
                    claim.lease_generation,
                )
                raise RuntimeError(
                    "continuous_collector_lease_release_failed: "
                    f"definition_id={definition_id} session_id={claim.session_id}"
                ) from exc

    async def _recover_orphaned_spools(
        self,
        *,
        definition: Mapping[str, Any],
        owner_id: str,
        lease_seconds: float,
        spool_root: Path,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        projection: ContinuousProjectionAdapter,
    ) -> None:
        await asyncio.to_thread(
            self._recover_orphaned_spools_sync,
            definition=definition,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            spool_root=spool_root,
            object_store=object_store,
            temporary_root=temporary_root,
            projection=projection,
        )

    def _recover_orphaned_spools_sync(
        self,
        *,
        definition: Mapping[str, Any],
        owner_id: str,
        lease_seconds: float,
        spool_root: Path,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        projection: ContinuousProjectionAdapter,
    ) -> None:
        active_projection = projection
        definition_id = str(definition["id"])
        grouped: dict[str, list[tuple[Path, DurableRawSpoolSegment]]] = {}
        for path in discover_spool_segments(spool_root):
            probe = DurableRawSpoolSegment.from_path(path)
            if probe.definition_id != definition_id:
                continue
            grouped.setdefault(probe.session_id, []).append((path, probe))
        for session_id, entries in sorted(grouped.items()):
            recovery_lease_seconds = max(float(lease_seconds), 600.0)
            claim = self.repository.claim_stream(
                definition_id=definition_id,
                owner_id=owner_id,
                lease_seconds=recovery_lease_seconds,
                bounded=True,
                resume_session_id=session_id,
            )
            try:
                event_writer = _SessionEventWriter(
                    self.repository,
                    claim,
                    initial_ordinal=self.repository.next_session_event_ordinal(claim),
                )
                max_epoch = max(probe.connection_epoch for _path, probe in entries)
                recovery_event_id = event_writer.append(
                    connection_epoch=max_epoch,
                    event_type="collector_restart_recovery_started",
                    occurred_at=datetime.now(UTC),
                    reason="durable spool segments found after worker interruption",
                    evidence={"spool_segment_count": len(entries)},
                )
                ordered_entries = sorted(
                    entries,
                    key=lambda item: (
                        item[1].connection_epoch,
                        item[1].segment_ordinal,
                        str(item[0]),
                    ),
                )
                last_segment_by_epoch = {
                    epoch: max(
                        probe.segment_ordinal
                        for _path, probe in ordered_entries
                        if probe.connection_epoch == epoch and probe.record_count > 0
                    )
                    for epoch in {
                        probe.connection_epoch
                        for _path, probe in ordered_entries
                        if probe.record_count > 0
                    }
                }
                projection_context = active_projection.begin_recovery(
                    self,
                    claim=claim,
                    entries=ordered_entries,
                    object_store=object_store,
                )
                recovered_records = 0
                recovered_segments = 0
                for path, probe in ordered_entries:
                    self.repository.heartbeat(
                        claim, lease_seconds=recovery_lease_seconds
                    )
                    segment = probe
                    truncated_tail_bytes = (
                        probe.recovery_evidence.truncated_tail_bytes
                        if probe.recovery_evidence is not None
                        else 0
                    )
                    if path.suffix == ".open":
                        if segment.record_count == 0:
                            segment.close()
                            with contextlib.suppress(FileNotFoundError):
                                segment.open_path.unlink()
                            event_writer.append(
                                connection_epoch=probe.connection_epoch,
                                event_type="empty_spool_discarded",
                                occurred_at=datetime.now(UTC),
                                evidence={
                                    "spool_segment_id": probe.spool_segment_id,
                                    "truncated_tail_bytes": truncated_tail_bytes,
                                },
                            )
                            continue
                        segment.seal()
                    terminal = (
                        segment.segment_ordinal
                        == last_segment_by_epoch.get(segment.connection_epoch)
                    )
                    record_count = active_projection.recover_segment(
                        self,
                        context=projection_context,
                        claim=claim,
                        segment=segment,
                        terminal=terminal,
                        truncated_tail_bytes=truncated_tail_bytes,
                        object_store=object_store,
                        temporary_root=temporary_root,
                        event_writer=event_writer,
                        recovery_event_id=recovery_event_id,
                    )
                    recovered_records += record_count
                    recovered_segments += 1
                    event_writer.append(
                        connection_epoch=segment.connection_epoch,
                        event_type="spool_segment_recovered",
                        occurred_at=datetime.now(UTC),
                        evidence={
                            "spool_segment_id": segment.spool_segment_id,
                            "record_count": record_count,
                            "truncated_tail_bytes": truncated_tail_bytes,
                        },
                    )
                projection_evidence = active_projection.complete_recovery(
                    self,
                    context=projection_context,
                    claim=claim,
                    recovery_event_id=recovery_event_id,
                )
                closed_intervals = int(
                    projection_evidence.get("closed_open_coverage_intervals", 0)
                )
                event_writer.append(
                    connection_epoch=max_epoch,
                    event_type="collector_restart_recovery_completed",
                    occurred_at=datetime.now(UTC),
                    evidence={
                        "recovered_segments": recovered_segments,
                        "recovered_records": recovered_records,
                        "projection_id": active_projection.projection_id,
                        **dict(projection_evidence),
                    },
                )
                logger.warning(
                    "continuous_collector_spool_recovered | definition_id=%s "
                    "session_id=%s segments=%s records=%s closed_intervals=%s",
                    definition_id,
                    session_id,
                    recovered_segments,
                    recovered_records,
                    closed_intervals,
                )
            finally:
                with contextlib.suppress(Exception):
                    self.repository.release(claim)

    @staticmethod
    def _book_position_from_row(
        row: Mapping[str, Any],
        *,
        prefix: str,
        definition_id: str,
        provider_product_id: str,
    ) -> BookSourcePosition:
        return BookSourcePosition(
            definition_id=definition_id,
            session_id=str(row[f"{prefix}_session_id"]),
            connection_epoch=int(row[f"{prefix}_connection_epoch"]),
            provider_product_id=provider_product_id,
            provider_sequence_num=(
                int(row[f"{prefix}_sequence_num"])
                if row.get(f"{prefix}_sequence_num") is not None
                else None
            ),
            receive_ordinal=int(row[f"{prefix}_receive_ordinal"]),
            event_ordinal=int(row[f"{prefix}_event_ordinal"]),
        )

    def _restore_level2_projection(
        self,
        *,
        claim: StreamClaim,
        connection_epoch: int,
        object_store: FilesystemRawArchiveObjectStore,
        excluded_spool_segment_ids: set[str],
    ) -> tuple[_EpochProjectionState, CoinbaseMessageParser, _CaptureAnalyzer]:
        """Restore a valid reducer from a verified checkpoint plus raw delta."""

        state = _EpochProjectionState()
        parser = CoinbaseMessageParser(
            symbol_by_product_id={
                claim.provider_product_id: claim.provider_product_id
            }
        )
        analyzer = _CaptureAnalyzer(claim, primary_channel="level2")
        reconstruction = self.repository.get_book_reconstruction_state(
            definition_id=claim.definition_id,
            session_id=claim.session_id,
            connection_epoch=connection_epoch,
        )
        if reconstruction is None:
            return state, parser, analyzer
        lifecycle = BookLifecycle(str(reconstruction["lifecycle"]))
        if lifecycle is not BookLifecycle.VALID:
            return state, parser, analyzer

        checkpoint_rows = [
            row
            for row in self.repository.list_book_checkpoints(
                definition_id=claim.definition_id,
                session_id=claim.session_id,
            )
            if int(row["connection_epoch"]) == int(connection_epoch)
            and (
                int(row["receive_ordinal"]),
                int(row["event_ordinal"]),
            )
            <= (
                int(reconstruction["receive_ordinal"]),
                int(reconstruction["event_ordinal"]),
            )
        ]
        if not checkpoint_rows:
            raise RuntimeError(
                "continuous_l2_recovery_checkpoint_missing: "
                f"definition_id={claim.definition_id} "
                f"session_id={claim.session_id} epoch={connection_epoch}"
            )
        checkpoint_row = max(
            checkpoint_rows,
            key=lambda row: (
                int(row["receive_ordinal"]),
                int(row["event_ordinal"]),
            ),
        )
        if (
            str(checkpoint_row["reconstruction_version"])
            != BOOK_RECONSTRUCTION_VERSION
            or str(checkpoint_row["schema_version"])
            != BOOK_CHECKPOINT_SCHEMA_VERSION
            or str(checkpoint_row["format"]) != BOOK_CHECKPOINT_FORMAT
            or str(checkpoint_row["compression"]) != BOOK_CHECKPOINT_COMPRESSION
        ):
            raise RuntimeError(
                "continuous_l2_recovery_checkpoint_contract_mismatch: "
                f"checkpoint_id={checkpoint_row['id']}"
            )
        opening_row = self.repository.get_book_validity_opening(
            series_id=claim.series_id,
            interval_id=str(checkpoint_row["validity_interval_id"]),
        )
        if opening_row is None:
            raise RuntimeError(
                "continuous_l2_recovery_validity_missing: "
                f"checkpoint_id={checkpoint_row['id']}"
            )
        opening_position = self._book_position_from_row(
            opening_row,
            prefix="opening",
            definition_id=claim.definition_id,
            provider_product_id=claim.provider_product_id,
        )
        last_position = self._book_position_from_row(
            opening_row,
            prefix="last",
            definition_id=claim.definition_id,
            provider_product_id=claim.provider_product_id,
        )
        validity = BookValidityIntervalVersion(
            version_id=str(opening_row["id"]),
            interval_id=str(opening_row["interval_id"]),
            revision=int(opening_row["revision"]),
            series_id=int(opening_row["series_id"]),
            status=BookValidityStatus(str(opening_row["status"])),
            ordering_assurance=OrderingAssurance(
                str(opening_row["ordering_assurance"])
            ),
            opening_snapshot_id=str(opening_row["opening_snapshot_id"]),
            opening_position=opening_position,
            opening_effective_at=opening_row["opening_effective_at"],
            opening_known_at=opening_row["opening_known_at"],
            last_valid_position=last_position,
            last_valid_effective_at=opening_row["last_valid_effective_at"],
            last_state_hash=str(opening_row["last_state_hash"]),
            known_at=opening_row["known_at"],
        )
        checkpoint_path = object_store.local_path(str(checkpoint_row["object_key"]))
        if not checkpoint_path.exists():
            raise RuntimeError(
                "continuous_l2_recovery_checkpoint_object_missing: "
                f"checkpoint_id={checkpoint_row['id']}"
            )
        checkpoint_digest = hashlib.sha256(checkpoint_path.read_bytes()).hexdigest()
        if checkpoint_digest != str(checkpoint_row["object_sha256"]):
            raise RuntimeError(
                "continuous_l2_recovery_checkpoint_checksum_mismatch: "
                f"checkpoint_id={checkpoint_row['id']}"
            )
        level_rows = read_book_checkpoint_parquet(checkpoint_path)
        bids = tuple(
            (Decimal(str(row["price"])), Decimal(str(row["quantity"])))
            for row in level_rows
            if str(row["side"]) == BookSide.BID.value
        )
        asks = tuple(
            (Decimal(str(row["price"])), Decimal(str(row["quantity"])))
            for row in level_rows
            if str(row["side"]) == BookSide.ASK.value
        )
        checkpoint_position = BookSourcePosition(
            definition_id=claim.definition_id,
            session_id=str(checkpoint_row["session_id"]),
            connection_epoch=int(checkpoint_row["connection_epoch"]),
            provider_product_id=claim.provider_product_id,
            provider_sequence_num=(
                int(checkpoint_row["provider_sequence_num"])
                if checkpoint_row.get("provider_sequence_num") is not None
                else None
            ),
            receive_ordinal=int(checkpoint_row["receive_ordinal"]),
            event_ordinal=int(checkpoint_row["event_ordinal"]),
        )
        checkpoint = BookCheckpointFact(
            checkpoint_id=str(checkpoint_row["id"]),
            series_id=int(checkpoint_row["series_id"]),
            validity_interval_id=str(checkpoint_row["validity_interval_id"]),
            source_position=checkpoint_position,
            product_definition_version_id=str(
                checkpoint_row["product_definition_version_id"]
            ),
            provider_size_unit=ProviderSizeUnit(
                str(checkpoint_row["provider_size_unit"])
            ),
            ordering_assurance=validity.ordering_assurance,
            effective_at=checkpoint_row["effective_at"],
            known_at=checkpoint_row["known_at"],
            state_hash=str(checkpoint_row["state_hash"]),
            bids=bids,
            asks=asks,
            mutation_count_since_prior=int(
                checkpoint_row["mutation_count_since_prior"]
            ),
        )
        if (
            checkpoint.content_fingerprint
            != str(checkpoint_row["content_fingerprint"])
            or len(level_rows) != int(checkpoint_row["level_count"])
            or len(bids) != int(checkpoint_row["bid_level_count"])
            or len(asks) != int(checkpoint_row["ask_level_count"])
        ):
            raise RuntimeError(
                "continuous_l2_recovery_checkpoint_material_mismatch: "
                f"checkpoint_id={checkpoint.checkpoint_id}"
            )
        config = dict(claim.config or {})
        contract = L2ProductContract(
            provider_product_id=claim.provider_product_id,
            product_definition_version_id=str(
                config.get("product_definition_version_id") or ""
            ),
            provider_size_unit=str(config.get("provider_size_unit") or ""),
            price_increment=config.get("price_increment"),
            quantity_increment=config.get("quantity_increment"),
        )
        reducer = Level2BookReconstructor.from_checkpoint(
            checkpoint,
            contract=contract,
            validity=validity,
        )
        replay_manifests = [
            row
            for row in self.repository.list_session_manifests(
                definition_id=claim.definition_id,
                session_id=claim.session_id,
            )
            if int(row["connection_epoch"]) == int(connection_epoch)
            and str(row["spool_segment_id"])
            not in excluded_spool_segment_ids
        ]
        for manifest in replay_manifests:
            path = object_store.local_path(str(manifest["object_key"]))
            if not path.exists():
                raise RuntimeError(
                    "continuous_l2_recovery_raw_object_missing: "
                    f"manifest_id={manifest['id']}"
                )
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != str(manifest["object_sha256"]):
                raise RuntimeError(
                    "continuous_l2_recovery_raw_checksum_mismatch: "
                    f"manifest_id={manifest['id']}"
                )
            for record in read_raw_archive_parquet(path):
                if record.receive_ordinal < checkpoint_position.receive_ordinal:
                    continue
                events = parser.parse_raw(
                    record.raw_frame,
                    received_at=record.received_at.isoformat(),
                    raw_ref={
                        "raw_record_id": record.raw_record_id,
                        "spool_segment_id": record.spool_segment_id,
                        "connection_epoch": record.connection_epoch,
                        "receive_ordinal": record.receive_ordinal,
                    },
                )
                analyzer.observe(record, events)
                for event in events:
                    if event.event_kind not in {
                        "market_l2_snapshot",
                        "market_l2_update",
                    }:
                        continue
                    fact = translate_coinbase_l2_event(
                        event,
                        raw_record=record,
                        contract=contract,
                        accepted_at=record.received_at,
                    )
                    if (
                        fact.position.connection_epoch,
                        fact.position.receive_ordinal,
                        fact.position.event_ordinal,
                    ) <= (
                        checkpoint_position.connection_epoch,
                        checkpoint_position.receive_ordinal,
                        checkpoint_position.event_ordinal,
                    ):
                        continue
                    reducer.process(fact)
                    state.book_last_position = fact.position
        analyzer.quality.clear()
        if (
            reducer.lifecycle is not BookLifecycle.VALID
            or reducer.current_state_hash != str(reconstruction["state_hash"])
            or reducer.current_interval is None
            or reducer.current_interval.interval_id
            != str(reconstruction["validity_interval_id"])
        ):
            raise RuntimeError(
                "continuous_l2_recovery_reconciliation_failed: "
                f"definition_id={claim.definition_id} "
                f"session_id={claim.session_id} epoch={connection_epoch}"
            )
        state.book_reducer = reducer
        state.book_checkpoint_id = checkpoint.checkpoint_id
        state.book_last_position = reducer.current_interval.last_valid_position
        return state, parser, analyzer

    def _recover_trade_segment(
        self,
        *,
        claim: StreamClaim,
        segment: DurableRawSpoolSegment,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
    ) -> int:
        encoded, acknowledgement, records = publish_spool_archive(
            segment,
            object_store=object_store,
            temporary_directory=temporary_root,
        )
        commit = self.repository.commit_archive(
            claim,
            encoded=encoded,
            acknowledgement=acknowledgement,
            records=records,
        )
        parser = CoinbaseMessageParser(
            symbol_by_product_id={
                claim.provider_product_id: claim.provider_product_id
            }
        )
        translated = []
        for record in records:
            events = parser.parse_raw(
                record.raw_frame,
                received_at=record.received_at.isoformat(),
                raw_ref={
                    "raw_record_id": record.raw_record_id,
                    "spool_segment_id": record.spool_segment_id,
                    "connection_epoch": record.connection_epoch,
                    "receive_ordinal": record.receive_ordinal,
                },
            )
            for event in events:
                if event.event_kind == "market_trade":
                    translated.append(
                        translate_coinbase_market_trade(
                            event,
                            contract=self.repository.get_product_contract(
                                str(claim.config["product_definition_version_id"])
                            ),
                            raw_record_id=record.raw_record_id,
                            connection_epoch=record.connection_epoch,
                            receive_ordinal=record.receive_ordinal,
                            accepted_at=datetime.now(UTC),
                            coverage_interval_id=None,
                        )
                    )
        self.repository.ingest_trades(
            claim,
            facts=translated,
            require_archive_mapping=True,
        )
        segment.mark_database_acknowledged(
            manifest_id=commit.manifest_id,
            object_key=acknowledgement.object_key,
            object_sha256=acknowledgement.sha256,
        )
        segment.discard_acknowledged_spool()
        return len(records)

    @staticmethod
    def _raise_if_finalizer_failed(
        finalizer: asyncio.Task, definition_id: str
    ) -> None:
        if not finalizer.done():
            return
        error = finalizer.exception()
        if error is not None:
            raise RuntimeError(
                f"continuous_collector_finalizer_failed: definition_id={definition_id}"
            ) from error

    async def _drain_finalizer(
        self,
        queue: asyncio.Queue[_SegmentCheckpoint | None],
        finalizer: asyncio.Task,
        *,
        claim: StreamClaim,
        heartbeat_seconds: float,
        lease_seconds: float,
    ) -> None:
        """Keep the fence live until terminal projection work is durable."""

        if finalizer.done():
            await finalizer
            return
        joined = asyncio.create_task(queue.join())
        heartbeat_interval = max(0.01, float(heartbeat_seconds))
        try:
            while True:
                done, _pending = await asyncio.wait(
                    {joined, finalizer},
                    timeout=heartbeat_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if finalizer in done:
                    await finalizer
                    raise RuntimeError(
                        "continuous_collector_finalizer_stopped_before_drain: "
                        f"definition_id={claim.definition_id}"
                    )
                if joined in done:
                    await joined
                    queue.put_nowait(None)
                    await finalizer
                    return
                await asyncio.to_thread(
                    self.repository.heartbeat,
                    claim,
                    lease_seconds=lease_seconds,
                )
        finally:
            if not joined.done():
                joined.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await joined

    @staticmethod
    async def _sleep_until_reconnect(
        *, backoff_seconds: float, stop_requested: Callable[[], bool]
    ) -> None:
        deadline = time.monotonic() + max(0.0, backoff_seconds)
        while not stop_requested() and time.monotonic() < deadline:
            await asyncio.sleep(
                min(0.25, max(0.0, deadline - time.monotonic()))
            )

    @staticmethod
    def _enqueue_checkpoint(
        queue: asyncio.Queue[_SegmentCheckpoint | None],
        *,
        segment: DurableRawSpoolSegment,
        analysis: Any,
        definition_id: str,
        terminal: bool = False,
        terminal_reason: Optional[str] = None,
        closing_session_event_id: Optional[str] = None,
        external_quality: tuple[Mapping[str, Any], ...] = (),
    ) -> None:
        segment.seal()
        try:
            queue.put_nowait(
                _SegmentCheckpoint(
                    segment=segment,
                    analysis=analysis,
                    terminal=terminal,
                    terminal_reason=terminal_reason,
                    closing_session_event_id=closing_session_event_id,
                    external_quality=external_quality,
                )
            )
        except asyncio.QueueFull as exc:
            raise SpoolBackpressureError(
                "continuous_collector_projection_backpressure: "
                f"definition_id={definition_id} finalizer queue is full"
            ) from exc

    async def _finalizer_loop(
        self,
        *,
        claim: StreamClaim,
        queue: asyncio.Queue[_SegmentCheckpoint | None],
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        event_writer: _SessionEventWriter,
        states: dict[int, Any],
        counters: dict[str, int],
        projection: ContinuousProjectionAdapter,
    ) -> None:
        while True:
            checkpoint = await queue.get()
            try:
                if checkpoint is None:
                    return
                await asyncio.to_thread(
                    projection.finalize_segment,
                    self,
                    claim=claim,
                    checkpoint=checkpoint,
                    object_store=object_store,
                    temporary_root=temporary_root,
                    event_writer=event_writer,
                    states=states,
                    counters=counters,
                )
            finally:
                queue.task_done()

    def _finalize_level2_segment(
        self,
        *,
        claim: StreamClaim,
        checkpoint: _SegmentCheckpoint,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        event_writer: _SessionEventWriter,
        states: dict[int, _EpochProjectionState],
        counters: dict[str, int],
    ) -> None:
        """Archive and incrementally reduce one continuous Level 2 segment."""

        segment = checkpoint.segment
        encoded, acknowledgement, records = publish_spool_archive(
            segment,
            object_store=object_store,
            temporary_directory=temporary_root,
        )
        commit = self.repository.commit_archive(
            claim,
            encoded=encoded,
            acknowledgement=acknowledgement,
            records=records,
        )
        counters["manifests"] += 1
        connection_epoch = records[0].connection_epoch
        state = states.setdefault(connection_epoch, _EpochProjectionState())
        config = dict(claim.config or {})
        contract = L2ProductContract(
            provider_product_id=claim.provider_product_id,
            product_definition_version_id=str(
                config.get("product_definition_version_id") or ""
            ),
            provider_size_unit=str(config.get("provider_size_unit") or ""),
            price_increment=config.get("price_increment"),
            quantity_increment=config.get("quantity_increment"),
        )
        if state.book_reducer is None:
            state.book_reducer = Level2BookReconstructor(
                series_id=claim.series_id,
                contract=contract,
                ordering_assurance=OrderingAssurance.PROVIDER_DELIVERY_GUARANTEED,
            )
        reducer = state.book_reducer
        parser = CoinbaseMessageParser(
            symbol_by_product_id={
                claim.provider_product_id: claim.provider_product_id
            }
        )
        quality_by_ordinal: dict[int, list[Mapping[str, Any]]] = {}
        for quality in checkpoint.analysis.quality_events:
            quality_by_ordinal.setdefault(int(quality["receive_ordinal"]), []).append(
                quality
            )

        snapshots = []
        batches = []
        validity_versions = []
        book_checkpoints = []
        book_quality: list[BookQualityEvidence] = []
        direct_quality: list[Mapping[str, Any]] = []
        valid_states = []
        interval_by_position: dict[tuple[int, int], str] = {}
        max_event_ordinal_by_receive: dict[int, int] = {}

        def collect(result) -> None:
            if result.snapshot is not None:
                snapshots.append(result.snapshot)
                interval_by_position[
                    (
                        result.snapshot.event.position.receive_ordinal,
                        result.snapshot.event.position.event_ordinal,
                    )
                ] = result.snapshot.validity_interval_id
            if result.batch is not None:
                batches.append(result.batch)
                interval_by_position[
                    (
                        result.batch.event.position.receive_ordinal,
                        result.batch.event.position.event_ordinal,
                    )
                ] = result.batch.validity_interval_id
            validity_versions.extend(result.validity_versions)
            book_checkpoints.extend(result.checkpoints)
            book_quality.extend(result.quality)
            if result.state is not None:
                valid_states.append(result.state)

        for record in records:
            raw_events = parser.parse_raw(
                record.raw_frame,
                received_at=record.received_at.isoformat(),
                raw_ref={
                    "raw_record_id": record.raw_record_id,
                    "spool_segment_id": record.spool_segment_id,
                    "connection_epoch": record.connection_epoch,
                    "receive_ordinal": record.receive_ordinal,
                },
            )
            sequence_num = next(
                (
                    event.provider_sequence_num
                    for event in raw_events
                    if event.provider_sequence_num is not None
                ),
                None,
            )
            for quality in quality_by_ordinal.get(record.receive_ordinal, []):
                if not bool(quality.get("invalidating")):
                    direct_quality.append(quality)
                    continue
                position = BookSourcePosition(
                    definition_id=claim.definition_id,
                    session_id=claim.session_id,
                    connection_epoch=record.connection_epoch,
                    provider_product_id=claim.provider_product_id,
                    provider_sequence_num=(
                        int(quality["sequence_after"])
                        if quality.get("sequence_after") is not None
                        else sequence_num
                    ),
                    receive_ordinal=record.receive_ordinal,
                    event_ordinal=0,
                )
                collect(
                    reducer.invalidate_transport(
                        position=position,
                        effective_at=quality["detected_at"],
                        known_at=quality["detected_at"],
                        raw_record_id=record.raw_record_id,
                        classification=str(quality["classification"]),
                        reason=str(quality["reason"]),
                        evidence=dict(quality.get("evidence") or {}),
                    )
                )
                state.book_last_position = position

            for event in raw_events:
                if event.event_kind not in {
                    "market_l2_snapshot",
                    "market_l2_update",
                }:
                    continue
                fact = translate_coinbase_l2_event(
                    event,
                    raw_record=record,
                    contract=contract,
                    accepted_at=datetime.now(UTC),
                )
                max_event_ordinal_by_receive[record.receive_ordinal] = max(
                    fact.position.event_ordinal,
                    max_event_ordinal_by_receive.get(record.receive_ordinal, -1),
                )
                prior_lifecycle = reducer.lifecycle
                result = reducer.process(fact)
                collect(result)
                state.book_last_position = fact.position
                if (
                    result.snapshot is not None
                    and prior_lifecycle is BookLifecycle.INVALID
                ):
                    book_quality.append(
                        BookQualityEvidence(
                            classification="resync_snapshot_accepted",
                            reason="fresh complete snapshot restored book validity",
                            position=fact.position,
                            known_at=fact.known_at,
                            raw_record_id=fact.raw_record_id,
                            invalidating=False,
                            evidence={
                                "state_hash": result.snapshot.state_hash,
                                "validity_interval_id": (
                                    result.snapshot.validity_interval_id
                                ),
                            },
                        )
                    )

        last_record = records[-1]
        terminal_position = BookSourcePosition(
            definition_id=claim.definition_id,
            session_id=claim.session_id,
            connection_epoch=connection_epoch,
            provider_product_id=claim.provider_product_id,
            provider_sequence_num=checkpoint.analysis.last_sequence_num,
            receive_ordinal=last_record.receive_ordinal,
            event_ordinal=(
                max_event_ordinal_by_receive.get(last_record.receive_ordinal, -1) + 1
            ),
        )
        for quality in checkpoint.external_quality:
            if not bool(quality.get("invalidating")):
                direct_quality.append(quality)
                continue
            collect(
                reducer.invalidate_transport(
                    position=terminal_position,
                    effective_at=quality["detected_at"],
                    known_at=quality["detected_at"],
                    raw_record_id=str(
                        quality.get("raw_record_id") or last_record.raw_record_id
                    ),
                    classification=str(quality["classification"]),
                    reason=str(quality["reason"]),
                    evidence=dict(quality.get("evidence") or {}),
                )
            )
            state.book_last_position = terminal_position

        if checkpoint.terminal and reducer.lifecycle is BookLifecycle.VALID:
            collect(
                reducer.close_valid_at(
                    position=terminal_position,
                    effective_at=last_record.received_at,
                    known_at=datetime.now(UTC),
                    reason=(
                        "continuous collector stopped at a proven transport boundary"
                    ),
                )
            )
            state.book_last_position = terminal_position

        final_position = state.book_last_position or terminal_position
        checkpoint_ids: list[str] = []
        for book_checkpoint in book_checkpoints:
            checkpoint_encoded, checkpoint_acknowledgement = publish_book_checkpoint(
                book_checkpoint,
                object_store=object_store,
                temporary_directory=temporary_root,
            )
            self.repository.commit_book_checkpoint(
                claim,
                checkpoint=book_checkpoint,
                encoded=checkpoint_encoded,
                acknowledgement=checkpoint_acknowledgement,
                source_manifest_ids=(commit.manifest_id,),
            )
            state.book_checkpoint_id = book_checkpoint.checkpoint_id
            checkpoint_ids.append(book_checkpoint.checkpoint_id)

        ingest = self.repository.ingest_book_facts(
            claim,
            snapshots=snapshots,
            batches=batches,
            validity_versions=validity_versions,
            lifecycle=reducer.lifecycle,
            final_validity_interval_id=(
                reducer.current_interval.interval_id
                if reducer.current_interval is not None
                else None
            ),
            checkpoint_id=state.book_checkpoint_id,
            final_state_hash=reducer.current_state_hash,
            final_connection_epoch=final_position.connection_epoch,
            final_receive_ordinal=final_position.receive_ordinal,
            final_event_ordinal=final_position.event_ordinal,
            final_sequence_num=final_position.provider_sequence_num,
        )

        bbo_facts = ()
        depth_facts = ()
        response_facts = ()
        feature_inserted = 0
        feature_noop = 0
        if valid_states:
            required = (
                "bbo_series_id",
                "depth_series_id",
                "base_currency",
                "quote_currency",
            )
            if not all(name in config for name in required):
                raise RuntimeError(
                    "market_book_feature_config_missing: re-run pair configuration"
                )
            valuation = MarketStateValuationContract(
                product_definition_version_id=str(
                    config.get("product_definition_version_id") or ""
                ),
                provider_size_unit=str(config.get("provider_size_unit") or ""),
                base_currency=str(config.get("base_currency") or ""),
                quote_currency=str(config.get("quote_currency") or ""),
                contract_size=config.get("contract_size"),
            )
            bbo_facts, depth_facts = derive_book_features(
                valid_states,
                contract=valuation,
                bbo_series_id=int(config["bbo_series_id"]),
                depth_series_id=int(config["depth_series_id"]),
                computed_at=datetime.now(UTC),
            )
            feature_outcome = self.repository.ingest_market_state_features(
                bbo_facts=bbo_facts,
                depth_facts=depth_facts,
            )
            feature_inserted += feature_outcome.inserted_count
            feature_noop += feature_outcome.noop_count
            response_config = (
                "trade_series_id",
                "flow_feature_series_ids",
                "response_feature_series_id",
            )
            if all(name in config for name in response_config):
                response_start = min(
                    row.effective_at for row in valid_states
                ) - timedelta(seconds=2)
                response_end = max(
                    row.effective_at for row in valid_states
                ) + timedelta(seconds=2)
                response_known_at = datetime.now(UTC)
                flow_rows = self.repository.read_trade_flow_features(
                    series_id=int(dict(config["flow_feature_series_ids"])["1"]),
                    start=response_start,
                    end=response_end,
                    known_at=response_known_at,
                )
                trade_rows = self.repository.read_trades(
                    series_id=int(config["trade_series_id"]),
                    start=response_start,
                    end=response_end,
                    known_at_lte=response_known_at,
                )
                response_facts = derive_response_features(
                    valid_states,
                    tuple(row.fact for row in trade_rows),
                    flow_rows,
                    contract=valuation,
                    series_id=int(config["response_feature_series_id"]),
                    computed_at=response_known_at,
                )
                response_outcome = self.repository.ingest_market_state_features(
                    response_facts=response_facts
                )
                feature_inserted += response_outcome.inserted_count
                feature_noop += response_outcome.noop_count

        closing_interval_by_quality_hash = {
            row.closing_quality_hash: row.interval_id
            for row in validity_versions
            if row.closing_quality_hash
        }
        for quality in book_quality:
            quality_id = self.repository.record_quality_event(
                claim,
                connection_epoch=quality.position.connection_epoch,
                receive_ordinal=quality.position.receive_ordinal,
                channel="level2",
                classification=quality.classification,
                reason=quality.reason,
                detected_at=quality.known_at,
                raw_record_id=quality.raw_record_id,
                sequence_after=quality.position.provider_sequence_num,
                evidence={
                    **dict(quality.evidence),
                    "book_quality_evidence_hash": quality.evidence_hash,
                    "event_ordinal": quality.position.event_ordinal,
                },
            )
            interval_id = closing_interval_by_quality_hash.get(
                quality.evidence_hash
            ) or interval_by_position.get(
                (
                    quality.position.receive_ordinal,
                    quality.position.event_ordinal,
                )
            )
            if interval_id:
                self.repository.link_book_quality_event(
                    claim,
                    quality_event_id=quality_id,
                    validity_interval_id=interval_id,
                    link_role=(
                        "invalidated" if quality.invalidating else "observed_within"
                    ),
                    known_at=quality.known_at,
                )
            counters["quality_events"] += 1
        for quality in direct_quality:
            dedupe_hash = str(quality.get("dedupe_hash") or "")
            if dedupe_hash and dedupe_hash in state.persisted_quality_hashes:
                continue
            self.repository.record_quality_event(
                claim,
                connection_epoch=int(quality["connection_epoch"]),
                receive_ordinal=int(quality["receive_ordinal"]),
                channel="level2",
                classification=str(quality["classification"]),
                reason=str(quality.get("reason") or "stream quality event"),
                detected_at=quality["detected_at"],
                raw_record_id=quality.get("raw_record_id"),
                sequence_before=quality.get("sequence_before"),
                sequence_after=quality.get("sequence_after"),
                evidence=quality.get("evidence"),
            )
            if dedupe_hash:
                state.persisted_quality_hashes.add(dedupe_hash)
            counters["quality_events"] += 1

        counters["book_snapshots"] += len(snapshots)
        counters["book_batches"] += len(batches)
        counters["book_mutations"] += sum(
            len(row.event.mutations) for row in batches
        )
        counters["book_checkpoints"] += len(checkpoint_ids)
        counters["book_features"] += (
            len(bbo_facts) + len(depth_facts) + len(response_facts)
        )
        event_writer.append(
            connection_epoch=connection_epoch,
            event_type="book_segment_canonicalized",
            occurred_at=datetime.now(UTC),
            evidence={
                "manifest_id": commit.manifest_id,
                "spool_segment_id": segment.spool_segment_id,
                "record_count": len(records),
                "snapshot_count": len(snapshots),
                "batch_count": len(batches),
                "mutation_count": sum(
                    len(row.event.mutations) for row in batches
                ),
                "checkpoint_ids": checkpoint_ids,
                "lifecycle": reducer.lifecycle.value,
                "state_hash": reducer.current_state_hash,
                "feature_inserted": feature_inserted,
                "feature_noop": feature_noop,
                "book_ingest_commit_seq": ingest.max_commit_seq,
                "terminal": checkpoint.terminal,
            },
        )
        segment.mark_database_acknowledged(
            manifest_id=commit.manifest_id,
            object_key=acknowledgement.object_key,
            object_sha256=acknowledgement.sha256,
        )
        segment.discard_acknowledged_spool()
        logger.info(
            "continuous_l2_segment_committed | definition_id=%s session_id=%s "
            "connection_epoch=%s segment_id=%s records=%s snapshots=%s "
            "batches=%s mutations=%s checkpoints=%s terminal=%s",
            claim.definition_id,
            claim.session_id,
            connection_epoch,
            segment.spool_segment_id,
            len(records),
            len(snapshots),
            len(batches),
            sum(len(row.event.mutations) for row in batches),
            len(checkpoint_ids),
            checkpoint.terminal,
        )
        if checkpoint.terminal:
            self._retire_epoch_state(
                states=states,
                connection_epoch=connection_epoch,
                claim=claim,
            )

    def _finalize_trade_segment(
        self,
        *,
        claim: StreamClaim,
        checkpoint: _SegmentCheckpoint,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
        event_writer: _SessionEventWriter,
        states: dict[int, _EpochProjectionState],
        counters: dict[str, int],
    ) -> None:
        segment = checkpoint.segment
        encoded, acknowledgement, records = publish_spool_archive(
            segment,
            object_store=object_store,
            temporary_directory=temporary_root,
        )
        commit = self.repository.commit_archive(
            claim,
            encoded=encoded,
            acknowledgement=acknowledgement,
            records=records,
        )
        counters["manifests"] += 1
        connection_epoch = records[0].connection_epoch
        state = states.setdefault(connection_epoch, _EpochProjectionState())
        analysis = checkpoint.analysis
        coverage_interval_id = _stable_hash(
            {
                "schema_version": "market.trade_coverage_interval_id.v1",
                "definition_id": claim.definition_id,
                "session_id": claim.session_id,
                "connection_epoch": connection_epoch,
                "product_id": claim.provider_product_id,
                "channel": "market_trades",
            }
        )

        quality_rows = tuple(analysis.quality_events) + tuple(
            checkpoint.external_quality
        )
        for quality in quality_rows:
            dedupe_hash = str(quality.get("dedupe_hash") or "")
            if dedupe_hash and dedupe_hash in state.persisted_quality_hashes:
                continue
            quality_id = self.repository.record_quality_event(
                claim,
                connection_epoch=connection_epoch,
                receive_ordinal=int(quality["receive_ordinal"]),
                channel=str(quality["channel"]),
                classification=str(quality["classification"]),
                reason=str(quality.get("reason") or "unspecified stream quality event"),
                detected_at=quality["detected_at"],
                raw_record_id=quality.get("raw_record_id"),
                coverage_interval_id=(
                    coverage_interval_id
                    if bool(quality.get("invalidating"))
                    else None
                ),
                sequence_before=quality.get("sequence_before"),
                sequence_after=quality.get("sequence_after"),
                evidence=quality.get("evidence"),
            )
            if dedupe_hash:
                state.persisted_quality_hashes.add(dedupe_hash)
            if bool(quality.get("invalidating")):
                state.invalidating_quality_ids.append(quality_id)
            counters["quality_events"] += 1

        coverage_opened = bool(
            analysis.subscription_acknowledged
            and analysis.heartbeat_healthy
            and analysis.snapshot_accepted
            and analysis.coverage_opening_ordinal is not None
            and analysis.coverage_opening_effective_at is not None
            and analysis.coverage_last_ordinal is not None
            and analysis.coverage_last_effective_at is not None
        )
        if coverage_opened and state.opening_session_event_id is None:
            opening_record = next(
                (
                    record
                    for record in records
                    if record.receive_ordinal == analysis.coverage_opening_ordinal
                ),
                None,
            )
            if opening_record is None:
                raise RuntimeError(
                    "continuous_collector_coverage_opening_missing: "
                    f"definition_id={claim.definition_id} "
                    f"session_id={claim.session_id} epoch={connection_epoch} "
                    f"ordinal={analysis.coverage_opening_ordinal}"
                )
            state.opening_session_event_id = event_writer.append(
                connection_epoch=connection_epoch,
                event_type="trade_coverage_opened",
                occurred_at=analysis.coverage_opening_effective_at,
                evidence={
                    "raw_record_id": opening_record.raw_record_id,
                    "receive_ordinal": opening_record.receive_ordinal,
                    "ordering_assurance": (
                        OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS.value
                    ),
                },
            )
            state.opening_raw_record_id = opening_record.raw_record_id
            state.opening_receive_ordinal = opening_record.receive_ordinal
            state.opening_effective_at = analysis.coverage_opening_effective_at

        parser = CoinbaseMessageParser(
            symbol_by_product_id={
                claim.provider_product_id: claim.provider_product_id
            }
        )
        translated = []
        for record in records:
            events = parser.parse_raw(
                record.raw_frame,
                received_at=record.received_at.isoformat(),
                raw_ref={
                    "raw_record_id": record.raw_record_id,
                    "spool_segment_id": record.spool_segment_id,
                    "connection_epoch": record.connection_epoch,
                    "receive_ordinal": record.receive_ordinal,
                },
            )
            for event in events:
                if event.event_kind != "market_trade":
                    continue
                delivery_kind = str(event.payload.get("type") or "").lower()
                coverage_ref = (
                    coverage_interval_id
                    if state.opening_receive_ordinal is not None
                    and delivery_kind == "update"
                    and record.receive_ordinal >= state.opening_receive_ordinal
                    else None
                )
                translated.append(
                    translate_coinbase_market_trade(
                        event,
                        contract=self.repository.get_product_contract(
                            str(claim.config["product_definition_version_id"])
                        ),
                        raw_record_id=record.raw_record_id,
                        connection_epoch=record.connection_epoch,
                        receive_ordinal=record.receive_ordinal,
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
                connection_epoch=connection_epoch,
                receive_ordinal=(
                    conflict.receive_ordinal
                    if conflict is not None
                    else records[-1].receive_ordinal
                ),
                channel="market_trades",
                classification="provider_trade_conflict",
                reason=str(exc),
                detected_at=datetime.now(UTC),
                raw_record_id=(
                    conflict.raw_record_id
                    if conflict is not None
                    else records[-1].raw_record_id
                ),
                coverage_interval_id=coverage_interval_id,
            )
            raise
        counters["trade_inserted"] += trade_outcome.inserted_count
        counters["trade_noop"] += trade_outcome.noop_count

        coverage: TradeCoverageIntervalVersion | None = None
        if coverage_opened and state.opening_session_event_id is not None:
            last_record = next(
                (
                    record
                    for record in reversed(records)
                    if record.receive_ordinal == analysis.coverage_last_ordinal
                ),
                None,
            )
            if last_record is None:
                raise RuntimeError(
                    "continuous_collector_coverage_watermark_missing: "
                    f"definition_id={claim.definition_id} "
                    f"session_id={claim.session_id} epoch={connection_epoch} "
                    f"ordinal={analysis.coverage_last_ordinal}"
                )
            state.coverage_revision += 1
            status = (
                CoverageStatus.INVALID
                if state.invalidating_quality_ids
                else (
                    CoverageStatus.CLOSED_VALID
                    if checkpoint.terminal
                    else CoverageStatus.OPEN_VALID
                )
            )
            closing = checkpoint.terminal
            coverage = TradeCoverageIntervalVersion(
                interval_id=coverage_interval_id,
                revision=state.coverage_revision,
                definition_id=claim.definition_id,
                session_id=claim.session_id,
                connection_epoch=connection_epoch,
                provider_product_id=claim.provider_product_id,
                channel="market_trades",
                status=status,
                ordering_assurance=OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS,
                archive_status=ArchiveStatus.COMPLETE,
                opening_raw_record_id=str(state.opening_raw_record_id),
                opening_receive_ordinal=int(state.opening_receive_ordinal or 0),
                opening_effective_at=state.opening_effective_at,
                last_raw_record_id=last_record.raw_record_id,
                last_receive_ordinal=last_record.receive_ordinal,
                last_effective_at=analysis.coverage_last_effective_at,
                closing_raw_record_id=last_record.raw_record_id if closing else None,
                closing_receive_ordinal=(
                    last_record.receive_ordinal if closing else None
                ),
                closing_effective_at=(
                    analysis.coverage_last_effective_at if closing else None
                ),
                canonicalization_watermark_ordinal=last_record.receive_ordinal,
                archive_complete_through_ordinal=last_record.receive_ordinal,
                known_at=datetime.now(UTC),
                first_provider_sequence_num=analysis.coverage_first_sequence_num,
                last_provider_sequence_num=analysis.coverage_last_sequence_num,
                gap_quality_event_ids=tuple(state.invalidating_quality_ids),
                opening_evidence={
                    "subscription_acknowledged": True,
                    "heartbeat_healthy": True,
                    "snapshot_accepted": True,
                },
                closing_evidence=(
                    {
                        "reason": checkpoint.terminal_reason,
                        "all_raw_records_mapped": True,
                    }
                    if closing
                    else {}
                ),
            )
            self.repository.append_coverage_version(
                claim,
                coverage=coverage,
                opening_session_event_id=state.opening_session_event_id,
                closing_session_event_id=(
                    checkpoint.closing_session_event_id if closing else None
                ),
            )
            if coverage.status is not CoverageStatus.INVALID:
                self._materialize_completed_buckets(
                    claim=claim,
                    coverage=coverage,
                    state=state,
                )

        event_writer.append(
            connection_epoch=connection_epoch,
            event_type="segment_canonicalized",
            occurred_at=datetime.now(UTC),
            evidence={
                "manifest_id": commit.manifest_id,
                "spool_segment_id": segment.spool_segment_id,
                "record_count": len(records),
                "trade_requested": trade_outcome.requested_count,
                "trade_inserted": trade_outcome.inserted_count,
                "trade_noop": trade_outcome.noop_count,
                "coverage_revision": (
                    coverage.revision if coverage is not None else None
                ),
                "terminal": checkpoint.terminal,
            },
        )
        segment.mark_database_acknowledged(
            manifest_id=commit.manifest_id,
            object_key=acknowledgement.object_key,
            object_sha256=acknowledgement.sha256,
        )
        segment.discard_acknowledged_spool()
        logger.info(
            "continuous_collector_segment_committed | definition_id=%s "
            "session_id=%s connection_epoch=%s segment_id=%s records=%s "
            "manifest_id=%s trade_inserted=%s trade_noop=%s terminal=%s",
            claim.definition_id,
            claim.session_id,
            connection_epoch,
            segment.spool_segment_id,
            len(records),
            commit.manifest_id,
            trade_outcome.inserted_count,
            trade_outcome.noop_count,
            checkpoint.terminal,
        )
        if checkpoint.terminal:
            self._retire_epoch_state(
                states=states,
                connection_epoch=connection_epoch,
                claim=claim,
            )

    @staticmethod
    def _retire_epoch_state(
        *,
        states: dict[int, _EpochProjectionState],
        connection_epoch: int,
        claim: StreamClaim,
    ) -> None:
        retired = states.pop(connection_epoch, None)
        if retired is None:
            raise RuntimeError(
                "continuous_collector_epoch_retirement_invalid: "
                f"definition_id={claim.definition_id} "
                f"session_id={claim.session_id} epoch={connection_epoch}"
            )
        logger.debug(
            "continuous_collector_epoch_state_retired | definition_id=%s "
            "session_id=%s connection_epoch=%s remaining_epochs=%s",
            claim.definition_id,
            claim.session_id,
            connection_epoch,
            len(states),
        )

    def _materialize_completed_buckets(
        self,
        *,
        claim: StreamClaim,
        coverage: TradeCoverageIntervalVersion,
        state: _EpochProjectionState,
    ) -> None:
        aggregate_series_ids = {
            int(key): int(value)
            for key, value in dict(
                claim.config.get("aggregate_series_ids") or {}
            ).items()
        }
        flow_feature_series_ids = {
            int(key): int(value)
            for key, value in dict(
                claim.config.get("flow_feature_series_ids") or {}
            ).items()
        }
        for interval in (1, 60):
            if interval not in aggregate_series_ids:
                raise RuntimeError(
                    "continuous_collector_aggregate_config_missing: "
                    f"definition_id={claim.definition_id} interval={interval}"
                )
            if interval not in flow_feature_series_ids:
                raise RuntimeError(
                    "continuous_collector_flow_config_missing: "
                    f"definition_id={claim.definition_id} interval={interval}"
                )
            cursor = state.next_bucket_start.get(interval)
            if cursor is None:
                cursor = bucket_start_for(
                    coverage.opening_effective_at,
                    interval_seconds=interval,
                )
                if cursor < coverage.opening_effective_at:
                    cursor += timedelta(seconds=interval)
            bucket_starts: list[datetime] = []
            while (
                cursor + timedelta(seconds=interval)
                <= coverage.last_effective_at
            ):
                bucket_starts.append(cursor)
                cursor += timedelta(seconds=interval)
            state.next_bucket_start[interval] = cursor
            if not bucket_starts:
                continue
            range_end = bucket_starts[-1] + timedelta(seconds=interval)
            records = self.repository.read_trades(
                series_id=claim.series_id,
                start=bucket_starts[0],
                end=range_end,
                known_at_lte=coverage.known_at,
            )
            eligible_trades = [
                record.fact
                for record in records
                if record.fact.coverage_interval_id == coverage.interval_id
            ]
            by_bucket: dict[datetime, list[Any]] = {
                bucket_start: [] for bucket_start in bucket_starts
            }
            for trade in eligible_trades:
                trade_bucket = bucket_start_for(
                    trade.provider_event_time,
                    interval_seconds=interval,
                )
                if trade_bucket in by_bucket:
                    by_bucket[trade_bucket].append(trade)
            aggregates = [
                aggregate_trade_bucket(
                    by_bucket[bucket_start],
                    interval_seconds=interval,
                    bucket_start=bucket_start,
                    coverage=coverage,
                    computed_at=max(
                        datetime.now(UTC),
                        bucket_start + timedelta(seconds=interval),
                    ),
                )
                for bucket_start in bucket_starts
            ]
            aggregate_outcome = self.repository.ingest_aggregates(
                series_id=aggregate_series_ids[interval],
                facts=aggregates,
            )
            features = []
            for aggregate in aggregates:
                feature = derive_trade_flow_feature(
                    series_id=flow_feature_series_ids[interval],
                    source_trade_flow_series_id=aggregate_series_ids[interval],
                    aggregate=aggregate,
                    trades=by_bucket[aggregate.bucket_start],
                    computed_at=max(datetime.now(UTC), aggregate.bucket_end),
                )
                if feature is not None:
                    features.append(feature)
            feature_outcome = self.repository.ingest_market_state_features(
                flow_facts=features
            )
            inserted_buckets = aggregate_outcome.inserted_count
            inserted_features = feature_outcome.inserted_count
            if inserted_buckets or inserted_features:
                logger.debug(
                    "continuous_collector_buckets_materialized | "
                    "definition_id=%s session_id=%s connection_epoch=%s "
                    "interval_seconds=%s aggregate_inserted=%s "
                    "feature_inserted=%s next_bucket_start=%s",
                    claim.definition_id,
                    claim.session_id,
                    coverage.connection_epoch,
                    interval,
                    inserted_buckets,
                    inserted_features,
                    cursor.isoformat(),
                )


# Compatibility names retained for existing callers. New registrations should
# use the provider-qualified adapter classes and provider-neutral runtime module.
ContinuousMarketStructureCollector = ContinuousStreamRuntime
MarketTradeProjectionAdapter = CoinbaseMarketTradeProjectionAdapter
Level2BookProjectionAdapter = CoinbaseLevel2BookProjectionAdapter
continuous_stream_runtime = ContinuousStreamRuntime()
continuous_market_structure_collector = continuous_stream_runtime


__all__ = [
    "CoinbaseContinuousTransportAdapter",
    "CoinbaseLevel2BookProjectionAdapter",
    "CoinbaseMarketTradeProjectionAdapter",
    "ContinuousCaptureAnalyzer",
    "ContinuousProjectionAdapter",
    "ContinuousTransportAdapter",
    "ContinuousMarketStructureCollector",
    "ContinuousStreamRuntime",
    "Level2BookProjectionAdapter",
    "MarketTradeProjectionAdapter",
    "continuous_market_structure_collector",
    "continuous_stream_runtime",
]
