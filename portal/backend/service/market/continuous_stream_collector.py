"""Supervised, bounded-memory market-structure stream collection.

The bounded proof command intentionally keeps its short diagnostic lifecycle.
This module owns the long-lived path: one fenced session, rotating durable
segments, asynchronous canonicalization, reconnect epochs, and an explicit
stop predicate supplied by a generic worker supervisor.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

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
    require_spool_capacity,
    spool_backlog_bytes,
)
from market_data.market_state import derive_trade_flow_feature
from market_data.structure import (
    ArchiveStatus,
    CoverageStatus,
    OrderingAssurance,
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
    CaptureAnalysis,
    _CaptureAnalyzer,
    _observed_channel,
    _stable_hash,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SegmentCheckpoint:
    segment: DurableRawSpoolSegment
    analysis: CaptureAnalysis
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


class ContinuousMarketStructureCollector:
    """Long-lived adapter for market-structure stream definitions."""

    def __init__(
        self,
        *,
        repository: PostgresMarketStructureRepository = market_structure_repository,
        stream_factory: Callable[..., CoinbaseAdvancedTradeStream] = CoinbaseAdvancedTradeStream,
    ) -> None:
        self.repository = repository
        self.stream_factory = stream_factory

    async def run(
        self,
        *,
        definition_id: str,
        owner_id: str,
        stop_requested: Callable[[], bool],
        bounded_validation: bool,
        storage_root: Path = DEFAULT_STORAGE_ROOT,
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
        if tuple(definition["channels"]) != ("market_trades", "heartbeats"):
            raise ValueError(
                "continuous_collector_adapter_unavailable: "
                f"definition_id={definition_id} channels={definition['channels']}"
            )
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
        finalizer_states: dict[int, _EpochProjectionState] = {}
        counters: dict[str, int] = {
            "raw_records": 0,
            "raw_bytes": 0,
            "segments": 0,
            "manifests": 0,
            "trade_inserted": 0,
            "trade_noop": 0,
            "reconnects": 0,
            "quality_events": 0,
        }
        finalizer = asyncio.create_task(
            self._finalizer_loop(
                claim=claim,
                queue=queue,
                object_store=object_store,
                temporary_root=temporary_root,
                event_writer=event_writer,
                states=finalizer_states,
                counters=counters,
            )
        )
        started_at = datetime.now(UTC)
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
                analyzer = _CaptureAnalyzer(claim, primary_channel="market_trades")
                parser = CoinbaseMessageParser(
                    symbol_by_product_id={
                        claim.provider_product_id: claim.provider_product_id
                    }
                )
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
                    await stream.subscribe(
                        [
                            MarketSubscription.from_values(
                                provider=claim.provider,
                                venue=claim.venue,
                                symbol=claim.provider_product_id,
                                product_id=claim.provider_product_id,
                                channels=claim.channels,
                                auth_mode=claim.auth_mode,
                            )
                        ]
                    )
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
                            requested_channel="market_trades",
                            observed_channel=_observed_channel(message.raw_frame),
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
                                "channel": analyzer.last_record.observed_channel,
                                "classification": "disconnect",
                                "reason": (
                                    disconnect_reason or "provider disconnected"
                                ),
                                "detected_at": datetime.now(UTC),
                                "raw_record_id": analyzer.last_record.raw_record_id,
                                "sequence_before": analyzer.last_sequence,
                                "sequence_after": None,
                                "invalidating": False,
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
            await queue.join()
            queue.put_nowait(None)
            await finalizer
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
                await asyncio.shield(self._drain_finalizer(queue, finalizer))
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
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self.repository.release, claim)

    async def _recover_orphaned_spools(
        self,
        *,
        definition: Mapping[str, Any],
        owner_id: str,
        lease_seconds: float,
        spool_root: Path,
        object_store: FilesystemRawArchiveObjectStore,
        temporary_root: Path,
    ) -> None:
        await asyncio.to_thread(
            self._recover_orphaned_spools_sync,
            definition=definition,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            spool_root=spool_root,
            object_store=object_store,
            temporary_root=temporary_root,
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
    ) -> None:
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
                recovered_records = 0
                recovered_segments = 0
                for path, probe in sorted(
                    entries,
                    key=lambda item: (
                        item[1].connection_epoch,
                        item[1].segment_ordinal,
                        str(item[0]),
                    ),
                ):
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
                    record_count = self._recover_trade_segment(
                        claim=claim,
                        segment=segment,
                        object_store=object_store,
                        temporary_root=temporary_root,
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
                closed_intervals = self.repository.close_open_session_coverages(
                    claim,
                    closing_session_event_id=recovery_event_id,
                    reason="collector_restart_closed_at_last_proven_event",
                )
                event_writer.append(
                    connection_epoch=max_epoch,
                    event_type="collector_restart_recovery_completed",
                    occurred_at=datetime.now(UTC),
                    evidence={
                        "recovered_segments": recovered_segments,
                        "recovered_records": recovered_records,
                        "closed_open_coverage_intervals": closed_intervals,
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

    @staticmethod
    async def _drain_finalizer(
        queue: asyncio.Queue[_SegmentCheckpoint | None],
        finalizer: asyncio.Task,
    ) -> None:
        """Wait for active projection work before the fencing lease is released."""

        if finalizer.done():
            await finalizer
            return
        joined = asyncio.create_task(queue.join())
        done, _pending = await asyncio.wait(
            {joined, finalizer}, return_when=asyncio.FIRST_COMPLETED
        )
        if finalizer in done:
            joined.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await joined
            await finalizer
            return
        await joined
        queue.put_nowait(None)
        await finalizer

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
        analysis: CaptureAnalysis,
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
        states: dict[int, _EpochProjectionState],
        counters: dict[str, int],
    ) -> None:
        while True:
            checkpoint = await queue.get()
            try:
                if checkpoint is None:
                    return
                await asyncio.to_thread(
                    self._finalize_trade_segment,
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
        segment.mark_database_acknowledged(
            manifest_id=commit.manifest_id,
            object_key=acknowledgement.object_key,
            object_sha256=acknowledgement.sha256,
        )
        segment.discard_acknowledged_spool()
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


continuous_market_structure_collector = ContinuousMarketStructureCollector()


__all__ = [
    "ContinuousMarketStructureCollector",
    "continuous_market_structure_collector",
]
