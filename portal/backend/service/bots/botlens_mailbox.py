"""Per-run and per-symbol intake structures for the BotLens telemetry pipeline.

Ownership:
  IntakeRouter writes to these structures.
  Projectors read from them.
  No projection logic lives here.

Key semantics:
  BootstrapSlot — last-writer-wins. Only the latest pending bootstrap per symbol
    is retained. Assigning a new bootstrap discards any previous pending one.
  SymbolMailbox — ordered fact queue + bootstrap slot for one (run_id, symbol_key).
  RunMailbox — lifecycle channel + per-symbol mailbox registry for one run_id.
  Fanout item types — typed messages from projectors destined for viewer delivery.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ..observability import BackendObserver
from .botlens_state import ProjectionBatch, RunConcernDelta, RunProjectionSnapshot, SymbolConcernDelta

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_mailbox", event_logger=logger)

_LIFECYCLE_CHANNEL_MAX = 64
_RUN_NOTIFICATION_QUEUE_MAX = 512
_FACT_QUEUE_MAX = 512
_FANOUT_CHANNEL_MAX = 2048


# ---------------------------------------------------------------------------
# Bootstrap slot — last-writer-wins snapshot holder
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueueEnvelope:
    payload: Any
    enqueued_monotonic: float = field(default_factory=time.monotonic)


class BootstrapSlot:
    """
    Holds at most one pending bootstrap payload per (run_id, symbol_key).

    Semantics:
    - put() replaces any existing pending payload (last-writer-wins).
    - take() atomically reads and clears the slot.
    - event fires whenever a payload is present; cleared on take().
    """

    __slots__ = ("run_id", "bot_id", "symbol_key", "_payload", "_event", "_superseded_count", "_pending_at")

    def __init__(self, *, run_id: str, bot_id: str, symbol_key: str) -> None:
        self.run_id = str(run_id)
        self.bot_id = str(bot_id)
        self.symbol_key = str(symbol_key)
        self._payload: Optional[ProjectionBatch] = None
        self._event: asyncio.Event = asyncio.Event()
        self._superseded_count: int = 0
        self._pending_at: Optional[float] = None

    def put(self, payload: ProjectionBatch) -> None:
        """Replace any pending bootstrap with payload. Signals waiting projector."""
        labels = {
            "bot_id": self.bot_id,
            "run_id": self.run_id,
            "series_key": self.symbol_key,
            "message_kind": "bootstrap",
        }
        _OBSERVER.increment("bootstrap_received_total", **labels)
        if self._payload is not None:
            self._superseded_count += 1
            _OBSERVER.increment("bootstrap_superseded_total", **labels)
            _OBSERVER.event(
                "bootstrap_superseded",
                level=logging.WARN,
                log_to_logger=False,
                superseded_count=self._superseded_count,
                **labels,
            )
        self._payload = payload
        self._pending_at = time.monotonic()
        self._event.set()
        self._emit_pending_gauges(age_ms=0.0)

    def take(self) -> Optional[ProjectionBatch]:
        """Return and clear the pending bootstrap (None if empty)."""
        pending_age_ms = self.pending_age_ms
        payload = self._payload
        self._payload = None
        self._pending_at = None
        self._event.clear()
        self._emit_pending_gauges(age_ms=pending_age_ms)
        return payload

    @property
    def pending(self) -> bool:
        return self._payload is not None

    @property
    def event(self) -> asyncio.Event:
        """asyncio.Event that is set while a bootstrap is pending."""
        return self._event

    @property
    def superseded_count(self) -> int:
        return self._superseded_count

    @property
    def pending_age_ms(self) -> float:
        if self._pending_at is None:
            return 0.0
        return max((time.monotonic() - self._pending_at) * 1000.0, 0.0)

    def _emit_pending_gauges(self, *, age_ms: float) -> None:
        _OBSERVER.gauge(
            "bootstrap_pending_count",
            1.0 if self._payload is not None else 0.0,
            bot_id=self.bot_id,
            run_id=self.run_id,
            series_key=self.symbol_key,
        )
        _OBSERVER.gauge(
            "bootstrap_pending_age_ms",
            max(float(age_ms), 0.0),
            bot_id=self.bot_id,
            run_id=self.run_id,
            series_key=self.symbol_key,
        )


# ---------------------------------------------------------------------------
# Per-symbol intake lane
# ---------------------------------------------------------------------------

class SymbolMailbox:
    """
    Intake lane for one (run_id, symbol_key).

    Holds:
    - fact_queue: ordered bounded queue of incremental fact-batch payloads.
    - bootstrap_slot: last-writer-wins replacement snapshot slot.
    """

    __slots__ = ("run_id", "bot_id", "symbol_key", "event_queue", "fact_queue", "bootstrap_slot", "_created_at")

    def __init__(self, run_id: str, bot_id: str, symbol_key: str) -> None:
        self.run_id = run_id
        self.bot_id = bot_id
        self.symbol_key = symbol_key
        self.event_queue: asyncio.Queue[QueueEnvelope] = asyncio.Queue(maxsize=_FACT_QUEUE_MAX)
        self.fact_queue = self.event_queue
        self.bootstrap_slot: BootstrapSlot = BootstrapSlot(
            run_id=run_id,
            bot_id=bot_id,
            symbol_key=symbol_key,
        )
        self._created_at: float = time.monotonic()

    def enqueue_batch(self, payload: ProjectionBatch) -> bool:
        """Enqueue an incremental domain-event batch. Returns False if the queue is full."""
        labels = {
            "bot_id": self.bot_id,
            "run_id": self.run_id,
            "series_key": self.symbol_key,
            "queue_name": "symbol_fact_queue",
            "message_kind": "facts",
        }
        try:
            self.event_queue.put_nowait(QueueEnvelope(payload=payload))
            _OBSERVER.increment("symbol_fact_enqueued_total", **labels)
            self._emit_fact_gauges()
            return True
        except asyncio.QueueFull:
            _OBSERVER.increment("symbol_fact_dropped_total", **labels)
            _OBSERVER.event(
                "symbol_fact_queue_overflow",
                level=logging.WARN,
                log_to_logger=False,
                depth=self.event_queue.qsize(),
                overflow_policy="drop_new",
                **labels,
            )
            self._emit_fact_gauges()
            return False

    def set_bootstrap(self, payload: ProjectionBatch) -> None:
        """Install a replacement bootstrap (last-writer-wins)."""
        self.bootstrap_slot.put(payload)

    def oldest_fact_age_ms(self) -> float:
        if self.event_queue.qsize() <= 0:
            return 0.0
        try:
            envelope = self.event_queue._queue[0]
        except Exception:
            return 0.0
        if not isinstance(envelope, QueueEnvelope):
            return 0.0
        return max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)

    def _emit_fact_gauges(self) -> None:
        _OBSERVER.maybe_emit_gauges(
            f"symbol_fact_queue:{self.run_id}:{self.symbol_key}",
            depth_metric="symbol_fact_queue_depth",
            utilization_metric="symbol_fact_queue_utilization",
            oldest_age_metric="symbol_fact_queue_oldest_age_ms",
            depth=self.event_queue.qsize(),
            capacity=_FACT_QUEUE_MAX,
            oldest_age_ms=self.oldest_fact_age_ms(),
            bot_id=self.bot_id,
            run_id=self.run_id,
            series_key=self.symbol_key,
            queue_name="symbol_fact_queue",
        )


# ---------------------------------------------------------------------------
# Per-run intake structure
# ---------------------------------------------------------------------------

class RunMailbox:
    """
    Per-run intake structure.

    Holds:
    - lifecycle_queue: bounded queue of run-scoped lifecycle batches.
    - notification_queue: bounded queue of symbol-summary notifications for run projection.
    - per-symbol SymbolMailbox instances, created on first message.
    """

    def __init__(self, run_id: str, bot_id: str) -> None:
        self.run_id = run_id
        self.bot_id = bot_id
        self.lifecycle_queue: asyncio.Queue[QueueEnvelope] = asyncio.Queue(
            maxsize=_LIFECYCLE_CHANNEL_MAX
        )
        # Backward-compatible alias used by older callers/tests.
        self.event_channel = self.lifecycle_queue
        self.notification_queue: asyncio.Queue[QueueEnvelope] = asyncio.Queue(
            maxsize=_RUN_NOTIFICATION_QUEUE_MAX
        )
        self._symbol_mailboxes: Dict[str, SymbolMailbox] = {}

    def get_or_create_symbol_mailbox(self, symbol_key: str) -> SymbolMailbox:
        if symbol_key not in self._symbol_mailboxes:
            self._symbol_mailboxes[symbol_key] = SymbolMailbox(
                run_id=self.run_id, bot_id=self.bot_id, symbol_key=symbol_key
            )
        return self._symbol_mailboxes[symbol_key]

    def symbol_mailbox(self, symbol_key: str) -> Optional[SymbolMailbox]:
        return self._symbol_mailboxes.get(symbol_key)

    def known_symbols(self) -> List[str]:
        return list(self._symbol_mailboxes)

    def enqueue_lifecycle(self, payload: ProjectionBatch) -> None:
        """Enqueue a run lifecycle batch. Fails loud if the queue is full."""
        labels = {
            "bot_id": self.bot_id,
            "run_id": self.run_id,
            "queue_name": "run_lifecycle_queue",
            "message_kind": "lifecycle",
        }
        envelope = QueueEnvelope(payload=payload)
        try:
            self.lifecycle_queue.put_nowait(envelope)
            _OBSERVER.increment("run_lifecycle_enqueued_total", **labels)
            self._emit_lifecycle_gauges()
        except asyncio.QueueFull:
            _OBSERVER.increment("run_lifecycle_rejected_total", **labels)
            _OBSERVER.event(
                "run_lifecycle_queue_overflow_failed_loud",
                level=logging.ERROR,
                log_to_logger=False,
                depth=self.lifecycle_queue.qsize(),
                failure_mode="queue_full",
                overflow_policy="reject_new",
                **labels,
            )
            self._emit_lifecycle_gauges()
            raise RuntimeError(
                f"run lifecycle queue overflow for run_id={self.run_id} bot_id={self.bot_id}"
            )

    def enqueue_notification(self, payload: Any) -> None:
        """Enqueue a run notification. Drops oldest if the queue is full."""
        labels = {
            "bot_id": self.bot_id,
            "run_id": self.run_id,
            "queue_name": "run_notification_queue",
            "message_kind": "symbol_summary",
        }
        envelope = QueueEnvelope(payload=payload)
        try:
            self.notification_queue.put_nowait(envelope)
            _OBSERVER.increment("run_notification_enqueued_total", **labels)
            self._emit_notification_gauges()
        except asyncio.QueueFull:
            try:
                self.notification_queue.get_nowait()
                self.notification_queue.put_nowait(envelope)
                _OBSERVER.increment("run_notification_enqueued_total", **labels)
                _OBSERVER.increment("run_notification_dropped_total", **labels)
                _OBSERVER.event(
                    "run_notification_queue_overflow",
                    level=logging.WARN,
                    log_to_logger=False,
                    depth=self.notification_queue.qsize(),
                    overflow_policy="drop_oldest",
                    **labels,
                )
            except (asyncio.QueueFull, asyncio.QueueEmpty):
                _OBSERVER.increment("run_notification_dropped_total", **labels)
                _OBSERVER.event(
                    "run_notification_queue_overflow",
                    level=logging.WARN,
                    log_to_logger=False,
                    depth=self.notification_queue.qsize(),
                    failure_mode="queue_full",
                    overflow_policy="drop_oldest",
                    **labels,
                )
            self._emit_notification_gauges()

    # Backward-compatible alias while callers are migrated.
    def enqueue_batch(self, payload: ProjectionBatch) -> None:
        self.enqueue_lifecycle(payload)

    def enqueue_facts(self, symbol_key: str, payload: ProjectionBatch) -> bool:
        mailbox = self.get_or_create_symbol_mailbox(symbol_key)
        return mailbox.enqueue_batch(payload)

    def set_bootstrap(self, symbol_key: str, payload: ProjectionBatch) -> None:
        mailbox = self.get_or_create_symbol_mailbox(symbol_key)
        mailbox.set_bootstrap(payload)

    def oldest_lifecycle_age_ms(self) -> float:
        if self.lifecycle_queue.qsize() <= 0:
            return 0.0
        try:
            envelope = self.lifecycle_queue._queue[0]
        except Exception:
            return 0.0
        if not isinstance(envelope, QueueEnvelope):
            return 0.0
        return max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)

    def oldest_notification_age_ms(self) -> float:
        if self.notification_queue.qsize() <= 0:
            return 0.0
        try:
            envelope = self.notification_queue._queue[0]
        except Exception:
            return 0.0
        if not isinstance(envelope, QueueEnvelope):
            return 0.0
        return max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)

    def _emit_lifecycle_gauges(self) -> None:
        _OBSERVER.maybe_emit_gauges(
            f"run_lifecycle_queue:{self.run_id}",
            depth_metric="run_lifecycle_queue_depth",
            utilization_metric="run_lifecycle_queue_utilization",
            oldest_age_metric="run_lifecycle_queue_oldest_age_ms",
            depth=self.lifecycle_queue.qsize(),
            capacity=_LIFECYCLE_CHANNEL_MAX,
            oldest_age_ms=self.oldest_lifecycle_age_ms(),
            bot_id=self.bot_id,
            run_id=self.run_id,
            queue_name="run_lifecycle_queue",
        )

    def _emit_notification_gauges(self) -> None:
        _OBSERVER.maybe_emit_gauges(
            f"run_notification_queue:{self.run_id}",
            depth_metric="run_notification_queue_depth",
            utilization_metric="run_notification_queue_utilization",
            oldest_age_metric="run_notification_queue_oldest_age_ms",
            depth=self.notification_queue.qsize(),
            capacity=_RUN_NOTIFICATION_QUEUE_MAX,
            oldest_age_ms=self.oldest_notification_age_ms(),
            bot_id=self.bot_id,
            run_id=self.run_id,
            queue_name="run_notification_queue",
        )


# ---------------------------------------------------------------------------
# Fanout item types — typed messages from projectors to viewer delivery
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FanoutSymbolDeltaBatch:
    """Symbol-level internal concern deltas from SymbolProjector."""
    run_id: str
    deltas: Tuple[SymbolConcernDelta, ...]


@dataclass(frozen=True)
class FanoutRunDeltaBatch:
    """Run-level internal concern deltas from RunProjector."""
    run_id: str
    state: RunProjectionSnapshot
    deltas: Tuple[RunConcernDelta, ...]


@dataclass(frozen=True)
class FanoutEnvelope:
    run_id: str
    item: Any
    message_kind: str
    payload_bytes: int
    enqueued_monotonic: float = field(default_factory=time.monotonic)


# Sentinel that signals the fanout delivery loop to shut down.
_FANOUT_STOP = object()


__all__ = [
    "BootstrapSlot",
    "FanoutEnvelope",
    "FanoutRunDeltaBatch",
    "FanoutSymbolDeltaBatch",
    "QueueEnvelope",
    "RunMailbox",
    "SymbolMailbox",
    "_FANOUT_STOP",
    "_FANOUT_CHANNEL_MAX",
    "_FACT_QUEUE_MAX",
    "_LIFECYCLE_CHANNEL_MAX",
    "_RUN_NOTIFICATION_QUEUE_MAX",
]
