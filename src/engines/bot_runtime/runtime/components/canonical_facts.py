"""Producer-side canonical BotLens fact append and post-append dispatch."""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple

logger = logging.getLogger(__name__)
CANONICAL_FACT_DEBUG_LOG_EVERY = 250


def _should_sample_debug_log(count: int, *, every: int = CANONICAL_FACT_DEBUG_LOG_EVERY) -> bool:
    resolved_every = max(int(every), 1)
    resolved_count = max(int(count), 0)
    return resolved_count == 1 or resolved_count % resolved_every == 0

_CANONICAL_SIMPLE_FACT_TYPES = frozenset(
    {
        "candle_continuity_summary",
        "trade_opened",
        "trade_updated",
        "trade_closed",
        "wallet_ledger_event",
    }
)
_CANONICAL_DECISION_EVENT_NAMES = frozenset(
    {
        "SIGNAL_EMITTED",
        "DECISION_ACCEPTED",
        "DECISION_REJECTED",
        "ENTRY_FILLED",
        "EXIT_FILLED",
        "RUNTIME_ERROR",
    }
)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def canonical_fact_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    root = dict(payload or {})
    facts = []
    for entry in root.get("facts") if isinstance(root.get("facts"), list) else []:
        if not isinstance(entry, Mapping):
            continue
        fact_type = str(entry.get("fact_type") or "").strip().lower()
        if fact_type in _CANONICAL_SIMPLE_FACT_TYPES:
            facts.append(dict(entry))
            continue
        if fact_type != "decision_emitted":
            continue
        decision = _mapping(entry.get("decision"))
        event_name = str(decision.get("event_name") or "").strip().upper()
        if event_name in _CANONICAL_DECISION_EVENT_NAMES:
            facts.append(dict(entry))
    root["facts"] = facts
    return root


def live_fact_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    root = dict(payload or {})
    raw_facts = root.get("facts") if isinstance(root.get("facts"), list) else []
    root["facts"] = [dict(entry) for entry in raw_facts if isinstance(entry, Mapping)]
    return root


def has_canonical_facts(payload: Mapping[str, Any]) -> bool:
    return bool(canonical_fact_payload(payload).get("facts"))


def has_live_facts(payload: Mapping[str, Any]) -> bool:
    return bool(live_fact_payload(payload).get("facts"))


@dataclass(frozen=True)
class CommittedCanonicalFactBatch:
    batch_kind: str
    bot_id: str
    run_id: str
    seq: int
    symbol_key: Optional[str]
    known_at: Any
    event_time: Any
    canonical_payload: Dict[str, Any]
    live_payload: Dict[str, Any]
    append_result: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PostAppendConsumerResult:
    consumer_name: str
    result: Any = None
    error: Optional[str] = None


@dataclass(frozen=True)
class CanonicalFactAppendOutcome:
    batch: CommittedCanonicalFactBatch
    consumer_results: Tuple[PostAppendConsumerResult, ...] = ()


class CanonicalFactConsumer(Protocol):
    def consume(self, batch: CommittedCanonicalFactBatch) -> Any:
        ...


class LiveFactsBroadcastConsumer:
    def __init__(self, broadcast: Callable[[str, Optional[Dict[str, Any]]], Any]) -> None:
        self._broadcast = broadcast

    def consume(self, batch: CommittedCanonicalFactBatch) -> Any:
        return self._broadcast("facts", batch.live_payload)


def _missing_append_batch(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    raise RuntimeError("bot runtime canonical fact persistence dependency is not configured")


@dataclass(frozen=True)
class CanonicalFactPersistItem:
    bot_id: str
    run_id: str
    seq: int
    batch_kind: str
    payload: Dict[str, Any]
    context: Dict[str, Any]
    enqueued_monotonic: float = 0.0

    def as_payload(self) -> Dict[str, Any]:
        return {
            "bot_id": self.bot_id,
            "run_id": self.run_id,
            "seq": self.seq,
            "batch_kind": self.batch_kind,
            "payload": dict(self.payload),
            "context": dict(self.context),
        }


@dataclass(frozen=True)
class CanonicalFactProjectionItem:
    batch: CommittedCanonicalFactBatch
    enqueued_monotonic: float = 0.0


class CanonicalFactPersistenceBuffer:
    """Bounded async writer for canonical facts.

    Canonical events are not optional telemetry. Queue overflow, writer failure,
    or terminal drain timeout is surfaced as a runtime failure.
    """

    def __init__(
        self,
        *,
        queue_max: int = 16_384,
        batch_size: int = 512,
        flush_interval_s: float = 0.025,
        drain_timeout_s: float = 60.0,
        append_batch: Optional[Callable[..., Mapping[str, Any]]] = None,
        append_batches: Optional[Callable[..., Mapping[str, Any]]] = None,
    ) -> None:
        self._queue_max = max(int(queue_max), 1)
        self._batch_size = max(int(batch_size), 1)
        self._flush_interval_s = max(float(flush_interval_s), 0.001)
        self._drain_timeout_s = max(float(drain_timeout_s), 0.1)
        self._append_batch = append_batch or _missing_append_batch
        self._append_batches = append_batches
        self._queue: "queue.Queue[CanonicalFactPersistItem]" = queue.Queue(maxsize=self._queue_max)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._error_lock = threading.Lock()
        self._first_error: Optional[BaseException] = None
        self._queued_count = 0
        self._persisted_row_count = 0
        self._persisted_batch_count = 0
        self._persist_flush_count = 0
        self._persist_lag_ms = 0.0
        self._persist_batch_ms = 0.0
        self._persist_error_count = 0
        self._overflow_count = 0

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, object],
        *,
        append_batch: Optional[Callable[..., Mapping[str, Any]]] = None,
        append_batches: Optional[Callable[..., Mapping[str, Any]]] = None,
    ) -> "CanonicalFactPersistenceBuffer":
        def _int(value: object, default: int) -> int:
            try:
                return int(value) if value is not None else int(default)
            except (TypeError, ValueError):
                return int(default)

        def _float(value: object, default: float) -> float:
            try:
                return float(value) if value is not None else float(default)
            except (TypeError, ValueError):
                return float(default)

        queue_max = _int(
            config.get("canonical_fact_queue_max") or config.get("BOT_RUNTIME_CANONICAL_FACT_QUEUE_MAX"),
            16_384,
        )
        batch_size = _int(
            config.get("canonical_fact_batch_size") or config.get("BOT_RUNTIME_CANONICAL_FACT_BATCH_SIZE"),
            512,
        )
        flush_interval_ms = _float(
            config.get("canonical_fact_flush_interval_ms")
            or config.get("BOT_RUNTIME_CANONICAL_FACT_FLUSH_INTERVAL_MS"),
            25.0,
        )
        drain_timeout_s = _float(
            config.get("canonical_fact_drain_timeout_s")
            or config.get("BOT_RUNTIME_CANONICAL_FACT_DRAIN_TIMEOUT_S"),
            60.0,
        )
        return cls(
            queue_max=queue_max,
            batch_size=batch_size,
            flush_interval_s=max(flush_interval_ms / 1000.0, 0.001),
            drain_timeout_s=drain_timeout_s,
            append_batch=append_batch,
            append_batches=append_batches,
        )

    def record(self, item: CanonicalFactPersistItem) -> Dict[str, Any]:
        self._raise_if_failed()
        self._ensure_started()
        if item.enqueued_monotonic <= 0.0:
            item = replace(item, enqueued_monotonic=time.monotonic())
        try:
            self._queue.put_nowait(item)
        except queue.Full as exc:
            with self._metrics_lock:
                self._overflow_count += 1
            error = RuntimeError(
                "canonical fact persistence queue overflow "
                f"| run_id={item.run_id} | seq={item.seq} | queue_max={self._queue_max}"
            )
            self._set_error(error)
            raise error from exc
        with self._metrics_lock:
            self._queued_count += 1
        return {
            "queued": True,
            "queue_depth": self._queue.qsize(),
            "batch_size": self._batch_size,
        }

    def flush(self, *, reason: str, shutdown: bool = False, timeout_s: float | None = None) -> None:
        if not self._thread and self._queue.empty():
            self._raise_if_failed()
            return
        self._ensure_started()
        wait_timeout = self._drain_timeout_s if timeout_s is None else max(float(timeout_s), 0.1)
        deadline = time.monotonic() + wait_timeout
        deferred_error: Optional[BaseException] = None
        while time.monotonic() < deadline:
            try:
                self._raise_if_failed()
            except Exception as exc:  # noqa: BLE001
                deferred_error = exc
                break
            if self._queue.unfinished_tasks <= 0 and self._queue.empty():
                break
            time.sleep(0.01)
        if deferred_error is None and (self._queue.unfinished_tasks > 0 or not self._queue.empty()):
            error = RuntimeError(
                "canonical fact persistence drain timed out "
                f"| reason={reason} | queue_depth={self._queue.qsize()} "
                f"| unfinished={self._queue.unfinished_tasks}"
            )
            self._set_error(error)
            deferred_error = error
        if shutdown:
            self._stop.set()
            thread = self._thread
            if thread and thread.is_alive():
                thread.join(timeout=wait_timeout)
            if thread and thread.is_alive():
                raise RuntimeError(f"canonical fact persistence writer did not stop | reason={reason}")
        if deferred_error is not None:
            raise deferred_error
        self._raise_if_failed()
        logger.debug(
            "bot_canonical_fact_flush | reason=%s | shutdown=%s | queue_depth=%s | unfinished=%s",
            reason,
            shutdown,
            self._queue.qsize(),
            self._queue.unfinished_tasks,
        )

    def metrics_snapshot(self) -> Dict[str, float]:
        with self._metrics_lock:
            return {
                "queue_depth": float(self._queue.qsize()),
                "queued_count": float(self._queued_count),
                "persisted_row_count": float(self._persisted_row_count),
                "persisted_batch_count": float(self._persisted_batch_count),
                "persist_lag_ms": float(self._persist_lag_ms),
                "persist_batch_ms": float(self._persist_batch_ms),
                "persist_error_count": float(self._persist_error_count),
                "overflow_count": float(self._overflow_count),
            }

    def _ensure_started(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        with self._start_lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            thread = threading.Thread(
                target=self._worker_loop,
                name="bot-canonical-fact-writer",
                daemon=True,
            )
            thread.start()
            self._thread = thread

    def _set_error(self, exc: BaseException) -> None:
        with self._error_lock:
            if self._first_error is None:
                self._first_error = exc

    def _raise_if_failed(self) -> None:
        with self._error_lock:
            first_error = self._first_error
        if first_error is not None:
            raise RuntimeError(f"canonical fact persistence failed: {first_error}") from first_error

    def _persist_batch(self, batch: Sequence[CanonicalFactPersistItem]) -> Mapping[str, Any]:
        if self._append_batches is not None:
            return dict(self._append_batches([item.as_payload() for item in batch]) or {})
        inserted_rows = 0
        event_count = 0
        row_count = 0
        event_ids: list[Any] = []
        for item in batch:
            result = dict(
                self._append_batch(
                    bot_id=item.bot_id,
                    run_id=item.run_id,
                    seq=item.seq,
                    batch_kind=item.batch_kind,
                    payload=item.payload,
                    context=item.context,
                )
                or {}
            )
            inserted_rows += int(result.get("inserted_rows") or 0)
            event_count += int(result.get("event_count") or 0)
            row_count += int(result.get("row_count") or 0)
            raw_event_ids = result.get("event_ids")
            if isinstance(raw_event_ids, Sequence) and not isinstance(raw_event_ids, (str, bytes)):
                event_ids.extend(raw_event_ids)
        return {
            "batch_count": len(batch),
            "event_count": event_count,
            "row_count": row_count,
            "inserted_rows": inserted_rows,
            "event_ids": tuple(event_ids),
        }

    def _worker_loop(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            batch: List[CanonicalFactPersistItem] = []
            oldest_enqueued = time.monotonic()
            try:
                first = self._queue.get(timeout=self._flush_interval_s)
            except queue.Empty:
                continue
            batch.append(first)
            oldest_enqueued = float(first.enqueued_monotonic or oldest_enqueued)
            batch_deadline = time.monotonic() + self._flush_interval_s
            for _ in range(max(0, self._batch_size - 1)):
                remaining = max(batch_deadline - time.monotonic(), 0.0)
                if remaining <= 0.0:
                    break
                try:
                    next_item = self._queue.get(timeout=remaining)
                except queue.Empty:
                    break
                batch.append(next_item)
                if next_item.enqueued_monotonic > 0.0:
                    oldest_enqueued = min(oldest_enqueued, float(next_item.enqueued_monotonic))

            persist_started = time.perf_counter()
            result: Mapping[str, Any] = {}
            try:
                result = self._persist_batch(batch)
            except Exception as exc:  # noqa: BLE001
                with self._metrics_lock:
                    self._persist_error_count += 1
                self._set_error(exc)
                logger.exception("bot_canonical_fact_batch_persist_failed | error=%s", exc)
            finally:
                for _ in batch:
                    self._queue.task_done()

            persist_batch_ms = max((time.perf_counter() - persist_started) * 1000.0, 0.0)
            persist_lag_ms = max((time.monotonic() - oldest_enqueued) * 1000.0, 0.0)
            with self._metrics_lock:
                self._persisted_row_count += int(result.get("inserted_rows") or 0)
                self._persisted_batch_count += len(batch)
                self._persist_flush_count += 1
                persist_flush_count = self._persist_flush_count
                self._persist_batch_ms = persist_batch_ms
                self._persist_lag_ms = persist_lag_ms

            if _should_sample_debug_log(persist_flush_count):
                logger.debug(
                    "bot_canonical_fact_batch_persisted | flush_count=%s | batch_size=%s | inserted_rows=%s | row_count=%s | persist_batch_ms=%.3f | persist_lag_ms=%.3f | queue_depth=%s",
                    persist_flush_count,
                    len(batch),
                    result.get("inserted_rows"),
                    result.get("row_count"),
                    persist_batch_ms,
                    persist_lag_ms,
                    self._queue.qsize(),
                )


def _consume_committed_batch(
    consumers: Sequence[CanonicalFactConsumer],
    batch: CommittedCanonicalFactBatch,
) -> Tuple[PostAppendConsumerResult, ...]:
    consumer_results = []
    for consumer in consumers:
        consumer_name = consumer.__class__.__name__
        try:
            result = consumer.consume(batch)
            consumer_results.append(PostAppendConsumerResult(consumer_name=consumer_name, result=result))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bot_runtime_post_append_consumer_failed | consumer=%s | bot_id=%s | run_id=%s | seq=%s | batch_kind=%s | error=%s",
                consumer_name,
                batch.bot_id,
                batch.run_id,
                batch.seq,
                batch.batch_kind,
                exc,
            )
            consumer_results.append(PostAppendConsumerResult(consumer_name=consumer_name, error=str(exc)))
    return tuple(consumer_results)


class CanonicalFactProjectionDispatcher:
    """Bounded async dispatcher for committed BotLens projection batches."""

    def __init__(
        self,
        *,
        consumers: Sequence[CanonicalFactConsumer],
        queue_max: int = 16_384,
        flush_interval_s: float = 0.025,
        drain_timeout_s: float = 60.0,
    ) -> None:
        self._consumers = tuple(consumers)
        self._queue_max = max(int(queue_max), 1)
        self._flush_interval_s = max(float(flush_interval_s), 0.001)
        self._drain_timeout_s = max(float(drain_timeout_s), 0.1)
        self._queue: "queue.Queue[CanonicalFactProjectionItem]" = queue.Queue(maxsize=self._queue_max)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._error_lock = threading.Lock()
        self._first_error: Optional[BaseException] = None
        self._queued_count = 0
        self._dispatched_count = 0
        self._dispatch_error_count = 0
        self._overflow_count = 0
        self._dropped_count = 0
        self._drain_timeout_count = 0
        self._degraded = False
        self._dispatch_lag_ms = 0.0
        self._dispatch_batch_ms = 0.0
        self._latest_subscriber_count: Optional[int] = None
        self._latest_dropped_messages: Optional[int] = None

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, object],
        *,
        consumers: Sequence[CanonicalFactConsumer],
    ) -> "CanonicalFactProjectionDispatcher":
        def _int(value: object, default: int) -> int:
            try:
                return int(value) if value is not None else int(default)
            except (TypeError, ValueError):
                return int(default)

        def _float(value: object, default: float) -> float:
            try:
                return float(value) if value is not None else float(default)
            except (TypeError, ValueError):
                return float(default)

        queue_max = _int(
            config.get("canonical_fact_projection_queue_max")
            or config.get("BOT_RUNTIME_CANONICAL_FACT_PROJECTION_QUEUE_MAX"),
            16_384,
        )
        flush_interval_ms = _float(
            config.get("canonical_fact_projection_flush_interval_ms")
            or config.get("BOT_RUNTIME_CANONICAL_FACT_PROJECTION_FLUSH_INTERVAL_MS"),
            25.0,
        )
        drain_timeout_s = _float(
            config.get("canonical_fact_projection_drain_timeout_s")
            or config.get("BOT_RUNTIME_CANONICAL_FACT_PROJECTION_DRAIN_TIMEOUT_S"),
            60.0,
        )
        return cls(
            consumers=consumers,
            queue_max=queue_max,
            flush_interval_s=max(flush_interval_ms / 1000.0, 0.001),
            drain_timeout_s=drain_timeout_s,
        )

    def dispatch(self, batch: CommittedCanonicalFactBatch) -> Tuple[PostAppendConsumerResult, ...]:
        if not self._consumers:
            return ()
        self._raise_if_failed()
        self._ensure_started()
        item = CanonicalFactProjectionItem(batch=batch, enqueued_monotonic=time.monotonic())
        try:
            self._queue.put_nowait(item)
            queued = True
        except queue.Full:
            with self._metrics_lock:
                self._overflow_count += 1
                self._degraded = True
            queued = False
            dropped_oldest = False
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                dropped_oldest = True
                with self._metrics_lock:
                    self._dropped_count += 1
                self._queue.put_nowait(item)
                queued = True
            except queue.Empty:
                try:
                    self._queue.put_nowait(item)
                    queued = True
                except queue.Full:
                    queued = False
            except queue.Full:
                queued = False
            logger.warning(
                "bot_canonical_fact_projection_degraded | reason=queue_overflow | bot_id=%s | run_id=%s | seq=%s | queue_depth=%s | queue_max=%s | dropped_oldest=%s | queued=%s",
                batch.bot_id,
                batch.run_id,
                batch.seq,
                self._queue.qsize(),
                self._queue_max,
                dropped_oldest,
                queued,
            )
        with self._metrics_lock:
            if queued:
                self._queued_count += 1
        return (
            PostAppendConsumerResult(
                consumer_name=self.__class__.__name__,
                result={
                    "queued": queued,
                    "queue_depth": self._queue.qsize(),
                    "degraded": not queued or self._degraded,
                    "overflow_policy": "drop_oldest" if self._degraded else None,
                },
            ),
        )

    def flush(self, *, reason: str, shutdown: bool = False, timeout_s: float | None = None) -> None:
        if not self._thread and self._queue.empty():
            self._raise_if_failed()
            return
        self._ensure_started()
        wait_timeout = self._drain_timeout_s if timeout_s is None else max(float(timeout_s), 0.1)
        deadline = time.monotonic() + wait_timeout
        deferred_error: Optional[BaseException] = None
        while time.monotonic() < deadline:
            try:
                self._raise_if_failed()
            except Exception as exc:  # noqa: BLE001
                deferred_error = exc
                break
            if self._queue.unfinished_tasks <= 0 and self._queue.empty():
                break
            time.sleep(0.01)
        if deferred_error is None and (self._queue.unfinished_tasks > 0 or not self._queue.empty()):
            with self._metrics_lock:
                self._drain_timeout_count += 1
                self._degraded = True
            logger.warning(
                "bot_canonical_fact_projection_degraded | reason=drain_timeout | flush_reason=%s | queue_depth=%s | unfinished=%s",
                reason,
                self._queue.qsize(),
                self._queue.unfinished_tasks,
            )
        if shutdown:
            self._stop.set()
            thread = self._thread
            if thread and thread.is_alive():
                thread.join(timeout=wait_timeout)
            if thread and thread.is_alive():
                logger.warning(
                    "bot_canonical_fact_projection_degraded | reason=shutdown_timeout | flush_reason=%s | queue_depth=%s | unfinished=%s",
                    reason,
                    self._queue.qsize(),
                    self._queue.unfinished_tasks,
                )
        if deferred_error is not None:
            raise deferred_error
        self._raise_if_failed()
        logger.debug(
            "bot_canonical_fact_projection_flush | reason=%s | shutdown=%s | queue_depth=%s | unfinished=%s",
            reason,
            shutdown,
            self._queue.qsize(),
            self._queue.unfinished_tasks,
        )

    def metrics_snapshot(self) -> Dict[str, float]:
        with self._metrics_lock:
            return {
                "projection_queue_depth": float(self._queue.qsize()),
                "projection_queued_count": float(self._queued_count),
                "projection_dispatched_count": float(self._dispatched_count),
                "projection_dispatch_lag_ms": float(self._dispatch_lag_ms),
                "projection_dispatch_batch_ms": float(self._dispatch_batch_ms),
                "projection_dispatch_error_count": float(self._dispatch_error_count),
                "projection_overflow_count": float(self._overflow_count),
                "projection_dropped_count": float(self._dropped_count),
                "projection_drain_timeout_count": float(self._drain_timeout_count),
                "projection_degraded": 1.0 if self._degraded else 0.0,
                "projection_latest_subscriber_count": float(self._latest_subscriber_count or 0),
                "projection_latest_dropped_messages": float(self._latest_dropped_messages or 0),
            }

    def _ensure_started(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        with self._start_lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            thread = threading.Thread(
                target=self._worker_loop,
                name="bot-canonical-fact-projection",
                daemon=True,
            )
            thread.start()
            self._thread = thread

    def _set_error(self, exc: BaseException) -> None:
        with self._error_lock:
            if self._first_error is None:
                self._first_error = exc

    def _raise_if_failed(self) -> None:
        with self._error_lock:
            first_error = self._first_error
        if first_error is not None:
            raise RuntimeError(f"canonical fact projection failed: {first_error}") from first_error

    def _record_consumer_metrics(self, consumer_results: Sequence[PostAppendConsumerResult]) -> None:
        error_count = 0
        latest_subscriber_count = None
        latest_dropped_messages = None
        for result in consumer_results:
            if result.error:
                error_count += 1
                continue
            if result.consumer_name != "LiveFactsBroadcastConsumer":
                continue
            value = result.result
            if (
                isinstance(value, tuple)
                and len(value) == 2
                and all(isinstance(entry, int) for entry in value)
            ):
                latest_subscriber_count = int(value[0])
                latest_dropped_messages = int(value[1])
        with self._metrics_lock:
            self._dispatch_error_count += error_count
            if latest_subscriber_count is not None:
                self._latest_subscriber_count = latest_subscriber_count
            if latest_dropped_messages is not None:
                self._latest_dropped_messages = latest_dropped_messages

    def _worker_loop(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                item = self._queue.get(timeout=self._flush_interval_s)
            except queue.Empty:
                continue

            dispatch_started = time.perf_counter()
            results: Tuple[PostAppendConsumerResult, ...] = ()
            try:
                results = _consume_committed_batch(self._consumers, item.batch)
                self._record_consumer_metrics(results)
            finally:
                self._queue.task_done()

            dispatch_batch_ms = max((time.perf_counter() - dispatch_started) * 1000.0, 0.0)
            dispatch_lag_ms = max((time.monotonic() - item.enqueued_monotonic) * 1000.0, 0.0)
            with self._metrics_lock:
                self._dispatched_count += 1
                dispatched_count = self._dispatched_count
                self._dispatch_batch_ms = dispatch_batch_ms
                self._dispatch_lag_ms = dispatch_lag_ms
            if _should_sample_debug_log(dispatched_count):
                logger.debug(
                    "bot_canonical_fact_projection_dispatched | dispatched_count=%s | run_id=%s | seq=%s | batch_kind=%s | dispatch_batch_ms=%.3f | dispatch_lag_ms=%.3f | queue_depth=%s | consumer_results=%s",
                    dispatched_count,
                    item.batch.run_id,
                    item.batch.seq,
                    item.batch.batch_kind,
                    dispatch_batch_ms,
                    dispatch_lag_ms,
                    self._queue.qsize(),
                    len(results),
                )


class CanonicalFactAppender:
    def __init__(
        self,
        *,
        allocate_seq: Callable[[], int],
        append_batch: Optional[Callable[..., Mapping[str, Any]]] = None,
        persistence_buffer: Optional[CanonicalFactPersistenceBuffer] = None,
        projection_dispatcher: Optional[CanonicalFactProjectionDispatcher] = None,
        consumers: Sequence[CanonicalFactConsumer] = (),
    ) -> None:
        self._allocate_seq = allocate_seq
        self._append_batch = append_batch
        self._persistence_buffer = persistence_buffer
        self._projection_dispatcher = projection_dispatcher
        self._consumers = tuple(consumers)

    def append_fact_batch(
        self,
        *,
        bot_id: str,
        run_id: str,
        batch_kind: str,
        payload: Mapping[str, Any],
        context: Mapping[str, Any] | None = None,
        live_payload: Mapping[str, Any] | None = None,
        dispatch: bool = True,
    ) -> CanonicalFactAppendOutcome | None:
        durable_payload = canonical_fact_payload(payload)
        durable_facts = durable_payload.get("facts") if isinstance(durable_payload.get("facts"), list) else []
        outbound_payload = live_fact_payload(live_payload or payload)
        live_facts = outbound_payload.get("facts") if isinstance(outbound_payload.get("facts"), list) else []
        if not live_facts:
            return None
        if durable_facts and self._append_batch is None and self._persistence_buffer is None:
            raise RuntimeError("bot runtime canonical fact appender is not configured")

        seq = int(self._allocate_seq())
        stamped_canonical = dict(durable_payload)
        stamped_canonical["run_seq"] = seq
        stamped_canonical["seq"] = seq
        stamped_durable = dict(durable_payload)
        stamped_durable["run_seq"] = seq
        stamped_durable["seq"] = seq
        stamped_live = dict(outbound_payload)
        stamped_live["run_seq"] = seq
        stamped_live["seq"] = seq

        if not durable_facts:
            append_result = {
                "queued": False,
                "inserted_rows": 0,
                "row_count": 0,
                "retention_action": "transport_only",
                "dropped_or_summarized_facts": len(live_facts),
            }
        elif self._persistence_buffer is not None:
            append_result = self._persistence_buffer.record(
                CanonicalFactPersistItem(
                    bot_id=str(bot_id),
                    run_id=str(run_id),
                    seq=seq,
                    batch_kind=batch_kind,
                    payload=stamped_durable,
                    context=dict(context or {}),
                )
            )
        else:
            append_result = dict(
                self._append_batch(
                    bot_id=str(bot_id),
                    run_id=str(run_id),
                    seq=seq,
                    batch_kind=batch_kind,
                    payload=stamped_durable,
                    context=context,
                )
                or {}
            )
        batch = CommittedCanonicalFactBatch(
            batch_kind=batch_kind,
            bot_id=str(bot_id),
            run_id=str(run_id),
            seq=seq,
            symbol_key=str(stamped_live.get("series_key") or "").strip() or None,
            known_at=stamped_live.get("known_at"),
            event_time=stamped_live.get("event_time"),
            canonical_payload=stamped_canonical,
            live_payload=stamped_live,
            append_result=append_result,
        )

        if not dispatch:
            return CanonicalFactAppendOutcome(batch=batch)
        return CanonicalFactAppendOutcome(batch=batch, consumer_results=self.dispatch(batch))

    def flush(self, *, reason: str, shutdown: bool = False, timeout_s: float | None = None) -> None:
        deferred_error: Optional[BaseException] = None
        if self._persistence_buffer is not None:
            try:
                self._persistence_buffer.flush(reason=reason, shutdown=shutdown, timeout_s=timeout_s)
            except Exception as exc:  # noqa: BLE001
                deferred_error = exc
        if self._projection_dispatcher is not None:
            try:
                self._projection_dispatcher.flush(reason=reason, shutdown=shutdown, timeout_s=timeout_s)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "bot_canonical_fact_projection_flush_degraded | reason=%s | shutdown=%s | error=%s",
                    reason,
                    shutdown,
                    exc,
                )
        if deferred_error is not None:
            raise deferred_error

    def metrics_snapshot(self) -> Dict[str, float]:
        if self._persistence_buffer is None:
            metrics = {
                "queue_depth": 0.0,
                "queued_count": 0.0,
                "persisted_row_count": 0.0,
                "persisted_batch_count": 0.0,
                "persist_lag_ms": 0.0,
                "persist_batch_ms": 0.0,
                "persist_error_count": 0.0,
                "overflow_count": 0.0,
            }
        else:
            metrics = self._persistence_buffer.metrics_snapshot()
        if self._projection_dispatcher is None:
            metrics.update(
                {
                    "projection_queue_depth": 0.0,
                    "projection_queued_count": 0.0,
                    "projection_dispatched_count": 0.0,
                    "projection_dispatch_lag_ms": 0.0,
                    "projection_dispatch_batch_ms": 0.0,
                    "projection_dispatch_error_count": 0.0,
                    "projection_overflow_count": 0.0,
                    "projection_dropped_count": 0.0,
                    "projection_drain_timeout_count": 0.0,
                    "projection_degraded": 0.0,
                    "projection_latest_subscriber_count": 0.0,
                    "projection_latest_dropped_messages": 0.0,
                }
            )
        else:
            metrics.update(self._projection_dispatcher.metrics_snapshot())
        return metrics

    def dispatch(self, batch: CommittedCanonicalFactBatch) -> Tuple[PostAppendConsumerResult, ...]:
        if self._projection_dispatcher is not None:
            return self._projection_dispatcher.dispatch(batch)
        if not self._consumers:
            return ()
        return _consume_committed_batch(self._consumers, batch)


__all__ = [
    "CanonicalFactAppendOutcome",
    "CanonicalFactAppender",
    "CanonicalFactConsumer",
    "CanonicalFactPersistenceBuffer",
    "CanonicalFactProjectionDispatcher",
    "CommittedCanonicalFactBatch",
    "LiveFactsBroadcastConsumer",
    "PostAppendConsumerResult",
    "canonical_fact_payload",
    "has_canonical_facts",
    "has_live_facts",
    "live_fact_payload",
]
