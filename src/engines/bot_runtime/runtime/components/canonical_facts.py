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

_CANONICAL_SIMPLE_FACT_TYPES = frozenset(
    {
        "trade_opened",
        "trade_updated",
        "trade_closed",
        "wallet_ledger_event",
    }
)
_CANONICAL_DECISION_EVENT_NAMES = frozenset(
    {"SIGNAL_EMITTED", "DECISION_ACCEPTED", "DECISION_REJECTED", "RUNTIME_ERROR"}
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
                self._persist_batch_ms = persist_batch_ms
                self._persist_lag_ms = persist_lag_ms

            logger.debug(
                "bot_canonical_fact_batch_persisted | batch_size=%s | inserted_rows=%s | row_count=%s | persist_batch_ms=%.3f | persist_lag_ms=%.3f | queue_depth=%s",
                len(batch),
                result.get("inserted_rows"),
                result.get("row_count"),
                persist_batch_ms,
                persist_lag_ms,
                self._queue.qsize(),
            )


class CanonicalFactAppender:
    def __init__(
        self,
        *,
        allocate_seq: Callable[[], int],
        append_batch: Optional[Callable[..., Mapping[str, Any]]] = None,
        persistence_buffer: Optional[CanonicalFactPersistenceBuffer] = None,
        consumers: Sequence[CanonicalFactConsumer] = (),
    ) -> None:
        self._allocate_seq = allocate_seq
        self._append_batch = append_batch
        self._persistence_buffer = persistence_buffer
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
        if self._persistence_buffer is None:
            return
        self._persistence_buffer.flush(reason=reason, shutdown=shutdown, timeout_s=timeout_s)

    def metrics_snapshot(self) -> Dict[str, float]:
        if self._persistence_buffer is None:
            return {
                "queue_depth": 0.0,
                "queued_count": 0.0,
                "persisted_row_count": 0.0,
                "persisted_batch_count": 0.0,
                "persist_lag_ms": 0.0,
                "persist_batch_ms": 0.0,
                "persist_error_count": 0.0,
                "overflow_count": 0.0,
            }
        return self._persistence_buffer.metrics_snapshot()

    def dispatch(self, batch: CommittedCanonicalFactBatch) -> Tuple[PostAppendConsumerResult, ...]:
        if not self._consumers:
            return ()
        consumer_results = []
        for consumer in self._consumers:
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


__all__ = [
    "CanonicalFactAppendOutcome",
    "CanonicalFactAppender",
    "CanonicalFactConsumer",
    "CanonicalFactPersistenceBuffer",
    "CommittedCanonicalFactBatch",
    "LiveFactsBroadcastConsumer",
    "PostAppendConsumerResult",
    "canonical_fact_payload",
    "has_canonical_facts",
    "has_live_facts",
    "live_fact_payload",
]
