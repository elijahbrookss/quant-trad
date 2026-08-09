"""Bounded asynchronous persistence for bot runtime trade evidence."""

from __future__ import annotations

from collections import deque
import logging
import threading
import time
from typing import Any, Callable, Deque, Dict, Optional, Tuple

from utils.log_context import with_log_context
from utils.perf_log import get_obs_enabled, get_obs_slow_ms, perf_log

logger = logging.getLogger(__name__)


def _missing_sink(name: str) -> Callable[..., Any]:
    def _raise(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(f"bot runtime persistence dependency is not configured: {name}")

    return _raise


_PendingWrite = Tuple[int, str, Dict[str, Any], float]


class TradePersistenceBuffer:
    """Persist ordered trade snapshots/events without blocking the bar path.

    This is a strict durability buffer, not best-effort telemetry. Payloads are
    copied before enqueue, the queue is bounded, and a writer failure, overflow,
    or terminal drain timeout is surfaced as a runtime failure. A single worker
    preserves enqueue order, including entry-before-close updates for one trade.
    """

    def __init__(
        self,
        *,
        max_batch_size: int = 200,
        queue_max: int = 4096,
        flush_interval_s: float = 2.0,
        drain_timeout_s: float = 30.0,
        record_trade: Optional[Callable[[Dict[str, Any]], None]] = None,
        record_trade_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        time_fn: Callable[[], float] = time.monotonic,
        log_context_fn: Optional[Callable[..., Dict[str, object]]] = None,
        obs_enabled: bool = True,
        obs_slow_ms: Optional[float] = None,
    ) -> None:
        self._max_batch_size = max(int(max_batch_size), 1)
        self._queue_max = max(int(queue_max), 1)
        self._flush_interval_s = max(float(flush_interval_s), 0.0)
        self._drain_timeout_s = max(float(drain_timeout_s), 0.1)
        self._record_trade = record_trade or _missing_sink("record_trade")
        self._record_trade_event = record_trade_event or _missing_sink("record_trade_event")
        self._time_fn = time_fn
        self._log_context_fn = log_context_fn
        self._obs_enabled = obs_enabled
        self._obs_slow_ms = obs_slow_ms

        self._condition = threading.Condition()
        self._queue: Deque[_PendingWrite] = deque()
        self._thread: Optional[threading.Thread] = None
        self._next_sequence = 0
        self._inflight = 0
        self._flush_requested = False
        self._stop_requested = False
        self._shutdown = False
        self._worker_error: Optional[BaseException] = None
        self._terminal_error: Optional[BaseException] = None
        self._persisted_count = 0
        self._persist_batch_ms = 0.0
        self._persist_lag_ms = 0.0

    @classmethod
    def from_config(
        cls,
        config: Dict[str, object],
        *,
        log_context_fn: Optional[Callable[..., Dict[str, object]]] = None,
        record_trade: Optional[Callable[[Dict[str, Any]], None]] = None,
        record_trade_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> "TradePersistenceBuffer":
        def _positive_int(value: object, default: int) -> int:
            try:
                parsed = int(value) if value is not None else int(default)
            except (TypeError, ValueError):
                return int(default)
            return parsed if parsed > 0 else int(default)

        def _nonnegative_float(value: object, default: float) -> float:
            try:
                parsed = float(value) if value is not None else float(default)
            except (TypeError, ValueError):
                return float(default)
            return parsed if parsed >= 0 else float(default)

        max_batch = _positive_int(config.get("persistence_batch_size"), 200)
        queue_max = _positive_int(config.get("persistence_queue_max"), 4096)
        flush_interval = _nonnegative_float(config.get("persistence_flush_interval_s"), 2.0)
        drain_timeout = _nonnegative_float(config.get("persistence_drain_timeout_s"), 30.0)
        return cls(
            max_batch_size=max_batch,
            queue_max=queue_max,
            flush_interval_s=flush_interval,
            drain_timeout_s=max(drain_timeout, 0.1),
            record_trade=record_trade,
            record_trade_event=record_trade_event,
            log_context_fn=log_context_fn,
            obs_enabled=get_obs_enabled(config),
            obs_slow_ms=get_obs_slow_ms(config),
        )

    def record_trade_entry(self, payload: Dict[str, Any]) -> None:
        self._enqueue("trade", payload)

    def record_trade_event(self, payload: Dict[str, Any], *, event_type: Optional[str] = None) -> None:
        self._enqueue("event", payload, force=event_type == "close")

    def flush(
        self,
        *,
        reason: str = "manual",
        shutdown: bool = False,
        timeout_s: Optional[float] = None,
    ) -> None:
        timeout = self._drain_timeout_s if timeout_s is None else max(float(timeout_s), 0.1)
        deadline = time.monotonic() + timeout
        thread: Optional[threading.Thread]
        with self._condition:
            self._raise_failure_locked()
            if self._shutdown and not self._queue and self._inflight <= 0:
                return
            if not self._queue and self._inflight <= 0 and self._thread is None:
                if shutdown:
                    self._shutdown = True
                    self._stop_requested = True
                return
            self._ensure_started_locked()
            self._flush_requested = True
            if shutdown:
                self._shutdown = True
                self._stop_requested = True
            self._condition.notify_all()
            while (self._queue or self._inflight > 0) and self._worker_error is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    error = RuntimeError(
                        "bot_trade_persistence_drain_timeout: "
                        f"reason={self._normalize_reason(reason)} "
                        f"queue_depth={len(self._queue)} inflight={self._inflight} timeout_s={timeout}"
                    )
                    self._terminal_error = error
                    self._stop_requested = True
                    self._shutdown = True
                    self._condition.notify_all()
                    raise error
                self._condition.wait(timeout=min(remaining, 0.05))
            self._raise_failure_locked()
            thread = self._thread

        if shutdown and thread and thread.is_alive():
            remaining = max(deadline - time.monotonic(), 0.0)
            thread.join(timeout=remaining)
            if thread.is_alive():
                with self._condition:
                    error = RuntimeError(
                        "bot_trade_persistence_shutdown_timeout: "
                        f"reason={self._normalize_reason(reason)} timeout_s={timeout}"
                    )
                    self._terminal_error = error
                raise error

        logger.debug(
            with_log_context(
                "trade_persistence_drain_complete",
                self._context(
                    reason=self._normalize_reason(reason),
                    shutdown=shutdown,
                    **self.metrics_snapshot(),
                ),
            )
        )

    def metrics_snapshot(self) -> Dict[str, object]:
        with self._condition:
            return {
                "queue_depth": len(self._queue) + self._inflight,
                "persisted_count": self._persisted_count,
                "persist_batch_ms": round(self._persist_batch_ms, 3),
                "persist_lag_ms": round(self._persist_lag_ms, 3),
                "worker_alive": bool(self._thread and self._thread.is_alive()),
                "worker_failed": self._worker_error is not None,
            }

    def _enqueue(self, kind: str, payload: Dict[str, Any], *, force: bool = False) -> None:
        with self._condition:
            self._raise_failure_locked()
            if self._shutdown:
                raise RuntimeError("bot_trade_persistence_closed: cannot enqueue after terminal drain")
            if len(self._queue) >= self._queue_max:
                raise RuntimeError(
                    "bot_trade_persistence_overflow: "
                    f"queue_max={self._queue_max} queue_depth={len(self._queue)} kind={kind}"
                )
            self._next_sequence += 1
            self._queue.append((self._next_sequence, kind, dict(payload), self._time_fn()))
            self._ensure_started_locked()
            if force or len(self._queue) >= self._max_batch_size:
                self._flush_requested = True
            self._condition.notify_all()

    def _ensure_started_locked(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._raise_failure_locked()
        if self._shutdown:
            raise RuntimeError("bot_trade_persistence_closed: writer is shut down")
        thread = threading.Thread(
            target=self._worker_loop,
            name="bot-trade-persistence-writer",
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def _raise_failure_locked(self) -> None:
        failure = self._worker_error or self._terminal_error
        if failure is None:
            return
        raise RuntimeError(f"bot_trade_persistence_failed: {failure}") from failure

    def _worker_loop(self) -> None:
        while True:
            with self._condition:
                batch = self._wait_for_batch_locked()
                if not batch:
                    self._condition.notify_all()
                    return
                self._inflight += len(batch)
            try:
                self._persist_batch(batch)
            except Exception as exc:  # noqa: BLE001
                with self._condition:
                    self._worker_error = exc
                    self._inflight = max(self._inflight - len(batch), 0)
                    self._stop_requested = True
                    self._condition.notify_all()
                logger.exception(
                    with_log_context(
                        "bot_trade_persistence_writer_failed",
                        self._context(
                            first_sequence=batch[0][0],
                            last_sequence=batch[-1][0],
                            batch_size=len(batch),
                            error=str(exc),
                        ),
                    )
                )
                return
            with self._condition:
                self._persisted_count += len(batch)
                self._inflight = max(self._inflight - len(batch), 0)
                self._condition.notify_all()

    def _wait_for_batch_locked(self) -> list[_PendingWrite]:
        while True:
            if self._worker_error is not None or self._terminal_error is not None:
                return []
            if self._queue:
                oldest_enqueued = self._queue[0][3]
                interval_elapsed = (
                    self._flush_interval_s > 0
                    and (self._time_fn() - oldest_enqueued) >= self._flush_interval_s
                )
                if self._stop_requested or self._flush_requested or interval_elapsed:
                    break
                elapsed = max(self._time_fn() - oldest_enqueued, 0.0)
                wait_timeout = (
                    max(self._flush_interval_s - elapsed, 0.001) if self._flush_interval_s > 0 else None
                )
                self._condition.wait(timeout=wait_timeout)
                continue
            if self._stop_requested:
                return []
            self._condition.wait()

        batch: list[_PendingWrite] = []
        while self._queue and len(batch) < self._max_batch_size:
            batch.append(self._queue.popleft())
        self._flush_requested = bool(self._queue) or self._stop_requested
        return batch

    def _persist_batch(self, batch: list[_PendingWrite]) -> None:
        started = time.perf_counter()
        oldest_enqueued = batch[0][3]
        entry_count = sum(1 for _sequence, kind, _payload, _queued_at in batch if kind == "trade")
        event_count = len(batch) - entry_count
        with perf_log(
            "bot_runtime_persistence_flush",
            logger=logger,
            base_context=self._context(),
            enabled=self._obs_enabled,
            slow_ms=self._obs_slow_ms,
            trade_entries_written=entry_count,
            trade_events_written=event_count,
            flush_reason="async_batch",
        ):
            for _sequence, kind, payload, _queued_at in batch:
                if kind == "trade":
                    self._record_trade(payload)
                else:
                    self._record_trade_event(payload)
        self._persist_batch_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
        self._persist_lag_ms = max((self._time_fn() - oldest_enqueued) * 1000.0, 0.0)
        logger.debug(
            with_log_context(
                "trade_persistence_batch_written",
                self._context(
                    first_sequence=batch[0][0],
                    last_sequence=batch[-1][0],
                    entries=entry_count,
                    events=event_count,
                    persist_batch_ms=round(self._persist_batch_ms, 3),
                    persist_lag_ms=round(self._persist_lag_ms, 3),
                ),
            )
        )

    def _context(self, **extra: object) -> Dict[str, object]:
        context = dict(self._log_context_fn() if self._log_context_fn else {})
        context.update(extra)
        return context

    @staticmethod
    def _normalize_reason(reason: str) -> str:
        if reason in {"batch", "interval", "close", "shutdown"}:
            return reason
        return "shutdown"
