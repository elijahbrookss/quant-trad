"""Async batched persistence for runtime step traces."""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any, Dict, List, Optional

from utils.log_context import with_log_context

logger = logging.getLogger(__name__)


class StepTracePersistenceBuffer:
    """Persist step traces asynchronously so bar execution is not DB-bound."""

    def __init__(
        self,
        *,
        queue_max: int = 8192,
        batch_size: int = 200,
        flush_interval_s: float = 0.2,
        overflow_policy: str = "drop_oldest",
    ) -> None:
        self._queue_max = max(int(queue_max), 32)
        self._batch_size = max(int(batch_size), 1)
        self._flush_interval_s = max(float(flush_interval_s), 0.01)
        policy = str(overflow_policy or "drop_oldest").strip().lower()
        self._overflow_policy = policy if policy in {"drop_oldest", "drop_newest"} else "drop_oldest"
        self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=self._queue_max)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._dropped_count = 0
        self._persisted_count = 0
        self._persist_lag_ms = 0.0
        self._persist_batch_ms = 0.0
        self._persist_error_count = 0

    @classmethod
    def from_config(cls, config: Dict[str, object]) -> "StepTracePersistenceBuffer":
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
            config.get("step_trace_queue_max") or config.get("BOT_RUNTIME_STEP_TRACE_QUEUE_MAX"),
            8192,
        )
        batch_size = _int(
            config.get("step_trace_batch_size") or config.get("BOT_RUNTIME_STEP_TRACE_BATCH_SIZE"),
            200,
        )
        flush_interval_ms = _float(
            config.get("step_trace_flush_interval_ms") or config.get("BOT_RUNTIME_STEP_TRACE_FLUSH_INTERVAL_MS"),
            200.0,
        )
        overflow_policy = str(
            config.get("step_trace_overflow_policy")
            or config.get("BOT_RUNTIME_STEP_TRACE_OVERFLOW_POLICY")
            or "drop_oldest"
        )
        return cls(
            queue_max=max(queue_max, 32),
            batch_size=max(batch_size, 1),
            flush_interval_s=max(flush_interval_ms / 1000.0, 0.01),
            overflow_policy=overflow_policy,
        )

    def _ensure_started(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        with self._start_lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            thread = threading.Thread(
                target=self._worker_loop,
                name="bot-step-trace-writer",
                daemon=True,
            )
            thread.start()
            self._thread = thread

    def record(self, payload: Dict[str, Any]) -> float:
        self._ensure_started()
        enqueue_started = time.perf_counter()
        item = {
            "payload": dict(payload),
            "enqueued_monotonic": time.monotonic(),
        }
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            if self._overflow_policy == "drop_oldest":
                dropped_oldest = False
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                    dropped_oldest = True
                except queue.Empty:
                    dropped_oldest = False
                if dropped_oldest:
                    try:
                        self._queue.put_nowait(item)
                    except queue.Full:
                        self._mark_dropped()
                        return max((time.perf_counter() - enqueue_started) * 1000.0, 0.0)
                    self._mark_dropped()
                else:
                    self._mark_dropped()
            else:
                self._mark_dropped()
        return max((time.perf_counter() - enqueue_started) * 1000.0, 0.0)

    def flush(self, *, reason: str, shutdown: bool = False, timeout_s: float = 5.0) -> None:
        thread = self._thread
        if not thread and self._queue.empty():
            return
        self._ensure_started()
        deadline = time.monotonic() + max(float(timeout_s), 0.1)
        while time.monotonic() < deadline:
            if self._queue.unfinished_tasks <= 0 and self._queue.empty():
                break
            time.sleep(0.01)
        if shutdown:
            self._stop.set()
            thread = self._thread
            if thread and thread.is_alive():
                thread.join(timeout=max(float(timeout_s), 0.1))

        logger.debug(
            "bot_step_trace_flush | reason=%s | queue_depth=%s | unfinished=%s",
            reason,
            self._queue.qsize(),
            self._queue.unfinished_tasks,
        )

    def metrics_snapshot(self) -> Dict[str, float]:
        with self._metrics_lock:
            return {
                "queue_depth": float(self._queue.qsize()),
                "dropped_count": float(self._dropped_count),
                "persisted_count": float(self._persisted_count),
                "persist_lag_ms": float(self._persist_lag_ms),
                "persist_batch_ms": float(self._persist_batch_ms),
                "persist_error_count": float(self._persist_error_count),
            }

    def _mark_dropped(self) -> None:
        with self._metrics_lock:
            self._dropped_count += 1

    def _worker_loop(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            batch: List[Dict[str, Any]] = []
            oldest_enqueued: Optional[float] = None
            try:
                first = self._queue.get(timeout=self._flush_interval_s)
            except queue.Empty:
                continue
            batch.append(first)
            oldest_enqueued = float(first.get("enqueued_monotonic") or time.monotonic())
            for _ in range(max(0, self._batch_size - 1)):
                try:
                    nxt = self._queue.get_nowait()
                except queue.Empty:
                    break
                batch.append(nxt)
                candidate = float(nxt.get("enqueued_monotonic") or oldest_enqueued)
                oldest_enqueued = min(oldest_enqueued, candidate)

            payloads = []
            for item in batch:
                payload = item.get("payload")
                if isinstance(payload, dict):
                    payloads.append(payload)

            persist_started = time.perf_counter()
            persisted = 0
            if payloads:
                try:
                    from portal.backend.service.storage import storage

                    persisted = int(storage.record_bot_run_steps_batch(payloads))
                except Exception as exc:  # noqa: BLE001
                    with self._metrics_lock:
                        self._persist_error_count += 1
                    logger.warning("bot_step_trace_batch_persist_failed | error=%s", exc)
            persist_batch_ms = max((time.perf_counter() - persist_started) * 1000.0, 0.0)
            persist_lag_ms = max((time.monotonic() - float(oldest_enqueued or time.monotonic())) * 1000.0, 0.0)
            with self._metrics_lock:
                self._persisted_count += max(persisted, 0)
                self._persist_batch_ms = persist_batch_ms
                self._persist_lag_ms = persist_lag_ms

            for _ in batch:
                self._queue.task_done()

            logger.debug(
                with_log_context(
                    "bot_step_trace_batch_persisted",
                    {
                        "batch_size": len(payloads),
                        "persisted": persisted,
                        "persist_batch_ms": round(persist_batch_ms, 3),
                        "persist_lag_ms": round(persist_lag_ms, 3),
                        "queue_depth": self._queue.qsize(),
                    },
                )
            )
