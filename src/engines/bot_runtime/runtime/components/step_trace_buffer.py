"""Async batched persistence for runtime step traces."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, Optional

from utils.log_context import with_log_context

from .step_trace_rollup import (
    merge_step_rollup_rows,
    rollup_step_metric_samples,
    step_rollup_identity,
)

logger = logging.getLogger(__name__)


def _missing_batch_writer(name: str):
    def _raise(payloads: list[dict[str, Any]]) -> int:
        raise RuntimeError(f"bot runtime persistence dependency is not configured: {name}")

    return _raise


class StepTracePersistenceBuffer:
    """Aggregate and persist step traces asynchronously.

    Runtime records many small timing samples in the hot path. The durable table
    stores bucketed rollups, so this buffer merges samples in memory and ships
    compact rollup rows instead of pushing one raw payload per bar to the writer.
    """

    def __init__(
        self,
        *,
        queue_max: int = 8192,
        batch_size: int = 512,
        flush_interval_s: float = 0.5,
        overflow_policy: str = "drop_oldest",
        record_batch: Optional[Callable[[list[dict[str, Any]]], int]] = None,
    ) -> None:
        self._queue_max = max(int(queue_max), 32)
        self._batch_size = max(int(batch_size), 1)
        self._flush_interval_s = max(float(flush_interval_s), 0.01)
        self._record_batch = record_batch or _missing_batch_writer("record_bot_run_steps_batch")
        policy = str(overflow_policy or "drop_oldest").strip().lower()
        self._overflow_policy = policy if policy in {"drop_oldest", "drop_newest"} else "drop_oldest"
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        self._condition = threading.Condition()
        self._metrics_lock = threading.Lock()
        self._pending_rollups: Dict[tuple[Any, ...], Dict[str, Any]] = {}
        self._oldest_pending_monotonic: Optional[float] = None
        self._write_inflight = 0
        self._dropped_count = 0
        self._persisted_count = 0
        self._persist_lag_ms = 0.0
        self._persist_batch_ms = 0.0
        self._persist_error_count = 0

    @classmethod
    def from_config(
        cls,
        config: Dict[str, object],
        *,
        record_batch: Optional[Callable[[list[dict[str, Any]]], int]] = None,
    ) -> "StepTracePersistenceBuffer":
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
            512,
        )
        flush_interval_ms = _float(
            config.get("step_trace_flush_interval_ms") or config.get("BOT_RUNTIME_STEP_TRACE_FLUSH_INTERVAL_MS"),
            5000.0,
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
            record_batch=record_batch,
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
        rollups = rollup_step_metric_samples([dict(payload)])
        if not rollups:
            return max((time.perf_counter() - enqueue_started) * 1000.0, 0.0)
        dropped = 0
        now = time.monotonic()
        with self._condition:
            if self._oldest_pending_monotonic is None:
                self._oldest_pending_monotonic = now
            for rollup in rollups:
                key = step_rollup_identity(rollup)
                if key not in self._pending_rollups and len(self._pending_rollups) >= self._queue_max:
                    if self._overflow_policy == "drop_oldest":
                        oldest_key = next(iter(self._pending_rollups), None)
                        if oldest_key is not None:
                            removed = self._pending_rollups.pop(oldest_key, {})
                            dropped += int(removed.get("raw_sample_count") or removed.get("sample_count") or 1)
                    else:
                        dropped += int(rollup.get("raw_sample_count") or rollup.get("sample_count") or 1)
                        continue
                current = self._pending_rollups.get(key)
                self._pending_rollups[key] = merge_step_rollup_rows(current or {}, rollup)
        if dropped:
            self._mark_dropped(dropped)
        return max((time.perf_counter() - enqueue_started) * 1000.0, 0.0)

    def flush(self, *, reason: str, shutdown: bool = False, timeout_s: float = 5.0) -> None:
        thread = self._thread
        with self._condition:
            is_empty = not self._pending_rollups and self._write_inflight <= 0
        if not thread and is_empty:
            return
        self._ensure_started()
        deadline = time.monotonic() + max(float(timeout_s), 0.1)
        with self._condition:
            self._condition.notify_all()
            while time.monotonic() < deadline:
                if not self._pending_rollups and self._write_inflight <= 0:
                    break
                self._condition.wait(timeout=0.01)
        if shutdown:
            self._stop.set()
            with self._condition:
                self._condition.notify_all()
            thread = self._thread
            if thread and thread.is_alive():
                thread.join(timeout=max(float(timeout_s), 0.1))
            with self._condition:
                if self._pending_rollups and not (thread and thread.is_alive()):
                    # Last-chance synchronous drain preserves shutdown summaries.
                    pending = self._drain_pending_locked()
                else:
                    pending = []
            if pending:
                self._persist_rollups(pending)

        with self._condition:
            queue_depth = len(self._pending_rollups) + self._write_inflight
            unfinished = self._write_inflight

        logger.debug(
            "bot_step_trace_flush | reason=%s | queue_depth=%s | unfinished=%s",
            reason,
            queue_depth,
            unfinished,
        )

    def metrics_snapshot(self) -> Dict[str, float]:
        with self._condition:
            queue_depth = len(self._pending_rollups) + self._write_inflight
        with self._metrics_lock:
            return {
                "queue_depth": float(queue_depth),
                "dropped_count": float(self._dropped_count),
                "persisted_count": float(self._persisted_count),
                "persist_lag_ms": float(self._persist_lag_ms),
                "persist_batch_ms": float(self._persist_batch_ms),
                "persist_error_count": float(self._persist_error_count),
            }

    def _mark_dropped(self, count: int = 1) -> None:
        with self._metrics_lock:
            self._dropped_count += max(int(count or 0), 1)

    def _drain_pending_locked(self) -> list[Dict[str, Any]]:
        pending = list(self._pending_rollups.values())
        self._pending_rollups.clear()
        self._oldest_pending_monotonic = None
        self._write_inflight += len(pending)
        return pending

    def _persist_rollups(self, rollups: list[Dict[str, Any]], *, oldest_enqueued: Optional[float] = None) -> None:
        persist_started = time.perf_counter()
        persisted = 0
        raw_sample_count = sum(int(row.get("raw_sample_count") or row.get("sample_count") or 0) for row in rollups)
        if rollups:
            try:
                persisted = int(self._record_batch(rollups))
            except Exception as exc:  # noqa: BLE001
                with self._metrics_lock:
                    self._persist_error_count += 1
                logger.warning("bot_step_trace_rollup_persist_failed | error=%s", exc)
        persist_batch_ms = max((time.perf_counter() - persist_started) * 1000.0, 0.0)
        persist_lag_ms = max((time.monotonic() - float(oldest_enqueued or time.monotonic())) * 1000.0, 0.0)
        with self._metrics_lock:
            self._persisted_count += max(raw_sample_count or persisted, 0)
            self._persist_batch_ms = persist_batch_ms
            self._persist_lag_ms = persist_lag_ms
        with self._condition:
            self._write_inflight = max(self._write_inflight - len(rollups), 0)
            self._condition.notify_all()

        logger.debug(
            with_log_context(
                "bot_step_trace_rollups_persisted",
                {
                    "rollup_count": len(rollups),
                    "raw_sample_count": raw_sample_count,
                    "persisted": persisted,
                    "persist_batch_ms": round(persist_batch_ms, 3),
                    "persist_lag_ms": round(persist_lag_ms, 3),
                    "queue_depth": self.metrics_snapshot().get("queue_depth"),
                },
            )
        )

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            with self._condition:
                if not self._pending_rollups:
                    self._condition.wait(timeout=self._flush_interval_s)
                if not self._pending_rollups:
                    continue
                oldest_enqueued = self._oldest_pending_monotonic
                rollups = self._drain_pending_locked()
            self._persist_rollups(rollups, oldest_enqueued=oldest_enqueued)
        while True:
            with self._condition:
                if not self._pending_rollups:
                    self._condition.notify_all()
                    return
                oldest_enqueued = self._oldest_pending_monotonic
                rollups = self._drain_pending_locked()
            self._persist_rollups(rollups, oldest_enqueued=oldest_enqueued)
