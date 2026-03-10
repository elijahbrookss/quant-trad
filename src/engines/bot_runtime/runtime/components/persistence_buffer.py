"""Buffered persistence for bot runtime trade events."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from utils.log_context import with_log_context
from utils.perf_log import get_obs_enabled, get_obs_slow_ms, perf_log

logger = logging.getLogger(__name__)


class TradePersistenceBuffer:
    """Batch storage writes for trade entries and events."""

    def __init__(
        self,
        *,
        max_batch_size: int = 200,
        flush_interval_s: float = 2.0,
        time_fn: Callable[[], float] = time.monotonic,
        log_context_fn: Optional[Callable[..., Dict[str, object]]] = None,
        obs_enabled: bool = True,
        obs_slow_ms: Optional[float] = None,
    ) -> None:
        self._max_batch_size = max(int(max_batch_size), 1)
        self._flush_interval_s = max(float(flush_interval_s), 0.0)
        self._time_fn = time_fn
        self._log_context_fn = log_context_fn
        self._obs_enabled = obs_enabled
        self._obs_slow_ms = obs_slow_ms
        self._entries: List[Dict[str, Any]] = []
        self._events: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush = self._time_fn()

    @classmethod
    def from_config(
        cls,
        config: Dict[str, object],
        log_context_fn: Optional[Callable[..., Dict[str, object]]] = None,
    ) -> "TradePersistenceBuffer":
        max_batch = config.get("persistence_batch_size")
        flush_interval = config.get("persistence_flush_interval_s")
        obs_enabled = get_obs_enabled(config)
        obs_slow_ms = get_obs_slow_ms(config)
        return cls(
            max_batch_size=max_batch if isinstance(max_batch, int) and max_batch > 0 else 200,
            flush_interval_s=float(flush_interval) if isinstance(flush_interval, (int, float)) else 2.0,
            log_context_fn=log_context_fn,
            obs_enabled=obs_enabled,
            obs_slow_ms=obs_slow_ms,
        )

    def record_trade_entry(self, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._entries.append(dict(payload))
            self._maybe_flush_locked()

    def record_trade_event(self, payload: Dict[str, Any], *, event_type: Optional[str] = None) -> None:
        with self._lock:
            self._events.append(dict(payload))
            flush_now = event_type == "close"
            self._maybe_flush_locked(force=flush_now)

    def flush(self, *, reason: str = "manual") -> None:
        with self._lock:
            self._flush_locked(reason=reason)

    def _maybe_flush_locked(self, *, force: bool = False) -> None:
        if force:
            self._flush_locked(reason="close")
            return
        total = len(self._entries) + len(self._events)
        if total >= self._max_batch_size:
            self._flush_locked(reason="batch")
            return
        if self._flush_interval_s <= 0:
            return
        if (self._time_fn() - self._last_flush) >= self._flush_interval_s:
            self._flush_locked(reason="interval")

    @staticmethod
    def _normalize_reason(reason: str) -> str:
        if reason in {"batch", "interval", "close", "shutdown"}:
            return reason
        return "shutdown"

    def _flush_locked(self, *, reason: str) -> None:
        if not self._entries and not self._events:
            return
        entries = self._entries
        events = self._events
        self._entries = []
        self._events = []
        self._last_flush = self._time_fn()
        from portal.backend.service.storage import storage

        normalized_reason = self._normalize_reason(reason)
        base_context = self._log_context_fn() if self._log_context_fn else {}
        with perf_log(
            "bot_runtime_persistence_flush",
            logger=logger,
            base_context=base_context,
            enabled=self._obs_enabled,
            slow_ms=self._obs_slow_ms,
            trade_entries_written=len(entries),
            trade_events_written=len(events),
            flush_reason=normalized_reason,
        ):
            for entry in entries:
                storage.record_bot_trade(entry)
            for event in events:
                storage.record_bot_trade_event(event)
        if self._log_context_fn:
            context = self._log_context_fn(
                reason=normalized_reason,
                entries=len(entries),
                events=len(events),
            )
            logger.debug(with_log_context("trade_persistence_flush", context))
