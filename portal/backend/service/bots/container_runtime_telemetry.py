"""Telemetry emitter transport for container-runtime to backend stream delivery."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from collections.abc import Mapping
from typing import Any, Dict, Optional

from ..observability import BackendObserver, normalize_failure_mode, payload_size_bytes

try:
    import websockets  # type: ignore
    from websockets.sync.client import connect as sync_connect  # type: ignore
except Exception:  # pragma: no cover
    websockets = None
    sync_connect = None

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="container_runtime_telemetry", event_logger=logger)


def emit_telemetry_ephemeral_message(url: str, message: str, *, instrument: bool = True) -> bool:
    if not url:
        return False
    if websockets is None:
        if instrument:
            _OBSERVER.increment(
                "telemetry_transport_send_fail_total",
                failure_mode="library_missing",
                message_kind="ephemeral",
            )
            _OBSERVER.event(
                "telemetry_transport_send_failed",
                level=logging.WARN,
                failure_mode="library_missing",
                message_kind="ephemeral",
                package="websockets",
            )
        return False

    async def _send() -> None:
        async with websockets.connect(url, open_timeout=2, close_timeout=1) as ws:
            await ws.send(message)

    started = time.perf_counter()
    try:
        asyncio.run(_send())
    except Exception as exc:  # noqa: BLE001
        if instrument:
            failure_mode = normalize_failure_mode(exc)
            _OBSERVER.increment(
                "telemetry_transport_send_total",
                message_kind="ephemeral",
            )
            _OBSERVER.increment(
                "telemetry_transport_send_fail_total",
                message_kind="ephemeral",
                failure_mode=failure_mode,
            )
            _OBSERVER.observe(
                "telemetry_transport_send_ms",
                max((time.perf_counter() - started) * 1000.0, 0.0),
                message_kind="ephemeral",
                failure_mode=failure_mode,
            )
            _OBSERVER.observe(
                "telemetry_transport_payload_bytes",
                float(payload_size_bytes(message)),
                message_kind="ephemeral",
            )
            _OBSERVER.event(
                "telemetry_transport_send_failed",
                level=logging.WARN,
                message_kind="ephemeral",
                failure_mode=failure_mode,
                error=str(exc),
            )
        return False
    if instrument:
        _OBSERVER.increment("telemetry_transport_send_total", message_kind="ephemeral")
        _OBSERVER.observe(
            "telemetry_transport_send_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            message_kind="ephemeral",
        )
        _OBSERVER.observe(
            "telemetry_transport_payload_bytes",
            float(payload_size_bytes(message)),
            message_kind="ephemeral",
        )
    return True


def telemetry_message_context(message: str) -> Dict[str, Any]:
    try:
        payload = json.loads(str(message or "{}"))
    except Exception:
        return {}
    if not isinstance(payload, Mapping):
        return {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    return {
        "kind": str(payload.get("kind") or ""),
        "bot_id": str(payload.get("bot_id") or ""),
        "run_id": str(payload.get("run_id") or ""),
        "worker_id": str(payload.get("worker_id") or ""),
        "run_seq": int(payload.get("run_seq") or 0),
        "series_seq": int(payload.get("series_seq") or 0),
        "series_key": str(payload.get("series_key") or ""),
        "payload_bytes": int(summary.get("payload_bytes") or 0),
        "known_at": payload.get("known_at"),
    }


class TelemetryEmitter:
    def __init__(self, url: str, *, queue_max: int, queue_timeout_ms: int, retry_ms: int) -> None:
        self.url = str(url or "").strip()
        self._queue_max = int(queue_max)
        self._queue_timeout_ms = int(queue_timeout_ms)
        self._retry_ms = int(retry_ms)
        self._sync_connect = None
        self._sync_ws = None
        self._state_lock = threading.Condition()
        self._pending_messages: deque[Dict[str, Any]] = deque()
        self._stop = False
        self._worker_thread: threading.Thread | None = None
        self._backpressure_active = False
        self._transport_connected = False
        self._transport_seen_connected = False
        self._transport_retry_pending = False
        if not self.url:
            return
        self._sync_connect = sync_connect
        self._worker_thread = threading.Thread(target=self._worker_loop, name="bot-telemetry-emitter", daemon=True)
        self._worker_thread.start()

    def _close_sync_ws(self) -> None:
        ws = self._sync_ws
        self._sync_ws = None
        if ws is None:
            return
        try:
            ws.close()
        except Exception:
            pass

    def _base_labels(self, context: Optional[Mapping[str, Any]] = None, **fields: Any) -> Dict[str, Any]:
        ctx = context or {}
        return {
            "bot_id": str(ctx.get("bot_id") or "").strip() or None,
            "run_id": str(ctx.get("run_id") or "").strip() or None,
            "series_key": str(ctx.get("series_key") or "").strip() or None,
            "worker_id": str(ctx.get("worker_id") or "").strip() or None,
            "message_kind": str(ctx.get("kind") or "").strip() or None,
            **fields,
        }

    def _emit_queue_gauges_locked(self, *, context: Optional[Mapping[str, Any]] = None) -> None:
        depth = len(self._pending_messages)
        oldest_age_ms = 0.0
        if self._pending_messages:
            oldest = self._pending_messages[0]
            oldest_age_ms = max(
                (time.monotonic() - float(oldest.get("enqueued_monotonic") or time.monotonic())) * 1000.0,
                0.0,
            )
        _OBSERVER.maybe_emit_gauges(
            "telemetry_emit_queue",
            depth_metric="telemetry_queue_depth",
            utilization_metric="telemetry_queue_utilization",
            oldest_age_metric="telemetry_queue_oldest_age_ms",
            depth=depth,
            capacity=max(self._queue_max, 1),
            oldest_age_ms=oldest_age_ms,
            queue_name="telemetry_emit_queue",
        )

    def _send_sync_message(self, message: str, context: Optional[Mapping[str, Any]] = None) -> bool:
        if self._sync_connect is None:
            return False
        labels = self._base_labels(context, pipeline_stage="transport_send")
        _OBSERVER.increment("telemetry_transport_send_total", **labels)
        _OBSERVER.observe(
            "telemetry_transport_payload_bytes",
            float(payload_size_bytes(message)),
            **labels,
        )
        started = time.monotonic()
        opened_connection = False
        try:
            if self._sync_ws is None:
                self._sync_ws = self._sync_connect(self.url, open_timeout=2, close_timeout=1)
                opened_connection = True
            self._sync_ws.send(message)
        except Exception as exc:  # noqa: BLE001
            failure_mode = normalize_failure_mode(exc)
            elapsed_ms = max((time.monotonic() - started) * 1000.0, 0.0)
            _OBSERVER.increment(
                "telemetry_transport_send_fail_total",
                failure_mode=failure_mode,
                **labels,
            )
            _OBSERVER.observe(
                "telemetry_transport_send_ms",
                elapsed_ms,
                failure_mode=failure_mode,
                **labels,
            )
            _OBSERVER.event(
                "telemetry_transport_send_failed",
                level=logging.WARN,
                failure_mode=failure_mode,
                **labels,
            )
            if self._transport_connected or self._sync_ws is not None:
                _OBSERVER.event(
                    "telemetry_transport_connection_lost",
                    level=logging.WARN,
                    failure_mode=failure_mode,
                    **labels,
                )
            self._transport_connected = False
            self._close_sync_ws()
            return False

        elapsed_ms = max((time.monotonic() - started) * 1000.0, 0.0)
        _OBSERVER.observe("telemetry_transport_send_ms", elapsed_ms, **labels)
        if opened_connection:
            if self._transport_seen_connected:
                _OBSERVER.event("telemetry_transport_connection_restored", **labels)
            else:
                _OBSERVER.event("telemetry_transport_connection_established", **labels)
        self._transport_connected = True
        self._transport_seen_connected = True
        if self._transport_retry_pending or self._backpressure_active:
            _OBSERVER.event("telemetry_transport_recovered", **labels)
        self._transport_retry_pending = False
        self._backpressure_active = False
        return True

    def _deliver_message(self, message: str, context: Optional[Mapping[str, Any]] = None) -> bool:
        if self._sync_connect is not None:
            return self._send_sync_message(message, context=context)
        return emit_telemetry_ephemeral_message(self.url, message, instrument=False)

    def _worker_loop(self) -> None:
        while True:
            entry = None
            with self._state_lock:
                while not self._stop and not self._pending_messages:
                    self._state_lock.wait(timeout=0.25)
                if self._stop and not self._pending_messages:
                    break
                entry = dict(self._pending_messages[0]) if self._pending_messages else None
            if not isinstance(entry, Mapping):
                continue
            message = str(entry.get("message") or "")
            context = entry.get("context") if isinstance(entry.get("context"), Mapping) else {}
            enqueued_at = float(entry.get("enqueued_monotonic") or time.monotonic())
            delivered = self._deliver_message(message, context=context)
            if delivered:
                queue_wait_ms = max((time.monotonic() - enqueued_at) * 1000.0, 0.0)
                labels = self._base_labels(context, queue_name="telemetry_emit_queue")
                with self._state_lock:
                    if self._pending_messages:
                        self._pending_messages.popleft()
                    queue_depth = len(self._pending_messages)
                    self._emit_queue_gauges_locked(context=context)
                    self._state_lock.notify_all()
                _OBSERVER.observe("telemetry_queue_wait_ms", queue_wait_ms, **labels)
                continue

            with self._state_lock:
                queue_depth = len(self._pending_messages)
                self._emit_queue_gauges_locked(context=context)
            labels = self._base_labels(context, queue_name="telemetry_emit_queue")
            _OBSERVER.increment("telemetry_transport_retries_total", **labels)
            _OBSERVER.event(
                "telemetry_transport_retry_scheduled",
                level=logging.WARN,
                queue_depth=queue_depth,
                retry_ms=self._retry_ms,
                **labels,
            )
            self._transport_retry_pending = True
            time.sleep(self._retry_ms / 1000.0)

    def send_message(self, message: str) -> bool:
        if not self.url:
            return False
        context = telemetry_message_context(message)
        labels = self._base_labels(context, queue_name="telemetry_emit_queue")
        _OBSERVER.increment("telemetry_enqueue_attempt_total", **labels)
        deadline = time.monotonic() + (self._queue_timeout_ms / 1000.0)
        with self._state_lock:
            while len(self._pending_messages) >= self._queue_max and not self._stop:
                if not self._backpressure_active:
                    _OBSERVER.event(
                        "telemetry_backpressure_entered",
                        level=logging.WARN,
                        queue_depth=len(self._pending_messages),
                        queue_max=self._queue_max,
                        **labels,
                    )
                    self._backpressure_active = True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    _OBSERVER.increment(
                        "telemetry_enqueue_drop_total",
                        failure_mode="timeout",
                        **labels,
                    )
                    _OBSERVER.event(
                        "telemetry_enqueue_timeout",
                        level=logging.WARN,
                        queue_depth=len(self._pending_messages),
                        queue_max=self._queue_max,
                        enqueue_timeout_ms=self._queue_timeout_ms,
                        failure_mode="timeout",
                        **labels,
                    )
                    self._emit_queue_gauges_locked(context=context)
                    return False
                self._state_lock.wait(timeout=remaining)
            if self._stop:
                return False
            self._pending_messages.append(
                {
                    "message": str(message),
                    "context": context,
                    "enqueued_monotonic": time.monotonic(),
                }
            )
            queue_depth = len(self._pending_messages)
            self._emit_queue_gauges_locked(context=context)
            self._state_lock.notify_all()
        _OBSERVER.increment("telemetry_enqueue_success_total", **labels)
        return True

    def send(self, payload: Mapping[str, Any]) -> bool:
        message = json.dumps(payload)
        context = telemetry_message_context(message)
        labels = self._base_labels(context)
        _OBSERVER.increment("telemetry_emitted_total", **labels)
        _OBSERVER.observe(
            "telemetry_payload_bytes",
            float(payload_size_bytes(message)),
            **labels,
        )
        return self.send_message(message)

    def close(self) -> None:
        with self._state_lock:
            self._stop = True
            self._pending_messages.clear()
            self._state_lock.notify_all()
        thread = self._worker_thread
        self._worker_thread = None
        if thread is not None:
            thread.join(timeout=0.5)
        self._close_sync_ws()


__all__ = ["TelemetryEmitter", "emit_telemetry_ephemeral_message", "telemetry_message_context"]
