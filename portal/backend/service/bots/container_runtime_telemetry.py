"""Telemetry emitter transport for container-runtime to backend stream delivery."""

from __future__ import annotations

import asyncio
import hashlib
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
_LARGE_FACT_DEDUPE_BYTES = 64 * 1024
_FACT_DEDUPE_TTL_S = 5.0
_GENERAL_QUEUE_NAME = "telemetry_emit_queue"
_CONTROL_QUEUE_NAME = "telemetry_control_queue"
_CONTROL_MESSAGE_KINDS = frozenset(
    {
        "botlens_runtime_bootstrap_facts",
        "botlens_lifecycle_event",
    }
)


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
        self._control_queue_max = max(8, min(32, self._queue_max))
        self._queue_timeout_ms = int(queue_timeout_ms)
        self._retry_ms = int(retry_ms)
        self._sync_connect = None
        self._sync_ws = None
        self._state_lock = threading.Condition()
        self._pending_messages: Dict[str, deque[Dict[str, Any]]] = {
            _CONTROL_QUEUE_NAME: deque(),
            _GENERAL_QUEUE_NAME: deque(),
        }
        self._queue_capacities = {
            _CONTROL_QUEUE_NAME: self._control_queue_max,
            _GENERAL_QUEUE_NAME: self._queue_max,
        }
        self._stop = False
        self._worker_thread: threading.Thread | None = None
        self._backpressure_active = {
            _CONTROL_QUEUE_NAME: False,
            _GENERAL_QUEUE_NAME: False,
        }
        self._transport_connected = False
        self._transport_seen_connected = False
        self._transport_retry_pending = False
        self._bootstrap_dedupe: Dict[tuple[str, str, str, str], str] = {}
        self._fact_dedupe: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
        self._suppressed_bootstrap_duplicates = 0
        self._suppressed_large_fact_duplicates = 0
        self._last_payload_bytes = 0
        self._last_send_ms = 0.0
        if not self.url:
            return
        self._sync_connect = sync_connect
        self._worker_thread = threading.Thread(target=self._worker_loop, name="bot-telemetry-emitter", daemon=True)
        self._worker_thread.start()

    @staticmethod
    def _lane_for_kind(kind: Any) -> str:
        normalized_kind = str(kind or "").strip()
        if normalized_kind in _CONTROL_MESSAGE_KINDS:
            return _CONTROL_QUEUE_NAME
        return _GENERAL_QUEUE_NAME

    def _lane_for_payload(self, payload: Mapping[str, Any]) -> str:
        return self._lane_for_kind(payload.get("kind"))

    def _lane_for_context(self, context: Optional[Mapping[str, Any]] = None) -> str:
        return self._lane_for_kind((context or {}).get("kind"))

    def _queue_oldest_age_ms_locked(self, queue_name: str) -> float:
        queue = self._pending_messages[queue_name]
        if not queue:
            return 0.0
        oldest = queue[0]
        return max(
            (time.monotonic() - float(oldest.get("enqueued_monotonic") or time.monotonic())) * 1000.0,
            0.0,
        )

    def _queue_depth_locked(self, queue_name: str) -> int:
        return len(self._pending_messages[queue_name])

    def _queue_depth_total_locked(self) -> int:
        return sum(len(queue) for queue in self._pending_messages.values())

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
            "queue_name": str(ctx.get("queue_name") or "").strip() or None,
            **fields,
        }

    def _emit_queue_gauges_locked(self, *, context: Optional[Mapping[str, Any]] = None) -> None:
        del context
        for queue_name in (_CONTROL_QUEUE_NAME, _GENERAL_QUEUE_NAME):
            _OBSERVER.maybe_emit_gauges(
                queue_name,
                depth_metric="telemetry_queue_depth",
                utilization_metric="telemetry_queue_utilization",
                oldest_age_metric="telemetry_queue_oldest_age_ms",
                depth=self._queue_depth_locked(queue_name),
                capacity=max(int(self._queue_capacities[queue_name]), 1),
                oldest_age_ms=self._queue_oldest_age_ms_locked(queue_name),
                queue_name=queue_name,
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
        self._last_send_ms = elapsed_ms
        if opened_connection:
            if self._transport_seen_connected:
                _OBSERVER.event("telemetry_transport_connection_restored", **labels)
            else:
                _OBSERVER.event("telemetry_transport_connection_established", **labels)
        self._transport_connected = True
        self._transport_seen_connected = True
        if self._transport_retry_pending or any(bool(active) for active in self._backpressure_active.values()):
            _OBSERVER.event("telemetry_transport_recovered", **labels)
        self._transport_retry_pending = False
        for queue_name in self._backpressure_active:
            self._backpressure_active[queue_name] = False
        return True

    def _deliver_message(self, message: str, context: Optional[Mapping[str, Any]] = None) -> bool:
        if self._sync_connect is not None:
            return self._send_sync_message(message, context=context)
        return emit_telemetry_ephemeral_message(self.url, message, instrument=False)

    def _worker_loop(self) -> None:
        while True:
            entry = None
            queue_name = _GENERAL_QUEUE_NAME
            with self._state_lock:
                while not self._stop and self._queue_depth_total_locked() <= 0:
                    self._state_lock.wait(timeout=0.25)
                if self._stop and self._queue_depth_total_locked() <= 0:
                    break
                for candidate in (_CONTROL_QUEUE_NAME, _GENERAL_QUEUE_NAME):
                    queue = self._pending_messages[candidate]
                    if queue:
                        queue_name = candidate
                        entry = dict(queue[0])
                        break
            if not isinstance(entry, Mapping):
                continue
            message = str(entry.get("message") or "")
            context = entry.get("context") if isinstance(entry.get("context"), Mapping) else {}
            delivery_context = {
                **context,
                "queue_name": queue_name,
            }
            enqueued_at = float(entry.get("enqueued_monotonic") or time.monotonic())
            delivered = self._deliver_message(message, context=delivery_context)
            if delivered:
                queue_wait_ms = max((time.monotonic() - enqueued_at) * 1000.0, 0.0)
                labels = self._base_labels(delivery_context, queue_name=queue_name)
                with self._state_lock:
                    queue = self._pending_messages[queue_name]
                    if queue:
                        queue.popleft()
                    self._emit_queue_gauges_locked(context=context)
                    self._state_lock.notify_all()
                _OBSERVER.observe("telemetry_queue_wait_ms", queue_wait_ms, **labels)
                continue

            with self._state_lock:
                queue_depth = self._queue_depth_locked(queue_name)
                self._emit_queue_gauges_locked(context=context)
            labels = self._base_labels(delivery_context, queue_name=queue_name)
            _OBSERVER.increment("telemetry_transport_retries_total", **labels)
            _OBSERVER.event(
                "telemetry_transport_retry_scheduled",
                level=logging.WARN,
                queue_depth=queue_depth,
                queue_capacity=self._queue_capacities[queue_name],
                retry_ms=self._retry_ms,
                **labels,
            )
            self._transport_retry_pending = True
            time.sleep(self._retry_ms / 1000.0)

    def send_message(self, message: str, *, queue_name: str | None = None) -> bool:
        if not self.url:
            return False
        context = telemetry_message_context(message)
        resolved_queue_name = queue_name or self._lane_for_context(context)
        labels = self._base_labels(context, queue_name=resolved_queue_name)
        _OBSERVER.increment("telemetry_enqueue_attempt_total", **labels)
        deadline = time.monotonic() + (self._queue_timeout_ms / 1000.0)
        with self._state_lock:
            queue = self._pending_messages[resolved_queue_name]
            queue_capacity = self._queue_capacities[resolved_queue_name]
            while len(queue) >= queue_capacity and not self._stop:
                if not self._backpressure_active[resolved_queue_name]:
                    _OBSERVER.event(
                        "telemetry_backpressure_entered",
                        level=logging.WARN,
                        queue_depth=len(queue),
                        queue_max=queue_capacity,
                        **labels,
                    )
                    self._backpressure_active[resolved_queue_name] = True
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
                        queue_depth=len(queue),
                        queue_max=queue_capacity,
                        enqueue_timeout_ms=self._queue_timeout_ms,
                        failure_mode="timeout",
                        **labels,
                    )
                    self._emit_queue_gauges_locked(context=context)
                    return False
                self._state_lock.wait(timeout=remaining)
                queue = self._pending_messages[resolved_queue_name]
            if self._stop:
                return False
            queue.append(
                {
                    "message": str(message),
                    "context": context,
                    "enqueued_monotonic": time.monotonic(),
                }
            )
            self._emit_queue_gauges_locked(context=context)
            self._state_lock.notify_all()
        _OBSERVER.increment("telemetry_enqueue_success_total", **labels)
        return True

    def send(self, payload: Mapping[str, Any]) -> bool:
        queue_name = self._lane_for_payload(payload)
        large_fact_dedupe = self._large_fact_dedupe_signature(payload)
        if large_fact_dedupe is not None:
            dedupe_key, dedupe_signature, dedupe_bytes = large_fact_dedupe
            now = time.monotonic()
            with self._state_lock:
                previous = dict(self._fact_dedupe.get(dedupe_key) or {})
            if (
                previous.get("signature") == dedupe_signature
                and (now - float(previous.get("timestamp") or 0.0)) <= _FACT_DEDUPE_TTL_S
            ):
                labels = self._base_labels(
                    {
                        "bot_id": dedupe_key[0],
                        "run_id": dedupe_key[1],
                        "series_key": dedupe_key[2],
                        "kind": str(payload.get("kind") or ""),
                    },
                    queue_name=queue_name,
                )
                with self._state_lock:
                    self._suppressed_large_fact_duplicates += 1
                _OBSERVER.increment("telemetry_duplicate_suppressed_total", **labels)
                _OBSERVER.event(
                    "telemetry_duplicate_suppressed",
                    level=logging.WARN,
                    payload_bytes=dedupe_bytes,
                    suppression_reason="large_fact_duplicate",
                    **labels,
                )
                return True
        dedupe_key, dedupe_signature = self._bootstrap_dedupe_signature(payload)
        if dedupe_key is not None and dedupe_signature is not None:
            with self._state_lock:
                previous = self._bootstrap_dedupe.get(dedupe_key)
            if previous == dedupe_signature:
                labels = self._base_labels(
                    {
                        "bot_id": dedupe_key[0],
                        "run_id": dedupe_key[1],
                        "series_key": dedupe_key[2],
                        "kind": str(payload.get("kind") or ""),
                    },
                    queue_name=queue_name,
                )
                _OBSERVER.increment("telemetry_duplicate_suppressed_total", **labels)
                _OBSERVER.event(
                    "telemetry_duplicate_suppressed",
                    level=logging.WARN,
                    suppression_reason="bootstrap_duplicate",
                    **labels,
                )
                with self._state_lock:
                    self._suppressed_bootstrap_duplicates += 1
                return True
        message = json.dumps(payload)
        context = telemetry_message_context(message)
        labels = self._base_labels(context, queue_name=queue_name)
        _OBSERVER.increment("telemetry_emitted_total", **labels)
        _OBSERVER.observe(
            "telemetry_payload_bytes",
            float(payload_size_bytes(message)),
            **labels,
        )
        accepted = self.send_message(message, queue_name=queue_name)
        if accepted and dedupe_key is not None and dedupe_signature is not None:
            with self._state_lock:
                self._bootstrap_dedupe[dedupe_key] = dedupe_signature
        if accepted and large_fact_dedupe is not None:
            dedupe_key, dedupe_signature, dedupe_bytes = large_fact_dedupe
            with self._state_lock:
                self._fact_dedupe[dedupe_key] = {
                    "signature": dedupe_signature,
                    "timestamp": time.monotonic(),
                    "payload_bytes": dedupe_bytes,
                }
                self._last_payload_bytes = dedupe_bytes
        elif accepted:
            with self._state_lock:
                self._last_payload_bytes = payload_size_bytes(message)
        return accepted

    @staticmethod
    def _bootstrap_dedupe_signature(payload: Mapping[str, Any]) -> tuple[tuple[str, str, str, str] | None, str | None]:
        kind = str(payload.get("kind") or "").strip()
        if kind != "botlens_runtime_bootstrap_facts":
            return None, None
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        series_key = str(payload.get("series_key") or "").strip()
        bridge_session_id = str(payload.get("bridge_session_id") or "").strip()
        if not bot_id or not run_id or not series_key:
            return None, None
        fingerprint_payload = {
            "kind": kind,
            "bot_id": bot_id,
            "run_id": run_id,
            "series_key": series_key,
            "facts": list(payload.get("facts") or []),
        }
        encoded = json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return (bot_id, run_id, series_key, bridge_session_id), hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _large_fact_dedupe_signature(
        payload: Mapping[str, Any],
    ) -> tuple[tuple[str, str, str, str], str, int] | None:
        kind = str(payload.get("kind") or "").strip()
        if kind != "botlens_runtime_facts":
            return None
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        series_key = str(payload.get("series_key") or "").strip()
        bridge_session_id = str(payload.get("bridge_session_id") or "").strip()
        if not bot_id or not run_id or not series_key:
            return None
        facts = list(payload.get("facts") or [])
        fingerprint_payload = {
            "kind": kind,
            "bot_id": bot_id,
            "run_id": run_id,
            "series_key": series_key,
            "facts": facts,
        }
        encoded = json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        payload_bytes = len(encoded)
        if payload_bytes < _LARGE_FACT_DEDUPE_BYTES:
            return None
        return (bot_id, run_id, series_key, bridge_session_id), hashlib.sha256(encoded).hexdigest(), payload_bytes

    def pressure_snapshot(self) -> Dict[str, Any]:
        with self._state_lock:
            queue_depth = self._queue_depth_total_locked()
            oldest_age_ms = max(
                self._queue_oldest_age_ms_locked(_CONTROL_QUEUE_NAME),
                self._queue_oldest_age_ms_locked(_GENERAL_QUEUE_NAME),
            )
            return {
                "queue_depth": queue_depth,
                "queue_capacity": max(int(sum(self._queue_capacities.values())), 1),
                "queue_oldest_age_ms": round(oldest_age_ms, 3),
                "backpressure_active": any(bool(active) for active in self._backpressure_active.values()),
                "transport_connected": bool(self._transport_connected),
                "transport_retry_pending": bool(self._transport_retry_pending),
                "control_queue_depth": self._queue_depth_locked(_CONTROL_QUEUE_NAME),
                "control_queue_capacity": int(self._queue_capacities[_CONTROL_QUEUE_NAME]),
                "control_queue_oldest_age_ms": round(self._queue_oldest_age_ms_locked(_CONTROL_QUEUE_NAME), 3),
                "control_backpressure_active": bool(self._backpressure_active[_CONTROL_QUEUE_NAME]),
                "emit_queue_depth": self._queue_depth_locked(_GENERAL_QUEUE_NAME),
                "emit_queue_capacity": int(self._queue_capacities[_GENERAL_QUEUE_NAME]),
                "emit_queue_oldest_age_ms": round(self._queue_oldest_age_ms_locked(_GENERAL_QUEUE_NAME), 3),
                "emit_backpressure_active": bool(self._backpressure_active[_GENERAL_QUEUE_NAME]),
                "suppressed_bootstrap_duplicates": int(self._suppressed_bootstrap_duplicates),
                "suppressed_large_fact_duplicates": int(self._suppressed_large_fact_duplicates),
                "last_payload_bytes": int(self._last_payload_bytes),
                "last_send_ms": round(float(self._last_send_ms), 3),
            }

    def close(self) -> None:
        with self._state_lock:
            self._stop = True
            for queue in self._pending_messages.values():
                queue.clear()
            self._state_lock.notify_all()
        thread = self._worker_thread
        self._worker_thread = None
        if thread is not None:
            thread.join(timeout=0.5)
        self._close_sync_ws()


__all__ = ["TelemetryEmitter", "emit_telemetry_ephemeral_message", "telemetry_message_context"]
