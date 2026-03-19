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

try:
    import websockets  # type: ignore
    from websockets.sync.client import connect as sync_connect  # type: ignore
except Exception:  # pragma: no cover
    websockets = None
    sync_connect = None

logger = logging.getLogger(__name__)


def emit_telemetry_ephemeral_message(url: str, message: str) -> bool:
    if not url:
        return False
    if websockets is None:
        logger.warning("bot_telemetry_library_missing | package=websockets")
        return False

    async def _send() -> None:
        async with websockets.connect(url, open_timeout=2, close_timeout=1) as ws:
            await ws.send(message)

    try:
        asyncio.run(_send())
    except Exception as exc:  # noqa: BLE001
        logger.warning("bot_telemetry_send_failed | error=%s", exc)
        return False
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

    def _send_sync_message(self, message: str, context: Optional[Mapping[str, Any]] = None) -> bool:
        if self._sync_connect is None:
            return False
        for attempt in range(2):
            try:
                if self._sync_ws is None:
                    self._sync_ws = self._sync_connect(self.url, open_timeout=2, close_timeout=1)
                started = time.monotonic()
                self._sync_ws.send(message)
                elapsed_ms = max((time.monotonic() - started) * 1000.0, 0.0)
                logger.debug(
                    "bot_telemetry_send_succeeded | mode=sync | attempt=%s | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | send_ms=%.3f",
                    attempt + 1,
                    (context or {}).get("kind"),
                    (context or {}).get("bot_id"),
                    (context or {}).get("run_id"),
                    (context or {}).get("run_seq"),
                    (context or {}).get("series_key"),
                    (context or {}).get("series_seq"),
                    (context or {}).get("payload_bytes"),
                    elapsed_ms,
                )
                return True
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "bot_telemetry_send_failed | mode=sync | attempt=%s | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | error=%s",
                    attempt + 1,
                    (context or {}).get("kind"),
                    (context or {}).get("bot_id"),
                    (context or {}).get("run_id"),
                    (context or {}).get("run_seq"),
                    (context or {}).get("series_key"),
                    (context or {}).get("series_seq"),
                    (context or {}).get("payload_bytes"),
                    exc,
                )
                self._close_sync_ws()
        return False

    def _deliver_message(self, message: str, context: Optional[Mapping[str, Any]] = None) -> bool:
        if self._sync_connect is not None:
            return self._send_sync_message(message, context=context)
        return emit_telemetry_ephemeral_message(self.url, message)

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
                with self._state_lock:
                    if self._pending_messages:
                        self._pending_messages.popleft()
                    queue_depth = len(self._pending_messages)
                    self._state_lock.notify_all()
                logger.debug(
                    "bot_telemetry_emit_dequeued | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | queue_wait_ms=%.3f | queue_depth=%s",
                    context.get("kind"),
                    context.get("bot_id"),
                    context.get("run_id"),
                    context.get("run_seq"),
                    context.get("series_key"),
                    context.get("series_seq"),
                    context.get("payload_bytes"),
                    queue_wait_ms,
                    queue_depth,
                )
                continue

            with self._state_lock:
                queue_depth = len(self._pending_messages)
            logger.warning(
                "bot_telemetry_emit_retry_scheduled | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | queue_depth=%s | retry_ms=%s",
                context.get("kind"),
                context.get("bot_id"),
                context.get("run_id"),
                context.get("run_seq"),
                context.get("series_key"),
                context.get("series_seq"),
                context.get("payload_bytes"),
                queue_depth,
                self._retry_ms,
            )
            time.sleep(self._retry_ms / 1000.0)

    def send_message(self, message: str) -> bool:
        if not self.url:
            return False
        context = telemetry_message_context(message)
        deadline = time.monotonic() + (self._queue_timeout_ms / 1000.0)
        with self._state_lock:
            while len(self._pending_messages) >= self._queue_max and not self._stop:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        "bot_telemetry_emit_queue_backpressure | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | queue_depth=%s | queue_max=%s | enqueue_timeout_ms=%s",
                        context.get("kind"),
                        context.get("bot_id"),
                        context.get("run_id"),
                        context.get("run_seq"),
                        context.get("series_key"),
                        context.get("series_seq"),
                        context.get("payload_bytes"),
                        len(self._pending_messages),
                        self._queue_max,
                        self._queue_timeout_ms,
                    )
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
            self._state_lock.notify_all()
        logger.debug(
            "bot_telemetry_emit_enqueued | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | queue_depth=%s | queue_max=%s",
            context.get("kind"),
            context.get("bot_id"),
            context.get("run_id"),
            context.get("run_seq"),
            context.get("series_key"),
            context.get("series_seq"),
            context.get("payload_bytes"),
            queue_depth,
            self._queue_max,
        )
        return True

    def send(self, payload: Mapping[str, Any]) -> bool:
        message = json.dumps(payload)
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
