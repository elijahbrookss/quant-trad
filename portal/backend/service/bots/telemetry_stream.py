from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Mapping as AbcMapping
from datetime import datetime
from typing import Any, DefaultDict, Dict, Optional, Tuple

from engines.bot_runtime.runtime.event_types import BOTLENS_SERIES_BOOTSTRAP, BOTLENS_SERIES_DELTA
from fastapi import WebSocket

from ..storage.storage import (
    get_latest_bot_run_view_state,
    record_bot_runtime_event,
    upsert_bot_run_view_state,
)
from .bot_service import publish_runtime_update
from .botlens_projection import apply_series_runtime_delta, canonicalize_projection, normalize_series_key
from .botlens_series_service import get_series_window

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_RING_SIZE = max(32, int(os.getenv("BOTLENS_STREAM_RING_SIZE") or 2048))
_INGEST_QUEUE_MAX = max(64, int(os.getenv("BOTLENS_INGEST_QUEUE_MAX") or 4096))


def _sanitize_json(value: Any) -> Any:
    if isinstance(value, AbcMapping):
        return {str(k): _sanitize_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_json(v) for v in value]
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat() + "Z"
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


class BotTelemetryHub:
    def __init__(self) -> None:
        self._latest_view_state: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._latest_run_by_bot: Dict[str, str] = {}
        self._series_viewers: DefaultDict[Tuple[str, str], Dict[WebSocket, Dict[str, Any]]] = defaultdict(dict)
        self._series_live_tail_ring: DefaultDict[Tuple[str, str], deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=_RING_SIZE)
        )
        self._run_stream_session_id: Dict[str, str] = {}
        self._ingest_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=_INGEST_QUEUE_MAX)
        self._ingest_task: Optional[asyncio.Task[None]] = None
        self._lock = asyncio.Lock()
        self._worker_lock = asyncio.Lock()

    @staticmethod
    def _series_bootstrap_envelope(window: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "type": "botlens_live_bootstrap",
            "run_id": str(window.get("run_id") or ""),
            "series_key": str(window.get("series_key") or ""),
            "schema_version": int(window.get("schema_version") or _SCHEMA_VERSION),
            "seq": _coerce_int(window.get("seq"), default=0),
            "event_time": window.get("event_time"),
            "stream_session_id": str(window.get("stream_session_id") or ""),
            "payload": {"window": _sanitize_json(window.get("window") or {})},
        }

    @staticmethod
    def _series_error_envelope(*, run_id: str, series_key: str, message: str) -> Dict[str, Any]:
        return {
            "type": "botlens_live_error",
            "run_id": str(run_id),
            "series_key": str(series_key),
            "payload": {"message": str(message or "BotLens stream failed")},
        }

    @staticmethod
    def _resync_required_envelope(
        *,
        run_id: str,
        series_key: str,
        reason: str,
        stream_session_id: str,
        previous_stream_session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = {"reason": str(reason or "continuity_lost"), "details": _sanitize_json(details or {})}
        if previous_stream_session_id:
            payload["previous_stream_session_id"] = str(previous_stream_session_id)
        return {
            "type": "botlens_live_resync_required",
            "run_id": str(run_id),
            "series_key": str(series_key),
            "stream_session_id": str(stream_session_id or ""),
            "payload": payload,
        }

    @staticmethod
    def _new_stream_session_id() -> str:
        return uuid.uuid4().hex

    def _ensure_run_stream_session_id_locked(self, run_id: str) -> str:
        key = str(run_id)
        session_id = str(self._run_stream_session_id.get(key) or "").strip()
        if session_id:
            return session_id
        session_id = self._new_stream_session_id()
        self._run_stream_session_id[key] = session_id
        return session_id

    async def _invalidate_run_live_continuity(
        self,
        *,
        run_id: str,
        reason: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        targets: list[tuple[Tuple[str, str], WebSocket]] = []
        previous_stream_session_id: Optional[str] = None
        next_stream_session_id: str = ""
        async with self._lock:
            key_run = str(run_id)
            previous_stream_session_id = str(self._run_stream_session_id.get(key_run) or "").strip() or None
            next_stream_session_id = self._new_stream_session_id()
            self._run_stream_session_id[key_run] = next_stream_session_id
            ring_keys = [key for key in self._series_live_tail_ring.keys() if key[0] == key_run]
            for key in ring_keys:
                self._series_live_tail_ring[key].clear()
            for key, viewers in list(self._series_viewers.items()):
                if key[0] != key_run:
                    continue
                for ws, state in list(viewers.items()):
                    state["invalidated"] = True
                    targets.append((key, ws))

        logger.warning(
            "bot_telemetry_run_stream_invalidated | run_id=%s | reason=%s | viewer_count=%s | previous_stream_session_id=%s | next_stream_session_id=%s",
            run_id,
            reason,
            len(targets),
            previous_stream_session_id,
            next_stream_session_id,
        )

        for key, ws in targets:
            try:
                await ws.send_text(
                    json.dumps(
                        self._resync_required_envelope(
                            run_id=key[0],
                            series_key=key[1],
                            reason=reason,
                            stream_session_id=next_stream_session_id,
                            previous_stream_session_id=previous_stream_session_id,
                            details=details,
                        )
                    )
                )
            except Exception:
                pass
            try:
                await ws.close(code=1013)
            except Exception:
                pass
            await self.remove_series_viewer(run_id=key[0], series_key=key[1], ws=ws)

    async def _ensure_workers(self) -> None:
        async with self._worker_lock:
            if self._ingest_task is None or self._ingest_task.done():
                self._ingest_task = asyncio.create_task(self._ingest_worker_loop(), name="bot-telemetry-ingest-worker")

    async def _ingest_worker_loop(self) -> None:
        while True:
            item = await self._ingest_queue.get()
            try:
                await self._process_ingest(item)
            except Exception as exc:  # noqa: BLE001
                logger.exception("bot_telemetry_ingest_worker_failed | error=%s", exc)
            finally:
                self._ingest_queue.task_done()

    async def ingest(self, payload: Dict[str, Any]) -> None:
        await self._ensure_workers()
        item = {"payload": dict(payload) if isinstance(payload, AbcMapping) else {}, "enqueued_monotonic": time.monotonic()}
        try:
            self._ingest_queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.warning(
                "bot_telemetry_ingest_queue_backpressure | queue_depth=%s | queue_max=%s",
                self._ingest_queue.qsize(),
                _INGEST_QUEUE_MAX,
            )
            await self._ingest_queue.put(item)

    async def _publish_runtime_update(self, *, bot_id: str, run_id: str, runtime_payload: Mapping[str, Any], seq: int, known_at: Any) -> None:
        try:
            await asyncio.to_thread(
                publish_runtime_update,
                bot_id,
                {
                    **dict(runtime_payload or {}),
                    "status": str(runtime_payload.get("status") or "running"),
                    "run_id": run_id,
                    "seq": seq,
                    "known_at": known_at,
                    "last_snapshot_at": known_at,
                    "warnings": list(runtime_payload.get("warnings") or []),
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bot_telemetry_runtime_broadcast_failed | bot_id=%s | run_id=%s | seq=%s | error=%s",
                bot_id,
                run_id,
                seq,
                exc,
            )

    @staticmethod
    def _live_delta_envelope(
        *,
        run_id: str,
        series_key: str,
        seq: int,
        known_at: Any,
        runtime_delta: Mapping[str, Any],
    ) -> Dict[str, Any]:
        series_entries = runtime_delta.get("series") if isinstance(runtime_delta.get("series"), list) else []
        series_delta = None
        for entry in series_entries:
            if not isinstance(entry, AbcMapping):
                continue
            series_delta = dict(entry)
            break
        return {
            "type": "botlens_live_tail",
            "run_id": str(run_id),
            "series_key": str(series_key),
            "schema_version": _SCHEMA_VERSION,
            "seq": int(seq),
            "known_at": known_at,
            "message_type": "series_delta",
            "payload": {
                "event": str(runtime_delta.get("event") or ""),
                "runtime": _sanitize_json(runtime_delta.get("runtime") or {}),
                "stats": _sanitize_json(runtime_delta.get("stats") or {}),
                "logs": _sanitize_json(runtime_delta.get("logs") or []),
                "decisions": _sanitize_json(runtime_delta.get("decisions") or []),
                "series_delta": _sanitize_json(series_delta or {}),
            },
        }

    async def _persist_series_projection(
        self,
        *,
        bot_id: str,
        run_id: str,
        series_key: str,
        run_seq: int,
        series_seq: int,
        event_type: str,
        projection: Mapping[str, Any],
        runtime_delta: Optional[Mapping[str, Any]],
        event_time: Any,
        known_at: Any,
    ) -> None:
        view_state_row = {
            "run_id": run_id,
            "bot_id": bot_id,
            "series_key": series_key,
            "seq": series_seq,
            "schema_version": _SCHEMA_VERSION,
            "payload": dict(projection or {}),
            "event_time": event_time,
            "known_at": known_at,
            "updated_at": known_at,
        }
        await asyncio.to_thread(upsert_bot_run_view_state, view_state_row)
        event_payload: Dict[str, Any] = {
            "series_key": series_key,
            "series_seq": series_seq,
        }
        if event_type == BOTLENS_SERIES_BOOTSTRAP:
            event_payload["projection"] = dict(projection or {})
        if isinstance(runtime_delta, AbcMapping):
            event_payload["runtime_delta"] = dict(runtime_delta)
        await asyncio.to_thread(
            record_bot_runtime_event,
            {
                "event_id": f"{bot_id}:{run_id}:{event_type}:{run_seq}",
                "bot_id": bot_id,
                "run_id": run_id,
                "seq": run_seq,
                "event_type": event_type,
                "critical": bool(event_type == BOTLENS_SERIES_BOOTSTRAP),
                "schema_version": _SCHEMA_VERSION,
                "event_time": event_time,
                "known_at": known_at,
                "payload": event_payload,
            },
        )

    async def _process_series_bootstrap(self, payload: Mapping[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        series_key = normalize_series_key(payload.get("series_key"))
        run_seq = _coerce_int(payload.get("run_seq"), default=0)
        series_seq = _coerce_int(payload.get("series_seq"), default=0)
        if not bot_id or not run_id or not series_key or run_seq <= 0 or series_seq <= 0:
            logger.warning(
                "bot_telemetry_bootstrap_invalid_payload | bot_id=%s | run_id=%s | series_key=%s | run_seq=%s | series_seq=%s",
                bot_id,
                run_id,
                series_key,
                run_seq,
                series_seq,
            )
            return
        projection_raw = payload.get("projection") if isinstance(payload.get("projection"), AbcMapping) else {}
        projection = canonicalize_projection(projection_raw)
        known_at = payload.get("known_at") or payload.get("event_time")
        event_time = payload.get("event_time") or known_at
        async with self._lock:
            self._latest_view_state[(bot_id, run_id, series_key)] = {
                "run_id": run_id,
                "bot_id": bot_id,
                "series_key": series_key,
                "seq": series_seq,
                "schema_version": _SCHEMA_VERSION,
                "payload": projection,
                "event_time": event_time,
                "known_at": known_at,
            }
            self._latest_run_by_bot[bot_id] = run_id
        await self._persist_series_projection(
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
            run_seq=run_seq,
            series_seq=series_seq,
            event_type=BOTLENS_SERIES_BOOTSTRAP,
            projection=projection,
            runtime_delta=None,
            event_time=event_time,
            known_at=known_at,
        )
        runtime_payload = projection.get("runtime") if isinstance(projection.get("runtime"), AbcMapping) else {}
        if runtime_payload:
            await self._publish_runtime_update(
                bot_id=bot_id,
                run_id=run_id,
                runtime_payload=runtime_payload,
                seq=series_seq,
                known_at=known_at,
            )

    async def _process_series_delta(self, payload: Mapping[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        series_key = normalize_series_key(payload.get("series_key"))
        run_seq = _coerce_int(payload.get("run_seq"), default=0)
        series_seq = _coerce_int(payload.get("series_seq"), default=0)
        runtime_delta = payload.get("runtime_delta") if isinstance(payload.get("runtime_delta"), AbcMapping) else {}
        if not bot_id or not run_id or not series_key or run_seq <= 0 or series_seq <= 0:
            logger.warning(
                "bot_telemetry_delta_invalid_payload | bot_id=%s | run_id=%s | series_key=%s | run_seq=%s | series_seq=%s",
                bot_id,
                run_id,
                series_key,
                run_seq,
                series_seq,
            )
            return
        key = (bot_id, run_id, series_key)
        async with self._lock:
            previous = self._latest_view_state.get(key)
        if previous is None:
            previous = await asyncio.to_thread(
                get_latest_bot_run_view_state,
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
            )
        previous_seq = _coerce_int((previous or {}).get("seq"), default=0)
        if previous_seq >= series_seq:
            logger.debug(
                "bot_telemetry_ingest_stale_view_state_ignored | bot_id=%s | run_id=%s | series_key=%s | incoming_seq=%s | latest_seq=%s",
                bot_id,
                run_id,
                series_key,
                series_seq,
                previous_seq,
            )
            return
        expected_next_seq = previous_seq + 1 if previous_seq > 0 else series_seq
        seq_gap = max(0, series_seq - expected_next_seq)
        if previous_seq > 0 and seq_gap > 0:
            logger.warning(
                "bot_telemetry_seq_gap_detected | bot_id=%s | run_id=%s | series_key=%s | previous_seq=%s | incoming_seq=%s | seq_gap=%s | action=resync_required",
                bot_id,
                run_id,
                series_key,
                previous_seq,
                series_seq,
                seq_gap,
            )
            await self._invalidate_run_live_continuity(
                run_id=run_id,
                reason="seq_gap",
                details={
                    "series_key": series_key,
                    "previous_seq": previous_seq,
                    "incoming_seq": series_seq,
                    "seq_gap": seq_gap,
                },
            )
        previous_projection = (previous or {}).get("payload") if isinstance((previous or {}).get("payload"), AbcMapping) else {}
        next_projection = apply_series_runtime_delta(
            previous_projection,
            series_key=series_key,
            seq=series_seq,
            runtime_delta=runtime_delta,
        )
        known_at = payload.get("known_at") or payload.get("event_time")
        event_time = payload.get("event_time") or known_at
        async with self._lock:
            self._latest_view_state[key] = {
                "run_id": run_id,
                "bot_id": bot_id,
                "series_key": series_key,
                "seq": series_seq,
                "schema_version": _SCHEMA_VERSION,
                "payload": next_projection,
                "event_time": event_time,
                "known_at": known_at,
            }
            self._latest_run_by_bot[bot_id] = run_id
        await self._persist_series_projection(
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
            run_seq=run_seq,
            series_seq=series_seq,
            event_type=BOTLENS_SERIES_DELTA,
            projection=next_projection,
            runtime_delta=runtime_delta,
            event_time=event_time,
            known_at=known_at,
        )
        runtime_payload = next_projection.get("runtime") if isinstance(next_projection.get("runtime"), AbcMapping) else {}
        if runtime_payload:
            await self._publish_runtime_update(
                bot_id=bot_id,
                run_id=run_id,
                runtime_payload=runtime_payload,
                seq=series_seq,
                known_at=known_at,
            )
        await self._broadcast_series_delta(
            run_id=run_id,
            series_key=series_key,
            seq=series_seq,
            known_at=known_at,
            runtime_delta=runtime_delta,
        )

    async def _process_ingest(self, item: Dict[str, Any]) -> None:
        payload = item.get("payload")
        if not isinstance(payload, AbcMapping):
            return
        kind = str(payload.get("kind") or "").strip().lower()
        if kind == "botlens_series_bootstrap":
            await self._process_series_bootstrap(payload)
            return
        if kind == "botlens_series_delta":
            await self._process_series_delta(payload)
            return
        logger.warning("bot_telemetry_ingest_unknown_kind | kind=%s", kind)

    async def add_series_viewer(
        self,
        *,
        run_id: str,
        series_key: str,
        ws: WebSocket,
        limit: int = 320,
    ) -> None:
        await self._ensure_workers()
        await ws.accept()
        key = (str(run_id), normalize_series_key(series_key))
        try:
            window = await asyncio.to_thread(
                get_series_window,
                run_id=str(run_id),
                series_key=str(series_key),
                to="now",
                limit=max(1, min(int(limit or 320), 2000)),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bot_telemetry_series_viewer_bootstrap_failed | run_id=%s | series_key=%s | error=%s",
                key[0],
                key[1],
                exc,
            )
            try:
                await ws.send_text(
                    json.dumps(
                        self._series_error_envelope(
                            run_id=key[0],
                            series_key=key[1],
                            message=str(exc),
                        )
                    )
                )
            finally:
                await ws.close(code=1011)
            return

        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(key[0])
            cursor = _coerce_int(window.get("seq"), default=0)
            baseline_seq = cursor
            bootstrap = self._series_bootstrap_envelope({**dict(window), "stream_session_id": stream_session_id})
            self._series_viewers[key][ws] = {"last_seq": cursor, "replaying": True}

        try:
            await ws.send_text(json.dumps(bootstrap))
        except Exception:
            await self.remove_series_viewer(run_id=str(run_id), series_key=str(series_key), ws=ws)
            return

        replayed_messages = 0
        while True:
            async with self._lock:
                slot = self._series_viewers.get(key, {}).get(ws)
                if slot is None:
                    return
                buffered = [
                    dict(message)
                    for message in self._series_live_tail_ring.get(key, ())
                    if _coerce_int(message.get("seq"), default=0) > cursor
                ]
                ring_depth = len(self._series_live_tail_ring.get(key, ()))
                if not buffered:
                    slot["last_seq"] = cursor
                    slot["replaying"] = False
                    logger.debug(
                        "bot_telemetry_series_viewer_replay_done | run_id=%s | series_key=%s | baseline_seq=%s | last_seq=%s | replayed_messages=%s | ring_depth=%s",
                        key[0],
                        key[1],
                        baseline_seq,
                        cursor,
                        replayed_messages,
                        ring_depth,
                    )
                    return

            first_seq = _coerce_int(buffered[0].get("seq"), default=0)
            if cursor > 0 and first_seq > cursor + 1:
                logger.warning(
                    "bot_telemetry_series_viewer_replay_buffer_miss | run_id=%s | series_key=%s | cursor_seq=%s | first_buffered_seq=%s | ring_depth=%s | ring_size=%s",
                    key[0],
                    key[1],
                    cursor,
                    first_seq,
                    ring_depth,
                    _RING_SIZE,
                )
                try:
                    await ws.send_text(
                        json.dumps(
                            self._resync_required_envelope(
                                run_id=key[0],
                                series_key=key[1],
                                reason="replay_buffer_miss",
                                stream_session_id=stream_session_id,
                                details={
                                    "baseline_seq": baseline_seq,
                                    "cursor_seq": cursor,
                                    "first_buffered_seq": first_seq,
                                    "ring_depth": ring_depth,
                                    "ring_size": _RING_SIZE,
                                },
                            )
                        )
                    )
                finally:
                    await self.remove_series_viewer(run_id=str(run_id), series_key=str(series_key), ws=ws)
                    try:
                        await ws.close(code=1013)
                    except Exception:
                        pass
                return

            logger.debug(
                "bot_telemetry_series_viewer_replay_start | run_id=%s | series_key=%s | cursor_seq=%s | replay_count=%s | first_seq=%s | last_seq=%s",
                key[0],
                key[1],
                cursor,
                len(buffered),
                first_seq,
                _coerce_int(buffered[-1].get("seq"), default=0),
            )
            try:
                for message in buffered:
                    await ws.send_text(json.dumps(message))
            except Exception:
                await self.remove_series_viewer(run_id=str(run_id), series_key=str(series_key), ws=ws)
                return

            cursor = max(cursor, max(_coerce_int(message.get("seq"), default=0) for message in buffered))
            replayed_messages += len(buffered)
            async with self._lock:
                slot = self._series_viewers.get(key, {}).get(ws)
                if slot is None:
                    return
                slot["last_seq"] = cursor

    async def remove_series_viewer(self, *, run_id: str, series_key: str, ws: WebSocket) -> None:
        key = (str(run_id), normalize_series_key(series_key))
        async with self._lock:
            viewers = self._series_viewers.get(key)
            if not viewers:
                return
            viewers.pop(ws, None)
            if not viewers:
                self._series_viewers.pop(key, None)

    async def _broadcast_series_delta(
        self,
        *,
        run_id: str,
        series_key: str,
        seq: int,
        known_at: Any,
        runtime_delta: Mapping[str, Any],
    ) -> None:
        key = (str(run_id), normalize_series_key(series_key))
        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(str(run_id))
            message = {
                **self._live_delta_envelope(
                    run_id=str(run_id),
                    series_key=str(series_key),
                    seq=int(seq),
                    known_at=known_at,
                    runtime_delta=runtime_delta,
                ),
                "stream_session_id": stream_session_id,
            }
            self._series_live_tail_ring[key].append(message)
            viewers = list(self._series_viewers.get(key, {}).items())

        for ws, state in viewers:
            last_seq = int(state.get("last_seq") or 0)
            if state.get("replaying") or state.get("invalidated"):
                continue
            if int(seq) <= last_seq:
                continue
            try:
                await ws.send_text(json.dumps(message))
            except Exception:
                await self.remove_series_viewer(run_id=str(run_id), series_key=str(series_key), ws=ws)
                continue
            async with self._lock:
                slot = self._series_viewers.get(key, {}).get(ws)
                if slot is not None and not slot.get("replaying"):
                    slot["last_seq"] = int(seq)


telemetry_hub = BotTelemetryHub()
