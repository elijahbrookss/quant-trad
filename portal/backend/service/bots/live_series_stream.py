"""BotLens live series continuity, replay, and fanout state."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import defaultdict, deque
from collections.abc import Mapping as AbcMapping
from typing import Any, DefaultDict, Dict, Optional, Tuple

from fastapi import WebSocket

from .botlens_series_service import get_series_window

logger = logging.getLogger(__name__)


class LiveSeriesStream:
    def __init__(self, *, ring_size: int, schema_version: int, sanitize_json, coerce_int) -> None:
        self._ring_size = int(ring_size)
        self._schema_version = int(schema_version)
        self._sanitize_json = sanitize_json
        self._coerce_int = coerce_int
        self._series_viewers: DefaultDict[Tuple[str, str], Dict[WebSocket, Dict[str, Any]]] = defaultdict(dict)
        self._series_live_tail_ring: DefaultDict[Tuple[str, str], deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self._ring_size)
        )
        self._run_stream_session_id: Dict[str, str] = {}
        self._lock = asyncio.Lock()

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

    def series_bootstrap_envelope(self, window: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "type": "botlens_live_bootstrap",
            "run_id": str(window.get("run_id") or ""),
            "series_key": str(window.get("series_key") or ""),
            "schema_version": int(window.get("schema_version") or self._schema_version),
            "seq": self._coerce_int(window.get("seq"), default=0),
            "event_time": window.get("event_time"),
            "stream_session_id": str(window.get("stream_session_id") or ""),
            "payload": {
                "cursor": self._sanitize_json(window.get("cursor") or {}),
                "continuity": self._sanitize_json(window.get("continuity") or {}),
                "lifecycle": self._sanitize_json(window.get("lifecycle") or {}),
                "window": self._sanitize_json(window.get("window") or {}),
            },
        }

    @staticmethod
    def series_error_envelope(*, run_id: str, series_key: str, message: str) -> Dict[str, Any]:
        return {
            "type": "botlens_live_error",
            "run_id": str(run_id),
            "series_key": str(series_key),
            "payload": {"message": str(message or "BotLens stream failed")},
        }

    def resync_required_envelope(
        self,
        *,
        run_id: str,
        series_key: str,
        reason: str,
        stream_session_id: str,
        previous_stream_session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = {"reason": str(reason or "continuity_lost"), "details": self._sanitize_json(details or {})}
        if previous_stream_session_id:
            payload["previous_stream_session_id"] = str(previous_stream_session_id)
        return {
            "type": "botlens_live_resync_required",
            "run_id": str(run_id),
            "series_key": str(series_key),
            "stream_session_id": str(stream_session_id or ""),
            "payload": payload,
        }

    def projection_update_envelope(self, *, window: Dict[str, Any], stream_session_id: str) -> Dict[str, Any]:
        return {
            "type": "botlens_live_tail",
            "run_id": str(window.get("run_id") or ""),
            "series_key": str(window.get("series_key") or ""),
            "schema_version": int(window.get("schema_version") or self._schema_version),
            "seq": self._coerce_int(window.get("seq"), default=0),
            "event_time": window.get("event_time"),
            "stream_session_id": str(stream_session_id or ""),
            "message_type": "projection_update",
            "payload": {
                "cursor": self._sanitize_json(window.get("cursor") or {}),
                "continuity": self._sanitize_json(window.get("continuity") or {}),
                "lifecycle": self._sanitize_json(window.get("lifecycle") or {}),
                "window": self._sanitize_json(window.get("window") or {}),
            },
        }

    def lifecycle_update_envelope(
        self,
        *,
        run_id: str,
        series_key: str,
        lifecycle: Dict[str, Any],
        seq: int,
        stream_session_id: str,
    ) -> Dict[str, Any]:
        return {
            "type": "botlens_live_tail",
            "run_id": str(run_id),
            "series_key": str(series_key),
            "schema_version": self._schema_version,
            "seq": int(seq),
            "stream_session_id": str(stream_session_id or ""),
            "message_type": "lifecycle_update",
            "payload": {
                "lifecycle": self._sanitize_json(lifecycle or {}),
            },
        }

    async def invalidate_run_live_continuity(
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
                        self.resync_required_envelope(
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

    async def add_series_viewer(self, *, run_id: str, series_key: str, ws: WebSocket, limit: int = 320) -> None:
        await ws.accept()
        key = (str(run_id), str(series_key))
        logger.info(
            "bot_telemetry_series_viewer_subscribe_started | run_id=%s | series_key=%s | limit=%s",
            key[0],
            key[1],
            max(1, min(int(limit or 320), 2000)),
        )
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
                        self.series_error_envelope(
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
            cursor = self._coerce_int(window.get("seq"), default=0)
            baseline_seq = cursor
            bootstrap = self.series_bootstrap_envelope({**dict(window), "stream_session_id": stream_session_id})
            self._series_viewers[key][ws] = {"last_seq": cursor, "replaying": True}
            viewer_count = len(self._series_viewers.get(key, {}))

        try:
            await ws.send_text(json.dumps(bootstrap))
        except Exception:
            await self.remove_series_viewer(run_id=str(run_id), series_key=str(series_key), ws=ws)
            return
        logger.info(
            "bot_telemetry_series_viewer_bootstrap_sent | run_id=%s | series_key=%s | seq=%s | stream_session_id=%s | continuity_status=%s | viewer_count=%s",
            key[0],
            key[1],
            cursor,
            stream_session_id,
            str((window.get("continuity") or {}).get("status") or "").strip() or "unknown",
            viewer_count,
        )

        replayed_messages = 0
        while True:
            async with self._lock:
                slot = self._series_viewers.get(key, {}).get(ws)
                if slot is None:
                    return
                buffered = [
                    dict(message)
                    for message in self._series_live_tail_ring.get(key, ())
                    if self._coerce_int(message.get("seq"), default=0) > cursor
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

            first_seq = self._coerce_int(buffered[0].get("seq"), default=0)
            if cursor > 0 and first_seq > cursor + 1:
                logger.warning(
                    "bot_telemetry_series_viewer_replay_buffer_miss | run_id=%s | series_key=%s | cursor_seq=%s | first_buffered_seq=%s | ring_depth=%s | ring_size=%s",
                    key[0],
                    key[1],
                    cursor,
                    first_seq,
                    ring_depth,
                    self._ring_size,
                )
                try:
                    await ws.send_text(
                        json.dumps(
                            self.resync_required_envelope(
                                run_id=key[0],
                                series_key=key[1],
                                reason="replay_buffer_miss",
                                stream_session_id=stream_session_id,
                                details={
                                    "baseline_seq": baseline_seq,
                                    "cursor_seq": cursor,
                                    "first_buffered_seq": first_seq,
                                    "ring_depth": ring_depth,
                                    "ring_size": self._ring_size,
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

            try:
                for message in buffered:
                    await ws.send_text(json.dumps(message))
            except Exception:
                await self.remove_series_viewer(run_id=str(run_id), series_key=str(series_key), ws=ws)
                return

            cursor = max(cursor, max(self._coerce_int(message.get("seq"), default=0) for message in buffered))
            replayed_messages += len(buffered)
            async with self._lock:
                slot = self._series_viewers.get(key, {}).get(ws)
                if slot is None:
                    return
                slot["last_seq"] = cursor

    async def remove_series_viewer(self, *, run_id: str, series_key: str, ws: WebSocket) -> None:
        key = (str(run_id), str(series_key))
        removed = False
        remaining = 0
        async with self._lock:
            viewers = self._series_viewers.get(key)
            if not viewers:
                return
            removed = viewers.pop(ws, None) is not None
            if not viewers:
                self._series_viewers.pop(key, None)
                remaining = 0
            else:
                remaining = len(viewers)
        if removed:
            logger.info(
                "bot_telemetry_series_viewer_removed | run_id=%s | series_key=%s | remaining_viewers=%s",
                key[0],
                key[1],
                remaining,
            )

    async def broadcast_projection_update(self, *, window: Dict[str, Any]) -> None:
        run_id = str(window.get("run_id") or "")
        series_key = str(window.get("series_key") or "")
        key = (run_id, series_key)
        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(run_id)
            message = self.projection_update_envelope(window=window, stream_session_id=stream_session_id)
            self._series_live_tail_ring[key].append(message)
            viewers = list(self._series_viewers.get(key, {}).items())
            ring_depth = len(self._series_live_tail_ring.get(key, ()))

        logger.debug(
            "bot_telemetry_series_projection_update_fanout | run_id=%s | series_key=%s | seq=%s | stream_session_id=%s | viewer_count=%s | ring_depth=%s",
            run_id,
            series_key,
            self._coerce_int(message.get("seq"), default=0),
            stream_session_id,
            len(viewers),
            ring_depth,
        )

        for ws, state in viewers:
            last_seq = int(state.get("last_seq") or 0)
            if state.get("replaying") or state.get("invalidated"):
                continue
            if self._coerce_int(message.get("seq"), default=0) <= last_seq:
                continue
            try:
                await ws.send_text(json.dumps(message))
            except Exception:
                await self.remove_series_viewer(run_id=run_id, series_key=series_key, ws=ws)
                continue
            async with self._lock:
                slot = self._series_viewers.get(key, {}).get(ws)
                if slot is not None and not slot.get("replaying"):
                    slot["last_seq"] = self._coerce_int(message.get("seq"), default=0)

    async def broadcast_run_lifecycle(self, *, run_id: str, lifecycle: Dict[str, Any]) -> None:
        targets: list[tuple[Tuple[str, str], WebSocket, Dict[str, Any], str]] = []
        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(str(run_id))
            for key, viewers in list(self._series_viewers.items()):
                if key[0] != str(run_id):
                    continue
                for ws, state in list(viewers.items()):
                    targets.append((key, ws, dict(state), stream_session_id))

        logger.debug(
            "bot_telemetry_run_lifecycle_fanout | run_id=%s | stream_session_id=%s | viewer_count=%s | phase=%s | status=%s",
            run_id,
            stream_session_id,
            len(targets),
            str(lifecycle.get("phase") or "").strip(),
            str(lifecycle.get("status") or "").strip(),
        )

        for key, ws, state, stream_session_id in targets:
            if state.get("replaying") or state.get("invalidated"):
                continue
            try:
                message = self.lifecycle_update_envelope(
                    run_id=key[0],
                    series_key=key[1],
                    lifecycle=lifecycle,
                    seq=int(state.get("last_seq") or 0),
                    stream_session_id=stream_session_id,
                )
                await ws.send_text(json.dumps(message))
            except Exception:
                await self.remove_series_viewer(run_id=key[0], series_key=key[1], ws=ws)


__all__ = ["LiveSeriesStream"]
