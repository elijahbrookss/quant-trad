from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict
from collections import deque
from collections.abc import Mapping
from typing import Any, DefaultDict, Deque, Dict

from core.settings import get_settings

try:
    from fastapi import WebSocket
except ModuleNotFoundError:  # pragma: no cover - test environments may not install FastAPI
    class WebSocket:  # type: ignore[override]
        pass

from ..observability import BackendObserver, normalize_failure_mode, payload_size_bytes
from .botlens_contract import STREAM_SYMBOL_DELTA_TYPES, normalize_series_key
from .botlens_transport import (
    BotLensTransport,
    LiveDeliveryStats,
    PreparedLiveDelta,
    stream_connected_message,
    stream_reset_required_message,
)

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_run_stream", event_logger=logger)
_BOTLENS_SETTINGS = get_settings().bot_runtime.botlens


class BotLensRunStream:
    def __init__(self, *, ring_size: int | None = None) -> None:
        self.transport = BotLensTransport()
        self._run_viewers: DefaultDict[str, Dict[WebSocket, Dict[str, Any]]] = defaultdict(dict)
        self._run_stream_session_id: Dict[str, str] = {}
        self._run_stream_seq: Dict[str, int] = {}
        self._run_scope_seq: Dict[str, int] = {}
        self._run_symbol_scope_seq: DefaultDict[str, Dict[str, int]] = defaultdict(dict)
        self._run_bot_ids: Dict[str, str] = {}
        self._ring_size = max(int(ring_size or _BOTLENS_SETTINGS.ring_size), 1)
        self._run_message_ring: Dict[str, Deque[Dict[str, Any]]] = {}
        self._run_ring_high_water: Dict[str, int] = {}
        self._run_max_requested_gap: Dict[str, int] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _new_stream_session_id() -> str:
        return uuid.uuid4().hex

    def _ensure_run_stream_session_id_locked(self, run_id: str) -> str:
        key = str(run_id)
        current = str(self._run_stream_session_id.get(key) or "").strip()
        if current:
            return current
        current = self._new_stream_session_id()
        self._run_stream_session_id[key] = current
        self._run_stream_seq[key] = 0
        return current

    def _next_stream_seq_locked(self, run_id: str) -> int:
        key = str(run_id)
        current = int(self._run_stream_seq.get(key) or 0) + 1
        self._run_stream_seq[key] = current
        return current

    def _message_ring_locked(self, run_id: str) -> Deque[Dict[str, Any]]:
        key = str(run_id)
        ring = self._run_message_ring.get(key)
        if ring is not None:
            return ring
        ring = deque(maxlen=self._ring_size)
        self._run_message_ring[key] = ring
        return ring

    def _append_replay_message_locked(self, *, run_id: str, message: Mapping[str, Any]) -> None:
        key = str(run_id)
        ring = self._message_ring_locked(key)
        ring.append(dict(message))
        occupancy = len(ring)
        high_water = max(int(self._run_ring_high_water.get(key) or 0), occupancy)
        self._run_ring_high_water[key] = high_water
        bot_id = self.bot_id_for_run(key)
        _OBSERVER.maybe_emit_gauges(
            f"replay_ring:{key}",
            depth_metric="replay_ring_occupancy",
            utilization_metric="replay_ring_utilization",
            depth=occupancy,
            capacity=self._ring_size,
            bot_id=bot_id,
            run_id=key,
        )
        _OBSERVER.gauge(
            "replay_ring_high_water_mark",
            float(high_water),
            bot_id=bot_id,
            run_id=key,
        )

    def _current_cursor_locked(self, run_id: str) -> Dict[str, Any]:
        key = str(run_id)
        return {
            "stream_session_id": self._ensure_run_stream_session_id_locked(key),
            "base_seq": int(self._run_stream_seq.get(key) or 0),
        }

    def _current_symbol_cursor_locked(self, run_id: str, symbol_key: str | None = None) -> Dict[str, Any]:
        key = str(run_id)
        cursor = dict(self._current_cursor_locked(key))
        normalized_symbol_key = normalize_series_key(symbol_key)
        cursor["run_scope_seq"] = int(self._run_scope_seq.get(key) or 0)
        cursor["symbol_scope_seq"] = int(
            self._run_symbol_scope_seq.get(key, {}).get(normalized_symbol_key, 0)
        ) if normalized_symbol_key else 0
        return cursor

    def _resume_messages_locked(
        self,
        *,
        run_id: str,
        requested_stream_session_id: str | None,
        resume_from_seq: int,
    ) -> tuple[str | None, list[Dict[str, Any]], Dict[str, Any]]:
        key = str(run_id)
        cursor = self._current_cursor_locked(key)
        current_stream_session_id = str(cursor["stream_session_id"])
        current_stream_seq = int(cursor["base_seq"])
        requested_seq = max(int(resume_from_seq or 0), 0)
        requested_session = str(requested_stream_session_id or "").strip() or None
        requested_gap = max(current_stream_seq - requested_seq, 0)
        self._run_max_requested_gap[key] = max(int(self._run_max_requested_gap.get(key) or 0), requested_gap)
        if requested_session and requested_session != current_stream_session_id:
            return (
                "stream_session_mismatch",
                [],
                {
                    "stream_session_id": current_stream_session_id,
                    "base_seq": current_stream_seq,
                    "requested_stream_session_id": requested_session,
                    "requested_resume_from_seq": requested_seq,
                    "requested_gap": requested_gap,
                },
            )
        if requested_seq > current_stream_seq:
            return (
                "resume_seq_ahead_of_stream",
                [],
                {
                    "stream_session_id": current_stream_session_id,
                    "base_seq": current_stream_seq,
                    "requested_stream_session_id": requested_session,
                    "requested_resume_from_seq": requested_seq,
                    "requested_gap": requested_gap,
                },
            )
        ring = self._message_ring_locked(key)
        if requested_seq < current_stream_seq:
            if not ring:
                return (
                    "resume_window_expired",
                    [],
                    {
                        "stream_session_id": current_stream_session_id,
                        "base_seq": current_stream_seq,
                        "requested_stream_session_id": requested_session,
                        "requested_resume_from_seq": requested_seq,
                        "requested_gap": requested_gap,
                    },
                )
            earliest_seq = int(ring[0].get("stream_seq") or 0)
            if requested_seq < max(earliest_seq - 1, 0):
                return (
                    "resume_window_expired",
                    [],
                    {
                        "stream_session_id": current_stream_session_id,
                        "base_seq": current_stream_seq,
                        "requested_stream_session_id": requested_session,
                        "requested_resume_from_seq": requested_seq,
                        "requested_gap": requested_gap,
                    },
                )
        messages = [dict(message) for message in ring if int(message.get("stream_seq") or 0) > requested_seq]
        return None, messages, {
            "stream_session_id": current_stream_session_id,
            "base_seq": current_stream_seq,
            "requested_stream_session_id": requested_session,
            "requested_resume_from_seq": requested_seq,
            "requested_gap": requested_gap,
        }

    def bind_run(self, *, run_id: str, bot_id: str) -> None:
        key = str(run_id)
        value = str(bot_id or "").strip()
        if not key or not value:
            return
        self._run_bot_ids[key] = value

    def bot_id_for_run(self, run_id: str) -> str | None:
        value = str(self._run_bot_ids.get(str(run_id)) or "").strip()
        return value or None

    def _emit_viewer_gauges_locked(self, *, run_id: str) -> None:
        bot_id = self.bot_id_for_run(run_id)
        viewer_count = len(self._run_viewers.get(str(run_id), {}))
        _OBSERVER.maybe_gauge(
            f"viewer_active:{run_id}",
            "viewer_active_count",
            float(viewer_count),
            bot_id=bot_id,
            run_id=run_id,
        )

    async def _send_message(
        self,
        ws: WebSocket,
        message: str,
        *,
        run_id: str,
        message_kind: str,
        series_key: str | None = None,
    ) -> bool:
        started = time.perf_counter()
        bot_id = self.bot_id_for_run(run_id)
        _OBSERVER.increment(
            "viewer_send_total",
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
            message_kind=message_kind,
        )
        try:
            await ws.send_text(message)
            _OBSERVER.observe(
                "viewer_send_ms",
                max((time.perf_counter() - started) * 1000.0, 0.0),
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
                message_kind=message_kind,
            )
            return True
        except Exception as exc:
            failure_mode = normalize_failure_mode(exc)
            _OBSERVER.increment(
                "viewer_send_fail_total",
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
                message_kind=message_kind,
                failure_mode=failure_mode,
            )
            _OBSERVER.observe(
                "viewer_send_ms",
                max((time.perf_counter() - started) * 1000.0, 0.0),
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
                message_kind=message_kind,
                failure_mode=failure_mode,
            )
            _OBSERVER.event(
                "viewer_send_failed",
                level=logging.WARN,
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
                message_kind=message_kind,
                failure_mode=failure_mode,
                error=str(exc),
            )
            return False

    @staticmethod
    def _viewer_wants_symbol(viewer_state: Mapping[str, Any], symbol_key: str) -> bool:
        normalized_symbol = normalize_series_key(symbol_key)
        if not normalized_symbol:
            return False
        selected = normalize_series_key(viewer_state.get("selected_symbol_key"))
        return bool(selected) and selected == normalized_symbol

    async def add_run_viewer(
        self,
        *,
        run_id: str,
        ws: WebSocket,
        selected_symbol_key: str | None = None,
        resume_from_seq: int = 0,
        stream_session_id: str | None = None,
    ) -> None:
        await ws.accept()
        key = str(run_id)
        normalized_selected = normalize_series_key(selected_symbol_key)
        replayed_count = 0

        async with self._lock:
            reason, replay_messages, cursor = self._resume_messages_locked(
                run_id=key,
                requested_stream_session_id=stream_session_id,
                resume_from_seq=resume_from_seq,
            )
            resolved_stream_session_id = str(cursor["stream_session_id"])
            viewer_count = len(self._run_viewers[key]) + 1
            self._run_viewers[key][ws] = {
                "selected_symbol_key": normalized_selected,
                "ready": False,
            }
            self._emit_viewer_gauges_locked(run_id=key)
            requested_gap = int(cursor.get("requested_gap") or 0)
            bot_id = self.bot_id_for_run(key)
            if requested_gap > 0:
                _OBSERVER.observe(
                    "replay_requested_gap",
                    float(requested_gap),
                    bot_id=bot_id,
                    run_id=key,
                )
            _OBSERVER.gauge(
                "replay_requested_gap_max",
                float(self._run_max_requested_gap.get(key) or 0),
                bot_id=bot_id,
                run_id=key,
            )

        if reason:
            _OBSERVER.increment(
                "replay_miss_total",
                bot_id=bot_id,
                run_id=key,
                failure_mode=reason,
            )
            _OBSERVER.increment(
                "reset_required_total",
                bot_id=bot_id,
                run_id=key,
                failure_mode=reason,
            )
            reset_message = json.dumps(
                stream_reset_required_message(
                    run_id=key,
                    stream_session_id=resolved_stream_session_id,
                    reason=reason,
                    requested_stream_session_id=cursor.get("requested_stream_session_id"),
                    requested_resume_from_seq=int(cursor.get("requested_resume_from_seq") or 0),
                    current_stream_seq=int(cursor.get("base_seq") or 0),
                )
            )
            sent = await self._send_message(
                ws,
                reset_message,
                run_id=key,
                message_kind="reset_required",
            )
            await self.remove_run_viewer(run_id=key, ws=ws)
            if sent:
                try:
                    await ws.close(code=1012)
                except Exception:
                    pass
            return
        if requested_gap > 0:
            _OBSERVER.increment(
                "replay_hit_total",
                bot_id=bot_id,
                run_id=key,
            )
            _OBSERVER.observe(
                "replay_message_count",
                float(len(replay_messages)),
                bot_id=bot_id,
                run_id=key,
            )

        connected_message = json.dumps(
            stream_connected_message(
                run_id=key,
                stream_session_id=resolved_stream_session_id,
                replayed_count=len(replay_messages),
            )
        )
        if not await self._send_message(
            ws,
            connected_message,
            run_id=key,
            message_kind="connected",
        ):
            await self.remove_run_viewer(run_id=key, ws=ws)
            return
        last_sent_stream_seq = max(int(resume_from_seq or 0), 0)
        for message in replay_messages:
            if not self._viewer_wants_symbol(
                {"selected_symbol_key": normalized_selected},
                str(message.get("symbol_key") or ""),
            ) and message.get("type") in STREAM_SYMBOL_DELTA_TYPES:
                last_sent_stream_seq = max(last_sent_stream_seq, int(message.get("stream_seq") or 0))
                continue
            sent = await self._send_message(
                ws,
                json.dumps(message),
                run_id=key,
                message_kind=str(message.get("type") or "replay"),
                series_key=normalize_series_key(message.get("symbol_key")),
            )
            if not sent:
                await self.remove_run_viewer(run_id=key, ws=ws)
                return
            last_sent_stream_seq = max(last_sent_stream_seq, int(message.get("stream_seq") or 0))
            replayed_count += 1

        while True:
            async with self._lock:
                _, catchup_messages, current_cursor = self._resume_messages_locked(
                    run_id=key,
                    requested_stream_session_id=resolved_stream_session_id,
                    resume_from_seq=last_sent_stream_seq,
                )
                if not catchup_messages:
                    state = self._run_viewers.get(key, {}).get(ws)
                    if state is not None:
                        state["ready"] = True
                    break
            for message in catchup_messages:
                if not self._viewer_wants_symbol(
                    {"selected_symbol_key": normalized_selected},
                    str(message.get("symbol_key") or ""),
                ) and message.get("type") in STREAM_SYMBOL_DELTA_TYPES:
                    last_sent_stream_seq = max(last_sent_stream_seq, int(message.get("stream_seq") or 0))
                    continue
                sent = await self._send_message(
                    ws,
                    json.dumps(message),
                    run_id=key,
                    message_kind=str(message.get("type") or "replay"),
                    series_key=normalize_series_key(message.get("symbol_key")),
                )
                if not sent:
                    await self.remove_run_viewer(run_id=key, ws=ws)
                    return
                last_sent_stream_seq = max(last_sent_stream_seq, int(message.get("stream_seq") or 0))

        _OBSERVER.increment("viewer_added_total", bot_id=self.bot_id_for_run(key), run_id=key)
        _OBSERVER.event(
            "viewer_added",
            bot_id=self.bot_id_for_run(key),
            run_id=key,
            series_key=normalized_selected or None,
            viewer_count=viewer_count,
            replayed_count=replayed_count,
        )

    async def update_viewer_subscription(self, *, run_id: str, ws: WebSocket, payload: Mapping[str, Any]) -> None:
        key = str(run_id)
        replay_messages: list[Dict[str, Any]] = []
        reset_payload: Dict[str, Any] | None = None
        close_after_reset = False
        normalized_symbol_key: str | None = None
        replay_cursor: Dict[str, Any] = {}
        async with self._lock:
            state = self._run_viewers.get(key, {}).get(ws)
            if state is None:
                return
            if str(payload.get("type") or "").strip().lower() == "set_selected_symbol":
                normalized_symbol_key = normalize_series_key(payload.get("symbol_key")) or None
                state["selected_symbol_key"] = normalized_symbol_key
                requested_seq: int | None = None
                raw_resume = payload.get("resume_from_seq", payload.get("base_seq"))
                if raw_resume not in (None, ""):
                    try:
                        requested_seq = max(int(raw_resume or 0), 0)
                    except (TypeError, ValueError):
                        requested_seq = 0
                if normalized_symbol_key and requested_seq is not None:
                    reason, messages, cursor = self._resume_messages_locked(
                        run_id=key,
                        requested_stream_session_id=str(payload.get("stream_session_id") or "").strip() or None,
                        resume_from_seq=requested_seq,
                    )
                    replay_cursor = dict(cursor)
                    if reason:
                        reset_payload = stream_reset_required_message(
                            run_id=key,
                            stream_session_id=str(cursor.get("stream_session_id") or ""),
                            reason=reason,
                            requested_stream_session_id=cursor.get("requested_stream_session_id"),
                            requested_resume_from_seq=int(cursor.get("requested_resume_from_seq") or 0),
                            current_stream_seq=int(cursor.get("base_seq") or 0),
                        )
                        close_after_reset = True
                    else:
                        replay_messages = [
                            dict(message)
                            for message in messages
                            if message.get("type") in STREAM_SYMBOL_DELTA_TYPES
                            and self._viewer_wants_symbol(
                                {"selected_symbol_key": normalized_symbol_key},
                                str(message.get("symbol_key") or ""),
                            )
                        ]

        bot_id = self.bot_id_for_run(key)
        if reset_payload is not None:
            _OBSERVER.increment(
                "reset_required_total",
                bot_id=bot_id,
                run_id=key,
                failure_mode=str(reset_payload.get("reason") or "selected_symbol_replay_failed"),
            )
            sent = await self._send_message(
                ws,
                json.dumps(reset_payload),
                run_id=key,
                message_kind="reset_required",
            )
            await self.remove_run_viewer(run_id=key, ws=ws)
            if sent and close_after_reset:
                try:
                    await ws.close(code=1012)
                except Exception:
                    pass
            return

        if replay_messages:
            _OBSERVER.observe(
                "selected_symbol_replay_message_count",
                float(len(replay_messages)),
                bot_id=bot_id,
                run_id=key,
                series_key=normalized_symbol_key,
                message_kind="selected_symbol_snapshot",
            )
            _OBSERVER.event(
                "selected_symbol_replay_sent",
                bot_id=bot_id,
                run_id=key,
                series_key=normalized_symbol_key,
                replayed_count=len(replay_messages),
                requested_resume_from_seq=int(replay_cursor.get("requested_resume_from_seq") or 0),
                current_stream_seq=int(replay_cursor.get("base_seq") or 0),
            )
        for message in replay_messages:
            sent = await self._send_message(
                ws,
                json.dumps(message),
                run_id=key,
                message_kind=str(message.get("type") or "replay"),
                series_key=normalize_series_key(message.get("symbol_key")),
            )
            if not sent:
                await self.remove_run_viewer(run_id=key, ws=ws)
                return

    async def remove_run_viewer(self, *, run_id: str, ws: WebSocket) -> None:
        remaining = 0
        bot_id = self.bot_id_for_run(run_id)
        async with self._lock:
            viewers = self._run_viewers.get(str(run_id))
            if not viewers:
                return
            viewers.pop(ws, None)
            remaining = len(viewers)
            self._emit_viewer_gauges_locked(run_id=str(run_id))
            if viewers:
                _OBSERVER.increment("viewer_removed_total", bot_id=bot_id, run_id=str(run_id))
                _OBSERVER.event(
                    "viewer_removed",
                    bot_id=bot_id,
                    run_id=str(run_id),
                    viewer_count=remaining,
                )
                return
            self._run_viewers.pop(str(run_id), None)
        _OBSERVER.increment("viewer_removed_total", bot_id=bot_id, run_id=str(run_id))
        _OBSERVER.event(
            "viewer_removed",
            bot_id=bot_id,
            run_id=str(run_id),
            viewer_count=remaining,
        )

    async def evict_run(self, *, run_id: str) -> None:
        async with self._lock:
            self._run_stream_session_id.pop(str(run_id), None)
            self._run_stream_seq.pop(str(run_id), None)
            self._run_scope_seq.pop(str(run_id), None)
            self._run_symbol_scope_seq.pop(str(run_id), None)
            bot_id = self._run_bot_ids.pop(str(run_id), None)
            self._run_message_ring.pop(str(run_id), None)
            self._run_ring_high_water.pop(str(run_id), None)
            self._run_max_requested_gap.pop(str(run_id), None)
            viewers = self._run_viewers.pop(str(run_id), {})
        for ws in list(viewers.keys()):
            try:
                await ws.close(code=1001)
            except Exception:
                pass
        _OBSERVER.gauge("viewer_active_count", 0.0, bot_id=bot_id, run_id=str(run_id))

    async def _broadcast(self, *, run_id: str, message: Dict[str, Any]) -> Dict[str, int]:
        started = time.perf_counter()
        serialized = json.dumps(message)
        targets: list[WebSocket] = []
        filtered_viewer_count = 0
        async with self._lock:
            key = str(run_id)
            self._ensure_run_stream_session_id_locked(key)
            viewers = self._run_viewers.get(key, {})
            for ws, state in viewers.items():
                if not bool(state.get("ready")):
                    continue
                if message.get("type") in STREAM_SYMBOL_DELTA_TYPES and not self._viewer_wants_symbol(
                    state,
                    str(message.get("symbol_key") or ""),
                ):
                    filtered_viewer_count += 1
                    continue
                targets.append(ws)
            self._emit_viewer_gauges_locked(run_id=key)

        stale: list[WebSocket] = []
        successful = 0
        for ws in targets:
            sent = await self._send_message(
                ws,
                serialized,
                run_id=str(run_id),
                message_kind=str(message.get("type") or "broadcast"),
                series_key=normalize_series_key(message.get("symbol_key")),
            )
            if not sent:
                stale.append(ws)
                continue
            successful += 1
        for ws in stale:
            await self.remove_run_viewer(run_id=str(run_id), ws=ws)
        bot_id = self.bot_id_for_run(run_id)
        _OBSERVER.increment(
            "viewer_broadcast_total",
            value=float(successful),
            bot_id=bot_id,
            run_id=str(run_id),
            series_key=normalize_series_key(message.get("symbol_key")),
            message_kind=str(message.get("type") or "broadcast"),
        )
        _OBSERVER.observe(
            "viewer_broadcast_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            bot_id=bot_id,
            run_id=str(run_id),
            series_key=normalize_series_key(message.get("symbol_key")),
            message_kind=str(message.get("type") or "broadcast"),
        )
        _OBSERVER.observe(
            "viewer_payload_bytes",
            float(payload_size_bytes(serialized)),
            bot_id=bot_id,
            run_id=str(run_id),
            series_key=normalize_series_key(message.get("symbol_key")),
            message_kind=str(message.get("type") or "broadcast"),
        )
        return {
            "viewer_count": len(targets),
            "filtered_viewer_count": filtered_viewer_count,
            "stale_viewer_count": len(stale),
        }

    async def broadcast_live_delta(self, prepared_delta: PreparedLiveDelta) -> LiveDeliveryStats:
        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(prepared_delta.event.run_id)
            stream_seq = self._next_stream_seq_locked(prepared_delta.event.run_id)
            message = prepared_delta.event.to_message(
                stream_session_id=stream_session_id,
                stream_seq=stream_seq,
            )
            if prepared_delta.event.symbol_key:
                normalized_symbol_key = normalize_series_key(prepared_delta.event.symbol_key)
                if normalized_symbol_key:
                    next_symbol_scope_seq = max(
                        int(self._run_symbol_scope_seq[prepared_delta.event.run_id].get(normalized_symbol_key) or 0),
                        int(prepared_delta.event.scope_seq),
                    )
                    self._run_symbol_scope_seq[prepared_delta.event.run_id][normalized_symbol_key] = next_symbol_scope_seq
            else:
                self._run_scope_seq[prepared_delta.event.run_id] = max(
                    int(self._run_scope_seq.get(prepared_delta.event.run_id) or 0),
                    int(prepared_delta.event.scope_seq),
                )
            self._append_replay_message_locked(
                run_id=prepared_delta.event.run_id,
                message=message,
            )
        emit_started = time.perf_counter()
        stats = await self._broadcast(
            run_id=prepared_delta.event.run_id,
            message=message,
        )
        return LiveDeliveryStats(
            emit_ms=max((time.perf_counter() - emit_started) * 1000.0, 0.0),
            viewer_count=int(stats.get("viewer_count") or 0),
            filtered_viewer_count=int(stats.get("filtered_viewer_count") or 0),
            stale_viewer_count=int(stats.get("stale_viewer_count") or 0),
        )

    async def current_cursor(self, *, run_id: str, bot_id: str | None = None) -> Dict[str, Any]:
        async with self._lock:
            if bot_id:
                self.bind_run(run_id=run_id, bot_id=bot_id)
            return dict(self._current_cursor_locked(run_id))

    async def current_symbol_cursor(
        self,
        *,
        run_id: str,
        symbol_key: str,
        bot_id: str | None = None,
    ) -> Dict[str, Any]:
        async with self._lock:
            if bot_id:
                self.bind_run(run_id=run_id, bot_id=bot_id)
            return dict(self._current_symbol_cursor_locked(run_id, symbol_key))

    def viewer_count(self) -> int:
        return sum(len(viewers) for viewers in self._run_viewers.values())

    def viewer_count_for_run(self, run_id: str) -> int:
        return len(self._run_viewers.get(str(run_id), {}))

    def viewer_run_count(self) -> int:
        return len(self._run_viewers)

    def ring_run_count(self) -> int:
        return len(self._run_message_ring)

    def ring_message_count(self) -> int:
        return sum(len(entries) for entries in self._run_message_ring.values())


__all__ = ["BotLensRunStream"]
