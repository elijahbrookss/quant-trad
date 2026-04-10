from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import defaultdict, deque
from collections.abc import Mapping
from typing import Any, DefaultDict, Dict, Iterable

from fastapi import WebSocket

from .botlens_contract import (
    SCHEMA_VERSION,
    STREAM_CONNECTED_TYPE,
    STREAM_DETAIL_DELTA_TYPE,
    STREAM_OPEN_TRADES_DELTA_TYPE,
    STREAM_RESYNC_REQUIRED_TYPE,
    STREAM_SUMMARY_DELTA_TYPE,
    normalize_series_key,
)

logger = logging.getLogger(__name__)


def _payload_size_bytes(payload: str) -> int:
    return len(payload.encode("utf-8"))


class BotLensRunStream:
    def __init__(self, *, ring_size: int) -> None:
        self._ring_size = max(1, int(ring_size))
        self._run_viewers: DefaultDict[str, Dict[WebSocket, Dict[str, Any]]] = defaultdict(dict)
        self._run_tail_ring: DefaultDict[str, deque[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=self._ring_size))
        self._run_stream_session_id: Dict[str, str] = {}
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
        return current

    @staticmethod
    def _normalize_hot_symbols(values: Iterable[Any]) -> set[str]:
        normalized = {normalize_series_key(value) for value in values}
        return {value for value in normalized if value}

    def _connected_envelope(self, *, run_id: str, stream_session_id: str, cursor_seq: int) -> Dict[str, Any]:
        return {
            "type": STREAM_CONNECTED_TYPE,
            "schema_version": SCHEMA_VERSION,
            "run_id": str(run_id),
            "stream_session_id": str(stream_session_id),
            "cursor_seq": int(cursor_seq),
        }

    def _resync_required_envelope(
        self,
        *,
        run_id: str,
        stream_session_id: str,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return {
            "type": STREAM_RESYNC_REQUIRED_TYPE,
            "schema_version": SCHEMA_VERSION,
            "run_id": str(run_id),
            "stream_session_id": str(stream_session_id),
            "payload": {
                "reason": str(reason or "resync_required"),
                "details": dict(details or {}),
            },
        }

    async def _send_message(self, ws: WebSocket, message: str) -> bool:
        try:
            await ws.send_text(message)
            return True
        except Exception:
            return False

    @staticmethod
    def _viewer_wants_detail(viewer_state: Mapping[str, Any], symbol_key: str) -> bool:
        normalized = normalize_series_key(symbol_key)
        if not normalized:
            return False
        selected = normalize_series_key(viewer_state.get("selected_symbol_key"))
        if selected and selected == normalized:
            return True
        hot_symbols = viewer_state.get("hot_symbols")
        return normalized in hot_symbols if isinstance(hot_symbols, set) else False

    async def add_run_viewer(
        self,
        *,
        run_id: str,
        ws: WebSocket,
        cursor_seq: int = 0,
        selected_symbol_key: str | None = None,
        hot_symbols: Iterable[Any] | None = None,
    ) -> None:
        await ws.accept()
        key = str(run_id)
        normalized_selected = normalize_series_key(selected_symbol_key)
        normalized_hot = self._normalize_hot_symbols(hot_symbols or [])
        if normalized_selected:
            normalized_hot.add(normalized_selected)

        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(key)
            viewer_count = len(self._run_viewers[key]) + 1
            self._run_viewers[key][ws] = {
                "last_seq": int(cursor_seq or 0),
                "replaying": True,
                "selected_symbol_key": normalized_selected,
                "hot_symbols": normalized_hot,
            }
            buffered = [dict(message) for message in self._run_tail_ring.get(key, ()) if int(message.get("seq") or 0) > int(cursor_seq or 0)]

        connected_message = json.dumps(
            self._connected_envelope(run_id=key, stream_session_id=stream_session_id, cursor_seq=int(cursor_seq or 0))
        )
        if not await self._send_message(
            ws,
            connected_message,
        ):
            await self.remove_run_viewer(run_id=key, ws=ws)
            return
        logger.info(
            "botlens_stream_viewer_added | run_id=%s | cursor_seq=%s | selected_symbol_key=%s | hot_symbol_count=%s | replay_count=%s | viewer_count=%s",
            key,
            int(cursor_seq or 0),
            normalized_selected or None,
            len(normalized_hot),
            len(buffered),
            viewer_count,
        )

        if buffered:
            first_seq = int(buffered[0].get("seq") or 0)
            cursor_value = int(cursor_seq or 0)
            if cursor_value > 0 and first_seq > cursor_value + 1:
                resync_message = json.dumps(
                    self._resync_required_envelope(
                        run_id=key,
                        stream_session_id=stream_session_id,
                        reason="replay_buffer_miss",
                        details={
                            "cursor_seq": cursor_value,
                            "first_buffered_seq": first_seq,
                            "ring_size": self._ring_size,
                        },
                    )
                )
                await self._send_message(
                    ws,
                    resync_message,
                )
                logger.warning(
                    "botlens_stream_replay_buffer_miss | run_id=%s | cursor_seq=%s | first_buffered_seq=%s | viewer_count=%s",
                    key,
                    cursor_value,
                    first_seq,
                    viewer_count,
                )
                await self.remove_run_viewer(run_id=key, ws=ws)
                try:
                    await ws.close(code=1013)
                except Exception:
                    pass
                return

            for message in buffered:
                if message.get("type") == STREAM_DETAIL_DELTA_TYPE and not self._viewer_wants_detail(
                    self._run_viewers.get(key, {}).get(ws, {}),
                    str(message.get("symbol_key") or ""),
                ):
                    continue
                if not await self._send_message(ws, json.dumps(message)):
                    await self.remove_run_viewer(run_id=key, ws=ws)
                    return
                cursor_value = max(cursor_value, int(message.get("seq") or 0))

        async with self._lock:
            state = self._run_viewers.get(key, {}).get(ws)
            if state is not None:
                state["last_seq"] = int(cursor_seq or 0) if not buffered else max(int(message.get("seq") or 0) for message in buffered)
                state["replaying"] = False

    async def update_viewer_subscription(self, *, run_id: str, ws: WebSocket, payload: Mapping[str, Any]) -> None:
        key = str(run_id)
        async with self._lock:
            state = self._run_viewers.get(key, {}).get(ws)
            if state is None:
                return
            message_type = str(payload.get("type") or "").strip().lower()
            if message_type == "set_selected_symbol":
                selected = normalize_series_key(payload.get("symbol_key"))
                state["selected_symbol_key"] = selected
                hot_symbols = state.get("hot_symbols")
                if isinstance(hot_symbols, set) and selected:
                    hot_symbols.add(selected)
            elif message_type == "set_hot_symbols":
                normalized_hot = self._normalize_hot_symbols(payload.get("symbol_keys") or [])
                selected = normalize_series_key(state.get("selected_symbol_key"))
                if selected:
                    normalized_hot.add(selected)
                state["hot_symbols"] = normalized_hot
            logger.debug(
                "botlens_stream_viewer_subscription_updated | run_id=%s | type=%s | selected_symbol_key=%s | hot_symbol_count=%s",
                key,
                message_type,
                state.get("selected_symbol_key"),
                len(state.get("hot_symbols") or ()),
            )

    async def remove_run_viewer(self, *, run_id: str, ws: WebSocket) -> None:
        remaining = 0
        async with self._lock:
            viewers = self._run_viewers.get(str(run_id))
            if not viewers:
                return
            viewers.pop(ws, None)
            remaining = len(viewers)
            if viewers:
                logger.info(
                    "botlens_stream_viewer_removed | run_id=%s | viewer_count=%s",
                    run_id,
                    remaining,
                )
                return
            self._run_viewers.pop(str(run_id), None)
        logger.info(
            "botlens_stream_viewer_removed | run_id=%s | viewer_count=%s",
            run_id,
            remaining,
        )

    async def evict_run(self, *, run_id: str) -> None:
        async with self._lock:
            ring = self._run_tail_ring.pop(str(run_id), None)
            self._run_stream_session_id.pop(str(run_id), None)
            viewers = self._run_viewers.pop(str(run_id), {})
            ring_depth = len(ring or ())
        for ws in list(viewers.keys()):
            try:
                await ws.close(code=1001)
            except Exception:
                pass
        logger.info(
            "botlens_stream_run_evicted | run_id=%s | viewer_count=%s | ring_depth=%s",
            run_id,
            len(viewers),
            ring_depth,
        )

    async def _broadcast(self, *, run_id: str, message: Dict[str, Any]) -> None:
        serialized = json.dumps(message)
        targets: list[tuple[WebSocket, Dict[str, Any]]] = []
        ring_depth = 0
        async with self._lock:
            key = str(run_id)
            self._ensure_run_stream_session_id_locked(key)
            self._run_tail_ring[key].append(dict(message))
            ring_depth = len(self._run_tail_ring.get(key, ()))
            viewers = self._run_viewers.get(key, {})
            for ws, state in viewers.items():
                if state.get("replaying"):
                    continue
                if message.get("type") == STREAM_DETAIL_DELTA_TYPE and not self._viewer_wants_detail(
                    state,
                    str(message.get("symbol_key") or ""),
                ):
                    continue
                targets.append((ws, dict(state)))

        stale: list[WebSocket] = []
        for ws, state in targets:
            sent = await self._send_message(ws, serialized)
            if not sent:
                stale.append(ws)
                continue
            async with self._lock:
                current = self._run_viewers.get(str(run_id), {}).get(ws)
                if current is not None:
                    current["last_seq"] = max(int(current.get("last_seq") or 0), int(message.get("seq") or 0))
        for ws in stale:
            await self.remove_run_viewer(run_id=str(run_id), ws=ws)
        logger.debug(
            "botlens_stream_broadcast | run_id=%s | type=%s | seq=%s | payload_bytes=%s | viewer_count=%s | stale_viewers=%s | ring_depth=%s",
            run_id,
            message.get("type"),
            int(message.get("seq") or 0),
            _payload_size_bytes(serialized),
            len(targets),
            len(stale),
            ring_depth,
        )

    async def broadcast_summary_delta(
        self,
        *,
        run_id: str,
        seq: int,
        health: Mapping[str, Any] | None,
        lifecycle: Mapping[str, Any] | None,
        symbol_upserts: list[Mapping[str, Any]],
        symbol_removals: list[str] | None = None,
    ) -> None:
        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(str(run_id))
        await self._broadcast(
            run_id=str(run_id),
            message={
                "type": STREAM_SUMMARY_DELTA_TYPE,
                "schema_version": SCHEMA_VERSION,
                "run_id": str(run_id),
                "seq": int(seq),
                "stream_session_id": stream_session_id,
                "payload": {
                    "health": dict(health or {}),
                    "lifecycle": dict(lifecycle or {}),
                    "symbol_upserts": [dict(entry) for entry in symbol_upserts if isinstance(entry, Mapping)],
                    "symbol_removals": [str(entry) for entry in (symbol_removals or []) if str(entry).strip()],
                },
            },
        )

    async def broadcast_open_trades_delta(
        self,
        *,
        run_id: str,
        seq: int,
        upserts: list[Mapping[str, Any]],
        removals: list[str],
    ) -> None:
        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(str(run_id))
        await self._broadcast(
            run_id=str(run_id),
            message={
                "type": STREAM_OPEN_TRADES_DELTA_TYPE,
                "schema_version": SCHEMA_VERSION,
                "run_id": str(run_id),
                "seq": int(seq),
                "stream_session_id": stream_session_id,
                "payload": {
                    "upserts": [dict(entry) for entry in upserts if isinstance(entry, Mapping)],
                    "removals": [str(entry) for entry in removals if str(entry).strip()],
                },
            },
        )

    async def broadcast_detail_delta(
        self,
        *,
        run_id: str,
        symbol_key: str,
        seq: int,
        payload: Mapping[str, Any],
    ) -> None:
        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(str(run_id))
        await self._broadcast(
            run_id=str(run_id),
            message={
                "type": STREAM_DETAIL_DELTA_TYPE,
                "schema_version": SCHEMA_VERSION,
                "run_id": str(run_id),
                "symbol_key": normalize_series_key(symbol_key),
                "seq": int(seq),
                "stream_session_id": stream_session_id,
                "payload": dict(payload or {}),
            },
        )

    def viewer_count(self) -> int:
        return sum(len(viewers) for viewers in self._run_viewers.values())

    def viewer_count_for_run(self, run_id: str) -> int:
        return len(self._run_viewers.get(str(run_id), {}))

    def viewer_run_count(self) -> int:
        return len(self._run_viewers)

    def ring_run_count(self) -> int:
        return len(self._run_tail_ring)

    def ring_message_count(self) -> int:
        return sum(len(messages) for messages in self._run_tail_ring.values())


__all__ = ["BotLensRunStream"]
