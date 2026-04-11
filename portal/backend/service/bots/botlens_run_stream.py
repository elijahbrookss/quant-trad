from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Mapping
from typing import Any, DefaultDict, Dict

try:
    from fastapi import WebSocket
except ModuleNotFoundError:  # pragma: no cover - test environments may not install FastAPI
    class WebSocket:  # type: ignore[override]
        pass

from .botlens_contract import (
    SCHEMA_VERSION,
    STREAM_CONNECTED_TYPE,
    STREAM_OPEN_TRADES_DELTA_TYPE,
    STREAM_SYMBOL_SNAPSHOT_TYPE,
    STREAM_SYMBOL_DELTA_TYPES,
    STREAM_SUMMARY_DELTA_TYPE,
    normalize_series_key,
)
from .botlens_typed_deltas import PreparedTypedDelta, TypedDeltaDeliveryStats, TypedDeltaInstrumentation

logger = logging.getLogger(__name__)
_PENDING_SYMBOL_DELTA_LIMIT = 200


def _payload_size_bytes(payload: str) -> int:
    return len(payload.encode("utf-8"))


class BotLensRunStream:
    def __init__(self, *, ring_size: int | None = None) -> None:
        self._run_viewers: DefaultDict[str, Dict[WebSocket, Dict[str, Any]]] = defaultdict(dict)
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

    def _connected_envelope(self, *, run_id: str, stream_session_id: str) -> Dict[str, Any]:
        return {
            "type": STREAM_CONNECTED_TYPE,
            "schema_version": SCHEMA_VERSION,
            "run_id": str(run_id),
            "stream_session_id": str(stream_session_id),
        }

    async def _send_message(self, ws: WebSocket, message: str) -> bool:
        try:
            await ws.send_text(message)
            return True
        except Exception:
            return False

    @staticmethod
    def _viewer_wants_symbol(viewer_state: Mapping[str, Any], symbol_key: str) -> bool:
        normalized = normalize_series_key(symbol_key)
        if not normalized:
            return False
        selected = normalize_series_key(viewer_state.get("selected_symbol_key"))
        return bool(selected) and selected == normalized

    @staticmethod
    def _buffer_viewer_symbol_delta(viewer_state: Dict[str, Any], message: Mapping[str, Any]) -> None:
        pending = viewer_state.get("pending_symbol_deltas")
        if not isinstance(pending, list):
            pending = []
            viewer_state["pending_symbol_deltas"] = pending
        pending.append(dict(message))
        if len(pending) > _PENDING_SYMBOL_DELTA_LIMIT:
            overflow = len(pending) - _PENDING_SYMBOL_DELTA_LIMIT
            del pending[:overflow]
            logger.warning(
                "botlens_stream_symbol_snapshot_overflow | symbol_key=%s | dropped_count=%s | max_buffered=%s",
                normalize_series_key(message.get("symbol_key")),
                overflow,
                _PENDING_SYMBOL_DELTA_LIMIT,
            )

    async def add_run_viewer(
        self,
        *,
        run_id: str,
        ws: WebSocket,
        selected_symbol_key: str | None = None,
    ) -> None:
        await ws.accept()
        key = str(run_id)
        normalized_selected = normalize_series_key(selected_symbol_key)

        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(key)
            viewer_count = len(self._run_viewers[key]) + 1
            pending_symbol_key = normalized_selected or None
            self._run_viewers[key][ws] = {
                "selected_symbol_key": normalized_selected,
                "snapshot_pending_symbol_key": pending_symbol_key,
                "pending_symbol_deltas": [],
            }

        connected_message = json.dumps(
            self._connected_envelope(run_id=key, stream_session_id=stream_session_id)
        )
        if not await self._send_message(
            ws,
            connected_message,
        ):
            await self.remove_run_viewer(run_id=key, ws=ws)
            return
        logger.info(
            "botlens_stream_viewer_added | run_id=%s | selected_symbol_key=%s | viewer_count=%s",
            key,
            normalized_selected or None,
            viewer_count,
        )

    async def update_viewer_subscription(self, *, run_id: str, ws: WebSocket, payload: Mapping[str, Any]) -> None:
        key = str(run_id)
        async with self._lock:
            state = self._run_viewers.get(key, {}).get(ws)
            if state is None:
                return
            message_type = str(payload.get("type") or "").strip().lower()
            if message_type == "set_selected_symbol":
                normalized_symbol_key = normalize_series_key(payload.get("symbol_key"))
                state["selected_symbol_key"] = normalized_symbol_key
                state["snapshot_pending_symbol_key"] = normalized_symbol_key or None
                state["pending_symbol_deltas"] = []
            logger.debug(
                "botlens_stream_viewer_subscription_updated | run_id=%s | type=%s | selected_symbol_key=%s",
                key,
                message_type,
                state.get("selected_symbol_key"),
            )

    async def deliver_symbol_snapshot(
        self,
        *,
        run_id: str,
        ws: WebSocket,
        snapshot: Mapping[str, Any],
    ) -> bool:
        key = str(run_id)
        symbol_key = normalize_series_key(snapshot.get("symbol_key"))
        snapshot_seq = int(snapshot.get("seq") or 0)
        pending: list[Dict[str, Any]] = []
        async with self._lock:
            state = self._run_viewers.get(key, {}).get(ws)
            if state is None:
                return False
            pending = [
                dict(message)
                for message in (state.get("pending_symbol_deltas") or [])
                if normalize_series_key(message.get("symbol_key")) == symbol_key
            ]
            state["snapshot_pending_symbol_key"] = None
            state["pending_symbol_deltas"] = []

        snapshot_message = json.dumps(
            {
                "type": STREAM_SYMBOL_SNAPSHOT_TYPE,
                "schema_version": SCHEMA_VERSION,
                "run_id": key,
                "symbol_key": symbol_key,
                "seq": snapshot_seq,
                "payload": dict(snapshot.get("detail") or {}),
            }
        )
        if not await self._send_message(ws, snapshot_message):
            await self.remove_run_viewer(run_id=key, ws=ws)
            return False

        replay_count = 0
        for message in sorted(pending, key=lambda entry: int(entry.get("seq") or 0)):
            if int(message.get("seq") or 0) <= snapshot_seq:
                continue
            if not await self._send_message(ws, json.dumps(message)):
                await self.remove_run_viewer(run_id=key, ws=ws)
                return False
            replay_count += 1

        logger.info(
            "botlens_stream_symbol_snapshot_sent | run_id=%s | symbol_key=%s | seq=%s | replay_count=%s",
            key,
            symbol_key or None,
            snapshot_seq,
            replay_count,
        )
        return True

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
            self._run_stream_session_id.pop(str(run_id), None)
            viewers = self._run_viewers.pop(str(run_id), {})
        for ws in list(viewers.keys()):
            try:
                await ws.close(code=1001)
            except Exception:
                pass
        logger.info(
            "botlens_stream_run_evicted | run_id=%s | viewer_count=%s",
            run_id,
            len(viewers),
        )

    async def _broadcast(self, *, run_id: str, message: Dict[str, Any]) -> Dict[str, int]:
        serialized = json.dumps(message)
        targets: list[tuple[WebSocket, Dict[str, Any]]] = []
        filtered_viewer_count = 0
        async with self._lock:
            key = str(run_id)
            self._ensure_run_stream_session_id_locked(key)
            viewers = self._run_viewers.get(key, {})
            for ws, state in viewers.items():
                pending_symbol_key = normalize_series_key(state.get("snapshot_pending_symbol_key"))
                if (
                    message.get("type") in STREAM_SYMBOL_DELTA_TYPES
                    and pending_symbol_key
                    and pending_symbol_key == normalize_series_key(message.get("symbol_key"))
                ):
                    self._buffer_viewer_symbol_delta(state, message)
                    continue
                if message.get("type") in STREAM_SYMBOL_DELTA_TYPES and not self._viewer_wants_symbol(
                    state,
                    str(message.get("symbol_key") or ""),
                ):
                    filtered_viewer_count += 1
                    continue
                targets.append((ws, dict(state)))

        stale: list[WebSocket] = []
        for ws, state in targets:
            sent = await self._send_message(ws, serialized)
            if not sent:
                stale.append(ws)
        for ws in stale:
            await self.remove_run_viewer(run_id=str(run_id), ws=ws)
        logger.debug(
            "botlens_stream_broadcast | run_id=%s | type=%s | seq=%s | payload_bytes=%s | viewer_count=%s | filtered_viewers=%s | stale_viewers=%s",
            run_id,
            message.get("type"),
            int(message.get("seq") or 0),
            _payload_size_bytes(serialized),
            len(targets),
            filtered_viewer_count,
            len(stale),
        )
        return {
            "viewer_count": len(targets),
            "filtered_viewer_count": filtered_viewer_count,
            "stale_viewer_count": len(stale),
        }

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

    async def broadcast_typed_delta(self, prepared_delta: PreparedTypedDelta) -> TypedDeltaDeliveryStats:
        async with self._lock:
            stream_session_id = self._ensure_run_stream_session_id_locked(prepared_delta.event.run_id)
        emit_started = time.perf_counter()
        stats = await self._broadcast(
            run_id=prepared_delta.event.run_id,
            message=prepared_delta.event.to_message(stream_session_id=stream_session_id),
        )
        delivery = TypedDeltaDeliveryStats(
            emit_ms=max((time.perf_counter() - emit_started) * 1000.0, 0.0),
            viewer_count=int(stats.get("viewer_count") or 0),
            filtered_viewer_count=int(stats.get("filtered_viewer_count") or 0),
            stale_viewer_count=int(stats.get("stale_viewer_count") or 0),
        )
        TypedDeltaInstrumentation.log_emission(
            logger=logger,
            prepared_delta=prepared_delta,
            delivery=delivery,
        )
        return delivery

    def viewer_count(self) -> int:
        return sum(len(viewers) for viewers in self._run_viewers.values())

    def viewer_count_for_run(self, run_id: str) -> int:
        return len(self._run_viewers.get(str(run_id), {}))

    def viewer_run_count(self) -> int:
        return len(self._run_viewers)

    def ring_run_count(self) -> int:
        return 0

    def ring_message_count(self) -> int:
        return 0


__all__ = ["BotLensRunStream"]
