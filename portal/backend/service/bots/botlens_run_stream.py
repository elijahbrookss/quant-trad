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

from ..observability import BackendObserver, normalize_failure_mode, payload_size_bytes
from .botlens_contract import (
    SCHEMA_VERSION,
    STREAM_CONNECTED_TYPE,
    STREAM_OPEN_TRADES_DELTA_TYPE,
    STREAM_SYMBOL_SNAPSHOT_TYPE,
    STREAM_SYMBOL_DELTA_TYPES,
    STREAM_SUMMARY_DELTA_TYPE,
    normalize_series_key,
)
from .botlens_typed_deltas import PreparedTypedDelta, TypedDeltaDeliveryStats

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_run_stream", event_logger=logger)
_PENDING_SYMBOL_DELTA_LIMIT = 200


class BotLensRunStream:
    def __init__(self, *, ring_size: int | None = None) -> None:
        self._run_viewers: DefaultDict[str, Dict[WebSocket, Dict[str, Any]]] = defaultdict(dict)
        self._run_viewer_metrics: DefaultDict[str, Dict[str, int]] = defaultdict(
            lambda: {
                "active_viewers": 0,
                "snapshot_pending_viewers": 0,
                "buffered_deltas": 0,
            }
        )
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

    @staticmethod
    def _viewer_metric_state(state: Mapping[str, Any]) -> Dict[str, int]:
        return {
            "snapshot_pending": 1 if normalize_series_key(state.get("snapshot_pending_symbol_key")) else 0,
            "buffered_deltas": int(state.get("buffered_delta_count") or 0),
        }

    def _apply_viewer_metric_delta(
        self,
        *,
        run_id: str,
        previous_state: Mapping[str, Any] | None = None,
        next_state: Mapping[str, Any] | None = None,
        active_delta: int = 0,
    ) -> None:
        metrics = self._run_viewer_metrics[str(run_id)]
        metrics["active_viewers"] = max(metrics["active_viewers"] + int(active_delta), 0)
        if previous_state is not None:
            previous = self._viewer_metric_state(previous_state)
            metrics["snapshot_pending_viewers"] = max(
                metrics["snapshot_pending_viewers"] - previous["snapshot_pending"],
                0,
            )
            metrics["buffered_deltas"] = max(
                metrics["buffered_deltas"] - previous["buffered_deltas"],
                0,
            )
        if next_state is not None:
            updated = self._viewer_metric_state(next_state)
            metrics["snapshot_pending_viewers"] += updated["snapshot_pending"]
            metrics["buffered_deltas"] += updated["buffered_deltas"]
        if (
            metrics["active_viewers"] == 0
            and metrics["snapshot_pending_viewers"] == 0
            and metrics["buffered_deltas"] == 0
        ):
            self._run_viewer_metrics.pop(str(run_id), None)

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
        _OBSERVER.increment(
            "viewer_send_total",
            run_id=run_id,
            series_key=series_key,
            message_kind=message_kind,
        )
        try:
            await ws.send_text(message)
            _OBSERVER.observe(
                "viewer_send_ms",
                max((time.perf_counter() - started) * 1000.0, 0.0),
                run_id=run_id,
                series_key=series_key,
                message_kind=message_kind,
            )
            return True
        except Exception as exc:
            failure_mode = normalize_failure_mode(exc)
            _OBSERVER.increment(
                "viewer_send_fail_total",
                run_id=run_id,
                series_key=series_key,
                message_kind=message_kind,
                failure_mode=failure_mode,
            )
            _OBSERVER.observe(
                "viewer_send_ms",
                max((time.perf_counter() - started) * 1000.0, 0.0),
                run_id=run_id,
                series_key=series_key,
                message_kind=message_kind,
                failure_mode=failure_mode,
            )
            _OBSERVER.event(
                "viewer_send_failed",
                level=logging.WARN,
                run_id=run_id,
                series_key=series_key,
                message_kind=message_kind,
                failure_mode=failure_mode,
                error=str(exc),
            )
            return False

    @staticmethod
    def _viewer_wants_symbol(viewer_state: Mapping[str, Any], symbol_key: str) -> bool:
        normalized = normalize_series_key(symbol_key)
        if not normalized:
            return False
        selected = normalize_series_key(viewer_state.get("selected_symbol_key"))
        return bool(selected) and selected == normalized

    def _buffer_viewer_symbol_delta(
        self,
        *,
        run_id: str,
        viewer_state: Dict[str, Any],
        message: Mapping[str, Any],
    ) -> None:
        pending = viewer_state.get("pending_symbol_deltas")
        if not isinstance(pending, list):
            pending = []
            viewer_state["pending_symbol_deltas"] = pending
        previous_state = dict(viewer_state)
        pending.append(dict(message))
        if len(pending) > _PENDING_SYMBOL_DELTA_LIMIT:
            overflow = len(pending) - _PENDING_SYMBOL_DELTA_LIMIT
            del pending[:overflow]
            _OBSERVER.increment(
                "snapshot_buffer_drop_total",
                value=float(overflow),
                run_id=run_id,
                series_key=normalize_series_key(message.get("symbol_key")),
                message_kind="snapshot_buffer",
            )
            _OBSERVER.event(
                "viewer_snapshot_buffer_overflow",
                level=logging.WARN,
                log_to_logger=False,
                run_id=run_id,
                series_key=normalize_series_key(message.get("symbol_key")),
                dropped_count=overflow,
                max_buffered=_PENDING_SYMBOL_DELTA_LIMIT,
            )
        viewer_state["buffered_delta_count"] = len(pending)
        self._apply_viewer_metric_delta(
            run_id=run_id,
            previous_state=previous_state,
            next_state=viewer_state,
        )

    def _emit_viewer_gauges_locked(self, *, run_id: str) -> None:
        metrics = self._run_viewer_metrics.get(str(run_id), {})
        _OBSERVER.maybe_gauge(
            f"viewer_active:{run_id}",
            "viewer_active_count",
            float(int(metrics.get("active_viewers") or 0)),
            run_id=run_id,
        )
        _OBSERVER.maybe_gauge(
            f"snapshot_pending:{run_id}",
            "snapshot_pending_viewers",
            float(int(metrics.get("snapshot_pending_viewers") or 0)),
            run_id=run_id,
        )
        _OBSERVER.maybe_gauge(
            f"snapshot_buffered:{run_id}",
            "snapshot_buffered_deltas",
            float(int(metrics.get("buffered_deltas") or 0)),
            run_id=run_id,
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
            viewer_state = {
                "selected_symbol_key": normalized_selected,
                "snapshot_pending_symbol_key": pending_symbol_key,
                "pending_symbol_deltas": [],
                "buffered_delta_count": 0,
            }
            self._run_viewers[key][ws] = viewer_state
            self._apply_viewer_metric_delta(
                run_id=key,
                next_state=viewer_state,
                active_delta=1,
            )
            self._emit_viewer_gauges_locked(run_id=key)

        connected_message = json.dumps(
            self._connected_envelope(run_id=key, stream_session_id=stream_session_id)
        )
        if not await self._send_message(
            ws,
            connected_message,
            run_id=key,
            message_kind="connected",
        ):
            await self.remove_run_viewer(run_id=key, ws=ws)
            return
        _OBSERVER.increment("viewer_added_total", run_id=key)
        _OBSERVER.event(
            "viewer_added",
            run_id=key,
            series_key=normalized_selected or None,
            viewer_count=viewer_count,
        )

    async def update_viewer_subscription(self, *, run_id: str, ws: WebSocket, payload: Mapping[str, Any]) -> None:
        key = str(run_id)
        async with self._lock:
            state = self._run_viewers.get(key, {}).get(ws)
            if state is None:
                return
            previous_state = dict(state)
            message_type = str(payload.get("type") or "").strip().lower()
            if message_type == "set_selected_symbol":
                normalized_symbol_key = normalize_series_key(payload.get("symbol_key"))
                state["selected_symbol_key"] = normalized_symbol_key
                state["snapshot_pending_symbol_key"] = normalized_symbol_key or None
                state["pending_symbol_deltas"] = []
                state["buffered_delta_count"] = 0
            self._apply_viewer_metric_delta(
                run_id=key,
                previous_state=previous_state,
                next_state=state,
            )
            self._emit_viewer_gauges_locked(run_id=key)

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
            previous_state = dict(state)
            pending = [
                dict(message)
                for message in (state.get("pending_symbol_deltas") or [])
                if normalize_series_key(message.get("symbol_key")) == symbol_key
            ]
            state["snapshot_pending_symbol_key"] = None
            state["pending_symbol_deltas"] = []
            state["buffered_delta_count"] = 0
            self._apply_viewer_metric_delta(
                run_id=key,
                previous_state=previous_state,
                next_state=state,
            )
            self._emit_viewer_gauges_locked(run_id=key)

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
        _OBSERVER.observe(
            "viewer_payload_bytes",
            float(payload_size_bytes(snapshot_message)),
            run_id=key,
            series_key=symbol_key,
            message_kind="snapshot",
        )
        if not await self._send_message(
            ws,
            snapshot_message,
            run_id=key,
            message_kind="snapshot",
            series_key=symbol_key,
        ):
            await self.remove_run_viewer(run_id=key, ws=ws)
            return False

        replay_count = 0
        for message in sorted(pending, key=lambda entry: int(entry.get("seq") or 0)):
            if int(message.get("seq") or 0) <= snapshot_seq:
                continue
            serialized_message = json.dumps(message)
            if not await self._send_message(
                ws,
                serialized_message,
                run_id=key,
                message_kind="snapshot_replay",
                series_key=symbol_key,
            ):
                await self.remove_run_viewer(run_id=key, ws=ws)
                return False
            replay_count += 1
        _OBSERVER.observe(
            "snapshot_replay_count",
            float(replay_count),
            run_id=key,
            series_key=symbol_key,
            message_kind="snapshot",
        )
        _OBSERVER.event(
            "viewer_snapshot_sent",
            run_id=key,
            series_key=symbol_key,
            seq=snapshot_seq,
            replay_count=replay_count,
        )
        return True

    async def remove_run_viewer(self, *, run_id: str, ws: WebSocket) -> None:
        remaining = 0
        async with self._lock:
            viewers = self._run_viewers.get(str(run_id))
            if not viewers:
                return
            removed_state = viewers.pop(ws, None)
            if removed_state is not None:
                self._apply_viewer_metric_delta(
                    run_id=str(run_id),
                    previous_state=removed_state,
                    active_delta=-1,
                )
            remaining = len(viewers)
            self._emit_viewer_gauges_locked(run_id=str(run_id))
            if viewers:
                _OBSERVER.increment("viewer_removed_total", run_id=str(run_id))
                _OBSERVER.event(
                    "viewer_removed",
                    run_id=str(run_id),
                    viewer_count=remaining,
                )
                return
            self._run_viewers.pop(str(run_id), None)
        _OBSERVER.increment("viewer_removed_total", run_id=str(run_id))
        _OBSERVER.event(
            "viewer_removed",
            run_id=str(run_id),
            viewer_count=remaining,
        )

    async def evict_run(self, *, run_id: str) -> None:
        async with self._lock:
            self._run_stream_session_id.pop(str(run_id), None)
            viewers = self._run_viewers.pop(str(run_id), {})
            self._run_viewer_metrics.pop(str(run_id), None)
        for ws in list(viewers.keys()):
            try:
                await ws.close(code=1001)
            except Exception:
                pass
        _OBSERVER.gauge("viewer_active_count", 0.0, run_id=str(run_id))
        _OBSERVER.gauge("snapshot_pending_viewers", 0.0, run_id=str(run_id))
        _OBSERVER.gauge("snapshot_buffered_deltas", 0.0, run_id=str(run_id))

    async def _broadcast(self, *, run_id: str, message: Dict[str, Any]) -> Dict[str, int]:
        started = time.perf_counter()
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
                    self._buffer_viewer_symbol_delta(
                        run_id=key,
                        viewer_state=state,
                        message=message,
                    )
                    continue
                if message.get("type") in STREAM_SYMBOL_DELTA_TYPES and not self._viewer_wants_symbol(
                    state,
                    str(message.get("symbol_key") or ""),
                ):
                    filtered_viewer_count += 1
                    continue
                targets.append((ws, dict(state)))
            self._emit_viewer_gauges_locked(run_id=key)

        stale: list[WebSocket] = []
        successful = 0
        for ws, state in targets:
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
        _OBSERVER.increment(
            "viewer_broadcast_total",
            value=float(successful),
            run_id=str(run_id),
            series_key=normalize_series_key(message.get("symbol_key")),
            message_kind=str(message.get("type") or "broadcast"),
        )
        _OBSERVER.observe(
            "viewer_broadcast_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            run_id=str(run_id),
            series_key=normalize_series_key(message.get("symbol_key")),
            message_kind=str(message.get("type") or "broadcast"),
        )
        _OBSERVER.observe(
            "viewer_payload_bytes",
            float(payload_size_bytes(serialized)),
            run_id=str(run_id),
            series_key=normalize_series_key(message.get("symbol_key")),
            message_kind=str(message.get("type") or "broadcast"),
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
