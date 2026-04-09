from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Mapping as AbcMapping
from datetime import datetime
from typing import Any, DefaultDict, Dict, Optional, Tuple

from core.settings import get_settings
from engines.bot_runtime.runtime.event_types import BOTLENS_SERIES_BOOTSTRAP, BOTLENS_SERIES_DELTA
from fastapi import WebSocket

from ..storage.storage import (
    get_latest_bot_run_view_state,
    record_bot_runtime_event,
    upsert_bot_run_view_state,
)
from .live_series_stream import LiveSeriesStream
from .bot_service import publish_projected_bot, publish_runtime_update
from .botlens_projection import apply_series_runtime_delta, canonicalize_projection, normalize_series_key

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_SETTINGS = get_settings()
_RING_SIZE = _SETTINGS.bot_runtime.botlens.ring_size
_INGEST_QUEUE_MAX = _SETTINGS.bot_runtime.botlens.ingest_queue_max


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
        self._ingest_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=_INGEST_QUEUE_MAX)
        self._ingest_task: Optional[asyncio.Task[None]] = None
        self._lock = asyncio.Lock()
        self._worker_lock = asyncio.Lock()
        self._live_series = LiveSeriesStream(
            ring_size=_RING_SIZE,
            schema_version=_SCHEMA_VERSION,
            sanitize_json=_sanitize_json,
            coerce_int=_coerce_int,
        )

    async def _invalidate_run_live_continuity(
        self,
        *,
        run_id: str,
        reason: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        await self._live_series.invalidate_run_live_continuity(
            run_id=run_id,
            reason=reason,
            details=details,
        )

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

    async def _publish_projected_bot(self, *, bot_id: str) -> None:
        try:
            await asyncio.to_thread(publish_projected_bot, bot_id, inspect_container=False)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bot_telemetry_projected_bot_broadcast_failed | bot_id=%s | error=%s",
                bot_id,
                exc,
            )

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
        if kind == "bot_projection_refresh":
            bot_id = str(payload.get("bot_id") or "").strip()
            if bot_id:
                logger.info(
                    "bot_telemetry_projection_refresh_ingested | bot_id=%s | run_id=%s | phase=%s | status=%s",
                    bot_id,
                    str(payload.get("run_id") or "").strip(),
                    str(payload.get("phase") or "").strip(),
                    str(payload.get("status") or "").strip(),
                )
                await self._publish_projected_bot(bot_id=bot_id)
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
        await self._live_series.add_series_viewer(
            run_id=str(run_id),
            series_key=normalize_series_key(series_key),
            ws=ws,
            limit=limit,
        )

    async def remove_series_viewer(self, *, run_id: str, series_key: str, ws: WebSocket) -> None:
        await self._live_series.remove_series_viewer(
            run_id=str(run_id),
            series_key=normalize_series_key(series_key),
            ws=ws,
        )

    async def _broadcast_series_delta(
        self,
        *,
        run_id: str,
        series_key: str,
        seq: int,
        known_at: Any,
        runtime_delta: Mapping[str, Any],
    ) -> None:
        await self._live_series.broadcast_series_delta(
            run_id=str(run_id),
            series_key=normalize_series_key(series_key),
            seq=int(seq),
            known_at=known_at,
            runtime_delta=runtime_delta,
        )


telemetry_hub = BotTelemetryHub()
