from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time
from collections.abc import Mapping as AbcMapping
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from core.settings import get_settings
from fastapi import WebSocket

from ..storage.storage import (
    get_latest_bot_run_view_state,
    get_latest_bot_runtime_event,
    record_bot_runtime_event,
    upsert_bot_run_view_state,
)
from .botlens_contract import (
    BRIDGE_BOOTSTRAP_KIND,
    BRIDGE_FACTS_KIND,
    CONTINUITY_READY,
    CONTINUITY_RESYNC_REQUIRED,
    EVENT_TYPE_LIFECYCLE,
    EVENT_TYPE_RUNTIME_FACTS,
    EVENT_TYPE_RUNTIME_BOOTSTRAP,
    LIFECYCLE_KIND,
    PROJECTION_REFRESH_KIND,
    SCHEMA_VERSION,
    build_window_payload,
    continuity_only,
    default_continuity,
    normalize_fact_entries,
    normalize_bridge_seq,
    normalize_bridge_session_id,
    normalize_ingest_kind,
    normalize_lifecycle_payload,
    projection_state_payload,
    read_projection_state,
)
from .botlens_projection import apply_series_fact_batch, normalize_series_key
from .live_series_stream import LiveSeriesStream

logger = logging.getLogger(__name__)

_SETTINGS = get_settings()
_RING_SIZE = _SETTINGS.bot_runtime.botlens.ring_size
_INGEST_QUEUE_MAX = _SETTINGS.bot_runtime.botlens.ingest_queue_max
_LIVE_TAIL_CANDLE_LIMIT = _SETTINGS.bot_runtime.botlens.max_candles
_LIFECYCLE_SEQ_OFFSET = 1_000_000_000
_BOTLENS_FALLBACK_SEQ_OFFSET = 1_100_000_000
_INT32_MAX = 2_147_483_647
_EVENT_ID_MAX_LEN = 128


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


def _series_event_id(
    *,
    bot_id: str,
    run_id: str,
    event_type: str,
    series_key: str,
    bridge_session_id: str,
    bridge_seq: int,
    projection_seq: int,
) -> str:
    base = f"{bot_id}:{run_id}:{event_type}"
    raw_suffix = f"{series_key}:{bridge_session_id}:{max(int(bridge_seq), int(projection_seq))}"
    event_id = f"{base}:{raw_suffix}"
    if len(event_id) <= _EVENT_ID_MAX_LEN:
        return event_id
    digest = hashlib.sha1(raw_suffix.encode("utf-8")).hexdigest()[:20]
    compact = f"{base}:{digest}:{max(int(bridge_seq), int(projection_seq))}"
    if len(compact) <= _EVENT_ID_MAX_LEN:
        return compact
    overflow = len(compact) - _EVENT_ID_MAX_LEN
    trimmed_base = base[: max(1, len(base) - overflow)]
    return f"{trimmed_base}:{digest}:{max(int(bridge_seq), int(projection_seq))}"


class BotTelemetryHub:
    def __init__(self) -> None:
        self._latest_view_state: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._latest_run_by_bot: Dict[str, str] = {}
        self._latest_run_lifecycle: Dict[str, Dict[str, Any]] = {}
        self._latest_lifecycle_seq: Dict[str, int] = {}
        self._ingest_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=_INGEST_QUEUE_MAX)
        self._ingest_task: Optional[asyncio.Task[None]] = None
        self._lock = asyncio.Lock()
        self._worker_lock = asyncio.Lock()
        self._live_series = LiveSeriesStream(
            ring_size=_RING_SIZE,
            schema_version=SCHEMA_VERSION,
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

    async def _publish_runtime_update(self, *, bot_id: str, run_id: str, runtime_payload: AbcMapping[str, Any], seq: int, known_at: Any) -> None:
        try:
            from .bot_service import publish_runtime_update

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
            from .bot_service import publish_projected_bot

            await asyncio.to_thread(publish_projected_bot, bot_id, inspect_container=False)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bot_telemetry_projected_bot_broadcast_failed | bot_id=%s | error=%s",
                bot_id,
                exc,
            )

    async def _load_latest_view_row(self, *, bot_id: str, run_id: str, series_key: str) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(
            get_latest_bot_run_view_state,
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
        )

    async def _next_projection_seq(self, *, bot_id: str, run_id: str, series_key: str, previous: Optional[Dict[str, Any]]) -> int:
        if isinstance(previous, AbcMapping):
            seq = _coerce_int(previous.get("seq"), default=0)
            if seq > 0:
                return seq + 1
        row = await self._load_latest_view_row(bot_id=bot_id, run_id=run_id, series_key=series_key)
        return max(1, _coerce_int((row or {}).get("seq"), default=0) + 1)

    @staticmethod
    def _raw_event_seq(payload: Mapping[str, Any], *, fallback_seq: int) -> int:
        run_seq = _coerce_int(payload.get("run_seq"), default=0)
        if run_seq > 0:
            return run_seq
        return _BOTLENS_FALLBACK_SEQ_OFFSET + max(1, int(fallback_seq))

    def _next_lifecycle_event_seq(self, *, bot_id: str, run_id: str, lifecycle_payload: Mapping[str, Any]) -> int:
        previous = self._latest_lifecycle_seq.get(run_id)
        if previous is None:
            latest = get_latest_bot_runtime_event(
                bot_id=bot_id,
                run_id=run_id,
                event_types=[EVENT_TYPE_LIFECYCLE],
            )
            previous = max(_LIFECYCLE_SEQ_OFFSET, _coerce_int((latest or {}).get("seq"), default=0))
        raw_seq = _coerce_int(lifecycle_payload.get("seq"), default=0)
        candidate = 0
        if raw_seq > 0:
            # Keep lifecycle events in a dedicated int32-safe range so persisted ordering
            # remains monotonic across restarts without violating the storage contract.
            candidate = _LIFECYCLE_SEQ_OFFSET + raw_seq
            if candidate > _INT32_MAX:
                candidate = 0
        next_seq = max(int(previous) + 1, candidate or (_LIFECYCLE_SEQ_OFFSET + 1))
        if next_seq > _INT32_MAX:
            raise RuntimeError(
                f"botlens lifecycle seq exceeded int32 range | bot_id={bot_id} | run_id={run_id} | seq={next_seq}"
            )
        self._latest_lifecycle_seq[run_id] = next_seq
        return next_seq

    async def _persist_series_projection(
        self,
        *,
        bot_id: str,
        run_id: str,
        series_key: str,
        projection_seq: int,
        event_type: str,
        state_payload: Mapping[str, Any],
        raw_payload: Mapping[str, Any],
        event_time: Any,
        known_at: Any,
    ) -> Dict[str, Any]:
        view_state_row = {
            "run_id": run_id,
            "bot_id": bot_id,
            "series_key": series_key,
            "seq": projection_seq,
            "schema_version": SCHEMA_VERSION,
            "payload": dict(state_payload or {}),
            "event_time": event_time,
            "known_at": known_at,
            "updated_at": known_at,
        }
        persisted_view = await asyncio.to_thread(upsert_bot_run_view_state, view_state_row)
        bridge_session_id = normalize_bridge_session_id(raw_payload)
        bridge_seq = normalize_bridge_seq(raw_payload)
        raw_event_payload: Dict[str, Any] = {
            "series_key": series_key,
            "projection_seq": projection_seq,
            "bridge_session_id": bridge_session_id,
            "bridge_seq": bridge_seq,
        }
        facts = normalize_fact_entries(raw_payload.get("facts"))
        if facts:
            raw_event_payload["facts"] = facts
        await asyncio.to_thread(
            record_bot_runtime_event,
            {
                "event_id": _series_event_id(
                    bot_id=bot_id,
                    run_id=run_id,
                    event_type=event_type,
                    series_key=series_key,
                    bridge_session_id=bridge_session_id,
                    bridge_seq=bridge_seq,
                    projection_seq=projection_seq,
                ),
                "bot_id": bot_id,
                "run_id": run_id,
                "seq": self._raw_event_seq(raw_payload, fallback_seq=projection_seq),
                "event_type": event_type,
                "critical": bool(event_type == EVENT_TYPE_RUNTIME_BOOTSTRAP),
                "schema_version": SCHEMA_VERSION,
                "event_time": event_time,
                "known_at": known_at,
                "payload": raw_event_payload,
            },
        )
        return dict(persisted_view or view_state_row)

    async def _persist_lifecycle_event(self, *, bot_id: str, run_id: str, lifecycle: Mapping[str, Any]) -> None:
        lifecycle_seq = self._next_lifecycle_event_seq(
            bot_id=bot_id,
            run_id=run_id,
            lifecycle_payload=lifecycle,
        )
        await asyncio.to_thread(
            record_bot_runtime_event,
            {
                "event_id": f"{bot_id}:{run_id}:{EVENT_TYPE_LIFECYCLE}:{lifecycle_seq}",
                "bot_id": bot_id,
                "run_id": run_id,
                "seq": lifecycle_seq,
                "event_type": EVENT_TYPE_LIFECYCLE,
                "critical": True,
                "schema_version": SCHEMA_VERSION,
                "event_time": lifecycle.get("checkpoint_at") or lifecycle.get("updated_at"),
                "known_at": lifecycle.get("checkpoint_at") or lifecycle.get("updated_at"),
                "payload": dict(lifecycle or {}),
            },
        )

    async def _mark_projection_resync_required(
        self,
        *,
        bot_id: str,
        run_id: str,
        series_key: str,
        previous: Dict[str, Any],
        reason: str,
        details: Dict[str, Any],
        known_at: Any,
    ) -> None:
        previous_state = read_projection_state(previous.get("payload"))
        projection_seq = await self._next_projection_seq(
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
            previous=previous,
        )
        next_payload = projection_state_payload(
            projection=previous_state["projection"],
            lifecycle=self._latest_run_lifecycle.get(run_id) or previous_state["lifecycle"],
            continuity=default_continuity(
                status=CONTINUITY_RESYNC_REQUIRED,
                reason=reason,
                bridge_session_id=continuity_only(previous.get("payload")).get("bridge_session_id"),
                bridge_seq=_coerce_int(details.get("previous_bridge_seq"), default=0),
                details=details,
                invalidated_at=known_at,
            ),
        )
        persisted = await asyncio.to_thread(
            upsert_bot_run_view_state,
            {
                "run_id": run_id,
                "bot_id": bot_id,
                "series_key": series_key,
                "seq": projection_seq,
                "schema_version": SCHEMA_VERSION,
                "payload": next_payload,
                "event_time": previous.get("event_time") or known_at,
                "known_at": known_at,
                "updated_at": known_at,
            },
        )
        async with self._lock:
            self._latest_view_state[(bot_id, run_id, series_key)] = dict(persisted or {})
        await self._invalidate_run_live_continuity(
            run_id=run_id,
            reason=reason,
            details=details,
        )

    async def _process_bridge_bootstrap(self, payload: Mapping[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        series_key = normalize_series_key(payload.get("series_key"))
        facts = normalize_fact_entries(payload.get("facts"))
        if not bot_id or not run_id or not series_key:
            logger.warning(
                "bot_telemetry_bootstrap_invalid_payload | bot_id=%s | run_id=%s | series_key=%s",
                bot_id,
                run_id,
                series_key,
            )
            return
        if not facts:
            logger.warning(
                "bot_telemetry_bootstrap_empty_facts | bot_id=%s | run_id=%s | series_key=%s",
                bot_id,
                run_id,
                series_key,
            )
            return
        key = (bot_id, run_id, series_key)
        async with self._lock:
            previous = self._latest_view_state.get(key)
        if previous is None:
            previous = await self._load_latest_view_row(bot_id=bot_id, run_id=run_id, series_key=series_key)
        previous_continuity = continuity_only((previous or {}).get("payload"))
        incoming_session = normalize_bridge_session_id(payload)
        incoming_bridge_seq = normalize_bridge_seq(payload)
        if previous and previous_continuity.get("bridge_session_id") not in {None, "", incoming_session}:
            await self._invalidate_run_live_continuity(
                run_id=run_id,
                reason="bridge_session_reset",
                details={
                    "series_key": series_key,
                    "previous_bridge_session_id": previous_continuity.get("bridge_session_id"),
                    "incoming_bridge_session_id": incoming_session,
                },
            )
        projection_seq = await self._next_projection_seq(
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
            previous=previous,
        )
        known_at = payload.get("known_at") or payload.get("event_time")
        event_time = payload.get("event_time") or known_at
        projection = apply_series_fact_batch(
            {},
            series_key=series_key,
            seq=projection_seq,
            facts=facts,
            reset=True,
        )
        next_payload = projection_state_payload(
            projection=projection,
            lifecycle=self._latest_run_lifecycle.get(run_id),
            continuity=default_continuity(
                status=CONTINUITY_READY,
                bridge_session_id=incoming_session,
                bridge_seq=incoming_bridge_seq,
                details={"run_seq": _coerce_int(payload.get("run_seq"), default=0)},
            ),
        )
        persisted = await self._persist_series_projection(
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
            projection_seq=projection_seq,
            event_type=EVENT_TYPE_RUNTIME_BOOTSTRAP,
            state_payload=next_payload,
            raw_payload=payload,
            event_time=event_time,
            known_at=known_at,
        )
        async with self._lock:
            self._latest_view_state[key] = dict(persisted or {})
            self._latest_run_by_bot[bot_id] = run_id
        runtime_payload = projection.get("runtime") if isinstance(projection.get("runtime"), AbcMapping) else {}
        if runtime_payload:
            await self._publish_runtime_update(
                bot_id=bot_id,
                run_id=run_id,
                runtime_payload=runtime_payload,
                seq=projection_seq,
                known_at=known_at,
            )
        window = build_window_payload(
            run_id=run_id,
            series_key=series_key,
            seq=projection_seq,
            event_time=event_time,
            payload=next_payload,
            limit=_LIVE_TAIL_CANDLE_LIMIT,
        )
        await self._live_series.broadcast_projection_update(window=window)

    async def _process_bridge_facts(self, payload: Mapping[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        series_key = normalize_series_key(payload.get("series_key"))
        facts = normalize_fact_entries(payload.get("facts"))
        if not bot_id or not run_id or not series_key or not facts:
            logger.warning(
                "bot_telemetry_facts_invalid_payload | bot_id=%s | run_id=%s | series_key=%s",
                bot_id,
                run_id,
                series_key,
            )
            return
        key = (bot_id, run_id, series_key)
        async with self._lock:
            previous = self._latest_view_state.get(key)
        if previous is None:
            previous = await self._load_latest_view_row(bot_id=bot_id, run_id=run_id, series_key=series_key)
        if previous is None:
            await self._invalidate_run_live_continuity(
                run_id=run_id,
                reason="baseline_missing",
                details={"series_key": series_key},
            )
            return
        previous_state = read_projection_state(previous.get("payload"))
        previous_continuity = continuity_only(previous.get("payload"))
        previous_bridge_session = str(previous_continuity.get("bridge_session_id") or "").strip()
        previous_bridge_seq = _coerce_int(previous_continuity.get("last_bridge_seq"), default=0)
        incoming_bridge_session = normalize_bridge_session_id(payload)
        incoming_bridge_seq = normalize_bridge_seq(payload)
        known_at = payload.get("known_at") or payload.get("event_time")
        if previous_continuity.get("status") != CONTINUITY_READY:
            logger.warning(
                "bot_telemetry_facts_ignored_until_rebootstrap | bot_id=%s | run_id=%s | series_key=%s | continuity_status=%s",
                bot_id,
                run_id,
                series_key,
                previous_continuity.get("status"),
            )
            return
        if not previous_bridge_session or incoming_bridge_session != previous_bridge_session:
            await self._mark_projection_resync_required(
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
                previous=previous,
                reason="bridge_session_changed",
                details={
                    "series_key": series_key,
                    "previous_bridge_session_id": previous_bridge_session or None,
                    "incoming_bridge_session_id": incoming_bridge_session,
                    "previous_bridge_seq": previous_bridge_seq,
                    "incoming_bridge_seq": incoming_bridge_seq,
                },
                known_at=known_at,
            )
            return
        expected_bridge_seq = previous_bridge_seq + 1 if previous_bridge_seq > 0 else incoming_bridge_seq
        if previous_bridge_seq > 0 and incoming_bridge_seq != expected_bridge_seq:
            await self._mark_projection_resync_required(
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
                previous=previous,
                reason="bridge_seq_gap",
                details={
                    "series_key": series_key,
                    "previous_bridge_seq": previous_bridge_seq,
                    "incoming_bridge_seq": incoming_bridge_seq,
                    "bridge_gap": max(0, incoming_bridge_seq - expected_bridge_seq),
                },
                known_at=known_at,
            )
            return
        next_projection = apply_series_fact_batch(
            previous_state["projection"],
            series_key=series_key,
            seq=_coerce_int(previous.get("seq"), default=0) + 1,
            facts=facts,
        )
        projection_seq = await self._next_projection_seq(
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
            previous=previous,
        )
        event_time = payload.get("event_time") or known_at
        next_payload = projection_state_payload(
            projection=next_projection,
            lifecycle=self._latest_run_lifecycle.get(run_id) or previous_state["lifecycle"],
            continuity=default_continuity(
                status=CONTINUITY_READY,
                bridge_session_id=incoming_bridge_session,
                bridge_seq=incoming_bridge_seq,
                details={"run_seq": _coerce_int(payload.get("run_seq"), default=0)},
            ),
        )
        persisted = await self._persist_series_projection(
            bot_id=bot_id,
            run_id=run_id,
            series_key=series_key,
            projection_seq=projection_seq,
            event_type=EVENT_TYPE_RUNTIME_FACTS,
            state_payload=next_payload,
            raw_payload=payload,
            event_time=event_time,
            known_at=known_at,
        )
        async with self._lock:
            self._latest_view_state[key] = dict(persisted or {})
            self._latest_run_by_bot[bot_id] = run_id
        runtime_payload = next_projection.get("runtime") if isinstance(next_projection.get("runtime"), AbcMapping) else {}
        if runtime_payload:
            await self._publish_runtime_update(
                bot_id=bot_id,
                run_id=run_id,
                runtime_payload=runtime_payload,
                seq=projection_seq,
                known_at=known_at,
            )
        window = build_window_payload(
            run_id=run_id,
            series_key=series_key,
            seq=projection_seq,
            event_time=event_time,
            payload=next_payload,
            limit=_LIVE_TAIL_CANDLE_LIMIT,
        )
        await self._live_series.broadcast_projection_update(window=window)

    async def _process_lifecycle_event(self, payload: Mapping[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        if not bot_id or not run_id:
            logger.warning(
                "bot_telemetry_lifecycle_invalid_payload | bot_id=%s | run_id=%s",
                bot_id,
                run_id,
            )
            return
        lifecycle = normalize_lifecycle_payload(payload)
        if not lifecycle:
            return
        async with self._lock:
            self._latest_run_lifecycle[run_id] = dict(lifecycle)
        await self._persist_lifecycle_event(bot_id=bot_id, run_id=run_id, lifecycle=lifecycle)
        await self._publish_projected_bot(bot_id=bot_id)
        await self._live_series.broadcast_run_lifecycle(run_id=run_id, lifecycle=dict(lifecycle))

    async def _process_ingest(self, item: Dict[str, Any]) -> None:
        payload = item.get("payload")
        if not isinstance(payload, AbcMapping):
            return
        kind = normalize_ingest_kind(payload.get("kind"))
        if kind == BRIDGE_BOOTSTRAP_KIND:
            await self._process_bridge_bootstrap(payload)
            return
        if kind == BRIDGE_FACTS_KIND:
            await self._process_bridge_facts(payload)
            return
        if kind == LIFECYCLE_KIND:
            await self._process_lifecycle_event(payload)
            return
        if kind == PROJECTION_REFRESH_KIND:
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


telemetry_hub = BotTelemetryHub()
