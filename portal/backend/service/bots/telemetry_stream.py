from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
from collections.abc import Mapping as AbcMapping
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from core.settings import get_settings
from fastapi import WebSocket

from ..storage.storage import (
    get_bot_run,
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
    FACT_TYPE_TRADE_UPSERTED,
    LIFECYCLE_KIND,
    PROJECTION_REFRESH_KIND,
    RUN_SCOPE_KEY,
    continuity_payload,
    normalize_bridge_seq,
    normalize_bridge_session_id,
    normalize_fact_entries,
    normalize_ingest_kind,
    normalize_lifecycle_payload,
    normalize_series_key,
)
from .botlens_run_stream import BotLensRunStream
from .botlens_state import (
    apply_fact_batch,
    build_symbol_summary,
    empty_run_summary,
    empty_symbol_detail,
    is_open_trade,
    read_run_summary_state,
    read_symbol_detail_state,
    serialize_run_summary_state,
    serialize_symbol_detail_state,
)

logger = logging.getLogger(__name__)

_SETTINGS = get_settings()
_RING_SIZE = _SETTINGS.bot_runtime.botlens.ring_size
_INGEST_QUEUE_MAX = _SETTINGS.bot_runtime.botlens.ingest_queue_max
_ACTIVE_RUN_TTL_S = 1800.0
_TERMINAL_RUN_TTL_S = 300.0
_PRUNE_INTERVAL_S = 15.0
_LIFECYCLE_SEQ_OFFSET = 1_000_000_000
_BOTLENS_FALLBACK_SEQ_OFFSET = 1_100_000_000
_INT32_MAX = 2_147_483_647
_EVENT_ID_MAX_LEN = 128
_TERMINAL_LIFECYCLE_PHASES = {"completed", "stopped", "error", "failed", "crashed", "startup_failed"}
_TERMINAL_LIFECYCLE_STATUSES = {"completed", "stopped", "error", "failed", "crashed", "startup_failed"}


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


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, AbcMapping) else {}


def _payload_size_bytes(value: Any) -> int:
    try:
        return len(json.dumps(_sanitize_json(value), separators=(",", ":"), default=str).encode("utf-8"))
    except Exception:
        return len(str(value).encode("utf-8"))


def _event_id(
    *,
    bot_id: str,
    run_id: str,
    event_type: str,
    symbol_key: str,
    bridge_session_id: str,
    bridge_seq: int,
    seq: int,
) -> str:
    base = f"{bot_id}:{run_id}:{event_type}"
    raw_suffix = f"{symbol_key}:{bridge_session_id}:{max(int(bridge_seq), int(seq))}"
    event_id = f"{base}:{raw_suffix}"
    if len(event_id) <= _EVENT_ID_MAX_LEN:
        return event_id
    digest = hashlib.sha1(raw_suffix.encode("utf-8")).hexdigest()[:20]
    compact = f"{base}:{digest}:{max(int(bridge_seq), int(seq))}"
    if len(compact) <= _EVENT_ID_MAX_LEN:
        return compact
    overflow = len(compact) - _EVENT_ID_MAX_LEN
    trimmed_base = base[: max(1, len(base) - overflow)]
    return f"{trimmed_base}:{digest}:{max(int(bridge_seq), int(seq))}"


class BotTelemetryHub:
    def __init__(self) -> None:
        self._latest_summary_state: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._latest_detail_state: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._latest_run_by_bot: Dict[str, str] = {}
        self._latest_run_lifecycle: Dict[str, Dict[str, Any]] = {}
        self._latest_lifecycle_seq: Dict[str, int] = {}
        self._run_last_activity: Dict[str, float] = {}
        self._run_terminal_at: Dict[str, float] = {}
        self._ingest_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=_INGEST_QUEUE_MAX)
        self._ingest_task: Optional[asyncio.Task[None]] = None
        self._prune_task: Optional[asyncio.Task[None]] = None
        self._last_prune_started_monotonic = 0.0
        self._lock = asyncio.Lock()
        self._worker_lock = asyncio.Lock()
        self._run_stream = BotLensRunStream(ring_size=_RING_SIZE)

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

    async def _load_summary_state(self, *, bot_id: str, run_id: str) -> Dict[str, Any]:
        started = time.perf_counter()
        key = (str(bot_id), str(run_id))
        async with self._lock:
            cached = self._latest_summary_state.get(key)
        if cached is not None:
            logger.debug(
                "botlens_summary_state_loaded | bot_id=%s | run_id=%s | source=memory | load_ms=%.3f",
                bot_id,
                run_id,
                (time.perf_counter() - started) * 1000.0,
            )
            return dict(cached)
        row = await asyncio.to_thread(get_latest_bot_run_view_state, bot_id=bot_id, run_id=run_id, series_key=RUN_SCOPE_KEY)
        state = read_run_summary_state(_mapping(row).get("payload"), bot_id=bot_id, run_id=run_id)
        async with self._lock:
            self._latest_summary_state[key] = dict(state)
        logger.debug(
            "botlens_summary_state_loaded | bot_id=%s | run_id=%s | source=storage | load_ms=%.3f",
            bot_id,
            run_id,
            (time.perf_counter() - started) * 1000.0,
        )
        return state

    async def _load_detail_state(self, *, bot_id: str, run_id: str, symbol_key: str) -> Dict[str, Any]:
        started = time.perf_counter()
        normalized_symbol_key = normalize_series_key(symbol_key)
        key = (str(bot_id), str(run_id), normalized_symbol_key)
        async with self._lock:
            cached = self._latest_detail_state.get(key)
        if cached is not None:
            logger.debug(
                "botlens_detail_state_loaded | bot_id=%s | run_id=%s | symbol_key=%s | source=memory | load_ms=%.3f",
                bot_id,
                run_id,
                normalized_symbol_key,
                (time.perf_counter() - started) * 1000.0,
            )
            return dict(cached)
        row = await asyncio.to_thread(
            get_latest_bot_run_view_state,
            bot_id=bot_id,
            run_id=run_id,
            series_key=normalized_symbol_key,
        )
        state = read_symbol_detail_state(_mapping(row).get("payload"), symbol_key=normalized_symbol_key)
        async with self._lock:
            self._latest_detail_state[key] = dict(state)
        logger.debug(
            "botlens_detail_state_loaded | bot_id=%s | run_id=%s | symbol_key=%s | source=storage | load_ms=%.3f",
            bot_id,
            run_id,
            normalized_symbol_key,
            (time.perf_counter() - started) * 1000.0,
        )
        return state

    async def _persist_summary_state(self, *, bot_id: str, run_id: str, seq: int, summary_state: Mapping[str, Any], event_time: Any, known_at: Any) -> Dict[str, Any]:
        row = await asyncio.to_thread(
            upsert_bot_run_view_state,
            {
                "run_id": run_id,
                "bot_id": bot_id,
                "series_key": RUN_SCOPE_KEY,
                "seq": int(seq),
                "schema_version": int(summary_state.get("schema_version") or 4),
                "payload": serialize_run_summary_state(summary_state),
                "event_time": event_time,
                "known_at": known_at,
                "updated_at": known_at,
            },
        )
        async with self._lock:
            self._latest_summary_state[(bot_id, run_id)] = dict(summary_state)
        return dict(row or {})

    async def _persist_detail_state(self, *, bot_id: str, run_id: str, symbol_key: str, seq: int, detail_state: Mapping[str, Any], event_time: Any, known_at: Any) -> Dict[str, Any]:
        row = await asyncio.to_thread(
            upsert_bot_run_view_state,
            {
                "run_id": run_id,
                "bot_id": bot_id,
                "series_key": normalize_series_key(symbol_key),
                "seq": int(seq),
                "schema_version": int(detail_state.get("schema_version") or 4),
                "payload": serialize_symbol_detail_state(detail_state),
                "event_time": event_time,
                "known_at": known_at,
                "updated_at": known_at,
            },
        )
        async with self._lock:
            self._latest_detail_state[(bot_id, run_id, normalize_series_key(symbol_key))] = dict(detail_state)
        return dict(row or {})

    async def _record_raw_runtime_event(
        self,
        *,
        bot_id: str,
        run_id: str,
        symbol_key: str,
        seq: int,
        event_type: str,
        raw_payload: Mapping[str, Any],
        event_time: Any,
        known_at: Any,
    ) -> None:
        bridge_session_id = normalize_bridge_session_id(raw_payload)
        bridge_seq = normalize_bridge_seq(raw_payload)
        event_payload: Dict[str, Any] = {
            "series_key": normalize_series_key(symbol_key),
            "run_seq": int(seq),
            "bridge_session_id": bridge_session_id,
            "bridge_seq": bridge_seq,
        }
        facts = normalize_fact_entries(raw_payload.get("facts"))
        if facts:
            event_payload["facts"] = facts
        await asyncio.to_thread(
            record_bot_runtime_event,
            {
                "event_id": _event_id(
                    bot_id=bot_id,
                    run_id=run_id,
                    event_type=event_type,
                    symbol_key=normalize_series_key(symbol_key),
                    bridge_session_id=bridge_session_id,
                    bridge_seq=bridge_seq,
                    seq=seq,
                ),
                "bot_id": bot_id,
                "run_id": run_id,
                "seq": int(seq),
                "event_type": event_type,
                "critical": bool(event_type == EVENT_TYPE_RUNTIME_BOOTSTRAP),
                "schema_version": 4,
                "event_time": event_time,
                "known_at": known_at,
                "payload": event_payload,
            },
        )

    @staticmethod
    def _raw_event_seq(payload: Mapping[str, Any], *, previous_seq: int) -> int:
        run_seq = _coerce_int(payload.get("run_seq"), default=0)
        if run_seq > 0:
            return run_seq
        return _BOTLENS_FALLBACK_SEQ_OFFSET + max(1, int(previous_seq) + 1)

    async def _next_lifecycle_event_seq(self, *, bot_id: str, run_id: str, lifecycle_payload: Mapping[str, Any]) -> int:
        async with self._lock:
            previous = self._latest_lifecycle_seq.get(run_id)
        if previous is None:
            fetch_started = time.perf_counter()
            latest = await asyncio.to_thread(
                get_latest_bot_runtime_event,
                bot_id=bot_id,
                run_id=run_id,
                event_types=[EVENT_TYPE_LIFECYCLE],
            )
            previous = max(_LIFECYCLE_SEQ_OFFSET, _coerce_int((latest or {}).get("seq"), default=0))
            logger.debug(
                "botlens_lifecycle_seq_loaded | bot_id=%s | run_id=%s | previous_seq=%s | load_ms=%.3f",
                bot_id,
                run_id,
                previous,
                (time.perf_counter() - fetch_started) * 1000.0,
            )
        raw_seq = _coerce_int(lifecycle_payload.get("seq"), default=0)
        candidate = 0
        if raw_seq > 0:
            candidate = _LIFECYCLE_SEQ_OFFSET + raw_seq
            if candidate > _INT32_MAX:
                candidate = 0
        next_seq = max(int(previous) + 1, candidate or (_LIFECYCLE_SEQ_OFFSET + 1))
        if next_seq > _INT32_MAX:
            raise RuntimeError(
                f"botlens lifecycle seq exceeded int32 range | bot_id={bot_id} | run_id={run_id} | seq={next_seq}"
            )
        async with self._lock:
            current = self._latest_lifecycle_seq.get(run_id)
            if current is not None:
                next_seq = max(int(current) + 1, next_seq)
            self._latest_lifecycle_seq[run_id] = next_seq
        return next_seq

    async def _persist_lifecycle_event(self, *, bot_id: str, run_id: str, lifecycle: Mapping[str, Any]) -> None:
        lifecycle_seq = await self._next_lifecycle_event_seq(
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
                "schema_version": 4,
                "event_time": lifecycle.get("checkpoint_at") or lifecycle.get("updated_at"),
                "known_at": lifecycle.get("checkpoint_at") or lifecycle.get("updated_at"),
                "payload": dict(lifecycle or {}),
            },
        )

    @staticmethod
    def _build_run_meta(*, bot_id: str, run_id: str, row: Mapping[str, Any] | None) -> Dict[str, Any]:
        row = _mapping(row)
        return {
            "run_id": str(run_id),
            "bot_id": str(row.get("bot_id") or bot_id).strip() or bot_id,
            "strategy_id": row.get("strategy_id"),
            "strategy_name": row.get("strategy_name"),
            "run_type": row.get("run_type"),
            "datasource": row.get("datasource"),
            "exchange": row.get("exchange"),
            "symbols": list(row.get("symbols") or []) if isinstance(row.get("symbols"), list) else [],
            "started_at": row.get("started_at"),
            "ended_at": row.get("ended_at"),
        }

    async def _ensure_run_meta(self, *, bot_id: str, run_id: str, summary_state: Dict[str, Any]) -> Dict[str, Any]:
        cached = _mapping(summary_state.get("run_meta"))
        if cached.get("run_id") == str(run_id):
            return cached
        fetch_started = time.perf_counter()
        row = await asyncio.to_thread(get_bot_run, str(run_id))
        meta = self._build_run_meta(bot_id=bot_id, run_id=run_id, row=row)
        summary_state["run_meta"] = meta
        logger.debug(
            "botlens_run_meta_loaded | bot_id=%s | run_id=%s | fetch_ms=%.3f",
            bot_id,
            run_id,
            (time.perf_counter() - fetch_started) * 1000.0,
        )
        return meta

    @staticmethod
    def _group_open_trades_by_symbol(open_trades_index: Mapping[str, Any]) -> Dict[str, list[Dict[str, Any]]]:
        grouped: Dict[str, list[Dict[str, Any]]] = {}
        for trade in open_trades_index.values():
            if not isinstance(trade, AbcMapping):
                continue
            symbol_key = normalize_series_key(trade.get("symbol_key"))
            if not symbol_key:
                continue
            grouped.setdefault(symbol_key, []).append(dict(trade))
        return grouped

    def _refresh_summary_for_symbol(self, summary_state: Dict[str, Any], detail_state: Mapping[str, Any]) -> Dict[str, Any]:
        open_trades_by_symbol = self._group_open_trades_by_symbol(summary_state.get("open_trades_index") or {})
        symbol_key = normalize_series_key(detail_state.get("symbol_key"))
        symbol_summary = build_symbol_summary(detail_state, open_trades=open_trades_by_symbol.get(symbol_key, []))
        summary_state.setdefault("symbol_index", {})[symbol_key] = symbol_summary
        return symbol_summary

    @staticmethod
    def _merge_open_trades(summary_state: Dict[str, Any], *, upserts: list[Mapping[str, Any]], removals: list[str]) -> None:
        open_trades_index = summary_state.setdefault("open_trades_index", {})
        for trade_id in removals:
            open_trades_index.pop(str(trade_id), None)
        for trade in upserts:
            if not isinstance(trade, AbcMapping):
                continue
            trade_id = str(trade.get("trade_id") or "").strip()
            if not trade_id:
                continue
            if is_open_trade(trade):
                open_trades_index[trade_id] = dict(trade)
            else:
                open_trades_index.pop(trade_id, None)

    @staticmethod
    def _runtime_warning_count(runtime_payload: Mapping[str, Any]) -> int:
        warnings = runtime_payload.get("warnings")
        return len(warnings) if isinstance(warnings, list) else 0

    @staticmethod
    def _runtime_warnings(runtime_payload: Mapping[str, Any]) -> list[Dict[str, Any]]:
        warnings = runtime_payload.get("warnings")
        if not isinstance(warnings, list):
            return []
        return [dict(entry) for entry in warnings if isinstance(entry, AbcMapping)]

    def _refresh_summary_health(
        self,
        summary_state: Dict[str, Any],
        *,
        runtime_payload: Mapping[str, Any] | None,
        lifecycle_payload: Mapping[str, Any] | None,
        known_at: Any,
    ) -> None:
        health = dict(summary_state.get("health") or {})
        runtime = _mapping(runtime_payload)
        lifecycle = _mapping(lifecycle_payload)
        if runtime:
            health["status"] = str(runtime.get("status") or health.get("status") or "waiting")
            health["worker_count"] = int(runtime.get("worker_count") or health.get("worker_count") or 0)
            health["active_workers"] = int(runtime.get("active_workers") or health.get("active_workers") or 0)
            health["warning_count"] = self._runtime_warning_count(runtime)
            health["warnings"] = self._runtime_warnings(runtime)
        if lifecycle:
            health["phase"] = lifecycle.get("phase") or health.get("phase")
            if not runtime:
                health["status"] = str(lifecycle.get("status") or health.get("status") or "waiting")
        health["last_event_at"] = known_at
        summary_state["health"] = health

    async def _touch_run(self, *, bot_id: str, run_id: str, terminal: bool = False) -> None:
        now = time.monotonic()
        async with self._lock:
            self._run_last_activity[run_id] = now
            self._latest_run_by_bot[bot_id] = run_id
            if terminal:
                self._run_terminal_at[run_id] = now
            else:
                self._run_terminal_at.pop(run_id, None)

    def _cache_stats_locked(self) -> Dict[str, int]:
        return {
            "run_count": len(self._run_last_activity),
            "summary_cache_size": len(self._latest_summary_state),
            "detail_cache_size": len(self._latest_detail_state),
            "lifecycle_cache_size": len(self._latest_run_lifecycle),
            "lifecycle_seq_cache_size": len(self._latest_lifecycle_seq),
            "latest_run_by_bot_size": len(self._latest_run_by_bot),
        }

    def _evict_run_state_locked(self, run_id: str) -> Dict[str, int]:
        removed = {
            "summary_states": 0,
            "detail_states": 0,
            "lifecycle_states": 0,
            "lifecycle_seq_entries": 0,
            "latest_run_refs": 0,
            "activity_entries": 0,
            "terminal_entries": 0,
        }
        if self._run_last_activity.pop(run_id, None) is not None:
            removed["activity_entries"] = 1
        if self._run_terminal_at.pop(run_id, None) is not None:
            removed["terminal_entries"] = 1
        if self._latest_run_lifecycle.pop(run_id, None) is not None:
            removed["lifecycle_states"] = 1
        if self._latest_lifecycle_seq.pop(run_id, None) is not None:
            removed["lifecycle_seq_entries"] = 1
        summary_keys = [key for key in self._latest_summary_state if key[1] == run_id]
        for key in summary_keys:
            self._latest_summary_state.pop(key, None)
        removed["summary_states"] = len(summary_keys)
        detail_keys = [key for key in self._latest_detail_state if key[1] == run_id]
        for key in detail_keys:
            self._latest_detail_state.pop(key, None)
        removed["detail_states"] = len(detail_keys)
        stale_bot_ids = [bot_id for bot_id, latest_run_id in self._latest_run_by_bot.items() if latest_run_id == run_id]
        for bot_id in stale_bot_ids:
            self._latest_run_by_bot.pop(bot_id, None)
        removed["latest_run_refs"] = len(stale_bot_ids)
        return removed

    def _on_prune_task_done(self, task: asyncio.Task[None]) -> None:
        if self._prune_task is task:
            self._prune_task = None
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("botlens_prune_failed | error=%s", exc)

    async def _run_scheduled_prune(self, *, reason: str, delay_s: float) -> None:
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        await self._run_prune_pass(reason=reason)

    async def _schedule_prune(self, *, reason: str, force: bool = False) -> None:
        async with self._lock:
            task = self._prune_task
            now = time.monotonic()
            if task is not None and not task.done():
                return
            delay_s = 0.0
            if not force and self._last_prune_started_monotonic:
                delay_s = max(0.0, _PRUNE_INTERVAL_S - (now - self._last_prune_started_monotonic))
            self._prune_task = asyncio.create_task(
                self._run_scheduled_prune(reason=reason, delay_s=delay_s),
                name="botlens-prune",
            )
            self._prune_task.add_done_callback(self._on_prune_task_done)
        logger.debug(
            "botlens_prune_scheduled | reason=%s | delay_s=%.3f",
            reason,
            delay_s,
        )

    async def _run_prune_pass(self, *, reason: str) -> None:
        started = time.perf_counter()
        now = time.monotonic()
        evictions: list[tuple[str, Dict[str, int]]] = []
        async with self._lock:
            self._last_prune_started_monotonic = now
            run_count_before = len(self._run_last_activity)
            cache_before = self._cache_stats_locked()
            for run_id, last_activity in list(self._run_last_activity.items()):
                terminal_at = self._run_terminal_at.get(run_id)
                ttl = _TERMINAL_RUN_TTL_S if terminal_at is not None else _ACTIVE_RUN_TTL_S
                if now - float(last_activity) <= ttl:
                    continue
                if self._run_stream.viewer_count_for_run(run_id) > 0:
                    continue
                evictions.append((run_id, self._evict_run_state_locked(run_id)))
            cache_after = self._cache_stats_locked()
        for run_id, _ in evictions:
            await self._run_stream.evict_run(run_id=run_id)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        logger.info(
            "botlens_prune_completed | reason=%s | considered_runs=%s | evicted_runs=%s | elapsed_ms=%.3f | summary_cache_size=%s | detail_cache_size=%s | lifecycle_cache_size=%s | lifecycle_seq_cache_size=%s | latest_run_by_bot_size=%s | viewer_runs=%s | viewer_count=%s | ring_runs=%s | ring_messages=%s",
            reason,
            run_count_before,
            len(evictions),
            elapsed_ms,
            cache_after["summary_cache_size"],
            cache_after["detail_cache_size"],
            cache_after["lifecycle_cache_size"],
            cache_after["lifecycle_seq_cache_size"],
            cache_after["latest_run_by_bot_size"],
            self._run_stream.viewer_run_count(),
            self._run_stream.viewer_count(),
            self._run_stream.ring_run_count(),
            self._run_stream.ring_message_count(),
        )
        if cache_before != cache_after:
            logger.debug(
                "botlens_prune_cache_delta | reason=%s | before=%s | after=%s",
                reason,
                cache_before,
                cache_after,
            )
        for run_id, removed in evictions:
            logger.info(
                "botlens_run_cache_evicted | run_id=%s | removed=%s",
                run_id,
                removed,
            )

    async def _process_bridge_bootstrap(self, payload: Mapping[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        symbol_key = normalize_series_key(payload.get("series_key"))
        facts = normalize_fact_entries(payload.get("facts"))
        if not bot_id or not run_id or not symbol_key or not facts:
            logger.warning(
                "bot_telemetry_bootstrap_invalid_payload | bot_id=%s | run_id=%s | symbol_key=%s",
                bot_id,
                run_id,
                symbol_key,
            )
            return

        summary_state = await self._load_summary_state(bot_id=bot_id, run_id=run_id)
        detail_state = empty_symbol_detail(symbol_key)
        seq = self._raw_event_seq(payload, previous_seq=int(summary_state.get("seq") or 0))
        known_at = payload.get("known_at") or payload.get("event_time")
        event_time = payload.get("event_time") or known_at
        continuity = continuity_payload(
            status=CONTINUITY_READY,
            bridge_session_id=normalize_bridge_session_id(payload),
            bridge_seq=normalize_bridge_seq(payload),
            details={"run_seq": _coerce_int(payload.get("run_seq"), default=0)},
        )
        applied = apply_fact_batch(
            detail_state,
            facts=facts,
            seq=seq,
            event_time=known_at,
            continuity=continuity,
        )
        detail_state = dict(applied["detail"])
        summary_state["seq"] = int(seq)
        await self._ensure_run_meta(bot_id=bot_id, run_id=run_id, summary_state=summary_state)
        summary_state["lifecycle"] = dict(self._latest_run_lifecycle.get(run_id) or summary_state.get("lifecycle") or {})
        self._merge_open_trades(
            summary_state,
            upserts=applied["delta"]["trade_upserts"],
            removals=applied["delta"]["trade_removals"],
        )
        symbol_summary = self._refresh_summary_for_symbol(summary_state, detail_state)
        self._refresh_summary_health(
            summary_state,
            runtime_payload=detail_state.get("runtime"),
            lifecycle_payload=summary_state.get("lifecycle"),
            known_at=known_at,
        )

        await self._persist_detail_state(
            bot_id=bot_id,
            run_id=run_id,
            symbol_key=symbol_key,
            seq=seq,
            detail_state=detail_state,
            event_time=event_time,
            known_at=known_at,
        )
        await self._persist_summary_state(
            bot_id=bot_id,
            run_id=run_id,
            seq=seq,
            summary_state=summary_state,
            event_time=event_time,
            known_at=known_at,
        )
        await self._record_raw_runtime_event(
            bot_id=bot_id,
            run_id=run_id,
            symbol_key=symbol_key,
            seq=seq,
            event_type=EVENT_TYPE_RUNTIME_BOOTSTRAP,
            raw_payload=payload,
            event_time=event_time,
            known_at=known_at,
        )
        await self._touch_run(bot_id=bot_id, run_id=run_id)
        if detail_state.get("runtime"):
            await self._publish_runtime_update(
                bot_id=bot_id,
                run_id=run_id,
                runtime_payload=_mapping(detail_state.get("runtime")),
                seq=seq,
                known_at=known_at,
            )
        await self._run_stream.broadcast_summary_delta(
            run_id=run_id,
            seq=seq,
            health=_mapping(summary_state.get("health")),
            lifecycle=_mapping(summary_state.get("lifecycle")),
            symbol_upserts=[symbol_summary],
        )
        if applied["delta"]["trade_upserts"] or applied["delta"]["trade_removals"]:
            await self._run_stream.broadcast_open_trades_delta(
                run_id=run_id,
                seq=seq,
                upserts=applied["delta"]["trade_upserts"],
                removals=applied["delta"]["trade_removals"],
            )
        await self._run_stream.broadcast_detail_delta(
            run_id=run_id,
            symbol_key=symbol_key,
            seq=seq,
            payload=_sanitize_json(applied["delta"]),
        )
        logger.info(
            "botlens_bootstrap_applied | bot_id=%s | run_id=%s | symbol_key=%s | seq=%s | payload_bytes=%s",
            bot_id,
            run_id,
            symbol_key,
            seq,
            _payload_size_bytes(payload),
        )

    async def _process_bridge_facts(self, payload: Mapping[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        symbol_key = normalize_series_key(payload.get("series_key"))
        facts = normalize_fact_entries(payload.get("facts"))
        if not bot_id or not run_id or not symbol_key or not facts:
            logger.warning(
                "bot_telemetry_facts_invalid_payload | bot_id=%s | run_id=%s | symbol_key=%s",
                bot_id,
                run_id,
                symbol_key,
            )
            return
        summary_state = await self._load_summary_state(bot_id=bot_id, run_id=run_id)
        detail_state = await self._load_detail_state(bot_id=bot_id, run_id=run_id, symbol_key=symbol_key)
        seq = self._raw_event_seq(payload, previous_seq=int(summary_state.get("seq") or 0))
        known_at = payload.get("known_at") or payload.get("event_time")
        event_time = payload.get("event_time") or known_at

        previous_continuity = _mapping(detail_state.get("continuity"))
        previous_bridge_session = str(previous_continuity.get("bridge_session_id") or "").strip()
        previous_bridge_seq = _coerce_int(previous_continuity.get("last_bridge_seq"), default=0)
        incoming_bridge_session = normalize_bridge_session_id(payload)
        incoming_bridge_seq = normalize_bridge_seq(payload)

        continuity = None
        if previous_continuity.get("status") not in {None, "", CONTINUITY_READY}:
            logger.warning(
                "botlens_detail_waiting_for_rebootstrap | run_id=%s | symbol_key=%s | continuity_status=%s",
                run_id,
                symbol_key,
                previous_continuity.get("status"),
            )
            return
        if not previous_bridge_session or incoming_bridge_session != previous_bridge_session:
            continuity = continuity_payload(
                status=CONTINUITY_RESYNC_REQUIRED,
                reason="bridge_session_changed",
                bridge_session_id=incoming_bridge_session,
                bridge_seq=incoming_bridge_seq,
                details={
                    "previous_bridge_session_id": previous_bridge_session or None,
                    "incoming_bridge_session_id": incoming_bridge_session,
                },
                invalidated_at=known_at,
            )
        else:
            expected_bridge_seq = previous_bridge_seq + 1 if previous_bridge_seq > 0 else incoming_bridge_seq
            if previous_bridge_seq > 0 and incoming_bridge_seq != expected_bridge_seq:
                continuity = continuity_payload(
                    status=CONTINUITY_RESYNC_REQUIRED,
                    reason="bridge_seq_gap",
                    bridge_session_id=incoming_bridge_session,
                    bridge_seq=incoming_bridge_seq,
                    details={
                        "previous_bridge_seq": previous_bridge_seq,
                        "incoming_bridge_seq": incoming_bridge_seq,
                    },
                    invalidated_at=known_at,
                )

        if continuity is not None:
            detail_state = dict(detail_state)
            detail_state["seq"] = int(seq)
            detail_state["last_event_at"] = known_at
            detail_state["continuity"] = continuity
            summary_state["seq"] = int(seq)
            await self._ensure_run_meta(bot_id=bot_id, run_id=run_id, summary_state=summary_state)
            summary_state["lifecycle"] = dict(self._latest_run_lifecycle.get(run_id) or summary_state.get("lifecycle") or {})
            symbol_summary = self._refresh_summary_for_symbol(summary_state, detail_state)
            self._refresh_summary_health(
                summary_state,
                runtime_payload=detail_state.get("runtime"),
                lifecycle_payload=summary_state.get("lifecycle"),
                known_at=known_at,
            )
            await self._persist_detail_state(
                bot_id=bot_id,
                run_id=run_id,
                symbol_key=symbol_key,
                seq=seq,
                detail_state=detail_state,
                event_time=event_time,
                known_at=known_at,
            )
            await self._persist_summary_state(
                bot_id=bot_id,
                run_id=run_id,
                seq=seq,
                summary_state=summary_state,
                event_time=event_time,
                known_at=known_at,
            )
            await self._touch_run(bot_id=bot_id, run_id=run_id)
            await self._run_stream.broadcast_summary_delta(
                run_id=run_id,
                seq=seq,
                health=_mapping(summary_state.get("health")),
                lifecycle=_mapping(summary_state.get("lifecycle")),
                symbol_upserts=[symbol_summary],
            )
            await self._run_stream.broadcast_detail_delta(
                run_id=run_id,
                symbol_key=symbol_key,
                seq=seq,
                payload={"symbol_key": symbol_key, "detail_seq": seq, "event_time": event_time, "continuity": continuity},
            )
            logger.warning(
                "botlens_detail_continuity_invalidated | bot_id=%s | run_id=%s | symbol_key=%s | reason=%s",
                bot_id,
                run_id,
                symbol_key,
                continuity.get("reason"),
            )
            return

        continuity = continuity_payload(
            status=CONTINUITY_READY,
            bridge_session_id=incoming_bridge_session,
            bridge_seq=incoming_bridge_seq,
            details={"run_seq": _coerce_int(payload.get("run_seq"), default=0)},
        )
        applied = apply_fact_batch(
            detail_state,
            facts=facts,
            seq=seq,
            event_time=known_at,
            continuity=continuity,
        )
        detail_state = dict(applied["detail"])
        summary_state["seq"] = int(seq)
        await self._ensure_run_meta(bot_id=bot_id, run_id=run_id, summary_state=summary_state)
        summary_state["lifecycle"] = dict(self._latest_run_lifecycle.get(run_id) or summary_state.get("lifecycle") or {})
        self._merge_open_trades(
            summary_state,
            upserts=applied["delta"]["trade_upserts"],
            removals=applied["delta"]["trade_removals"],
        )
        symbol_summary = self._refresh_summary_for_symbol(summary_state, detail_state)
        self._refresh_summary_health(
            summary_state,
            runtime_payload=detail_state.get("runtime"),
            lifecycle_payload=summary_state.get("lifecycle"),
            known_at=known_at,
        )

        await self._persist_detail_state(
            bot_id=bot_id,
            run_id=run_id,
            symbol_key=symbol_key,
            seq=seq,
            detail_state=detail_state,
            event_time=event_time,
            known_at=known_at,
        )
        await self._persist_summary_state(
            bot_id=bot_id,
            run_id=run_id,
            seq=seq,
            summary_state=summary_state,
            event_time=event_time,
            known_at=known_at,
        )
        await self._record_raw_runtime_event(
            bot_id=bot_id,
            run_id=run_id,
            symbol_key=symbol_key,
            seq=seq,
            event_type=EVENT_TYPE_RUNTIME_FACTS,
            raw_payload=payload,
            event_time=event_time,
            known_at=known_at,
        )
        await self._touch_run(bot_id=bot_id, run_id=run_id)
        if detail_state.get("runtime"):
            await self._publish_runtime_update(
                bot_id=bot_id,
                run_id=run_id,
                runtime_payload=_mapping(detail_state.get("runtime")),
                seq=seq,
                known_at=known_at,
            )
        await self._run_stream.broadcast_summary_delta(
            run_id=run_id,
            seq=seq,
            health=_mapping(summary_state.get("health")),
            lifecycle=_mapping(summary_state.get("lifecycle")),
            symbol_upserts=[symbol_summary],
        )
        if applied["delta"]["trade_upserts"] or applied["delta"]["trade_removals"]:
            await self._run_stream.broadcast_open_trades_delta(
                run_id=run_id,
                seq=seq,
                upserts=applied["delta"]["trade_upserts"],
                removals=applied["delta"]["trade_removals"],
            )
        await self._run_stream.broadcast_detail_delta(
            run_id=run_id,
            symbol_key=symbol_key,
            seq=seq,
            payload=_sanitize_json(applied["delta"]),
        )

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
        summary_state = await self._load_summary_state(bot_id=bot_id, run_id=run_id)
        seq = max(int(summary_state.get("seq") or 0) + 1, _coerce_int(payload.get("seq"), default=0))
        known_at = lifecycle.get("checkpoint_at") or lifecycle.get("updated_at")
        summary_state["seq"] = int(seq)
        await self._ensure_run_meta(bot_id=bot_id, run_id=run_id, summary_state=summary_state)
        summary_state["lifecycle"] = dict(lifecycle)
        self._refresh_summary_health(
            summary_state,
            runtime_payload=None,
            lifecycle_payload=lifecycle,
            known_at=known_at,
        )
        await self._persist_summary_state(
            bot_id=bot_id,
            run_id=run_id,
            seq=seq,
            summary_state=summary_state,
            event_time=known_at,
            known_at=known_at,
        )
        terminal = (
            str(lifecycle.get("phase") or "").strip().lower() in _TERMINAL_LIFECYCLE_PHASES
            or str(lifecycle.get("status") or "").strip().lower() in _TERMINAL_LIFECYCLE_STATUSES
        )
        await self._touch_run(bot_id=bot_id, run_id=run_id, terminal=terminal)
        await self._publish_projected_bot(bot_id=bot_id)
        await self._run_stream.broadcast_summary_delta(
            run_id=run_id,
            seq=seq,
            health=_mapping(summary_state.get("health")),
            lifecycle=lifecycle,
            symbol_upserts=[],
        )

    async def _process_ingest(self, item: Dict[str, Any]) -> None:
        started = time.perf_counter()
        payload = item.get("payload")
        if not isinstance(payload, AbcMapping):
            return
        kind = normalize_ingest_kind(payload.get("kind"))
        bot_id = str(payload.get("bot_id") or "").strip() or None
        run_id = str(payload.get("run_id") or "").strip() or None
        if kind == BRIDGE_BOOTSTRAP_KIND:
            await self._process_bridge_bootstrap(payload)
        elif kind == BRIDGE_FACTS_KIND:
            await self._process_bridge_facts(payload)
        elif kind == LIFECYCLE_KIND:
            await self._process_lifecycle_event(payload)
        elif kind == PROJECTION_REFRESH_KIND:
            bot_id = str(payload.get("bot_id") or "").strip()
            if bot_id:
                await self._publish_projected_bot(bot_id=bot_id)
        else:
            logger.warning("bot_telemetry_ingest_unknown_kind | kind=%s", kind)
        await self._schedule_prune(reason=f"ingest:{kind or 'unknown'}")
        logger.debug(
            "botlens_ingest_processed | kind=%s | bot_id=%s | run_id=%s | queue_depth=%s | queue_wait_ms=%.3f | process_ms=%.3f | payload_bytes=%s",
            kind,
            bot_id,
            run_id,
            self._ingest_queue.qsize(),
            max(0.0, (time.monotonic() - float(item.get("enqueued_monotonic") or time.monotonic())) * 1000.0),
            (time.perf_counter() - started) * 1000.0,
            _payload_size_bytes(payload),
        )

    async def add_run_viewer(
        self,
        *,
        run_id: str,
        ws: WebSocket,
        cursor_seq: int = 0,
        selected_symbol_key: str | None = None,
        hot_symbols: list[str] | None = None,
    ) -> None:
        await self._ensure_workers()
        await self._run_stream.add_run_viewer(
            run_id=str(run_id),
            ws=ws,
            cursor_seq=cursor_seq,
            selected_symbol_key=selected_symbol_key,
            hot_symbols=hot_symbols or [],
        )
        async with self._lock:
            self._run_last_activity[str(run_id)] = time.monotonic()

    async def update_run_viewer(self, *, run_id: str, ws: WebSocket, payload: Mapping[str, Any]) -> None:
        await self._run_stream.update_viewer_subscription(run_id=str(run_id), ws=ws, payload=payload)
        async with self._lock:
            self._run_last_activity[str(run_id)] = time.monotonic()

    async def remove_run_viewer(self, *, run_id: str, ws: WebSocket) -> None:
        await self._run_stream.remove_run_viewer(run_id=str(run_id), ws=ws)
        await self._schedule_prune(reason="viewer_removed")


telemetry_hub = BotTelemetryHub()
