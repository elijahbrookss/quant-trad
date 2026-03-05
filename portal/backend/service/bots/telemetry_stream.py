from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from collections import defaultdict
from collections.abc import Mapping as AbcMapping
from datetime import datetime
from typing import Any, DefaultDict, Dict, Optional, Tuple

from fastapi import WebSocket

from ..storage.storage import (
    get_latest_bot_run_view_state,
    get_latest_bot_runtime_run_id,
    upsert_bot_run_view_state,
)
from .botlens_series_service import build_live_tail_messages

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_MAX_SERIES = 12
_QTY_EPSILON = 1e-9


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


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _resolve_limit(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        parsed = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{name} must be an integer >= 0; received {raw!r}") from exc
    if parsed < 0:
        raise RuntimeError(f"{name} must be >= 0; received {parsed}")
    return int(parsed)


def _tail_limit(entries: Any, limit: int) -> list[Any]:
    if not isinstance(entries, list):
        return []
    values = list(entries)
    if limit > 0:
        return values[-limit:]
    return values


def _overlay_identity(overlay: Any, index: int) -> str:
    if not isinstance(overlay, AbcMapping):
        return f"index:{index}"
    for key in ("id", "overlay_id", "name", "key", "slug", "indicator_id", "type"):
        value = str(overlay.get(key) or "").strip()
        if value:
            return f"{key}:{value}"
    return f"index:{index}"


def _overlay_fingerprint(overlay: Any) -> str:
    if not isinstance(overlay, AbcMapping):
        return ""
    return json.dumps(_sanitize_json(dict(overlay)), sort_keys=True, separators=(",", ":"))


def _series_identity(series: Any, index: int) -> str:
    if not isinstance(series, AbcMapping):
        return f"series_index:{index}"
    strategy_id = str(series.get("strategy_id") or "").strip()
    symbol = str(series.get("symbol") or "").strip()
    timeframe = str(series.get("timeframe") or "").strip()
    if strategy_id or symbol or timeframe:
        return f"{strategy_id}|{symbol}|{timeframe}"
    return f"series_index:{index}"


def _compact_overlay_geometry(value: Any, *, max_points: int) -> Any:
    if isinstance(value, AbcMapping):
        compact: Dict[str, Any] = {}
        for key, entry in value.items():
            compact[str(key)] = _compact_overlay_geometry(entry, max_points=max_points)
        return compact
    if isinstance(value, list):
        subset = _tail_limit(value, max_points)
        return [_compact_overlay_geometry(entry, max_points=max_points) for entry in subset]
    return value


def _compact_overlay_window(overlays: Any) -> list[Dict[str, Any]]:
    trimmed = _tail_limit(overlays, _MAX_OVERLAYS)
    compacted: list[Dict[str, Any]] = []
    for overlay in trimmed:
        if not isinstance(overlay, AbcMapping):
            continue
        compacted.append(dict(_compact_overlay_geometry(dict(overlay), max_points=_MAX_OVERLAY_POINTS)))
    return compacted


def _build_overlay_delta_snapshot(*, previous: Any, current: Dict[str, Any]) -> Dict[str, Any]:
    previous_snapshot = previous if isinstance(previous, AbcMapping) else {}
    previous_series_raw = previous_snapshot.get("series")
    previous_series = previous_series_raw if isinstance(previous_series_raw, list) else []
    previous_by_id: Dict[str, Dict[str, Any]] = {}
    for series_index, series_entry in enumerate(previous_series):
        if not isinstance(series_entry, AbcMapping):
            continue
        previous_by_id[_series_identity(series_entry, series_index)] = dict(series_entry)

    current_series_raw = current.get("series")
    current_series = current_series_raw if isinstance(current_series_raw, list) else []
    next_series: list[Dict[str, Any]] = []
    for series_index, series_entry in enumerate(current_series):
        if not isinstance(series_entry, AbcMapping):
            continue
        series_row = dict(series_entry)
        current_overlays_raw = series_row.get("overlays")
        current_overlays = list(current_overlays_raw) if isinstance(current_overlays_raw, list) else []

        previous_series_entry = previous_by_id.get(_series_identity(series_entry, series_index))
        if previous_series_entry is None:
            series_row["overlay_delta"] = {"mode": "replace", "removed": []}
            next_series.append(series_row)
            continue

        previous_overlays_raw = previous_series_entry.get("overlays")
        previous_overlays = list(previous_overlays_raw) if isinstance(previous_overlays_raw, list) else []
        previous_map: Dict[str, Dict[str, Any]] = {}
        for overlay_index, overlay in enumerate(previous_overlays):
            if not isinstance(overlay, AbcMapping):
                continue
            previous_map[_overlay_identity(overlay, overlay_index)] = dict(overlay)

        current_ids: list[str] = []
        changed: list[Dict[str, Any]] = []
        for overlay_index, overlay in enumerate(current_overlays):
            if not isinstance(overlay, AbcMapping):
                continue
            overlay_id = _overlay_identity(overlay, overlay_index)
            current_ids.append(overlay_id)
            previous_overlay = previous_map.get(overlay_id)
            if previous_overlay is None or _overlay_fingerprint(previous_overlay) != _overlay_fingerprint(overlay):
                changed.append(dict(overlay))

        current_id_set = set(current_ids)
        removed = [overlay_id for overlay_id in previous_map.keys() if overlay_id not in current_id_set]
        series_row["overlays"] = changed
        series_row["overlay_delta"] = {"mode": "delta", "removed": removed}
        next_series.append(series_row)

    next_snapshot = dict(current)
    next_snapshot["series"] = next_series
    return next_snapshot


_MAX_CANDLES = _resolve_limit("BOTLENS_MAX_CANDLES", 320)
_MAX_TRADES = _resolve_limit("BOTLENS_MAX_TRADES", 400)
_MAX_OVERLAYS = _resolve_limit("BOTLENS_MAX_OVERLAYS", 400)
_MAX_OVERLAY_POINTS = _resolve_limit("BOTLENS_MAX_OVERLAY_POINTS", 160)
_MAX_LOGS = _resolve_limit("BOTLENS_MAX_LOGS", 400)
_MAX_DECISIONS = _resolve_limit("BOTLENS_MAX_DECISIONS", 800)
_RING_SIZE = max(32, _resolve_limit("BOTLENS_STREAM_RING_SIZE", 2048))
_INGEST_QUEUE_MAX = max(64, _resolve_limit("BOTLENS_INGEST_QUEUE_MAX", 4096))
_PERSIST_QUEUE_MAX = max(64, _resolve_limit("BOTLENS_PERSIST_QUEUE_MAX", 4096))
_PERSIST_BATCH_MAX = max(1, _resolve_limit("BOTLENS_PERSIST_BATCH_MAX", 256))


def _resolve_schema_version(value: Any, default: int = _SCHEMA_VERSION) -> int:
    if value is None:
        return int(default)
    parsed = _coerce_int(value, default=-1)
    if parsed < 1:
        raise ValueError(f"invalid snapshot schema_version: {value!r}")
    if parsed != int(default):
        raise ValueError(f"unsupported snapshot schema_version: {parsed}")
    return parsed


def _trade_id_from_payload(trade: Any) -> Optional[str]:
    if not isinstance(trade, AbcMapping):
        return None
    for key in ("trade_id", "id"):
        raw = trade.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            return text
    return None


def _normalise_trade_state(trade: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(trade, AbcMapping):
        return None
    trade_id = _trade_id_from_payload(trade)
    if not trade_id:
        return None
    legs_raw = trade.get("legs")
    legs = list(legs_raw) if isinstance(legs_raw, list) else []
    open_legs = 0
    closed_legs = 0
    open_contracts = 0.0
    legs_signature = []
    for leg in legs:
        if not isinstance(leg, AbcMapping):
            continue
        status = str(leg.get("status") or "").lower()
        contracts = max(0.0, _coerce_float(leg.get("contracts"), 0.0))
        if status == "open":
            open_legs += 1
            open_contracts += contracts
        else:
            closed_legs += 1
        legs_signature.append(
            {
                "id": str(leg.get("id") or leg.get("leg_id") or leg.get("name") or ""),
                "status": status,
                "contracts": round(contracts, 12),
                "exit_time": str(leg.get("exit_time") or ""),
            }
        )
    legs_signature.sort(key=lambda row: (row["id"], row["status"], row["contracts"], row["exit_time"]))

    closed_at = trade.get("closed_at")
    closed = bool(closed_at)
    net_pnl = _coerce_float(trade.get("net_pnl"), 0.0)
    stop_price = _coerce_float(trade.get("stop_price"), 0.0)
    entry_price = _coerce_float(trade.get("entry_price"), 0.0)

    fingerprint = json.dumps(
        {
            "closed": closed,
            "closed_at": str(closed_at or ""),
            "open_legs": int(open_legs),
            "closed_legs": int(closed_legs),
            "open_contracts": round(open_contracts, 12),
            "net_pnl": round(net_pnl, 12),
            "stop_price": round(stop_price, 12),
            "entry_price": round(entry_price, 12),
            "legs": legs_signature,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "trade_id": trade_id,
        "direction": str(trade.get("direction") or ""),
        "entry_time": str(trade.get("entry_time") or ""),
        "closed_at": str(closed_at or ""),
        "closed": closed,
        "open_legs": int(open_legs),
        "closed_legs": int(closed_legs),
        "open_contracts": float(open_contracts),
        "net_pnl": float(net_pnl),
        "fingerprint": fingerprint,
    }


def _lifecycle_event_priority(event_type: str) -> int:
    order = {
        "trade_closed": 4,
        "trade_partially_closed": 3,
        "trade_opened": 2,
        "trade_updated": 1,
    }
    return int(order.get(str(event_type), 0))


def _trim_chart_snapshot(raw_chart: Any) -> Dict[str, Any]:
    chart = raw_chart if isinstance(raw_chart, AbcMapping) else {}

    raw_series = chart.get("series")
    series_entries = raw_series if isinstance(raw_series, list) else []
    series: list[Dict[str, Any]] = []
    for entry in series_entries[:_MAX_SERIES]:
        if not isinstance(entry, AbcMapping):
            continue
        candles_raw = entry.get("candles")
        overlays_raw = entry.get("overlays")
        stats_raw = entry.get("stats")
        candles = _tail_limit(candles_raw, _MAX_CANDLES)
        overlays = _compact_overlay_window(overlays_raw)
        series.append(
            {
                "strategy_id": entry.get("strategy_id"),
                "symbol": entry.get("symbol"),
                "timeframe": entry.get("timeframe"),
                "candles": _sanitize_json(candles),
                "overlays": _sanitize_json(overlays),
                "stats": _sanitize_json(dict(stats_raw) if isinstance(stats_raw, AbcMapping) else {}),
            }
        )

    trades_raw = chart.get("trades")
    trades = _tail_limit(trades_raw, _MAX_TRADES)
    logs_raw = chart.get("logs")
    logs = _tail_limit(logs_raw, _MAX_LOGS)
    decisions_raw = chart.get("decisions")
    decisions = _tail_limit(decisions_raw, _MAX_DECISIONS)

    runtime_raw = chart.get("runtime")
    runtime = dict(runtime_raw) if isinstance(runtime_raw, AbcMapping) else {}

    return {
        "series": series,
        "trades": _sanitize_json(trades),
        "logs": _sanitize_json(logs),
        "decisions": _sanitize_json(decisions),
        "runtime": _sanitize_json(runtime),
        "warnings": _sanitize_json(chart.get("warnings") or []),
    }


class BotTelemetryHub:
    def __init__(self) -> None:
        self._trade_state: Dict[tuple[str, str], Dict[str, Dict[str, Any]]] = {}
        self._latest_view_state: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._latest_run_by_bot: Dict[str, str] = {}
        self._persisted_seq: Dict[Tuple[str, str], int] = {}
        self._persist_lag_ms: Dict[Tuple[str, str], float] = {}
        self._series_viewers: DefaultDict[Tuple[str, str], Dict[WebSocket, Dict[str, Any]]] = defaultdict(dict)
        self._ingest_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=_INGEST_QUEUE_MAX)
        self._persist_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=_PERSIST_QUEUE_MAX)
        self._ingest_task: Optional[asyncio.Task[None]] = None
        self._persist_task: Optional[asyncio.Task[None]] = None
        self._lock = asyncio.Lock()
        self._worker_lock = asyncio.Lock()

    async def _ensure_workers(self) -> None:
        async with self._worker_lock:
            if self._ingest_task is None or self._ingest_task.done():
                self._ingest_task = asyncio.create_task(self._ingest_worker_loop(), name="bot-telemetry-ingest-worker")
            if self._persist_task is None or self._persist_task.done():
                self._persist_task = asyncio.create_task(
                    self._persist_worker_loop(),
                    name="bot-telemetry-persist-worker",
                )

    async def _latest_view_state_for(
        self,
        *,
        bot_id: str,
        run_id: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        target_run = str(run_id or "").strip() or None
        cached: Optional[Dict[str, Any]] = None
        async with self._lock:
            if target_run:
                cached = self._latest_view_state.get((str(bot_id), target_run))
            else:
                latest_run = self._latest_run_by_bot.get(str(bot_id))
                if latest_run:
                    cached = self._latest_view_state.get((str(bot_id), str(latest_run)))
        if isinstance(cached, dict):
            return dict(cached)
        return await asyncio.to_thread(
            get_latest_bot_run_view_state,
            bot_id=str(bot_id),
            run_id=target_run,
            series_key="bot",
        )

    async def _ingest_worker_loop(self) -> None:
        while True:
            item = await self._ingest_queue.get()
            try:
                await self._process_ingest(item)
            except Exception as exc:  # noqa: BLE001
                logger.exception("bot_telemetry_ingest_worker_failed | error=%s", exc)
            finally:
                self._ingest_queue.task_done()

    async def _persist_worker_loop(self) -> None:
        while True:
            first = await self._persist_queue.get()
            batch: list[Dict[str, Any]] = [first]
            for _ in range(max(0, _PERSIST_BATCH_MAX - 1)):
                try:
                    batch.append(self._persist_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            latest_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for entry in batch:
                row = entry.get("row")
                if not isinstance(row, AbcMapping):
                    continue
                key = (str(row.get("bot_id") or ""), str(row.get("run_id") or ""))
                if not key[0] or not key[1]:
                    continue
                previous = latest_by_key.get(key)
                incoming_seq = _coerce_int(row.get("seq"), default=0)
                previous_seq = _coerce_int(previous.get("seq"), default=0) if isinstance(previous, AbcMapping) else -1
                if previous is None or incoming_seq >= previous_seq:
                    latest_by_key[key] = dict(entry)

            for key, entry in latest_by_key.items():
                row = entry.get("row")
                if not isinstance(row, AbcMapping):
                    continue
                started = time.monotonic()
                try:
                    await asyncio.to_thread(upsert_bot_run_view_state, dict(row))
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "bot_telemetry_persist_failed | bot_id=%s | run_id=%s | seq=%s | error=%s",
                        key[0],
                        key[1],
                        row.get("seq"),
                        exc,
                    )
                    continue
                elapsed_ms = max((time.monotonic() - started) * 1000.0, 0.0)
                enqueued_at = float(entry.get("enqueued_monotonic") or started)
                end_to_end_lag_ms = max((time.monotonic() - enqueued_at) * 1000.0, 0.0)
                async with self._lock:
                    self._persisted_seq[key] = _coerce_int(row.get("seq"), default=0)
                    self._persist_lag_ms[key] = end_to_end_lag_ms
                logger.debug(
                    "bot_telemetry_persist_ok | bot_id=%s | run_id=%s | seq=%s | persist_ms=%.3f | lag_ms=%.3f | queue_depth=%s",
                    key[0],
                    key[1],
                    row.get("seq"),
                    elapsed_ms,
                    end_to_end_lag_ms,
                    self._persist_queue.qsize(),
                )

            for _ in batch:
                self._persist_queue.task_done()

    def _derive_trade_lifecycle_events(self, *, bot_id: str, run_id: str, trades: Any) -> list[Dict[str, Any]]:
        trade_list = list(trades) if isinstance(trades, list) else []
        current: Dict[str, Dict[str, Any]] = {}
        for trade in trade_list:
            normalized = _normalise_trade_state(trade)
            if normalized is None:
                continue
            current[normalized["trade_id"]] = normalized

        key = (str(bot_id), str(run_id))
        previous = self._trade_state.get(key, {})
        lifecycle: list[Dict[str, Any]] = []

        current_ids = set(current.keys())
        previous_ids = set(previous.keys())

        for trade_id in sorted(current_ids - previous_ids):
            trade = current[trade_id]
            lifecycle.append(
                {
                    "type": "trade_opened",
                    "trade_id": trade_id,
                    "direction": trade.get("direction"),
                    "entry_time": trade.get("entry_time"),
                    "open_contracts": trade.get("open_contracts"),
                }
            )

        for trade_id in sorted(current_ids & previous_ids):
            prev = previous[trade_id]
            curr = current[trade_id]
            if curr.get("fingerprint") == prev.get("fingerprint"):
                continue
            prev_closed = bool(prev.get("closed"))
            curr_closed = bool(curr.get("closed"))
            if (not prev_closed) and curr_closed:
                lifecycle.append(
                    {
                        "type": "trade_closed",
                        "trade_id": trade_id,
                        "closed_at": curr.get("closed_at"),
                        "net_pnl": curr.get("net_pnl"),
                    }
                )
                continue

            prev_open_contracts = _coerce_float(prev.get("open_contracts"), 0.0)
            curr_open_contracts = _coerce_float(curr.get("open_contracts"), 0.0)
            prev_closed_legs = _coerce_int(prev.get("closed_legs"), 0)
            curr_closed_legs = _coerce_int(curr.get("closed_legs"), 0)
            if (
                curr_open_contracts + _QTY_EPSILON < prev_open_contracts
                or curr_closed_legs > prev_closed_legs
            ):
                lifecycle.append(
                    {
                        "type": "trade_partially_closed",
                        "trade_id": trade_id,
                        "open_contracts_before": prev_open_contracts,
                        "open_contracts_after": curr_open_contracts,
                        "closed_legs_before": prev_closed_legs,
                        "closed_legs_after": curr_closed_legs,
                    }
                )
                continue

            lifecycle.append(
                {
                    "type": "trade_updated",
                    "trade_id": trade_id,
                }
            )

        self._trade_state[key] = current
        return lifecycle

    async def ingest(self, payload: Dict[str, Any]) -> None:
        await self._ensure_workers()
        item = {
            "payload": dict(payload) if isinstance(payload, AbcMapping) else {},
            "enqueued_monotonic": time.monotonic(),
        }
        try:
            self._ingest_queue.put_nowait(item)
        except asyncio.QueueFull:
            logger.warning(
                "bot_telemetry_ingest_queue_backpressure | queue_depth=%s | queue_max=%s",
                self._ingest_queue.qsize(),
                _INGEST_QUEUE_MAX,
            )
            await self._ingest_queue.put(item)

    async def _process_ingest(self, item: Dict[str, Any]) -> None:
        payload = item.get("payload")
        if not isinstance(payload, AbcMapping):
            return

        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        seq = _coerce_int(payload.get("seq"), default=0)
        if not bot_id or not run_id or seq <= 0:
            logger.warning(
                "bot_telemetry_ingest_invalid_payload | bot_id=%s | run_id=%s | seq=%s",
                bot_id,
                run_id,
                seq,
            )
            return

        view_envelope = payload.get("view_state") if isinstance(payload.get("view_state"), AbcMapping) else {}
        if not view_envelope:
            return
        view_seq = _coerce_int(view_envelope.get("seq"), default=seq)
        if view_seq <= 0:
            logger.warning(
                "bot_telemetry_ingest_invalid_view_seq | bot_id=%s | run_id=%s | seq=%s | view_seq=%s",
                bot_id,
                run_id,
                seq,
                view_seq,
            )
            return
        raw_chart = view_envelope.get("payload") if isinstance(view_envelope, AbcMapping) else {}
        full_snapshot = _trim_chart_snapshot(raw_chart)
        snapshot_at = view_envelope.get("at") or payload.get("at")
        snapshot_known_at = view_envelope.get("known_at") or payload.get("known_at") or snapshot_at
        snapshot_schema_version = _resolve_schema_version(view_envelope.get("schema_version"))

        key = (bot_id, run_id)
        async with self._lock:
            previous = self._latest_view_state.get(key)
            previous_seq = _coerce_int(previous.get("seq"), default=0) if isinstance(previous, AbcMapping) else 0
            previous_snapshot = previous.get("payload") if isinstance(previous, AbcMapping) else None
        if previous_seq >= view_seq:
            logger.debug(
                "bot_telemetry_ingest_stale_view_state_ignored | bot_id=%s | run_id=%s | incoming_seq=%s | latest_seq=%s",
                bot_id,
                run_id,
                view_seq,
                previous_seq,
            )
            return
        expected_next_seq = previous_seq + 1 if previous_seq > 0 else view_seq
        seq_gap = max(0, view_seq - expected_next_seq)
        resync_required = previous_seq > 0 and seq_gap > 0
        if resync_required:
            logger.warning(
                "bot_telemetry_seq_gap_detected | bot_id=%s | run_id=%s | previous_seq=%s | incoming_seq=%s | seq_gap=%s | action=resync_required",
                bot_id,
                run_id,
                previous_seq,
                view_seq,
                seq_gap,
            )
        stream_snapshot = _build_overlay_delta_snapshot(previous=previous_snapshot, current=full_snapshot)

        view_state_row = {
            "run_id": run_id,
            "bot_id": bot_id,
            "series_key": "bot",
            "seq": view_seq,
            "schema_version": snapshot_schema_version,
            "payload": full_snapshot,
            "event_time": snapshot_at,
            "known_at": snapshot_known_at,
            "updated_at": snapshot_known_at,
        }
        async with self._lock:
            self._latest_view_state[key] = dict(view_state_row)
            self._latest_run_by_bot[bot_id] = run_id

        persist_item = {
            "row": view_state_row,
            "enqueued_monotonic": time.monotonic(),
        }
        try:
            self._persist_queue.put_nowait(persist_item)
        except asyncio.QueueFull:
            logger.warning(
                "bot_telemetry_persist_queue_backpressure | bot_id=%s | run_id=%s | seq=%s | queue_depth=%s | queue_max=%s",
                bot_id,
                run_id,
                view_seq,
                self._persist_queue.qsize(),
                _PERSIST_QUEUE_MAX,
            )
            await self._persist_queue.put(persist_item)

        summary_raw = payload.get("summary")
        summary = summary_raw if isinstance(summary_raw, AbcMapping) else {}
        payload_bytes = _coerce_int(summary.get("payload_bytes"), default=0)
        if payload_bytes <= 0:
            payload_bytes = len(json.dumps(_sanitize_json(payload), separators=(",", ":")).encode("utf-8"))

        async with self._lock:
            persisted_seq = self._persisted_seq.get(key, 0)
            persist_lag_ms = float(self._persist_lag_ms.get(key, 0.0))
        persist_seq_lag = max(0, view_seq - int(persisted_seq))
        ingest_queue_depth = self._ingest_queue.qsize()
        persist_queue_depth = self._persist_queue.qsize()

        lifecycle = self._derive_trade_lifecycle_events(
            bot_id=bot_id,
            run_id=run_id,
            trades=full_snapshot.get("trades") or [],
        )
        trade_count = len(full_snapshot.get("trades") or [])
        if lifecycle:
            primary = max(lifecycle, key=lambda item: _lifecycle_event_priority(str(item.get("type") or "")))
            event_type = str(primary.get("type") or "trade_updated")
        else:
            event_type = "state_delta"
        critical = event_type in {"trade_opened", "trade_partially_closed", "trade_closed"}

        row = {
            "event_id": f"{bot_id}:{run_id}:view_state:{view_seq}",
            "bot_id": bot_id,
            "run_id": run_id,
            "seq": view_seq,
            "event_type": event_type,
            "critical": critical,
            "schema_version": snapshot_schema_version,
            "event_time": snapshot_at,
            "known_at": snapshot_known_at,
            "payload": {
                "snapshot": stream_snapshot,
                "summary": {
                    "series_count": len(full_snapshot.get("series") or []),
                    "trade_count": trade_count,
                    "warning_count": len(full_snapshot.get("warnings") or []),
                    "payload_bytes": payload_bytes,
                    "ingest_queue_depth": ingest_queue_depth,
                    "persist_queue_depth": persist_queue_depth,
                    "persist_seq_lag": persist_seq_lag,
                    "persist_lag_ms": persist_lag_ms,
                },
                "snapshot_meta": {
                    "schema_version": snapshot_schema_version,
                    "known_at": snapshot_known_at,
                    "previous_seq": previous_seq,
                    "seq_gap_from_previous": seq_gap,
                    "resync_required": bool(resync_required),
                },
                "stream_metrics": {
                    "payload_bytes": payload_bytes,
                    "ingest_queue_depth": ingest_queue_depth,
                    "persist_queue_depth": persist_queue_depth,
                    "persist_seq_lag": persist_seq_lag,
                    "persist_lag_ms": persist_lag_ms,
                },
                "trade_lifecycle_events": lifecycle,
            },
        }
        envelope = self._state_event_envelope(row)
        await self._broadcast_event(envelope)
        await self._broadcast_series_live_tail(
            run_id=run_id,
            seq=view_seq,
            known_at=snapshot_known_at,
            previous_snapshot=previous_snapshot if isinstance(previous_snapshot, AbcMapping) else None,
            current_snapshot=full_snapshot,
        )


    async def _broadcast_event(self, event: Dict[str, Any]) -> None:
        # State-delta fanout intentionally disabled for BotLens live model (series WS is canonical).
        # Method exists for observability hooks/tests and future internal subscribers.
        _ = event
        return

    async def add_series_viewer(
        self,
        *,
        run_id: str,
        series_key: str,
        ws: WebSocket,
        after_seq: int = 0,
    ) -> None:
        await self._ensure_workers()
        await ws.accept()
        key = (str(run_id), str(series_key).upper())
        async with self._lock:
            self._series_viewers[key][ws] = {"last_seq": max(0, int(after_seq or 0))}

    async def remove_series_viewer(self, *, run_id: str, series_key: str, ws: WebSocket) -> None:
        key = (str(run_id), str(series_key).upper())
        async with self._lock:
            viewers = self._series_viewers.get(key)
            if not viewers:
                return
            viewers.pop(ws, None)
            if not viewers:
                self._series_viewers.pop(key, None)

    async def _broadcast_series_live_tail(
        self,
        *,
        run_id: str,
        seq: int,
        known_at: Any,
        previous_snapshot: Optional[AbcMapping],
        current_snapshot: Dict[str, Any],
    ) -> None:
        async with self._lock:
            targets = list(self._series_viewers.items())
        for key, viewers in targets:
            target_run, series_key = key
            if str(target_run) != str(run_id):
                continue
            messages = build_live_tail_messages(
                run_id=str(run_id),
                series_key=str(series_key),
                seq=int(seq),
                known_at=known_at,
                previous_snapshot=previous_snapshot,
                current_snapshot=current_snapshot,
            )
            if not messages:
                continue
            for ws, state in list(viewers.items()):
                last_seq = int(state.get("last_seq") or 0)
                if int(seq) <= last_seq:
                    continue
                try:
                    for message in messages:
                        await ws.send_text(json.dumps(message))
                except Exception:
                    await self.remove_series_viewer(run_id=str(run_id), series_key=str(series_key), ws=ws)
                    continue
                async with self._lock:
                    slot = self._series_viewers.get((str(run_id), str(series_key).upper()), {}).get(ws)
                    if slot is not None:
                        slot["last_seq"] = int(seq)

    @staticmethod
    def _state_event_envelope(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "type": "botlens_state_delta",
            "bot_id": str(row.get("bot_id") or ""),
            "run_id": str(row.get("run_id") or ""),
            "seq": int(row.get("seq") or 0),
            "schema_version": int(row.get("schema_version") or _SCHEMA_VERSION),
            "event_time": row.get("event_time"),
            "known_at": row.get("known_at"),
            "payload": _sanitize_json(row.get("payload") or {}),
        }



telemetry_hub = BotTelemetryHub()
