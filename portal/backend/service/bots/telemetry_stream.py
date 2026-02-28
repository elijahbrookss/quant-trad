from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import defaultdict
from collections.abc import Mapping as AbcMapping
from datetime import datetime
from typing import Any, DefaultDict, Dict, Optional

from fastapi import WebSocket

from ..storage.storage import (
    get_latest_bot_runtime_event,
    get_latest_bot_runtime_run_id,
    list_bot_runtime_events,
    record_bot_runtime_event,
)

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_MAX_SERIES = 12
_MAX_CANDLES = 800
_MAX_TRADES = 400
_MAX_OVERLAYS = 400
_QTY_EPSILON = 1e-9
_BOTLENS_EVENT_TYPES = (
    "state_delta",
    "trade_opened",
    "trade_partially_closed",
    "trade_closed",
    "trade_updated",
)


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
    except Exception:
        return int(default)


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


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
        candles = list(candles_raw)[-_MAX_CANDLES:] if isinstance(candles_raw, list) else []
        overlays = list(overlays_raw)[-_MAX_OVERLAYS:] if isinstance(overlays_raw, list) else []
        series.append(
            {
                "strategy_id": entry.get("strategy_id"),
                "symbol": entry.get("symbol"),
                "timeframe": entry.get("timeframe"),
                "candles": _sanitize_json(candles),
                "overlays": _sanitize_json(overlays),
            }
        )

    trades_raw = chart.get("trades")
    trades = list(trades_raw)[-_MAX_TRADES:] if isinstance(trades_raw, list) else []

    runtime_raw = chart.get("runtime")
    runtime = dict(runtime_raw) if isinstance(runtime_raw, AbcMapping) else {}

    return {
        "series": series,
        "trades": _sanitize_json(trades),
        "runtime": _sanitize_json(runtime),
        "warnings": _sanitize_json(chart.get("warnings") or []),
    }


class BotTelemetryHub:
    def __init__(self) -> None:
        self._viewers: DefaultDict[str, Dict[WebSocket, Dict[str, Any]]] = defaultdict(dict)
        self._trade_state: Dict[tuple[str, str], Dict[str, Dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

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

    async def bootstrap(self, *, bot_id: str, run_id: Optional[str] = None) -> Dict[str, Any]:
        target_run = str(run_id or "").strip() or await asyncio.to_thread(get_latest_bot_runtime_run_id, str(bot_id))
        if not target_run:
            return {
                "bot_id": str(bot_id),
                "run_id": None,
                "seq": 0,
                "schema_version": _SCHEMA_VERSION,
                "snapshot": None,
                "state": "waiting",
            }
        latest = await asyncio.to_thread(
            get_latest_bot_runtime_event,
            bot_id=str(bot_id),
            run_id=target_run,
            event_types=_BOTLENS_EVENT_TYPES,
        )
        if not latest:
            return {
                "bot_id": str(bot_id),
                "run_id": target_run,
                "seq": 0,
                "schema_version": _SCHEMA_VERSION,
                "snapshot": None,
                "state": "waiting",
            }
        payload = latest.get("payload") if isinstance(latest.get("payload"), dict) else {}
        return {
            "bot_id": str(bot_id),
            "run_id": target_run,
            "seq": int(latest.get("seq") or 0),
            "schema_version": int(latest.get("schema_version") or _SCHEMA_VERSION),
            "event_time": latest.get("event_time"),
            "known_at": latest.get("known_at"),
            "snapshot": payload.get("snapshot"),
            "state": "ok",
        }

    async def ingest(self, payload: Dict[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        seq = _coerce_int(payload.get("snapshot_seq"), default=0)
        if not bot_id or not run_id or seq <= 0:
            logger.warning(
                "bot_telemetry_ingest_invalid_payload | bot_id=%s | run_id=%s | seq=%s",
                bot_id,
                run_id,
                seq,
            )
            return

        snapshot_envelope = payload.get("snapshot") if isinstance(payload.get("snapshot"), AbcMapping) else {}
        raw_chart = snapshot_envelope.get("snapshot") if isinstance(snapshot_envelope, AbcMapping) else {}
        snapshot = _trim_chart_snapshot(raw_chart)
        snapshot_at = snapshot_envelope.get("at")
        snapshot_known_at = snapshot_envelope.get("known_at") or snapshot_at
        snapshot_schema_version = _resolve_schema_version(snapshot_envelope.get("schema_version"))
        lifecycle = self._derive_trade_lifecycle_events(
            bot_id=bot_id,
            run_id=run_id,
            trades=snapshot.get("trades") or [],
        )
        trade_count = len(snapshot.get("trades") or [])
        if lifecycle:
            primary = max(lifecycle, key=lambda item: _lifecycle_event_priority(str(item.get("type") or "")))
            event_type = str(primary.get("type") or "trade_updated")
        else:
            event_type = "state_delta"
        critical = event_type in {"trade_opened", "trade_partially_closed", "trade_closed"}

        row = await asyncio.to_thread(
            record_bot_runtime_event,
            {
                "event_id": f"{bot_id}:{run_id}:{seq}",
                "bot_id": bot_id,
                "run_id": run_id,
                "seq": seq,
                "event_type": event_type,
                "critical": critical,
                "schema_version": snapshot_schema_version,
                "event_time": snapshot_at,
                "payload": {
                    "snapshot": snapshot,
                    "summary": {
                        "series_count": len(snapshot.get("series") or []),
                        "trade_count": trade_count,
                        "warning_count": len(snapshot.get("warnings") or []),
                    },
                    "snapshot_meta": {
                        "schema_version": snapshot_schema_version,
                        "known_at": snapshot_known_at,
                    },
                    "trade_lifecycle_events": lifecycle,
                },
            },
        )
        await self._broadcast_event(self._event_envelope(row))

    async def add_viewer(
        self,
        *,
        bot_id: str,
        ws: WebSocket,
        run_id: Optional[str] = None,
        since_seq: int = 0,
    ) -> None:
        await ws.accept()
        requested_run = str(run_id or "").strip() or None
        requested_seq = max(0, _coerce_int(since_seq, default=0))
        if requested_run is None:
            requested_run = await asyncio.to_thread(get_latest_bot_runtime_run_id, str(bot_id))

        async with self._lock:
            self._viewers[str(bot_id)][ws] = {
                "run_id": requested_run,
                "last_seq": requested_seq,
            }

        if not requested_run:
            return

        catchup = await asyncio.to_thread(
            list_bot_runtime_events,
            bot_id=str(bot_id),
            run_id=requested_run,
            after_seq=requested_seq,
            limit=2500,
            event_types=_BOTLENS_EVENT_TYPES,
        )
        for row in catchup:
            envelope = self._event_envelope(row)
            try:
                await ws.send_text(json.dumps(envelope))
            except Exception:
                await self.remove_viewer(bot_id=bot_id, ws=ws)
                return
            async with self._lock:
                state = self._viewers.get(str(bot_id), {}).get(ws)
                if state is not None:
                    state["run_id"] = str(envelope.get("run_id") or state.get("run_id") or "")
                    state["last_seq"] = int(envelope.get("seq") or state.get("last_seq") or 0)

    async def remove_viewer(self, *, bot_id: str, ws: WebSocket) -> None:
        async with self._lock:
            viewers = self._viewers.get(str(bot_id))
            if not viewers:
                return
            viewers.pop(ws, None)
            if not viewers:
                self._viewers.pop(str(bot_id), None)

    async def _broadcast_event(self, event: Dict[str, Any]) -> None:
        bot_id = str(event.get("bot_id") or "").strip()
        run_id = str(event.get("run_id") or "").strip()
        seq = int(event.get("seq") or 0)
        if not bot_id or not run_id or seq <= 0:
            return

        async with self._lock:
            viewers = list(self._viewers.get(bot_id, {}).items())

        for ws, state in viewers:
            viewer_run = str(state.get("run_id") or "").strip() or None
            viewer_last_seq = int(state.get("last_seq") or 0)

            # Auto-attach to latest run when run_id changes.
            if viewer_run is None or viewer_run != run_id:
                viewer_last_seq = 0
                viewer_run = run_id
            if seq <= viewer_last_seq:
                continue

            try:
                await ws.send_text(json.dumps(event))
            except Exception:
                logger.warning("bot_telemetry_viewer_send_failed | bot_id=%s", bot_id)
                await self.remove_viewer(bot_id=bot_id, ws=ws)
                continue

            async with self._lock:
                slot = self._viewers.get(bot_id, {}).get(ws)
                if slot is not None:
                    slot["run_id"] = run_id
                    slot["last_seq"] = seq

    @staticmethod
    def _event_envelope(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "type": "bot_runtime_event",
            "bot_id": str(row.get("bot_id") or ""),
            "run_id": str(row.get("run_id") or ""),
            "event_id": str(row.get("event_id") or ""),
            "seq": int(row.get("seq") or 0),
            "event_type": str(row.get("event_type") or "state_delta"),
            "critical": bool(row.get("critical", False)),
            "schema_version": int(row.get("schema_version") or _SCHEMA_VERSION),
            "event_time": row.get("event_time"),
            "known_at": row.get("known_at"),
            "payload": _sanitize_json(row.get("payload") or {}),
        }


telemetry_hub = BotTelemetryHub()
