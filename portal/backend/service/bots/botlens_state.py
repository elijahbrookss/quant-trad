from __future__ import annotations

import json
import math
from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from core.settings import get_settings

from .botlens_contract import (
    CONTINUITY_BOOTSTRAP_REQUIRED,
    FACT_TYPE_CANDLE_UPSERTED,
    FACT_TYPE_DECISION_EMITTED,
    FACT_TYPE_LOG_EMITTED,
    FACT_TYPE_OVERLAY_OPS,
    FACT_TYPE_RUNTIME_STATE,
    FACT_TYPE_SERIES_STATE,
    FACT_TYPE_SERIES_STATS,
    FACT_TYPE_TRADE_UPSERTED,
    RUN_SCOPE_KEY,
    SCHEMA_VERSION,
    continuity_payload,
    normalize_fact_entries,
    normalize_series_key,
)

_SETTINGS = get_settings()
_BOTLENS = _SETTINGS.bot_runtime.botlens
_MAX_CANDLES = max(1, int(_BOTLENS.max_candles))
_MAX_LOGS = max(1, int(_BOTLENS.max_logs))
_MAX_DECISIONS = max(1, int(_BOTLENS.max_decisions))
_MAX_TRADES = max(1, int(_BOTLENS.max_closed_trades))


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _iso_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def normalize_candle_time(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        if abs(numeric) > 2e10:
            numeric /= 1000.0
        return int(math.floor(numeric))
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
        if not math.isfinite(numeric):
            return None
        if abs(numeric) > 2e10:
            numeric /= 1000.0
        return int(math.floor(numeric))
    except ValueError:
        pass
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        return None


def canonicalize_candle(candle: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(candle, Mapping):
        return None
    time_value = normalize_candle_time(candle.get("time"))
    if time_value is None:
        return None
    normalized = dict(candle)
    normalized["time"] = time_value
    for key in ("open", "high", "low", "close"):
        if key not in normalized:
            continue
        try:
            numeric = float(normalized.get(key))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric):
            return None
        normalized[key] = numeric
    return normalized


def merge_candles(*streams: Any, limit: int = _MAX_CANDLES) -> List[Dict[str, Any]]:
    by_time: Dict[int, Dict[str, Any]] = {}
    for stream in streams:
        values = stream if isinstance(stream, list) else []
        for candle in values:
            normalized = canonicalize_candle(candle)
            if normalized is None:
                continue
            by_time[int(normalized["time"])] = normalized
    ordered = [by_time[key] for key in sorted(by_time.keys())]
    if int(limit) > 0 and len(ordered) > int(limit):
        ordered = ordered[-int(limit) :]
    return ordered


def overlay_identity(overlay: Any, index: int) -> str:
    if not isinstance(overlay, Mapping):
        return f"index:{index}"
    explicit = str(overlay.get("overlay_id") or "").strip()
    if explicit:
        return explicit
    for key in ("id", "name", "key", "slug", "indicator_id", "type"):
        value = str(overlay.get(key) or "").strip()
        if value:
            return f"{key}:{value}"
    return f"index:{index}"


def overlay_revision(overlay: Any) -> str:
    return json.dumps(_normalize_scalar_value(overlay), sort_keys=True, separators=(",", ":"))


def project_overlay_state(overlays: Any) -> List[Dict[str, Any]]:
    projected: OrderedDict[str, Dict[str, Any]] = OrderedDict()
    for index, overlay in enumerate(overlays if isinstance(overlays, list) else []):
        if not isinstance(overlay, Mapping):
            continue
        identity = overlay_identity(overlay, index)
        normalized = dict(overlay)
        normalized["overlay_id"] = identity
        normalized["overlay_revision"] = overlay_revision(
            {key: value for key, value in normalized.items() if key != "overlay_revision"}
        )
        projected[identity] = normalized
    return list(projected.values())


def apply_overlay_delta(overlays: Any, delta: Any) -> List[Dict[str, Any]]:
    current = project_overlay_state(overlays)
    payload = delta if isinstance(delta, Mapping) else {}
    ops = payload.get("ops") if isinstance(payload.get("ops"), list) else []
    overlay_map: OrderedDict[str, Dict[str, Any]] = OrderedDict()
    for index, overlay in enumerate(current):
        if not isinstance(overlay, Mapping):
            continue
        overlay_id = str(overlay.get("overlay_id") or overlay_identity(overlay, index))
        normalized = dict(overlay)
        normalized["overlay_id"] = overlay_id
        overlay_map[overlay_id] = normalized
    for op in ops:
        if not isinstance(op, Mapping):
            continue
        op_name = str(op.get("op") or "").strip().lower()
        key = str(op.get("key") or "").strip()
        if not key:
            continue
        if op_name == "remove":
            overlay_map.pop(key, None)
            continue
        if op_name != "upsert":
            continue
        overlay = op.get("overlay")
        if not isinstance(overlay, Mapping):
            continue
        normalized = dict(overlay)
        normalized["overlay_id"] = key
        normalized["overlay_revision"] = overlay_revision(
            {entry_key: entry_value for entry_key, entry_value in normalized.items() if entry_key != "overlay_revision"}
        )
        overlay_map[key] = normalized
    return list(overlay_map.values())


def _upsert_key(entry: Mapping[str, Any], key_fields: tuple[str, ...]) -> str:
    for field in key_fields:
        value = str(entry.get(field) or "").strip()
        if value:
            return f"{field}:{value}"
    return ""


def _upsert_tail(entries: Any, item: Mapping[str, Any], *, key_fields: tuple[str, ...], limit: int) -> List[Dict[str, Any]]:
    ordered: OrderedDict[str, Dict[str, Any]] = OrderedDict()
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, Mapping):
            continue
        key = _upsert_key(entry, key_fields)
        if not key:
            continue
        ordered[key] = dict(entry)
    key = _upsert_key(item, key_fields)
    if not key:
        return list(ordered.values())
    ordered[key] = dict(item)
    values = list(ordered.values())
    if int(limit) > 0 and len(values) > int(limit):
        values = values[-int(limit) :]
    return values


def _normalize_scalar_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_scalar_value(entry) for key, entry in value.items()}
    if isinstance(value, list):
        return [_normalize_scalar_value(entry) for entry in value]
    if isinstance(value, tuple):
        return [_normalize_scalar_value(entry) for entry in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def is_open_trade(trade: Any) -> bool:
    if not isinstance(trade, Mapping):
        return False
    if trade.get("closed_at"):
        return False
    status = str(trade.get("status") or "").strip().lower()
    if status in {"closed", "completed", "complete"}:
        return False
    legs = trade.get("legs") if isinstance(trade.get("legs"), list) else []
    if not legs:
        return True
    return any(
        isinstance(leg, Mapping)
        and (not leg.get("exit_time") or str(leg.get("status") or "").strip().lower() == "open")
        for leg in legs
    )


def normalize_trade(trade: Any, *, symbol_key: str) -> Optional[Dict[str, Any]]:
    if not isinstance(trade, Mapping):
        return None
    trade_id = str(trade.get("trade_id") or trade.get("id") or "").strip()
    if not trade_id:
        return None
    normalized = dict(trade)
    normalized["trade_id"] = trade_id
    normalized["symbol_key"] = normalize_series_key(symbol_key)
    return normalized


def display_label(*, symbol: str, timeframe: str, symbol_key: str) -> str:
    display_symbol = str(symbol or "").strip() or str(symbol_key.split("|", 1)[0] if "|" in symbol_key else symbol_key).strip()
    display_timeframe = str(timeframe or "").strip()
    if display_timeframe:
        return f"{display_symbol} · {display_timeframe}"
    return display_symbol or "Unknown symbol"


def empty_symbol_detail(symbol_key: str) -> Dict[str, Any]:
    instrument_id, timeframe = str(symbol_key).split("|", 1) if "|" in str(symbol_key) else ("", "")
    return {
        "schema_version": SCHEMA_VERSION,
        "symbol_key": normalize_series_key(symbol_key),
        "instrument_id": instrument_id,
        "symbol": "",
        "timeframe": timeframe,
        "display_label": display_label(symbol="", timeframe=timeframe, symbol_key=symbol_key),
        "seq": 0,
        "status": "waiting",
        "last_event_at": None,
        "continuity": continuity_payload(status=CONTINUITY_BOOTSTRAP_REQUIRED),
        "candles": [],
        "overlays": [],
        "recent_trades": [],
        "logs": [],
        "decisions": [],
        "stats": {},
        "runtime": {},
    }


def empty_run_summary(*, bot_id: str, run_id: str) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "bot_id": str(bot_id),
        "run_id": str(run_id),
        "seq": 0,
        "run_meta": {"run_id": str(run_id)},
        "lifecycle": {},
        "health": {
            "status": "waiting",
            "phase": None,
            "warning_count": 0,
            "warnings": [],
            "last_event_at": None,
            "worker_count": 0,
            "active_workers": 0,
        },
        "symbol_index": {},
        "open_trades_index": {},
    }


def read_symbol_detail_state(payload: Any, *, symbol_key: str) -> Dict[str, Any]:
    source = _mapping(payload)
    detail = _mapping(source.get("detail")) if "detail" in source else source
    state = empty_symbol_detail(symbol_key)
    state.update(
        {
            "schema_version": int(detail.get("schema_version") or source.get("schema_version") or SCHEMA_VERSION),
            "symbol_key": normalize_series_key(detail.get("symbol_key") or symbol_key),
            "instrument_id": str(detail.get("instrument_id") or state["instrument_id"]).strip(),
            "symbol": str(detail.get("symbol") or "").strip().upper(),
            "timeframe": str(detail.get("timeframe") or state["timeframe"]).strip().lower(),
            "seq": int(detail.get("seq") or 0),
            "status": str(detail.get("status") or state["status"]).strip() or "waiting",
            "last_event_at": _iso_or_none(detail.get("last_event_at")),
            "continuity": dict(detail.get("continuity") or state["continuity"]),
            "candles": merge_candles(detail.get("candles"), limit=_MAX_CANDLES),
            "overlays": project_overlay_state(detail.get("overlays")),
            "recent_trades": [dict(entry) for entry in detail.get("recent_trades") if isinstance(entry, Mapping)] if isinstance(detail.get("recent_trades"), list) else [],
            "logs": [dict(entry) for entry in detail.get("logs") if isinstance(entry, Mapping)] if isinstance(detail.get("logs"), list) else [],
            "decisions": [dict(entry) for entry in detail.get("decisions") if isinstance(entry, Mapping)] if isinstance(detail.get("decisions"), list) else [],
            "stats": dict(detail.get("stats") or {}) if isinstance(detail.get("stats"), Mapping) else {},
            "runtime": dict(detail.get("runtime") or {}) if isinstance(detail.get("runtime"), Mapping) else {},
        }
    )
    state["display_label"] = display_label(
        symbol=state["symbol"],
        timeframe=state["timeframe"],
        symbol_key=state["symbol_key"],
    )
    return state


def read_run_summary_state(payload: Any, *, bot_id: str, run_id: str) -> Dict[str, Any]:
    source = _mapping(payload)
    summary = _mapping(source.get("summary")) if "summary" in source else source
    state = empty_run_summary(bot_id=bot_id, run_id=run_id)
    state.update(
        {
            "schema_version": int(summary.get("schema_version") or source.get("schema_version") or SCHEMA_VERSION),
            "bot_id": str(summary.get("bot_id") or bot_id),
            "run_id": str(summary.get("run_id") or run_id),
            "seq": int(summary.get("seq") or 0),
            "run_meta": dict(summary.get("run_meta") or state["run_meta"]) if isinstance(summary.get("run_meta"), Mapping) else state["run_meta"],
            "lifecycle": dict(summary.get("lifecycle") or {}) if isinstance(summary.get("lifecycle"), Mapping) else {},
            "health": dict(summary.get("health") or state["health"]) if isinstance(summary.get("health"), Mapping) else state["health"],
            "symbol_index": {
                normalize_series_key(key): dict(value)
                for key, value in (summary.get("symbol_index") or {}).items()
                if normalize_series_key(key) and isinstance(value, Mapping)
            }
            if isinstance(summary.get("symbol_index"), Mapping)
            else {},
            "open_trades_index": {
                str(key): dict(value)
                for key, value in (summary.get("open_trades_index") or {}).items()
                if str(key).strip() and isinstance(value, Mapping)
            }
            if isinstance(summary.get("open_trades_index"), Mapping)
            else {},
        }
    )
    return state


def serialize_symbol_detail_state(detail: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "symbol_detail",
        "detail": {
            "schema_version": SCHEMA_VERSION,
            "symbol_key": str(detail.get("symbol_key") or ""),
            "instrument_id": detail.get("instrument_id"),
            "symbol": detail.get("symbol"),
            "timeframe": detail.get("timeframe"),
            "display_label": detail.get("display_label"),
            "seq": int(detail.get("seq") or 0),
            "status": detail.get("status"),
            "last_event_at": detail.get("last_event_at"),
            "continuity": dict(detail.get("continuity") or {}),
            "candles": list(detail.get("candles") or []) if isinstance(detail.get("candles"), list) else [],
            "overlays": list(detail.get("overlays") or []) if isinstance(detail.get("overlays"), list) else [],
            "recent_trades": list(detail.get("recent_trades") or []) if isinstance(detail.get("recent_trades"), list) else [],
            "logs": list(detail.get("logs") or []) if isinstance(detail.get("logs"), list) else [],
            "decisions": list(detail.get("decisions") or []) if isinstance(detail.get("decisions"), list) else [],
            "stats": dict(detail.get("stats") or {}) if isinstance(detail.get("stats"), Mapping) else {},
            "runtime": dict(detail.get("runtime") or {}) if isinstance(detail.get("runtime"), Mapping) else {},
        },
    }


def serialize_run_summary_state(summary: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "run_summary",
        "summary": {
            "schema_version": SCHEMA_VERSION,
            "bot_id": summary.get("bot_id"),
            "run_id": summary.get("run_id"),
            "seq": int(summary.get("seq") or 0),
            "run_meta": dict(summary.get("run_meta") or {}) if isinstance(summary.get("run_meta"), Mapping) else {},
            "lifecycle": dict(summary.get("lifecycle") or {}) if isinstance(summary.get("lifecycle"), Mapping) else {},
            "health": dict(summary.get("health") or {}) if isinstance(summary.get("health"), Mapping) else {},
            "symbol_index": {
                normalize_series_key(key): dict(value)
                for key, value in (summary.get("symbol_index") or {}).items()
                if normalize_series_key(key) and isinstance(value, Mapping)
            }
            if isinstance(summary.get("symbol_index"), Mapping)
            else {},
            "open_trades_index": {
                str(key): dict(value)
                for key, value in (summary.get("open_trades_index") or {}).items()
                if str(key).strip() and isinstance(value, Mapping)
            }
            if isinstance(summary.get("open_trades_index"), Mapping)
            else {},
        },
    }


def build_symbol_summary(detail: Mapping[str, Any], *, open_trades: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    candles = detail.get("candles") if isinstance(detail.get("candles"), list) else []
    last_candle = candles[-1] if candles else {}
    open_trade_list = [dict(entry) for entry in open_trades if isinstance(entry, Mapping)]
    stats = dict(detail.get("stats") or {}) if isinstance(detail.get("stats"), Mapping) else {}
    summary = {
        "symbol_key": str(detail.get("symbol_key") or ""),
        "instrument_id": str(detail.get("instrument_id") or "").strip() or None,
        "symbol": str(detail.get("symbol") or "").strip().upper() or None,
        "timeframe": str(detail.get("timeframe") or "").strip().lower() or None,
        "display_label": str(detail.get("display_label") or "").strip() or None,
        "status": str(detail.get("status") or "waiting").strip() or "waiting",
        "continuity_status": str((_mapping(detail.get("continuity"))).get("status") or "").strip() or None,
        "last_event_at": detail.get("last_event_at"),
        "last_bar_time": last_candle.get("time"),
        "last_price": last_candle.get("close"),
        "candle_count": len(candles),
        "has_open_trade": bool(open_trade_list),
        "open_trade_count": len(open_trade_list),
        "last_trade_at": max(
            [str(entry.get("updated_at") or entry.get("closed_at") or entry.get("entry_time") or "") for entry in open_trade_list]
            or [""]
        )
        or None,
        "last_activity_at": detail.get("last_event_at") or None,
        "stats": stats,
    }
    return summary


def select_default_symbol_key(*, symbol_index: Mapping[str, Any], open_trades_index: Mapping[str, Any]) -> str | None:
    open_trade_by_symbol: Dict[str, list[Mapping[str, Any]]] = {}
    for trade in open_trades_index.values():
        if not isinstance(trade, Mapping):
            continue
        symbol_key = normalize_series_key(trade.get("symbol_key"))
        if not symbol_key:
            continue
        open_trade_by_symbol.setdefault(symbol_key, []).append(trade)

    candidates = []
    for symbol_key, summary in symbol_index.items():
        if not isinstance(summary, Mapping):
            continue
        symbol = str(summary.get("symbol") or "").strip().upper()
        timeframe = str(summary.get("timeframe") or "").strip().lower()
        last_activity = str(summary.get("last_activity_at") or "").strip()
        last_trade_at = str(summary.get("last_trade_at") or "").strip()
        candidates.append(
            {
                "symbol_key": symbol_key,
                "has_open_trade": bool(open_trade_by_symbol.get(symbol_key)),
                "last_trade_at": last_trade_at,
                "last_activity_at": last_activity,
                "symbol": symbol,
                "timeframe": timeframe,
            }
        )
    if not candidates:
        return None
    with_open_trade = [entry for entry in candidates if entry["has_open_trade"]]
    if with_open_trade:
        with_open_trade.sort(
            key=lambda entry: (
                entry["last_trade_at"],
                entry["last_activity_at"],
                entry["symbol"],
                entry["timeframe"],
                entry["symbol_key"],
            ),
            reverse=True,
        )
        return with_open_trade[0]["symbol_key"]
    candidates.sort(
        key=lambda entry: (
            entry["last_activity_at"],
            entry["symbol"],
            entry["timeframe"],
            entry["symbol_key"],
        ),
        reverse=True,
    )
    return candidates[0]["symbol_key"]


def apply_fact_batch(
    detail: Mapping[str, Any],
    *,
    facts: Any,
    seq: int,
    event_time: Any,
    continuity: Mapping[str, Any],
) -> Dict[str, Any]:
    next_detail = read_symbol_detail_state(detail, symbol_key=str(detail.get("symbol_key") or ""))
    next_detail["seq"] = int(seq)
    next_detail["last_event_at"] = _iso_or_none(event_time) or next_detail.get("last_event_at")
    next_detail["continuity"] = dict(continuity or next_detail.get("continuity") or {})

    delta: Dict[str, Any] = {
        "symbol_key": next_detail["symbol_key"],
        "detail_seq": int(seq),
        "event_time": event_time,
        "continuity": dict(next_detail["continuity"]),
        "runtime": None,
        "stats": None,
        "candle": None,
        "overlay_delta": None,
        "trade_upserts": [],
        "trade_removals": [],
        "log_append": [],
        "decision_append": [],
    }

    for fact in normalize_fact_entries(facts):
        fact_type = str(fact.get("fact_type") or "").strip().lower()
        if fact_type == FACT_TYPE_RUNTIME_STATE:
            runtime_payload = fact.get("runtime") if isinstance(fact.get("runtime"), Mapping) else {}
            next_detail["runtime"] = dict(runtime_payload)
            next_detail["status"] = str(runtime_payload.get("status") or next_detail.get("status") or "waiting")
            delta["runtime"] = dict(next_detail["runtime"])
            continue

        if fact_type == FACT_TYPE_SERIES_STATE:
            for field in ("instrument_id", "symbol", "timeframe"):
                if field in fact:
                    value = str(fact.get(field) or "").strip()
                    if field == "symbol":
                        value = value.upper()
                    if field == "timeframe":
                        value = value.lower()
                    next_detail[field] = value
            next_detail["display_label"] = display_label(
                symbol=str(next_detail.get("symbol") or ""),
                timeframe=str(next_detail.get("timeframe") or ""),
                symbol_key=str(next_detail.get("symbol_key") or ""),
            )
            continue

        if fact_type == FACT_TYPE_CANDLE_UPSERTED:
            candle = canonicalize_candle(fact.get("candle"))
            if candle is None:
                continue
            next_detail["candles"] = merge_candles(next_detail.get("candles"), [candle], limit=_MAX_CANDLES)
            delta["candle"] = dict(candle)
            continue

        if fact_type == FACT_TYPE_OVERLAY_OPS:
            overlay_delta = fact.get("overlay_delta")
            if not isinstance(overlay_delta, Mapping):
                continue
            next_detail["overlays"] = apply_overlay_delta(next_detail.get("overlays"), overlay_delta)
            delta["overlay_delta"] = dict(overlay_delta)
            continue

        if fact_type == FACT_TYPE_SERIES_STATS:
            stats = fact.get("stats")
            if not isinstance(stats, Mapping):
                continue
            next_detail["stats"] = dict(stats)
            delta["stats"] = dict(stats)
            continue

        if fact_type == FACT_TYPE_TRADE_UPSERTED:
            trade = normalize_trade(fact.get("trade"), symbol_key=str(next_detail.get("symbol_key") or ""))
            if trade is None:
                continue
            next_detail["recent_trades"] = _upsert_tail(
                next_detail.get("recent_trades"),
                trade,
                key_fields=("trade_id", "id"),
                limit=_MAX_TRADES,
            )
            if is_open_trade(trade):
                delta["trade_upserts"].append(dict(trade))
            else:
                delta["trade_removals"].append(str(trade.get("trade_id") or ""))
            continue

        if fact_type == FACT_TYPE_LOG_EMITTED:
            log_entry = fact.get("log")
            if not isinstance(log_entry, Mapping):
                continue
            next_detail["logs"] = _upsert_tail(
                next_detail.get("logs"),
                dict(log_entry),
                key_fields=("id", "event_id"),
                limit=_MAX_LOGS,
            )
            delta["log_append"].append(dict(log_entry))
            continue

        if fact_type == FACT_TYPE_DECISION_EMITTED:
            decision = fact.get("decision")
            if not isinstance(decision, Mapping):
                continue
            next_detail["decisions"] = _upsert_tail(
                next_detail.get("decisions"),
                dict(decision),
                key_fields=("event_id", "id"),
                limit=_MAX_DECISIONS,
            )
            delta["decision_append"].append(dict(decision))
            continue

    if not next_detail.get("status"):
        next_detail["status"] = str((_mapping(next_detail.get("runtime"))).get("status") or "waiting")
    return {"detail": next_detail, "delta": delta}


def detail_snapshot_contract(*, run_id: str, detail: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": str(run_id),
        "symbol_key": str(detail.get("symbol_key") or ""),
        "seq": int(detail.get("seq") or 0),
        "detail": {
            "symbol_key": str(detail.get("symbol_key") or ""),
            "instrument_id": detail.get("instrument_id"),
            "symbol": detail.get("symbol"),
            "timeframe": detail.get("timeframe"),
            "display_label": detail.get("display_label"),
            "status": detail.get("status"),
            "last_event_at": detail.get("last_event_at"),
            "continuity": dict(detail.get("continuity") or {}),
            "candles": list(detail.get("candles") or []) if isinstance(detail.get("candles"), list) else [],
            "overlays": list(detail.get("overlays") or []) if isinstance(detail.get("overlays"), list) else [],
            "recent_trades": list(detail.get("recent_trades") or []) if isinstance(detail.get("recent_trades"), list) else [],
            "logs": list(detail.get("logs") or []) if isinstance(detail.get("logs"), list) else [],
            "decisions": list(detail.get("decisions") or []) if isinstance(detail.get("decisions"), list) else [],
            "stats": dict(detail.get("stats") or {}) if isinstance(detail.get("stats"), Mapping) else {},
            "runtime": dict(detail.get("runtime") or {}) if isinstance(detail.get("runtime"), Mapping) else {},
        },
    }


def run_bootstrap_contract(
    *,
    bot_id: str,
    run_meta: Mapping[str, Any] | None,
    lifecycle: Mapping[str, Any] | None,
    health: Mapping[str, Any] | None,
    symbol_index: Mapping[str, Any],
    open_trades_index: Mapping[str, Any],
    selected_symbol_key: str | None,
    detail: Mapping[str, Any] | None,
    state: str,
    live: bool,
    message: str,
    seq: int,
) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "bot_id": str(bot_id),
        "state": str(state),
        "live": bool(live),
        "message": str(message),
        "run_meta": dict(run_meta or {}) if isinstance(run_meta, Mapping) else None,
        "lifecycle": dict(lifecycle or {}) if isinstance(lifecycle, Mapping) else {},
        "health": dict(health or {}) if isinstance(health, Mapping) else {},
        "symbol_summaries": [
            dict(value)
            for _, value in sorted(
                (
                    (key, value)
                    for key, value in symbol_index.items()
                    if normalize_series_key(key) and isinstance(value, Mapping)
                ),
                key=lambda item: (
                    str(item[1].get("symbol") or ""),
                    str(item[1].get("timeframe") or ""),
                    item[0],
                ),
            )
        ],
        "open_trades": [dict(value) for _, value in sorted(open_trades_index.items()) if isinstance(value, Mapping)],
        "selected_symbol_key": normalize_series_key(selected_symbol_key),
        "detail": detail_snapshot_contract(run_id=str((run_meta or {}).get("run_id") or ""), detail=detail)["detail"]
        if isinstance(detail, Mapping)
        else None,
        "seq": int(seq),
    }


__all__ = [
    "RUN_SCOPE_KEY",
    "SCHEMA_VERSION",
    "apply_fact_batch",
    "apply_overlay_delta",
    "build_symbol_summary",
    "detail_snapshot_contract",
    "display_label",
    "empty_run_summary",
    "empty_symbol_detail",
    "is_open_trade",
    "merge_candles",
    "normalize_candle_time",
    "normalize_trade",
    "overlay_identity",
    "overlay_revision",
    "project_overlay_state",
    "read_run_summary_state",
    "read_symbol_detail_state",
    "run_bootstrap_contract",
    "select_default_symbol_key",
    "serialize_run_summary_state",
    "serialize_symbol_detail_state",
]
