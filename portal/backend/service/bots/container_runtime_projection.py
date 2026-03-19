"""Projection and compaction helpers for container-runtime worker state."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def runtime_bar_marker(runtime_payload: Mapping[str, Any]) -> str:
    last_bar = runtime_payload.get("last_bar")
    if not isinstance(last_bar, Mapping):
        return ""
    marker = last_bar.get("end") or last_bar.get("time")
    if marker is None:
        return ""
    return str(marker).strip()


def runtime_trade_count(runtime_payload: Mapping[str, Any]) -> int:
    stats = runtime_payload.get("stats")
    if not isinstance(stats, Mapping):
        return -1
    return coerce_int(stats.get("total_trades"), -1)


def json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat() + "Z"
        return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def tail_window(entries: Any, max_items: int) -> List[Any]:
    if not isinstance(entries, list):
        return []
    if int(max_items) <= 0:
        return list(entries)
    if len(entries) <= int(max_items):
        return list(entries)
    return list(entries[-int(max_items):])


def compact_overlay_geometry(value: Any, *, max_points: int) -> Any:
    if isinstance(value, Mapping):
        compact: Dict[str, Any] = {}
        for key, entry in value.items():
            compact[str(key)] = compact_overlay_geometry(entry, max_points=max_points)
        return compact
    if isinstance(value, list):
        if int(max_points) <= 0:
            subset = list(value)
        elif len(value) <= int(max_points):
            subset = list(value)
        else:
            subset = list(value[-int(max_points):])
        return [compact_overlay_geometry(entry, max_points=max_points) for entry in subset]
    return value


def compact_overlay_window(overlays: Any, *, max_overlays: int, max_points: int) -> List[Dict[str, Any]]:
    if not isinstance(overlays, list):
        return []
    if int(max_overlays) <= 0:
        subset = list(overlays)
    elif len(overlays) <= int(max_overlays):
        subset = list(overlays)
    else:
        subset = list(overlays[-int(max_overlays):])
    compacted: List[Dict[str, Any]] = []
    for overlay in subset:
        if not isinstance(overlay, Mapping):
            continue
        compacted.append(dict(compact_overlay_geometry(overlay, max_points=max_points)))
    return compacted


def compact_trades_window(trades: Any, *, max_closed: int) -> List[Dict[str, Any]]:
    if not isinstance(trades, list):
        return []
    if int(max_closed) <= 0:
        return [dict(entry) for entry in trades if isinstance(entry, Mapping)]

    keep_mask: List[bool] = [False] * len(trades)
    closed_indices: List[int] = []
    for index, trade in enumerate(trades):
        if not isinstance(trade, Mapping):
            continue
        closed_at = trade.get("closed_at")
        if closed_at:
            closed_indices.append(index)
            continue
        keep_mask[index] = True
    for index in closed_indices[-int(max_closed):]:
        keep_mask[index] = True

    compacted: List[Dict[str, Any]] = []
    for index, keep in enumerate(keep_mask):
        if not keep:
            continue
        trade = trades[index]
        if isinstance(trade, Mapping):
            compacted.append(dict(trade))
    return compacted


def compact_view_state_payload(
    chart_snapshot: Mapping[str, Any],
    *,
    max_series: int,
    max_candles: int,
    max_overlays: int,
    max_overlay_points: int,
    max_closed_trades: int,
    max_logs: int,
    max_decisions: int,
    max_warnings: int,
) -> Dict[str, Any]:
    raw_series = chart_snapshot.get("series")
    compact_series: List[Dict[str, Any]] = []
    if isinstance(raw_series, list):
        for entry in raw_series[: max(int(max_series), 0) or None]:
            if not isinstance(entry, Mapping):
                continue
            compact_series.append(
                {
                    "strategy_id": entry.get("strategy_id"),
                    "symbol": entry.get("symbol"),
                    "timeframe": entry.get("timeframe"),
                    "datasource": entry.get("datasource"),
                    "exchange": entry.get("exchange"),
                    "instrument": entry.get("instrument"),
                    "candles": tail_window(entry.get("candles"), max_candles),
                    "overlays": compact_overlay_window(
                        entry.get("overlays"),
                        max_overlays=max_overlays,
                        max_points=max_overlay_points,
                    ),
                    "stats": dict(entry.get("stats") or {}) if isinstance(entry.get("stats"), Mapping) else {},
                }
            )

    runtime_payload = chart_snapshot.get("runtime")
    compact_runtime = dict(runtime_payload) if isinstance(runtime_payload, Mapping) else {}

    return {
        "series": compact_series,
        "trades": compact_trades_window(chart_snapshot.get("trades"), max_closed=max_closed_trades),
        "logs": tail_window(chart_snapshot.get("logs"), max_logs),
        "decisions": tail_window(chart_snapshot.get("decisions"), max_decisions),
        "warnings": tail_window(chart_snapshot.get("warnings"), max_warnings),
        "runtime": compact_runtime,
    }


def merge_runtime_stats(runtime_payloads: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    if not runtime_payloads:
        return {}
    counter_keys = (
        "total_trades",
        "completed_trades",
        "legs_closed",
        "wins",
        "losses",
        "breakeven_trades",
        "long_trades",
        "short_trades",
    )
    pnl_keys = ("gross_pnl", "fees_paid", "total_fees", "net_pnl")
    summary: Dict[str, Any] = {key: 0 for key in counter_keys}
    for key in pnl_keys:
        summary[key] = 0.0
    avg_win_weighted = 0.0
    avg_loss_weighted = 0.0
    max_drawdown = 0.0
    largest_win = None
    largest_loss = None
    quote_currency: str | None = None
    multi_currency = False

    for runtime in runtime_payloads:
        stats = runtime.get("stats")
        if not isinstance(stats, Mapping):
            continue
        for key in counter_keys:
            summary[key] += coerce_int(stats.get(key), 0)
        for key in pnl_keys:
            summary[key] += coerce_float(stats.get(key), 0.0)
        wins = max(coerce_int(stats.get("wins"), 0), 0)
        losses = max(coerce_int(stats.get("losses"), 0), 0)
        avg_win_weighted += coerce_float(stats.get("avg_win"), 0.0) * wins
        avg_loss_weighted += coerce_float(stats.get("avg_loss"), 0.0) * losses
        max_drawdown = max(max_drawdown, coerce_float(stats.get("max_drawdown"), 0.0))
        current_largest_win = coerce_float(stats.get("largest_win"), 0.0)
        current_largest_loss = coerce_float(stats.get("largest_loss"), 0.0)
        largest_win = current_largest_win if largest_win is None else max(largest_win, current_largest_win)
        largest_loss = current_largest_loss if largest_loss is None else min(largest_loss, current_largest_loss)
        current_quote = stats.get("quote_currency")
        if isinstance(current_quote, str) and current_quote:
            if quote_currency is None:
                quote_currency = current_quote
            elif quote_currency != current_quote:
                multi_currency = True

    completed = max(coerce_int(summary.get("completed_trades"), 0), 0)
    wins = max(coerce_int(summary.get("wins"), 0), 0)
    losses = max(coerce_int(summary.get("losses"), 0), 0)
    summary["win_rate"] = round((wins / completed), 4) if completed else 0.0
    summary["avg_win"] = round(avg_win_weighted / wins, 4) if wins else 0.0
    summary["avg_loss"] = round(avg_loss_weighted / losses, 4) if losses else 0.0
    summary["largest_win"] = round(float(largest_win or 0.0), 4)
    summary["largest_loss"] = round(float(largest_loss or 0.0), 4)
    summary["max_drawdown"] = round(max_drawdown, 4)
    for key in pnl_keys:
        summary[key] = round(coerce_float(summary.get(key), 0.0), 4)
    if quote_currency:
        summary["quote_currency"] = "MULTI" if multi_currency else quote_currency
    return summary


def merge_runtime_payloads(
    runtime_payloads: Sequence[Mapping[str, Any]],
    *,
    worker_count: int,
    active_workers: int,
    degraded_symbols: Sequence[str],
) -> Dict[str, Any]:
    progress_values = [coerce_float(payload.get("progress"), 0.0) for payload in runtime_payloads]
    status_values = [str(payload.get("status") or "").lower() for payload in runtime_payloads]
    paused = any(bool(payload.get("paused")) for payload in runtime_payloads)
    if active_workers > 0:
        status = "running"
    elif degraded_symbols:
        status = "degraded"
    elif status_values and all(value == "completed" for value in status_values):
        status = "completed"
    elif any(value in {"error", "failed", "crashed"} for value in status_values):
        status = "error"
    else:
        status = "stopped"

    runtime: Dict[str, Any] = {
        "status": status,
        "progress": round(sum(progress_values) / len(progress_values), 6) if progress_values else 0.0,
        "paused": paused,
        "worker_count": int(worker_count),
        "active_workers": int(active_workers),
        "degraded_symbols": sorted({str(symbol) for symbol in degraded_symbols if str(symbol).strip()}),
        "stats": merge_runtime_stats(runtime_payloads),
    }
    return runtime


def merge_worker_view_state(
    latest_worker_view_state: Mapping[str, Mapping[str, Any]],
    *,
    worker_count: int,
    active_workers: int,
    degraded_symbols: Sequence[str],
) -> Dict[str, Any]:
    series_by_key: MutableMapping[str, Dict[str, Any]] = {}
    trades_by_key: MutableMapping[str, Dict[str, Any]] = {}
    logs_by_key: MutableMapping[str, Dict[str, Any]] = {}
    decisions_by_key: MutableMapping[str, Dict[str, Any]] = {}
    warnings: List[Any] = []
    runtime_payloads: List[Mapping[str, Any]] = []

    for envelope in latest_worker_view_state.values():
        chart = envelope.get("view_state")
        if not isinstance(chart, Mapping):
            continue
        raw_series = chart.get("series")
        if isinstance(raw_series, list):
            for entry in raw_series:
                if not isinstance(entry, Mapping):
                    continue
                key = "|".join(
                    [
                        str(entry.get("strategy_id") or ""),
                        str(entry.get("symbol") or ""),
                        str(entry.get("timeframe") or ""),
                    ]
                )
                series_by_key[key] = dict(entry)
        raw_trades = chart.get("trades")
        if isinstance(raw_trades, list):
            for index, trade in enumerate(raw_trades):
                if not isinstance(trade, Mapping):
                    continue
                trade_id = str(trade.get("trade_id") or trade.get("id") or "").strip()
                if not trade_id:
                    trade_id = "|".join(
                        [
                            str(trade.get("symbol") or ""),
                            str(trade.get("entry_time") or ""),
                            str(trade.get("direction") or ""),
                            str(index),
                        ]
                    )
                trades_by_key[trade_id] = dict(trade)
        raw_warnings = chart.get("warnings")
        if isinstance(raw_warnings, list):
            warnings.extend(raw_warnings)
        raw_logs = chart.get("logs")
        if isinstance(raw_logs, list):
            for index, log_entry in enumerate(raw_logs):
                if not isinstance(log_entry, Mapping):
                    continue
                log_key = str(log_entry.get("id") or "").strip()
                if not log_key:
                    log_key = "|".join(
                        [
                            str(log_entry.get("timestamp") or log_entry.get("event_time") or ""),
                            str(log_entry.get("event") or log_entry.get("message") or ""),
                            str(log_entry.get("symbol") or ""),
                            str(index),
                        ]
                    )
                logs_by_key[log_key] = dict(log_entry)
        raw_decisions = chart.get("decisions")
        if isinstance(raw_decisions, list):
            for index, decision in enumerate(raw_decisions):
                if not isinstance(decision, Mapping):
                    continue
                event_id = str(decision.get("event_id") or "").strip()
                if not event_id:
                    event_id = "|".join(
                        [
                            str(decision.get("event_ts") or decision.get("created_at") or ""),
                            str(decision.get("event_type") or ""),
                            str(decision.get("event_subtype") or ""),
                            str(decision.get("symbol") or ""),
                            str(decision.get("trade_id") or ""),
                            str(index),
                        ]
                    )
                decisions_by_key[event_id] = dict(decision)
        runtime_payload = chart.get("runtime")
        if isinstance(runtime_payload, Mapping):
            runtime_payloads.append(runtime_payload)

    runtime = merge_runtime_payloads(
        runtime_payloads,
        worker_count=worker_count,
        active_workers=active_workers,
        degraded_symbols=degraded_symbols,
    )
    if degraded_symbols:
        warnings.append(
            {
                "id": f"degraded:{','.join(sorted(set(degraded_symbols)))}",
                "type": "symbol_degraded",
                "message": "One or more symbols were degraded due to worker failure.",
                "context": {"symbols": sorted(set(degraded_symbols))},
                "level": "warning",
                "source": "container_runtime",
                "timestamp": utc_now_iso(),
            }
        )

    merged_series = list(series_by_key.values())
    merged_series.sort(key=lambda entry: (str(entry.get("symbol") or ""), str(entry.get("timeframe") or "")))
    merged_trades = list(trades_by_key.values())
    merged_logs = list(logs_by_key.values())
    merged_decisions = list(decisions_by_key.values())
    return {
        "series": merged_series,
        "trades": merged_trades,
        "logs": merged_logs,
        "decisions": merged_decisions,
        "warnings": warnings,
        "runtime": runtime,
    }


__all__ = [
    "compact_view_state_payload",
    "coerce_float",
    "coerce_int",
    "json_safe",
    "merge_runtime_payloads",
    "merge_runtime_stats",
    "merge_worker_view_state",
    "runtime_bar_marker",
    "runtime_trade_count",
    "utc_now_iso",
]
