"""Generate bounded, deterministic run exports for LLM analysis."""

from __future__ import annotations

import csv
import io
import json
import logging
import zipfile
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from data_providers.utils.ohlcv import interval_to_timedelta
from sqlalchemy import create_engine

from portal.backend.service.market.stats_contract import REGIME_VERSION, STATS_VERSION

from data_providers.config.runtime import runtime_config_from_env
from utils.log_context import build_log_context, with_log_context

from ...storage.storage import _parse_optional_timestamp
from .. import report_data
from .repository import ExportTables, ReportExportRepository
from .schema import (
    CANDLE_STATS_BASE_COLUMNS,
    DECISION_LEDGER_BASE_COLUMNS,
    DERIVATIVES_COLUMNS,
    INSTRUMENT_COLUMNS,
    LEDGER_EVENT_COLUMNS,
    RAW_CANDLES_COLUMNS,
    REGIME_STATS_BASE_COLUMNS,
    RUN_COLUMNS,
    TRADE_COLUMNS_BASE,
    TRADE_EVENT_COLUMNS,
    derive_fieldnames,
)


logger = logging.getLogger(__name__)


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if value in (None, ""):
        return None
    parsed = _parse_optional_timestamp(value)
    if not parsed:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True)


def _mapping_from_jsonish(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


DEFAULT_STATS_VERSION = STATS_VERSION
DEFAULT_REGIME_VERSION = REGIME_VERSION


def _write_csv(
    zip_file: zipfile.ZipFile,
    name: str,
    rows: Sequence[Dict[str, Any]],
    fieldnames: Optional[Sequence[str]] = None,
) -> None:
    if fieldnames is None:
        # Derive from rows if no explicit order provided
        keys = set()
        for row in rows:
            keys.update(row.keys())
        fieldnames = sorted(keys)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    zip_file.writestr(name, buffer.getvalue())


def _select_fields(
    row: Dict[str, Any],
    base_order: Sequence[str],
    dynamic_limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a new row preserving base order and limited dynamic keys."""
    base = {key: row.get(key) for key in base_order if key in row}
    dynamic_keys = [k for k in row.keys() if k not in base_order]
    dynamic_keys.sort()
    if dynamic_limit is not None:
        dynamic_keys = dynamic_keys[:dynamic_limit]
    for key in dynamic_keys:
        base[key] = row.get(key)
    return base


def _floor_to_interval(value: Optional[datetime], interval_seconds: int) -> Optional[datetime]:
    if not value:
        return None
    value = value.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    total_seconds = int((value - epoch).total_seconds())
    remainder = total_seconds % interval_seconds
    return epoch + timedelta(seconds=total_seconds - remainder)


def _resolve_run_window(run: Dict[str, Any]) -> Tuple[datetime, datetime]:
    start = _parse_iso(run.get("backtest_start"))
    end = _parse_iso(run.get("backtest_end"))
    if not start or not end:
        config_range = (run.get("config_snapshot") or {}).get("date_range") or {}
        start = start or _parse_iso(config_range.get("start"))
        end = end or _parse_iso(config_range.get("end"))
    if not start or not end:
        raise ValueError("Run is missing backtest_start/backtest_end window.")
    return start, end


def _resolve_symbols(run: Dict[str, Any], trades: Sequence[Dict[str, Any]]) -> List[str]:
    symbols = list(run.get("symbols") or [])
    for trade in trades:
        symbol = trade.get("symbol")
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _resolve_instruments(run: Dict[str, Any], symbols: Sequence[str]) -> Tuple[List[Dict[str, Any]], Dict[str, Optional[str]]]:
    instruments: List[Dict[str, Any]] = []
    instrument_by_symbol: Dict[str, Optional[str]] = {}
    snapshots: List[Dict[str, Any]] = []
    for strategy in (run.get("config_snapshot") or {}).get("strategies") or []:
        snapshots.extend(strategy.get("instruments") or [])

    snapshot_map = {}
    for entry in snapshots:
        symbol = entry.get("symbol")
        if symbol:
            snapshot_map[str(symbol)] = entry

    for symbol in symbols:
        record = snapshot_map.get(symbol)
        if record is None:
            record = report_data.find_instrument(run.get("datasource"), run.get("exchange"), symbol)
        if record is None:
            raise KeyError(f"Instrument not found for symbol {symbol}")
        instruments.append(record)
        instrument_by_symbol[symbol] = record.get("id")

    return instruments, instrument_by_symbol


def _regime_metrics_from_row(regime_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not regime_row:
        return {}
    raw = regime_row.get("regime_json")
    if not raw:
        return {}
    try:
        regime = json.loads(raw)
    except ValueError:
        return {}
    volatility = regime.get("volatility") or {}
    structure = regime.get("structure") or {}
    expansion = regime.get("expansion") or {}
    return {
        "entry_tr_pct": volatility.get("tr_pct"),
        "entry_atr_ratio": volatility.get("atr_ratio"),
        "entry_atr_zscore": volatility.get("atr_zscore"),
        "entry_atr_slope": expansion.get("atr_slope"),
        "entry_overlap_pct": expansion.get("overlap_pct"),
        "entry_directional_efficiency": structure.get("directional_efficiency"),
        "entry_range_position": structure.get("range_position"),
    }


def _build_candle_stats_export_rows(
    db_rows: Sequence[Dict[str, Any]],
    *,
    symbol: str,
    timeframe: str,
    stats_key_limit: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entry in db_rows:
        stats = _mapping_from_jsonish(entry.get("stats"))
        row = {
            "instrument_id": entry.get("instrument_id"),
            "symbol": symbol,
            "timeframe": timeframe,
            "timeframe_seconds": entry.get("timeframe_seconds"),
            "candle_time": entry.get("candle_time"),
            "stats_version": entry.get("stats_version"),
            "computed_at": entry.get("computed_at"),
            "stats_json": _json_dumps(stats),
            **stats,
        }
        rows.append(_select_fields(row, CANDLE_STATS_BASE_COLUMNS, stats_key_limit))
    return rows


def _build_regime_stats_export_rows(
    db_rows: Sequence[Dict[str, Any]],
    *,
    symbol: str,
    timeframe: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for entry in db_rows:
        regime = _mapping_from_jsonish(entry.get("regime"))
        row = {
            "instrument_id": entry.get("instrument_id"),
            "symbol": symbol,
            "timeframe": timeframe,
            "timeframe_seconds": entry.get("timeframe_seconds"),
            "candle_time": entry.get("candle_time"),
            "regime_version": entry.get("regime_version"),
            "computed_at": entry.get("computed_at"),
            "regime_json": _json_dumps(regime),
            "volatility_state": (regime.get("volatility") or {}).get("state"),
            "structure_state": (regime.get("structure") or {}).get("state"),
            "expansion_state": (regime.get("expansion") or {}).get("state"),
            "liquidity_state": (regime.get("liquidity") or {}).get("state"),
            "confidence": regime.get("confidence"),
        }
        rows.append(_select_fields(row, REGIME_STATS_BASE_COLUMNS))
    return rows


def _build_trade_rows(
    trades: Sequence[Dict[str, Any]],
    instrument_by_symbol: Dict[str, Optional[str]],
    timeframe_seconds: int,
    timeframe: str,
    stats_index: Dict[Tuple[str, int, str], List[Tuple[datetime, Dict[str, Any]]]],
    regime_index: Dict[Tuple[str, int, str], List[Tuple[datetime, Dict[str, Any]]]],
    stats_version: str,
    regime_version: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    timeframe_delta = timedelta(seconds=timeframe_seconds)
    for trade in trades:
        entry = _parse_iso(trade.get("entry_time"))
        exit_time = _parse_iso(trade.get("exit_time"))
        duration = None
        if entry and exit_time:
            duration = int((exit_time - entry).total_seconds())
        net_pnl = trade.get("net_pnl")
        was_win = None if net_pnl is None else bool(net_pnl > 0)
        entry_candle = _floor_to_interval(entry, timeframe_seconds) if entry else None
        entry_candle_time = _isoformat(entry_candle)
        instrument_id = instrument_by_symbol.get(trade.get("symbol"))
        stats_row = None
        regime_row = None
        stats_fallback = False
        regime_fallback = False
        if entry_candle and instrument_id:
            stats_row, stats_fallback = _lookup_entry_row(
                stats_index,
                instrument_id,
                timeframe_seconds,
                stats_version,
                entry_candle,
                timeframe_delta,
            )
            regime_row, regime_fallback = _lookup_entry_row(
                regime_index,
                instrument_id,
                timeframe_seconds,
                regime_version,
                entry_candle,
                timeframe_delta,
            )
        entry_fallback_used = stats_fallback or regime_fallback
        entry_regime_missing = regime_row is None
        entry_stats_warmup = bool(stats_row.get("slope_stability_warmup")) if stats_row else False
        entry_volatility_state = regime_row.get("volatility_state") if regime_row else None
        entry_structure_state = regime_row.get("structure_state") if regime_row else None
        entry_expansion_state = regime_row.get("expansion_state") if regime_row else None
        entry_liquidity_state = regime_row.get("liquidity_state") if regime_row else None
        entry_regime_confidence = regime_row.get("confidence") if regime_row else None
        entry_values = {
            "entry_atr_zscore": stats_row.get("atr_zscore") if stats_row else None,
            "entry_atr_ratio": stats_row.get("atr_ratio") if stats_row else None,
            "entry_atr_slope": stats_row.get("atr_slope") if stats_row else None,
            "entry_tr_pct": stats_row.get("tr_pct") if stats_row else None,
            "entry_overlap_pct": stats_row.get("overlap_pct") if stats_row else None,
            "entry_directional_efficiency": stats_row.get("directional_efficiency") if stats_row else None,
            "entry_range_position": stats_row.get("range_position") if stats_row else None,
        }
        regime_metrics = _regime_metrics_from_row(regime_row)
        for key, value in regime_metrics.items():
            if entry_values.get(key) is None:
                entry_values[key] = value
        entry_metrics = dict(trade.get("metrics") or {})
        entry_metrics.update(
            {
                "entry_tr_pct": entry_values["entry_tr_pct"],
                "entry_atr_ratio": entry_values["entry_atr_ratio"],
                "entry_atr_slope": entry_values["entry_atr_slope"],
                "entry_atr_zscore": entry_values["entry_atr_zscore"],
                "entry_overlap_pct": entry_values["entry_overlap_pct"],
                "entry_directional_efficiency": entry_values["entry_directional_efficiency"],
                "entry_range_position": entry_values["entry_range_position"],
                "entry_volatility_state": entry_volatility_state,
                "entry_structure_state": entry_structure_state,
                "entry_expansion_state": entry_expansion_state,
                "entry_liquidity_state": entry_liquidity_state,
                "entry_regime_confidence": entry_regime_confidence,
                "entry_stats_warmup": entry_stats_warmup,
                "entry_regime_missing": entry_regime_missing,
                "entry_fallback_used": entry_fallback_used,
            }
        )
        rows.append(
            {
                "trade_id": trade.get("id"),
                "run_id": trade.get("run_id"),
                "bot_id": trade.get("bot_id"),
                "strategy_id": trade.get("strategy_id"),
                "instrument_id": instrument_id,
                "symbol": trade.get("symbol"),
                "direction": trade.get("direction"),
                "status": trade.get("status"),
                "contracts": trade.get("contracts"),
                "entry_time": trade.get("entry_time"),
                "entry_price": trade.get("entry_price"),
                "stop_price": trade.get("stop_price"),
                "exit_time": trade.get("exit_time"),
                "gross_pnl": trade.get("gross_pnl"),
                "fees_paid": trade.get("fees_paid"),
                "net_pnl": net_pnl,
                "duration_seconds": duration,
                "was_win": was_win,
                "metrics_json": _json_dumps(entry_metrics),
                "created_at": trade.get("created_at"),
                "updated_at": trade.get("updated_at"),
                "timeframe": timeframe,
                "entry_candle_time": entry_candle_time,
                "entry_stats_warmup_flags": entry_stats_warmup,
                "entry_fallback_used": entry_fallback_used,
                "entry_regime_missing": entry_regime_missing,
                "entry_volatility_state": entry_volatility_state,
                "entry_structure_state": entry_structure_state,
                "entry_expansion_state": entry_expansion_state,
                "entry_liquidity_state": entry_liquidity_state,
                "entry_regime_confidence": entry_regime_confidence,
                "entry_tr_pct": entry_values["entry_tr_pct"],
                "entry_atr_ratio": entry_values["entry_atr_ratio"],
                "entry_atr_slope": entry_values["entry_atr_slope"],
                "entry_atr_zscore": entry_values["entry_atr_zscore"],
                "entry_overlap_pct": entry_values["entry_overlap_pct"],
                "entry_directional_efficiency": entry_values["entry_directional_efficiency"],
                "entry_range_position": entry_values["entry_range_position"],
            }
        )
    return rows


def _build_trade_event_rows(
    events: Sequence[Dict[str, Any]],
    instrument_by_symbol: Dict[str, Optional[str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for event in events:
        rows.append(
            {
                "event_id": event.get("id"),
                "trade_id": event.get("trade_id"),
                "bot_id": event.get("bot_id"),
                "strategy_id": event.get("strategy_id"),
                "instrument_id": instrument_by_symbol.get(event.get("symbol")),
                "symbol": event.get("symbol"),
                "event_type": event.get("event_type"),
                "reason_code": event.get("reason_code"),
                "leg": event.get("leg"),
                "contracts": event.get("contracts"),
                "price": event.get("price"),
                "pnl": event.get("pnl"),
                "event_time": event.get("event_time"),
                "created_at": event.get("created_at"),
            }
        )
    return rows


def _build_ledger_rows(
    events: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for event in events:
        rows.append(
            {
                "event_id": event.get("event_id"),
                "parent_event_id": event.get("parent_event_id"),
                "reason_code": event.get("reason_code"),
                "event_type": event.get("event_type"),
                "event_subtype": event.get("event_subtype"),
                "event_ts": event.get("event_ts"),
                "created_at": event.get("created_at"),
                "symbol": event.get("symbol"),
                "instrument_id": event.get("instrument_id"),
                "trade_id": event.get("trade_id"),
                "strategy_id": event.get("strategy_id"),
                "strategy_name": event.get("strategy_name"),
                "timeframe": event.get("timeframe"),
                "side": event.get("side"),
                "qty": event.get("qty"),
                "price": event.get("price"),
                "event_impact_pnl": event.get("event_impact_pnl"),
                "trade_net_pnl": event.get("trade_net_pnl"),
                "reason_detail": event.get("reason_detail"),
                "evidence_refs_json": _json_dumps(event.get("evidence_refs") or []),
                "alternatives_rejected_json": _json_dumps(event.get("alternatives_rejected") or []),
                "context_json": _json_dumps(event.get("context") or {}),
            }
        )
    return rows


def _build_decision_ledger_rows(
    events: Sequence[Dict[str, Any]],
    default_timeframe_seconds: Optional[int],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for event in events:
        timeframe = event.get("timeframe")
        timeframe_secs = default_timeframe_seconds
        if timeframe:
            try:
                timeframe_secs = int(interval_to_timedelta(timeframe).total_seconds())
            except Exception as exc:
                logger.warning(
                    with_log_context(
                        "report_export_timeframe_parse_failed",
                        build_log_context(
                            timeframe=timeframe,
                            event_id=event.get("event_id"),
                            error=str(exc),
                        ),
                    )
                )
        rows.append(
            {
                "ts": event.get("event_ts"),
                "decision_id": event.get("event_id"),
                "trade_id": event.get("trade_id"),
                "instrument_id": event.get("instrument_id"),
                "symbol": event.get("symbol"),
                "timeframe_seconds": timeframe_secs,
                "decision_type": event.get("event_type"),
                "action": event.get("event_subtype"),
                "reason_code": event.get("reason_code"),
                "outcome": event.get("event_subtype") if event.get("event_type") == "outcome" else None,
                "context_json": _json_dumps(event.get("context") or {}),
                "side": event.get("side"),
                "qty": event.get("qty"),
                "price": event.get("price"),
                "event_impact_pnl": event.get("event_impact_pnl"),
                "trade_net_pnl": event.get("trade_net_pnl"),
                "reason_detail": event.get("reason_detail"),
                "evidence_refs_json": _json_dumps(event.get("evidence_refs") or []),
                "alternatives_rejected_json": _json_dumps(event.get("alternatives_rejected") or []),
                "created_at": event.get("created_at"),
            }
        )
    return rows


def _build_instrument_rows(instruments: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for instrument in instruments:
        flags = {
            "instrument_type": instrument.get("instrument_type"),
            "can_short": instrument.get("can_short"),
            "short_requires_borrow": instrument.get("short_requires_borrow"),
            "has_funding": instrument.get("has_funding"),
            "expiry_ts": instrument.get("expiry_ts"),
        }
        fees = {
            "maker_fee_rate": instrument.get("maker_fee_rate"),
            "taker_fee_rate": instrument.get("taker_fee_rate"),
            "tick_size": instrument.get("tick_size"),
            "tick_value": instrument.get("tick_value"),
            "contract_size": instrument.get("contract_size"),
            "min_order_size": instrument.get("min_order_size"),
        }
        rows.append(
            {
                "instrument_id": instrument.get("id"),
                "symbol": instrument.get("symbol"),
                "datasource": instrument.get("datasource"),
                "exchange": instrument.get("exchange"),
                "flags": _json_dumps(flags),
                "fees": _json_dumps(fees),
                "metadata_json": _json_dumps(instrument.get("metadata") or {}),
            }
        )
    return rows


def _build_run_rows(run: Dict[str, Any], duration_seconds: Optional[int]) -> List[Dict[str, Any]]:
    return [
        {
            "run_id": run.get("run_id"),
            "bot_id": run.get("bot_id"),
            "bot_name": run.get("bot_name"),
            "strategy_id": run.get("strategy_id"),
            "strategy_name": run.get("strategy_name"),
            "run_type": run.get("run_type"),
            "status": run.get("status"),
            "timeframe": run.get("timeframe"),
            "datasource": run.get("datasource"),
            "exchange": run.get("exchange"),
            "symbols": _json_dumps(run.get("symbols") or []),
            "backtest_start": run.get("backtest_start"),
            "backtest_end": run.get("backtest_end"),
            "started_at": run.get("started_at"),
            "ended_at": run.get("ended_at"),
            "duration_seconds": duration_seconds,
            "summary_json": _json_dumps(run.get("summary") or {}),
            "config_snapshot_json": _json_dumps(run.get("config_snapshot") or {}),
            "created_at": run.get("created_at"),
            "updated_at": run.get("updated_at"),
        }
    ]


def _build_readme(
    run_id: str,
    start: datetime,
    end: datetime,
    pre_roll_hours: int,
    post_roll_hours: int,
    stats_versions: Sequence[str],
    stats_key_limit: int,
) -> str:
    lines = [
        f"Run export for run_id={run_id}",
        "",
        "Strategy Parameter Analysis – Long-Horizon Optimization",
        "",
        "Objective:",
        "- Evaluate parameters for long-term survival and robustness across symbols and market regimes.",
        "- Resist optimizing for short-term profit, win rate, or recent PnL; prioritize stability and overfitting resistance.",
        "",
        "Constraints & philosophy:",
        "- Parameter robustness matters more than peak performance.",
        "- Prefer parameter sets that perform \"well enough\" across many conditions versus exceptional in a narrow window.",
        "- Evaluate across multiple instruments, regimes (trend, range, volatility expansion/contraction), and time periods.",
        "- Parameters should degrade gracefully when conditions change.",
        "- Large drawdowns, tail risk, and outcome volatility matter more than raw return.",
        "",
        "Platform constraints:",
        "- Strategy execution time advances on one primary timeframe per run; indicators may consume additional source timeframes via runtime input specs.",
        "- A strategy uses one datasource/provider/venue per run; do not mix providers, venues, or symbol feeds inside a run.",
        "- Derived artifacts must respect known-at timing; nothing should appear before it could exist live.",
        "",
        "Files included:",
        "- run: Core run metadata and JSON snapshots.",
        "- instruments: Instruments referenced by the run symbols.",
        "- trades: Trades for the run with derived fields (duration_seconds, was_win).",
        "- trade_events: Trade stop/target events for run trades.",
        "- ledger_events: Decision ledger events captured during the run (if any).",
        "- candles_raw: Raw OHLCV candles bounded to the requested window.",
        "- derivatives_state: Per-instrument funding/derivatives state (bounded).",
        "- candle_stats_flat: DB-backed candle_stats rows for the requested instruments/window.",
        "- regime_stats_flat: DB-backed regime_stats rows for the requested instruments/window.",
        "- decision_ledger: Canonical runtime.* decision/execution events from the bot ledger.",
        "",
        "Time window:",
        f"- backtest_start={_isoformat(start)}",
        f"- backtest_end={_isoformat(end)}",
        f"- pre_roll_hours={pre_roll_hours}",
        f"- post_roll_hours={post_roll_hours}",
        "",
        f"Stats versions: {', '.join(stats_versions) if stats_versions else 'none'}",
        f"Stats key limit: {stats_key_limit}",
        "",
        "What to look for:",
        "- Parameter sets with stable expectancy across regimes (trend, range, volatility expansion/contraction).",
        "- Sensitivity to small parameter changes and any hidden regime dependencies.",
        "- Overfitting signals such as sharp optima or recent-period dominance.",
        "- Cross-symbol consistency for the symbols present in this run.",
        "- Risk asymmetry: drawdowns, tail risk, and outcome volatility relative to upside.",
        "",
        "What to avoid:",
        "- Tuning to the most recent data or optimizing for maximum PnL/win rate alone.",
        "- Recommending parameters that only work on a single symbol.",
        "- \"Trade more / trade less\" style advice without structural reasoning.",
        "",
        "Expected output:",
        "- Ranked assessment of parameter robustness with clear trade-offs versus performance.",
        "- Identification of fragile versus stable parameter regions and recommended ranges (not exact values).",
        "- Structural observations about long-term viability and how parameters degrade when regimes shift.",
        "",
        "Notes:",
        "- All timestamps are UTC ISO8601.",
        "- JSON columns are raw JSON strings.",
    ]
    return "\n".join(lines)


def _build_time_index(
    rows: Sequence[Dict[str, Any]],
    version_key: str,
) -> Dict[Tuple[str, int, str], List[Tuple[datetime, Dict[str, Any]]]]:
    index: Dict[Tuple[str, int, str], List[Tuple[datetime, Dict[str, Any]]]] = defaultdict(list)
    for row in rows:
        key = (row["instrument_id"], row["timeframe_seconds"], row.get(version_key) or "")
        dt = _parse_iso(row.get("candle_time"))
        if not dt:
            continue
        index[key].append((dt, row))
    for entries in index.values():
        entries.sort(key=lambda item: item[0])
    return index


def _lookup_entry_row(
    index: Dict[Tuple[str, int, str], List[Tuple[datetime, Dict[str, Any]]]],
    instrument_id: str,
    timeframe_seconds: int,
    version: str,
    target: datetime,
    delta: timedelta,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    key = (instrument_id, timeframe_seconds, version)
    entries = index.get(key)
    if not entries:
        return None, False
    times = [entry[0] for entry in entries]
    pos = bisect_right(times, target)
    if pos and pos <= len(entries) and times[pos - 1] == target:
        return entries[pos - 1][1], False
    if pos:
        prev_dt, prev_row = entries[pos - 1]
        if target - prev_dt <= delta:
            return prev_row, True
    return None, False


def build_run_export(
    run_id: str,
    *,
    pre_roll_hours: int = 48,
    post_roll_hours: int = 48,
    stats_versions: Optional[Sequence[str]] = None,
    stats_key_limit: int = 20,
    regime_versions: Optional[Sequence[str]] = None,
) -> Tuple[bytes, str]:
    """Return a ZIP export payload and filename for the requested run."""

    stats_versions_param_list = list(stats_versions) if stats_versions else [DEFAULT_STATS_VERSION]
    regime_versions_param_list = list(regime_versions) if regime_versions else [DEFAULT_REGIME_VERSION]
    context = build_log_context(
        run_id=run_id,
        pre_roll_hours=pre_roll_hours,
        post_roll_hours=post_roll_hours,
        stats_versions=stats_versions_param_list,
        stats_key_limit=stats_key_limit,
        regime_versions=regime_versions_param_list,
    )
    logger.info(with_log_context("report_export_start", context))

    if pre_roll_hours < 0 or post_roll_hours < 0:
        raise ValueError("pre_roll_hours and post_roll_hours must be >= 0")

    run = report_data.get_run(run_id)
    if not run:
        raise KeyError(f"Run not found: {run_id}")

    trades = report_data.list_trades_for_run(run_id)
    trade_ids = [trade.get("id") for trade in trades if trade.get("id")]
    trade_events = report_data.list_trade_events_for_trades(trade_ids)
    decision_ledger = report_data.list_decision_ledger(run_id)
    stats_versions_param = stats_versions_param_list
    regime_versions_param = regime_versions_param_list

    start, end = _resolve_run_window(run)
    window_start = start - timedelta(hours=pre_roll_hours)
    window_end = end + timedelta(hours=post_roll_hours)

    symbols = _resolve_symbols(run, trades)
    instruments, instrument_by_symbol = _resolve_instruments(run, symbols)

    duration_seconds = None
    started_at = _parse_iso(run.get("started_at"))
    ended_at = _parse_iso(run.get("ended_at"))
    if started_at and ended_at:
        duration_seconds = int((ended_at - started_at).total_seconds())

    export_rows = {
        "run": _build_run_rows(run, duration_seconds),
        "instruments": _build_instrument_rows(instruments),
        "trade_events": _build_trade_event_rows(trade_events, instrument_by_symbol),
        "ledger_events": _build_ledger_rows(decision_ledger),
    }

    runtime_config = runtime_config_from_env()
    if not runtime_config.persistence.dsn:
        raise RuntimeError("PG_DSN is required for candle export.")

    engine = create_engine(runtime_config.persistence.dsn)
    repo = ReportExportRepository(
        engine,
        ExportTables(
            candles_raw=runtime_config.persistence.candles_raw_table,
            derivatives_state=runtime_config.persistence.derivatives_state_table,
            candle_stats=runtime_config.persistence.candle_stats_table,
            regime_stats=runtime_config.persistence.regime_stats_table,
        ),
    )
    timeframe = run.get("timeframe")
    if not timeframe:
        raise ValueError("Run timeframe is required for candle export.")

    timeframe_seconds = int(interval_to_timedelta(timeframe).total_seconds())
    candles_rows: List[Dict[str, Any]] = []
    derivatives_rows: List[Dict[str, Any]] = []
    for instrument in instruments:
        instrument_id = instrument.get("id")
        symbol = instrument.get("symbol")
        if not instrument_id:
            continue
        candles = repo.fetch_candles(instrument_id, timeframe_seconds, window_start, window_end)
        for row in candles:
            row["symbol"] = symbol
            row["timeframe"] = timeframe
        candles_rows.extend(candles)
        if instrument.get("has_funding"):
            derivatives = repo.fetch_derivatives_state(instrument_id, window_start, window_end)
            for row in derivatives:
                row["symbol"] = symbol
            derivatives_rows.extend(derivatives)

    stats_rows: List[Dict[str, Any]] = []
    regime_rows: List[Dict[str, Any]] = []
    for instrument in instruments:
        instrument_id = instrument.get("id")
        symbol = instrument.get("symbol")
        if not instrument_id:
            continue
        stats_rows.extend(
            _build_candle_stats_export_rows(
                repo.fetch_candle_stats(
                    instrument_id,
                    timeframe_seconds,
                    window_start,
                    window_end,
                    versions=stats_versions_param_list,
                ),
                symbol=str(symbol or ""),
                timeframe=timeframe,
                stats_key_limit=stats_key_limit,
            )
        )
        regime_rows.extend(
            _build_regime_stats_export_rows(
                repo.fetch_regime_stats(
                    instrument_id,
                    timeframe_seconds,
                    window_start,
                    window_end,
                    versions=regime_versions_param_list,
                ),
                symbol=str(symbol or ""),
                timeframe=timeframe,
            )
        )

    export_rows["candle_stats_flat"] = stats_rows
    export_rows["regime_stats_flat"] = regime_rows
    stats_lookup_version = stats_versions_param[0] if stats_versions_param else DEFAULT_STATS_VERSION
    regime_lookup_version = regime_versions_param[0] if regime_versions_param else DEFAULT_REGIME_VERSION
    stats_index = _build_time_index(stats_rows, "stats_version")
    regime_index = _build_time_index(regime_rows, "regime_version")
    trade_rows = _build_trade_rows(
        trades,
        instrument_by_symbol,
        timeframe_seconds,
        timeframe,
        stats_index,
        regime_index,
        stats_lookup_version,
        regime_lookup_version,
    )
    export_rows["trades"] = trade_rows
    if trade_rows:
        total_trades = len(trade_rows)
        fallback_rate = sum(1 for row in trade_rows if row.get("entry_fallback_used")) / total_trades
        regime_missing_rate = sum(1 for row in trade_rows if row.get("entry_regime_missing")) / total_trades
        log_context = {
            **context,
            "trade_count": total_trades,
            "stats_version": stats_lookup_version,
            "regime_version": regime_lookup_version,
            "fallback_rate": round(fallback_rate, 4),
            "regime_missing_rate": round(regime_missing_rate, 4),
        }
        logger.info(with_log_context("report_export_entry_context_stats", log_context))
    export_rows["decision_ledger"] = _build_decision_ledger_rows(decision_ledger, timeframe_seconds)
    export_rows["candles_raw"] = candles_rows
    export_rows["derivatives_state"] = derivatives_rows

    readme = _build_readme(
        run_id,
        start,
        end,
        pre_roll_hours,
        post_roll_hours,
        stats_versions_param,
        stats_key_limit,
    )

    filename = f"run_{run_id}_llm_export.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("README.md", readme)
        _write_csv(
            zip_file,
            "run.csv",
            export_rows["run"],
            derive_fieldnames(export_rows["run"], RUN_COLUMNS),
        )
        _write_csv(
            zip_file,
            "instruments.csv",
            export_rows["instruments"],
            derive_fieldnames(export_rows["instruments"], INSTRUMENT_COLUMNS),
        )
        _write_csv(
            zip_file,
            "trades.csv",
            export_rows["trades"],
            derive_fieldnames(export_rows["trades"], TRADE_COLUMNS_BASE),
        )
        _write_csv(
            zip_file,
            "trade_events.csv",
            export_rows["trade_events"],
            derive_fieldnames(export_rows["trade_events"], TRADE_EVENT_COLUMNS),
        )
        _write_csv(
            zip_file,
            "ledger_events.csv",
            export_rows["ledger_events"],
            derive_fieldnames(export_rows["ledger_events"], LEDGER_EVENT_COLUMNS),
        )
        _write_csv(
            zip_file,
            "candles_raw.csv",
            export_rows["candles_raw"],
            derive_fieldnames(export_rows["candles_raw"], RAW_CANDLES_COLUMNS),
        )
        _write_csv(
            zip_file,
            "derivatives_state.csv",
            export_rows["derivatives_state"],
            derive_fieldnames(export_rows["derivatives_state"], DERIVATIVES_COLUMNS),
        )
        _write_csv(
            zip_file,
            "candle_stats_flat.csv",
            export_rows["candle_stats_flat"],
            derive_fieldnames(export_rows["candle_stats_flat"], CANDLE_STATS_BASE_COLUMNS),
        )
        _write_csv(
            zip_file,
            "regime_stats_flat.csv",
            export_rows["regime_stats_flat"],
            derive_fieldnames(export_rows["regime_stats_flat"], REGIME_STATS_BASE_COLUMNS),
        )
        _write_csv(
            zip_file,
            "decision_ledger.csv",
            export_rows["decision_ledger"],
            derive_fieldnames(export_rows["decision_ledger"], DECISION_LEDGER_BASE_COLUMNS),
        )

    logger.info(with_log_context("report_export_success", context))
    return buffer.getvalue(), filename
