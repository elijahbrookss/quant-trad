"""Canonical reporting contract services."""

from __future__ import annotations

from collections import Counter, OrderedDict
from collections.abc import Mapping, Sequence
from concurrent.futures import Future
from datetime import datetime, timezone
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from data_providers.utils.ohlcv import interval_to_timedelta
from utils.log_context import build_log_context, with_log_context

import logging

from ..market.candle_service import fetch_ohlcv_by_instrument
from . import report_data
from .run_research_dataset import DATASET_SCHEMA_VERSION, build_run_research_dataset


logger = logging.getLogger(__name__)

_COMPARABLE_SUMMARY_METRICS = (
    "net_pnl",
    "gross_pnl",
    "fees",
    "return_pct",
    "max_drawdown_pct",
    "profit_factor",
    "expectancy",
    "win_rate",
    "closed_trades",
    "accepted_decisions",
    "rejected_decisions",
)
_DATASET_CACHE_TTL_SECONDS = 15.0
_DATASET_CACHE_MAX_ENTRIES = 32
_DATASET_CACHE: "OrderedDict[Tuple[str, int], Tuple[float, Dict[str, Any]]]" = OrderedDict()
_DATASET_INFLIGHT: Dict[Tuple[str, int], Future] = {}
_DATASET_CACHE_LOCK = threading.RLock()


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _clean_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _execution_mode_from_run(run: Mapping[str, Any]) -> str:
    config = _mapping(run.get("config_snapshot"))
    bot = _mapping(config.get("bot"))
    risk = _mapping(config.get("risk_settings")) or _mapping(bot.get("risk"))
    value = run.get("execution_mode") or config.get("execution_mode") or bot.get("execution_mode") or risk.get("execution_mode")
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"fast", "full"} else "fast"


def _dataset_cache_key(run_id: str) -> Tuple[str, int]:
    return (str(run_id), id(build_run_research_dataset))


def _cached_dataset_unlocked(key: Tuple[str, int]) -> Optional[Dict[str, Any]]:
    entry = _DATASET_CACHE.get(key)
    if not entry:
        return None
    stored_at, dataset = entry
    if time.monotonic() - stored_at > _DATASET_CACHE_TTL_SECONDS:
        _DATASET_CACHE.pop(key, None)
        return None
    _DATASET_CACHE.move_to_end(key)
    return dataset


def clear_report_dataset_cache(run_id: Optional[str] = None) -> None:
    """Clear the short-lived report dataset cache used to absorb request bursts."""

    with _DATASET_CACHE_LOCK:
        if run_id is None:
            _DATASET_CACHE.clear()
            _DATASET_INFLIGHT.clear()
            return
        run_text = str(run_id)
        for key in list(_DATASET_CACHE):
            if key[0] == run_text:
                _DATASET_CACHE.pop(key, None)


def _store_dataset_unlocked(key: Tuple[str, int], dataset: Dict[str, Any]) -> None:
    _DATASET_CACHE[key] = (time.monotonic(), dataset)
    _DATASET_CACHE.move_to_end(key)
    while len(_DATASET_CACHE) > _DATASET_CACHE_MAX_ENTRIES:
        _DATASET_CACHE.popitem(last=False)


def _dataset(run_id: str) -> Dict[str, Any]:
    key = _dataset_cache_key(run_id)
    with _DATASET_CACHE_LOCK:
        cached = _cached_dataset_unlocked(key)
        if cached is not None:
            logger.debug(with_log_context("report_dataset_cache_hit", build_log_context(run_id=run_id)))
            return cached
        future = _DATASET_INFLIGHT.get(key)
        if future is None:
            future = Future()
            _DATASET_INFLIGHT[key] = future
            should_build = True
        else:
            should_build = False

    if not should_build:
        logger.debug(with_log_context("report_dataset_inflight_wait", build_log_context(run_id=run_id)))
        return future.result()

    try:
        logger.debug(with_log_context("report_dataset_build_start", build_log_context(run_id=run_id)))
        dataset = build_run_research_dataset(run_id)
    except BaseException as exc:
        with _DATASET_CACHE_LOCK:
            _DATASET_INFLIGHT.pop(key, None)
            future.set_exception(exc)
        raise

    with _DATASET_CACHE_LOCK:
        _store_dataset_unlocked(key, dataset)
        _DATASET_INFLIGHT.pop(key, None)
        future.set_result(dataset)
    logger.debug(with_log_context("report_dataset_build_done", build_log_context(run_id=run_id)))
    return dataset


def list_report_summaries(
    *,
    run_type: str = "backtest",
    status: str = "completed",
    limit: int = 50,
    offset: int = 0,
    search: Optional[str] = None,
    bot_id: Optional[str] = None,
    instrument: Optional[str] = None,
    timeframe: Optional[str] = None,
    started_after: Optional[str] = None,
    started_before: Optional[str] = None,
) -> Dict[str, Any]:
    """Return lightweight report catalog rows from durable run metadata."""

    context = build_log_context(
        run_type=run_type,
        status=status,
        limit=limit,
        offset=offset,
        bot_id=bot_id,
        instrument=instrument,
        timeframe=timeframe,
        search=search,
        start=started_after,
        end=started_before,
    )
    logger.debug(with_log_context("report_catalog_list_start", context))
    runs = report_data.list_runs(
        run_type=run_type,
        status=status,
        bot_id=bot_id,
        timeframe=timeframe,
        started_after=started_after,
        started_before=started_before,
    )
    search_key = str(search or "").strip().lower()
    instrument_key = str(instrument or "").strip().upper()
    filtered = []
    for run in runs:
        symbols = [str(symbol) for symbol in (run.get("symbols") or [])]
        if instrument_key and instrument_key not in {symbol.upper() for symbol in symbols}:
            continue
        if search_key:
            haystack = " ".join(
                [
                    str(run.get("run_id") or ""),
                    str(run.get("bot_name") or ""),
                    str(run.get("strategy_name") or ""),
                    " ".join(symbols),
                ]
            ).lower()
            if search_key not in haystack:
                continue
        filtered.append(run)

    filtered.sort(key=lambda entry: entry.get("ended_at") or "", reverse=True)
    total = len(filtered)
    sliced = filtered[offset : offset + limit] if limit else filtered[offset:]

    items: List[Dict[str, Any]] = []
    for run in sliced:
        run_id = str(run.get("run_id") or "")
        summary = _mapping(run.get("summary"))
        readiness = report_data.get_result_readiness(
            run_id,
            financial_summary=summary or None,
        )
        items.append(
            {
                "schema_version": "run_report_summary_item.v1",
                "run_id": run_id,
                "bot_id": run.get("bot_id"),
                "bot_name": run.get("bot_name"),
                "strategy_id": run.get("strategy_id"),
                "strategy_name": run.get("strategy_name"),
                "symbols": symbols,
                "timeframe": run.get("timeframe"),
                "execution_mode": _execution_mode_from_run(run),
                "simulated_window": {
                    "start": run.get("backtest_start"),
                    "end": run.get("backtest_end"),
                },
                "wall_clock_window": {
                    "start": run.get("started_at"),
                    "end": run.get("ended_at"),
                },
                "status": run.get("status"),
                "completed_at": run.get("ended_at"),
                "summary": {
                    "net_pnl": summary.get("net_pnl"),
                    "total_return": summary.get("total_return"),
                    "max_drawdown_pct": summary.get("max_drawdown_pct"),
                    "sharpe": summary.get("sharpe"),
                    "total_trades": summary.get("total_trades"),
                },
                "readiness": {
                    "dataset_ready": bool(readiness.get("dataset_ready")),
                    "results_ready": bool(readiness.get("results_ready")),
                    "safe_to_compare": bool(readiness.get("safe_to_compare")),
                    "reason": readiness.get("reason"),
                    "dataset_status": readiness.get("dataset_status"),
                },
            }
        )
    logger.debug(with_log_context("report_catalog_list_done", context | {"items": len(items), "total": total}))
    return {"schema_version": "report_list.v1", "items": items, "total": total, "limit": limit, "offset": offset}


def get_run_research_dataset(run_id: str) -> Dict[str, Any]:
    return _dataset(run_id)


def get_report_readiness(run_id: str) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    readiness = _mapping(dataset.get("readiness"))
    return {
        "schema_version": "report_readiness.v1",
        "run_id": run_id,
        **readiness,
        "diagnostics": dataset.get("diagnostics") or {},
    }


def get_run_report_summary(run_id: str) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    return {
        "schema_version": "run_report_summary.v1",
        "run_id": run_id,
        "metadata": dataset.get("metadata") or {},
        "readiness": dataset.get("readiness") or {},
        "summary": dataset.get("summary") or {},
        "portfolio_metrics": dataset.get("portfolio_metrics") or {},
        "sections": dataset.get("sections") or {},
    }


def get_report_sections(run_id: str) -> Dict[str, Any]:
    return _mapping(_dataset(run_id).get("sections"))


def get_report_diagnostics(run_id: str) -> Dict[str, Any]:
    return _mapping(_dataset(run_id).get("diagnostics"))


def get_report_metrics(run_id: str) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    return {
        "schema_version": "report_metrics.v1",
        "run_id": run_id,
        "summary": dataset.get("summary") or {},
        "fee_accounting": dataset.get("fee_accounting") or {},
        "wallet_accounting": dataset.get("wallet_accounting") or {},
        "execution": dataset.get("execution") or {},
        "data_quality": dataset.get("candle_gaps") or {},
        "portfolio_metrics": dataset.get("portfolio_metrics") or {},
        "performance": dataset.get("performance") or {},
        "operational_health": dataset.get("operational_health") or {},
        "strategy_insights": dataset.get("strategy_insights") or {},
    }


def get_operational_health(run_id: str) -> Dict[str, Any]:
    return _mapping(_dataset(run_id).get("operational_health")) | {"run_id": run_id}


def _page(
    *,
    run_id: str,
    section: str,
    rows: Sequence[Mapping[str, Any]],
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    max_limit = max(1, min(int(limit or 100), 1000))
    start = max(0, int(offset or 0))
    clean_rows = [dict(row) for row in rows]
    return {
        "schema_version": f"{section}_dataset.v1",
        "run_id": run_id,
        "section": section,
        "limit": max_limit,
        "offset": start,
        "total": len(clean_rows),
        "items": clean_rows[start : start + max_limit],
    }


def get_trade_dataset(
    run_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    symbol: Optional[str] = None,
    instrument_id: Optional[str] = None,
) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    rows = [dict(row) for row in dataset.get("trades") or []]
    if symbol:
        rows = [row for row in rows if str(row.get("symbol") or "") == symbol]
    if instrument_id:
        rows = [row for row in rows if str(row.get("instrument_id") or "") == instrument_id]
    rows.sort(key=lambda row: (str(row.get("exit_time") or row.get("entry_time") or ""), str(row.get("trade_id") or "")))
    return _page(run_id=run_id, section="trades", rows=rows, limit=limit, offset=offset)


def get_decision_dataset(
    run_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    state: Optional[str] = None,
    symbol: Optional[str] = None,
    instrument_id: Optional[str] = None,
) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    rows = [dict(row) for row in dataset.get("decisions") or []]
    normalized_state = str(state or "").strip().lower()
    if normalized_state in {"accepted", "rejected"}:
        rows = [row for row in rows if bool(row.get(normalized_state))]
    if symbol:
        rows = [row for row in rows if str(row.get("symbol") or "") == symbol]
    if instrument_id:
        rows = [row for row in rows if str(row.get("instrument_id") or "") == instrument_id]
    rows.sort(key=lambda row: (str(row.get("bar_time") or ""), str(row.get("decision_id") or "")))
    return _page(run_id=run_id, section="decisions", rows=rows, limit=limit, offset=offset)


def get_signal_dataset(
    run_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    symbol: Optional[str] = None,
    instrument_id: Optional[str] = None,
) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    rows = [dict(row) for row in dataset.get("signals") or []]
    if symbol:
        rows = [row for row in rows if str(row.get("symbol") or "") == symbol]
    if instrument_id:
        rows = [row for row in rows if str(row.get("instrument_id") or "") == instrument_id]
    rows.sort(key=lambda row: (str(row.get("bar_time") or row.get("known_at") or ""), str(row.get("signal_id") or "")))
    return _page(run_id=run_id, section="signals", rows=rows, limit=limit, offset=offset)


def get_timeseries_dataset(
    run_id: str,
    section: str,
    *,
    limit: int = 1000,
    offset: int = 0,
) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    timeseries = _mapping(dataset.get("timeseries"))
    section_key = str(section or "").strip()
    payload = _mapping(_mapping(timeseries.get("items")).get(section_key))
    rows = [dict(row) for row in payload.get("items") or [] if isinstance(row, Mapping)]
    return _page(run_id=run_id, section=f"timeseries.{section_key}", rows=rows, limit=limit, offset=offset) | {
        "availability": {
            "available": bool(payload.get("available")),
            "reason": payload.get("reason"),
            "schema_version": payload.get("schema_version"),
        }
    }


def get_context_dataset(
    run_id: str,
    *,
    section: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    context = _mapping(dataset.get("context"))
    section_key = str(section or "decision_context").strip()
    payload = _mapping(context.get(section_key))
    rows = [dict(row) for row in payload.get("items") or [] if isinstance(row, Mapping)]
    return _page(run_id=run_id, section=f"context.{section_key}", rows=rows, limit=limit, offset=offset) | {
        "availability": {
            "available": bool(payload.get("available")),
            "reason": payload.get("reason"),
            "schema_version": payload.get("schema_version"),
        }
    }


def get_candle_catalog(run_id: str) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    catalog = _mapping(dataset.get("candle_catalog"))
    return {
        "schema_version": "candle_catalog.v1",
        "run_id": run_id,
        "items": [dict(row) for row in catalog.get("items") or [] if isinstance(row, Mapping)],
        "caveats": list(catalog.get("caveats") or []),
    }


def _parse_time(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _candle_rows_from_frame(
    *,
    run_id: str,
    instrument_id: str,
    symbol: Optional[str],
    timeframe: str,
    frame: Any,
) -> List[Dict[str, Any]]:
    if frame is None or getattr(frame, "empty", True):
        return []
    rows: List[Dict[str, Any]] = []
    for index, row in frame.iterrows():
        source_ts = row.get("timestamp") if "timestamp" in row else index
        timestamp = pd.to_datetime(source_ts, utc=True, errors="coerce")
        if pd.isna(timestamp):
            timestamp = pd.to_datetime(row.get("timestamp"), utc=True, errors="coerce")
        if pd.isna(timestamp):
            continue
        rows.append(
            {
                "run_id": run_id,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
                "open": _safe_float(row.get("open")),
                "high": _safe_float(row.get("high")),
                "low": _safe_float(row.get("low")),
                "close": _safe_float(row.get("close")),
                "volume": _safe_float(row.get("volume")),
                "source": "reporting_candle_service",
            }
        )
    return rows


def get_candle_dataset(
    run_id: str,
    *,
    instrument_id: str,
    timeframe: str,
    start: str,
    end: str,
    limit: int = 1000,
    offset: int = 0,
) -> Dict[str, Any]:
    catalog = get_candle_catalog(run_id)
    instrument_id = str(instrument_id or "").strip()
    timeframe = str(timeframe or "").strip()
    start_dt = _parse_time(start)
    end_dt = _parse_time(end)
    caveats: List[str] = []
    if not instrument_id:
        raise ValueError("instrument_id is required")
    if not timeframe:
        raise ValueError("timeframe is required")
    if not start_dt or not end_dt or end_dt <= start_dt:
        raise ValueError("valid start and end timestamps are required")
    catalog_row = next(
        (
            row
            for row in catalog.get("items") or []
            if str(row.get("instrument_id") or "") == instrument_id
            and str(row.get("timeframe") or "") == timeframe
        ),
        {},
    )
    try:
        frame = fetch_ohlcv_by_instrument(instrument_id, _iso(start_dt), _iso(end_dt), timeframe)
        rows = _candle_rows_from_frame(
            run_id=run_id,
            instrument_id=instrument_id,
            symbol=catalog_row.get("symbol"),
            timeframe=timeframe,
            frame=frame,
        )
    except Exception as exc:  # noqa: BLE001 - candle availability is reported as a section caveat.
        rows = []
        caveats.append(f"candle_fetch_unavailable:{type(exc).__name__}")
    page = _page(run_id=run_id, section="candles", rows=rows, limit=limit, offset=offset)
    return page | {
        "schema_version": "report_candles.v1",
        "window": {
            "instrument_id": instrument_id,
            "symbol": catalog_row.get("symbol"),
            "timeframe": timeframe,
            "start": _iso(start_dt),
            "end": _iso(end_dt),
        },
        "caveats": caveats,
    }


def _anchor_window(
    *,
    run_id: str,
    row: Mapping[str, Any],
    anchor_time: Any,
    anchor_type: str,
    before: int,
    after: int,
) -> Dict[str, Any]:
    instrument_id = str(row.get("instrument_id") or "").strip()
    timeframe = str(row.get("timeframe") or "").strip()
    anchor_dt = _parse_time(anchor_time)
    if not instrument_id:
        raise ValueError(f"instrument_id unavailable for {anchor_type}")
    if not timeframe:
        raise ValueError(f"timeframe unavailable for {anchor_type}")
    if anchor_dt is None:
        raise ValueError(f"anchor timestamp unavailable for {anchor_type}")
    step = interval_to_timedelta(timeframe)
    start_dt = anchor_dt - (step * max(int(before or 0), 0))
    end_dt = anchor_dt + (step * max(int(after or 0), 0))
    payload = get_candle_dataset(
        run_id,
        instrument_id=instrument_id,
        timeframe=timeframe,
        start=_iso(start_dt),
        end=_iso(end_dt),
        limit=max(1, int(before or 0) + int(after or 0) + 1),
        offset=0,
    )
    payload["window"] = dict(payload.get("window") or {}) | {
        "anchor_type": anchor_type,
        "anchor_time": _iso(anchor_dt),
        "before": int(before or 0),
        "after": int(after or 0),
    }
    return payload


def get_trade_candle_window(
    run_id: str,
    trade_id: str,
    *,
    anchor: str = "entry",
    before: int = 20,
    after: int = 20,
) -> Dict[str, Any]:
    rows = [dict(row) for row in _dataset(run_id).get("trades") or []]
    trade = next((row for row in rows if str(row.get("trade_id") or "") == str(trade_id)), None)
    if not trade:
        raise KeyError(f"Trade {trade_id} was not found for run {run_id}")
    anchor_key = "exit_time" if str(anchor or "").strip().lower() == "exit" else "entry_time"
    return _anchor_window(run_id=run_id, row=trade, anchor_time=trade.get(anchor_key), anchor_type=f"trade_{anchor_key}", before=before, after=after)


def get_decision_candle_window(
    run_id: str,
    decision_id: str,
    *,
    before: int = 20,
    after: int = 20,
) -> Dict[str, Any]:
    rows = [dict(row) for row in _dataset(run_id).get("decisions") or []]
    decision = next((row for row in rows if str(row.get("decision_id") or "") == str(decision_id)), None)
    if not decision:
        raise KeyError(f"Decision {decision_id} was not found for run {run_id}")
    return _anchor_window(run_id=run_id, row=decision, anchor_time=decision.get("bar_time") or decision.get("known_at"), anchor_type="decision", before=before, after=after)


def get_signal_candle_window(
    run_id: str,
    signal_id: str,
    *,
    before: int = 20,
    after: int = 20,
) -> Dict[str, Any]:
    rows = [dict(row) for row in _dataset(run_id).get("signals") or []]
    signal = next((row for row in rows if str(row.get("signal_id") or "") == str(signal_id)), None)
    if not signal:
        raise KeyError(f"Signal {signal_id} was not found for run {run_id}")
    return _anchor_window(run_id=run_id, row=signal, anchor_time=signal.get("bar_time") or signal.get("known_at"), anchor_type="signal", before=before, after=after)


def get_metric_explanation(run_id: str, metric_name: str) -> Dict[str, Any]:
    dataset = _dataset(run_id)
    summary = _mapping(dataset.get("summary"))
    portfolio_metrics = _mapping(dataset.get("portfolio_metrics"))
    metric = str(metric_name or "").strip()
    formulas: Dict[str, Dict[str, Any]] = {
        "net_pnl": {
            "unit": "currency",
            "formula": "sum(closed_trade.net_pnl)",
            "source_sections": ["trades"],
        },
        "gross_pnl": {
            "unit": "currency",
            "formula": "sum(closed_trade.gross_pnl)",
            "source_sections": ["trades"],
        },
        "fees": {
            "unit": "currency",
            "formula": "sum(closed_trade.fees_paid)",
            "source_sections": ["trades", "fee_accounting"],
        },
        "return_pct": {
            "unit": "ratio",
            "formula": "net_pnl / equity_start",
            "source_sections": ["summary", "trades"],
        },
        "win_rate": {
            "unit": "ratio",
            "formula": "winning_closed_trades / closed_trades",
            "source_sections": ["trades"],
        },
        "expectancy": {
            "unit": "currency",
            "formula": "mean(closed_trade.net_pnl)",
            "source_sections": ["trades"],
        },
        "profit_factor": {
            "unit": "ratio",
            "formula": "gross_profit / abs(gross_loss)",
            "source_sections": ["trades"],
        },
        "max_drawdown_pct": {
            "unit": "ratio",
            "formula": "max peak-to-trough decline over equity series",
            "source_sections": ["trades", "summary", "portfolio_metrics"],
        },
        "sharpe": {
            "unit": "ratio",
            "formula": "mean(daily_returns - risk_free_rate) / population_stddev(daily_returns - risk_free_rate) * sqrt(annualization_periods)",
            "source_sections": ["trades", "portfolio_metrics"],
        },
        "sortino": {
            "unit": "ratio",
            "formula": "mean(daily_returns - risk_free_rate) / population_stddev(downside_returns) * sqrt(annualization_periods)",
            "source_sections": ["trades", "portfolio_metrics"],
        },
        "calmar": {
            "unit": "ratio",
            "formula": "cagr / max_drawdown_pct",
            "source_sections": ["trades", "portfolio_metrics"],
        },
        "annualized_volatility": {
            "unit": "ratio",
            "formula": "population_stddev(daily_returns) * sqrt(annualization_periods)",
            "source_sections": ["trades", "portfolio_metrics"],
        },
        "exposure_pct": {
            "unit": "ratio",
            "formula": "merged_trade_holding_time / simulated_window_duration",
            "source_sections": ["trades", "portfolio_metrics"],
        },
    }
    spec = formulas.get(metric)
    value = summary.get(metric)
    if value is None:
        value = portfolio_metrics.get(metric)
    availability = "available" if spec and value is not None else "unsupported" if not spec else "unavailable"
    source_refs = []
    if metric in {
        "net_pnl",
        "gross_pnl",
        "fees",
        "win_rate",
        "expectancy",
        "profit_factor",
        "max_drawdown_pct",
        "sharpe",
        "sortino",
        "calmar",
        "annualized_volatility",
        "exposure_pct",
    }:
        source_refs = [
            {"section": "trades", "trade_id": row.get("trade_id")}
            for row in (dataset.get("trades") or [])[:25]
            if row.get("trade_id")
        ]
    return {
        "schema_version": "metric_explanation.v1",
        "run_id": run_id,
        "metric_name": metric,
        "value": value,
        "unit": spec.get("unit") if spec else None,
        "formula": {"version": "v1", "description": spec.get("formula") if spec else None},
        "source_sections": list(spec.get("source_sections") or []) if spec else [],
        "source_refs": source_refs,
        "availability": availability,
        "caveats": list(_mapping(dataset.get("readiness")).get("caveats") or []),
    }


def _compatibility(datasets: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    metadata = [_mapping(dataset.get("metadata")) for dataset in datasets]
    schema_versions = {str(dataset.get("schema_version") or "") for dataset in datasets}
    strategy_ids = {str(item.get("strategy_id") or "") for item in metadata}
    strategy_hashes = {str(item.get("strategy_hash") or item.get("material_config_hash") or item.get("config_hash") or "") for item in metadata}
    timeframes = {tuple(item.get("timeframes") or ([item.get("timeframe")] if item.get("timeframe") else [])) for item in metadata}
    symbol_sets = {tuple(sorted(str(symbol) for symbol in item.get("symbols") or [])) for item in metadata}
    instrument_sets = {tuple(sorted(str(value) for value in item.get("instrument_ids") or [])) for item in metadata}
    readiness_rows = [_mapping(dataset.get("readiness")) for dataset in datasets]
    required_metrics = {"net_pnl", "gross_pnl", "fees", "return_pct", "win_rate", "profit_factor", "expectancy", "max_drawdown_pct"}
    metric_sets = []
    for dataset in datasets:
        summary = _mapping(dataset.get("summary"))
        metric_sets.append({metric for metric in required_metrics if summary.get(metric) is not None})
    return {
        "dataset_schema_version_match": len(schema_versions) == 1,
        "strategy_id_match": len(strategy_ids) == 1,
        "strategy_hash_match": len(strategy_hashes - {""}) <= 1,
        "timeframe_match": len(timeframes) == 1,
        "symbols_match": len(symbol_sets) == 1,
        "instrument_ids_match": len(instrument_sets - {()}) <= 1,
        "dataset_ready": all(row.get("dataset_status") == "ready" for row in readiness_rows),
        "results_ready": all(row.get("results_status") == "ready" for row in readiness_rows),
        "metrics_available": all(required_metrics.issubset(metric_set) for metric_set in metric_sets),
        "data_quality_not_blocked": all(str(row.get("data_quality_status") or "") != "blocked" for row in readiness_rows),
        "execution_quality_not_blocked": all(str(row.get("execution_quality_status") or "") != "blocked" for row in readiness_rows),
        "comparison_not_blocked": all(str(row.get("comparison_status") or "") != "blocked" for row in readiness_rows),
    }


def _blocked_reasons(datasets: Sequence[Mapping[str, Any]], compatibility: Mapping[str, Any]) -> List[Dict[str, Any]]:
    reasons: List[Dict[str, Any]] = []
    for dataset in datasets:
        metadata = _mapping(dataset.get("metadata"))
        readiness = _mapping(dataset.get("readiness"))
        if readiness.get("comparison_status") == "blocked" or not readiness.get("safe_to_compare"):
            reasons.append(
                {
                    "run_id": metadata.get("run_id"),
                    "code": readiness.get("reason") or "not_safe_to_compare",
                    "message": "Run is not safe to compare.",
                }
            )
    for key, passed in compatibility.items():
        if not passed:
            reasons.append({"code": key, "message": f"Compatibility check failed: {key}."})
    return reasons


def _summary_delta(base: Mapping[str, Any], compare: Mapping[str, Any]) -> Dict[str, Any]:
    delta: Dict[str, Any] = {}
    for metric in _COMPARABLE_SUMMARY_METRICS:
        base_value = _safe_float(base.get(metric))
        compare_value = _safe_float(compare.get(metric))
        delta[metric] = compare_value - base_value if base_value is not None and compare_value is not None else None
    return delta


def compare_run_datasets(run_ids: Sequence[str]) -> Dict[str, Any]:
    if not run_ids or len(run_ids) < 2:
        raise ValueError("At least two run ids are required for comparison")

    ordered_ids = [str(run_id) for run_id in run_ids]
    context = build_log_context(run_ids=ordered_ids, runs=len(ordered_ids))
    logger.info(with_log_context("run_dataset_compare_start", context))
    datasets = [_dataset(run_id) for run_id in ordered_ids]
    compatibility = _compatibility(datasets)
    blocked = _blocked_reasons(datasets, compatibility)
    if blocked:
        logger.info(with_log_context("run_dataset_compare_blocked", context | {"blocked": len(blocked)}))
        return {
            "schema_version": "run_comparison_result.v1",
            "status": "blocked",
            "run_ids": ordered_ids,
            "baseline_run_id": ordered_ids[0],
            "dataset_schema_version": DATASET_SCHEMA_VERSION,
            "readiness": {
                str(_mapping(dataset.get("metadata")).get("run_id")): dataset.get("readiness") or {}
                for dataset in datasets
            },
            "compatibility": dict(compatibility),
            "blocked_reasons": blocked,
            "reports": [],
            "comparisons": [],
        }

    baseline = datasets[0]
    baseline_summary = _mapping(baseline.get("summary"))
    reports = [get_run_report_summary(str(_mapping(dataset.get("metadata")).get("run_id"))) for dataset in datasets]
    comparisons = []
    comparison_statuses = {
        str(_mapping(dataset.get("metadata")).get("run_id")): _mapping(dataset.get("readiness")).get("comparison_status")
        for dataset in datasets
    }
    result_status = "ready_with_caveats" if any(status == "ready_with_caveats" for status in comparison_statuses.values()) else "ready"
    for dataset in datasets[1:]:
        metadata = _mapping(dataset.get("metadata"))
        compare_summary = _mapping(dataset.get("summary"))
        comparisons.append(
            {
                "base_run_id": ordered_ids[0],
                "compare_run_id": metadata.get("run_id"),
                "summary_delta": _summary_delta(baseline_summary, compare_summary),
            }
        )
    logger.info(with_log_context("run_dataset_compare_ready", context))
    return {
        "schema_version": "run_comparison_result.v1",
        "status": result_status,
        "run_ids": ordered_ids,
        "baseline_run_id": ordered_ids[0],
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "readiness": {
            str(_mapping(dataset.get("metadata")).get("run_id")): dataset.get("readiness") or {}
            for dataset in datasets
        },
        "compatibility": dict(compatibility),
        "blocked_reasons": [],
        "reports": reports,
        "comparisons": comparisons,
    }


def diagnostic_counts(diagnostics: Mapping[str, Any]) -> Dict[str, int]:
    items = diagnostics.get("items") if isinstance(diagnostics, Mapping) else []
    return dict(Counter(str(item.get("severity") or "unknown") for item in items if isinstance(item, Mapping)))
