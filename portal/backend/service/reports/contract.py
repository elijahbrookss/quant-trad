"""Canonical reporting contract services."""

from __future__ import annotations

from collections import Counter, OrderedDict
from collections.abc import Mapping, Sequence
from concurrent.futures import Future
from datetime import datetime, timezone
import statistics
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


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return int(number)


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
        report_materialization = report_data.get_report_materialization_status(run_id)
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
                "report_materialization": report_materialization,
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


def _metric_subset(summary: Mapping[str, Any], portfolio: Mapping[str, Any]) -> Dict[str, Any]:
    keys = (
        "net_pnl",
        "gross_pnl",
        "fees",
        "return_pct",
        "total_return_pct",
        "max_drawdown",
        "max_drawdown_pct",
        "profit_factor",
        "expectancy",
        "win_rate",
        "trades",
        "closed_trades",
        "total_trades",
        "accepted_decisions",
        "rejected_decisions",
        "exposure_pct",
        "time_in_market_pct",
        "average_holding_seconds",
        "sharpe",
        "sortino",
        "calmar",
    )
    payload: Dict[str, Any] = {}
    for key in keys:
        value = summary.get(key)
        if value is None:
            value = portfolio.get(key)
        if value is not None:
            payload[key] = value
    return payload


def _section_counts(sections: Mapping[str, Any]) -> List[Dict[str, Any]]:
    items = sections.get("items")
    if not isinstance(items, list):
        return []
    result: List[Dict[str, Any]] = []
    for item in items:
        row = _mapping(item)
        if not row:
            continue
        result.append(
            {
                "name": row.get("name") or row.get("section"),
                "status": row.get("status"),
                "available": row.get("available"),
                "row_count": row.get("row_count"),
            }
        )
    return result


def _strategy_snapshot_summary(dataset: Mapping[str, Any]) -> Dict[str, Any]:
    metadata = _mapping(dataset.get("metadata"))
    config = _mapping(metadata.get("configuration")) or _mapping(metadata.get("config_snapshot"))
    run_strategy_snapshot = _mapping(config.get("run_strategy_snapshot"))
    if not run_strategy_snapshot:
        run_strategy_snapshot = _mapping(metadata.get("run_strategy_snapshot"))
    return {
        "strategy_id": metadata.get("strategy_id") or run_strategy_snapshot.get("strategy_id"),
        "strategy_name": metadata.get("strategy_name") or run_strategy_snapshot.get("strategy_name"),
        "strategy_hash": metadata.get("strategy_hash") or run_strategy_snapshot.get("strategy_hash"),
        "strategy_variant_id": metadata.get("strategy_variant_id") or run_strategy_snapshot.get("strategy_variant_id"),
        "strategy_variant_name": metadata.get("strategy_variant_name") or run_strategy_snapshot.get("strategy_variant_name"),
        "effective_strategy_config_hash": run_strategy_snapshot.get("effective_strategy_config_hash"),
        "effective_params": run_strategy_snapshot.get("effective_params"),
        "variant_overrides": run_strategy_snapshot.get("variant_overrides"),
        "param_source_map": run_strategy_snapshot.get("param_source_map"),
    }


def get_run_research_summary(run_id: str) -> Dict[str, Any]:
    """Return a compact research summary for CLI/agent workflows.

    This is intentionally narrower than ``run_report_summary.v1`` so command
    flows do not need to transfer full metadata/configuration payloads.
    """

    dataset = _dataset(run_id)
    metadata = _mapping(dataset.get("metadata"))
    readiness = _mapping(dataset.get("readiness"))
    summary = _mapping(dataset.get("summary"))
    portfolio = _mapping(dataset.get("portfolio_metrics"))
    sections = _mapping(dataset.get("sections"))
    simulated_window = _mapping(metadata.get("simulated_window"))
    return {
        "schema_version": "run_research_summary.v1",
        "run_id": run_id,
        "status": metadata.get("status") or summary.get("run_status"),
        "bot_id": metadata.get("bot_id"),
        "run_type": metadata.get("run_type"),
        "execution_mode": metadata.get("execution_mode"),
        "symbols": list(metadata.get("symbols") or []),
        "timeframe": metadata.get("timeframe"),
        "window": {
            "start": simulated_window.get("start") or metadata.get("backtest_start"),
            "end": simulated_window.get("end") or metadata.get("backtest_end"),
        },
        "strategy": _strategy_snapshot_summary(dataset),
        "readiness": {
            "dataset_ready": readiness.get("dataset_ready"),
            "results_ready": readiness.get("results_ready"),
            "safe_to_compare": readiness.get("safe_to_compare"),
            "reason": readiness.get("reason"),
            "dataset_status": readiness.get("dataset_status"),
            "results_status": readiness.get("results_status"),
            "comparison_status": readiness.get("comparison_status"),
            "export_status": readiness.get("export_status"),
            "golden_candidate_status": readiness.get("golden_candidate_status"),
            "degraded_sections": list(readiness.get("degraded_sections") or []),
            "unavailable_sections": list(readiness.get("unavailable_sections") or []),
            "caveats": list(readiness.get("caveats") or []),
        },
        "metrics": _metric_subset(summary, portfolio),
        "sections": _section_counts(sections),
    }


def _metric_value(
    *,
    value: Any = None,
    valid: Optional[bool] = None,
    unit: Optional[str] = None,
    method: Optional[str] = None,
    source: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
    sample_count: Optional[int] = None,
    minimum_sample_count: Optional[int] = None,
    invalid_reason: Optional[str] = None,
    caveats: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    resolved_valid = bool(valid) if valid is not None else value is not None
    if resolved_valid and value is None:
        resolved_valid = False
    reason = invalid_reason
    if not resolved_valid and not reason:
        reason = "not_available" if value is None else "invalid"
    return {
        "value": value,
        "valid": resolved_valid,
        "unit": unit,
        "method": method,
        "source": source,
        "metadata": dict(metadata or {}),
        "sample_count": sample_count,
        "minimum_sample_count": minimum_sample_count,
        "invalid_reason": None if resolved_valid else reason,
        "caveats": list(dict.fromkeys(str(item) for item in (caveats or []) if str(item or "").strip())),
    }


def _unavailable_metric(reason: str = "not_available", *, unit: Optional[str] = None, method: Optional[str] = None) -> Dict[str, Any]:
    return _metric_value(value=None, valid=False, unit=unit, method=method, invalid_reason=reason)


def _series_count(dataset: Mapping[str, Any], key: str) -> Optional[int]:
    payload = _mapping(_mapping(_mapping(dataset.get("timeseries")).get("items")).get(key))
    count = _safe_int(payload.get("row_count"))
    if count is not None:
        return count
    items = payload.get("items")
    return len(items) if isinstance(items, list) else None


def _simulated_window_days(metadata: Mapping[str, Any]) -> Optional[float]:
    window = _mapping(metadata.get("simulated_window"))
    start = _parse_time(window.get("start"))
    end = _parse_time(window.get("end"))
    if start is None or end is None or end <= start:
        return None
    return (end - start).total_seconds() / 86400.0


def _summary_number(summary: Mapping[str, Any], portfolio: Mapping[str, Any], key: str) -> Any:
    value = summary.get(key)
    return portfolio.get(key) if value is None else value


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _performance_metrics(dataset: Mapping[str, Any]) -> Dict[str, Any]:
    metadata = _mapping(dataset.get("metadata"))
    summary = _mapping(dataset.get("summary"))
    portfolio = _mapping(dataset.get("portfolio_metrics"))
    execution = _mapping(dataset.get("execution"))
    slippage_accounting = _mapping(execution.get("slippage"))
    closed_trades = _safe_int(_first_present(summary.get("closed_trades"), summary.get("trades"))) or 0
    wins = _safe_int(summary.get("wins")) or 0
    losses = _safe_int(summary.get("losses")) or 0
    breakeven = max(closed_trades - wins - losses, 0)
    duration_days = _simulated_window_days(metadata)
    returns_count = _series_count(dataset, "returns_series")
    equity_count = _series_count(dataset, "equity_curve")
    portfolio_caveats = [str(item) for item in portfolio.get("caveats") or []]
    unavailable = _mapping(summary.get("unavailable_metrics"))
    avg_win = _safe_float(summary.get("avg_win"))
    avg_loss = _safe_float(summary.get("avg_loss"))
    avg_win_loss_ratio = abs(avg_win / avg_loss) if avg_win is not None and avg_loss not in (None, 0.0) else None
    cagr = _summary_number(summary, portfolio, "cagr")
    drawdown = _safe_float(summary.get("max_drawdown"))
    drawdown_pct = _safe_float(summary.get("max_drawdown_pct"))
    profit_factor = _summary_number(summary, portfolio, "profit_factor")
    exposure_pct = _summary_number(summary, portfolio, "exposure_pct")
    average_holding = _safe_float(summary.get("average_holding_seconds"))
    annualization_factor = _safe_int(portfolio.get("annualization_periods"))
    basis = _mapping(portfolio.get("basis"))
    risk_free_rate = _safe_float(basis.get("risk_free_rate"))
    ratio_caveats = [item for item in portfolio_caveats if item.endswith("_unavailable") or "risk_metrics" in item]

    sharpe = _summary_number(summary, portfolio, "sharpe")
    sortino = _summary_number(summary, portfolio, "sortino")
    calmar = _summary_number(summary, portfolio, "calmar")
    sharpe_valid = sharpe is not None and (returns_count or 0) >= 2
    sortino_valid = sortino is not None and (returns_count or 0) >= 2
    calmar_valid = calmar is not None and cagr is not None and drawdown_pct not in (None, 0.0)
    cagr_valid = cagr is not None and duration_days is not None and duration_days >= 90.0
    drawdown_valid = (drawdown is not None or drawdown_pct is not None) and (equity_count is None or equity_count >= 1)
    profit_factor_valid = profit_factor is not None and wins > 0 and losses > 0
    exposure_valid = exposure_pct is not None and duration_days is not None and any(
        str(row.get("entry_time") or "").strip() and str(row.get("exit_time") or row.get("closed_at") or "").strip()
        for row in dataset.get("trades") or []
        if isinstance(row, Mapping)
    )
    slippage_invalid_reason = (
        "requires_execution_slippage_facts_or_configured_zero_slippage_model"
        if slippage_accounting
        else "not_modeled"
    )

    return {
        "net_pnl": _metric_value(
            value=summary.get("net_pnl"),
            valid=summary.get("net_pnl") is not None,
            unit="currency",
            method="sum_closed_trade_net_pnl",
            source="RunResearchDataset.summary",
        ),
        "gross_pnl": _metric_value(
            value=summary.get("gross_pnl"),
            valid=summary.get("gross_pnl") is not None,
            unit="currency",
            method="sum_closed_trade_gross_pnl",
            source="RunResearchDataset.summary",
        ),
        "realized_pnl": _metric_value(
            value=summary.get("net_pnl"),
            valid=summary.get("net_pnl") is not None,
            unit="currency",
            method="closed_trade_realized_net_pnl",
            source="RunResearchDataset.summary",
        ),
        "unrealized_pnl": _unavailable_metric("not_modeled", unit="currency", method="requires_open_position_mark_to_market"),
        "total_return_pct": _metric_value(
            value=summary.get("return_pct"),
            valid=summary.get("return_pct") is not None,
            unit="ratio",
            method="net_pnl_over_starting_equity",
            source="RunResearchDataset.summary",
        ),
        "annualized_return_pct": _metric_value(
            value=cagr,
            valid=cagr_valid,
            unit="ratio",
            method="compound_annual_growth_rate",
            source="RunResearchDataset.portfolio_metrics",
            metadata={"minimum_duration_days": 90.0, "simulated_duration_days": duration_days},
            sample_count=int(duration_days) if duration_days is not None else None,
            minimum_sample_count=90,
            invalid_reason="simulated_window_less_than_90_days" if cagr is not None else unavailable.get("cagr") or "not_available",
            caveats=portfolio_caveats,
        ),
        "max_drawdown": _metric_value(
            value=drawdown,
            valid=drawdown_valid,
            unit="currency",
            method="max_peak_to_trough_equity_decline",
            source="RunResearchDataset.summary",
            sample_count=equity_count,
            minimum_sample_count=1,
            invalid_reason=unavailable.get("max_drawdown") or "equity_curve_unavailable",
        ),
        "max_drawdown_pct": _metric_value(
            value=drawdown_pct,
            valid=drawdown_valid,
            unit="ratio",
            method="max_peak_to_trough_equity_decline_pct",
            source="RunResearchDataset.summary",
            sample_count=equity_count,
            minimum_sample_count=1,
            invalid_reason=unavailable.get("max_drawdown_pct") or "equity_curve_unavailable",
        ),
        "drawdown_duration": _metric_value(
            value=summary.get("drawdown_duration_seconds"),
            valid=summary.get("drawdown_duration_seconds") is not None and drawdown_valid,
            unit="seconds",
            method="longest_drawdown_duration",
            source="RunResearchDataset.summary",
            sample_count=equity_count,
            minimum_sample_count=1,
            invalid_reason=unavailable.get("drawdown_duration_seconds") or "equity_curve_unavailable",
        ),
        "sharpe": _metric_value(
            value=sharpe,
            valid=sharpe_valid,
            unit="ratio",
            method="annualized_mean_excess_daily_return_over_population_stddev",
            source="RunResearchDataset.portfolio_metrics",
            metadata={"frequency": "daily", "annualization_factor": annualization_factor, "risk_free_rate": risk_free_rate},
            sample_count=returns_count,
            minimum_sample_count=2,
            invalid_reason="insufficient_return_samples_or_zero_variance",
            caveats=ratio_caveats + [f"annualization_factor={annualization_factor}", f"risk_free_rate={risk_free_rate}"],
        ),
        "sortino": _metric_value(
            value=sortino,
            valid=sortino_valid,
            unit="ratio",
            method="annualized_mean_excess_daily_return_over_downside_stddev",
            source="RunResearchDataset.portfolio_metrics",
            metadata={"frequency": "daily", "annualization_factor": annualization_factor, "risk_free_rate": risk_free_rate},
            sample_count=returns_count,
            minimum_sample_count=2,
            invalid_reason="insufficient_downside_return_samples_or_zero_downside_deviation",
            caveats=ratio_caveats + [f"annualization_factor={annualization_factor}", f"risk_free_rate={risk_free_rate}"],
        ),
        "calmar": _metric_value(
            value=calmar,
            valid=calmar_valid,
            unit="ratio",
            method="cagr_over_max_drawdown_pct",
            source="RunResearchDataset.portfolio_metrics",
            metadata={"minimum_duration_days": 90.0, "simulated_duration_days": duration_days},
            sample_count=int(duration_days) if duration_days is not None else None,
            minimum_sample_count=90,
            invalid_reason="requires_cagr_and_nonzero_drawdown",
            caveats=portfolio_caveats,
        ),
        "profit_factor": _metric_value(
            value=profit_factor,
            valid=profit_factor_valid,
            unit="ratio",
            method="gross_profit_over_absolute_gross_loss",
            source="RunResearchDataset.summary",
            sample_count=closed_trades,
            minimum_sample_count=1,
            invalid_reason="requires_winning_and_losing_trades",
        ),
        "expectancy": _metric_value(
            value=summary.get("expectancy"),
            valid=summary.get("expectancy") is not None and closed_trades > 0,
            unit="currency",
            method="average_closed_trade_net_pnl",
            source="RunResearchDataset.summary",
            sample_count=closed_trades,
            minimum_sample_count=1,
        ),
        "win_rate": _metric_value(value=summary.get("win_rate"), valid=summary.get("win_rate") is not None, unit="ratio", method="wins_over_closed_trades", source="RunResearchDataset.summary", sample_count=closed_trades),
        "loss_rate": _metric_value(value=summary.get("loss_rate"), valid=summary.get("loss_rate") is not None, unit="ratio", method="losses_over_closed_trades", source="RunResearchDataset.summary", sample_count=closed_trades),
        "average_win": _metric_value(value=avg_win, valid=avg_win is not None, unit="currency", method="average_winning_trade_net_pnl", source="RunResearchDataset.summary", sample_count=wins),
        "average_loss": _metric_value(value=avg_loss, valid=avg_loss is not None, unit="currency", method="average_losing_trade_net_pnl", source="RunResearchDataset.summary", sample_count=losses),
        "average_win_loss_ratio": _metric_value(value=avg_win_loss_ratio, valid=avg_win_loss_ratio is not None, unit="ratio", method="average_win_over_absolute_average_loss", source="RunResearchDataset.summary"),
        "largest_win": _metric_value(value=summary.get("largest_win"), valid=summary.get("largest_win") is not None, unit="currency", method="max_winning_trade_net_pnl", source="RunResearchDataset.summary", sample_count=wins),
        "largest_loss": _metric_value(value=summary.get("largest_loss"), valid=summary.get("largest_loss") is not None, unit="currency", method="min_losing_trade_net_pnl", source="RunResearchDataset.summary", sample_count=losses),
        "trade_count": _metric_value(value=closed_trades, valid=True, unit="count", method="closed_trade_count", source="RunResearchDataset.summary"),
        "winning_trades": _metric_value(value=wins, valid=True, unit="count", method="winning_closed_trade_count", source="RunResearchDataset.summary"),
        "losing_trades": _metric_value(value=losses, valid=True, unit="count", method="losing_closed_trade_count", source="RunResearchDataset.summary"),
        "breakeven_trades": _metric_value(value=breakeven, valid=True, unit="count", method="closed_trades_minus_wins_losses", source="RunResearchDataset.summary"),
        "fees": _metric_value(value=summary.get("fees"), valid=summary.get("fees") is not None, unit="currency", method="sum_closed_trade_fees", source="RunResearchDataset.summary"),
        "slippage": _metric_value(
            value=slippage_accounting.get("total_slippage_cost"),
            valid=slippage_accounting.get("total_slippage_cost") is not None,
            unit="currency",
            method="sum_execution_slippage_cost_when_fill_facts_or_zero_slippage_model_available",
            source="RunResearchDataset.execution.slippage",
            metadata={
                "configured_slippage_bps": slippage_accounting.get("configured_slippage_bps"),
                "fill_fact_count": slippage_accounting.get("fill_fact_count"),
                "status": slippage_accounting.get("status"),
            },
            sample_count=_safe_int(slippage_accounting.get("fill_fact_count")),
            invalid_reason=slippage_invalid_reason,
            caveats=[str(item) for item in slippage_accounting.get("caveats") or []],
        ),
        "exposure_pct": _metric_value(value=exposure_pct, valid=exposure_valid, unit="ratio", method="merged_trade_holding_time_over_simulated_window", source="RunResearchDataset.portfolio_metrics", invalid_reason="requires_entry_exit_timestamps_and_simulated_window", caveats=portfolio_caveats),
        "time_in_market_pct": _metric_value(value=exposure_pct, valid=exposure_valid, unit="ratio", method="merged_trade_holding_time_over_simulated_window", source="RunResearchDataset.portfolio_metrics", invalid_reason="requires_entry_exit_timestamps_and_simulated_window", caveats=portfolio_caveats),
        "average_trade_duration": _metric_value(value=average_holding, valid=average_holding is not None, unit="seconds", method="average_closed_trade_holding_seconds", source="RunResearchDataset.summary", sample_count=closed_trades),
        "margin_usage": _unavailable_metric("not_available_until_wallet_timeseries_exposes_margin_state", unit="ratio", method="requires_wallet_margin_time_series"),
    }


def _event_context(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload = _mapping(row.get("payload"))
    return _mapping(payload.get("context"))


def _event_run_seq(row: Mapping[str, Any]) -> Optional[int]:
    return _safe_int(row.get("run_seq")) or _safe_int(_event_context(row).get("run_seq")) or _safe_int(row.get("seq"))


def _event_code_count(events: Sequence[Mapping[str, Any]], code: str) -> int:
    needle = str(code or "").strip().lower()
    if not needle:
        return 0
    count = 0
    for row in events:
        context = _event_context(row)
        payload = _mapping(row.get("payload"))
        values = [
            row.get("event_name"),
            row.get("reason_code"),
            payload.get("event_name"),
            context.get("reason"),
            context.get("reason_code"),
            context.get("fault_code"),
            context.get("code"),
        ]
        if any(needle == str(value or "").strip().lower() for value in values):
            count += 1
    return count


def _runtime_ordering_summary(events: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    sequences = [value for row in events if (value := _event_run_seq(row)) is not None]
    if not events:
        return {"status": "not_available", "gap_count": None, "duplicate_count": None}
    if not sequences:
        return {"status": "not_available", "gap_count": None, "duplicate_count": None}
    present = set(sequences)
    expected = set(range(min(present), max(present) + 1))
    gap_count = len(expected - present)
    duplicate_count = len(sequences) - len(present)
    return {
        "status": "gapless" if gap_count == 0 and duplicate_count == 0 else "inconsistent",
        "gap_count": gap_count,
        "duplicate_count": duplicate_count,
        "min_run_seq": min(present),
        "max_run_seq": max(present),
    }


def _decision_market_epoch(row: Mapping[str, Any]) -> Optional[float]:
    for key in ("bar_time", "known_at", "event_ts"):
        parsed = _parse_time(row.get(key))
        if parsed is not None:
            return parsed.timestamp()
    return None


def _wallet_market_time_overtake_count(decisions: Sequence[Mapping[str, Any]]) -> Optional[int]:
    rows = []
    accepted_wallet_candidates = 0
    for decision in decisions:
        if not isinstance(decision, Mapping):
            continue
        if not bool(decision.get("accepted")):
            continue
        accepted_wallet_candidates += 1
        if not str(decision.get("decision_id") or "").strip():
            continue
        run_seq = _safe_int(decision.get("run_seq"))
        market_epoch = _decision_market_epoch(decision)
        if run_seq is None or market_epoch is None:
            continue
        rows.append((run_seq, market_epoch))
    if accepted_wallet_candidates and not rows:
        return None
    latest_epoch: Optional[float] = None
    overtakes = 0
    for _run_seq, market_epoch in sorted(rows):
        if latest_epoch is not None and market_epoch < latest_epoch:
            overtakes += 1
        latest_epoch = market_epoch if latest_epoch is None else max(latest_epoch, market_epoch)
    return overtakes


def _candle_continuity_status(candle_gaps: Mapping[str, Any]) -> str:
    canonical = str(candle_gaps.get("canonical_evidence_status") or "").strip().lower()
    if canonical == "missing" or "missing_canonical_continuity_evidence" in set(candle_gaps.get("caveats") or []):
        return "missing_canonical_continuity_evidence"
    if int(candle_gaps.get("blocking_gap_count") or 0) > 0:
        return "blocked"
    if int(candle_gaps.get("provider_gap_count") or 0) > 0:
        return "source_sparse"
    if canonical == "present":
        return "clean"
    return "unknown"


def _research_status(readiness: Mapping[str, Any]) -> str:
    if str(readiness.get("golden_candidate_status") or "").strip().lower() == "certified":
        return "research_valid"
    if readiness.get("results_ready") and readiness.get("safe_to_compare"):
        return "research_valid_with_caveats"
    if readiness.get("blocking_reasons") or readiness.get("golden_blocking_reasons"):
        return "blocked"
    if readiness.get("results_ready"):
        return "results_ready"
    return "not_ready"


def _first_failure_reason(readiness: Mapping[str, Any], diagnostics: Mapping[str, Any]) -> Optional[str]:
    for key in ("blocking_reasons", "golden_blocking_reasons"):
        values = readiness.get(key)
        if isinstance(values, list) and values:
            return str(values[0])
    summary = _mapping(diagnostics.get("summary"))
    blocking = summary.get("blocking_codes")
    if isinstance(blocking, list) and blocking:
        return str(blocking[0])
    reason = str(readiness.get("reason") or "").strip()
    return reason if reason and reason != "ready" else None


def _research_trust(dataset: Mapping[str, Any], events: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    metadata = _mapping(dataset.get("metadata"))
    readiness = _mapping(dataset.get("readiness"))
    diagnostics = _mapping(dataset.get("diagnostics"))
    candle_gaps = _mapping(dataset.get("candle_gaps"))
    wallet = _mapping(dataset.get("wallet_accounting"))
    ordering = _runtime_ordering_summary(events)
    wallet_missing = _safe_int(wallet.get("missing_wallet_trace_count"))
    observer_fact_count = int(candle_gaps.get("noncanonical_fact_count") or 0)
    canonical_status = str(candle_gaps.get("canonical_evidence_status") or "unknown")
    if canonical_status == "present" and observer_fact_count > 0:
        observer_status = "observer_diagnostics_ignored"
    elif canonical_status == "present":
        observer_status = "observer_safe"
    else:
        observer_status = "unknown"
    timeout_count = _event_code_count(events, "entry_decision_order_turn_timeout") if events else None
    return {
        "lifecycle_status": metadata.get("status"),
        "terminal_reason": metadata.get("terminal_reason") or readiness.get("reason"),
        "golden_status": readiness.get("golden_candidate_status") or "not_available",
        "golden_candidate_status": readiness.get("golden_candidate_status") or "unknown",
        "research_status": _research_status(readiness),
        "readiness_status": readiness.get("results_status") or readiness.get("dataset_status") or "unknown",
        "readiness_blockers": list(readiness.get("blocking_reasons") or []) + list(readiness.get("golden_blocking_reasons") or []),
        "caveats": list(readiness.get("caveats") or []),
        "config_hash": metadata.get("config_hash"),
        "material_config_hash": metadata.get("material_config_hash"),
        "data_snapshot_hash": metadata.get("data_snapshot_hash"),
        "strategy_hash": metadata.get("strategy_hash"),
        "semantic_fingerprint": readiness.get("semantic_fingerprint") or metadata.get("report_semantic_fingerprint"),
        "operational_fingerprint": readiness.get("operational_fingerprint") or metadata.get("report_operational_fingerprint"),
        "runtime_ordering_status": ordering.get("status") or "unknown",
        "run_seq_gap_count": ordering.get("gap_count"),
        "run_seq_duplicate_count": ordering.get("duplicate_count"),
        "wallet_trace_complete": None if wallet_missing is None else wallet_missing == 0,
        "wallet_market_time_overtake_count": _wallet_market_time_overtake_count(dataset.get("decisions") or []),
        "entry_decision_order_timeout_count": timeout_count,
        "candle_continuity_status": _candle_continuity_status(candle_gaps),
        "canonical_continuity_evidence_status": canonical_status,
        "observer_invariance_status": observer_status,
        "first_failure_reason": _first_failure_reason(readiness, diagnostics),
    }


def _holding_seconds(trade: Mapping[str, Any]) -> Optional[float]:
    value = _safe_float(trade.get("holding_seconds") or trade.get("duration_seconds"))
    if value is not None:
        return value
    start = _parse_time(trade.get("entry_time") or trade.get("opened_at"))
    end = _parse_time(trade.get("exit_time") or trade.get("closed_at"))
    if start is None or end is None or end < start:
        return None
    return (end - start).total_seconds()


def _decision_behavior(dataset: Mapping[str, Any]) -> Dict[str, Any]:
    summary = _mapping(dataset.get("summary"))
    decisions = [dict(row) for row in dataset.get("decisions") or [] if isinstance(row, Mapping)]
    trades = [dict(row) for row in dataset.get("trades") or [] if isinstance(row, Mapping)]
    rejection_reasons = Counter()
    action_distribution = Counter()
    for decision in decisions:
        action = str(decision.get("action") or decision.get("intent") or "unknown").strip().lower() or "unknown"
        action_distribution[action] += 1
        if bool(decision.get("rejected")) or str(decision.get("decision_state") or "").strip().lower() == "rejected":
            rejection_reasons[str(decision.get("reason_code") or decision.get("status") or "unknown")] += 1
    holds = [value for trade in trades if (value := _holding_seconds(trade)) is not None]
    margin_rejections = sum(count for reason, count in rejection_reasons.items() if "MARGIN" in reason.upper())
    position_rejections = sum(count for reason, count in rejection_reasons.items() if "POSITION" in reason.upper())
    entry_count = sum(count for action, count in action_distribution.items() if "enter" in action or action in {"buy", "sell"})
    exit_count = sum(1 for trade in trades if str(trade.get("exit_time") or trade.get("closed_at") or "").strip())
    return {
        "total_signals": len(dataset.get("signals") or []),
        "total_decisions": int(summary.get("total_decisions") or len(decisions)),
        "accepted_decisions": int(summary.get("accepted_decisions") or sum(1 for row in decisions if row.get("accepted"))),
        "rejected_decisions": int(summary.get("rejected_decisions") or sum(1 for row in decisions if row.get("rejected"))),
        "rejection_reasons": dict(sorted(rejection_reasons.items())),
        "action_distribution": dict(sorted(action_distribution.items())),
        "entry_count": entry_count,
        "exit_count": exit_count,
        "average_holding_period": _metric_value(value=summary.get("average_holding_seconds"), valid=summary.get("average_holding_seconds") is not None, unit="seconds", method="average_closed_trade_holding_seconds", source="RunResearchDataset.summary", sample_count=len(holds)),
        "median_holding_period": _metric_value(value=statistics.median(holds) if holds else None, valid=bool(holds), unit="seconds", method="median_closed_trade_holding_seconds", source="RunResearchDataset.trades", sample_count=len(holds)),
        "longest_trade_duration": _metric_value(value=max(holds) if holds else None, valid=bool(holds), unit="seconds", method="max_closed_trade_holding_seconds", source="RunResearchDataset.trades", sample_count=len(holds)),
        "shortest_trade_duration": _metric_value(value=min(holds) if holds else None, valid=bool(holds), unit="seconds", method="min_closed_trade_holding_seconds", source="RunResearchDataset.trades", sample_count=len(holds)),
        "margin_rejection_count": margin_rejections,
        "position_policy_rejection_count": position_rejections,
    }


def _wallet_performance(dataset: Mapping[str, Any]) -> Dict[str, Any]:
    summary = _mapping(dataset.get("summary"))
    wallet = _mapping(dataset.get("wallet_accounting"))
    diagnostics = _mapping(wallet.get("wallet_diagnostics"))
    projection = _mapping(diagnostics.get("replay_projection"))
    balances = _mapping(projection.get("balances"))
    collateral = _mapping(projection.get("free_collateral"))
    missing_count = _safe_int(wallet.get("missing_wallet_trace_count"))
    final_value = balances.get("USD") if "USD" in balances else _first_present(summary.get("equity_end"), summary.get("final_equity"))
    final_collateral = collateral.get("USD") if "USD" in collateral else None
    return {
        "wallet_trace_complete": None if missing_count is None else missing_count == 0,
        "missing_wallet_trace_count": missing_count,
        "wallet_projection_status": wallet.get("wallet_replay_status") or diagnostics.get("wallet_replay_status") or "unknown",
        "final_wallet_value": _metric_value(value=final_value, valid=final_value is not None, unit="currency", method="wallet_replay_final_balance_or_summary_equity_end", source="RunResearchDataset.wallet_accounting"),
        "final_cash_collateral": _metric_value(value=final_collateral, valid=final_collateral is not None, unit="currency", method="wallet_replay_free_collateral", source="RunResearchDataset.wallet_accounting"),
        "margin_warnings": [dict(row) for row in wallet.get("margin_warnings") or [] if isinstance(row, Mapping)],
        "reservation_leaks": _mapping(wallet.get("reservation_leaks")),
        "caveats": list(wallet.get("caveats") or []),
    }


def _symbol_breakdown(dataset: Mapping[str, Any]) -> List[Dict[str, Any]]:
    insights = _mapping(dataset.get("strategy_insights"))
    summary = _mapping(dataset.get("summary"))
    total_net = _safe_float(summary.get("net_pnl")) or 0.0
    rows_by_symbol: Dict[str, Dict[str, Any]] = {}
    for row in insights.get("per_symbol_performance") or []:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol") or "UNKNOWN")
        rows_by_symbol[symbol] = dict(row)
    decisions_by_symbol: Dict[str, List[Mapping[str, Any]]] = {}
    for decision in dataset.get("decisions") or []:
        if isinstance(decision, Mapping):
            decisions_by_symbol.setdefault(str(decision.get("symbol") or "UNKNOWN"), []).append(decision)
    trades_by_symbol: Dict[str, List[Mapping[str, Any]]] = {}
    for trade in dataset.get("trades") or []:
        if isinstance(trade, Mapping):
            trades_by_symbol.setdefault(str(trade.get("symbol") or "UNKNOWN"), []).append(trade)
            rows_by_symbol.setdefault(str(trade.get("symbol") or "UNKNOWN"), {})
    output = []
    for symbol in sorted(rows_by_symbol):
        row = rows_by_symbol[symbol]
        decisions = decisions_by_symbol.get(symbol, [])
        trades = trades_by_symbol.get(symbol, [])
        wins = [_safe_float(trade.get("net_pnl")) for trade in trades if (_safe_float(trade.get("net_pnl")) or 0.0) > 0]
        losses = [_safe_float(trade.get("net_pnl")) for trade in trades if (_safe_float(trade.get("net_pnl")) or 0.0) < 0]
        net = row.get("net_pnl") if row.get("net_pnl") is not None else sum(_safe_float(trade.get("net_pnl")) or 0.0 for trade in trades)
        gross = row.get("gross_pnl") if row.get("gross_pnl") is not None else sum(_safe_float(trade.get("gross_pnl")) or 0.0 for trade in trades)
        fees = row.get("fees") if row.get("fees") is not None else sum(_safe_float(trade.get("fees_paid") or trade.get("fees")) or 0.0 for trade in trades)
        rejection_reasons = Counter(str(decision.get("reason_code") or "unknown") for decision in decisions if decision.get("rejected"))
        contribution = (float(net) / total_net) if total_net else None
        output.append(
            {
                "symbol": symbol,
                "trade_count": int(row.get("trades") or row.get("trade_count") or len(trades)),
                "decision_count": len(decisions) if decisions else None,
                "accepted_decisions": sum(1 for decision in decisions if decision.get("accepted")) if decisions else None,
                "rejected_decisions": sum(1 for decision in decisions if decision.get("rejected")) if decisions else None,
                "rejection_count": sum(rejection_reasons.values()) if decisions else None,
                "rejection_reasons": dict(sorted(rejection_reasons.items())),
                "net_pnl": _metric_value(value=net, valid=net is not None, unit="currency", method="sum_symbol_closed_trade_net_pnl", source="RunResearchDataset.strategy_insights"),
                "gross_pnl": _metric_value(value=gross, valid=gross is not None, unit="currency", method="sum_symbol_closed_trade_gross_pnl", source="RunResearchDataset.strategy_insights"),
                "fees": _metric_value(value=fees, valid=fees is not None, unit="currency", method="sum_symbol_closed_trade_fees", source="RunResearchDataset.strategy_insights"),
                "win_rate": _metric_value(value=row.get("win_rate"), valid=row.get("win_rate") is not None, unit="ratio", method="symbol_wins_over_closed_trades", source="RunResearchDataset.strategy_insights"),
                "average_win": _metric_value(value=(sum(wins) / len(wins)) if wins else None, valid=bool(wins), unit="currency", method="average_symbol_winning_trade_net_pnl", source="RunResearchDataset.trades"),
                "average_loss": _metric_value(value=(sum(losses) / len(losses)) if losses else None, valid=bool(losses), unit="currency", method="average_symbol_losing_trade_net_pnl", source="RunResearchDataset.trades"),
                "contribution_pct": _metric_value(value=contribution, valid=contribution is not None, unit="ratio", method="symbol_net_pnl_over_run_net_pnl", source="RunResearchDataset.strategy_insights"),
                "caveats": [],
            }
        )
    return output


def _details(row: Mapping[str, Any]) -> Dict[str, Any]:
    return _mapping(row.get("details"))


def _latest_event(rows: Sequence[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
    if not rows:
        return None
    return max(rows, key=lambda row: str(row.get("observed_at") or row.get("created_at") or ""))


def _blocker_list(value: Any) -> List[Mapping[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, Mapping)]
    return []


def _wait_watermarks(wait: Mapping[str, Any], *keys: str) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for key in keys:
        for blocker in _blocker_list(wait.get(key)):
            output.append(
                {
                    "participant_key": blocker.get("participant_key"),
                    "symbol": blocker.get("symbol") or blocker.get("participant_symbol"),
                    "timeframe": blocker.get("timeframe") or blocker.get("participant_timeframe"),
                    "next_bar_time": blocker.get("next_bar_time") or blocker.get("next_bar_watermark"),
                    "current_bar_time": blocker.get("current_bar_time") or blocker.get("current_bar_watermark"),
                    "active": blocker.get("active"),
                    "done": blocker.get("done"),
                    "failed": blocker.get("failed"),
                }
            )
    return output


def _coordinator_waits(run_id: str) -> Dict[str, Any]:
    try:
        events = [
            row
            for row in report_data.list_observability_events(run_id, limit=2000)
            if str(row.get("event_name") or "") == "decision_order_top_waits_merged"
        ]
    except Exception as exc:  # noqa: BLE001 - coordinator waits are diagnostic, not material truth.
        logger.warning(
            with_log_context(
                "run_report_v2_coordinator_waits_unavailable",
                build_log_context(run_id=run_id, error=str(exc)),
            )
        )
        events = []
    event = _latest_event(events)
    if event is None:
        return {
            "status": "not_available",
            "top_waits": [],
            "caveats": ["decision_order_top_waits_merged_unavailable"],
        }
    details = _details(event)
    waits = []
    for wait in details.get("top_waits") or details.get("waits") or []:
        if not isinstance(wait, Mapping):
            continue
        first_watermarks = _wait_watermarks(wait, "blocking_participants", "first_blockers", "first_blocker_context")
        release_watermarks = _wait_watermarks(wait, "release_participants", "release_blockers", "release_context")
        blocker_symbols = sorted(
            {
                str(row.get("symbol") or row.get("participant_symbol") or "")
                for row in _blocker_list(wait.get("blocking_participants")) + _blocker_list(wait.get("first_blockers"))
                if str(row.get("symbol") or row.get("participant_symbol") or "").strip()
            }
        )
        waits.append(
            {
                "candidate_id": wait.get("candidate_id"),
                "decision_id": wait.get("decision_id"),
                "candidate_symbol": wait.get("candidate_symbol") or wait.get("symbol"),
                "candidate_timeframe": wait.get("candidate_timeframe") or wait.get("timeframe"),
                "candidate_bar_time": wait.get("candidate_bar_time") or wait.get("bar_time"),
                "wait_elapsed_ms": _safe_float(wait.get("elapsed_wait_ms") or wait.get("wait_elapsed_ms")),
                "wait_poll_count": _safe_int(wait.get("poll_count") or wait.get("wait_poll_count")),
                "final_action": wait.get("final_action"),
                "release_reason": wait.get("release_reason"),
                "blocker_symbols": blocker_symbols,
                "first_blocker_watermarks": first_watermarks,
                "release_watermarks": release_watermarks,
                "worker_id": wait.get("worker_id") or wait.get("process_id"),
                "caveats": [],
            }
        )
    caveats = []
    if details.get("release_count") is None or details.get("fail_count") is None:
        caveats.append("decision_order_release_fail_counts_unavailable")
    return {
        "status": "available",
        "total_wait_ms": _safe_float(details.get("total_wait_ms") or details.get("decision_order_wait_ms")),
        "wait_count": _safe_int(details.get("wait_count") or details.get("decision_order_wait_count")),
        "max_wait_ms": _safe_float(details.get("max_wait_ms") or details.get("decision_order_max_wait_ms")),
        "release_count": _safe_int(details.get("release_count") or details.get("decision_order_release_count")),
        "fail_count": _safe_int(details.get("fail_count") or details.get("decision_order_fail_count")),
        "top_waits": sorted(waits, key=lambda row: _safe_float(row.get("wait_elapsed_ms")) or 0.0, reverse=True),
        "caveats": caveats,
    }


def _operational_diagnostics(dataset: Mapping[str, Any]) -> Dict[str, Any]:
    readiness = _mapping(dataset.get("readiness"))
    diagnostics = _mapping(dataset.get("diagnostics"))
    summary = _mapping(diagnostics.get("summary"))
    by_code = _mapping(summary.get("by_code"))
    caveats = list(readiness.get("caveats") or [])
    telemetry_warnings = [
        dict(item)
        for item in diagnostics.get("items") or []
        if isinstance(item, Mapping) and "telemetry" in str(item.get("source") or item.get("code") or "").lower()
    ]
    step_trace_warnings = [
        dict(item)
        for item in diagnostics.get("items") or []
        if isinstance(item, Mapping) and "step_trace" in str(item.get("source") or item.get("code") or "").lower()
    ]
    botlens_caveats = [item for item in caveats if "botlens" in str(item).lower()]
    degraded = "degraded" if readiness.get("degraded_sections") or summary.get("degraded_codes") else "clean"
    return {
        "operational_fingerprint": readiness.get("operational_fingerprint") or _mapping(dataset.get("metadata")).get("report_operational_fingerprint"),
        "operational_drift_status": "not_computed",
        "telemetry_warnings": telemetry_warnings,
        "db_slow_write_warning_count": _safe_int(by_code.get("db_write_slow") or by_code.get("slow_db_write")),
        "step_trace_warnings": step_trace_warnings,
        "botlens_diagnostic_caveats": botlens_caveats,
        "diagnostics_degraded_status": degraded,
        "caveats": caveats,
    }


def _run_report_identity(dataset: Mapping[str, Any]) -> Dict[str, Any]:
    metadata = _mapping(dataset.get("metadata"))
    return {
        "run_id": metadata.get("run_id"),
        "bot_id": metadata.get("bot_id"),
        "strategy_id": metadata.get("strategy_id"),
        "strategy_name": metadata.get("strategy_name"),
        "run_type": metadata.get("run_type"),
        "execution_mode": metadata.get("execution_mode"),
        "symbols": list(metadata.get("symbols") or []),
        "instrument_ids": list(metadata.get("instrument_ids") or []),
        "timeframe": metadata.get("timeframe"),
        "timeframes": list(metadata.get("timeframes") or []),
        "provider": metadata.get("provider"),
        "exchange": metadata.get("exchange"),
        "simulated_window": _mapping(metadata.get("simulated_window")),
        "wall_clock_window": _mapping(metadata.get("wall_clock_window")),
        "starting_capital": metadata.get("starting_capital"),
    }


def build_run_report(run_id: str) -> Dict[str, Any]:
    """Build the RunReportDTO v2 payload from canonical report inputs."""

    dataset = _dataset(run_id)
    try:
        events = report_data.list_run_events(run_id)
    except Exception as exc:  # noqa: BLE001 - report v2 should degrade optional runtime ordering context.
        logger.warning(
            with_log_context(
                "run_report_v2_runtime_ordering_context_unavailable",
                build_log_context(run_id=run_id, error=str(exc)),
            )
        )
        events = []
    return {
        "contract_version": "run_report_v2",
        "schema_version": "run_report.v2",
        "run_id": run_id,
        "identity": _run_report_identity(dataset),
        "trust": _research_trust(dataset, events),
        "performance": _performance_metrics(dataset),
        "behavior": _decision_behavior(dataset),
        "wallet": _wallet_performance(dataset),
        "symbol_breakdown": _symbol_breakdown(dataset),
        "coordinator_waits": _coordinator_waits(run_id),
        "operational_diagnostics": _operational_diagnostics(dataset),
        "sections": _mapping(dataset.get("sections")),
        "raw_refs": {
            "source_contract": "RunResearchDataset.v1",
            "dataset_schema_version": dataset.get("schema_version") or DATASET_SCHEMA_VERSION,
            "dataset_route": f"/api/reports/{run_id}",
            "readiness_route": f"/api/reports/{run_id}/readiness",
            "diagnostics_route": f"/api/reports/{run_id}/diagnostics",
            "metrics_route": f"/api/reports/{run_id}/metrics",
        },
    }


def get_run_report(run_id: str) -> Dict[str, Any]:
    return build_run_report(run_id)


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
