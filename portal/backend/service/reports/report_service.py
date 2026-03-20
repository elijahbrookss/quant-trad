"""Report generation for backtest runs."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import logging
import math
import statistics
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from utils.log_context import build_log_context, with_log_context

from . import report_data
from .metrics import (
    compute_expectancy,
    compute_max_drawdown,
    compute_monthly_returns,
    compute_profit_factor,
    compute_sharpe,
    compute_sortino,
)


logger = logging.getLogger(__name__)

ROLLING_WINDOW = 20
ANNUALIZATION_PERIODS = 252


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    try:
        return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _closed_trades(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    closed = []
    for trade in trades:
        if trade.get("exit_time") and trade.get("net_pnl") is not None:
            closed.append(trade)
    return closed


def _sorted_trades(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key(entry: Dict[str, Any]) -> datetime:
        ts = _parse_iso(entry.get("exit_time")) or _parse_iso(entry.get("entry_time")) or datetime.now(timezone.utc)
        return ts

    return sorted(trades, key=key)


def _extract_start_balance(config: Dict[str, Any]) -> Optional[float]:
    wallet = config.get("wallet_start") or {}
    balances = wallet.get("balances") if isinstance(wallet, dict) else None
    if not isinstance(balances, dict) or not balances:
        return None
    if len(balances) == 1:
        return _safe_float(next(iter(balances.values())))
    return None


def _build_equity_series(
    trades: Sequence[Dict[str, Any]],
    start_balance: Optional[float],
    *,
    start_time: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], List[float]]:
    equity_series: List[Dict[str, Any]] = []
    returns: List[float] = []
    equity = float(start_balance or 0.0)
    if start_time:
        equity_series.append({"time": _isoformat(start_time), "value": equity})
    for trade in _sorted_trades(trades):
        net = _safe_float(trade.get("net_pnl"))
        if net is None or not trade.get("exit_time"):
            continue
        previous = equity
        equity += net
        returns.append(net / previous if previous else 0.0)
        equity_series.append({"time": trade.get("exit_time"), "value": equity})
    return equity_series, returns


def _build_daily_equity(
    trades: Sequence[Dict[str, Any]],
    start_balance: Optional[float],
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> Tuple[List[Tuple[date, float]], List[float], List[float]]:
    if not start_time or not end_time:
        return [], [], []
    if start_time > end_time:
        return [], [], []
    daily_pnl: Dict[date, float] = defaultdict(float)
    for trade in trades:
        closed = _parse_iso(trade.get("exit_time"))
        net = _safe_float(trade.get("net_pnl"))
        if closed is None or net is None:
            continue
        daily_pnl[closed.date()] += net

    equity = float(start_balance or 0.0)
    series: List[Tuple[date, float]] = []
    returns: List[float] = []
    pnl_series: List[float] = []
    day = start_time.date()
    end_day = end_time.date()
    previous = equity
    while day <= end_day:
        equity += daily_pnl.get(day, 0.0)
        series.append((day, equity))
        pnl_series.append(equity - previous)
        if previous:
            returns.append((equity / previous) - 1.0)
        else:
            returns.append(0.0)
        previous = equity
        day += timedelta(days=1)
    return series, returns, pnl_series


def _drawdown_curve(equity_series: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    peak = None
    curve = []
    for point in equity_series:
        value = _safe_float(point.get("value"))
        if value is None:
            continue
        if peak is None or value > peak:
            peak = value
        if peak in (None, 0):
            drawdown = 0.0
        else:
            drawdown = (peak - value) / peak
        curve.append({"time": point.get("time"), "value": drawdown})
    return curve


def _rolling_sharpe_series(dates: Sequence[date], returns: Sequence[float]) -> List[Dict[str, Any]]:
    series: List[Dict[str, Any]] = []
    for idx in range(len(returns)):
        if idx + 1 < ROLLING_WINDOW:
            continue
        window = returns[idx + 1 - ROLLING_WINDOW : idx + 1]
        sharpe = compute_sharpe(window, periods_per_year=ANNUALIZATION_PERIODS)
        if sharpe is None:
            continue
        series.append({"time": dates[idx].isoformat() + "Z", "value": sharpe})
    return series


def _histogram(values: Sequence[float], bins: int = 20) -> List[Dict[str, Any]]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return []
    low = min(clean)
    high = max(clean)
    if low == high:
        return [{"bin_start": low, "bin_end": high, "count": len(clean)}]
    width = (high - low) / bins
    buckets = [0 for _ in range(bins)]
    for value in clean:
        idx = min(int((value - low) / width), bins - 1)
        buckets[idx] += 1
    results = []
    for i, count in enumerate(buckets):
        results.append(
            {
                "bin_start": low + (i * width),
                "bin_end": low + ((i + 1) * width),
                "count": count,
            }
        )
    return results


def _compute_drawdown_duration(daily_equity: Sequence[Tuple[date, float]]) -> Optional[int]:
    if not daily_equity:
        return None
    peak = None
    duration = 0
    max_duration = 0
    for _, equity in daily_equity:
        if peak is None or equity >= peak:
            peak = equity
            duration = 0
            continue
        duration += 1
        max_duration = max(max_duration, duration)
    return max_duration


def _compute_exposure(trades: Sequence[Dict[str, Any]], start_time: Optional[datetime], end_time: Optional[datetime]) -> Optional[float]:
    if not start_time or not end_time or end_time <= start_time:
        return None
    intervals: List[Tuple[datetime, datetime]] = []
    for trade in trades:
        entry = _parse_iso(trade.get("entry_time"))
        exit_time = _parse_iso(trade.get("exit_time"))
        if not entry or not exit_time:
            continue
        intervals.append((entry, exit_time))
    if not intervals:
        return None
    intervals.sort(key=lambda item: item[0])
    merged: List[Tuple[datetime, datetime]] = []
    current_start, current_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
        else:
            merged.append((current_start, current_end))
            current_start, current_end = start, end
    merged.append((current_start, current_end))
    exposure_seconds = sum((end - start).total_seconds() for start, end in merged)
    total_seconds = (end_time - start_time).total_seconds()
    return min(exposure_seconds / total_seconds, 1.0) if total_seconds > 0 else None


def _long_short_breakdown(trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    breakdown = {"long": {"count": 0, "net_pnl": 0.0}, "short": {"count": 0, "net_pnl": 0.0}}
    for trade in trades:
        direction = str(trade.get("direction") or "long").lower()
        net = _safe_float(trade.get("net_pnl")) or 0.0
        key = "short" if direction == "short" else "long"
        breakdown[key]["count"] += 1
        breakdown[key]["net_pnl"] += net
    return breakdown


def _instrument_breakdown(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for trade in trades:
        symbol = trade.get("symbol") or "UNKNOWN"
        net = _safe_float(trade.get("net_pnl")) or 0.0
        win = net > 0
        stats = grouped.setdefault(symbol, {"symbol": symbol, "trades": 0, "wins": 0, "net_pnl": 0.0})
        stats["trades"] += 1
        stats["wins"] += 1 if win else 0
        stats["net_pnl"] += net
    results = []
    for stats in grouped.values():
        trades_count = stats["trades"]
        stats["win_rate"] = stats["wins"] / trades_count if trades_count else None
        results.append(stats)
    return results


def _win_loss_streaks(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    streaks: List[Dict[str, Any]] = []
    current_type = None
    length = 0
    for trade in _sorted_trades(trades):
        net = _safe_float(trade.get("net_pnl"))
        if net is None:
            continue
        trade_type = "win" if net > 0 else "loss" if net < 0 else "flat"
        if trade_type == "flat":
            continue
        if current_type is None or trade_type != current_type:
            if current_type is not None:
                streaks.append({"type": current_type, "length": length})
            current_type = trade_type
            length = 1
        else:
            length += 1
    if current_type is not None:
        streaks.append({"type": current_type, "length": length})
    return streaks


def _hold_time_histogram(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    durations = []
    for trade in trades:
        entry = _parse_iso(trade.get("entry_time"))
        exit_time = _parse_iso(trade.get("exit_time"))
        if not entry or not exit_time:
            continue
        durations.append((exit_time - entry).total_seconds() / 3600.0)
    return _histogram(durations, bins=12)


def _trade_table(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for trade in _sorted_trades(trades):
        entry = trade.get("entry_time")
        exit_time = trade.get("exit_time")
        rows.append(
            {
                "trade_id": trade.get("id"),
                "symbol": trade.get("symbol"),
                "direction": trade.get("direction"),
                "entry_time": entry,
                "exit_time": exit_time,
                "gross_pnl": trade.get("gross_pnl"),
                "fees_paid": trade.get("fees_paid"),
                "net_pnl": trade.get("net_pnl"),
            }
        )
    return rows


def _trade_signature(trade: Dict[str, Any]) -> str:
    symbol = str(trade.get("symbol") or "")
    direction = str(trade.get("direction") or "")
    entry_time = trade.get("entry_time") or ""
    return f"{symbol}|{direction}|{entry_time}"


def _compact_trade(trade: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "trade_id": trade.get("trade_id") or trade.get("id"),
        "symbol": trade.get("symbol"),
        "direction": trade.get("direction"),
        "entry_time": trade.get("entry_time"),
        "exit_time": trade.get("exit_time"),
        "net_pnl": trade.get("net_pnl"),
        "fees_paid": trade.get("fees_paid"),
    }


def _build_trade_alignment(
    base_trades: Sequence[Dict[str, Any]],
    compare_trades: Sequence[Dict[str, Any]],
    *,
    sample_limit: int = 10,
    top_limit: int = 6,
) -> Dict[str, Any]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for trade in base_trades:
        buckets[_trade_signature(trade)].append(trade)

    matched: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    compare_only: List[Dict[str, Any]] = []
    for trade in compare_trades:
        signature = _trade_signature(trade)
        bucket = buckets.get(signature)
        if bucket:
            matched.append((bucket.pop(0), trade))
        else:
            compare_only.append(trade)

    base_only = [trade for trades in buckets.values() for trade in trades]

    def _trade_value(entry: Dict[str, Any], key: str) -> float:
        return _safe_float(entry.get(key)) or 0.0

    matched_pnl_delta = sum(
        _trade_value(compare, "net_pnl") - _trade_value(base, "net_pnl")
        for base, compare in matched
    )
    matched_fees_delta = sum(
        _trade_value(compare, "fees_paid") - _trade_value(base, "fees_paid")
        for base, compare in matched
    )

    top_deltas = []
    for base, compare in matched:
        base_pnl = _trade_value(base, "net_pnl")
        compare_pnl = _trade_value(compare, "net_pnl")
        base_fees = _trade_value(base, "fees_paid")
        compare_fees = _trade_value(compare, "fees_paid")
        top_deltas.append(
            {
                "entry_time": base.get("entry_time"),
                "symbol": base.get("symbol"),
                "direction": base.get("direction"),
                "base_trade_id": base.get("trade_id") or base.get("id"),
                "compare_trade_id": compare.get("trade_id") or compare.get("id"),
                "base_net_pnl": base_pnl,
                "compare_net_pnl": compare_pnl,
                "delta": compare_pnl - base_pnl,
                "fee_delta": compare_fees - base_fees,
            }
        )
    top_deltas.sort(key=lambda item: abs(item.get("delta") or 0), reverse=True)
    top_deltas = top_deltas[:top_limit]

    match_rate = len(matched) / len(base_trades) if base_trades else None
    return {
        "base_total": len(base_trades),
        "compare_total": len(compare_trades),
        "matched_count": len(matched),
        "base_only_count": len(base_only),
        "compare_only_count": len(compare_only),
        "match_rate": match_rate,
        "matched_pnl_delta": matched_pnl_delta,
        "matched_fees_delta": matched_fees_delta,
        "top_deltas": top_deltas,
        "base_only_sample": [_compact_trade(trade) for trade in base_only[:sample_limit]],
        "compare_only_sample": [_compact_trade(trade) for trade in compare_only[:sample_limit]],
    }


def _compute_summary(
    trades: Sequence[Dict[str, Any]],
    run_config: Dict[str, Any],
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> Dict[str, Any]:
    closed = _closed_trades(trades)
    start_balance = _extract_start_balance(run_config)
    total_net = sum(_safe_float(t.get("net_pnl")) or 0.0 for t in closed)
    equity_series, trade_returns = _build_equity_series(closed, start_balance, start_time=start_time)
    daily_equity, daily_returns, daily_pnls = _build_daily_equity(
        closed,
        start_balance,
        start_time=start_time,
        end_time=end_time,
    )
    daily_dates = [day for day, _ in daily_equity]
    max_drawdown_pct, max_drawdown_abs = compute_max_drawdown([value for _, value in daily_equity])
    sharpe = compute_sharpe(daily_returns, periods_per_year=ANNUALIZATION_PERIODS)
    sortino = compute_sortino(daily_returns, periods_per_year=ANNUALIZATION_PERIODS)
    vol = None
    if len(daily_returns) >= 2:
        sigma = statistics.pstdev(daily_returns)
        vol = sigma * math.sqrt(ANNUALIZATION_PERIODS)

    wins = [t for t in closed if (_safe_float(t.get("net_pnl")) or 0.0) > 0]
    losses = [t for t in closed if (_safe_float(t.get("net_pnl")) or 0.0) < 0]
    total_trades = len(closed)
    win_rate = len(wins) / total_trades if total_trades else None

    gross_profit = sum(_safe_float(t.get("gross_pnl")) or 0.0 for t in wins)
    gross_loss = sum(_safe_float(t.get("gross_pnl")) or 0.0 for t in losses)
    avg_win = statistics.mean([_safe_float(t.get("net_pnl")) or 0.0 for t in wins]) if wins else None
    avg_loss = statistics.mean([_safe_float(t.get("net_pnl")) or 0.0 for t in losses]) if losses else None
    payoff = None
    if avg_win is not None and avg_loss not in (None, 0):
        payoff = abs(avg_win / avg_loss)

    total_fees = None
    if closed:
        total_fees = sum(_safe_float(t.get("fees_paid")) or 0.0 for t in closed)

    total_return = None
    end_balance = None
    if start_balance is not None:
        end_balance = start_balance + total_net
        if start_balance:
            total_return = (end_balance / start_balance) - 1.0

    cagr = None
    if start_balance and end_balance and start_time and end_time:
        duration_days = (end_time - start_time).days
        if duration_days >= 90:
            years = duration_days / 365.25
            if years > 0:
                cagr = (end_balance / start_balance) ** (1 / years) - 1.0

    calmar = None
    if cagr is not None and max_drawdown_pct not in (None, 0):
        calmar = cagr / max_drawdown_pct

    best_day = max(daily_pnls) if daily_pnls else None
    worst_day = min(daily_pnls) if daily_pnls else None

    expectancy = compute_expectancy([_safe_float(t.get("net_pnl")) or 0.0 for t in closed]) if closed else None
    profit_factor = compute_profit_factor([_safe_float(t.get("net_pnl")) or 0.0 for t in closed]) if closed else None

    exposure = _compute_exposure(closed, start_time, end_time)

    summary = {
        "total_return": total_return,
        "net_pnl": total_net,
        "cagr": cagr,
        "annualized_volatility": vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "max_drawdown_pct": max_drawdown_pct,
        "max_drawdown": max_drawdown_abs,
        "drawdown_duration_days": _compute_drawdown_duration(daily_equity),
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "payoff_ratio": payoff,
        "total_trades": total_trades,
        "fees": total_fees,
        "slippage": None,
        "exposure_pct": exposure,
        "best_day": best_day,
        "worst_day": worst_day,
        "equity_end": end_balance,
    }
    return summary


def _build_compare_report(run: Dict[str, Any], trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    run_config = run.get("config_snapshot") or {}
    start_time = _parse_iso(run.get("backtest_start") or run.get("started_at"))
    end_time = _parse_iso(run.get("backtest_end") or run.get("ended_at"))
    closed = _closed_trades(trades)
    try:
        summary = _compute_summary(closed, run_config, start_time=start_time, end_time=end_time)
    except Exception as exc:  # noqa: BLE001 - diagnostics
        context = build_log_context(run_id=run.get("run_id"), phase="compare_summary")
        logger.error(with_log_context("report_compare_failed", context), exc_info=exc)
        raise

    return {
        "run_id": run.get("run_id"),
        "bot_id": run.get("bot_id"),
        "bot_name": run.get("bot_name"),
        "strategy_id": run.get("strategy_id"),
        "strategy_name": run.get("strategy_name"),
        "run_type": run.get("run_type"),
        "timeframe": run.get("timeframe"),
        "symbols": run.get("symbols") or [],
        "datasource": run.get("datasource"),
        "exchange": run.get("exchange"),
        "started_at": run.get("started_at"),
        "ended_at": run.get("ended_at"),
        "completed_at": run.get("ended_at"),
        "summary": summary,
        "run_config": run_config,
        "tables": {"trades": _trade_table(closed)},
    }


def _runtime_seconds(run: Dict[str, Any]) -> Optional[float]:
    start = _parse_iso(run.get("started_at"))
    end = _parse_iso(run.get("ended_at") or run.get("completed_at"))
    if not start or not end:
        return None
    return max((end - start).total_seconds(), 0.0)


def _delta_value(base: Any, compare: Any) -> Optional[float]:
    base_val = _safe_float(base)
    compare_val = _safe_float(compare)
    if base_val is None or compare_val is None:
        return None
    return compare_val - base_val


def _build_summary_delta(base: Dict[str, Any], compare: Dict[str, Any]) -> Dict[str, Optional[float]]:
    keys = [
        "net_pnl",
        "total_return",
        "cagr",
        "max_drawdown_pct",
        "sharpe",
        "sortino",
        "profit_factor",
        "win_rate",
        "total_trades",
        "avg_win",
        "avg_loss",
        "fees",
        "exposure_pct",
        "expectancy",
        "annualized_volatility",
        "calmar",
        "payoff_ratio",
        "best_day",
        "worst_day",
        "drawdown_duration_days",
    ]
    deltas = {}
    for key in keys:
        deltas[key] = _delta_value(base.get(key), compare.get(key))
    return deltas


def _build_config_diff(base: Dict[str, Any], compare: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    fields = [
        "fee_model",
        "slippage_model",
        "risk_settings",
        "timeframe",
        "symbols",
        "date_range",
        "wallet_start",
        "strategies",
    ]
    diff = {}
    for field in fields:
        base_value = base.get(field)
        compare_value = compare.get(field)
        if base_value != compare_value:
            diff[field] = {"base": base_value, "compare": compare_value}
    return diff


def _build_report(run: Dict[str, Any], trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    run_config = run.get("config_snapshot") or {}
    start_time = _parse_iso(run.get("backtest_start") or run.get("started_at"))
    end_time = _parse_iso(run.get("backtest_end") or run.get("ended_at"))
    closed = _closed_trades(trades)
    start_balance = _extract_start_balance(run_config)
    run_id = run.get("run_id")

    try:
        equity_series, _ = _build_equity_series(closed, start_balance, start_time=start_time)
        drawdown_series = _drawdown_curve(equity_series)
    except Exception as exc:  # noqa: BLE001 - diagnostics
        context = build_log_context(run_id=run_id, phase="equity_series")
        logger.error(with_log_context("report_build_failed", context), exc_info=exc)
        raise

    try:
        daily_equity, daily_returns, _ = _build_daily_equity(
            closed,
            start_balance,
            start_time=start_time,
            end_time=end_time,
        )
        daily_dates = [day for day, _ in daily_equity]
        rolling_sharpe = _rolling_sharpe_series(daily_dates, daily_returns) if daily_dates else []
        monthly_returns = compute_monthly_returns(daily_equity)
    except Exception as exc:  # noqa: BLE001 - diagnostics
        context = build_log_context(run_id=run_id, phase="daily_series")
        logger.error(with_log_context("report_build_failed", context), exc_info=exc)
        raise

    try:
        summary = _compute_summary(closed, run_config, start_time=start_time, end_time=end_time)
    except Exception as exc:  # noqa: BLE001 - diagnostics
        context = build_log_context(run_id=run_id, phase="summary")
        logger.error(with_log_context("report_build_failed", context), exc_info=exc)
        raise

    report = {
        "run_id": run.get("run_id"),
        "bot_id": run.get("bot_id"),
        "bot_name": run.get("bot_name"),
        "strategy_id": run.get("strategy_id"),
        "strategy_name": run.get("strategy_name"),
        "run_type": run.get("run_type"),
        "timeframe": run.get("timeframe"),
        "symbols": run.get("symbols") or [],
        "datasource": run.get("datasource"),
        "exchange": run.get("exchange"),
        "started_at": run.get("started_at"),
        "ended_at": run.get("ended_at"),
        "status": run.get("status"),
        "completed_at": run.get("ended_at"),
        "summary": summary,
        "charts": {
            "equity_curve": equity_series,
            "drawdown_curve": drawdown_series,
            "rolling_sharpe": rolling_sharpe,
            "returns_histogram": _histogram(daily_returns, bins=20),
            "monthly_returns": monthly_returns,
        },
        "trade_analytics": {
            "r_multiple_distribution": [],
            "hold_time_histogram": _hold_time_histogram(closed),
            "win_loss_streaks": _win_loss_streaks(closed),
            "direction_breakdown": _long_short_breakdown(closed),
            "instrument_breakdown": _instrument_breakdown(closed),
        },
        "tables": {
            "trades": _trade_table(closed),
            "instruments": _instrument_breakdown(closed),
            "monthly": monthly_returns,
        },
        "run_config": run_config,
    }
    report["decision_ledger"] = list(run.get("decision_ledger") or [])
    return report


def list_reports(
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
    """Return report list entries for completed runs."""

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
    logger.debug(with_log_context("report_list_start", context))
    try:
        runs = report_data.list_runs(
            run_type=run_type,
            status=status,
            bot_id=bot_id,
            timeframe=timeframe,
            started_after=started_after,
            started_before=started_before,
        )
    except Exception as exc:  # noqa: BLE001 - logging boundary
        logger.error(with_log_context("report_list_failed", context), exc_info=exc)
        raise

    search_key = (search or "").strip().lower()
    instrument_key = (instrument or "").strip().upper()
    filtered = []
    for run in runs:
        symbols = [str(sym) for sym in (run.get("symbols") or [])]
        if instrument_key and instrument_key not in {s.upper() for s in symbols}:
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

    items = []
    for run in sliced:
        summary = run.get("summary") or {}
        items.append(
            {
                "run_id": run.get("run_id"),
                "bot_id": run.get("bot_id"),
                "bot_name": run.get("bot_name"),
                "strategy_name": run.get("strategy_name"),
                "symbols": run.get("symbols") or [],
                "date_range": {
                    "start": run.get("backtest_start"),
                    "end": run.get("backtest_end"),
                },
                "timeframe": run.get("timeframe"),
                "net_pnl": summary.get("net_pnl"),
                "total_return": summary.get("total_return"),
                "max_drawdown_pct": summary.get("max_drawdown_pct"),
                "sharpe": summary.get("sharpe"),
                "trades": summary.get("total_trades"),
                "status": run.get("status"),
                "completed_at": run.get("ended_at"),
                "run_duration_seconds": _runtime_seconds(run),
            }
        )
    return {"items": items, "total": total, "limit": limit, "offset": offset}


def get_report(run_id: str) -> Dict[str, Any]:
    """Return a full report payload for *run_id*."""

    run = report_data.get_run(run_id)
    if not run:
        raise KeyError(f"Run {run_id} was not found")
    run = dict(run)
    run["decision_ledger"] = report_data.list_decision_ledger(run_id)
    trades = report_data.list_trades_for_run(run_id)
    try:
        return _build_report(run, trades)
    except Exception as exc:  # noqa: BLE001 - logging boundary
        context = build_log_context(run_id=run_id)
        logger.error(with_log_context("report_build_failed", context), exc_info=exc)
        raise


def compare_reports(run_ids: Sequence[str]) -> Dict[str, Any]:
    """Return a comparison payload for multiple run ids."""

    if not run_ids or len(run_ids) < 2:
        raise ValueError("At least two run ids are required for comparison")

    context = build_log_context(run_ids=run_ids, runs=len(run_ids))
    logger.info(with_log_context("report_compare_start", context))

    reports: List[Dict[str, Any]] = []
    for run_id in run_ids:
        run = report_data.get_run(run_id)
        if not run:
            raise KeyError(f"Run {run_id} was not found")
        trades = report_data.list_trades_for_run(run_id)
        reports.append(_build_compare_report(run, trades))

    baseline = reports[0]
    comparisons: List[Dict[str, Any]] = []
    base_trades = baseline.get("tables", {}).get("trades", [])
    base_summary = baseline.get("summary") or {}
    base_runtime = _runtime_seconds(baseline)

    for compare in reports[1:]:
        compare_trades = compare.get("tables", {}).get("trades", [])
        trade_alignment = _build_trade_alignment(base_trades, compare_trades)
        summary_delta = _build_summary_delta(base_summary, compare.get("summary") or {})
        config_diff = _build_config_diff(baseline.get("run_config") or {}, compare.get("run_config") or {})
        compare_runtime = _runtime_seconds(compare)
        runtime_delta = _delta_value(base_runtime, compare_runtime)
        comparisons.append(
            {
                "base_run_id": baseline.get("run_id"),
                "compare_run_id": compare.get("run_id"),
                "summary_delta": summary_delta,
                "runtime_delta_seconds": runtime_delta,
                "trade_alignment": trade_alignment,
                "config_diff": config_diff,
            }
        )

    payload = {
        "run_ids": [entry.get("run_id") for entry in reports],
        "baseline_run_id": baseline.get("run_id"),
        "reports": reports,
        "comparisons": comparisons,
    }
    logger.info(with_log_context("report_compare_success", context))
    return payload


__all__ = ["list_reports", "get_report", "compare_reports"]
