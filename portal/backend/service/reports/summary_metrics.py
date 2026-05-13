"""Shared summary helpers for run-scoped reporting outputs."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import math
import statistics
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .metrics import (
    compute_expectancy,
    compute_max_drawdown,
    compute_profit_factor,
    compute_sharpe,
    compute_sortino,
)


ANNUALIZATION_PERIODS = 252


def parse_iso(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def closed_trades(trades: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(trade) for trade in trades if trade.get("exit_time") and trade.get("net_pnl") is not None]


def _sorted_trades(trades: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    def key(entry: Mapping[str, Any]) -> datetime:
        ts = parse_iso(entry.get("exit_time")) or parse_iso(entry.get("entry_time"))
        return ts or datetime.now(timezone.utc)

    return sorted([dict(trade) for trade in trades], key=key)


def _extract_start_balance(config: Mapping[str, Any]) -> Optional[float]:
    wallet = config.get("wallet_start") or {}
    balances = wallet.get("balances") if isinstance(wallet, Mapping) else None
    if not isinstance(balances, Mapping) or not balances:
        return None
    if len(balances) == 1:
        return _safe_float(next(iter(balances.values())))
    return None


def _build_equity_series(
    trades: Sequence[Mapping[str, Any]],
    start_balance: Optional[float],
    *,
    start_time: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], List[float]]:
    equity_series: List[Dict[str, Any]] = []
    returns: List[float] = []
    equity = float(start_balance or 0.0)
    if start_time:
        equity_series.append(
            {
                "time": start_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "value": equity,
            }
        )
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
    trades: Sequence[Mapping[str, Any]],
    start_balance: Optional[float],
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> Tuple[List[Tuple[date, float]], List[float], List[float]]:
    if not start_time or not end_time or start_time > end_time:
        return [], [], []
    daily_pnl: Dict[date, float] = defaultdict(float)
    for trade in trades:
        closed_at = parse_iso(trade.get("exit_time"))
        net = _safe_float(trade.get("net_pnl"))
        if closed_at is None or net is None:
            continue
        daily_pnl[closed_at.date()] += net

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
        returns.append((equity / previous) - 1.0 if previous else 0.0)
        previous = equity
        day += timedelta(days=1)
    return series, returns, pnl_series


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


def _compute_exposure(
    trades: Sequence[Mapping[str, Any]],
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> Optional[float]:
    if not start_time or not end_time or end_time <= start_time:
        return None
    intervals: List[Tuple[datetime, datetime]] = []
    for trade in trades:
        entry = parse_iso(trade.get("entry_time"))
        exit_time = parse_iso(trade.get("exit_time"))
        if entry and exit_time:
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


def compute_summary(
    trades: Sequence[Mapping[str, Any]],
    run_config: Mapping[str, Any],
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> Dict[str, Any]:
    closed = closed_trades(trades)
    start_balance = _extract_start_balance(run_config)
    total_net = sum(_safe_float(trade.get("net_pnl")) or 0.0 for trade in closed)
    _equity_series, _trade_returns = _build_equity_series(closed, start_balance, start_time=start_time)
    daily_equity, daily_returns, daily_pnls = _build_daily_equity(
        closed,
        start_balance,
        start_time=start_time,
        end_time=end_time,
    )
    max_drawdown_pct, max_drawdown_abs = compute_max_drawdown([value for _, value in daily_equity])
    sharpe = compute_sharpe(daily_returns, periods_per_year=ANNUALIZATION_PERIODS)
    sortino = compute_sortino(daily_returns, periods_per_year=ANNUALIZATION_PERIODS)
    volatility = statistics.pstdev(daily_returns) * math.sqrt(ANNUALIZATION_PERIODS) if len(daily_returns) >= 2 else None

    wins = [trade for trade in closed if (_safe_float(trade.get("net_pnl")) or 0.0) > 0]
    losses = [trade for trade in closed if (_safe_float(trade.get("net_pnl")) or 0.0) < 0]
    total_trades = len(closed)
    win_rate = len(wins) / total_trades if total_trades else None
    avg_win = statistics.mean([_safe_float(trade.get("net_pnl")) or 0.0 for trade in wins]) if wins else None
    avg_loss = statistics.mean([_safe_float(trade.get("net_pnl")) or 0.0 for trade in losses]) if losses else None
    payoff = abs(avg_win / avg_loss) if avg_win is not None and avg_loss not in (None, 0) else None
    total_fees = sum(_safe_float(trade.get("fees_paid")) or 0.0 for trade in closed) if closed else None

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

    calmar = cagr / max_drawdown_pct if cagr is not None and max_drawdown_pct not in (None, 0) else None
    return {
        "total_return": total_return,
        "net_pnl": total_net,
        "cagr": cagr,
        "annualized_volatility": volatility,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "max_drawdown_pct": max_drawdown_pct,
        "max_drawdown": max_drawdown_abs,
        "drawdown_duration_days": _compute_drawdown_duration(daily_equity),
        "win_rate": win_rate,
        "profit_factor": compute_profit_factor([_safe_float(trade.get("net_pnl")) or 0.0 for trade in closed]) if closed else None,
        "expectancy": compute_expectancy([_safe_float(trade.get("net_pnl")) or 0.0 for trade in closed]) if closed else None,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "payoff_ratio": payoff,
        "total_trades": total_trades,
        "fees": total_fees,
        "slippage": None,
        "exposure_pct": _compute_exposure(closed, start_time, end_time),
        "best_day": max(daily_pnls) if daily_pnls else None,
        "worst_day": min(daily_pnls) if daily_pnls else None,
        "equity_end": end_balance,
    }


__all__ = ["closed_trades", "compute_summary", "parse_iso"]
