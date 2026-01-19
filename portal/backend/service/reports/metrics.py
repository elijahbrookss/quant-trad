"""Metric helpers for backtest reports."""

from __future__ import annotations

from datetime import date
import math
import statistics
from typing import Iterable, List, Optional, Sequence, Tuple


def compute_sharpe(returns: Sequence[float], periods_per_year: int = 252, risk_free: float = 0.0) -> Optional[float]:
    """Compute annualized Sharpe ratio for a return series."""

    clean = [float(r) for r in returns if r is not None]
    if len(clean) < 2:
        return None
    excess = [r - risk_free for r in clean]
    sigma = statistics.pstdev(excess)
    if sigma == 0:
        return None
    mean = statistics.mean(excess)
    return mean / sigma * math.sqrt(periods_per_year)


def compute_sortino(returns: Sequence[float], periods_per_year: int = 252, risk_free: float = 0.0) -> Optional[float]:
    """Compute annualized Sortino ratio."""

    clean = [float(r) for r in returns if r is not None]
    if len(clean) < 2:
        return None
    excess = [r - risk_free for r in clean]
    downside = [r for r in excess if r < 0]
    if len(downside) < 2:
        return None
    downside_sigma = statistics.pstdev(downside)
    if downside_sigma == 0:
        return None
    mean = statistics.mean(excess)
    return mean / downside_sigma * math.sqrt(periods_per_year)


def compute_max_drawdown(equity_curve: Sequence[float]) -> Tuple[Optional[float], Optional[float]]:
    """Return max drawdown percent and absolute drawdown."""

    if not equity_curve:
        return None, None
    peak = None
    max_drawdown = 0.0
    max_drawdown_pct = 0.0
    for value in equity_curve:
        if value is None:
            continue
        equity = float(value)
        if peak is None or equity > peak:
            peak = equity
        if peak is None or peak == 0:
            continue
        drawdown = peak - equity
        drawdown_pct = drawdown / peak
        if drawdown > max_drawdown:
            max_drawdown = drawdown
        if drawdown_pct > max_drawdown_pct:
            max_drawdown_pct = drawdown_pct
    if peak is None:
        return None, None
    return max_drawdown_pct, max_drawdown


def compute_profit_factor(trade_pnls: Sequence[float]) -> Optional[float]:
    """Return profit factor for trade PnLs."""

    profits = sum(pnl for pnl in trade_pnls if pnl > 0)
    losses = sum(pnl for pnl in trade_pnls if pnl < 0)
    if profits == 0 or losses == 0:
        return None
    return profits / abs(losses)


def compute_expectancy(trade_pnls: Sequence[float]) -> Optional[float]:
    """Return expectancy for trade PnLs."""

    if not trade_pnls:
        return None
    wins = [pnl for pnl in trade_pnls if pnl > 0]
    losses = [pnl for pnl in trade_pnls if pnl < 0]
    total = len(trade_pnls)
    win_rate = len(wins) / total if total else 0.0
    loss_rate = len(losses) / total if total else 0.0
    avg_win = statistics.mean(wins) if wins else 0.0
    avg_loss = statistics.mean(losses) if losses else 0.0
    return (avg_win * win_rate) + (avg_loss * loss_rate)


def compute_monthly_returns(
    daily_equity: Sequence[Tuple[date, float]],
) -> List[dict]:
    """Aggregate daily equity into monthly return series."""

    if not daily_equity:
        return []
    months: List[dict] = []
    current_key = None
    month_start_equity = None
    last_equity = None
    for day, equity in daily_equity:
        key = (day.year, day.month)
        if key != current_key:
            if current_key is not None and month_start_equity is not None and last_equity is not None:
                change = (last_equity / month_start_equity - 1.0) if month_start_equity else None
                months.append(
                    {
                        "month": f"{current_key[0]}-{current_key[1]:02d}",
                        "return": change,
                    }
                )
            current_key = key
            month_start_equity = equity
        last_equity = equity
    if current_key is not None and month_start_equity is not None and last_equity is not None:
        change = (last_equity / month_start_equity - 1.0) if month_start_equity else None
        months.append(
            {
                "month": f"{current_key[0]}-{current_key[1]:02d}",
                "return": change,
            }
        )
    return months


__all__ = [
    "compute_sharpe",
    "compute_sortino",
    "compute_max_drawdown",
    "compute_profit_factor",
    "compute_expectancy",
    "compute_monthly_returns",
]
