"""Order and exit execution policy primitives for bot runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


LiquidityRole = Literal["maker", "taker"]


@dataclass(frozen=True)
class ExitExecutionPolicy:
    """Canonical execution semantics for a runtime exit fill."""

    event_type: str
    exit_kind: str
    order_type: str
    liquidity_role: LiquidityRole
    price_source: str
    reason_code: str


def normalize_liquidity_role(value: object) -> LiquidityRole:
    """Return a canonical liquidity role or reject malformed execution state."""

    normalized = str(value or "").strip().lower()
    if normalized not in {"maker", "taker"}:
        raise ValueError(
            f"liquidity_role={normalized!r} is unsupported; "
            "supported=['maker', 'taker']"
        )
    return "maker" if normalized == "maker" else "taker"


def fee_rate_for_role(*, role: object, maker_fee_rate: float, taker_fee_rate: float) -> float:
    """Return the fee rate that corresponds to a normalized liquidity role."""

    normalized = normalize_liquidity_role(role)
    if normalized == "maker":
        return float(maker_fee_rate or 0.0)
    return float(taker_fee_rate or 0.0)


def exit_policy_for(event_type: str) -> ExitExecutionPolicy:
    """Return deterministic order semantics for runtime exit event types."""

    normalized = str(event_type or "").strip().lower()
    if normalized == "target":
        return ExitExecutionPolicy(
            event_type="target",
            exit_kind="TARGET",
            order_type="limit_resting",
            liquidity_role="maker",
            price_source="target_price",
            reason_code="EXEC_EXIT_TARGET",
        )
    if normalized == "stop":
        return ExitExecutionPolicy(
            event_type="stop",
            exit_kind="STOP",
            order_type="stop_market",
            liquidity_role="taker",
            price_source="stop_price",
            reason_code="EXEC_EXIT_STOP",
        )
    if normalized == "fixed_horizon":
        return ExitExecutionPolicy(
            event_type="fixed_horizon",
            exit_kind="CLOSE",
            order_type="market",
            liquidity_role="taker",
            price_source="bar_close",
            reason_code="FIXED_HORIZON",
        )
    if normalized == "terminal_liquidation":
        return ExitExecutionPolicy(
            event_type="terminal_liquidation",
            exit_kind="CLOSE",
            order_type="market",
            liquidity_role="taker",
            price_source="liquidation_price",
            reason_code="TERMINAL_LIQUIDATION",
        )
    if normalized == "backtest_end":
        return ExitExecutionPolicy(
            event_type="backtest_end",
            exit_kind="CLOSE",
            order_type="market",
            liquidity_role="taker",
            price_source="bar_close",
            reason_code="BACKTEST_END",
        )
    raise ValueError(
        f"exit event_type={normalized!r} is unsupported; "
        "supported=['backtest_end', 'fixed_horizon', 'stop', 'target', "
        "'terminal_liquidation']"
    )


__all__ = [
    "ExitExecutionPolicy",
    "LiquidityRole",
    "exit_policy_for",
    "fee_rate_for_role",
    "normalize_liquidity_role",
]
