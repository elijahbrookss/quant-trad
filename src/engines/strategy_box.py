"""Strategy box orchestrating indicator signals and order submission."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, Iterable, Optional

import pandas as pd

from core.logger import logger
from signals.base import BaseSignal

from .order_engine import ExecutionRequest, OrderEngine

SignalGenerator = Callable[[pd.DataFrame], Iterable[BaseSignal]]
RuleEvaluator = Callable[[Iterable[BaseSignal], pd.Series], Optional[Dict[str, str]]]
QtySizer = Callable[[Dict[str, str], Iterable[BaseSignal], pd.Series], float]
CorrelationIdFactory = Callable[[str, datetime, Dict[str, str]], str]


@dataclass
class StrategyContext:
    """Static attributes describing the strategy box."""

    strategy_id: str
    symbol: str
    timeframe: str


class StrategyBox:
    """Runs a strategy-specific loop and emits execution requests."""

    def __init__(
        self,
        context: StrategyContext,
        signal_generator: SignalGenerator,
        rule_evaluator: RuleEvaluator,
        qty_sizer: QtySizer,
        order_engine: OrderEngine,
        correlation_id_factory: Optional[CorrelationIdFactory] = None,
    ) -> None:
        self._ctx = context
        self._signal_generator = signal_generator
        self._rule_evaluator = rule_evaluator
        self._qty_sizer = qty_sizer
        self._order_engine = order_engine
        self._correlation_id_factory = (
            correlation_id_factory
            if correlation_id_factory is not None
            else self._default_correlation_id
        )

    def on_bar_close(self, df: pd.DataFrame) -> Optional[str]:
        """Evaluate signals on the provided price history and submit orders."""

        if df is None or df.empty:
            logger.debug(
                "strategy_box.on_bar_close skipped: empty frame",
                extra={"strategy_id": self._ctx.strategy_id},
            )
            return None

        signals = list(self._signal_generator(df))
        latest_row = df.iloc[-1]
        decision = self._rule_evaluator(signals, latest_row)
        if not decision:
            logger.debug(
                "strategy_box.on_bar_close no-trade",
                extra={"strategy_id": self._ctx.strategy_id, "timestamp": latest_row.name},
            )
            return None

        qty = self._qty_sizer(decision, signals, latest_row)
        if qty <= 0:
            logger.info(
                "strategy_box sizing rejected",
                extra={"strategy_id": self._ctx.strategy_id, "qty": qty, "timestamp": latest_row.name},
            )
            return None

        correlation_id = self._correlation_id_factory(self._ctx.strategy_id, latest_row.name, decision)
        metadata = {
            "timeframe": self._ctx.timeframe,
            "decision": decision,
            "signals": [s.to_dict() for s in signals[:5]],
        }
        request = ExecutionRequest(
            correlation_id=correlation_id,
            strategy_id=self._ctx.strategy_id,
            symbol=self._ctx.symbol,
            side=decision.get("side", ""),
            qty=qty,
            timestamp=_ensure_datetime(latest_row.name),
            metadata=metadata,
        )

        response = self._order_engine.submit(request)
        logger.info(
            "strategy_box order_response",
            extra={
                "strategy_id": self._ctx.strategy_id,
                "correlation_id": correlation_id,
                "status": response.status,
                "reason": response.reason,
                "order_id": response.order_id,
            },
        )
        return response.status

    def _default_correlation_id(self, strategy_id: str, timestamp: datetime, decision: Dict[str, str]) -> str:
        decision_side = decision.get("side", "na")
        ts = _ensure_datetime(timestamp)
        return f"{strategy_id}-{decision_side}-{int(ts.timestamp())}"


def _ensure_datetime(value: datetime | pd.Timestamp) -> datetime:
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value
