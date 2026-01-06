"""Bot runtime package."""

from engines.bot_runtime.core.domain import (
    Candle,
    LadderPosition,
    LadderRiskEngine,
    Leg,
    StrategySignal,
)
from .strategy.models import (
    Strategy,
    StrategyIndicatorLink,
    StrategyInstrumentLink,
)
from .strategy.series_builder import StrategySeries
from .strategy.strategy_loader import StrategyLoader

__all__ = [
    "BotRuntime",
    "Candle",
    "LadderPosition",
    "LadderRiskEngine",
    "Leg",
    "Strategy",
    "StrategyIndicatorLink",
    "StrategyInstrumentLink",
    "StrategyLoader",
    "StrategySignal",
    "StrategySeries",
]


def __getattr__(name: str):
    if name == "BotRuntime":
        from .runtime.runtime import BotRuntime

        return BotRuntime
    if name == "_timeframe_to_seconds":
        from .runtime.runtime import _timeframe_to_seconds

        return _timeframe_to_seconds
    if name == "fetch_ohlcv":
        from ...market.candle_service import fetch_ohlcv

        return fetch_ohlcv
    if name == "pd":
        import pandas as _pd

        return _pd
    if name == "strategy_service":
        from ...strategies import strategy_service

        return strategy_service
    raise AttributeError(name)
