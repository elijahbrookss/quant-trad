"""Bot runtime package."""

from .domain import (
    Candle,
    LadderPosition,
    LadderRiskEngine,
    Leg,
    StrategySignal,
)
from .models import (
    Strategy,
    StrategyIndicatorLink,
    StrategyInstrumentLink,
)
from .series_builder import StrategySeries
from .strategy_loader import StrategyLoader

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
        from .runtime import BotRuntime

        return BotRuntime
    if name == "_timeframe_to_seconds":
        from .runtime import _timeframe_to_seconds

        return _timeframe_to_seconds
    if name == "fetch_ohlcv":
        from ..candle_service import fetch_ohlcv

        return fetch_ohlcv
    if name == "pd":
        import pandas as _pd

        return _pd
    if name == "strategy_service":
        from .. import strategy_service

        return strategy_service
    raise AttributeError(name)
