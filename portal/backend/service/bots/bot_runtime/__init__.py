"""Bot runtime package."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines.bot_runtime.core.domain import Candle, LadderPosition, LadderRiskEngine, Leg, StrategySignal
    from .strategy.models import Strategy, StrategyIndicatorLink, StrategyInstrumentLink
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
    if name in {"Candle", "LadderPosition", "LadderRiskEngine", "Leg", "StrategySignal"}:
        from engines.bot_runtime.core import domain as _domain

        return getattr(_domain, name)
    if name in {"Strategy", "StrategyIndicatorLink", "StrategyInstrumentLink"}:
        from .strategy import models as _models

        return getattr(_models, name)
    if name == "StrategySeries":
        from .strategy.series_builder import StrategySeries

        return StrategySeries
    if name == "StrategyLoader":
        from .strategy.strategy_loader import StrategyLoader

        return StrategyLoader
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
