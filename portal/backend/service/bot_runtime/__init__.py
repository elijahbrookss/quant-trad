"""Bot runtime package."""

from .domain import (
    Candle,
    LadderPosition,
    LadderRiskEngine,
    Leg,
    StrategySignal,
)
from .series_builder import StrategySeries
__all__ = [
    "BotRuntime",
    "Candle",
    "LadderPosition",
    "LadderRiskEngine",
    "Leg",
    "StrategySignal",
    "StrategySeries",
]


def __getattr__(name: str):
    if name == "BotRuntime":
        from .runtime import BotRuntime

        return BotRuntime
    raise AttributeError(name)
