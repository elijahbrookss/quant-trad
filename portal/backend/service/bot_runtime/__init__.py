"""Bot runtime package."""

from .domain import (
    Candle,
    LadderPosition,
    LadderRiskEngine,
    Leg,
    StrategySignal,
    DEFAULT_RISK,
)
from .series_builder import StrategySeries
__all__ = [
    "BotRuntime",
    "DEFAULT_RISK",
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
