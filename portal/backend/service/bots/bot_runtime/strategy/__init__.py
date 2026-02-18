"""Strategy loading and series preparation for bot runtime."""

from .models import Strategy, StrategyIndicatorLink, StrategyInstrumentLink

__all__ = [
    "Strategy",
    "StrategyIndicatorLink",
    "StrategyInstrumentLink",
    "StrategySeries",
    "SeriesBuilder",
    "StrategyLoader",
]


def __getattr__(name: str):
    if name in {"StrategySeries", "SeriesBuilder"}:
        from .series_builder import StrategySeries, SeriesBuilder

        return {"StrategySeries": StrategySeries, "SeriesBuilder": SeriesBuilder}[name]
    if name == "StrategyLoader":
        from .strategy_loader import StrategyLoader

        return StrategyLoader
    raise AttributeError(name)
