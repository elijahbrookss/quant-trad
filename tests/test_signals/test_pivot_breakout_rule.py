import pytest

pd = pytest.importorskip("pandas")

from indicators.pivot_level import Level
from signals.rules import PivotBreakoutConfig, pivot_breakout_rule


class DummyPivotIndicator:
    NAME = "pivot_level"

    def __init__(self, levels, symbol="TEST"):
        self.levels = levels
        self.symbol = symbol


def _build_dataframe(closes):
    periods = len(closes)
    index = pd.date_range("2024-01-01", periods=periods, freq="H")
    data = {
        "open": [float(price) for price in closes],
        "high": [float(price) + 0.5 for price in closes],
        "low": [float(price) - 0.5 for price in closes],
        "close": [float(price) for price in closes],
        "volume": [1000.0] * periods,
    }
    return pd.DataFrame(data, index=index)


def _build_level(price, kind="resistance"):
    ts = pd.Timestamp("2023-12-31T00:00:00Z")
    return Level(price=float(price), kind=kind, lookback=5, first_touched=ts, timeframe="1h")


def test_pivot_breakout_rule_detects_resistance_breakout():
    df = _build_dataframe([100, 101, 102, 105, 106])
    level = _build_level(104, kind="resistance")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_config": PivotBreakoutConfig(confirmation_bars=2),
    }

    results = pivot_breakout_rule(context)

    assert len(results) == 1
    result = results[0]

    assert result["type"] == "breakout"
    assert result["symbol"] == indicator.symbol
    assert result["direction"] == "resistance"
    assert result["level_price"] == pytest.approx(104)
    assert result["bars_closed_beyond_level"] == 2
    assert result["breakout_direction"] == "above"


def test_pivot_breakout_rule_requires_transition_from_range():
    df = _build_dataframe([103, 102, 101, 100, 99])
    level = _build_level(100, kind="support")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_confirmation_bars": 2,
    }

    results = pivot_breakout_rule(context)

    assert results == []


def test_pivot_breakout_rule_requires_enough_bars():
    df = _build_dataframe([100, 101])
    level = _build_level(99, kind="support")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_confirmation_bars": 3,
    }

    results = pivot_breakout_rule(context)

    assert results == []
