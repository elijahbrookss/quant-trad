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


def _build_dataframe_from_ohlc(rows):
    periods = len(rows)
    index = pd.date_range("2024-02-01", periods=periods, freq="H")
    data = {"open": [], "high": [], "low": [], "close": [], "volume": []}

    for open_, high, low, close in rows:
        data["open"].append(float(open_))
        data["high"].append(float(high))
        data["low"].append(float(low))
        data["close"].append(float(close))
        data["volume"].append(1000.0)

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
    assert result["level_kind"] == "resistance"
    assert result["source_level_kind"] == "resistance"


def test_pivot_breakout_rule_emits_mid_series_breakout():
    closes = [100, 101, 102, 105, 106, 107, 108]
    df = _build_dataframe(closes)
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
    breakout = results[0]

    expected_time = df.index[4]
    assert breakout["time"] == expected_time.to_pydatetime()
    assert breakout["breakout_start"] == df.index[3].to_pydatetime()
    assert breakout["trigger_close"] == pytest.approx(closes[4])


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


def test_pivot_breakout_rule_detects_support_breakdown_after_flip():
    closes = [99, 103, 105, 106, 102, 101, 99]
    df = _build_dataframe(closes)
    # Level initially labelled as resistance because last price is below.
    level = _build_level(104, kind="resistance")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_config": PivotBreakoutConfig(confirmation_bars=2),
    }

    results = pivot_breakout_rule(context)

    assert len(results) == 2
    first, second = results

    assert first["direction"] == "resistance"
    assert first["breakout_direction"] == "above"
    assert first["time"] == df.index[3].to_pydatetime()

    assert second["direction"] == "support"
    assert second["breakout_direction"] == "below"
    assert second["time"] == df.index[5].to_pydatetime()


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


def test_pivot_breakout_rule_emits_multiple_breakouts_in_backtest():
    closes = [100, 103, 104, 105, 106, 103, 102, 105, 107]
    df = _build_dataframe(closes)
    level = _build_level(104, kind="resistance")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_config": PivotBreakoutConfig(confirmation_bars=2),
    }

    results = pivot_breakout_rule(context)

    assert len(results) == 3
    first, second, third = results

    assert first["trigger_close"] == pytest.approx(closes[4])
    assert first["time"] == df.index[4].to_pydatetime()
    assert first["direction"] == "resistance"
    assert second["trigger_close"] == pytest.approx(closes[6])
    assert second["time"] == df.index[6].to_pydatetime()
    assert second["direction"] == "support"
    assert third["trigger_close"] == pytest.approx(closes[8])
    assert third["time"] == df.index[8].to_pydatetime()
    assert third["direction"] == "resistance"


def test_pivot_breakout_rule_accelerates_confirmation_on_large_move():
    # Level at 100 with a strong close 5% above the level on the first bar.
    closes = [100, 105, 104, 103, 102]
    df = _build_dataframe(closes)
    level = _build_level(100, kind="resistance")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_config": PivotBreakoutConfig(
            confirmation_bars=3,
            early_confirmation_distance_pct=0.03,
        ),
    }

    results = pivot_breakout_rule(context)

    assert len(results) == 1
    breakout = results[0]

    assert breakout["bars_closed_beyond_level"] == 1
    assert breakout["accelerated_confirmation"] is True
    assert breakout["time"] == df.index[1].to_pydatetime()


def test_pivot_breakout_rule_requires_full_candles_for_confirmation():
    rows = [
        (100.0, 101.0, 99.5, 100.5),
        (101.0, 101.8, 100.2, 101.4),
        (102.5, 103.5, 101.5, 103.2),  # straddles the level with a low below
        (103.0, 104.0, 101.8, 103.5),  # straddles the level with a low below
        (104.0, 104.8, 102.1, 104.2),
        (104.5, 105.2, 102.6, 104.9),
    ]
    df = _build_dataframe_from_ohlc(rows)
    level = _build_level(102.0, kind="resistance")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_config": PivotBreakoutConfig(confirmation_bars=2),
    }

    results = pivot_breakout_rule(context)

    assert len(results) == 1
    breakout = results[0]

    # Breakout should begin on the first full candle above the level (index 4)
    assert breakout["breakout_start"] == df.index[4].to_pydatetime()
    assert breakout["time"] == df.index[5].to_pydatetime()
    assert breakout["bars_closed_beyond_level"] == 2


def test_pivot_breakout_rule_allows_straddle_between_confirmation_and_breakout():
    rows = [
        (99.2, 99.6, 98.6, 99.1),
        (99.0, 99.4, 98.4, 98.9),
        (99.4, 100.6, 98.8, 99.7),  # straddle with wick above the level
        (100.1, 100.9, 99.2, 100.0),  # straddle that resets the run
        (100.8, 101.4, 100.3, 101.1),
        (101.2, 101.8, 100.6, 101.5),
    ]
    df = _build_dataframe_from_ohlc(rows)
    level = _build_level(100.0, kind="resistance")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_config": PivotBreakoutConfig(confirmation_bars=2),
    }

    results = pivot_breakout_rule(context)

    assert len(results) == 1
    breakout = results[0]

    assert breakout["breakout_start"] == df.index[4].to_pydatetime()
    assert breakout["time"] == df.index[5].to_pydatetime()
    assert breakout["breakout_direction"] == "above"
    assert breakout["direction"] == "resistance"


def test_pivot_breakout_rule_requires_prior_confirmation_for_breakout_label():
    # Price only spends one bar below the level before breaking above and failing.
    closes = [103, 105, 103, 102, 101]
    df = _build_dataframe(closes)
    level = _build_level(104, kind="resistance")
    indicator = DummyPivotIndicator([level])

    context = {
        "indicator": indicator,
        "df": df,
        "symbol": indicator.symbol,
        "pivot_breakout_config": PivotBreakoutConfig(confirmation_bars=2),
    }

    results = pivot_breakout_rule(context)

    assert results == []
