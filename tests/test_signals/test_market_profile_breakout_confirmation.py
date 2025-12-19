import pytest

pd = pytest.importorskip("pandas")

from indicators.market_profile import MarketProfileIndicator
from signals.rules.market_profile.confirmation import enforce_full_bar_confirmation
from signals.rules.market_profile._evaluators.breakout_eval import _value_area_breakout_evaluator


def _make_df_above():
    index = pd.date_range("2024-07-01", periods=8, freq="30min", tz="UTC")
    data = {
        "open": [99, 99, 100.2, 101.0, 101.5, 102.1, 102.5, 102.9],
        "high": [99.5, 99.7, 101.0, 102.0, 102.5, 102.9, 103.3, 103.6],
        "low": [98.8, 98.9, 100.1, 100.8, 101.2, 101.7, 102.2, 102.6],
        "close": [99.2, 99.6, 100.9, 101.6, 102.1, 102.5, 102.8, 103.1],
    }
    return pd.DataFrame(data, index=index)


def test_enforce_full_bar_confirmation_counts_consecutive():
    df = _make_df_above()
    # Require 3 bars fully above 101; starting at idx 2 yields 3 full bars (idx 2,3,4)
    assert enforce_full_bar_confirmation(
        df,
        start_index=2,
        boundary_price=101.0,
        direction="above",
        required_bars=3,
    )

    # Require 4 bars should fail with same slice
    assert not enforce_full_bar_confirmation(
        df,
        start_index=2,
        boundary_price=101.0,
        direction="above",
        required_bars=4,
    )


def test_value_area_breakout_requires_full_bar_confirmation():
    df = _make_df_above()
    indicator = MarketProfileIndicator(df)

    value_area = {
        "VAH": 101.0,
        "VAL": 98.0,
        "POC": 99.5,
        "start": df.index[0],
        "end": df.index[-1],
        "value_area_id": "session-1",
    }

    # With confirmation_bars=3 we should emit a breakout since bars 2-4 are fully above VAH
    context = {
        "indicator": indicator,
        "df": df,
        "market_profile_breakout_confirmation_bars": 3,
    }
    signals = _value_area_breakout_evaluator(context, value_area)
    assert signals, "Expected breakout signal when 3 full bars confirm above VAH"

    # With confirmation_bars=4 the sequence is insufficient; expect no breakout
    context_strict = {
        "indicator": indicator,
        "df": df,
        "market_profile_breakout_confirmation_bars": 4,
    }
    strict_signals = _value_area_breakout_evaluator(context_strict, value_area)
    assert not strict_signals, "Breakout should require 4 full bars when configured"
