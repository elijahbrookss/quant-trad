import pytest

pd = pytest.importorskip("pandas")

from indicators.market_profile import MarketProfileIndicator
from signals.rules.market_profile import (
    _value_area_breakout_evaluator,
    _detect_value_area_retest,
    market_profile_breakout_rule,
    market_profile_retest_rule,
    _BREAKOUT_CACHE_KEY,
)


@pytest.fixture
def sample_market_profile_df():
    index = pd.date_range("2025-01-01 09:30", periods=7, freq="15min", tz="UTC")
    data = {
        "open": [100.0, 100.2, 100.8, 101.4, 100.8, 100.4, 98.9],
        "high": [100.5, 100.9, 101.8, 102.0, 101.0, 100.6, 99.2],
        "low": [99.8, 100.0, 100.6, 100.9, 100.1, 98.4, 98.5],
        "close": [100.1, 100.7, 101.6, 101.2, 100.6, 98.6, 98.9],
        "volume": [1000, 1100, 1200, 1300, 1250, 1400, 1450],
    }
    return pd.DataFrame(data, index=index)


@pytest.fixture
def sample_value_area(sample_market_profile_df):
    start = sample_market_profile_df.index[0] - pd.Timedelta(days=2)
    end = sample_market_profile_df.index[-1]
    return {
        "start": start,
        "end": end,
        "VAH": 101.0,
        "VAL": 99.0,
        "POC": 100.0,
    }


@pytest.fixture
def sample_context(sample_market_profile_df):
    indicator = MarketProfileIndicator(sample_market_profile_df)
    return {
        "indicator": indicator,
        "df": sample_market_profile_df,
        "symbol": "TEST",
        "mode": "backtest",
        "market_profile_breakout_min_age_hours": 0,
        "market_profile_breakout_confirmation_bars": 1,
    }


def test_breakout_evaluator_detects_multiple_events(sample_context, sample_value_area, sample_market_profile_df):
    metas = _value_area_breakout_evaluator(sample_context, sample_value_area)
    assert len(metas) == 2

    metas = sorted(metas, key=lambda meta: meta["trigger_bar_index"])

    first = metas[0]
    second = metas[1]

    assert first["breakout_direction"] == "above"
    assert first["trigger_bar_index"] == 2
    assert first["trigger_time"] == sample_market_profile_df.index[2].to_pydatetime()
    assert first["prev_close"] == pytest.approx(sample_market_profile_df.iloc[1]["close"])
    assert first["trigger_open"] == pytest.approx(sample_market_profile_df.iloc[2]["open"])
    assert first["bars_closed_beyond_level"] == 1
    assert first["confirmation_bars_required"] == 1
    assert not first["accelerated_confirmation"]

    assert second["breakout_direction"] == "below"
    assert second["trigger_bar_index"] == 5
    assert second["trigger_time"] == sample_market_profile_df.index[5].to_pydatetime()
    assert second["trigger_close"] == pytest.approx(sample_market_profile_df.iloc[5]["close"])
    assert second["bars_closed_beyond_level"] == 1
    assert second["confirmation_bars_required"] == 1
    assert not second["accelerated_confirmation"]

    chart_end = sample_market_profile_df.index[-1].to_pydatetime()
    for meta in metas:
        assert meta["value_area_end"] == chart_end
        assert meta["value_area_end_index"] == len(sample_market_profile_df) - 1


def test_breakout_evaluator_respects_confirmation_setting(sample_context, sample_value_area, sample_market_profile_df):
    sample_context["market_profile_breakout_confirmation_bars"] = 2

    metas = _value_area_breakout_evaluator(sample_context, sample_value_area)
    assert len(metas) == 2

    metas = sorted(metas, key=lambda meta: meta["trigger_bar_index"])
    above, below = metas

    assert above["breakout_direction"] == "above"
    assert above["trigger_bar_index"] == 3
    assert above["bars_closed_beyond_level"] == 2
    assert above["confirmation_bars_required"] == 2
    assert not above["accelerated_confirmation"]
    assert above["breakout_start_bar_index"] == 2
    assert above["breakout_start_index_label"] == sample_market_profile_df.index[2]

    assert below["breakout_direction"] == "below"
    assert below["trigger_bar_index"] == 6
    assert below["bars_closed_beyond_level"] == 2
    assert below["confirmation_bars_required"] == 2
    assert not below["accelerated_confirmation"]
    assert below["breakout_start_bar_index"] == 5
    assert below["breakout_start_index_label"] == sample_market_profile_df.index[5]


def test_breakout_evaluator_flags_accelerated_confirmation(sample_value_area):
    index = pd.date_range("2025-01-03 09:30", periods=5, freq="15min", tz="UTC")
    data = {
        "open": [100.0, 100.4, 101.2, 101.5, 101.7],
        "high": [100.4, 100.8, 101.8, 102.0, 102.2],
        "low": [99.8, 100.1, 101.0, 101.2, 101.4],
        "close": [100.1, 100.6, 101.6, 101.9, 101.8],
        "volume": [900, 950, 980, 990, 995],
    }
    df = pd.DataFrame(data, index=index)
    indicator = MarketProfileIndicator(df)
    context = {
        "indicator": indicator,
        "df": df,
        "symbol": "TEST",
        "mode": "backtest",
        "market_profile_breakout_min_age_hours": 0,
        "market_profile_breakout_confirmation_bars": 3,
        "market_profile_breakout_early_window": 2,
        "market_profile_breakout_early_distance_pct": 0.001,
    }

    value_area = dict(sample_value_area)
    value_area.update({"VAH": 101.0, "VAL": 99.0, "start": index[0] - pd.Timedelta(days=2), "end": index[-1]})

    metas = _value_area_breakout_evaluator(context, value_area)
    assert len(metas) == 1
    meta = metas[0]
    assert meta["breakout_direction"] == "above"
    assert meta["trigger_bar_index"] == 3
    assert meta["bars_closed_beyond_level"] == 2
    assert meta["accelerated_confirmation"]


def test_breakout_evaluator_extends_value_area_end_to_chart_close(sample_market_profile_df):
    indicator = MarketProfileIndicator(sample_market_profile_df)
    context = {
        "indicator": indicator,
        "df": sample_market_profile_df,
        "symbol": "TEST",
        "mode": "backtest",
        "market_profile_breakout_min_age_hours": 0,
    }

    truncated_end = sample_market_profile_df.index[3]
    value_area = {
        "start": sample_market_profile_df.index[0],
        "end": truncated_end,
        "VAH": 102.0,
        "VAL": 98.0,
        "POC": 100.0,
    }

    metas = _value_area_breakout_evaluator(context, value_area)
    assert metas, "Expected breakout metadata"
    expected_end = sample_market_profile_df.index[-1].to_pydatetime()
    expected_index = len(sample_market_profile_df) - 1
    for meta in metas:
        assert meta["value_area_end"] == expected_end
        assert meta["value_area_end_index"] == expected_index


def test_breakout_evaluator_respects_indicator_extend_flag(sample_market_profile_df):
    indicator = MarketProfileIndicator(
        sample_market_profile_df,
        extend_value_area_to_chart_end=False,
    )
    context = {
        "indicator": indicator,
        "df": sample_market_profile_df,
        "symbol": "TEST",
        "mode": "backtest",
        "market_profile_breakout_min_age_hours": 0,
    }

    truncated_end = sample_market_profile_df.index[3]
    value_area = {
        "start": sample_market_profile_df.index[0],
        "end": truncated_end,
        "VAH": 102.0,
        "VAL": 98.0,
        "POC": 100.0,
    }

    metas = _value_area_breakout_evaluator(context, value_area)
    assert metas, "Expected breakout metadata"
    expected_end = truncated_end.to_pydatetime()
    expected_index = 3
    for meta in metas:
        assert meta["value_area_end"] == expected_end
        assert meta["value_area_end_index"] == expected_index


def test_breakout_evaluator_live_mode_only_reports_latest(sample_value_area):
    index = pd.date_range("2025-01-02 09:30", periods=3, freq="15min", tz="UTC")
    data = {
        "open": [100.0, 100.4, 101.2],
        "high": [100.4, 101.0, 101.6],
        "low": [99.8, 100.1, 100.9],
        "close": [100.1, 100.9, 101.4],
        "volume": [900, 950, 980],
    }
    df = pd.DataFrame(data, index=index)
    indicator = MarketProfileIndicator(df)
    context = {
        "indicator": indicator,
        "df": df,
        "symbol": "TEST",
        "mode": "live",
        "market_profile_breakout_min_age_hours": 0,
    }

    value_area = dict(sample_value_area)
    value_area.update({"start": index[0] - pd.Timedelta(days=2), "end": index[-1]})

    metas = _value_area_breakout_evaluator(context, value_area)
    assert len(metas) == 1
    meta = metas[0]
    assert meta["trigger_bar_index"] == len(df) - 1
    assert meta["breakout_direction"] == "above"


def test_breakout_evaluator_live_mode_honours_confirmation(sample_value_area):
    index = pd.date_range("2025-01-04 09:30", periods=4, freq="15min", tz="UTC")
    data = {
        "open": [100.0, 100.3, 101.1, 101.4],
        "high": [100.4, 100.9, 101.5, 101.9],
        "low": [99.8, 100.0, 100.9, 101.1],
        "close": [100.1, 100.8, 101.4, 101.6],
        "volume": [800, 820, 840, 860],
    }
    df = pd.DataFrame(data, index=index)
    indicator = MarketProfileIndicator(df)
    context = {
        "indicator": indicator,
        "df": df,
        "symbol": "TEST",
        "mode": "live",
        "market_profile_breakout_min_age_hours": 0,
        "market_profile_breakout_confirmation_bars": 2,
    }

    value_area = dict(sample_value_area)
    value_area.update({"start": index[0] - pd.Timedelta(days=2), "end": index[-1]})

    metas = _value_area_breakout_evaluator(context, value_area)
    assert len(metas) == 1
    meta = metas[0]
    assert meta["trigger_bar_index"] == len(df) - 1
    assert meta["bars_closed_beyond_level"] == 2
    assert meta["confirmation_bars_required"] == 2


def test_retest_rule_emits_retests_for_cached_breakouts(sample_context, sample_value_area):
    breakouts = market_profile_breakout_rule(sample_context, sample_value_area)
    assert len(breakouts) == 2
    assert len(sample_context[_BREAKOUT_CACHE_KEY]) == 2

    retests = market_profile_retest_rule(sample_context, sample_value_area)
    assert len(retests) == 2

    directions = {retest["retest_role"] for retest in retests}
    assert directions == {"support", "resistance"}

    bars_since = sorted(retest["bars_since_breakout"] for retest in retests)
    assert bars_since == [1, 1]


def test_retest_rule_ignores_distant_closes():
    index = pd.date_range("2025-02-01 09:30", periods=6, freq="30min", tz="UTC")
    data = {
        "open": [9.6, 9.8, 10.2, 10.9, 11.4, 11.6],
        "high": [9.9, 10.1, 10.7, 11.2, 11.5, 11.7],
        "low": [9.4, 9.7, 10.1, 10.8, 9.99, 11.1],
        "close": [9.7, 10.0, 10.6, 11.1, 11.3, 11.6],
        "volume": [800, 820, 840, 860, 880, 900],
    }
    df = pd.DataFrame(data, index=index)
    indicator = MarketProfileIndicator(df)
    context = {
        "indicator": indicator,
        "df": df,
        "symbol": "TEST",
        "mode": "backtest",
        "market_profile_breakout_min_age_hours": 0,
        "market_profile_breakout_confirmation_bars": 1,
    }

    value_area = {
        "start": index[0] - pd.Timedelta(days=1),
        "end": index[-1],
        "VAH": 10.0,
        "VAL": 8.5,
        "POC": 9.2,
    }

    breakouts = market_profile_breakout_rule(context, value_area)
    assert breakouts, "Expected breakout meta for retest evaluation"

    retests = market_profile_retest_rule(context, value_area)
    assert retests == []


def test_detect_value_area_retest_respects_value_area_start_index():
    index = pd.date_range("2025-03-01 09:30", periods=6, freq="15min", tz="UTC")
    df = pd.DataFrame(
        {
            "open": [106.2, 106.3, 106.6, 106.9, 107.2, 107.05],
            "high": [106.4, 106.5, 106.8, 107.1, 107.6, 107.3],
            "low": [106.0, 106.1, 106.4, 106.8, 107.0, 107.02],
            "close": [106.3, 106.4, 106.7, 107.0, 107.45, 107.15],
        },
        index=index,
    )

    breakout_meta = {
        "level_price": 107.2,
        "breakout_direction": "above",
        "trigger_bar_index": 4,
        "trigger_time": index[4].to_pydatetime(),
        "trigger_index_label": index[4],
        "value_area_start_index": 2,
        "value_area_start": index[2].to_pydatetime(),
        "value_area_id": "session-123",
    }

    retest = _detect_value_area_retest(
        df,
        breakout_meta,
        tolerance_pct=0.0015,
        max_bars=10,
        min_bars=1,
        mode="backtest",
    )

    assert retest is not None, "Expected a retest within the scoped window"
    assert retest["bars_since_breakout"] == 1
    assert retest["time"] == index[5].to_pydatetime()


def test_detect_value_area_retest_respects_value_area_end_index():
    index = pd.date_range("2025-03-02 09:30", periods=7, freq="15min", tz="UTC")
    df = pd.DataFrame(
        {
            "open": [206.2, 206.3, 206.6, 206.9, 207.4, 207.0, 207.1],
            "high": [206.4, 206.5, 206.9, 207.2, 207.7, 207.3, 207.2],
            "low": [206.0, 206.1, 206.4, 206.8, 207.1, 206.7, 206.9],
            "close": [206.3, 206.4, 206.8, 207.05, 207.4, 207.05, 207.15],
        },
        index=index,
    )

    breakout_meta = {
        "level_price": 207.2,
        "breakout_direction": "above",
        "trigger_bar_index": 4,
        "trigger_time": index[4].to_pydatetime(),
        "trigger_index_label": index[4],
        "value_area_start_index": 1,
        "value_area_start": index[1].to_pydatetime(),
        "value_area_end_index": 4,
        "value_area_end": index[4].to_pydatetime(),
        "value_area_id": "session-456",
    }

    retest = _detect_value_area_retest(
        df,
        breakout_meta,
        tolerance_pct=0.0015,
        max_bars=10,
        min_bars=1,
        mode="backtest",
    )

    assert retest is None
