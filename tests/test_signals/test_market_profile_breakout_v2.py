import pytest

pd = pytest.importorskip("pandas")

from signals.rules.market_profile._evaluators.breakout_v2_eval import detect_breakouts_v2
from signals.rules.market_profile._evaluators.retest_v2_eval import detect_retests_v2


def _df_from_closes(closes):
    index = pd.date_range("2024-08-01", periods=len(closes), freq="1h", tz="UTC")
    data = {
        "open": closes,
        "high": closes,
        "low": closes,
        "close": closes,
    }
    return pd.DataFrame(data, index=index)


def _value_area():
    return {
        "VAH": 100.0,
        "VAL": 90.0,
        "value_area_id": "va-1",
        "formed_at": pd.Timestamp("2024-08-01T00:00:00Z"),
        "session_count": 1,
        "min_merge_sessions": 1,
        "va_start": pd.Timestamp("2024-08-01T00:00:00Z"),
        "va_end": pd.Timestamp("2024-08-01T00:00:00Z"),
    }


def test_breakout_v2_type1_inside_to_outside_above():
    closes = [95, 96, 97, 101, 102, 103]  # last 3 closes > VAH, prior inside
    df = _df_from_closes(closes)
    ctx = {"df": df}
    vas = _value_area()

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3)

    assert len(signals) == 1
    sig = signals[0]
    assert sig["boundary"] == "VAH"
    assert sig["breakout_variant"] == "inside_to_outside_above"
    assert sig["direction"] == "above"
    assert sig["confirm_bars"] == 3
    assert sig["VAH"] == 100.0 and sig["VAL"] == 90.0
    assert sig["level_type"] == "VAH"
    assert sig["lockout_bars"] == 3


def test_breakout_v2_type2_outside_above_to_inside():
    closes = [102, 101, 101.5, 98, 99, 100]  # last 3 inside, prior above
    df = _df_from_closes(closes)
    ctx = {"df": df}
    vas = _value_area()

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3)

    assert len(signals) == 1
    sig = signals[0]
    assert sig["boundary"] == "VAH"
    assert sig["breakout_variant"] == "outside_above_to_inside"
    assert sig["direction"] == "below"
    assert sig["VAH"] == 100.0 and sig["VAL"] == 90.0
    assert sig["level_type"] == "VAH"
    assert sig["lockout_bars"] == 3


def test_breakout_v2_type3_outside_below_to_inside():
    closes = [85, 88, 89, 91, 92, 93]  # last 3 inside, prior below
    df = _df_from_closes(closes)
    ctx = {"df": df}
    vas = _value_area()

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3)

    assert len(signals) == 1
    sig = signals[0]
    assert sig["boundary"] == "VAL"
    assert sig["breakout_variant"] == "outside_below_to_inside"
    assert sig["direction"] == "above"
    assert sig["VAH"] == 100.0 and sig["VAL"] == 90.0
    assert sig["level_type"] == "VAL"
    assert sig["lockout_bars"] == 3


def test_breakout_v2_type4_inside_to_outside_below():
    closes = [95, 96, 94, 88, 87, 86]  # last 3 below VAL, prior inside
    df = _df_from_closes(closes)
    ctx = {"df": df}
    vas = _value_area()

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3)

    assert len(signals) == 1
    sig = signals[0]
    assert sig["boundary"] == "VAL"
    assert sig["breakout_variant"] == "inside_to_outside_below"
    assert sig["direction"] == "below"
    assert sig["VAH"] == 100.0 and sig["VAL"] == 90.0
    assert sig["level_type"] == "VAL"
    assert sig["lockout_bars"] == 3


def test_retest_v2_after_breakout_above():
    # Type1 breakout then touch below VAH then reclaim above
    closes = [95, 96, 97, 101, 102, 103, 99, 101]
    df = _df_from_closes(closes)
    ctx = {"df": df}
    vas = _value_area()

    breakouts = detect_breakouts_v2(ctx, vas, confirm_bars=3)
    retests = detect_retests_v2(ctx, vas, breakouts, window=3, reclaim_bars=1)

    assert retests, "Expected retest after touch and reclaim above VAH"
    retest = retests[0]
    assert retest["breakout_id"] == breakouts[0]["breakout_id"]
    assert retest["boundary"] == "VAH"
    assert retest["retest_type"] == "reclaim"
    assert retest["direction"] == "above"
    assert retest["VAH"] == 100.0 and retest["VAL"] == 90.0
    assert retest["level_type"] == "VAH"


def test_retest_v2_after_breakout_below():
    # Type4 breakout then touch above VAL then reject below
    closes = [95, 96, 94, 88, 87, 86, 92, 89]
    df = _df_from_closes(closes)
    ctx = {"df": df}
    vas = _value_area()

    breakouts = detect_breakouts_v2(ctx, vas, confirm_bars=3)
    retests = detect_retests_v2(ctx, vas, breakouts, window=3, reclaim_bars=1)

    assert retests, "Expected retest after touch and reject below VAL"
    retest = retests[0]
    assert retest["boundary"] == "VAL"
    assert retest["retest_type"] == "reject"
    assert retest["direction"] == "below"
    assert retest["VAH"] == 100.0 and retest["VAL"] == 90.0
    assert retest["level_type"] == "VAL"


def test_breakout_v2_straddle_confirmation_does_not_emit():
    # Bodies straddle VAH (close above but body touches inside) -> should NOT confirm
    opens = [95, 96, 101, 100.0, 102, 103]
    closes = [95, 96, 102, 99.5, 102.5, 103.5]
    df = _df_from_closes(closes)
    df["open"] = opens
    ctx = {"df": df}
    vas = _value_area()

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3)
    assert not signals, "Straddled bodies should not emit breakout_v2"


def test_breakout_v2_lockout_suppresses_second_signal():
    # Two separate clean moves above VAH within lockout window -> only first emits
    closes = [95, 96, 97, 101, 102, 103, 95, 96, 101, 102, 103]
    df = _df_from_closes(closes)
    ctx = {"df": df}
    vas = _value_area()

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3, lockout_bars=10)
    assert len(signals) == 1


def test_breakout_v2_respects_formed_at_no_lookahead():
    # Formed_at after the first breakout-like sequence; only later bars eligible
    closes = [101, 102, 103, 95, 96, 97, 101, 102, 103]
    df = _df_from_closes(closes)
    ctx = {"df": df}
    vas = _value_area()
    vas["formed_at"] = df.index[5]  # eligible only from bar 5 onwards

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3, lockout_bars=3)
    assert len(signals) == 1
    assert signals[0]["bar_index"] >= 5
    assert pd.Timestamp(signals[0]["formed_at"]) <= signals[0]["break_time"]


def test_breakout_v2_body_outside_confirms():
    # Bodies fully above VAH for 3 bars -> confirm
    opens = [95, 96, 101, 101.5, 102.0, 102.5]
    closes = [95, 96, 102, 102.0, 102.3, 102.7]
    df = _df_from_closes(closes)
    df["open"] = opens
    ctx = {"df": df}
    vas = _value_area()

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3)
    assert signals, "Bodies fully above VAH should confirm"


def test_breakout_v2_doji_touching_boundary_rejects():
    # Doji that touches VAH should not count as fully above
    opens = [95, 96, 101, 101.0, 101.0, 101.0]
    closes = [95, 96, 101, 101.0, 101.0, 101.0]
    df = _df_from_closes(closes)
    df["open"] = opens
    ctx = {"df": df}
    vas = _value_area()

    signals = detect_breakouts_v2(ctx, vas, confirm_bars=3)
    assert not signals, "Doji touching boundary should not confirm"
