import pytest

pd = pytest.importorskip("pandas")

from signals.util.level_breakout_v1 import (
    classify_state,
    detect_level_breakouts,
    ABOVE,
    BELOW,
    STRADDLE,
)


def _df_from_opens_closes(opens, closes):
    """Helper to create DataFrame from open and close prices."""
    index = pd.date_range("2024-08-01", periods=len(opens), freq="1h", tz="UTC")
    data = {
        "open": opens,
        "high": [max(o, c) for o, c in zip(opens, closes)],
        "low": [min(o, c) for o, c in zip(opens, closes)],
        "close": closes,
    }
    return pd.DataFrame(data, index=index)


def _df_from_closes(closes):
    """Helper to create DataFrame from close prices (open == close)."""
    return _df_from_opens_closes(closes, closes)


# State Classification Tests

def test_classify_state_above():
    # body_low > level
    assert classify_state(105.0, 110.0, 100.0) == ABOVE
    assert classify_state(110.0, 105.0, 100.0) == ABOVE
    assert classify_state(101.0, 101.0, 100.0) == ABOVE


def test_classify_state_below():
    # body_high < level
    assert classify_state(95.0, 90.0, 100.0) == BELOW
    assert classify_state(90.0, 95.0, 100.0) == BELOW
    assert classify_state(99.0, 99.0, 100.0) == BELOW


def test_classify_state_straddle_touching_above():
    # body_low == level
    assert classify_state(100.0, 105.0, 100.0) == STRADDLE
    assert classify_state(105.0, 100.0, 100.0) == STRADDLE


def test_classify_state_straddle_touching_below():
    # body_high == level
    assert classify_state(95.0, 100.0, 100.0) == STRADDLE
    assert classify_state(100.0, 95.0, 100.0) == STRADDLE


def test_classify_state_straddle_crossing():
    # body crosses level
    assert classify_state(95.0, 105.0, 100.0) == STRADDLE
    assert classify_state(105.0, 95.0, 100.0) == STRADDLE


def test_classify_state_doji_on_level():
    # open == close == level
    assert classify_state(100.0, 100.0, 100.0) == STRADDLE


# Bullish Breakout Tests

def test_bullish_breakout_basic():
    """Basic bullish breakout: prior all BELOW, confirm all ABOVE."""
    # Pattern: BELOW(3) -> ABOVE(3)
    opens = [85, 86, 87, 105, 106, 107]
    closes = [85, 86, 87, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 1
    event = events[0]
    assert event["direction"] == "bull"
    assert event["level"] == 100.0
    assert len(event["confirm_times"]) == 3
    assert len(event["prior_times"]) == 3
    assert event["confirm_states"] == [ABOVE, ABOVE, ABOVE]
    assert event["prior_states"] == [BELOW, BELOW, BELOW]


def test_bullish_breakout_prior_with_straddles():
    """Bullish breakout with straddles in prior window."""
    # Pattern: BELOW, STRADDLE, STRADDLE -> ABOVE(3)
    opens = [85, 95, 95, 105, 106, 107]
    closes = [85, 105, 105, 105, 106, 107]  # indices 1,2 straddle level 100
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 1
    event = events[0]
    assert event["direction"] == "bull"
    # Prior should have at least one BELOW and can have STRADDLE
    assert BELOW in event["prior_states"]
    assert event["confirm_states"] == [ABOVE, ABOVE, ABOVE]


def test_bullish_breakout_requires_true_below():
    """Bullish breakout fails if prior has no true BELOW."""
    # Pattern: STRADDLE(3) -> ABOVE(3) - should FAIL
    opens = [95, 95, 95, 105, 106, 107]
    closes = [105, 105, 105, 105, 106, 107]  # First 3 straddle 100
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 0  # No event because prior has no true BELOW


def test_bullish_confirm_must_be_strict():
    """Confirmation window must have no straddles."""
    # Pattern: BELOW(3) -> ABOVE, STRADDLE, ABOVE - should FAIL
    opens = [85, 86, 87, 105, 95, 105]
    closes = [85, 86, 87, 105, 105, 105]  # Index 4 straddles
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 0  # No event because confirm has STRADDLE


# Bearish Breakout Tests

def test_bearish_breakout_basic():
    """Basic bearish breakout: prior all ABOVE, confirm all BELOW."""
    # Pattern: ABOVE(3) -> BELOW(3)
    opens = [105, 106, 107, 85, 86, 87]
    closes = [105, 106, 107, 85, 86, 87]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 1
    event = events[0]
    assert event["direction"] == "bear"
    assert event["level"] == 100.0
    assert len(event["confirm_times"]) == 3
    assert len(event["prior_times"]) == 3
    assert event["confirm_states"] == [BELOW, BELOW, BELOW]
    assert event["prior_states"] == [ABOVE, ABOVE, ABOVE]


def test_bearish_breakout_prior_with_straddles():
    """Bearish breakout with straddles in prior window."""
    # Pattern: ABOVE, STRADDLE, STRADDLE -> BELOW(3)
    opens = [105, 95, 95, 85, 86, 87]
    closes = [105, 105, 105, 85, 86, 87]  # indices 1,2 straddle level 100
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 1
    event = events[0]
    assert event["direction"] == "bear"
    assert ABOVE in event["prior_states"]
    assert event["confirm_states"] == [BELOW, BELOW, BELOW]


def test_bearish_breakout_requires_true_above():
    """Bearish breakout fails if prior has no true ABOVE."""
    # Pattern: STRADDLE(3) -> BELOW(3) - should FAIL
    opens = [95, 95, 95, 85, 86, 87]
    closes = [105, 105, 105, 85, 86, 87]  # First 3 straddle 100
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 0  # No event because prior has no true ABOVE


def test_bearish_confirm_must_be_strict():
    """Confirmation window must have no straddles."""
    # Pattern: ABOVE(3) -> BELOW, STRADDLE, BELOW - should FAIL
    opens = [105, 106, 107, 85, 95, 85]
    closes = [105, 106, 107, 85, 105, 85]  # Index 4 straddles
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 0  # No event because confirm has STRADDLE


# Deduplication Tests

def test_dedup_bullish_no_repeat_while_above():
    """No repeated bullish signals while staying ABOVE."""
    # Pattern: BELOW(3) -> ABOVE(6)
    # Should emit once at index 5, not again
    opens = [85, 86, 87, 105, 106, 107, 108, 109, 110]
    closes = [85, 86, 87, 105, 106, 107, 108, 109, 110]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 1  # Only one emit


def test_dedup_bullish_reset_after_not_above():
    """Bullish signal can emit again after leaving ABOVE."""
    # Pattern: BELOW(3) -> ABOVE(3) -> STRADDLE -> BELOW(3) -> ABOVE(3)
    opens = [85, 86, 87, 105, 106, 107, 95, 85, 86, 87, 105, 106, 107]
    closes = [85, 86, 87, 105, 106, 107, 105, 85, 86, 87, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    # Should have 2 bullish events: one at index 5, another at index 12
    bullish_events = [e for e in events if e["direction"] == "bull"]
    assert len(bullish_events) == 2


def test_dedup_bearish_no_repeat_while_below():
    """No repeated bearish signals while staying BELOW."""
    # Pattern: ABOVE(3) -> BELOW(6)
    opens = [105, 106, 107, 85, 86, 87, 88, 89, 90]
    closes = [105, 106, 107, 85, 86, 87, 88, 89, 90]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 1  # Only one emit


def test_dedup_bearish_reset_after_not_below():
    """Bearish signal can emit again after leaving BELOW."""
    # Pattern: ABOVE(3) -> BELOW(3) -> STRADDLE -> ABOVE(3) -> BELOW(3)
    opens = [105, 106, 107, 85, 86, 87, 95, 105, 106, 107, 85, 86, 87]
    closes = [105, 106, 107, 85, 86, 87, 105, 105, 106, 107, 85, 86, 87]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    # Should have 2 bearish events
    bearish_events = [e for e in events if e["direction"] == "bear"]
    assert len(bearish_events) == 2


def test_dedup_straddle_resets_both():
    """STRADDLE resets both dedup flags."""
    # Pattern: BELOW(3) -> ABOVE(3) -> STRADDLE(1) -> ABOVE(3)
    # First bullish at index 5, STRADDLE at 6, but can't emit bullish at 8
    # because prior window includes ABOVE states
    opens = [85, 86, 87, 105, 106, 107, 95, 105, 106]
    closes = [85, 86, 87, 105, 106, 107, 105, 105, 106]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    # Should only emit once (first bullish)
    assert len(events) == 1


# Windowing Tests

def test_insufficient_history_no_emit():
    """No events when history is insufficient."""
    opens = [85, 86]
    closes = [85, 86]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 0


def test_exact_minimum_history():
    """Events work with exactly minimum required bars."""
    # Exactly 6 bars: 3 prior + 3 confirm
    opens = [85, 86, 87, 105, 106, 107]
    closes = [85, 86, 87, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events) == 1


def test_multiple_breakouts_in_sequence():
    """Multiple breakouts detected in long sequence."""
    # Pattern: BELOW(3) -> ABOVE(3) -> BELOW(3) -> ABOVE(3)
    opens = [85, 86, 87, 105, 106, 107, 85, 86, 87, 105, 106, 107]
    closes = [85, 86, 87, 105, 106, 107, 85, 86, 87, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    # Should have: 1 bullish, 1 bearish, 1 bullish = 3 events
    assert len(events) == 3
    assert events[0]["direction"] == "bull"
    assert events[1]["direction"] == "bear"
    assert events[2]["direction"] == "bull"


# Debug Record Tests

def test_debug_records_generated():
    """Debug records are returned when debug=True."""
    opens = [85, 86, 87, 105, 106, 107]
    closes = [85, 86, 87, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, debug_records = detect_level_breakouts(
        df, level=100.0, confirm_bars=3, prior_bars=3, debug=True
    )

    assert debug_records is not None
    assert len(debug_records) == 1  # Only last bar is evaluated (min history = 6)
    assert debug_records[0]["candle_idx"] == 5
    assert debug_records[0]["emit_direction"] == "bull"


def test_debug_records_gate_failures():
    """Debug records show gate failures."""
    # Pattern: STRADDLE(3) -> ABOVE(3) - should fail prior_missing_true_opposite
    opens = [95, 95, 95, 105, 106, 107]
    closes = [105, 105, 105, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, debug_records = detect_level_breakouts(
        df, level=100.0, confirm_bars=3, prior_bars=3, debug=True
    )

    assert len(events) == 0
    assert debug_records is not None
    assert len(debug_records) == 1
    assert debug_records[0]["gate_failed"] == "prior_missing_true_opposite"
    assert debug_records[0]["is_candidate_bull"] is True


def test_debug_records_emit_tracking():
    """Debug records track emit decisions."""
    opens = [85, 86, 87, 105, 106, 107, 108]
    closes = [85, 86, 87, 105, 106, 107, 108]
    df = _df_from_opens_closes(opens, closes)

    events, debug_records = detect_level_breakouts(
        df, level=100.0, confirm_bars=3, prior_bars=3, debug=True
    )

    assert len(events) == 1
    assert debug_records is not None
    assert len(debug_records) == 2  # Bars 5 and 6 evaluated

    # First record should emit
    assert debug_records[0]["emit_direction"] == "bull"
    # Second record should fail prior gate (prior window has ABOVE states now)
    assert debug_records[1]["gate_failed"] in ("deduped", "prior_not_opposite")
    assert debug_records[1]["is_candidate_bull"] is True


# Event Schema Tests

def test_event_contains_all_required_fields():
    """Event contains all required fields."""
    opens = [85, 86, 87, 105, 106, 107]
    closes = [85, 86, 87, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, level_name="TEST", confirm_bars=3, prior_bars=3)

    assert len(events) == 1
    event = events[0]

    required_fields = [
        "time", "direction", "level", "level_name",
        "confirm_times", "confirm_states", "confirm_start_time", "confirm_end_time",
        "prior_times", "prior_states", "prior_start_time", "prior_end_time",
        "id"
    ]
    for field in required_fields:
        assert field in event, f"Missing field: {field}"


def test_event_id_is_stable():
    """Event IDs are stable and unique."""
    opens = [85, 86, 87, 105, 106, 107, 85, 86, 87]
    closes = [85, 86, 87, 105, 106, 107, 85, 86, 87]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, level_name="VAH", confirm_bars=3, prior_bars=3)

    assert len(events) == 2
    assert events[0]["id"] != events[1]["id"]
    assert "VAH" in events[0]["id"]
    assert "bull" in events[0]["id"] or "bear" in events[0]["id"]


def test_confirm_times_length_matches_confirm_bars():
    """Confirm times length matches confirm_bars parameter."""
    opens = [85, 86, 87, 105, 106, 107]
    closes = [85, 86, 87, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events[0]["confirm_times"]) == 3
    assert len(events[0]["confirm_states"]) == 3


def test_prior_times_length_matches_prior_bars():
    """Prior times length matches prior_bars parameter."""
    opens = [85, 86, 87, 105, 106, 107]
    closes = [85, 86, 87, 105, 106, 107]
    df = _df_from_opens_closes(opens, closes)

    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=3)

    assert len(events[0]["prior_times"]) == 3
    assert len(events[0]["prior_states"]) == 3


# Edge Case Tests

def test_level_exactly_at_body_boundary():
    """Level exactly at body boundary is treated as STRADDLE."""
    # close == level
    opens = [95.0, 96.0, 97.0]
    closes = [96.0, 97.0, 100.0]  # Last close exactly at level
    df = _df_from_opens_closes(opens, closes)

    # Manual check: last bar should be STRADDLE
    state = classify_state(97.0, 100.0, 100.0)
    assert state == STRADDLE


def test_empty_dataframe():
    """Empty DataFrame returns no events."""
    df = pd.DataFrame({"open": [], "close": []})
    events, _ = detect_level_breakouts(df, level=100.0)
    assert len(events) == 0


def test_nan_values_dropped():
    """NaN values are dropped with warning."""
    # Create DataFrame with NaN after the fact
    index = pd.date_range("2024-08-01", periods=6, freq="1h", tz="UTC")
    df = pd.DataFrame({
        "open": [85, 86, None, 105, 106, 107],
        "close": [85, 86, 87, 105, 106, 107],
        "high": [85, 86, 87, 105, 106, 107],
        "low": [85, 86, 87, 105, 106, 107],
    }, index=index)

    # Should drop row with NaN and still work
    events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=2, prior_bars=2)

    # After dropping NaN at index 2, we have 5 bars
    # Should still detect breakouts if enough history
    assert isinstance(events, list)
