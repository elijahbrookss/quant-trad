
import pytest
import pandas as pd
import numpy as np

# New API: TrendlineIndicator now exposes .lines and .to_lightweight()
from indicators.trendline import TrendlineIndicator
from data_providers.alpaca_provider import AlpacaProvider
from indicators.config import DataContext


# ----------------------
# Integration test
# ----------------------
@pytest.mark.integration
def test_trendline_indicator_lightweight_integration():
    """
    Integration test (kept lightweight and observable):

    - Pulls 15m chart data for CL between 2025-05-15 and 2025-06-13
    - Builds TrendlineIndicator (default algo='pivot_ransac', projection +40 bars)
    - Emits lightweight chart payload (segments + markers)
    - Asserts structure and basic invariants (non-empty when lines exist, markers within solid range)
    """
    plot_ctx = DataContext(
        symbol="CL",
        start="2025-05-15",
        end="2025-06-13",
        interval="15m",
    )
    provider = AlpacaProvider()
    plot_df = provider.get_ohlcv(plot_ctx)
    assert not plot_df.empty, "15m price data is empty"

    # Indicator context can be the same or a bit wider than the plotting range
    ind_ctx = DataContext(
        symbol="CL",
        start="2025-05-01",
        end="2025-06-13",
        interval="15m",
    )

    indicator = TrendlineIndicator.from_context(
        provider=provider,
        ctx=ind_ctx,
        # ensure RANSAC path and default projection behavior
        algo="pivot_ransac",
        projection_bars=40,
    )

    payload = indicator.to_lightweight(plot_df=plot_df, include_touches=True, top_n=5)
    assert isinstance(payload, dict) and "segments" in payload and "markers" in payload
    segments, markers = payload["segments"], payload["markers"]
    assert isinstance(segments, list) and isinstance(markers, list)

    # When lines exist we expect at least one segment
    if indicator.lines:
        assert segments, "Expected at least one segment when lines are detected"

    # Basic schema checks for segments
    for seg in segments:
        for k in ("x1", "x2", "y1", "y2", "lineStyle", "lineWidth", "color"):
            assert k in seg, f"segment missing key: {k}"
        assert seg["x2"] >= seg["x1"], "segment x2 should be >= x1"
        assert seg["lineStyle"] in (0, 2), "lineStyle should be 0 (solid) or 2 (dashed projection)"

    # All touch markers should be circles with subtype 'touch'
    for m in markers:
        assert m.get("shape") == "circle"
        assert m.get("subtype") == "touch"
        assert "time" in m and isinstance(m["time"], (int, float))

    # Markers must not be beyond the latest solid segment end-time
    solid_x2s = [seg["x2"] for seg in segments if seg.get("lineStyle") == 0]
    if solid_x2s:  # only check if any solid segments exist
        max_solid_end = max(solid_x2s)
        assert all(m["time"] <= max_solid_end for m in markers), "marker appears beyond solid segment end"


# ----------------------
# Unit fixtures
# ----------------------
@pytest.fixture
def dummy_df():
    """
    30-min bars over 30 points tracing a linear uptrend + small noise.
    """
    idx = pd.date_range("2025-01-01 09:30", periods=30, freq="30min")
    base = np.array([100 + 0.5 * i for i in range(30)], dtype=float)
    rng = np.random.default_rng(42)
    noise = rng.normal(scale=0.1, size=30)
    close = base + noise
    df = pd.DataFrame(
        {
            "open": close,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": [1000] * 30,
        },
        index=idx,
    )
    return df


# ----------------------
# Unit tests
# ----------------------
@pytest.mark.unit
def test_find_pivots(dummy_df):
    """_find_pivots should return lists of (timestamp, price) tuples."""
    ind = TrendlineIndicator(dummy_df, lookbacks=[2], tolerance=0.01, min_span_bars=4, algo="pivot_ransac")
    highs, lows = ind._find_pivots(2)
    assert isinstance(highs, list) and isinstance(lows, list)
    assert all(isinstance(tp, tuple) and len(tp) == 2 for tp in highs + lows)
    assert all(isinstance(tp[0], pd.Timestamp) and isinstance(tp[1], (int, float)) for tp in highs + lows)


@pytest.mark.unit
def test_compute_generates_lines(dummy_df):
    """After init, .lines should be a list with dict-like entries when using pivot_ransac."""
    ind = TrendlineIndicator(dummy_df, lookbacks=[2, 4], tolerance=0.01, min_span_bars=4, algo="pivot_ransac")
    assert isinstance(ind.lines, list)
    # When present, each entry should have the expected keys
    for ln in ind.lines:
        assert isinstance(ln, dict)
        for k in ("side", "m", "c", "i_from", "i_solid", "i_proj", "touches"):
            assert k in ln, f"line dict missing key: {k}"
        assert ln["side"] in ("support", "resistance")


@pytest.mark.unit
def test_to_lightweight_structure_and_bounds(dummy_df):
    """to_lightweight must return segments/markers with sane bounds and schema."""
    ind = TrendlineIndicator(dummy_df, lookbacks=[3], tolerance=0.01, min_span_bars=4, algo="pivot_ransac", projection_bars=40)
    payload = ind.to_lightweight(plot_df=dummy_df, include_touches=True)
    assert isinstance(payload, dict)
    assert "segments" in payload and "markers" in payload

    segments, markers = payload["segments"], payload["markers"]
    assert isinstance(segments, list) and isinstance(markers, list)

    # Schema checks
    for seg in segments:
        for k in ("x1", "x2", "y1", "y2", "lineStyle", "lineWidth", "color"):
            assert k in seg
        assert seg["x2"] >= seg["x1"]
        assert seg["lineStyle"] in (0, 2)

    # Marker schema
    for m in markers:
        assert m.get("shape") == "circle"
        assert m.get("subtype") == "touch"
        assert "price" in m and isinstance(m["price"], (int, float))

    # Marker time within plot index bounds
    idx = dummy_df.index
    def tsec(ts):
        t = pd.Timestamp(ts)
        t = t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
        return int(t.timestamp())
    lo, hi = tsec(idx[0]), tsec(idx[-1])
    for m in markers:
        assert lo <= m["time"] <= hi, "marker time outside plotting window"


@pytest.mark.unit
def test_projection_toggle(dummy_df):
    """When projection_bars=0, no dashed segments should be produced."""
    ind = TrendlineIndicator(dummy_df, lookbacks=[3], tolerance=0.01, min_span_bars=4, algo="pivot_ransac", projection_bars=0)
    payload = ind.to_lightweight(plot_df=dummy_df, include_touches=False)
    segments = payload["segments"]
    assert all(seg.get("lineStyle") == 0 for seg in segments), "Found dashed projection with projection_bars=0"
