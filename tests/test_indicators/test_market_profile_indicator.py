# test_market_profile_indicator.py

import pytest
import pandas as pd
from indicators.market_profile import MarketProfileIndicator
from indicators.config import DataContext
from data_providers.alpaca_provider import AlpacaProvider


@pytest.mark.integration
def test_market_profile_indicator_integration_plot():
    """
    Integration test for MarketProfileIndicator:
    - Pulls 30m chart data for AAPL between 2025-05-01 and 2025-05-15
    - Builds MarketProfileIndicator via from_context
    - Merges value areas using default thresholds
    - Generates overlays on the same 30m data
    - Verifies overlays and legend entries exist
    - Uses provider’s plot_ohlcv to write a file under integration_tests/market_profile
    """
    # Define a 30-minute context over two weeks
    ctx = DataContext(
        symbol="CL",
        start="2025-06-01",
        end="2025-06-30",
        interval="30m"
    )

    provider = AlpacaProvider()
    # Fetch the same 30m data for plotting
    plot_df = provider.get_ohlcv(ctx)
    assert not plot_df.empty, "Integration: fetched plot_df is empty"

    # Build MarketProfileIndicator (internally taps provider.get_ohlcv)
    mpi = MarketProfileIndicator.from_context(
        provider=provider,
        ctx=ctx,
        bin_size=0.5,      # can adjust bin size as desired
        mode="tpo",
        interval="30m"
    )

    # At least one daily profile should be computed
    assert isinstance(mpi.daily_profiles, list)
    assert mpi.daily_profiles, "Integration: daily_profiles is empty"

    # Merge value areas (default threshold=0.6, min_merge=2)
    merged = mpi.merge_value_areas()
    # merged_profiles may be empty if no consecutive overlaps exist, but daily_profiles must exist
    assert isinstance(merged, list)

    # Generate overlays on the plot_df index
    overlays, legend_entries = mpi.to_overlays(plot_df, use_merged=True)

    # At least one overlay should have been generated
    assert overlays, "Integration: no overlays generated"
    assert legend_entries, "Integration: no legend entries generated"

    # Finally, call provider.plot_ohlcv (integration) to save a chart
    provider.plot_ohlcv(
        plot_ctx=ctx,
        title="Integration Test – Market Profile (CL 30m)",
        overlays=overlays,
        legend_entries=legend_entries,
        show_volume=True,
        output_subdir="integration_tests/market_profile"
    )


@pytest.fixture
def dummy_df():
    """
    A small dummy DataFrame spanning two calendar dates, hourly bars.
    Used to test TPO histogram, value area extraction, merging, and overlays.
    """
    idx = [
        pd.Timestamp("2025-01-01 10:00", tz="UTC"),
        pd.Timestamp("2025-01-01 11:00", tz="UTC"),
        pd.Timestamp("2025-01-02 10:00", tz="UTC"),
        pd.Timestamp("2025-01-02 11:00", tz="UTC"),
    ]
    data = {
        "open":  [100, 101, 102, 103],
        "high":  [101, 102, 103, 104],
        "low":   [ 99, 100, 101, 102],
        "close": [100, 101, 103, 103],
        "volume":[1000,1100,1200,1300],
    }
    return pd.DataFrame(data, index=idx)

@pytest.mark.unit
def test_build_tpo_histogram_and_extract_value_area(dummy_df):
    """
    Unit test for _build_tpo_histogram & _extract_value_area on the first day's bars:
      - Day 1 has two bars:
          Bar A: low=99 → high=101  (buckets: 99, 100, 101)
          Bar B: low=100→ high=102  (buckets: 100, 101, 102)
        Combined TPO counts:
          99 → 1, 100 → 2, 101 → 2, 102 → 1  (total = 6, threshold = 4.2)

        sorted_buckets by count desc: [(100,2),(101,2),(99,1),(102,1)]
        cumulative hits ≥4.2 only after including the third bucket (99).
        → value‐area prices = [100, 101, 99]
        → POC = 100.0, VAL = 99.0, VAH = 101.0
    """
    # Slice out exactly the two rows from 2025-01-01
    df_day1 = dummy_df.loc["2025-01-01 10:00":"2025-01-01 11:00"]
    mpi = MarketProfileIndicator(df_day1, bin_size=1.0, mode="tpo")

    # 1) Build the histogram manually for those two bars:
    hist = mpi._build_tpo_histogram(df_day1)
    # Expect each bucket exactly as described:
    #   99 → 1, 100 → 2, 101 → 2, 102 → 1
    expected_hist = {99.0: 1, 100.0: 2, 101.0: 2, 102.0: 1}
    assert hist == expected_hist

    # 2) Extract the value area:
    va = mpi._extract_value_area(hist)
    assert va["POC"] == pytest.approx(100.0)
    # Because cum at 100→2 (<4.2), then at 101→4 (<4.2), then including 99→5 (≥4.2),
    # the VA prices = [100, 101, 99], so VAL = 99.0, VAH = 101.0
    assert va["VAL"] == pytest.approx(99.0)
    assert va["VAH"] == pytest.approx(101.0)


@pytest.mark.unit
def test_calculate_overlap():
    """
    Unit test for _calculate_overlap:
    - VA1=[100, 105], VA2=[103, 108] ⇒ overlap= min(105,108)-max(100,103)=2
      range2=108-103=5 ⇒ 2/5=0.4
    """
    overlap = MarketProfileIndicator._calculate_overlap(100.0, 105.0, 103.0, 108.0)
    assert pytest.approx(overlap, rel=1e-6) == 0.4

@pytest.mark.unit
def test_merge_value_areas_unit(dummy_df):
    """
    Unit test for merge_value_areas on manually crafted daily_profiles:
    - Day1: VAL=99, VAH=101, POC=100   (covers 2025-01-01)
    - Day2: VAL=100, VAH=102, POC=101  (covers 2025-01-02)
    With threshold=0.5 ⇒ overlap ratio = (min(101,102)-max(99,100))=1 / (102-100)=0.5 ⇒ equals threshold ⇒ merge.
    """
    mpi = MarketProfileIndicator(dummy_df, bin_size=1.0, mode="tpo")

    # Overwrite the automatically computed daily_profiles with two synthetic ones
    day1_start = pd.Timestamp("2025-01-01 10:00", tz="UTC")
    day1_end   = pd.Timestamp("2025-01-01 11:00", tz="UTC")
    day2_start = pd.Timestamp("2025-01-02 10:00", tz="UTC")
    day2_end   = pd.Timestamp("2025-01-02 11:00", tz="UTC")

    mpi.daily_profiles = [
        {
            "start_date": day1_start,
            "end_date": day1_end,
            "VAL": 99.0, "VAH": 101.0, "POC": 100.0
        },
        {
            "start_date": day2_start,
            "end_date": day2_end,
            "VAL": 100.0, "VAH": 102.0, "POC": 101.0
        }
    ]

    merged = mpi.merge_value_areas(threshold=0.5, min_merge=2)
    # Expect a single merged entry:
    assert len(merged) == 1
    m = merged[0]
    assert m["start"] == day1_start
    assert m["end"]   == day2_end
    assert m["VAL"] == pytest.approx(99.0)
    assert m["VAH"] == pytest.approx(102.0)
    assert m["POC"] == pytest.approx((100.0 + 101.0) / 2)

@pytest.mark.unit
def test_merge_value_areas_no_merge_due_to_threshold_unit(dummy_df):
    """
    Unit test for merge_value_areas when overlap < threshold:
    - VA1=[99,100], VA2=[101,102] ⇒ no overlap ⇒ should return [].
    """
    mpi = MarketProfileIndicator(dummy_df, bin_size=1.0, mode="tpo")

    day1_start = pd.Timestamp("2025-01-01 10:00", tz="UTC")
    day1_end   = pd.Timestamp("2025-01-01 11:00", tz="UTC")
    day2_start = pd.Timestamp("2025-01-02 10:00", tz="UTC")
    day2_end   = pd.Timestamp("2025-01-02 11:00", tz="UTC")

    mpi.daily_profiles = [
        {"start_date": day1_start, "end_date": day1_end, "VAL": 99.0, "VAH": 100.0, "POC": 99.5},
        {"start_date": day2_start, "end_date": day2_end, "VAL": 101.0, "VAH": 102.0, "POC": 101.5}
    ]

    merged = mpi.merge_value_areas(threshold=0.1, min_merge=2)
    assert merged == []

@pytest.mark.unit
def test_to_overlays_using_merge(dummy_df):
    """
    Instead of manually setting merged_profiles, we:
      1. Manually assign daily_profiles so merge_value_areas has something to merge.
      2. Call merge_value_areas(...) to populate merged_profiles.
      3. Build a small plot_df index that exactly spans one session.
      4. Call to_overlays(...) and assert we get 3 overlays and the correct legend set.
    """

    # 1) Create an indicator instance using dummy_df
    mpi = MarketProfileIndicator(dummy_df, bin_size=1.0, mode="tpo")

    # 2) Overwrite daily_profiles with two synthetic profiles that overlap at threshold=0.5
    day1_start = pd.Timestamp("2025-01-01 10:00", tz="UTC")
    day1_end   = pd.Timestamp("2025-01-01 11:00", tz="UTC")
    day2_start = pd.Timestamp("2025-01-02 10:00", tz="UTC")
    day2_end   = pd.Timestamp("2025-01-02 11:00", tz="UTC")

    mpi.daily_profiles = [
        {
            "start_date": day1_start,
            "end_date":   day1_end,
            "VAL":  99.0,
            "VAH": 101.0,
            "POC": 100.0
        },
        {
            "start_date": day2_start,
            "end_date":   day2_end,
            "VAL": 100.0,
            "VAH": 102.0,
            "POC": 101.0
        }
    ]

    # 3) Call merge_value_areas so merged_profiles is computed “under the hood”
    merged = mpi.merge_value_areas(threshold=0.5, min_merge=2)
    assert len(merged) == 1, "We expected exactly one merged session"

    # 4) Build a small plot_df index spanning exactly the first session’s times
    #    (this ensures to_overlays sees at least one index within [start_date, end_date]).
    plot_start = day1_start
    plot_end   = day1_end
    plot_idx   = pd.date_range(start=plot_start, end=plot_end, freq="30min", tz="UTC")
    plot_df    = pd.DataFrame({"open": [100]*len(plot_idx)}, index=plot_idx)

    # 5) Call to_overlays with use_merged=True
    overlays, legend_entries = mpi.to_overlays(plot_df, use_merged=True)

    # We expect 2 overlays (VAH, VAL) for that merged session
    assert len(overlays) == 2