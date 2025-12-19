import pytest

pd = pytest.importorskip("pandas")

from datetime import datetime

from indicators.market_profile import MarketProfileIndicator
import signals.engine.market_profile.payloads as payloads_module
from signals.base import BaseSignal
from signals.engine.signal_generator import build_signal_overlays
from signals.engine.market_profile_generator import build_value_area_payloads
from signals.engine import market_profile_generator  # noqa: F401 ensure adapter registration
from signals.rules.market_profile._evaluators._shared import (
    VALUE_AREA_SIGNATURE_KEYS,
    enrich_with_value_area_fields,
)


def _make_df():
    index = pd.date_range("2024-06-01", periods=10, freq="30min", tz="UTC")
    data = {
        "open": [4300.0 + i for i in range(10)],
        "high": [4301.0 + i for i in range(10)],
        "low": [4299.0 + i for i in range(10)],
        "close": [4300.5 + i for i in range(10)],
    }
    return pd.DataFrame(data, index=index)


def _breakout_signal(time: datetime) -> BaseSignal:
    metadata = {
        "source": "MarketProfile",
        "level_type": "VAH",
        "level_price": 4310.0,
        "breakout_direction": "above",
        "trigger_close": 4311.5,
        "trigger_high": 4312.0,
        "trigger_low": 4310.5,
        "confidence": 0.72,
        "value_area_id": "session-123",
    }
    return BaseSignal(
        type="breakout",
        symbol="ES",
        time=time,
        confidence=metadata["confidence"],
        metadata=metadata,
    )


def _retest_signal(time: datetime) -> BaseSignal:
    metadata = {
        "source": "MarketProfile",
        "level_type": "VAL",
        "VAL": 4295.0,
        "breakout_direction": "below",
        "direction": "down",
        "retest_role": "resistance",
        "retest_close": 4294.5,
        "bars_since_breakout": 3,
        "confidence": 0.58,
    }
    return BaseSignal(
        type="retest",
        symbol="ES",
        time=time,
        confidence=metadata["confidence"],
        metadata=metadata,
    )


def test_market_profile_signals_render_as_bubbles():
    df = _make_df()
    breakout = _breakout_signal(df.index[5].to_pydatetime())
    retest = _retest_signal(df.index[7].to_pydatetime())

    overlays = build_signal_overlays("market_profile", [breakout, retest], df)

    assert len(overlays) == 1
    payload = overlays[0]["payload"]

    assert payload["price_lines"] == []
    assert payload["markers"] == []

    bubbles = payload["bubbles"]
    assert len(bubbles) == 2

    labels = {bubble["label"] for bubble in bubbles}
    assert "VAH breakout" in labels
    assert "VAL retest" in labels

    breakout_bubble = next(b for b in bubbles if b["label"] == "VAH breakout")
    retest_bubble = next(b for b in bubbles if b["label"] == "VAL retest")

    assert breakout_bubble["direction"] == "above"
    assert breakout_bubble["accentColor"] == "#16a34a"
    assert breakout_bubble["subtype"] == "bubble"
    assert breakout_bubble["price"] > 4311.0

    assert retest_bubble["direction"] == "down"
    assert retest_bubble["accentColor"] == "#f97316"
    assert retest_bubble["detail"].startswith("Retest after 3 bars")


def test_default_min_merge_consistency_between_overlay_and_signal(monkeypatch):
    df = _make_df()
    indicator = MarketProfileIndicator(df)

    captured_min_merge = []

    def _capture_merge(self, threshold=None, min_sessions=None):
        captured_min_merge.append(min_sessions)
        return []

    monkeypatch.setattr(MarketProfileIndicator, "get_merged_profiles", _capture_merge)

    indicator.to_lightweight(df, use_merged=True)
    build_value_area_payloads(indicator, df)

    assert captured_min_merge, "Expected merge_value_areas to be invoked"
    assert all(
        value == MarketProfileIndicator.DEFAULT_MIN_MERGE_SESSIONS
        for value in captured_min_merge
    ), "Default min-merge sessions should match for overlays and signals"


def test_build_value_area_payloads_can_reuse_precomputed_indicator(monkeypatch):
    df = _make_df()
    indicator = MarketProfileIndicator(df)

    def _fail_clone(*args, **kwargs):  # pragma: no cover - defensive
        raise AssertionError("_clone_indicator_for_runtime should not be called")

    monkeypatch.setattr(payloads_module, "_clone_indicator_for_runtime", _fail_clone)

    payloads = build_value_area_payloads(
        indicator,
        df,
        runtime_indicator=indicator,
    )

    assert payloads, "Expected payloads when using precomputed indicator"


def test_value_area_payloads_include_signature():
    df = _make_df()
    indicator = MarketProfileIndicator(df)

    payloads = build_value_area_payloads(
        indicator,
        df,
        use_merged=False,
        merge_threshold=0.7,
        min_merge_sessions=4,
    )

    assert payloads, "Expected value area payloads"
    signature_payload = payloads[0]

    assert signature_payload["va_source"] == "stored_indicator"
    assert signature_payload["use_merged_value_areas"] is False
    assert signature_payload["merge_threshold"] == pytest.approx(0.7)
    assert signature_payload["min_merge_sessions"] == 4


def test_enrich_with_value_area_fields_copies_signature():
    enriched: dict = {}
    breakout_meta = {
        "value_area_id": "session-abc",
        "VAH": 4305.0,
        "VAL": 4295.0,
        "POC": 4300.0,
        "va_source": "stored_indicator",
        "use_merged_value_areas": True,
        "merge_threshold": 0.55,
        "min_merge_sessions": 5,
    }

    enrich_with_value_area_fields(enriched, breakout_meta)

    for key in VALUE_AREA_SIGNATURE_KEYS:
        assert enriched[key] == breakout_meta[key]
