from __future__ import annotations

from datetime import datetime, timedelta, timezone

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import OutputRef, RuntimeOutput
from indicators.regime.engine import RegimeEngine
from indicators.regime.overlays import build_regime_overlay, build_regime_overlays
from indicators.regime.runtime import TypedRegimeIndicator


def _candle(index: int, *, close: float) -> Candle:
    start = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)
    time = start + timedelta(minutes=index)
    return Candle(
        time=time,
        open=close - 5.0,
        high=close + 10.0,
        low=close - 10.0,
        close=close,
        volume=1000.0 + index,
    )


def _dependency_output(bar_time: datetime, stats: dict[str, float]) -> RuntimeOutput:
    return RuntimeOutput(bar_time=bar_time, ready=True, value=dict(stats))


def _trend_stats() -> dict[str, float]:
    return {
        "atr_zscore": 1.1,
        "tr_pct": 0.022,
        "atr_ratio": 1.22,
        "directional_efficiency": 0.84,
        "slope": 180.0,
        "slope_stability": 0.12,
        "range_position": 0.94,
        "atr_short": 850.0,
        "atr_slope": 65.0,
        "range_contraction": 1.16,
        "overlap_pct": 0.18,
        "volume_zscore": 0.35,
        "volume_vs_median": 1.04,
    }


def _range_stats() -> dict[str, float]:
    return {
        "atr_zscore": -0.15,
        "tr_pct": 0.009,
        "atr_ratio": 0.94,
        "directional_efficiency": 0.18,
        "slope": 8.0,
        "slope_stability": 0.35,
        "range_position": 0.52,
        "atr_short": 640.0,
        "atr_slope": -18.0,
        "range_contraction": 0.90,
        "overlap_pct": 0.79,
        "volume_zscore": -0.10,
        "volume_vs_median": 0.98,
    }


def _transitionish_stats() -> dict[str, float]:
    return {
        "atr_zscore": 0.10,
        "tr_pct": 0.012,
        "atr_ratio": 1.01,
        "directional_efficiency": 0.49,
        "slope": 12.0,
        "slope_stability": 0.48,
        "range_position": 0.58,
        "atr_short": 700.0,
        "atr_slope": 8.0,
        "range_contraction": 1.00,
        "overlap_pct": 0.51,
        "volume_zscore": 0.04,
        "volume_vs_median": 1.00,
    }


def _dependency_ref() -> OutputRef:
    return OutputRef(indicator_id="stats-1", output_name="candle_stats")


def _context_fields(
    *,
    state: str,
    direction: str = "neutral",
    is_known: bool = True,
    is_mature: bool = True,
    is_trustworthy: bool = True,
    trust_score: float = 0.72,
    recent_switch_count: int = 1,
) -> dict[str, object]:
    return {
        "context_regime_state": state,
        "context_regime_direction": direction,
        "context_is_known": is_known,
        "context_is_mature": is_mature,
        "context_is_trustworthy": is_trustworthy,
        "context_trust_score": trust_score,
        "context_recent_switch_count": recent_switch_count,
    }


def test_regime_engine_scores_distinguish_trend_from_range() -> None:
    engine = RegimeEngine()

    trend = engine.classify(candle={}, stats=_trend_stats()).as_dict()
    range_result = engine.classify(candle={}, stats=_range_stats()).as_dict()

    assert trend["structure"]["state"] == "trend"
    assert trend["structure"]["trend_direction"] == "up"
    assert trend["structure"]["trend_score"] > trend["structure"]["range_score"]
    assert range_result["structure"]["state"] == "range"
    assert range_result["structure"]["range_score"] > range_result["structure"]["trend_score"]


def test_regime_runtime_emits_context_and_metric_outputs() -> None:
    indicator = TypedRegimeIndicator(
        indicator_id="regime-1",
        version="v1",
        params={
            "min_confidence": 0.5,
            "structure_min_confidence": 0.4,
            "structure_confirm_bars": 1,
            "volatility_confirm_bars": 1,
            "liquidity_confirm_bars": 1,
            "expansion_confirm_bars": 1,
            "smoothing_alpha": 1.0,
        },
        candle_stats_indicator_id="stats-1",
    )
    bar = _candle(0, close=100_000.0)
    indicator.apply_bar(
        bar,
        {
            _dependency_ref(): _dependency_output(
                bar.time,
                _trend_stats(),
            )
        },
    )

    snapshot = indicator.snapshot()
    context = snapshot["market_regime"]
    metrics = snapshot["regime_metrics"]

    assert context.ready is True
    assert context.value["state_key"] == "transition_up"
    assert context.value["fields"]["committed_state"] == "trend"
    assert context.value["fields"]["context_regime_state"] == "transition_up"
    assert context.value["fields"]["actionable_state"] == "transition_up"
    assert context.value["fields"]["trend_direction"] == "up"
    assert context.value["fields"]["bars_in_regime"] == 1
    assert context.value["fields"]["is_known"] is False
    assert context.value["fields"]["is_trustworthy"] is False
    assert metrics.ready is True
    assert metrics.value["trend_score"] > metrics.value["range_score"]
    assert metrics.value["trend_direction_value"] == 1.0
    assert metrics.value["bars_in_regime"] == 1.0
    assert metrics.value["is_trustworthy"] == 0.0


def test_regime_overlay_markers_align_to_confirmed_block_known_at() -> None:
    candles = [_candle(index, close=100_000.0 + index * 20.0) for index in range(12)]
    regime_rows = {}
    for candle in candles[:6]:
        regime_rows[candle.time.replace(tzinfo=None)] = {
            "structure": {
                "state": "trend",
                "trend_direction": "up",
                "confidence": 0.68,
                "score_margin": 0.18,
                "trust_score": 0.74,
                "is_known": True,
                "is_mature": True,
                "is_trustworthy": True,
                "recent_switch_count": 1,
                **_context_fields(state="trend_up", direction="up"),
            },
            "volatility": {"state": "normal"},
            "liquidity": {"state": "normal"},
            "expansion": {"state": "stable"},
            "confidence": 0.68,
            "regime_key": "trend|normal|normal|stable",
        }
    for candle in candles[6:]:
        regime_rows[candle.time.replace(tzinfo=None)] = {
            "structure": {
                "state": "range",
                "trend_direction": "neutral",
                "confidence": 0.63,
                "score_margin": 0.15,
                "trust_score": 0.71,
                "is_known": True,
                "is_mature": True,
                "is_trustworthy": True,
                "recent_switch_count": 2,
                **_context_fields(state="range", direction="neutral"),
            },
            "volatility": {"state": "normal"},
            "liquidity": {"state": "normal"},
            "expansion": {"state": "stable"},
            "confidence": 0.63,
            "regime_key": "range|normal|normal|stable",
        }

    overlay = build_regime_overlay(
        candles=candles,
        regime_rows=regime_rows,
        timeframe_seconds=60,
        regime_version="v1",
        include_change_markers=True,
        include_regime_points=True,
    )
    built = build_regime_overlays(
        candles=candles,
        regime_rows=regime_rows,
        timeframe_seconds=60,
        regime_version="v1",
        include_change_markers=True,
        include_marker_overlay=True,
    )
    assert overlay is not None
    regime_payload = overlay["payload"]
    marker_payload = next(
        overlay["payload"] for overlay in built if overlay["type"] == "regime_markers"
    )

    assert len(regime_payload["regime_blocks"]) == 2
    known_at_markers = [marker for marker in marker_payload["markers"] if marker["subtype"] == "regime_known_at"]
    label_markers = [marker for marker in marker_payload["markers"] if marker["subtype"] == "regime_block_label"]
    assert len(known_at_markers) == 2
    assert len(label_markers) == len(regime_payload["regime_blocks"])

    second_block = regime_payload["regime_blocks"][1]
    change_marker = next(
        marker for marker in known_at_markers if marker["time"] == int(second_block["known_at"])
    )
    known_at = int(second_block["known_at"])
    block_start = int(second_block["x1"])

    assert change_marker["time"] == known_at
    assert any(segment["x1"] == block_start for segment in regime_payload["segments"])
    assert all(point.get("regime_block_id") for point in regime_payload["regime_points"])


def test_established_range_persists_through_brief_transition_noise() -> None:
    indicator = TypedRegimeIndicator(
        indicator_id="regime-1",
        version="v1",
        params={
            "min_confidence": 0.5,
            "structure_min_confidence": 0.4,
            "structure_confirm_bars": 1,
            "volatility_confirm_bars": 1,
            "liquidity_confirm_bars": 1,
            "expansion_confirm_bars": 1,
            "smoothing_alpha": 1.0,
        },
        candle_stats_indicator_id="stats-1",
    )

    for index in range(6):
        candle = _candle(index, close=100_000.0 + index)
        indicator.apply_bar(
            candle,
            {_dependency_ref(): _dependency_output(candle.time, _range_stats())},
        )
    for index in range(6, 8):
        candle = _candle(index, close=100_010.0 + index)
        indicator.apply_bar(
            candle,
            {_dependency_ref(): _dependency_output(candle.time, _transitionish_stats())},
        )

    snapshot = indicator.snapshot()["market_regime"]
    assert snapshot.value["fields"]["context_regime_state"] == "range"
    assert snapshot.value["state_key"] == "range"


def test_established_trend_persists_through_brief_transition_noise() -> None:
    indicator = TypedRegimeIndicator(
        indicator_id="regime-1",
        version="v1",
        params={
            "min_confidence": 0.5,
            "structure_min_confidence": 0.4,
            "structure_confirm_bars": 1,
            "volatility_confirm_bars": 1,
            "liquidity_confirm_bars": 1,
            "expansion_confirm_bars": 1,
            "smoothing_alpha": 1.0,
        },
        candle_stats_indicator_id="stats-1",
    )

    for index in range(6):
        candle = _candle(index, close=100_000.0 + index * 60.0)
        indicator.apply_bar(
            candle,
            {_dependency_ref(): _dependency_output(candle.time, _trend_stats())},
        )
    for index in range(6, 8):
        candle = _candle(index, close=100_340.0 + index * 5.0)
        indicator.apply_bar(
            candle,
            {_dependency_ref(): _dependency_output(candle.time, _transitionish_stats())},
        )

    snapshot = indicator.snapshot()["market_regime"]
    assert snapshot.value["fields"]["context_regime_state"] == "trend_up"
    assert snapshot.value["fields"]["context_regime_direction"] == "up"
    assert snapshot.value["state_key"] == "trend_up"


def test_transition_boxes_use_local_price_envelope() -> None:
    candles = [
        _candle(0, close=98_400.0),
        _candle(1, close=98_600.0),
        _candle(2, close=98_800.0),
        _candle(3, close=99_000.0),
        _candle(4, close=99_100.0),
        _candle(5, close=99_250.0),
        _candle(6, close=100_000.0),
        _candle(7, close=100_040.0),
        _candle(8, close=100_080.0),
        _candle(9, close=100_120.0),
        _candle(10, close=100_160.0),
        _candle(11, close=100_220.0),
        _candle(12, close=101_200.0),
        _candle(13, close=101_450.0),
        _candle(14, close=101_700.0),
        _candle(15, close=101_900.0),
        _candle(16, close=102_100.0),
        _candle(17, close=102_350.0),
    ]
    regime_rows = {}
    for candle in candles[:6]:
        regime_rows[candle.time.replace(tzinfo=None)] = {
            "structure": {
                "state": "range",
                "trend_direction": "neutral",
                "confidence": 0.58,
                "score_margin": 0.12,
                **_context_fields(state="range", direction="neutral", trust_score=0.58),
            },
            "volatility": {"state": "normal"},
            "liquidity": {"state": "normal"},
            "expansion": {"state": "stable"},
            "confidence": 0.58,
            "regime_key": "range|normal|normal|stable",
        }
    for candle in candles[6:12]:
        regime_rows[candle.time.replace(tzinfo=None)] = {
            "structure": {
                "state": "transition",
                "trend_direction": "neutral",
                "confidence": 0.62,
                "score_margin": 0.11,
                **_context_fields(
                    state="transition_neutral",
                    direction="neutral",
                    trust_score=0.45,
                    is_trustworthy=False,
                ),
            },
            "volatility": {"state": "normal"},
            "liquidity": {"state": "normal"},
            "expansion": {"state": "stable"},
            "confidence": 0.62,
            "regime_key": "transition_neutral|normal|normal|stable",
        }
    for candle in candles[12:]:
        regime_rows[candle.time.replace(tzinfo=None)] = {
            "structure": {
                "state": "trend",
                "trend_direction": "up",
                "confidence": 0.66,
                "score_margin": 0.16,
                **_context_fields(state="trend_up", direction="up", trust_score=0.74),
            },
            "volatility": {"state": "normal"},
            "liquidity": {"state": "normal"},
            "expansion": {"state": "stable"},
            "confidence": 0.66,
            "regime_key": "trend_up|normal|normal|stable",
        }

    overlay = build_regime_overlay(
        candles=candles,
        regime_rows=regime_rows,
        timeframe_seconds=60,
        regime_version="v1",
        include_change_markers=True,
        include_regime_points=True,
    )
    assert overlay is not None
    payload = overlay["payload"]
    transition_box = next(box for box in payload["boxes"] if box["state"] == "transition_neutral")
    local_low = min(candle.low for candle in candles[6:12])
    local_high = max(candle.high for candle in candles[6:12])

    assert transition_box["y1"] > min(candle.low for candle in candles)
    assert transition_box["y1"] < local_low
    assert transition_box["y2"] > local_high
    assert transition_box["y2"] < max(candle.high for candle in candles)


def test_actionable_state_turns_trend_once_known_and_mature() -> None:
    indicator = TypedRegimeIndicator(
        indicator_id="regime-1",
        version="v1",
        params={
            "min_confidence": 0.5,
            "structure_min_confidence": 0.4,
            "structure_confirm_bars": 1,
            "volatility_confirm_bars": 1,
            "liquidity_confirm_bars": 1,
            "expansion_confirm_bars": 1,
            "smoothing_alpha": 1.0,
        },
        candle_stats_indicator_id="stats-1",
    )

    for index in range(14):
        candle = _candle(index, close=100_000.0 + index * 70.0)
        indicator.apply_bar(
            candle,
            {_dependency_ref(): _dependency_output(candle.time, _trend_stats())},
        )

    snapshot = indicator.snapshot()["market_regime"]
    assert snapshot.value["state_key"] == "trend_up"
    assert snapshot.value["fields"]["committed_state"] == "trend"
    assert snapshot.value["fields"]["context_regime_state"] == "trend_up"
    assert snapshot.value["fields"]["context_regime_direction"] == "up"
    assert snapshot.value["fields"]["is_known"] is True
    assert snapshot.value["fields"]["is_mature"] is True
    assert snapshot.value["fields"]["is_trustworthy"] is True
    assert snapshot.value["fields"]["trust_score"] > 0.6


def test_regime_overlay_uses_sparse_block_labels_for_mature_blocks() -> None:
    indicator = TypedRegimeIndicator(
        indicator_id="regime-1",
        version="v1",
        params={
            "min_confidence": 0.5,
            "structure_min_confidence": 0.4,
            "structure_confirm_bars": 1,
            "volatility_confirm_bars": 1,
            "liquidity_confirm_bars": 1,
            "expansion_confirm_bars": 1,
            "smoothing_alpha": 1.0,
        },
        candle_stats_indicator_id="stats-1",
    )

    for index in range(14):
        candle = _candle(index, close=100_000.0 + index * 65.0)
        indicator.apply_bar(
            candle,
            {_dependency_ref(): _dependency_output(candle.time, _trend_stats())},
        )

    overlays = indicator.overlay_snapshot()
    markers = overlays["regime_markers"].value["payload"]["markers"]
    label_markers = [marker for marker in markers if marker["subtype"] == "regime_block_label"]
    known_at_markers = [marker for marker in markers if marker["subtype"] == "regime_known_at"]

    assert known_at_markers
    assert len(label_markers) == 2
    assert any("Transition Up" in marker["text"] for marker in label_markers)
    assert any("Trend Up" in marker["text"] for marker in label_markers)
    trend_label = next(marker for marker in label_markers if "Trend Up" in marker["text"])
    assert trend_label["time"] > min(marker["time"] for marker in known_at_markers)


def test_regime_overlay_emits_compact_label_for_known_mature_block() -> None:
    candles = [_candle(index, close=100_000.0 + index * 10.0) for index in range(8)]
    regime_rows = {}
    for candle in candles:
        regime_rows[candle.time.replace(tzinfo=None)] = {
            "structure": {
                "state": "range",
                "trend_direction": "neutral",
                "confidence": 0.55,
                "score_margin": 0.08,
                "trust_score": 0.52,
                "is_known": True,
                "is_mature": True,
                "is_trustworthy": False,
                "recent_switch_count": 2,
                **_context_fields(
                    state="range",
                    direction="neutral",
                    is_trustworthy=False,
                    trust_score=0.52,
                    recent_switch_count=2,
                ),
            },
            "volatility": {"state": "normal"},
            "liquidity": {"state": "normal"},
            "expansion": {"state": "stable"},
            "confidence": 0.55,
            "regime_key": "range|normal|normal|stable",
        }

    built = build_regime_overlays(
        candles=candles,
        regime_rows=regime_rows,
        timeframe_seconds=60,
        regime_version="v1",
        include_change_markers=True,
        include_marker_overlay=True,
    )
    marker_payload = next(
        overlay["payload"] for overlay in built if overlay["type"] == "regime_markers"
    )
    label_markers = [marker for marker in marker_payload["markers"] if marker["subtype"] == "regime_block_label"]

    assert len(label_markers) == 1
    assert label_markers[0]["text"] == "Range"


def test_regime_overlay_emits_compact_label_for_known_non_trustworthy_block() -> None:
    candles = [_candle(index, close=100_000.0 + index * 8.0) for index in range(4)]
    regime_rows = {}
    for candle in candles:
        regime_rows[candle.time.replace(tzinfo=None)] = {
            "structure": {
                "state": "trend",
                "trend_direction": "down",
                "confidence": 0.46,
                "score_margin": 0.05,
                "trust_score": 0.34,
                "is_known": True,
                "is_mature": False,
                "is_trustworthy": False,
                "recent_switch_count": 4,
                **_context_fields(
                    state="transition_neutral",
                    direction="neutral",
                    is_mature=False,
                    is_trustworthy=False,
                    trust_score=0.34,
                    recent_switch_count=4,
                ),
            },
            "volatility": {"state": "normal"},
            "liquidity": {"state": "normal"},
            "expansion": {"state": "stable"},
            "confidence": 0.46,
            "regime_key": "transition_neutral|normal|normal|stable",
        }

    built = build_regime_overlays(
        candles=candles,
        regime_rows=regime_rows,
        timeframe_seconds=60,
        regime_version="v1",
        include_change_markers=True,
        include_marker_overlay=True,
    )
    marker_payload = next(
        overlay["payload"] for overlay in built if overlay["type"] == "regime_markers"
    )
    label_markers = [marker for marker in marker_payload["markers"] if marker["subtype"] == "regime_block_label"]

    assert len(label_markers) == 1
    assert label_markers[0]["text"] == "Transition"


def test_regime_overlay_uses_context_regime_not_committed_structure() -> None:
    candles = [_candle(index, close=100_000.0 + index * 12.0) for index in range(8)]
    regime_rows = {}
    for candle in candles:
        regime_rows[candle.time.replace(tzinfo=None)] = {
            "structure": {
                "state": "trend",
                "trend_direction": "up",
                "confidence": 0.66,
                "score_margin": 0.16,
                "trust_score": 0.72,
                "is_known": True,
                "is_mature": True,
                "is_trustworthy": True,
                "recent_switch_count": 1,
                **_context_fields(state="range", direction="neutral", trust_score=0.69),
            },
            "volatility": {"state": "normal"},
            "liquidity": {"state": "normal"},
            "expansion": {"state": "stable"},
            "confidence": 0.66,
            "regime_key": "range|normal|normal|stable",
        }

    built = build_regime_overlays(
        candles=candles,
        regime_rows=regime_rows,
        timeframe_seconds=60,
        regime_version="v1",
        include_change_markers=True,
        include_marker_overlay=True,
    )
    regime_payload = next(overlay["payload"] for overlay in built if overlay["type"] == "regime_overlay")
    marker_payload = next(overlay["payload"] for overlay in built if overlay["type"] == "regime_markers")

    assert regime_payload["regime_blocks"][0]["structure"]["state"] == "range"
    assert marker_payload["markers"][-1]["text"] == "Range"
