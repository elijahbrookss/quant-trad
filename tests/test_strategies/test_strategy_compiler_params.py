from __future__ import annotations

from typing import Any

import pytest

from strategies.compiler import IndicatorMetaGetter, compile_strategy
from strategies.contracts import ContextMatchSpec, HoldsForBarsSpec, MetricMatchSpec


_SIGNAL_OUTPUT = {"name": "sig", "type": "signal", "event_keys": ["breakout_long"]}
_METRIC_OUTPUT = {"name": "metrics", "type": "metric", "fields": ["atr_zscore", "trend_score"]}
_CONTEXT_OUTPUT = {
    "name": "regime",
    "type": "context",
    "state_keys": ["trend_up", "range"],
    "fields": ["regime_confidence"],
}


def _make_meta_getter(outputs: list[dict[str, Any]]) -> IndicatorMetaGetter:
    def getter(indicator_id: str) -> dict[str, Any]:
        _ = indicator_id
        return {"typed_outputs": outputs}

    return getter


def _trigger() -> dict[str, Any]:
    return {
        "type": "signal_match",
        "indicator_id": "ind-1",
        "output_name": "sig",
        "event_key": "breakout_long",
    }


def _metric_guard(*, field: str = "atr_zscore", value: Any = 1.5, operator: str = ">") -> dict[str, Any]:
    return {
        "type": "metric_match",
        "indicator_id": "ind-1",
        "output_name": "metrics",
        "field": field,
        "operator": operator,
        "value": value,
    }


def _context_guard(*, field: str = "state", value: Any = "trend_up") -> dict[str, Any]:
    return {
        "type": "context_match",
        "indicator_id": "ind-1",
        "output_name": "regime",
        "field": field,
        "value": value,
    }


def _holds_for_bars_guard(*, bars: int = 3, guard: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "holds_for_bars",
        "bars": bars,
        "guard": guard,
    }


def _rules(*guards: dict[str, Any]) -> dict[str, Any]:
    return {
        "r1": {
            "id": "r1",
            "name": "test",
            "intent": "enter_long",
            "trigger": _trigger(),
            "guards": list(guards),
        }
    }


def test_existing_literal_strategy_still_compiles() -> None:
    rules = _rules(_metric_guard(value=1.5))

    spec = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
    )

    guard = spec.rules[0].guards[0]
    assert isinstance(guard, MetricMatchSpec)
    assert guard.value == 1.5


def test_parameterized_metric_match_resolves_correctly() -> None:
    rules = _rules(_metric_guard(value="$params.min_atr_z"))

    spec = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params={"min_atr_z": 1.0},
    )

    guard = spec.rules[0].guards[0]
    assert isinstance(guard, MetricMatchSpec)
    assert guard.value == 1.0


def test_multiple_params_in_one_strategy_resolve_independently() -> None:
    rules = _rules(
        _metric_guard(value="$params.min_atr_z"),
        _context_guard(value="$params.target_regime"),
    )

    spec = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT, _CONTEXT_OUTPUT]),
        params={"min_atr_z": 1.25, "target_regime": "trend_up"},
    )

    metric_guard = spec.rules[0].guards[0]
    context_guard = spec.rules[0].guards[1]
    assert isinstance(metric_guard, MetricMatchSpec)
    assert metric_guard.value == 1.25
    assert isinstance(context_guard, ContextMatchSpec)
    assert context_guard.value == ("trend_up",)


def test_nested_param_ref_in_holds_for_bars_resolves_correctly() -> None:
    rules = _rules(
        _holds_for_bars_guard(
            bars=3,
            guard=_metric_guard(field="trend_score", value="$params.min_trend"),
        )
    )

    spec = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params={"min_trend": 0.65},
    )

    guard = spec.rules[0].guards[0]
    assert isinstance(guard, HoldsForBarsSpec)
    assert isinstance(guard.guard, MetricMatchSpec)
    assert guard.guard.value == 0.65


def test_reference_to_undefined_parameter_raises_value_error() -> None:
    rules = _rules(_metric_guard(value="$params.min_atr_z"))

    with pytest.raises(ValueError, match="undefined parameter"):
        compile_strategy(
            strategy_id="s1",
            timeframe="1m",
            rules=rules,
            attached_indicator_ids=["ind-1"],
            indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
            params={},
        )


def test_compile_strategy_with_no_params_and_no_refs_is_identical_to_before() -> None:
    rules = _rules(_metric_guard(value=1.5))

    without_params_kwarg = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
    )
    with_params_none = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params=None,
    )

    assert without_params_kwarg == with_params_none


def test_compile_strategy_with_params_none_and_ref_raises_value_error() -> None:
    rules = _rules(_metric_guard(value="$params.min_atr_z"))

    with pytest.raises(ValueError, match="undefined parameter"):
        compile_strategy(
            strategy_id="s1",
            timeframe="1m",
            rules=rules,
            attached_indicator_ids=["ind-1"],
            indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
            params=None,
        )


def test_invalid_param_key_in_params_dict_raises_value_error() -> None:
    rules = _rules(_metric_guard(value="$params.min_atr_z"))

    with pytest.raises(ValueError, match="Invalid parameter key"):
        compile_strategy(
            strategy_id="s1",
            timeframe="1m",
            rules=rules,
            attached_indicator_ids=["ind-1"],
            indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
            params={"bad key!": 1.0},
        )


def test_unused_param_keys_are_silently_ignored() -> None:
    rules = _rules(_metric_guard(value="$params.min_atr_z"))

    spec = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params={"min_atr_z": 1.0, "unused_key": 99.9},
    )

    guard = spec.rules[0].guards[0]
    assert isinstance(guard, MetricMatchSpec)
    assert guard.value == 1.0


def test_compiled_spec_from_params_matches_literal_compiled_spec() -> None:
    literal_rules = _rules(_metric_guard(value=1.0))
    param_rules = _rules(_metric_guard(value="$params.x"))

    literal_spec = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=literal_rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
    )
    param_spec = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=param_rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params={"x": 1.0},
    )

    assert literal_spec == param_spec
