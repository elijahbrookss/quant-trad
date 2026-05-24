from __future__ import annotations

from typing import Any

import pytest

from strategies.compiler import IndicatorMetaGetter, compile_strategy
from strategies.contracts import MetricMatchSpec
from strategies.template import ParamSpec, StrategyTemplate


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


def _template(
    *,
    rules: dict[str, Any] | None = None,
    param_specs: tuple[ParamSpec, ...] | None = None,
) -> StrategyTemplate:
    return StrategyTemplate(
        template_id="template-1",
        name="Template 1",
        timeframe="1m",
        rules=rules or _rules(_metric_guard(value="$params.min_atr_z")),
        param_specs=param_specs
        if param_specs is not None
        else (ParamSpec(key="min_atr_z", type="float", default=1.5),),
    )


def test_instantiate_with_no_overrides_uses_all_defaults() -> None:
    template = _template()

    rules, params = template.instantiate()

    assert params == {"min_atr_z": 1.5}
    assert rules["r1"]["guards"][0]["value"] == "$params.min_atr_z"


def test_instantiate_with_overrides_applies_them_over_defaults() -> None:
    template = _template()

    _, params = template.instantiate(overrides={"min_atr_z": 2.0})

    assert params["min_atr_z"] == 2.0


def test_instantiate_output_feeds_compile_strategy_and_produces_correct_compiled_spec() -> None:
    template = _template()
    rules, params = template.instantiate(overrides={"min_atr_z": 2.25})

    spec = compile_strategy(
        strategy_id="s1",
        timeframe=template.timeframe,
        rules=rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params=params,
    )

    guard = spec.rules[0].guards[0]
    assert isinstance(guard, MetricMatchSpec)
    assert guard.value == 2.25


def test_instantiate_with_no_param_specs_returns_empty_params_and_passes_rules_through() -> None:
    literal_rules = _rules(_metric_guard(value=1.5))
    template = StrategyTemplate(
        template_id="template-1",
        name="Template 1",
        timeframe="1m",
        rules=literal_rules,
        param_specs=(),
    )

    rules, params = template.instantiate()

    assert rules == literal_rules
    assert rules is not literal_rules
    assert params == {}


def test_instantiate_raises_value_error_for_unknown_override_key() -> None:
    template = _template()

    with pytest.raises(ValueError, match="Unknown parameter override"):
        template.instantiate(overrides={"unknown_key": 2.0})


def test_instantiate_coerces_int_override_to_float_param_type() -> None:
    template = StrategyTemplate(
        template_id="template-1",
        name="Template 1",
        timeframe="1m",
        rules=_rules(_metric_guard(value="$params.x")),
        param_specs=(ParamSpec(key="x", type="float", default=1.0),),
    )

    _, params = template.instantiate(overrides={"x": 2})

    assert params["x"] == 2.0
    assert isinstance(params["x"], float)


def test_instantiate_raises_value_error_for_uncoercible_override_value() -> None:
    template = StrategyTemplate(
        template_id="template-1",
        name="Template 1",
        timeframe="1m",
        rules=_rules(_metric_guard(value="$params.x")),
        param_specs=(ParamSpec(key="x", type="float", default=1.0),),
    )

    with pytest.raises(ValueError, match="Cannot coerce"):
        template.instantiate(overrides={"x": "not_a_float"})


def test_strategy_template_raises_value_error_for_duplicate_param_keys() -> None:
    with pytest.raises(ValueError, match="Duplicate param key"):
        StrategyTemplate(
            template_id="template-1",
            name="Template 1",
            timeframe="1m",
            rules=_rules(_metric_guard(value="$params.x")),
            param_specs=(
                ParamSpec(key="x", type="float", default=1.0),
                ParamSpec(key="x", type="float", default=2.0),
            ),
        )


def test_param_spec_raises_value_error_for_invalid_key_formats() -> None:
    with pytest.raises(ValueError):
        ParamSpec(key="bad key!", type="float", default=1.0)
    with pytest.raises(ValueError):
        ParamSpec(key="1starts_with_digit", type="float", default=1.0)


def test_param_spec_raises_value_error_when_default_cannot_be_coerced() -> None:
    with pytest.raises(ValueError):
        ParamSpec(key="x", type="float", default="not_a_float")


def test_two_templates_with_same_structure_but_different_defaults_produce_different_compiled_specs() -> None:
    rules = _rules(_metric_guard(value="$params.min_atr_z"))
    template_a = StrategyTemplate(
        template_id="template-a",
        name="Template A",
        timeframe="1m",
        rules=rules,
        param_specs=(ParamSpec(key="min_atr_z", type="float", default=1.0),),
    )
    template_b = StrategyTemplate(
        template_id="template-b",
        name="Template B",
        timeframe="1m",
        rules=rules,
        param_specs=(ParamSpec(key="min_atr_z", type="float", default=2.0),),
    )
    rules_a, params_a = template_a.instantiate()
    rules_b, params_b = template_b.instantiate()

    spec_a = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules_a,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params=params_a,
    )
    spec_b = compile_strategy(
        strategy_id="s1",
        timeframe="1m",
        rules=rules_b,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params=params_b,
    )

    guard_a = spec_a.rules[0].guards[0]
    guard_b = spec_b.rules[0].guards[0]
    assert isinstance(guard_a, MetricMatchSpec)
    assert isinstance(guard_b, MetricMatchSpec)
    assert guard_a.value != guard_b.value


def test_instantiate_overrides_none_is_identical_to_overrides_empty_dict() -> None:
    template = _template()

    instantiated_with_none = template.instantiate(overrides=None)
    instantiated_with_empty = template.instantiate(overrides={})

    assert instantiated_with_none == instantiated_with_empty
