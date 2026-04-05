from __future__ import annotations

from typing import Any

import pytest

from strategies.compiler import IndicatorMetaGetter, compile_strategy
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


def _template(*, variants: dict[str, dict[str, Any]] | None = None) -> StrategyTemplate:
    return StrategyTemplate(
        template_id="template-1",
        name="Template 1",
        timeframe="1m",
        rules=_rules(_metric_guard(value="$params.conviction_min")),
        param_specs=(
            ParamSpec(key="conviction_min", type="float", default=0.6),
            ParamSpec(key="trend_floor", type="float", default=0.75),
        ),
        variants=variants
        if variants is not None
        else {
            "aggressive": {"conviction_min": 0.5},
            "conservative": {"conviction_min": 0.7},
        },
    )


def test_variant_resolves_correctly_from_defaults() -> None:
    template = _template()

    rules, params = template.instantiate_variant("aggressive")

    assert rules["r1"]["guards"][0]["value"] == "$params.conviction_min"
    assert params == {"conviction_min": 0.5, "trend_floor": 0.75}


def test_variant_overrides_take_precedence_over_variant_values() -> None:
    template = _template()

    _, params = template.instantiate_variant("aggressive", overrides={"conviction_min": 0.55})

    assert params["conviction_min"] == 0.55
    assert params["trend_floor"] == 0.75


def test_unknown_variant_raises_error() -> None:
    template = _template()

    with pytest.raises(ValueError, match="Unknown strategy variant"):
        template.instantiate_variant("unknown")


def test_invalid_param_keys_raise_error() -> None:
    template = _template()

    with pytest.raises(ValueError, match="Unknown parameter override"):
        template.instantiate_variant("aggressive", overrides={"unknown_key": 1.0})


def test_variant_instantiation_produces_same_params_as_manual_override() -> None:
    template = _template()
    variant_rules, variant_params = template.instantiate_variant("aggressive")
    manual_rules, manual_params = template.instantiate(overrides={"conviction_min": 0.5})

    assert variant_rules == manual_rules
    assert variant_params == manual_params

    variant_spec = compile_strategy(
        strategy_id="s1",
        timeframe=template.timeframe,
        rules=variant_rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params=variant_params,
    )
    manual_spec = compile_strategy(
        strategy_id="s1",
        timeframe=template.timeframe,
        rules=manual_rules,
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params=manual_params,
    )

    assert variant_spec == manual_spec


def test_existing_instantiate_behavior_unchanged() -> None:
    template = _template()

    rules, params = template.instantiate()

    assert rules["r1"]["guards"][0]["value"] == "$params.conviction_min"
    assert params == {"conviction_min": 0.6, "trend_floor": 0.75}
