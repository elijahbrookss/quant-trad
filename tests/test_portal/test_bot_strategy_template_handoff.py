from __future__ import annotations

from typing import Any

from engines.bot_runtime.strategy.models import (
    Strategy,
    StrategyIndicatorLink,
    StrategyInstrumentLink,
)
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


def _template() -> StrategyTemplate:
    return StrategyTemplate(
        template_id="regime-template",
        name="Regime Template",
        timeframe="1m",
        rules=_rules(_metric_guard(value="$params.conviction_min")),
        param_specs=(ParamSpec(key="conviction_min", type="float", default=0.6),),
        variants={
            "aggressive": {"conviction_min": 0.5},
            "conservative": {"conviction_min": 0.7},
        },
    )


def _strategy_links() -> tuple[list[StrategyIndicatorLink], list[StrategyInstrumentLink]]:
    return (
        [
            StrategyIndicatorLink(
                id="link-1",
                strategy_id="strategy-1",
                indicator_id="ind-1",
            )
        ],
        [
            StrategyInstrumentLink(
                id="inst-link-1",
                strategy_id="strategy-1",
                instrument_id="instrument-1",
                instrument_snapshot={"symbol": "BTC/USDT"},
            )
        ],
    )


def test_bot_strategy_can_be_materialized_from_template_defaults_only() -> None:
    indicator_links, instrument_links = _strategy_links()

    strategy = Strategy.from_template(
        id="strategy-1",
        name="Template Strategy",
        datasource="demo",
        exchange="demo",
        template=_template(),
        indicator_links=indicator_links,
        instrument_links=instrument_links,
    )

    assert strategy.template_id == "regime-template"
    assert strategy.variant_name is None
    assert strategy.resolved_params == {"conviction_min": 0.6}


def test_bot_strategy_can_be_materialized_from_named_variant() -> None:
    indicator_links, instrument_links = _strategy_links()

    strategy = Strategy.from_template(
        id="strategy-1",
        name="Template Strategy",
        datasource="demo",
        exchange="demo",
        template=_template(),
        indicator_links=indicator_links,
        instrument_links=instrument_links,
        variant_name="aggressive",
    )

    assert strategy.variant_name == "aggressive"
    assert strategy.resolved_params == {"conviction_min": 0.5}


def test_explicit_overrides_beat_variant_and_default_values() -> None:
    indicator_links, instrument_links = _strategy_links()

    strategy = Strategy.from_template(
        id="strategy-1",
        name="Template Strategy",
        datasource="demo",
        exchange="demo",
        template=_template(),
        indicator_links=indicator_links,
        instrument_links=instrument_links,
        variant_name="aggressive",
        param_overrides={"conviction_min": 0.55},
    )

    assert strategy.resolved_params == {"conviction_min": 0.55}


def test_bot_side_strategy_config_compiles_through_existing_path_unchanged() -> None:
    indicator_links, instrument_links = _strategy_links()
    strategy = Strategy.from_template(
        id="strategy-1",
        name="Template Strategy",
        datasource="demo",
        exchange="demo",
        template=_template(),
        indicator_links=indicator_links,
        instrument_links=instrument_links,
        variant_name="conservative",
    )
    rules, params = strategy.compilation_inputs()

    spec = compile_strategy(
        strategy_id=strategy.id,
        timeframe=strategy.timeframe,
        rules=list(rules.values()),
        attached_indicator_ids=strategy.indicator_ids,
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params=params,
    )

    guard = spec.rules[0].guards[0]
    assert isinstance(guard, MetricMatchSpec)
    assert guard.value == 0.7


def test_existing_non_template_strategy_flow_still_compiles() -> None:
    strategy = Strategy(
        id="strategy-1",
        name="Literal Strategy",
        timeframe="1m",
        datasource="demo",
        exchange="demo",
        atm_template_id=None,
        atm_template={},
        risk_config={},
        indicator_links=[
            StrategyIndicatorLink(
                id="link-1",
                strategy_id="strategy-1",
                indicator_id="ind-1",
            )
        ],
        instrument_links=[],
        rules=_rules(_metric_guard(value=1.5)),
    )
    rules, params = strategy.compilation_inputs()

    spec = compile_strategy(
        strategy_id=strategy.id,
        timeframe=strategy.timeframe,
        rules=list(rules.values()),
        attached_indicator_ids=strategy.indicator_ids,
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT, _METRIC_OUTPUT]),
        params=params,
    )

    guard = spec.rules[0].guards[0]
    assert isinstance(guard, MetricMatchSpec)
    assert params == {}
    assert guard.value == 1.5


def test_template_strategy_to_dict_preserves_runtime_shape_and_provenance() -> None:
    indicator_links, instrument_links = _strategy_links()
    strategy = Strategy.from_template(
        id="strategy-1",
        name="Template Strategy",
        datasource="demo",
        exchange="demo",
        template=_template(),
        indicator_links=indicator_links,
        instrument_links=instrument_links,
        variant_name="aggressive",
    )

    payload = strategy.to_dict()

    assert payload["rules"]["r1"]["guards"][0]["value"] == "$params.conviction_min"
    assert payload["template_id"] == "regime-template"
    assert payload["variant_name"] == "aggressive"
    assert payload["resolved_params"] == {"conviction_min": 0.5}
