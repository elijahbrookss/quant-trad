from __future__ import annotations

from types import SimpleNamespace

from portal.backend.service.strategy_variant_resolution import (
    materialize_output_filters,
    resolve_strategy_variant,
)


def test_resolve_strategy_variant_preserves_output_filters_and_hashes_stably() -> None:
    strategy = SimpleNamespace(id="strategy-1", name="Strategy 1", timeframe="15m")
    selected_variant = {
        "id": "variant-expanding",
        "strategy_id": "strategy-1",
        "name": "expanding-only",
        "output_filters": [
            {
                "scope": {"intent": ["enter_long", "enter_short"]},
                "indicator_id": "regime-1",
                "output_name": "market_regime",
                "field": "expansion_state",
                "operator": "equals",
                "value": "expanding",
            }
        ],
        "is_default": False,
    }

    left = resolve_strategy_variant(strategy, selected_variant)
    right = resolve_strategy_variant(
        strategy,
        {
            **selected_variant,
            "output_filters": [
                {
                    "operator": "equals",
                    "field": "expansion_state",
                    "output_name": "market_regime",
                    "value": "expanding",
                    "indicator_id": "regime-1",
                    "scope": {"intent": ["enter_long", "enter_short"]},
                }
            ],
        },
    )

    assert left.output_filters == selected_variant["output_filters"]
    assert left.to_run_strategy_snapshot()["output_filters"] == selected_variant["output_filters"]
    assert left.effective_strategy_config_hash == right.effective_strategy_config_hash


def test_materialize_output_filters_appends_context_guard_by_intent_scope() -> None:
    rules = [
        {
            "id": "rule-long",
            "name": "Long",
            "intent": "enter_long",
            "trigger": {"type": "signal_match"},
            "guards": [],
        },
        {
            "id": "rule-short",
            "name": "Short",
            "intent": "enter_short",
            "trigger": {"type": "signal_match"},
            "guards": [],
        },
        {
            "id": "rule-other",
            "name": "Other",
            "intent": "exit",
            "trigger": {"type": "signal_match"},
            "guards": [],
        },
    ]

    materialized = materialize_output_filters(
        rules,
        [
            {
                "scope": {"intent": ["enter_long", "enter_short"]},
                "indicator_id": "regime-1",
                "output_name": "market_regime",
                "field": "expansion_state",
                "operator": "equals",
                "value": "expanding",
            }
        ],
    )

    assert materialized[0]["guards"][0] | {"source": {}} == {
        "type": "context_match",
        "indicator_id": "regime-1",
        "output_name": "market_regime",
        "field": "expansion_state",
        "value": "expanding",
        "source": {},
    }
    assert materialized[1]["guards"][0] | {"source": {}} == {
        "type": "context_match",
        "indicator_id": "regime-1",
        "output_name": "market_regime",
        "field": "expansion_state",
        "value": "expanding",
        "source": {},
    }
    assert materialized[0]["guards"][0]["source"]["type"] == "variant_output_filter"
    assert materialized[0]["guards"][0]["source"]["filter_index"] == 0
    assert materialized[0]["guards"][0]["source"]["filter_hash"]
    assert materialized[2]["guards"] == []


def test_materialize_output_filters_dedupes_by_guard_semantics_not_trace_source() -> None:
    rules = [
        {
            "id": "rule-long",
            "name": "Long",
            "intent": "enter_long",
            "trigger": {"type": "signal_match"},
            "guards": [
                {
                    "type": "context_match",
                    "indicator_id": "regime-1",
                    "output_name": "market_regime",
                    "field": "expansion_state",
                    "value": "expanding",
                }
            ],
        }
    ]
    output_filter = {
        "scope": {"intent": ["enter_long"]},
        "indicator_id": "regime-1",
        "output_name": "market_regime",
        "field": "expansion_state",
        "operator": "equals",
        "value": "expanding",
    }

    materialized = materialize_output_filters(rules, [output_filter, output_filter])

    assert len(materialized[0]["guards"]) == 1
