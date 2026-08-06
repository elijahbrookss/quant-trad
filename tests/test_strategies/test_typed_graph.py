from __future__ import annotations

from copy import deepcopy

import pytest

from strategies.typed_graph import (
    TYPED_STRATEGY_GRAPH_VERSION,
    TypedStrategyGraph,
    compile_typed_strategy_graph,
)


def _payload() -> dict:
    return {
        "schema_version": TYPED_STRATEGY_GRAPH_VERSION,
        "graph_id": "graph:momentum:1",
        "family_id": "family:momentum",
        "protocol_hash": "a" * 64,
        "timeframe": "5m",
        "facts": [
            {"name": "indicator.rsi.value", "value_type": "number"},
            {"name": "position.is_flat", "value_type": "boolean"},
            {"name": "risk.entry_allowed", "value_type": "boolean"},
        ],
        "rules": [
            {
                "rule_id": "enter-oversold",
                "priority": 100,
                "condition": {
                    "op": "all",
                    "args": [
                        {
                            "op": "lt",
                            "args": [
                                {"op": "fact", "name": "indicator.rsi.value"},
                                {"op": "const", "value_type": "number", "value": 30},
                            ],
                        },
                        {"op": "fact", "name": "position.is_flat"},
                        {"op": "fact", "name": "risk.entry_allowed"},
                    ],
                },
                "action": "enter",
                "side": "long",
                "sizing": {"mode": "risk_budget_fraction", "value": 0.01},
                "execution": {
                    "style": "passive_limit",
                    "time_in_force": "gtc",
                    "expiration_bars": 2,
                    "price_offset_bps": -1,
                    "chase_limit": 1,
                    "stage_count": 1,
                },
            }
        ],
        "risk": {
            "max_position_notional": 10000,
            "max_risk_fraction": 0.02,
            "allow_short": False,
        },
        "parent_graph_ids": [],
        "created_by": "agent:researcher",
    }


def test_typed_graph_compiles_and_emits_canonical_policy_deterministically() -> None:
    graph = TypedStrategyGraph.from_dict(_payload())
    compiled = compile_typed_strategy_graph(graph)
    repeated = compile_typed_strategy_graph(
        TypedStrategyGraph.from_dict(graph.to_dict())
    )
    assert repeated.compiled_hash == compiled.compiled_hash
    intent, trace = compiled.evaluate(
        {
            "indicator.rsi.value": 25.0,
            "position.is_flat": True,
            "risk.entry_allowed": True,
        }
    )
    assert intent is not None
    assert intent.action == "enter"
    assert intent.execution_policy["order_type"] == "limit_resting"
    assert trace["evaluations"] == [
        {"rule_id": "enter-oversold", "matched": True, "selected": True}
    ]


def test_typed_graph_rejects_undeclared_facts_and_unsafe_capabilities() -> None:
    undeclared = _payload()
    undeclared["rules"][0]["condition"] = {
        "op": "fact",
        "name": "market.future_close",
    }
    with pytest.raises(ValueError, match="undeclared fact"):
        TypedStrategyGraph.from_dict(undeclared)

    unsafe = _payload()
    unsafe["shell"] = "submit-live-order"
    with pytest.raises(ValueError, match="capability is forbidden"):
        TypedStrategyGraph.from_dict(unsafe)


def test_typed_graph_hash_detects_mutation_and_runtime_fact_types_are_exact() -> None:
    graph = TypedStrategyGraph.from_dict(_payload())
    tampered = deepcopy(graph.to_dict())
    tampered["risk"]["max_position_notional"] = 99999
    with pytest.raises(ValueError, match="typed_strategy_graph_hash_mismatch"):
        TypedStrategyGraph.from_dict(tampered)
    compiled = compile_typed_strategy_graph(graph)
    with pytest.raises(ValueError, match="runtime fact type mismatch"):
        compiled.evaluate(
            {
                "indicator.rsi.value": "25",
                "position.is_flat": True,
                "risk.entry_allowed": True,
            }
        )


def test_hold_and_cancel_cannot_smuggle_execution_or_sizing() -> None:
    payload = _payload()
    payload["rules"][0]["action"] = "cancel"
    with pytest.raises(ValueError, match="cancel cannot carry"):
        TypedStrategyGraph.from_dict(payload)
