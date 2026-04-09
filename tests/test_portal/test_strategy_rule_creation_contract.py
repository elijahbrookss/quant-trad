from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.strategies.strategy_service import facade


def test_add_rule_persists_canonical_conditions_payload(monkeypatch) -> None:
    monkeypatch.setattr(facade, "storage_load_strategies", lambda: [])
    registry = facade.StrategyRegistry()
    registry._records["strategy-1"] = facade.StrategyDefinition(
        id="strategy-1",
        name="Breakout",
        instruments=[facade.InstrumentSlot(symbol="ES")],
        timeframe="1m",
        datasource="demo",
        exchange="paper",
    )

    canonical_rule = {
        "id": "rule-1",
        "name": "Breakout Long",
        "intent": "enter_long",
        "priority": 5,
        "trigger": {
            "type": "signal_match",
            "indicator_id": "indicator-1",
            "output_name": "breakout",
            "event_key": "breakout_long",
        },
        "guards": [
            {
                "type": "metric_match",
                "indicator_id": "indicator-1",
                "output_name": "profile_stats",
                "field": "width",
                "operator": ">=",
                "value": 10,
            }
        ],
        "description": "Canonical typed rule",
        "enabled": True,
    }

    persisted: dict[str, object] = {}

    monkeypatch.setattr(facade, "_compile_strategy_definition", lambda *args, **kwargs: (None, {}, {}))
    monkeypatch.setattr(facade, "_validate_rule_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(facade, "_normalize_rule_contract", lambda *args, **kwargs: dict(canonical_rule))
    monkeypatch.setattr(facade, "storage_upsert_strategy", lambda payload: payload)
    monkeypatch.setattr(
        facade,
        "storage_upsert_strategy_rule",
        lambda payload: persisted.update(payload),
    )

    registry.add_rule(
        "strategy-1",
        name="Breakout Long",
        intent="enter_long",
        priority=5,
        trigger={"type": "signal_match", "indicator_id": "indicator-1", "output_name": "breakout", "event_key": "breakout_long"},
        guards=[
            {
                "type": "metric_match",
                "indicator_id": "indicator-1",
                "output_name": "profile_stats",
                "field": "width",
                "operator": ">=",
                "value": 10,
            }
        ],
        description="Canonical typed rule",
        enabled=True,
    )

    assert persisted["strategy_id"] == "strategy-1"
    assert persisted["conditions"] == {
        "intent": "enter_long",
        "priority": 5,
        "trigger": {
            "type": "signal_match",
            "indicator_id": "indicator-1",
            "output_name": "breakout",
            "event_key": "breakout_long",
        },
        "guards": [
            {
                "type": "metric_match",
                "indicator_id": "indicator-1",
                "output_name": "profile_stats",
                "field": "width",
                "operator": ">=",
                "value": 10,
            }
        ],
    }
