from __future__ import annotations

from types import SimpleNamespace

from portal.backend.service.strategies.strategy_service import facade


def test_strategy_clone_owns_rules_and_default_variant_semantics(monkeypatch) -> None:
    source = SimpleNamespace(
        timeframe="1h",
        datasource="source-data",
        exchange="source-exchange",
        indicator_ids=["indicator-1"],
        atm_template_id="atm-1",
        risk_config={"max_risk": 0.01},
        rules={
            "rule-1": SimpleNamespace(
                id="rule-1",
                name="Long",
                intent="enter_long",
                priority=1,
                trigger={"type": "always"},
                guards=[],
                description=None,
                enabled=True,
            )
        },
    )
    monkeypatch.setattr(facade._REGISTRY, "get", lambda strategy_id: source)
    variants = {
        "source": [
            {
                "id": "source-default",
                "name": "default",
                "output_filters": [],
                "is_default": True,
            },
            {
                "id": "source-alt",
                "name": "alternate",
                "output_filters": [{"output": "signal"}],
                "is_default": False,
            },
        ],
        "target": [
            {
                "id": "target-default",
                "name": "default",
                "output_filters": [],
                "is_default": True,
            }
        ],
    }
    monkeypatch.setattr(
        facade,
        "list_strategy_variants",
        lambda strategy_id: variants["source" if strategy_id == "source-1" else "target"],
    )
    create_payloads = []
    monkeypatch.setattr(
        facade,
        "create_strategy",
        lambda name, **kwargs: create_payloads.append({"name": name, **kwargs})
        or {"id": "target-1"},
    )
    rules = []
    monkeypatch.setattr(
        facade,
        "create_rule",
        lambda strategy_id, **kwargs: rules.append(
            {"strategy_id": strategy_id, **kwargs}
        ),
    )
    updates = []
    monkeypatch.setattr(
        facade,
        "update_strategy_variant",
        lambda strategy_id, variant_id, **kwargs: updates.append(
            {"strategy_id": strategy_id, "variant_id": variant_id, **kwargs}
        )
        or {"id": variant_id},
    )
    creates = []
    monkeypatch.setattr(
        facade,
        "create_strategy_variant",
        lambda strategy_id, **kwargs: creates.append(
            {"strategy_id": strategy_id, **kwargs}
        )
        or {"id": "target-alt"},
    )
    monkeypatch.setattr(
        facade,
        "get_strategy",
        lambda strategy_id: {"id": strategy_id, "name": "Clone"},
    )

    result = facade.clone_strategy(
        "source-1",
        name="Clone",
        symbols=[{"symbol": "ETH-USD"}],
        datasource="target-data",
        exchange="target-exchange",
    )

    assert create_payloads[0]["indicator_ids"] == ["indicator-1"]
    assert create_payloads[0]["atm_template_id"] == "atm-1"
    assert rules == [
        {
            "strategy_id": "target-1",
            "name": "Long",
            "intent": "enter_long",
            "priority": 1,
            "trigger": {"type": "always"},
            "guards": [],
            "description": None,
            "enabled": True,
        }
    ]
    assert updates[0]["variant_id"] == "target-default"
    assert updates[0]["is_default"] is True
    assert creates[0]["name"] == "alternate"
    assert result["variant_id_by_source_id"] == {
        "source-default": "target-default",
        "source-alt": "target-alt",
    }
