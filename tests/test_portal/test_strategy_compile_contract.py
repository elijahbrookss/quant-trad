from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.strategies.strategy_service import facade


def _strategy_record() -> SimpleNamespace:
    return SimpleNamespace(
        id="strategy-1",
        name="Strategy One",
        timeframe="15m",
        indicator_ids=["indicator-1"],
        rules={
            "rule-1": SimpleNamespace(
                to_dict=lambda: {
                    "id": "rule-1",
                    "name": "Breakout Long",
                    "intent": "enter_long",
                    "priority": 1,
                    "trigger": {
                        "type": "signal_match",
                        "indicator_id": "indicator-1",
                        "output_name": "balance_breakout",
                        "event_key": "breakout_long",
                    },
                    "guards": [],
                    "enabled": True,
                }
            )
        },
    )


def test_compile_strategy_contract_materializes_selected_variant_output_filters(monkeypatch) -> None:
    captured: dict[str, object] = {}
    record = _strategy_record()

    monkeypatch.setattr(facade._REGISTRY, "get", lambda strategy_id: record)  # type: ignore[attr-defined]
    monkeypatch.setattr(
        facade,
        "storage_ensure_default_strategy_variant",
        lambda strategy_id: {
            "id": "variant-default",
            "strategy_id": strategy_id,
            "name": "default",
            "output_filters": [],
            "is_default": True,
        },
    )
    monkeypatch.setattr(
        facade,
        "storage_get_strategy_variant",
        lambda variant_id: {
            "id": variant_id,
            "strategy_id": "strategy-1",
            "name": "expanding-only",
            "output_filters": [
                {
                    "scope": {"intent": ["enter_long"]},
                    "indicator_id": "indicator-1",
                    "output_name": "market_state",
                    "field": "expansion_state",
                    "operator": "equals",
                    "value": "expanding",
                }
            ],
            "is_default": False,
        },
    )

    def _compile_strategy(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            strategy_id=kwargs["strategy_id"],
            timeframe=kwargs["timeframe"],
            strategy_hash="hash-1",
            max_history_bars=3,
            rules=(
                SimpleNamespace(
                    id="rule-1",
                    name="Breakout Long",
                    intent="enter_long",
                    priority=1,
                    enabled=True,
                    description=None,
                    trigger=SimpleNamespace(
                        type="signal_match",
                        indicator_id="indicator-1",
                        output_name="balance_breakout",
                        output_key="indicator-1.balance_breakout",
                        event_key="breakout_long",
                    ),
                    guards=(),
                ),
            ),
        )

    monkeypatch.setattr(facade, "compile_strategy", _compile_strategy)

    payload = facade.compile_strategy_contract("strategy-1", variant_id="variant-1")

    assert captured["params"] == {}
    captured_guard = captured["rules"][0]["guards"][0]
    assert captured_guard | {"source": {}} == {
        "type": "context_match",
        "indicator_id": "indicator-1",
        "output_name": "market_state",
        "field": "expansion_state",
        "value": "expanding",
        "source": {},
    }
    assert captured_guard["source"]["type"] == "variant_output_filter"
    assert captured_guard["source"]["filter_index"] == 0
    assert payload["strategy_hash"] == "hash-1"
    assert payload["variant"] == {
        "id": "variant-1",
        "name": "expanding-only",
        "description": None,
        "output_filters": [
            {
                "scope": {"intent": ["enter_long"]},
                "indicator_id": "indicator-1",
                "output_name": "market_state",
                "field": "expansion_state",
                "operator": "equals",
                "value": "expanding",
            }
        ],
        "is_default": False,
    }
    assert payload["compiled"]["rule_count"] == 1


def test_compile_strategy_contract_can_select_variant_by_name(monkeypatch) -> None:
    captured: dict[str, object] = {}
    record = _strategy_record()

    monkeypatch.setattr(facade._REGISTRY, "get", lambda strategy_id: record)  # type: ignore[attr-defined]
    monkeypatch.setattr(
        facade,
        "storage_ensure_default_strategy_variant",
        lambda strategy_id: {
            "id": "variant-default",
            "strategy_id": strategy_id,
            "name": "default",
            "output_filters": [],
            "is_default": True,
        },
    )
    monkeypatch.setattr(
        facade,
        "storage_list_strategy_variants",
        lambda strategy_id: [
            {
                "id": "variant-default",
                "strategy_id": strategy_id,
                "name": "default",
                "output_filters": [],
                "is_default": True,
            },
            {
                "id": "variant-expanding",
                "strategy_id": strategy_id,
                "name": "expanding-only",
                "output_filters": [
                    {
                        "scope": {"intent": ["enter_long"]},
                        "indicator_id": "indicator-1",
                        "output_name": "market_state",
                        "field": "expansion_state",
                        "operator": "equals",
                        "value": "expanding",
                    }
                ],
                "is_default": False,
            },
        ],
    )

    def _compile_strategy(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            strategy_id=kwargs["strategy_id"],
            timeframe=kwargs["timeframe"],
            strategy_hash="hash-1",
            max_history_bars=0,
            rules=(),
        )

    monkeypatch.setattr(facade, "compile_strategy", _compile_strategy)

    payload = facade.compile_strategy_contract("strategy-1", variant_name="expanding-only")

    assert payload["variant"]["id"] == "variant-expanding"
    assert captured["rules"][0]["guards"][0]["field"] == "expansion_state"


def test_compiled_context_guard_serialization_does_not_emit_tuple_repr() -> None:
    payload = facade._serialize_guard(  # type: ignore[attr-defined]
        {
            "type": "context_match",
            "indicator_id": "regime-1",
            "output_name": "market_regime",
            "field": "expansion_state",
            "value": ("expanding",),
        }
    )

    assert payload["value"] == "expanding"


def test_create_strategy_variant_validates_output_filters_against_compile(monkeypatch) -> None:
    record = _strategy_record()

    monkeypatch.setattr(facade._REGISTRY, "get", lambda strategy_id: record)  # type: ignore[attr-defined]
    monkeypatch.setattr(
        facade,
        "storage_ensure_default_strategy_variant",
        lambda strategy_id: {
            "id": "variant-default",
            "strategy_id": strategy_id,
            "name": "default",
            "output_filters": [],
            "is_default": True,
        },
    )
    saved: dict[str, object] = {}

    def _compile_strategy(**kwargs):
        saved["rules"] = kwargs["rules"]
        return SimpleNamespace(
            strategy_id=kwargs["strategy_id"],
            timeframe=kwargs["timeframe"],
            strategy_hash="hash-1",
            max_history_bars=0,
            rules=(),
        )

    def _upsert_variant(payload):
        saved["payload"] = payload
        now = datetime.utcnow().isoformat() + "Z"
        return {
            "id": "variant-1",
            "created_at": now,
            "updated_at": now,
            **payload,
        }

    monkeypatch.setattr(facade, "compile_strategy", _compile_strategy)
    monkeypatch.setattr(facade, "storage_upsert_strategy_variant", _upsert_variant)

    payload = facade.create_strategy_variant(
        "strategy-1",
        name="expanding-only",
        output_filters=[
            {
                "scope": {"intent": ["enter_long"]},
                "indicator_id": "indicator-1",
                "output_name": "market_state",
                "field": "expansion_state",
                "operator": "equals",
                "value": "expanding",
            }
        ],
    )

    assert saved["rules"][0]["guards"][0]["field"] == "expansion_state"
    assert payload["name"] == "expanding-only"
