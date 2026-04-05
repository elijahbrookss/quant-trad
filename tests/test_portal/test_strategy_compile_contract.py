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


def test_compile_strategy_contract_uses_default_plus_selected_variant(monkeypatch) -> None:
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
            "param_overrides": {"conviction_min": 0.5, "risk_multiple": 1.0},
            "is_default": True,
        },
    )
    monkeypatch.setattr(
        facade,
        "storage_get_strategy_variant",
        lambda variant_id: {
            "id": variant_id,
            "strategy_id": "strategy-1",
            "name": "aggressive",
            "param_overrides": {"conviction_min": 0.65},
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

    assert captured["params"] == {"conviction_min": 0.65, "risk_multiple": 1.0}
    assert payload["strategy_hash"] == "hash-1"
    assert payload["variant"] == {
        "id": "variant-1",
        "name": "aggressive",
        "description": None,
        "param_overrides": {"conviction_min": 0.65},
        "resolved_params": {"conviction_min": 0.65, "risk_multiple": 1.0},
        "atm_template_id": None,
        "is_default": False,
    }
    assert payload["compiled"]["rule_count"] == 1


def test_create_strategy_variant_validates_against_effective_variant_params(monkeypatch) -> None:
    record = _strategy_record()

    monkeypatch.setattr(facade._REGISTRY, "get", lambda strategy_id: record)  # type: ignore[attr-defined]
    monkeypatch.setattr(
        facade,
        "get_atm_template",
        lambda template_id: {"id": template_id, "template": {}} if template_id else None,
    )
    monkeypatch.setattr(
        facade,
        "storage_ensure_default_strategy_variant",
        lambda strategy_id: {
            "id": "variant-default",
            "strategy_id": strategy_id,
            "name": "default",
            "param_overrides": {"conviction_min": 0.5},
            "is_default": True,
        },
    )
    saved: dict[str, object] = {}

    def _compile_strategy(**kwargs):
        saved["params"] = kwargs["params"]
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
        name="aggressive",
        param_overrides={"risk_multiple": 1.5},
    )

    assert saved["params"] == {"conviction_min": 0.5, "risk_multiple": 1.5}
    assert payload["name"] == "aggressive"
