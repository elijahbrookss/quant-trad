from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots.config_service import BotConfigService


def test_create_bot_uses_explicit_bot_atm_not_variant(monkeypatch) -> None:
    service = BotConfigService()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.load_strategies",
        lambda: [{"id": "strategy-1"}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.StrategyLoader.fetch_strategy",
        lambda strategy_id: SimpleNamespace(
            id=strategy_id,
            atm_template_id="atm-base",
            risk_config={"base_risk_per_trade": 100.0, "global_risk_multiplier": 1.0},
        ),
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_strategy_variant",
        lambda variant_id: {"id": variant_id, "strategy_id": "strategy-1", "output_filters": []},
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.list_strategy_variants",
        lambda strategy_id: [{"id": "variant-default", "strategy_id": strategy_id, "name": "default", "output_filters": [], "is_default": True}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_atm_template",
        lambda template_id: {"id": template_id, "template": {}} if template_id else None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.upsert_bot",
        lambda payload: captured.update(dict(payload)),
    )

    bot = service.create_bot(
        name="Bot 1",
        strategy_id="strategy-1",
        strategy_variant_id="variant-1",
        atm_template_id="atm-explicit",
        wallet_config={"balances": {"USD": 1000}},
        snapshot_interval_ms=1000,
        backtest_start="2026-01-01T00:00:00Z",
        backtest_end="2026-01-02T00:00:00Z",
    )

    assert bot["atm_template_id"] == "atm-explicit"
    assert captured["atm_template_id"] == "atm-explicit"
    assert bot["execution_mode"] == "fast"
    assert captured["risk"]["execution_mode"] == "fast"


def test_create_bot_resolves_variant_name_to_id(monkeypatch) -> None:
    service = BotConfigService()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.load_strategies",
        lambda: [{"id": "strategy-1"}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.StrategyLoader.fetch_strategy",
        lambda strategy_id: SimpleNamespace(
            id=strategy_id,
            atm_template_id="atm-base",
            risk_config={"base_risk_per_trade": 100.0, "global_risk_multiplier": 1.0},
        ),
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_strategy_variant",
        lambda _variant_id: None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.list_strategy_variants",
        lambda strategy_id: [
            {"id": "variant-default", "strategy_id": strategy_id, "name": "default", "output_filters": [], "is_default": True},
            {"id": "variant-expanding", "strategy_id": strategy_id, "name": "expanding-only", "output_filters": [], "is_default": False},
        ],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_atm_template",
        lambda template_id: {"id": template_id, "template": {}} if template_id else None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.upsert_bot",
        lambda payload: captured.update(dict(payload)),
    )

    bot = service.create_bot(
        name="Bot 1",
        strategy_id="strategy-1",
        strategy_variant_name="expanding-only",
        wallet_config={"balances": {"USD": 1000}},
        snapshot_interval_ms=1000,
        backtest_start="2026-01-01T00:00:00Z",
        backtest_end="2026-01-02T00:00:00Z",
    )

    assert bot["strategy_variant_id"] == "variant-expanding"
    assert bot["strategy_variant_name"] == "expanding-only"
    assert captured["strategy_variant_id"] == "variant-expanding"
    assert captured["strategy_variant_name"] == "expanding-only"


def test_update_bot_uses_explicit_bot_atm_not_variant(monkeypatch) -> None:
    service = BotConfigService()
    persisted: dict[str, object] = {
        "id": "bot-1",
        "name": "Bot 1",
        "strategy_id": "strategy-1",
        "strategy_variant_id": "variant-default",
        "strategy_variant_name": "default",
        "atm_template_id": "atm-base",
        "resolved_params": {},
        "risk_config": {"base_risk_per_trade": 100.0, "global_risk_multiplier": 1.0},
        "mode": "instant",
        "run_type": "backtest",
        "backtest_start": "2026-01-01T00:00:00Z",
        "backtest_end": "2026-01-02T00:00:00Z",
        "wallet_config": {"balances": {"USD": 1000}},
        "risk": {},
        "snapshot_interval_ms": 1000,
        "bot_env": {},
        "status": "idle",
        "last_stats": {},
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.load_bots",
        lambda: [dict(persisted)],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.load_strategies",
        lambda: [{"id": "strategy-1"}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.StrategyLoader.fetch_strategy",
        lambda strategy_id: SimpleNamespace(
            id=strategy_id,
            atm_template_id="atm-base",
            risk_config={"base_risk_per_trade": 100.0, "global_risk_multiplier": 1.0},
        ),
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_strategy_variant",
        lambda variant_id: {
            "id": variant_id,
            "strategy_id": "strategy-1",
            "name": "variant-one" if variant_id == "variant-1" else "default",
            "output_filters": [],
        },
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.list_strategy_variants",
        lambda strategy_id: [{"id": "variant-default", "strategy_id": strategy_id, "name": "default", "output_filters": [], "is_default": True}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_atm_template",
        lambda template_id: {"id": template_id, "template": {}} if template_id else None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.upsert_bot",
        lambda payload: captured.update(dict(payload)),
    )

    bot = service.update_bot(
        "bot-1",
        strategy_variant_id="variant-1",
        atm_template_id="atm-explicit",
    )

    assert bot["atm_template_id"] == "atm-explicit"
    assert bot["strategy_variant_id"] == "variant-1"
    assert bot["strategy_variant_name"] == "variant-one"
    assert captured["atm_template_id"] == "atm-explicit"
    assert captured["strategy_variant_id"] == "variant-1"
    assert captured["strategy_variant_name"] == "variant-one"


def test_update_bot_persists_execution_mode_in_runtime_config(monkeypatch) -> None:
    service = BotConfigService()
    persisted: dict[str, object] = {
        "id": "bot-1",
        "name": "Bot 1",
        "strategy_id": "strategy-1",
        "strategy_variant_id": None,
        "strategy_variant_name": None,
        "atm_template_id": "atm-base",
        "resolved_params": {},
        "risk_config": {"base_risk_per_trade": 100.0, "global_risk_multiplier": 1.0},
        "mode": "walk-forward",
        "run_type": "backtest",
        "backtest_start": "2026-01-01T00:00:00Z",
        "backtest_end": "2026-01-02T00:00:00Z",
        "wallet_config": {"balances": {"USD": 1000}},
        "risk": {},
        "snapshot_interval_ms": 1000,
        "bot_env": {},
        "status": "idle",
        "last_stats": {},
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.load_bots",
        lambda: [dict(persisted)],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.load_strategies",
        lambda: [{"id": "strategy-1"}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.StrategyLoader.fetch_strategy",
        lambda strategy_id: SimpleNamespace(
            id=strategy_id,
            atm_template_id="atm-base",
            risk_config={"base_risk_per_trade": 100.0, "global_risk_multiplier": 1.0},
        ),
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_strategy_variant",
        lambda _variant_id: None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.list_strategy_variants",
        lambda strategy_id: [{"id": "variant-default", "strategy_id": strategy_id, "name": "default", "output_filters": [], "is_default": True}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_atm_template",
        lambda template_id: {"id": template_id, "template": {}} if template_id else None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.upsert_bot",
        lambda payload: captured.update(dict(payload)),
    )

    bot = service.update_bot("bot-1", execution_mode="fast")

    assert bot["mode"] == "walk-forward"
    assert bot["execution_mode"] == "fast"
    assert captured["risk"]["execution_mode"] == "fast"


def test_create_bot_persists_full_execution_mode(monkeypatch) -> None:
    service = BotConfigService()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.load_strategies",
        lambda: [{"id": "strategy-1"}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.StrategyLoader.fetch_strategy",
        lambda strategy_id: SimpleNamespace(
            id=strategy_id,
            atm_template_id="atm-base",
            risk_config={"base_risk_per_trade": 100.0, "global_risk_multiplier": 1.0},
        ),
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_strategy_variant",
        lambda _variant_id: None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.list_strategy_variants",
        lambda strategy_id: [{"id": "variant-default", "strategy_id": strategy_id, "name": "default", "output_filters": [], "is_default": True}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.get_atm_template",
        lambda template_id: {"id": template_id, "template": {}} if template_id else None,
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.upsert_bot",
        lambda payload: captured.update(dict(payload)),
    )

    bot = service.create_bot(
        name="Bot 1",
        strategy_id="strategy-1",
        execution_mode="full",
        wallet_config={"balances": {"USD": 1000}},
        snapshot_interval_ms=1000,
        backtest_start="2026-01-01T00:00:00Z",
        backtest_end="2026-01-02T00:00:00Z",
    )

    assert bot["execution_mode"] == "full"
    assert captured["execution_mode"] == "full"
    assert captured["risk"]["execution_mode"] == "full"
