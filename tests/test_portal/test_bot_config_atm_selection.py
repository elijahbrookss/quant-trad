from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots.config_service import BotConfigService


def test_create_bot_derives_atm_from_variant_not_payload(monkeypatch) -> None:
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
        lambda variant_id: {"id": variant_id, "strategy_id": "strategy-1", "atm_template_id": "atm-variant"},
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.upsert_bot",
        lambda payload: captured.update(dict(payload)),
    )

    bot = service.create_bot(
        name="Bot 1",
        strategy_id="strategy-1",
        strategy_variant_id="variant-1",
        atm_template_id="atm-should-be-ignored",
        wallet_config={"balances": {"USD": 1000}},
        snapshot_interval_ms=1000,
        backtest_start="2026-01-01T00:00:00Z",
        backtest_end="2026-01-02T00:00:00Z",
    )

    assert bot["atm_template_id"] == "atm-variant"
    assert captured["atm_template_id"] == "atm-variant"


def test_update_bot_recomputes_effective_atm_from_variant(monkeypatch) -> None:
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
        lambda variant_id: {"id": variant_id, "strategy_id": "strategy-1", "atm_template_id": "atm-variant"},
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.upsert_bot",
        lambda payload: captured.update(dict(payload)),
    )

    bot = service.update_bot(
        "bot-1",
        strategy_variant_id="variant-1",
        atm_template_id="atm-should-be-ignored",
    )

    assert bot["atm_template_id"] == "atm-variant"
    assert captured["atm_template_id"] == "atm-variant"
