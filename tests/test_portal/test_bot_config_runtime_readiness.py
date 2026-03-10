from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots.config_service import BotConfigService


def _strategy_with_instrument(snapshot: dict) -> SimpleNamespace:
    runtime_snapshot = {
        "tick_size": 0.5,
        "contract_size": 1.0,
        "tick_value": 0.5,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "min_order_size": 0.001,
        "qty_step": 0.001,
    }
    runtime_snapshot.update(snapshot)
    link = SimpleNamespace(
        symbol=runtime_snapshot.get("symbol", "BTC-PERP"),
        instrument_id=runtime_snapshot.get("id", "instrument-1"),
        instrument_snapshot=dict(runtime_snapshot),
    )
    return SimpleNamespace(
        datasource="COINBASE",
        exchange="COINBASE_DIRECT",
        instrument_links=[link],
    )


def test_runtime_readiness_blocks_non_derivatives(monkeypatch):
    service = BotConfigService()
    strategy = _strategy_with_instrument(
        {
            "symbol": "BTC-USD",
            "instrument_type": "spot",
        }
    )

    monkeypatch.setattr(
        "portal.backend.service.bots.bot_runtime.strategy.strategy_loader.StrategyLoader.fetch_strategy",
        lambda _strategy_id: strategy,
    )
    monkeypatch.setattr(
        "portal.backend.service.market.instrument_service.resolve_instrument",
        lambda _datasource, _exchange, _symbol: None,
    )

    with pytest.raises(ValueError, match="supports only futures/perps instruments"):
        service.validate_runtime_readiness({"strategy_id": "strategy-1"})


def test_runtime_readiness_blocks_derivatives_missing_margin_rates(monkeypatch):
    service = BotConfigService()
    strategy = _strategy_with_instrument(
        {
            "symbol": "BTC-PERP",
            "instrument_type": "future",
        }
    )

    monkeypatch.setattr(
        "portal.backend.service.bots.bot_runtime.strategy.strategy_loader.StrategyLoader.fetch_strategy",
        lambda _strategy_id: strategy,
    )
    monkeypatch.setattr(
        "portal.backend.service.market.instrument_service.resolve_instrument",
        lambda _datasource, _exchange, _symbol: None,
    )

    with pytest.raises(ValueError, match="missing margin_rates"):
        service.validate_runtime_readiness({"strategy_id": "strategy-1"})


def test_runtime_readiness_accepts_derivatives_with_margin_rates(monkeypatch):
    service = BotConfigService()
    strategy = _strategy_with_instrument(
        {
            "symbol": "BTC-PERP",
            "instrument_type": "future",
            "margin_rates": {
                "intraday": {"long_margin_rate": 0.10, "short_margin_rate": 0.10},
                "overnight": {"long_margin_rate": 0.20, "short_margin_rate": 0.20},
            },
        }
    )

    monkeypatch.setattr(
        "portal.backend.service.bots.bot_runtime.strategy.strategy_loader.StrategyLoader.fetch_strategy",
        lambda _strategy_id: strategy,
    )
    monkeypatch.setattr(
        "portal.backend.service.market.instrument_service.resolve_instrument",
        lambda _datasource, _exchange, _symbol: None,
    )

    service.validate_runtime_readiness({"strategy_id": "strategy-1"})
