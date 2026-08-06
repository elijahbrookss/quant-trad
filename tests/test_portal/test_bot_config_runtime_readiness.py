from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots.config_service import BotConfigService


def _bot_payload(**overrides: object) -> dict:
    payload = {
        "strategy_id": "strategy-1",
        "run_type": "backtest",
        "backtest_start": "2026-01-01T00:00:00Z",
        "backtest_end": "2026-01-02T00:00:00Z",
        "wallet_config": {"balances": {"USD": 10_000}},
    }
    payload.update(overrides)
    return payload


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
        timeframe="1h",
        instrument_links=[link],
    )


def _patch_strategy_lookup(monkeypatch, strategy: SimpleNamespace) -> None:
    monkeypatch.setattr(
        "portal.backend.service.bots.config_service.load_strategies",
        lambda: [{"id": "strategy-1"}],
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.strategy_loader.StrategyLoader.fetch_strategy",
        lambda _strategy_id, **_kwargs: strategy,
    )


def test_runtime_readiness_accepts_spot_as_proxy_derivative_for_backtest(monkeypatch):
    service = BotConfigService()
    strategy = _strategy_with_instrument(
        {
            "symbol": "BTC-USD",
            "instrument_type": "spot",
            "proxy_derivative_margin_rates": {
                "intraday": {"long_margin_rate": 0.1, "short_margin_rate": 0.1},
                "overnight": {"long_margin_rate": 0.25, "short_margin_rate": 0.3},
            },
            "proxy_derivative_instrument_fields": {
                "tick_size": 5.0,
                "contract_size": 0.01,
                "tick_value": 0.05,
                "min_order_size": 1.0,
                "qty_step": 1.0,
                "can_short": True,
                "short_requires_borrow": False,
            },
        }
    )

    _patch_strategy_lookup(monkeypatch, strategy)
    monkeypatch.setattr(
        "portal.backend.service.market.instrument_service.resolve_instrument",
        lambda _datasource, _exchange, _symbol: None,
    )

    artifacts = service.prepare_startup_artifacts(_bot_payload())

    profile = artifacts["runtime_readiness"]["profiles"][0]
    assert profile["instrument_type"] == "spot"
    assert profile["source_instrument_type"] == "spot"
    assert profile["execution_semantics"] == "proxy_derivative"
    assert profile["research_market_role"] == "proxy_underlier"
    assert profile["margin_calc_type"] == "margin"
    assert profile["resolved_execution_context_hash"]
    assert profile["instrument_execution_contract_hash"]
    assert profile["venue_execution_profile_id"] == "canonical_bar_simulation"
    assert profile["order_policy_conformance"] == {
        "status": "passed",
        "required_order_types": ["limit_resting", "market", "stop_market"],
        "required_time_in_force": ["gtc"],
        "post_only_order_types": [],
        "venue_execution_profile_id": "canonical_bar_simulation",
        "venue_execution_profile_version": "canonical_bar_simulation.v1",
        "venue_execution_profile_hash": profile["venue_execution_profile_hash"],
    }
    bundle = artifacts["resolved_execution_context_bundle"]
    assert bundle["bundle_hash"]
    assert len(bundle["contexts"]) == 1
    assert bundle["contexts"][0]["context_hash"] == profile["resolved_execution_context_hash"]


def test_runtime_readiness_blocks_proxy_derivative_outside_backtest(monkeypatch):
    service = BotConfigService()
    strategy = _strategy_with_instrument(
        {
            "symbol": "BTC-USD",
            "instrument_type": "spot",
            "proxy_derivative_margin_rates": {
                "intraday": {"long_margin_rate": 0.1, "short_margin_rate": 0.1},
                "overnight": {"long_margin_rate": 0.25, "short_margin_rate": 0.3},
            },
            "proxy_derivative_instrument_fields": {
                "tick_size": 5.0,
                "contract_size": 0.01,
                "tick_value": 0.05,
                "min_order_size": 1.0,
                "qty_step": 1.0,
                "can_short": True,
                "short_requires_borrow": False,
            },
        }
    )

    _patch_strategy_lookup(monkeypatch, strategy)
    monkeypatch.setattr(
        "portal.backend.service.market.instrument_service.resolve_instrument",
        lambda _datasource, _exchange, _symbol: None,
    )

    with pytest.raises(ValueError, match="proxy_derivative execution is currently supported for backtest runs only"):
        service.validate_runtime_readiness(
            _bot_payload(run_type="paper", execution_semantics="proxy_derivative")
        )


def test_runtime_readiness_blocks_derivatives_missing_margin_rates(monkeypatch):
    service = BotConfigService()
    strategy = _strategy_with_instrument(
        {
            "symbol": "BTC-PERP",
            "instrument_type": "future",
        }
    )

    _patch_strategy_lookup(monkeypatch, strategy)
    monkeypatch.setattr(
        "portal.backend.service.market.instrument_service.resolve_instrument",
        lambda _datasource, _exchange, _symbol: None,
    )

    with pytest.raises(ValueError, match="missing margin_rates"):
        service.validate_runtime_readiness(_bot_payload())


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

    _patch_strategy_lookup(monkeypatch, strategy)
    monkeypatch.setattr(
        "portal.backend.service.market.instrument_service.resolve_instrument",
        lambda _datasource, _exchange, _symbol: None,
    )

    service.validate_runtime_readiness(_bot_payload())


def test_runtime_readiness_rejects_strategy_orders_unsupported_by_venue_profile(monkeypatch):
    service = BotConfigService()
    strategy = _strategy_with_instrument(
        {
            "symbol": "BTC-USD",
            "instrument_type": "spot",
            "venue_execution_profile": {
                "profile_id": "market-only-fixture",
                "version": "market-only-fixture.v1",
                "venue_id": "synthetic-market-only",
                "supported_order_types": ["market"],
                "supported_time_in_force": ["gtc"],
                "post_only_supported": False,
                "post_only_behavior": "reject_would_cross",
                "liquidity_role_by_order_type": {"market": "taker"},
                "price_increment_policy": "reject",
                "quantity_increment_policy": "reject",
                "max_market_order_notional": None,
                "market_price_collar_bps": None,
                "book_data_capability": "bars",
                "lifecycle_event_mapping": {},
                "external_order_submission_enabled": False,
                "source": "test_fixture",
            },
        }
    )

    _patch_strategy_lookup(monkeypatch, strategy)
    monkeypatch.setattr(
        "portal.backend.service.market.instrument_service.resolve_instrument",
        lambda _datasource, _exchange, _symbol: None,
    )

    with pytest.raises(
        ValueError,
        match=r"venue_profile_unsupported_order_types .*limit_resting,stop_market",
    ):
        service.prepare_startup_artifacts(_bot_payload(execution_semantics="spot"))
