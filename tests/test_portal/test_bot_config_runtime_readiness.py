from __future__ import annotations

from types import SimpleNamespace
from datetime import UTC, datetime

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots.config_service import BotConfigService
from engines.bot_runtime.core.book_execution import (
    EXECUTION_BOOK_SNAPSHOT_SCHEMA_VERSION,
    EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION,
    EXECUTION_BOOK_TAPE_SCHEMA_VERSION,
    ExecutionBookLevel,
    ExecutionBookSnapshot,
    ExecutionBookSourceReference,
    ExecutionBookTape,
    ExecutionBookTapeBundle,
)


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


def _execution_book_bundle() -> dict:
    snapshot = ExecutionBookSnapshot(
        schema_version=EXECUTION_BOOK_SNAPSHOT_SCHEMA_VERSION,
        instrument_id="instrument-1",
        series_id=17,
        validity_interval_id="validity-1",
        source_reference=ExecutionBookSourceReference(
            definition_id="definition-1",
            session_id="session-1",
            connection_epoch=0,
            source_product_id="BTC-USD",
            source_sequence=1,
            receive_ordinal=1,
            event_ordinal=0,
        ),
        product_definition_version_id="btc-usd.v1",
        quantity_unit="base",
        effective_at=datetime(2026, 1, 1, tzinfo=UTC),
        known_at=datetime(2026, 1, 1, tzinfo=UTC),
        reconstruction_state_hash="state-hash",
        bids=(ExecutionBookLevel("99", "2"),),
        asks=(ExecutionBookLevel("101", "2"),),
    )
    tape = ExecutionBookTape(
        schema_version=EXECUTION_BOOK_TAPE_SCHEMA_VERSION,
        tape_id="",
        instrument_id="instrument-1",
        source_capability="l2",
        reconstruction_version="fixture.v1",
        replay_fingerprint="replay-hash",
        replay_certified=True,
        snapshots=(snapshot,),
    )
    return ExecutionBookTapeBundle(
        schema_version=EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION,
        tapes=(tape,),
    ).to_dict()


def test_runtime_readiness_pins_x4_book_model_and_tape(monkeypatch):
    service = BotConfigService()
    strategy = _strategy_with_instrument(
        {
            "id": "instrument-1",
            "symbol": "BTC-USD",
            "instrument_type": "spot",
            "venue_execution_profile": {
                "profile_id": "l2-fixture",
                "version": "l2-fixture.v1",
                "venue_id": "synthetic-l2",
                "supported_order_types": [
                    "market",
                    "limit_aggressive",
                    "limit_maker",
                    "limit_resting",
                    "stop_market",
                ],
                "supported_time_in_force": ["gtc", "ioc", "fok"],
                "post_only_supported": True,
                "post_only_behavior": "reject_would_cross",
                "liquidity_role_by_order_type": {
                    "market": "taker",
                    "limit_aggressive": "taker",
                    "limit_maker": "maker",
                    "limit_resting": "maker",
                    "stop_market": "taker",
                },
                "price_increment_policy": "reject",
                "quantity_increment_policy": "reject",
                "max_market_order_notional": None,
                "market_price_collar_bps": None,
                "book_data_capability": "l2",
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

    artifacts = service.prepare_startup_artifacts(
        _bot_payload(
            execution_semantics="spot",
            execution_book_tape_bundle=_execution_book_bundle(),
        )
    )

    profile = artifacts["runtime_readiness"]["profiles"][0]
    context = artifacts["resolved_execution_context_bundle"]["contexts"][0]
    assert context["model"]["execution_quality_ceiling"] == "X4"
    assert context["model"]["input_capability"] == "l2"
    assert context["model"]["supports_partial_fills"] is True
    assert profile["execution_book_tape_hash"]
    assert artifacts["execution_book_tape_bundle"]["bundle_hash"]

    with pytest.raises(ValueError, match="only for backtest"):
        service.prepare_startup_artifacts(
            _bot_payload(
                run_type="paper",
                execution_semantics="spot",
                execution_book_tape_bundle=_execution_book_bundle(),
            )
        )

    strategy.instrument_links[0].instrument_snapshot[
        "venue_execution_profile"
    ]["book_data_capability"] = "l1"
    with pytest.raises(ValueError, match="venue_book_capability"):
        service.prepare_startup_artifacts(
            _bot_payload(
                execution_semantics="spot",
                execution_book_tape_bundle=_execution_book_bundle(),
            )
        )
