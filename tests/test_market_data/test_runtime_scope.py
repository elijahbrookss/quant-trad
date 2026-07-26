from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from market_data.backtest import (
    build_backtest_execution_config_hash,
    build_backtest_execution_instrument,
    normalize_backtest_execution_instruments,
)
from portal.backend.service.bots import runtime_dependencies
from portal.backend.service.market import candle_service


def _instrument() -> dict:
    return {
        "id": "instrument-1",
        "symbol": "BTC/USD",
        "datasource": "CCXT",
        "exchange": "coinbase",
        "instrument_type": "spot",
        "maker_fee_rate": 0.004,
        "taker_fee_rate": 0.006,
        "price_tick_size": 0.01,
        "quantity_step_size": 0.00000001,
    }


def _strategy():
    return SimpleNamespace(
        id="strategy-1",
        timeframe="1h",
        indicator_ids=[],
        rules={},
        resolved_params={},
        atm_template_id=None,
        atm_template={},
        risk_config={},
        instrument_links=[
            SimpleNamespace(
                instrument_id="instrument-1",
                symbol="BTC/USD",
                instrument_snapshot=_instrument(),
            )
        ],
        run_strategy_snapshot={
            "effective_strategy_config_hash": "strategy-config-hash-1"
        },
        effective_strategy_config={
            "effective_strategy_config_hash": "strategy-config-hash-1"
        },
    )


def _binding(runtime_config: dict | None = None) -> dict:
    dataset_hash = "a" * 64
    strategy_identity = runtime_dependencies.resolve_backtest_strategy_identity(
        _strategy()
    )
    instruments, instrument_config_hash = normalize_backtest_execution_instruments(
        [build_backtest_execution_instrument("instrument-1", _instrument())]
    )
    execution_config_hash = build_backtest_execution_config_hash(
        bot=dict(runtime_config or {}),
        strategy_identity=strategy_identity,
        instrument_config_hash=instrument_config_hash,
    )
    return {
        "schema_version": "backtest_dataset_binding.v1",
        "dataset_contract_version": "market_dataset.v1",
        "dataset_id": f"mds_{dataset_hash[:32]}",
        "dataset_hash": dataset_hash,
        "max_commit_seq": 73,
        **strategy_identity,
        "execution_config_hash": execution_config_hash,
        "instrument_config_hash": instrument_config_hash,
        "instruments": instruments,
        "validation_status": "ready",
        "provider_call_performed": False,
        "evaluation_range": {
            "start": "2026-01-01T00:00:00Z",
            "end_exclusive": "2026-01-02T00:00:00Z",
        },
        "warmup_range": {
            "start": "2025-12-31T10:00:00Z",
            "end_exclusive": "2026-01-01T00:00:00Z",
        },
        "materialization_range": {
            "start": "2025-12-31T10:00:00Z",
            "end_exclusive": "2026-01-02T00:00:00Z",
        },
        "decision_range": {
            "start": "2026-01-01T00:00:00Z",
            "end_exclusive": "2026-01-02T00:00:00Z",
        },
        "series": [
            {
                "series_id": 7,
                "identity_key": "series-identity-1",
                "instrument_id": "instrument-1",
                "fact_type": "candle.ohlcv",
                "contract_version": "candle.ohlcv.v1",
                "timeframe_seconds": 3600,
                "range_start": "2025-12-31T10:00:00Z",
                "range_end": "2026-01-02T00:00:00Z",
                "row_count": 38,
                "max_commit_seq": 73,
                "material_hash": "material-hash",
                "provenance_hash": "provenance-hash",
                "quality_hash": "quality-hash",
                "quality_evidence": [],
            }
        ],
        "quality": {"status": "ready", "evidence_count": 0},
    }


def test_market_data_scope_reads_only_the_bound_dataset_and_restores_context(
    monkeypatch,
) -> None:
    observed: dict = {}
    monkeypatch.setattr(
        candle_service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {"id": instrument_id, "symbol": "BTC/USD"},
    )
    monkeypatch.setattr(
        candle_service.canonical_candle_feed,
        "read_by_instrument",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("mutable latest-state read is forbidden")
        ),
    )

    def fake_dataset_read(**kwargs):
        observed.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(
        candle_service.canonical_candle_feed,
        "read_dataset_series",
        fake_dataset_read,
    )

    assert candle_service.current_market_data_read_scope() is None
    with candle_service.market_data_read_scope(dataset_binding=_binding()):
        frame = candle_service.fetch_ohlcv_by_instrument(
            "instrument-1",
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
            "1h",
        )

    assert observed["dataset_id"] == _binding()["dataset_id"]
    assert observed["series_id"] == 7
    assert observed["start"] == "2026-01-01T00:00:00Z"
    assert observed["end"] == "2026-01-02T00:00:00Z"
    assert frame.attrs["market_data_read_scope"] == {
        "schema_version": "market_data_read_scope.v2",
        "dataset_id": _binding()["dataset_id"],
        "dataset_hash": "a" * 64,
        "as_of_commit_seq": 73,
    }
    assert candle_service.current_market_data_read_scope() is None


def test_runtime_dependency_wraps_nested_strategy_reads_in_same_scope(monkeypatch) -> None:
    observed = {}

    def fake_preview(*args, **kwargs):
        observed["scope"] = candle_service.current_market_data_read_scope()
        return {"ok": True}

    monkeypatch.setattr(runtime_dependencies, "run_strategy_preview", fake_preview)
    deps = runtime_dependencies.build_bot_runtime_deps(dataset_binding=_binding())

    assert deps.strategy_run_preview() == {"ok": True}
    assert observed["scope"].dataset_id == _binding()["dataset_id"]
    assert observed["scope"].as_of_commit_seq == 73
    assert candle_service.current_market_data_read_scope() is None


def test_dataset_scope_rejects_range_expansion_before_store_read(monkeypatch) -> None:
    monkeypatch.setattr(
        candle_service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {"id": instrument_id, "symbol": "BTC/USD"},
    )
    monkeypatch.setattr(
        candle_service.canonical_candle_feed,
        "read_dataset_series",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("store read occurred")),
    )
    with candle_service.market_data_read_scope(dataset_binding=_binding()):
        with pytest.raises(ValueError, match="range_expansion_forbidden"):
            candle_service.fetch_ohlcv_by_instrument(
                "instrument-1",
                "2025-12-31T09:00:00Z",
                "2026-01-02T00:00:00Z",
                "1h",
            )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"validation_status": "failed"}, "validation_status"),
        ({"provider_call_performed": True}, "provider-free"),
        (
            {
                "decision_range": {
                    "start": "2026-01-01T01:00:00Z",
                    "end_exclusive": "2026-01-02T00:00:00Z",
                }
            },
            "decision range",
        ),
    ],
)
def test_runtime_binding_rejects_unadmitted_or_inconsistent_material(
    change,
    message,
) -> None:
    payload = {**_binding(), **change}
    with pytest.raises(ValueError, match=message):
        runtime_dependencies.build_bot_runtime_deps(dataset_binding=payload)


def test_runtime_dependency_rejects_strategy_or_indicator_substitution(
    monkeypatch,
) -> None:
    strategy = _strategy()
    monkeypatch.setattr(
        runtime_dependencies.StrategyLoader,
        "fetch_strategy",
        lambda *_args, **_kwargs: strategy,
    )
    deps = runtime_dependencies.build_bot_runtime_deps(dataset_binding=_binding())

    loaded = deps.fetch_strategy("strategy-1", {})
    assert loaded is not strategy
    assert loaded.instrument_links[0].instrument_snapshot == _instrument()

    admitted = runtime_dependencies.resolve_backtest_strategy_identity(strategy)
    monkeypatch.setattr(
        runtime_dependencies,
        "resolve_backtest_strategy_identity",
        lambda _strategy: {
            **admitted,
            "indicator_config_hash": "different-indicator-configuration",
        },
    )
    with pytest.raises(RuntimeError, match="strategy_substitution_forbidden"):
        deps.fetch_strategy("strategy-1", {})


def test_runtime_dependency_rejects_execution_policy_substitution(monkeypatch) -> None:
    binding = _binding()
    drifted_strategy = _strategy()
    drifted_strategy.atm_template_id = "atm-drifted"
    drifted_strategy.atm_template = {
        "stop_loss": {"type": "percent", "value": 0.01}
    }
    monkeypatch.setattr(
        runtime_dependencies.StrategyLoader,
        "fetch_strategy",
        lambda *_args, **_kwargs: drifted_strategy,
    )
    deps = runtime_dependencies.build_bot_runtime_deps(dataset_binding=binding)

    with pytest.raises(RuntimeError, match="strategy_substitution_forbidden"):
        deps.fetch_strategy("strategy-1", {})


def test_runtime_dependency_uses_only_bound_instrument_snapshots(monkeypatch) -> None:
    binding = _binding()
    deps = runtime_dependencies.build_bot_runtime_deps(dataset_binding=binding)
    monkeypatch.setattr(
        runtime_dependencies,
        "get_instrument_record",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("mutable instrument read is forbidden")
        ),
    )
    monkeypatch.setattr(
        runtime_dependencies,
        "resolve_instrument",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("mutable instrument resolution is forbidden")
        ),
    )

    assert deps.get_instrument_record("instrument-1") == _instrument()
    assert deps.resolve_instrument("CCXT", "coinbase", "BTC/USD") == _instrument()


def test_runtime_dependency_keeps_unbound_paper_strategy_loading_mutable(
    monkeypatch,
) -> None:
    strategy = _strategy()
    monkeypatch.setattr(
        runtime_dependencies.StrategyLoader,
        "fetch_strategy",
        lambda *_args, **_kwargs: strategy,
    )

    assert runtime_dependencies.build_bot_runtime_deps().fetch_strategy(
        "strategy-1", {}
    ) is strategy
