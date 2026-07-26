from __future__ import annotations

import pandas as pd

from portal.backend.service.bots import runtime_dependencies
from portal.backend.service.market import candle_service


def test_market_data_read_scope_pins_canonical_read_and_restores_context(monkeypatch) -> None:
    observed = {}
    monkeypatch.setattr(
        candle_service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {"id": instrument_id, "symbol": "BTC/USD"},
    )

    def fake_read(instrument, **kwargs):
        observed.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(
        candle_service.canonical_candle_feed, "read_by_instrument", fake_read
    )

    assert candle_service.current_market_data_read_scope() is None
    with candle_service.market_data_read_scope(as_of_commit_seq=42):
        frame = candle_service.fetch_ohlcv_by_instrument(
            "instrument-1",
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
            "1h",
        )

    assert observed["as_of_commit_seq"] == 42
    assert frame.attrs["market_data_read_scope"]["as_of_commit_seq"] == 42
    assert candle_service.current_market_data_read_scope() is None


def test_runtime_dependency_wraps_nested_strategy_reads_in_same_scope(monkeypatch) -> None:
    observed = {}

    def fake_preview(*args, **kwargs):
        observed["scope"] = candle_service.current_market_data_read_scope()
        return {"ok": True}

    monkeypatch.setattr(runtime_dependencies, "run_strategy_preview", fake_preview)
    deps = runtime_dependencies.build_bot_runtime_deps(
        market_data_as_of_commit_seq=77
    )

    assert deps.strategy_run_preview() == {"ok": True}
    assert observed["scope"].as_of_commit_seq == 77
    assert candle_service.current_market_data_read_scope() is None
