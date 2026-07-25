from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.candle_snapshot import (
    aggregate_candle_series_snapshots,
    build_candle_series_snapshot,
    build_expected_candle_series_inventory,
)
from engines.bot_runtime.core.domain import Candle


def _candles() -> list[Candle]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        Candle(
            time=start,
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            atr=1.25,
            volume=10.0,
        ),
        Candle(
            time=start + timedelta(hours=1),
            open=101.0,
            high=103.0,
            low=100.0,
            close=102.0,
            atr=1.5,
            volume=12.0,
        ),
    ]


def _snapshot(candles: list[Candle]):
    return build_candle_series_snapshot(
        candles,
        instrument_id="instrument-btc",
        symbol="BTC",
        timeframe="1h",
        datasource="coinbase",
        exchange="cbi",
        strategy_id="strategy-1",
        replay_start_index=1,
    )


def test_candle_series_snapshot_hashes_exact_material_values() -> None:
    baseline = _snapshot(_candles())
    changed_candles = _candles()
    changed_candles[-1].close += 0.00000001

    changed = _snapshot(changed_candles)

    assert changed["candle_value_hash"] != baseline["candle_value_hash"]


def test_expected_candle_series_inventory_rejects_incomplete_or_conflicting_identity() -> None:
    with pytest.raises(
        ValueError,
        match="strategy_id, instrument_id, and timeframe are required",
    ):
        build_expected_candle_series_inventory(
            [{"strategy_id": "strategy-1", "instrument_id": "", "timeframe": "1h"}]
        )

    with pytest.raises(ValueError, match="conflicting symbols"):
        build_expected_candle_series_inventory(
            [
                {
                    "strategy_id": "strategy-1",
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1h",
                },
                {
                    "strategy_id": "strategy-1",
                    "instrument_id": "instrument-btc",
                    "symbol": "ETH",
                    "timeframe": "1H",
                },
            ]
        )


def test_candle_series_snapshot_is_independent_of_input_row_order() -> None:
    candles = _candles()

    assert _snapshot(list(reversed(candles)))["candle_value_hash"] == _snapshot(candles)[
        "candle_value_hash"
    ]


def test_candle_series_snapshot_rejects_duplicate_or_malformed_candles() -> None:
    duplicate = _candles()
    duplicate.append(duplicate[-1])
    with pytest.raises(ValueError, match="duplicate candle time"):
        _snapshot(duplicate)

    malformed = _candles()
    malformed[-1].high = 99.0
    with pytest.raises(ValueError, match="high=.*below low"):
        _snapshot(malformed)

    with pytest.raises(ValueError, match="replay_start_index must be an integer"):
        build_candle_series_snapshot(
            _candles(),
            instrument_id="instrument-btc",
            symbol="BTC",
            timeframe="1h",
            strategy_id="strategy-1",
            replay_start_index=1.5,
        )


def test_run_snapshot_is_order_independent_and_rejects_conflicting_series() -> None:
    btc = _snapshot(_candles())
    eth = build_candle_series_snapshot(
        _candles(),
        instrument_id="instrument-eth",
        symbol="ETH",
        timeframe="1h",
        datasource="coinbase",
        exchange="cbi",
        strategy_id="strategy-1",
        replay_start_index=1,
    )

    forward = aggregate_candle_series_snapshots([btc, eth])
    reversed_order = aggregate_candle_series_snapshots([eth, btc])

    assert forward["data_snapshot_hash"] == reversed_order["data_snapshot_hash"]
    conflicting = dict(btc)
    conflicting["candle_value_hash"] = "f" * 64
    with pytest.raises(ValueError, match="conflicting hashes"):
        aggregate_candle_series_snapshots([btc, conflicting])

    malformed_counts = dict(btc)
    malformed_counts["replay_candle_count"] = 2
    with pytest.raises(ValueError, match="do not sum"):
        aggregate_candle_series_snapshots([malformed_counts])

    second_strategy = dict(btc)
    second_strategy["strategy_id"] = "strategy-2"
    second_strategy["candle_value_hash"] = "e" * 64
    assert aggregate_candle_series_snapshots(
        [btc, second_strategy]
    )["series_count"] == 2
