from __future__ import annotations

from datetime import datetime, timezone

import pytest

from engines.bot_runtime.core.domain import Candle
from signals.contract import (
    assert_no_execution_fields,
    assert_signal_contract,
    assert_signal_time_is_closed_bar,
)


def _base_signal() -> dict:
    return {
        "signal_type": "breakout",
        "signal_time": int(datetime(2026, 1, 1, 12, tzinfo=timezone.utc).timestamp()),
        "symbol": "BTC-USD",
        "timeframe_seconds": 3600,
        "indicator_id": "ind-1",
        "rule_id": "market_profile_breakout_v3_confirmed",
        "pattern_id": "breakout_v3",
        "runtime_scope": "s|e|1h",
        "metadata": {"profile_key": "p1"},
    }


def test_assert_signal_contract_accepts_minimum_payload() -> None:
    signal = _base_signal()
    assert_signal_contract(signal)


def test_assert_signal_contract_requires_signal_time() -> None:
    signal = _base_signal()
    signal.pop("signal_time")
    with pytest.raises(RuntimeError, match="signal_time"):
        assert_signal_contract(signal)


def test_assert_signal_time_is_closed_bar_requires_exact_match() -> None:
    candle = Candle(
        time=datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1.0,
    )
    signal = _base_signal()
    assert_signal_time_is_closed_bar(signal, candle)
    signal["signal_time"] = int(datetime(2026, 1, 1, 13, tzinfo=timezone.utc).timestamp())
    with pytest.raises(RuntimeError, match="signal_time_validation_failed"):
        assert_signal_time_is_closed_bar(signal, candle)


def test_assert_no_execution_fields_rejects_signal_execution_payload() -> None:
    signal = _base_signal()
    signal["entry_time"] = "2026-01-01T13:00:00Z"
    with pytest.raises(RuntimeError, match="execution fields"):
        assert_no_execution_fields(signal)
