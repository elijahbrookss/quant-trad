"""Tests for the portal backend strategy service."""

import importlib.util
import json
from pathlib import Path

import pytest

from portal.backend.service.strategy_service import StrategyService


YAML_AVAILABLE = importlib.util.find_spec("yaml") is not None


def _build_payload():
    return {
        "name": "Breakout Momentum",
        "symbol": "BTCUSD",
        "timeframe": "1h",
        "description": "Test strategy",
        "indicators": [
            {"id": "ind-1", "name": "VWAP"},
            {"id": "ind-2", "name": "MP"},
        ],
        "selected_signals": {
            "ind-1": ["vwap_cross_long"],
            "ind-2": ["profile_retest_short", "profile_break"]
        },
    }


def test_save_strategy_and_listings(tmp_path: Path):
    service = StrategyService(storage_path=tmp_path / "strategies.json")
    record = service.save_strategy(_build_payload())

    assert record.strategy_id.startswith("strategy-")
    listed = service.list_strategies()
    assert listed and listed[0].strategy_id == record.strategy_id


def test_attach_yaml_and_generate_signals(tmp_path: Path):
    service = StrategyService(storage_path=tmp_path / "strategies.json")
    record = service.save_strategy(_build_payload())

    if not YAML_AVAILABLE:
        pytest.skip("PyYAML is not installed")

    yaml_body = """
    stops:
      take_profit: 1.5
      stop_loss: 0.75
    tags:
      - momentum
      - breakout
    """

    service.attach_yaml(record.strategy_id, yaml_body)
    signals = service.generate_order_signals(record.strategy_id)

    assert len(signals) == 3
    assert signals[0]["action"] == "enter_long"
    assert signals[1]["action"] == "enter_short"
    assert signals[0]["stops"]["take_profit"] == 1.5


def test_backtest_and_launch_placeholders(tmp_path: Path):
    service = StrategyService(storage_path=tmp_path / "strategies.json")
    record = service.save_strategy(_build_payload())

    backtest = service.request_backtest(record.strategy_id, {"start": "2023-01-01"})
    assert backtest["status"] == "queued"
    assert "requested_at" in backtest

    launch = service.launch_strategy(record.strategy_id, mode="live")
    assert launch["mode"] == "live"
    assert launch["status"] == "pending"


def test_reload_from_persisted_file(tmp_path: Path):
    storage_path = tmp_path / "strategies.json"
    service = StrategyService(storage_path=storage_path)
    record = service.save_strategy(_build_payload())

    # Force reload via a new service instance
    reloaded = StrategyService(storage_path=storage_path)
    fetched = reloaded.get_strategy(record.strategy_id)
    assert fetched is not None
    assert fetched.name == record.name

    # Ensure persisted JSON is structured as expected
    with storage_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["strategies"][0]["strategy_id"] == record.strategy_id
