from __future__ import annotations

from pathlib import Path

import pytest

import portal.backend.workers.single_node_initializer as initializer


class _MarketStructure:
    def __init__(self) -> None:
        self.paths: list[Path] = []

    def apply_stream_enrollment_manifest(self, *, manifest_path: Path):
        self.paths.append(Path(manifest_path))
        return {"manifest_path": str(manifest_path)}


class _ScheduledCollector:
    def __init__(self) -> None:
        self.open_interest: list[dict] = []
        self.funding: list[dict] = []
        self.structured: list[dict] = []

    def create_coinbase_open_interest_definition(self, **kwargs):
        self.open_interest.append(dict(kwargs))
        return {"kind": "open_interest", **kwargs}

    def create_coinbase_funding_rate_definition(self, **kwargs):
        self.funding.append(dict(kwargs))
        return {"kind": "funding_rate", **kwargs}

    def create_structured_fact_definition(self, **kwargs):
        self.structured.append(dict(kwargs))
        return {"kind": "structured_fact", **kwargs}


def test_single_node_initializer_installs_full_reviewed_market_data_fleet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(__file__).parents[2]
    trade = root / "config/market_data/coinbase_perpetual_trade_fleet.v1.json"
    level2 = root / "config/market_data/coinbase_perpetual_l2_fleet.v1.json"
    instruments = (
        root / "config/market_data/coinbase_perpetual_instruments.v1.json"
    )
    structured = (
        root
        / "config/market-data/structured-facts"
        / "chainlink-nxtassets-btc-etp-reserves.json"
    )
    structure = _MarketStructure()
    scheduled = _ScheduledCollector()
    installed: list[dict] = []
    monkeypatch.setattr(initializer, "market_structure_service", structure)
    monkeypatch.setattr(initializer, "market_data_collector", scheduled)
    monkeypatch.setattr(
        initializer,
        "install_code_owned_instrument",
        lambda payload: installed.append(dict(payload)) or dict(payload),
    )
    monkeypatch.setenv("QT_SINGLE_NODE_INSTRUMENT_MANIFEST", str(instruments))
    monkeypatch.setenv("QT_SINGLE_NODE_TRADE_MANIFEST", str(trade))
    monkeypatch.setenv("QT_SINGLE_NODE_L2_MANIFEST", str(level2))
    monkeypatch.setenv("QT_SINGLE_NODE_STRUCTURED_FACT_MANIFESTS", str(structured))

    result = initializer.initialize_single_node_market_data()

    assert result["status"] == "initialized"
    assert structure.paths == [trade.resolve(), level2.resolve()]
    assert len(installed) == 3
    assert len(result["instruments"]) == 3
    assert len(scheduled.open_interest) == 3
    assert len(scheduled.funding) == 3
    assert len(scheduled.structured) == 1
    assert len(result["scheduled_facts"]) == 7
    assert all(row["enabled"] is True for row in scheduled.open_interest)
    assert all(row["poll_interval_seconds"] == 60 for row in scheduled.funding)
    assert scheduled.structured == [
        {
            "manifest_path": str(structured.resolve()),
            "binding_id": "nxtassets-btc-direct-etp-reserves",
            "max_attempts": 3,
            "minimum_spacing_seconds": 1.0,
            "enabled": True,
        }
    ]
    assert result["structured_fact_manifests"] == [
        {
            "id": "chainlink-nxtassets-btc-etp-reserves",
            "manifest_hash": initializer.load_structured_fact_manifest(
                structured
            ).manifest_hash,
            "path": str(structured.resolve()),
        }
    ]


def test_single_node_initializer_can_skip_all_market_data_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    structure = _MarketStructure()
    scheduled = _ScheduledCollector()
    installed: list[dict] = []
    monkeypatch.setattr(initializer, "market_structure_service", structure)
    monkeypatch.setattr(initializer, "market_data_collector", scheduled)
    monkeypatch.setattr(
        initializer,
        "install_code_owned_instrument",
        lambda payload: installed.append(dict(payload)) or dict(payload),
    )
    monkeypatch.setenv("QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA", "false")

    result = initializer.initialize_single_node_market_data()

    assert result["status"] == "skipped"
    assert structure.paths == []
    assert scheduled.open_interest == []
    assert scheduled.funding == []
    assert scheduled.structured == []
    assert installed == []
