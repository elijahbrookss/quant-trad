from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import pytest

from data_providers.providers import chainlink
from data_providers.structured_facts import (
    STRUCTURED_FACT_MANIFEST_VERSION,
    load_structured_fact_manifest,
)
from market_data.canonical import CanonicalFact
from market_data.contracts import SourceIdentity
from market_data.fact_registry import get_fact_contract, get_fact_payload_schema


_ROOT = Path(__file__).resolve().parents[2]
_MANIFEST = (
    _ROOT
    / "config"
    / "market-data"
    / "structured-facts"
    / "chainlink-nxtassets-btc-etp-reserves.json"
)


def _word(value: int) -> bytes:
    return int(value).to_bytes(32, "big")


def _dynamic(value: bytes) -> str:
    padded = value + (b"\x00" * ((32 - len(value) % 32) % 32))
    return "0x" + (_word(32) + _word(len(value)) + padded).hex()


def _string_uint_bundle(report_id: str, quantity: int) -> str:
    encoded_id = report_id.encode("utf-8")
    padded_id = encoded_id + (b"\x00" * ((32 - len(encoded_id) % 32) % 32))
    bundle = _word(64) + _word(quantity) + _word(len(encoded_id)) + padded_id
    return _dynamic(bundle)


def _uint8_array(values: Sequence[int]) -> str:
    encoded = _word(32) + _word(len(values)) + b"".join(_word(value) for value in values)
    return "0x" + encoded.hex()


class _MvrRpc:
    def __init__(self, *, report_time: datetime) -> None:
        self.report_time = report_time
        self.calls: list[tuple[str, list[Any]]] = []

    def call(self, method: str, params: Sequence[Any]) -> Any:
        self.calls.append((method, list(params)))
        if method == "eth_chainId":
            return hex(42161)
        if method == "eth_blockNumber":
            return hex(10_000)
        if method == "eth_getBlockByNumber":
            assert params[0] == hex(9_980)
            return {
                "hash": "0x" + ("ab" * 32),
                "timestamp": hex(int((self.report_time + timedelta(minutes=1)).timestamp())),
            }
        if method != "eth_call":
            raise AssertionError(f"unexpected method {method}")
        request = dict(params[0])
        assert params[1] == hex(9_980)
        selector = request["data"]
        if selector == chainlink._DESCRIPTION:
            return _dynamic(
                b"nxtAssets Bitcoin Direct ETP Proof of Reserves (DE000NXTA018)"
            )
        if selector == chainlink._VERSION:
            return "0x" + _word(7).hex()
        if selector == chainlink._AGGREGATOR:
            return "0x" + (b"\x00" * 12 + bytes.fromhex("12" * 20)).hex()
        if selector == chainlink._BUNDLE_DECIMALS:
            return _uint8_array((0, 8))
        if selector == chainlink._LATEST_BUNDLE_TIMESTAMP:
            return "0x" + _word(int(self.report_time.timestamp())).hex()
        if selector == chainlink._LATEST_BUNDLE:
            return _string_uint_bundle("DE000NXTA018", 51_432_323_119)
        raise AssertionError(f"unexpected selector {selector}")


def test_production_mvr_manifest_pins_atomic_reserve_semantics() -> None:
    manifest = load_structured_fact_manifest(_MANIFEST)
    binding = manifest.binding("nxtassets-btc-direct-etp-reserves")

    assert manifest.schema_version == STRUCTURED_FACT_MANIFEST_VERSION
    assert manifest.enabled is True
    assert len(manifest.manifest_hash) == 64
    assert binding.adapter == chainlink.CHAINLINK_MVR_ADAPTER_ID
    assert binding.fact_type == "asset.reserve_state"
    assert binding.payload_schema_id == "asset.reserve_state.v1"
    assert binding.canonical_instrument["id"] == (
        "nxtassets-de000nxta018"
    )
    assert binding.dimensions == {"reserve_asset": "BTC"}
    assert binding.config["expected_bundle_fields"] == [
        {"name": "ID", "type": "string", "decimals": 0},
        {"name": "TotalReserve", "type": "uint256", "decimals": 8},
    ]
    assert binding.source["provider"] == "CHAINLINK"
    assert binding.risk["official_catalog_url"].startswith("https://data.chain.link/")


def test_structured_manifest_rejects_unreviewed_payload_shape(tmp_path: Path) -> None:
    payload = json.loads(_MANIFEST.read_text(encoding="utf-8"))
    payload["bindings"][0]["dimensions"]["provider"] = "chainlink"
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="unexpected=provider"):
        load_structured_fact_manifest(path)


def test_mvr_adapter_decodes_one_atomic_report_at_confirmed_head() -> None:
    report_time = datetime.now(UTC) - timedelta(hours=1)
    transport = _MvrRpc(report_time=report_time)
    binding = load_structured_fact_manifest(_MANIFEST).binding(
        "nxtassets-btc-direct-etp-reserves"
    )
    provider = chainlink.ChainlinkMvrReserveProvider(
        transport,
        endpoint_ref=binding.endpoint_ref,
    )

    snapshot = provider.fetch_reserve_state(binding)

    assert snapshot.report_id == "DE000NXTA018"
    assert snapshot.subject_id == "DE000NXTA018"
    assert snapshot.reserve_asset == "BTC"
    assert snapshot.reserve_quantity == Decimal("514.32323119")
    assert snapshot.raw_reserve_quantity == "51432323119"
    assert snapshot.observation_time == report_time.replace(microsecond=0)
    assert snapshot.metadata["bundle_decimals"] == [0, 8]
    assert snapshot.metadata["confirmed_head_block"] == 9_980
    assert snapshot.metadata["confirmations"] == 20
    assert snapshot.metadata["bundle"].startswith("0x")
    assert len(snapshot.response_hash) == 64
    assert len(snapshot.source_event_key) < 512


def test_reserve_payload_is_strict_provider_neutral_and_hash_stable() -> None:
    contract = get_fact_contract("asset.reserve_state")
    schema = get_fact_payload_schema("asset.reserve_state.v1")
    dimensions = contract.normalize_dimensions({"reserve_asset": "btc"})
    source = SourceIdentity(
        provider="CHAINLINK",
        venue="ARBITRUM_MAINNET",
        source_kind="public_evm_contract",
        adapter_version=chainlink.CHAINLINK_MVR_ADAPTER_ID,
    )
    observed = datetime(2026, 8, 7, 19, 0, tzinfo=UTC)
    accepted = datetime(2026, 8, 7, 19, 1, tzinfo=UTC)
    fact = CanonicalFact(
        fact_type="asset.reserve_state",
        payload_schema_id="asset.reserve_state.v1",
        observation_key="report-1",
        observation_time=observed,
        observation_time_method="source_report_timestamp",
        source_published_at=observed,
        received_at=accepted,
        accepted_at=accepted,
        known_at=accepted,
        known_at_method="platform_acceptance",
        source=source,
        transformation_id="chainlink_mvr_reserve_state.v1",
        external_event_key="report-1",
        payload={
            "report_id": "DE000NXTA018",
            "reserve_asset": dimensions["reserve_asset"],
            "reserve_quantity": Decimal("514.32323119"),
            "unit": dimensions["reserve_asset"],
        },
    )

    assert fact.payload == {
        "report_id": "DE000NXTA018",
        "reserve_asset": "BTC",
        "reserve_quantity": "514.32323119",
        "unit": "BTC",
    }
    assert schema.contract["additional_properties"] is False
    assert len(fact.payload_hash) == 64
    with pytest.raises(ValueError, match="unexpected=provider"):
        schema.normalize_payload({**fact.payload, "provider": "chainlink"})
