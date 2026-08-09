from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_providers.numeric_facts import (
    NUMERIC_FACT_MANIFEST_VERSION,
    load_numeric_fact_manifest,
)
from market_data.fact_registry import get_fact_contract


_ROOT = Path(__file__).resolve().parents[2]
_MANIFEST_DIR = _ROOT / "config" / "market-data" / "numeric-facts"
_ETH_MANIFEST = _MANIFEST_DIR / "chainlink-eth-usd.reference.json"
_TUSD_MANIFEST = _MANIFEST_DIR / "chainlink-tusd-reserves.reference.json"


@pytest.mark.parametrize(
    ("path", "binding_id", "fact_type", "contract_version", "unit", "dimensions"),
    (
        (
            _ETH_MANIFEST,
            "eth-usd",
            "market.reference_price",
            "market.reference_price.v1",
            "USD",
            {"quote_currency": "USD"},
        ),
        (
            _TUSD_MANIFEST,
            "tusd-reserves",
            "market.reserve_balance",
            "market.reserve_balance.v1",
            "USD",
            {"reserve_unit": "USD"},
        ),
    ),
)
def test_reference_manifests_are_disabled_exact_numeric_bindings(
    path: Path,
    binding_id: str,
    fact_type: str,
    contract_version: str,
    unit: str,
    dimensions: dict[str, str],
) -> None:
    manifest = load_numeric_fact_manifest(path)

    assert manifest.schema_version == NUMERIC_FACT_MANIFEST_VERSION
    assert manifest.enabled is False
    assert len(manifest.manifest_hash) == 64
    assert manifest.manifest_hash == load_numeric_fact_manifest(path).manifest_hash

    with pytest.raises(RuntimeError, match="numeric_fact_binding_disabled"):
        manifest.binding(binding_id)

    binding = manifest.binding(binding_id, require_enabled=False)
    assert binding.enabled is False
    assert binding.adapter == "chainlink_aggregator_v3.v1"
    assert binding.fact_type == fact_type
    assert binding.contract_version == contract_version
    assert binding.unit == unit
    assert binding.dimensions == dimensions
    assert binding.endpoint_ref == "CHAINLINK_ETHEREUM_RPC_URL"
    assert binding.instrument_role in {"benchmark", "primary"}
    assert set(binding.schedule) == {
        "expected_update_interval_seconds",
        "deviation_threshold_basis_points",
    }
    assert binding.quality_policy["stale_behavior"] == "gap"
    assert binding.quality_policy["max_staleness_seconds"] > 0
    assert binding.risk["official_catalog_url"].startswith("https://data.chain.link/")
    assert binding.risk["deprecation_status"] == "not_marked_deprecated"
    assert binding.source == {
        "provider": "CHAINLINK",
        "venue": "ETHEREUM_MAINNET",
        "source_kind": "public_evm_contract",
        "adapter_version": "chainlink_aggregator_v3.v1",
    }

    contract = get_fact_contract(binding.fact_type)
    assert contract.uses_exact_numeric_storage is True
    assert contract.numeric_type == "decimal"
    assert contract.subject_type == "instrument"


def test_reference_manifests_keep_feed_deployments_in_data() -> None:
    eth = load_numeric_fact_manifest(_ETH_MANIFEST).binding(
        "eth-usd", require_enabled=False
    )
    tusd = load_numeric_fact_manifest(_TUSD_MANIFEST).binding(
        "tusd-reserves", require_enabled=False
    )

    assert eth.config["chain_id"] == tusd.config["chain_id"] == 1
    assert eth.config["proxy_address"].lower() == (
        "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
    )
    assert tusd.config["proxy_address"].lower() == (
        "0xbe456fd14720c3accc30a2013bffd782c9cb75d5"
    )
    assert eth.config["expected_decimals"] == 8
    assert tusd.config["expected_decimals"] == 18
    assert eth.config["expected_description"] == "ETH / USD"
    assert tusd.config["expected_description"] == "TUSD Reserves"
    assert eth.schedule["deviation_threshold_basis_points"] == 50
    assert tusd.schedule == {
        "expected_update_interval_seconds": 86400,
        "deviation_threshold_basis_points": 500,
    }
    assert eth.config["confirmations"] > 0
    assert tusd.config["confirmations"] > 0
    assert eth.config["history_start"]
    assert tusd.config["history_start"]

    forbidden_keys = {
        "wallet",
        "signer",
        "private_key",
        "link_balance",
        "transaction",
    }
    for path in (_ETH_MANIFEST, _TUSD_MANIFEST):
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert forbidden_keys.isdisjoint(_all_keys(payload))


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda payload: payload.update({"unexpected": True}),
            "unexpected root fields",
        ),
        (
            lambda payload: payload["bindings"][0]["dimensions"].update(
                {"provider_payload": "arbitrary"}
            ),
            "unexpected=provider_payload",
        ),
        (
            lambda payload: payload["bindings"][0].update({"unit": "EUR"}),
            "expected_from_dimension=USD",
        ),
        (
            lambda payload: payload["bindings"][0]["schedule"].update(
                {"deviation_threshold_basis_points": "50"}
            ),
            "schedule deviation_threshold_basis_points must be a nonnegative integer",
        ),
    ),
)
def test_manifest_rejects_unknown_shape_dimensions_and_unit_mismatch(
    tmp_path: Path,
    mutation,
    message: str,
) -> None:
    payload = json.loads(_ETH_MANIFEST.read_text(encoding="utf-8"))
    mutation(payload)
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_numeric_fact_manifest(path)


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        keys = {str(key).lower() for key in value}
        for nested in value.values():
            keys.update(_all_keys(nested))
        return keys
    if isinstance(value, list):
        keys: set[str] = set()
        for nested in value:
            keys.update(_all_keys(nested))
        return keys
    return set()
