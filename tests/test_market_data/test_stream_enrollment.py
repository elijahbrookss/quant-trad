from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_data.stream_enrollment import (
    STREAM_ENROLLMENT_MANIFEST_VERSION,
    StreamEnrollmentManifest,
    load_stream_enrollment_manifest,
)


MANIFEST = Path("config/market_data/coinbase_perpetual_trade_fleet.v1.json")
L2_MANIFEST = Path("config/market_data/coinbase_perpetual_l2_fleet.v1.json")


def test_perpetual_trade_fleet_is_hash_stable_and_continuous() -> None:
    manifest = load_stream_enrollment_manifest(MANIFEST)
    restored = StreamEnrollmentManifest.from_dict(manifest.to_dict())

    assert restored.manifest_hash == manifest.manifest_hash
    assert manifest.schema_version == STREAM_ENROLLMENT_MANIFEST_VERSION
    assert [row.product_contract.provider_product_id for row in manifest.enrollments] == [
        "BIP-20DEC30-CDE",
        "ETP-20DEC30-CDE",
        "SLP-20DEC30-CDE",
    ]
    assert all(row.continuous for row in manifest.enrollments)


@pytest.mark.parametrize("path", [MANIFEST, L2_MANIFEST])
def test_public_market_stream_fleets_do_not_require_credentials(path: Path) -> None:
    manifest = load_stream_enrollment_manifest(path)

    assert {row.auth_mode for row in manifest.enrollments} == {"public"}
    assert all(
        set(row.channels) <= {"market_trades", "level2", "heartbeats"}
        for row in manifest.enrollments
    )


def test_compatible_product_requires_manifest_data_only(tmp_path: Path) -> None:
    raw = json.loads(MANIFEST.read_text(encoding="utf-8"))
    extra = dict(raw["enrollments"][0])
    extra["enrollment_id"] = "coinbase.TEST-USD-CDE.market_trades.v1"
    extra["instrument_id"] = "test-instrument"
    extra["product_contract"] = dict(extra["product_contract"])
    extra["product_contract"]["provider_product_id"] = "TEST-USD-CDE"
    extra["product_contract"]["product_definition_version_id"] = (
        "coinbase.TEST-USD-CDE.product_contract.v1"
    )
    raw["enrollments"].append(extra)
    path = tmp_path / "fleet.json"
    path.write_text(json.dumps(raw), encoding="utf-8")

    manifest = load_stream_enrollment_manifest(path)

    assert manifest.enrollments[-1].product_contract.provider_product_id == "TEST-USD-CDE"


def test_manifest_rejects_mutation_after_hashing() -> None:
    raw = load_stream_enrollment_manifest(MANIFEST).to_dict()
    raw["enrollments"][0]["max_spool_bytes"] += 1

    with pytest.raises(ValueError, match="manifest_hash_mismatch"):
        StreamEnrollmentManifest.from_dict(raw)
