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


@pytest.mark.parametrize("path,expected", [
    (MANIFEST, "a39169ad14ee3fde29d732781776cfcf5c001e3b595ae7f19618ffcddb9c9db9"),
    (L2_MANIFEST, "fa29d8793e00d61828762e4fd3be766cbd8e383fe82f8656a48440615532b762"),
])
def test_existing_reviewed_manifest_hash_does_not_gain_an_optional_field(path, expected):
    manifest = load_stream_enrollment_manifest(path)
    assert manifest.manifest_hash == expected
    assert all("max_inflight_segments" not in row for row in manifest.to_dict()["enrollments"])


def test_reviewed_queue_limit_roundtrips_and_is_part_of_manifest_identity():
    raw = load_stream_enrollment_manifest(MANIFEST).to_dict()
    original_hash = raw.pop("manifest_hash")
    raw["enrollments"][0]["max_inflight_segments"] = 64
    manifest = StreamEnrollmentManifest.from_dict(raw)
    assert manifest.manifest_hash != original_hash
    restored = StreamEnrollmentManifest.from_dict(manifest.to_dict())
    assert restored.enrollments[0].max_inflight_segments == 64
    assert restored.manifest_hash == manifest.manifest_hash
    changed = manifest.to_dict()
    changed["enrollments"][0]["max_inflight_segments"] = 65
    with pytest.raises(ValueError, match="manifest_hash_mismatch"):
        StreamEnrollmentManifest.from_dict(changed)


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "64"])
def test_reviewed_queue_limit_rejects_invalid_values(value):
    raw = json.loads(MANIFEST.read_text())
    raw["enrollments"][0]["max_inflight_segments"] = value
    with pytest.raises(ValueError, match="max_inflight_segments must be a positive integer"):
        StreamEnrollmentManifest.from_dict(raw)
