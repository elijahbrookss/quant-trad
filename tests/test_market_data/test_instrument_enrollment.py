from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_data.instrument_enrollment import (
    InstrumentEnrollmentManifest,
    load_instrument_enrollment_manifest,
)


MANIFEST = (
    Path(__file__).parents[2]
    / "config/market_data/coinbase_perpetual_instruments.v1.json"
)


def test_reviewed_coinbase_instrument_manifest_is_strict_and_hashed() -> None:
    manifest = load_instrument_enrollment_manifest(MANIFEST)

    assert manifest.fleet_id == "coinbase_perpetuals"
    assert len(manifest.instruments) == 3
    assert len(manifest.manifest_hash) == 64
    assert {row.symbol for row in manifest.instruments} == {
        "BIP-20DEC30-CDE",
        "ETP-20DEC30-CDE",
        "SLP-20DEC30-CDE",
    }
    for row in manifest.instruments:
        fields = row.metadata["instrument_fields"]
        assert fields["qty_step"] == "1"
        assert fields["can_short"] is True
        assert fields["short_requires_borrow"] is False
        assert fields["has_funding"] is True


def test_instrument_manifest_rejects_unknown_fields_and_hash_drift(
    tmp_path: Path,
) -> None:
    raw = json.loads(MANIFEST.read_text(encoding="utf-8"))
    raw["unexpected"] = True
    path = tmp_path / "unexpected.json"
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="fields_invalid"):
        load_instrument_enrollment_manifest(path)

    clean = json.loads(MANIFEST.read_text(encoding="utf-8"))
    parsed = InstrumentEnrollmentManifest.from_dict(clean)
    clean["manifest_hash"] = parsed.manifest_hash
    clean["instruments"][0]["metadata"]["instrument_fields"]["tick_size"] = "10"
    drifted = tmp_path / "drifted.json"
    drifted.write_text(json.dumps(clean), encoding="utf-8")
    with pytest.raises(ValueError, match="hash_mismatch"):
        load_instrument_enrollment_manifest(drifted)
