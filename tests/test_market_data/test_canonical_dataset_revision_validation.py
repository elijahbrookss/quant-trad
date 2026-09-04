"""Canonical funding history must not be treated as legacy sample-time rows."""
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest

from market_data.canonical import CanonicalFactRecord, build_canonical_fact_provenance_hash
from market_data.contracts import build_quality_hash
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos.market_data import _build_material_hash
from tests.test_market_data.test_canonical_fact_contracts import _funding_fact


@pytest.mark.parametrize("damage", [None, "watermark", "duplicate", "hash"])
def test_canonical_scheduled_history_accepts_revisions_but_rejects_corruption(damage):
    fact = _funding_fact()
    revised = replace(fact, known_at=fact.known_at + timedelta(seconds=1),
                      accepted_at=fact.accepted_at + timedelta(seconds=1), state="invalidated")
    records = [CanonicalFactRecord(fact=value, series_id=41, source_id=7, revision=index,
                                  market_commit_seq=index) for index, value in enumerate((fact, revised), 1)]
    identity = dict(identity_key="canonical-funding-history", instrument_id="fixture",
        fact_type=fact.fact_type, contract_version=fact.payload_schema_id, timeframe_seconds=None)
    entry = {**identity, "dataset_id": "fixture", "series_id": 41,
        "range_start": fact.observation_time, "range_end": fact.observation_time + timedelta(minutes=1),
        "max_commit_seq": 2, "row_count": 2, "quality_evidence": [], "quality_hash": build_quality_hash([]),
        "source_summary": {"record_selection": "all_canonical_revisions.v1"},
        "material_hash": _build_material_hash(fact_type=fact.fact_type, series_identity=identity, records=records),
        "provenance_hash": build_canonical_fact_provenance_hash(records)}
    if damage == "watermark":
        entry["max_commit_seq"] = 1
    elif damage == "duplicate":
        records[1] = records[0]
    elif damage == "hash":
        entry["material_hash"] = "0" * 64
    store = SimpleNamespace(read_dataset_fact_revisions=lambda **kwargs: records)
    if damage is not None:
        with pytest.raises(RuntimeError, match="revision_disagreement|duplicate typed version|hash_disagreement"):
            validate_frozen_dataset_series(store=store, entry=entry)
    else:
        verified, _, found = validate_frozen_dataset_series(store=store, entry=entry)
        assert found == records and verified["row_count"] == 2
        assert verified["loaded_range"]["first_effective_at"] == verified["loaded_range"]["last_effective_at"]
