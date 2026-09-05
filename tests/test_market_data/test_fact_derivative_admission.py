from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import hashlib
import json
from types import SimpleNamespace

import pytest

from market_data.canonical import CanonicalFactRecord
from market_data.canonical_adapters import canonicalize_derivative_state_feature
from market_data.canonical_storage import record_to_storage_row
from market_data.market_state import derive_derivative_state_features, derivative_state_input_fingerprint
from portal.backend.service.storage.repos import fact_derivative_admission as admission, market_data
from tests.test_market_data.test_market_state_phase3 import BASE, SOURCE, _oi_record, _funding_record


def _fixture(*, missing_oi=False, missing_funding=False, missing_previous=False):
    previous = _oi_record(50, 0, 100, 1)
    current = _oi_record(50, 60, 110, 2)
    funding = _funding_record(51, 60, 3)
    oi = [] if missing_oi else ([current] if missing_previous else [previous, current])
    typed = derive_derivative_state_features(instrument_id="instrument", oi_records=oi,
        funding_records=[] if missing_funding else [funding], oi_gaps=(), series_id=52,
        expected_oi_interval_seconds=60, computed_at=BASE + timedelta(seconds=121))[0]
    root = record_to_storage_row(CanonicalFactRecord(fact=canonicalize_derivative_state_feature(typed),
        series_id=52, source_id=1, revision=1, market_commit_seq=10), series_dimensions={})
    rows = {}
    for record in [*oi, *([] if missing_funding else [funding])]:
        canonical = (market_data._funding_rate_to_canonical if record.series_id == 51 else market_data._open_interest_to_canonical)(
            record.fact, source=SOURCE, provenance={})
        stored = record_to_storage_row(CanonicalFactRecord(fact=canonical, series_id=record.series_id,
            source_id=1, revision=1, market_commit_seq=record.market_commit_seq,
            ingestion_run_id=record.ingestion_run_id), series_dimensions={})
        rows[stored["id"]] = stored
    return root, rows, (previous, current, funding)


def test_extracted_derivative_fingerprint_preserves_the_exact_v1_hash():
    root, _, (previous, current, funding) = _fixture()
    material = {"schema_version": "market.derivative_state_input.v1", "instrument_id": "instrument",
        "effective_at": root["observation_time"].isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "oi": {"series_id": 50, "commit_seq": 2, "row_hash": current.fact.row_hash, "previous_commit_seq": 1},
        "funding": {"series_id": 51, "commit_seq": 3, "row_hash": funding.fact.row_hash}}
    expected = hashlib.sha256(json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
    assert root["payload"]["input_fingerprint"] == expected
    assert derivative_state_input_fingerprint(instrument_id="instrument", effective_at=root["observation_time"],
        oi_record=current, previous_oi_record=previous, funding_record=funding) == expected
    assert derivative_state_input_fingerprint(instrument_id="instrument", effective_at=root["observation_time"],
        oi_record=current, previous_oi_record=replace(previous, market_commit_seq=4), funding_record=funding) != expected


def test_derivative_decoder_preserves_the_exact_retained_v1_precision():
    root, _, _ = _fixture()
    decoded = admission.decode_derivative_state_feature_record(admission.record_from_storage_row(root)).fact
    assert decoded.oi_log_change == Decimal(root["payload"]["oi_log_change"])
    assert decoded.material_hash == root["provenance"]["_qt_derivative_state_evidence"]["legacy_material_hash"]
    with pytest.raises(ValueError, match="OI log change does not reconcile"):
        replace(decoded, oi_log_change=decoded.oi_log_change + Decimal("0.00000000000000000000000000001"))


@pytest.mark.parametrize("mode", ["valid", "funding_only", "oi_only", "no_previous", "missing_current", "missing_previous",
                                 "wrong_commit", "future_known", "sample", "state", "budget", "fingerprint", "output", "v2_schema"])
def test_derivative_sources_are_exact_causal_commits_not_latest_values(monkeypatch, mode):
    root, rows, _ = _fixture(missing_oi=mode == "funding_only", missing_funding=mode == "oi_only",
                             missing_previous=mode == "no_previous")
    if mode in {"fingerprint", "output"}:
        record = admission.record_from_storage_row(root)
        # Keep typed legacy evidence coherent while corrupting the declared
        # input selection. A full canonical hash alone cannot detect this.
        typed = admission.decode_derivative_state_feature_record(record).fact
        changed = (replace(typed, input_fingerprint="f" * 64) if mode == "fingerprint"
                   else replace(typed, funding_rate=Decimal("0.0002")))
        root = record_to_storage_row(CanonicalFactRecord(fact=canonicalize_derivative_state_feature(
            changed), series_id=record.series_id,
            source_id=record.source_id, revision=record.revision,
            market_commit_seq=record.market_commit_seq), series_dimensions={})
    previous_id = next((key for key, row in rows.items() if row["market_commit_seq"] == 1), None)
    alternative_id = None
    if previous_id:
        prior = admission.record_from_storage_row(rows[previous_id])
        alternative = record_to_storage_row(CanonicalFactRecord(fact=prior.fact, series_id=prior.series_id,
            source_id=prior.source_id, revision=2, market_commit_seq=4,
            ingestion_run_id=prior.ingestion_run_id), series_dimensions={})
        alternative_id = alternative["id"]
        rows[alternative["id"]] = alternative
    returned = []
    def execute(statement, params):
        sql = str(statement)
        assert "source.market_commit_seq<=requested.root_commit AND source.known_at<=requested.known_at" in sql
        assert "source_series.instrument_id=requested.instrument_id" in sql
        assert "root_series.instrument_id=requested.instrument_id" in sql
        requests = json.loads(params["requests"])
        assert len(requests) <= 128
        pairs = []
        for request in requests:
            for identity, row in rows.items():
                if row["series_id"] != request["series_id"] or row["observation_time"].isoformat() != request["sample_time"]:
                    continue
                if request["commit_seq"] is not None and row["market_commit_seq"] != request["commit_seq"]:
                    continue
                if (mode == "missing_current" and request["role"] == "oi") or (mode == "missing_previous" and request["role"] == "previous_oi"):
                    continue
                pairs.append((root["id"], request["role"], identity))
        returned.extend(pairs)
        return SimpleNamespace(all=lambda: pairs)
    def read(session, ids, **kwargs):
        selected = {identity: deepcopy(rows[identity]) for identity in ids}
        current = next((row for row in selected.values() if row["market_commit_seq"] == 2), None)
        if mode in {"wrong_commit", "future_known", "sample", "state", "v2_schema"}:
            field, value = {
                "wrong_commit": ("market_commit_seq", 11), "future_known": ("known_at", BASE + timedelta(days=1)),
                "sample": ("observation_time", BASE), "state": ("state", "invalidated"),
                "v2_schema": ("payload_schema_id", "derivatives.open_interest.v2"),
            }[mode]
            current[field] = value
        return selected
    monkeypatch.setattr(admission, "read_canonical_dependency_rows", read)
    # Wrong metadata is tested independently of the canonical codec's earlier
    # hash guard. Real rows and legacy decoding still validate all other cases.
    real_codec = admission.record_from_storage_row
    if mode in {"wrong_commit", "future_known", "sample", "state", "v2_schema"}:
        monkeypatch.setattr(admission, "record_from_storage_row", lambda row: real_codec(root) if row["id"] == root["id"] else None)
        monkeypatch.setattr(market_data, "_canonical_to_open_interest_record", lambda row: SimpleNamespace())
    kwargs = dict(rows=[root], object_store=None, max_rows=2 if mode == "budget" else 10, max_logical_bytes=1024 * 1024)
    if mode in {"valid", "funding_only", "oi_only", "no_previous"}:
        result = admission.resolve_derivative_source_revisions(SimpleNamespace(execute=execute), **kwargs)
        expected = {identity for identity in rows if identity != alternative_id}
        assert {row["id"] for row in result} == expected
        assert market_data._preserves_canonical_revision_history("market.derivative_state.v1")
    else:
        with pytest.raises(RuntimeError, match="canonical_derivative_"):
            admission.resolve_derivative_source_revisions(SimpleNamespace(execute=execute), **kwargs)
