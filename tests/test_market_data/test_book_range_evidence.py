from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from data_providers.streams.coinbase import quiet_transport_witness
from market_data.range_evidence import producer_range_evidence, is_complete_range_evidence
from market_data.gaps import recorded_gaps_cover_interval
from market_data.fact_registry import get_fact_contract
from portal.backend.service.storage.repos.book_range_evidence import (
    project_book_range_evidence,
)
from portal.backend.service.research.planning import _coverage_for_requirement
from portal.backend.service.research.execution import _quality

BASE = datetime(2026, 9, 1, tzinfo=UTC)


def position(ordinal, epoch=1):
    return {"definition_id": "definition", "session_id": "session", "connection_epoch": epoch,
            "provider_product_id": "EXAMPLE-PRODUCT", "receive_ordinal": ordinal,
            "event_ordinal": 0, "provider_sequence_num": ordinal + 9}


def raw(ordinal, offset, *, heartbeat=False, counter=None, event_offset=None):
    timestamp = (BASE + timedelta(seconds=offset)).isoformat()
    event = ({"heartbeat_counter": counter} if heartbeat else
             {"type": "update", "product_id": "EXAMPLE-PRODUCT", "updates": [
                 {"side": "bid", "price_level": "100", "new_quantity": "1",
                  "event_time": (BASE + timedelta(seconds=event_offset if event_offset is not None else offset)).isoformat()}]})
    return SimpleNamespace(**{key: value for key, value in position(ordinal).items()
                              if key != "provider_sequence_num"}, provider="COINBASE",
                           received_at=BASE + timedelta(seconds=offset),
                           raw_frame=json.dumps({"channel": "heartbeats" if heartbeat else "l2_data",
                               "sequence_num": ordinal + 9, "timestamp": timestamp, "events": [event]}).encode())


def transport():
    return [raw(1, .2), raw(2, 1.1, heartbeat=True, counter=50),
            raw(3, 2.1, heartbeat=True, counter=51), raw(4, 2.2)]


def fact(second, ordinal, *, interval="valid", epoch=1):
    return {"id": f"fact-{ordinal}", "series_id": 20, "source_id": 19,
            "source_identity_key": "derived-source", "state": "active", "fact_type": "market.bbo",
            "observation_time": BASE + timedelta(seconds=second),
            "known_at": BASE + timedelta(seconds=second + 1), "row_hash": str(ordinal) * 64,
            "payload": {"validity_interval_id": interval},
            "provenance": {"_qt_bbo_evidence": {"source_l2_series_id": 16,
                                                  "source_position": position(ordinal, epoch)}}}


class Witnesses:
    def __init__(self, *, quiet=True):
        self.proven = quiet
        self.intervals = {"valid": {"interval_id": "valid", "series_id": 16, "revision": 1,
            "status": "open_valid", "opening_effective_at": BASE, "opening_known_at": BASE,
            "opening_session_id": "session", "opening_connection_epoch": 1,
            "closing_effective_at": None, "known_at": BASE}}

    def validity(self, identity):
        return self.intervals.get(identity)

    def quiet(self, left, right, start, end):
        if not self.proven:
            return None
        proof = quiet_transport_witness(transport(), left=left, right=right, start=start, end=end)
        return {"known_at": BASE + timedelta(seconds=4), "transport": proof, "archives": [
            {"manifest_id": "archive", "object_sha256": "a" * 64, "completion_receipt_id": "receipt"}
        ]} if proof else None


def project(witnesses=None, rows=None):
    return project_book_range_evidence(rows=rows or [fact(0, 1), fact(2, 4)],
        start=BASE, end=BASE + timedelta(seconds=3), watermark=77, witnesses=witnesses or Witnesses())


def test_quiet_interval_proven_without_synthetic_facts_and_json_roundtrip():
    evidence = project()
    assert len(evidence) == 1
    row = evidence[0]
    assert is_complete_range_evidence(json.loads(json.dumps(row)))
    assert row["known_at"] == "2026-09-01T00:00:04.000000Z"
    assert recorded_gaps_cover_interval(start=BASE + timedelta(seconds=1),
                                      end=BASE + timedelta(seconds=2), evidence=evidence)
    assert _quality(SimpleNamespace(quality_evidence=evidence, gap_policy="reject", missing_coverage=[]))["status"] == "clean"


@pytest.mark.parametrize("failure", ["missing_frame", "heartbeat_jump", "wrong_provider", "event_in_hole", "wrong_scope"])
def test_quiet_proof_fails_for_unproven_transport(failure):
    records = transport()
    if failure == "missing_frame":
        records.pop(1)
    elif failure == "heartbeat_jump":
        records[2] = raw(3, 2.1, heartbeat=True, counter=55)
    elif failure == "wrong_provider":
        records[1].provider = "UNKNOWN"
    elif failure == "event_in_hole":
        records[3] = raw(4, 2.2, event_offset=1.5)
    else:
        records[1].session_id = "another-session"
    assert quiet_transport_witness(records, left=position(1), right=position(4),
        start=BASE + timedelta(seconds=1), end=BASE + timedelta(seconds=2)) is None


def test_missing_processing_or_archive_witness_remains_unexplained():
    assert project(Witnesses(quiet=False)) == []


def test_partial_second_disconnect_is_not_quiet_even_with_adjacent_populated_buckets():
    witnesses = Witnesses()
    witnesses.intervals["valid"].update(status="closed_invalidated",
        closing_effective_at=BASE + timedelta(seconds=.8), known_at=BASE + timedelta(seconds=1))
    witnesses.intervals["recovered"] = {**witnesses.intervals["valid"], "interval_id": "recovered",
        "opening_effective_at": BASE + timedelta(seconds=1.2), "opening_known_at": BASE + timedelta(seconds=2),
        "opening_connection_epoch": 2, "status": "open_valid", "closing_effective_at": None}
    evidence = project(witnesses, [fact(0, 1), fact(1, 4, interval="recovered", epoch=2)])
    assert len(evidence) == 1 and not is_complete_range_evidence(evidence[0])
    assert evidence[0]["start"] == "2026-09-01T00:00:00.800000Z"
    assert evidence[0]["end"] == "2026-09-01T00:00:01.200000Z"
    assert _quality(SimpleNamespace(quality_evidence=evidence, gap_policy="reject", missing_coverage=[]))["recorded_gap_count"] == 1


def test_mismatched_validity_source_fails_loud():
    witnesses = Witnesses()
    witnesses.intervals["valid"]["series_id"] = 999
    with pytest.raises(RuntimeError, match="lineage_disagreement"):
        project(witnesses)


def test_tampered_complete_proof_fails_loud():
    row = project()[0]
    row["end"] = "2026-09-01T00:00:03.000000Z"
    with pytest.raises(ValueError, match="hash disagreement"):
        recorded_gaps_cover_interval(start=BASE, end=BASE + timedelta(seconds=3), evidence=[row])


def test_generic_planning_admits_producer_quiet_evidence_but_not_a_different_source():
    class Store:
        def list_series_metadata(self, **kwargs):
            return [{"series_id": 20, "fact_type": "market.bbo", "contract_version": "market.bbo.v1",
                     "timeframe_seconds": 1, "dimensions": {}}]
        def read_series_records(self, **kwargs):
            return [SimpleNamespace(fact=SimpleNamespace(observation_time=BASE + timedelta(seconds=n))) for n in (0, 2)]
        def list_gap_evidence(self, **kwargs):
            return self.evidence
    store = Store()
    store.evidence = project()
    request = {"alias": "book", "instrument_id": "any-instrument", "fact_type": "market.bbo",
               "contract_version": "market.bbo.v1", "timeframe_seconds": 1, "alignment": "exact_interval",
               "required_start": BASE, "required_end": BASE + timedelta(seconds=3),
               "source_policy": {"mode": "exact", "source_identity_key": "derived-source"}}
    assert _coverage_for_requirement(request, store=store, as_of_commit_seq=77)[0] == []
    store.evidence = []
    assert _coverage_for_requirement(request, store=store, as_of_commit_seq=77)[0][0]["reason"] == "source_bound_interval_unrecorded"
    store.evidence = [producer_range_evidence(series_id=20, source_id=100, source_identity_key="different-source",
        start=BASE, end=BASE + timedelta(seconds=3), status="complete", known_at=BASE + timedelta(seconds=4),
        commit_seq=77, witness={"producer": "another-producer"})]
    assert _coverage_for_requirement(request, store=store, as_of_commit_seq=77)[0]


def test_existing_dense_contracts_and_old_gap_evidence_are_unchanged():
    for fact_type in ("candle.ohlcv", "market.trade_flow"):
        contract = get_fact_contract(fact_type)
        assert contract.default_alignment == "exact_interval"
        assert contract.range_evidence_owner is None
    assert not is_complete_range_evidence({"classification": "source_sparse", "start": BASE, "end": BASE + timedelta(seconds=1)})


def test_quiet_proof_cannot_be_known_before_its_validity_witness():
    witnesses = Witnesses()
    witnesses.intervals["valid"]["known_at"] = BASE + timedelta(minutes=1)
    assert project(witnesses)[0]["known_at"] == "2026-09-01T00:01:00.000000Z"
