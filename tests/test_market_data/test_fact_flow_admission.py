from dataclasses import asdict, replace
from datetime import timedelta
import json
from types import SimpleNamespace

import pytest

from market_data.canonical import CanonicalFactRecord
from market_data.canonical_adapters import canonicalize_market_trade, canonicalize_trade_flow
from market_data.canonical_storage import record_to_storage_row
from market_data.contracts import SourceIdentity
from market_data.structure import aggregate_trade_bucket, bucket_start_for
from portal.backend.service.storage.repos import fact_flow_admission as admission
from tests.test_market_data.test_market_structure_phase1 import _fact, _coverage

SOURCE = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream", adapter_version="flow.fixture.v1")


def _stored_trade(fact, *, revision=1, commit=1, series=20, state="active"):
    canonical = replace(canonicalize_market_trade(fact, source=SOURCE), state=state)
    return record_to_storage_row(CanonicalFactRecord(fact=canonical, series_id=series, source_id=1,
        revision=revision, market_commit_seq=commit), series_dimensions={})


def _fixture(*, producer="bounded", zero=False, uncovered=False):
    fact, _ = _fact()
    fact = replace(fact, received_at=fact.provider_event_time + timedelta(milliseconds=1),
        accepted_at=fact.provider_event_time + timedelta(milliseconds=2),
        known_at=fact.provider_event_time + timedelta(milliseconds=2),
        coverage_interval_id=None if uncovered else fact.coverage_interval_id)
    duplicate = replace(fact, receive_ordinal=8, raw_record_id="second-delivery",
        received_at=fact.received_at + timedelta(milliseconds=10), accepted_at=fact.accepted_at + timedelta(milliseconds=10),
        known_at=fact.known_at + timedelta(milliseconds=10))
    coverage = None if uncovered else _coverage(fact)
    start = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    inputs = [] if zero else ([duplicate] if producer == "continuous" else [fact, duplicate])
    typed = aggregate_trade_bucket(inputs, interval_seconds=1, bucket_start=start,
        coverage=coverage, computed_at=start + timedelta(seconds=5))
    row = record_to_storage_row(CanonicalFactRecord(fact=canonicalize_trade_flow(typed, source=SOURCE),
        series_id=40, source_id=1, revision=1, market_commit_seq=10), series_dimensions={})
    root = admission.TradeFlowRoot(row, typed, "flow-fixture", coverage)
    sources = [] if zero else [_stored_trade(fact), _stored_trade(duplicate, revision=2, commit=2)]
    return root, sources, (fact, duplicate)


@pytest.mark.parametrize("producer", ["bounded", "continuous"])
def test_historical_producer_selection_keeps_all_compatible_deliveries(producer):
    root, rows, _ = _fixture(producer=producer)
    result = admission.select_trade_flow_inputs(root, rows)
    assert {row["id"] for row in result} == {row["id"] for row in rows}
    assert root.fact.first_receive_ordinal == (7 if producer == "bounded" else 8)


def test_uncovered_populated_flow_keeps_exact_raw_inputs_without_fabricating_coverage():
    root, rows, _ = _fixture(uncovered=True)
    assert not root.fact.aggregate_complete and not root.fact.archive_complete
    assert len(admission.select_trade_flow_inputs(root, rows)) == 2
    assert admission.trade_flow_prefix_requirements([root]) == ([], [])


def test_zero_preserves_candidates_and_invalidations_without_recertifying_market_quality():
    root, _, (fact, _) = _fixture(zero=True)
    assert admission.select_trade_flow_inputs(root, []) == []
    unrelated = _stored_trade(replace(fact, coverage_interval_id=None), series=21)
    assert admission.select_trade_flow_inputs(root, [unrelated]) == [unrelated]
    # Latest invalidation is applied before active-state filtering, as in the
    # original continuous reader; it must not unmask the older active revision.
    invalidated = _stored_trade(fact, revision=2, commit=2, state="invalidated")
    rows = [unrelated, _stored_trade(fact), invalidated]
    assert {row["id"] for row in admission.select_trade_flow_inputs(root, rows)} == {row["id"] for row in rows}
    assert root.fact.trade_count == 0


def test_unrecorded_arbitrary_correction_subsets_are_not_invented():
    root, rows, (_, duplicate) = _fixture(uncovered=True)
    changed = replace(duplicate, price=duplicate.price * 2, quote_notional=duplicate.quote_notional * 2)
    with pytest.raises(RuntimeError, match="source_derivation_unproven"):
        admission.select_trade_flow_inputs(root, [rows[0], _stored_trade(changed, revision=2, commit=2)])


def test_covered_flow_retains_snapshot_and_old_session_deliveries_without_fabricating_revisions():
    root, _, (fact, duplicate) = _fixture(producer="continuous")
    snapshot = replace(fact, coverage_interval_id=None, delivery_kind="snapshot")
    old_session = replace(fact, coverage_interval_id="older-coverage", connection_epoch=1)
    rows = [_stored_trade(snapshot), _stored_trade(old_session, series=21)]
    result = admission.select_trade_flow_inputs(root, rows)
    assert result == sorted(rows, key=lambda row: row["id"])
    assert all(row["provenance"]["_qt_trade_evidence"]["raw_record_id"] != duplicate.raw_record_id for row in result)
    # Bounded collection establishes coverage before accepting its translated
    # trades. Coverage.known_at is not an upper bound on those input clocks.
    accepted = root.coverage.known_at + timedelta(milliseconds=1)
    late = _stored_trade(replace(fact, known_at=accepted, accepted_at=accepted))
    assert admission.select_trade_flow_inputs(root, [late]) == [late]
    # A raw-only bucket has no synthetic canonical input rows; its full raw
    # prefix must independently pass the archive owner's mandatory admission.
    assert admission.select_trade_flow_inputs(root, []) == []


def test_covered_flow_preserves_partial_quality_and_all_correction_evidence():
    root, _, (fact, duplicate) = _fixture()
    from market_data.structure import ArchiveStatus
    coverage = replace(root.coverage, archive_status=ArchiveStatus.PENDING)
    typed = aggregate_trade_bucket([fact], interval_seconds=1, bucket_start=root.fact.bucket_start,
        coverage=coverage, computed_at=root.fact.known_at)
    partial = replace(root, fact=typed, coverage=coverage)
    changed = replace(duplicate, price=duplicate.price * 2, quote_notional=duplicate.quote_notional * 2)
    rows = [_stored_trade(fact), _stored_trade(changed, revision=2, commit=2)]
    assert len(admission.select_trade_flow_inputs(partial, rows)) == 2
    assert partial.fact.archive_complete is False


@pytest.mark.parametrize("damage", ["future_known", "future_commit", "source", "window"])
def test_source_clocks_and_scope_are_rechecked_after_hydration(damage):
    root, _, (fact, _) = _fixture()
    if damage == "future_known":
        fact = replace(fact, known_at=root.fact.known_at + timedelta(days=1), accepted_at=root.fact.known_at + timedelta(days=1))
    elif damage == "window":
        fact = replace(fact, provider_event_time=fact.provider_event_time - timedelta(days=1))
    row = _stored_trade(fact, commit=11 if damage == "future_commit" else 1)
    if damage == "source":
        record = admission.record_from_storage_row(row)
        row = record_to_storage_row(CanonicalFactRecord(fact=record.fact, series_id=record.series_id, source_id=2,
            revision=1, market_commit_seq=1), series_dimensions={})
    with pytest.raises(RuntimeError, match="source_scope_mismatch"):
        admission.select_trade_flow_inputs(root, [row])


def test_coverage_prefixes_preserve_watermarks_and_bind_each_named_endpoint():
    root, _, _ = _fixture()
    later = replace(root, row={**root.row, "id": "later-root"},
                    coverage=replace(root.coverage, archive_complete_through_ordinal=25))
    prefixes, witnesses = admission.trade_flow_prefix_requirements([root, later])
    assert len(prefixes) == 1 and prefixes[0]["receive_ordinal"] == 25
    assert len(witnesses) == 6 and len({item["root_fact_version_id"] for item in witnesses}) == 6
    assert {item["raw_record_id"] for item in witnesses} == {root.coverage.opening_raw_record_id, root.coverage.last_raw_record_id}
    assert all(item["first_receive_ordinal"] == item["receive_ordinal"] and item["requested_channel"] == "market_trades"
               and item["provider"] == SOURCE.provider for item in witnesses)
    with pytest.raises(RuntimeError, match="closing_identity_invalid"):
        admission.trade_flow_prefix_requirements([replace(root, coverage=replace(root.coverage, closing_raw_record_id=None))])
    with pytest.raises(RuntimeError, match="prefix_scope_conflict"):
        admission.trade_flow_prefix_requirements([root, replace(later, coverage=replace(later.coverage, provider_product_id="ETH-USD"))])


@pytest.mark.parametrize("damage", [None, "bytes", "root", "coverage", "clock", "product", "channel", "hash"])
def test_immutable_coverage_scope_and_byte_budget_are_verified(damage):
    root, _, _ = _fixture()
    coverage = replace(root.coverage, channel="level2") if damage == "channel" else root.coverage
    metadata = {**asdict(coverage), "id": "coverage-version", "material_hash": coverage.material_hash,
        "status": coverage.status.value, "ordering_assurance": coverage.ordering_assurance.value,
        "archive_status": coverage.archive_status.value}
    if damage == "hash":
        metadata["material_hash"] = "f" * 64
    transferred = []
    def execute(statement, params):
        sql = str(statement)
        if "product_scope_valid" in sql:
            assert "product.known_at<=coverage.known_at" in sql and "product.instrument_id=series.instrument_id" in sql
            assert len(json.loads(params["requests"])) <= 128
            found = [] if damage == "root" else [{"root_id": root.row["id"], "instrument_id": root.instrument_id,
                "coverage_version_id": None if damage == "coverage" else "coverage-version",
                "coverage_known_at": root.fact.known_at + timedelta(days=1) if damage == "clock" else coverage.known_at,
                "product_scope_valid": damage != "product", "logical_bytes": 200}]
        else:
            transferred.append(True)
            found = [metadata]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: found))
    args = dict(rows=[root.row], max_rows=10, max_logical_bytes=100 if damage == "bytes" else 1000)
    if damage is None:
        result = admission.load_trade_flow_roots(SimpleNamespace(execute=execute), **args)
        assert len(result) == 1 and result[0] == root
    else:
        with pytest.raises(RuntimeError, match="canonical_flow_|market_stream_coverage_storage_corrupt"):
            admission.load_trade_flow_roots(SimpleNamespace(execute=execute), **args)
        if damage in {"bytes", "root", "coverage", "clock", "product"}:
            assert transferred == []


@pytest.mark.parametrize("damage", [None, "budget", "missing"])
def test_bounded_source_query_preserves_the_complete_causal_window(monkeypatch, damage):
    root, rows, _ = _fixture()
    monkeypatch.setattr(admission, "load_trade_flow_roots", lambda *args, **kwargs: [root])
    by_id = {row["id"]: row for row in rows}
    def execute(statement, params):
        sql = str(statement)
        assert len(json.loads(params["requests"])) <= 128
        assert "source.market_commit_seq<=requested.root_commit" in sql
        assert "source.known_at<=requested.known_at" in sql
        assert json.loads(params["requests"])[0]["known_at"] == root.fact.known_at.isoformat()
        assert "source.state" not in sql
        found = [(root.row["id"], row["id"]) for row in rows]
        return SimpleNamespace(all=lambda: found)
    monkeypatch.setattr(admission, "read_canonical_dependency_rows", lambda *args, **kwargs: {} if damage == "missing" else by_id)
    kwargs = dict(rows=[root.row], object_store=None, max_rows=1 if damage == "budget" else 10, max_logical_bytes=1024**2)
    if damage is None:
        assert {row["id"] for row in admission.resolve_trade_flow_source_revisions(SimpleNamespace(execute=execute), **kwargs)} == set(by_id)
    else:
        with pytest.raises(RuntimeError, match="canonical_flow_"):
            admission.resolve_trade_flow_source_revisions(SimpleNamespace(execute=execute), **kwargs)
