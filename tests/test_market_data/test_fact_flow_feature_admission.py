from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace

import pytest

from market_data.canonical import CanonicalFactRecord
from market_data.canonical_adapters import canonicalize_trade_flow_feature
from market_data.canonical_storage import record_to_storage_row
from market_data.market_state import derive_trade_flow_feature
from portal.backend.service.storage.repos import fact_flow_feature_admission as admission
from tests.test_market_data.test_fact_flow_admission import _fixture, _stored_trade


def _feature(root, trades):
    return derive_trade_flow_feature(series_id=50, source_trade_flow_series_id=root.row["series_id"],
        aggregate=root.fact, trades=trades, computed_at=root.fact.known_at)


@pytest.mark.parametrize("mode", ["bounded", "snapshot", "continuous", "latest_correction", "invalidated"])
def test_feature_owner_accepts_real_historical_material_without_fabricating_delivery(mode):
    root, rows, trades = _fixture(producer="continuous")
    feature = _feature(root, trades)
    if mode == "snapshot":
        rows = [_stored_trade(replace(trades[0], delivery_kind="snapshot", coverage_interval_id=None))]
    elif mode == "continuous":
        rows = [rows[-1]]
    elif mode == "latest_correction":
        other = replace(trades[0], price=trades[0].price * 2, quote_notional=trades[0].quote_notional * 2)
        rows = [_stored_trade(other), _stored_trade(trades[-1], revision=2, commit=2)]
    elif mode == "invalidated":
        rows.append(_stored_trade(trades[-1], revision=3, commit=3, state="invalidated"))
    before = list(rows)
    assert admission._matches_feature_inputs(feature, root.fact, rows)
    assert rows == before


@pytest.mark.parametrize("damage", ["none", "fingerprint", "notional", "conflict"])
def test_feature_owner_never_certifies_missing_or_nonmatching_trade_material(damage):
    root, rows, trades = _fixture()
    feature = _feature(root, trades)
    if damage == "none":
        rows = []
    elif damage == "fingerprint":
        feature = replace(feature, input_fingerprint="a" * 64)
    elif damage == "notional":
        changed = replace(trades[0], price=trades[0].price * 2, quote_notional=trades[0].quote_notional * 2)
        rows = [_stored_trade(changed)]
    else:
        changed = replace(trades[0], price=trades[0].price * 2, quote_notional=trades[0].quote_notional * 2)
        rows = [rows[0], _stored_trade(changed, revision=2, commit=2)]
    assert not admission._matches_feature_inputs(feature, root.fact, rows)


@pytest.mark.parametrize("damage", [None, "scope", "missing_aggregate", "aggregate_input", "feature_input", "budget"])
def test_exact_aggregate_closure_keeps_all_sources_and_rechecks_owner(monkeypatch, damage):
    root, trades, deliveries = _fixture()
    feature = _feature(root, deliveries)
    if damage == "aggregate_input":
        feature = replace(feature, aggregate_input_fingerprint="a" * 64)
    elif damage == "feature_input":
        feature = replace(feature, input_fingerprint="a" * 64)
    canonical = canonicalize_trade_flow_feature(feature)
    # Later invalidation still preserves the original feature and input clocks.
    canonical = replace(canonical, state="invalidated", known_at=feature.known_at + timedelta(seconds=1),
        accepted_at=feature.known_at + timedelta(seconds=1))
    row = record_to_storage_row(CanonicalFactRecord(fact=canonical, series_id=50, source_id=2,
        revision=2, market_commit_seq=20), series_dimensions={})
    def resolve(session, **kwargs):
        request = kwargs["requests"][0]
        assert request == dict(root_id=row["id"], role="aggregate", series_id=40, fact_type="market.trade_flow",
            material_hash=root.fact.material_hash, commit_seq=20, known_at=canonical.known_at)
        return {root.row["id"]: root.row}, {(row["id"], "aggregate"): [root.row["id"]]}
    monkeypatch.setattr(admission, "resolve_material_source_revisions", resolve)
    monkeypatch.setattr(admission, "load_trade_flow_source_closure", lambda *args, **kwargs:
        ([] if damage == "missing_aggregate" else [root], {item["id"]: item for item in trades},
         {root.row["id"]: [item["id"] for item in trades]}))
    def execute(statement, params):
        assert "source.instrument_id=root.instrument_id" in str(statement)
        assert json.loads(params["requests"])[0]["interval_seconds"] == 1
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [] if damage == "scope" else [row["id"]]))
    kwargs = dict(rows=[row], object_store=None, max_rows=2 if damage == "budget" else 10, max_logical_bytes=1024**2)
    if damage is None:
        result = admission.resolve_flow_feature_source_revisions(SimpleNamespace(execute=execute), **kwargs)
        assert result == sorted([root.row, *trades], key=lambda item: item["id"])
    else:
        with pytest.raises(RuntimeError, match="canonical_flow_feature_"):
            admission.resolve_flow_feature_source_revisions(SimpleNamespace(execute=execute), **kwargs)


def test_repeated_features_bound_total_derivation_visits_not_only_unique_sources(monkeypatch):
    aggregate, trades, deliveries = _fixture()
    feature = _feature(aggregate, deliveries)
    rows = [record_to_storage_row(CanonicalFactRecord(
        fact=canonicalize_trade_flow_feature(feature, provenance={"delivery": index}),
        series_id=50, source_id=2, revision=index + 1, market_commit_seq=20 + index), series_dimensions={}) for index in range(2)]
    def resolve(session, **kwargs):
        return {aggregate.row["id"]: aggregate.row}, {
            (row["id"], "aggregate"): [aggregate.row["id"]] for row in rows}
    monkeypatch.setattr(admission, "resolve_material_source_revisions", resolve)
    monkeypatch.setattr(admission, "load_trade_flow_source_closure", lambda *args, **kwargs:
        ([aggregate], {row["id"]: row for row in trades}, {aggregate.row["id"]: [row["id"] for row in trades]}))
    visited = []
    monkeypatch.setattr(admission, "_matches_feature_inputs", lambda *args, **kwargs: visited.append(True) or True)
    session = SimpleNamespace(execute=lambda *args: SimpleNamespace(scalars=lambda:
        SimpleNamespace(all=lambda: [row["id"] for row in rows])))
    with pytest.raises(RuntimeError, match="derivation_budget_exceeded"):
        admission.resolve_flow_feature_source_revisions(session, rows=rows, object_store=None,
            max_rows=3, max_logical_bytes=1024**2)
    assert len(visited) == 1, "second repeated input set must fail before decoding/deriving it"


def test_unexpected_owner_failures_are_not_suppressed(monkeypatch):
    root, rows, deliveries = _fixture()
    feature = _feature(root, deliveries)
    def broken(**kwargs):
        raise ValueError("unexpected owner contract violation")
    monkeypatch.setattr(admission, "derive_trade_flow_feature", broken)
    with pytest.raises(ValueError, match="unexpected owner contract violation"):
        admission._matches_feature_inputs(feature, root.fact, rows)
