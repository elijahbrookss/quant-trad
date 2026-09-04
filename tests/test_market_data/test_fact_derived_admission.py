from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace

import pytest

from market_data.canonical import CanonicalFactRecord
from market_data.canonical_adapters import canonicalize_basis_feature, canonicalize_bbo_feature
from market_data.canonical_storage import record_to_storage_row
from market_data.market_state import derive_basis_features, derive_book_features
from portal.backend.service.storage.repos import fact_derived_admission as admission
from portal.backend.service.storage.repos.market_data import _preserves_canonical_revision_history
from tests.test_market_data.test_market_state_phase3 import BASE, SOURCE, _state, _contract


def _row(fact, series_id, commit=1):
    return record_to_storage_row(CanonicalFactRecord(fact=fact, series_id=series_id, source_id=1,
                                                    revision=1, market_commit_seq=commit), series_dimensions={})


def _pair():
    bbo, _ = derive_book_features([_state(1, "0.2")], contract=_contract(), bbo_series_id=20,
                                 depth_series_id=21, computed_at=BASE + timedelta(seconds=2))
    future = bbo[0]
    spot = replace(future, series_id=22)
    basis = derive_basis_features([future], [spot], mapping_id="mapping", computed_at=future.known_at, series_id=23)[0]
    return future, spot, basis


def test_basis_freeze_binds_canonical_history_without_removing_typed_readers():
    assert _preserves_canonical_revision_history("market.futures_spot_basis.v1")


@pytest.mark.parametrize("mode", ["valid", "canonical_hash", "missing", "wrong_alias", "wrong_series", "wrong_type",
                                 "later_known", "later_commit", "ambiguous", "budget", "missing_payload"])
def test_material_sources_revalidate_every_candidate_and_causal_clock(monkeypatch, mode):
    future, _, _ = _pair()
    first = _row(canonicalize_bbo_feature(future, source=SOURCE), 20)
    second = {**deepcopy(first), "id": "another-revision", "market_commit_seq": 2}
    rows = {row["id"]: row for row in (first, second)}
    request = {"root_id": "root", "role": "futures", "series_id": 20, "fact_type": "market.bbo",
               "material_hash": first["material_hash"] if mode == "canonical_hash" else future.material_hash,
               "commit_seq": 3, "known_at": future.known_at}
    if mode == "wrong_alias":
        second["provenance"]["_qt_bbo_evidence"]["legacy_material_hash"] = "b" * 64
    elif mode == "wrong_series":
        second["series_id"] = 99
    elif mode == "wrong_type":
        second["fact_type"] = "market.trade"
    elif mode == "later_known":
        second["known_at"] += timedelta(seconds=1)
    elif mode == "later_commit":
        second["market_commit_seq"] = 4
    elif mode == "ambiguous":
        second["observation_key"] = "other"
    def execute(statement, params):
        sql = str(statement)
        assert sql.count("source.market_commit_seq<=requested.commit_seq AND source.known_at<=requested.known_at") == 3
        assert "fact_archive_material_aliases" in sql and "DISTINCT ON" not in sql
        parsed = json.loads(params["requests"])
        assert parsed[0]["evidence_key"] == "_qt_bbo_evidence"
        return SimpleNamespace(all=lambda: [] if mode == "missing" else [("root", "futures", identity) for identity in rows])
    monkeypatch.setattr(admission, "read_canonical_dependency_rows", lambda *args, **kwargs:
                        {} if mode == "missing_payload" else rows)
    checked = []
    monkeypatch.setattr(admission, "record_from_storage_row", lambda row: checked.append(row["id"]))
    kwargs = dict(requests=[request], reader=None, max_rows=1 if mode == "budget" else 10, max_logical_bytes=1024)
    if mode in {"valid", "canonical_hash"}:
        sources, selections = admission.resolve_material_source_revisions(SimpleNamespace(execute=execute), **kwargs)
        assert sources == rows and selections == {("root", "futures"): list(rows)}
        assert checked == list(rows)
    else:
        with pytest.raises(RuntimeError, match="canonical_material_source_"):
            admission.resolve_material_source_revisions(SimpleNamespace(execute=execute), **kwargs)


@pytest.mark.parametrize("field,value", [("series_id", True), ("series_id", None), ("commit_seq", 0),
                                       ("material_hash", None), ("material_hash", "bad")])
def test_invalid_source_identity_never_reaches_sql(field, value):
    request = {"root_id": "root", "role": "spot", "series_id": 1, "fact_type": "market.bbo",
               "material_hash": "a" * 64, "commit_seq": 1, "known_at": BASE, field: value}
    with pytest.raises(RuntimeError, match="canonical_material_source_identity_invalid"):
        admission.resolve_material_source_revisions(None, requests=[request], reader=None, max_rows=10, max_logical_bytes=1024)


def test_material_selection_bounds_each_query_and_the_total_edge_count(monkeypatch):
    source = {"id": "source", "series_id": 1, "fact_type": "market.trade", "material_hash": "a" * 64,
              "provenance": {}, "known_at": BASE, "market_commit_seq": 1, "observation_key": "observation"}
    requests = [{"root_id": str(index), "role": "input", "series_id": 1, "fact_type": "market.trade",
                 "material_hash": "a" * 64, "commit_seq": 2, "known_at": BASE} for index in range(300)]
    batches = []
    def execute(statement, params):
        batch = json.loads(params["requests"])
        batches.append(len(batch))
        return SimpleNamespace(all=lambda: [(item["root_id"], "input", "source") for item in batch])
    monkeypatch.setattr(admission, "read_canonical_dependency_rows", lambda *args, **kwargs: {"source": source})
    monkeypatch.setattr(admission, "record_from_storage_row", lambda row: None)
    sources, edges = admission.resolve_material_source_revisions(SimpleNamespace(execute=execute), requests=requests,
        reader=None, max_rows=300, max_logical_bytes=1024)
    assert batches == [128, 128, 44] and len(edges) == 300 and len(sources) == 1


@pytest.mark.parametrize("mode", ["valid", "missing_mapping", "role", "root_instrument", "futures_instrument",
                                 "spot_instrument", "not_yet_effective", "expired", "input_value", "input_hash"])
def test_basis_requires_immutable_mapping_scope_and_owner_derived_values(monkeypatch, mode):
    future, spot, basis = _pair()
    source_rows = [_row(canonicalize_bbo_feature(item, source=SOURCE), item.series_id, index + 1) for index, item in enumerate((future, spot))]
    canonical = canonicalize_basis_feature(basis)
    if mode in {"input_value", "input_hash"}:
        payload = {**canonical.payload, **({"futures_mid": "101"} if mode == "input_value" else {"input_fingerprint": "a" * 64})}
        canonical = replace(canonical, payload=payload)
    root = _row(canonical, basis.series_id, 3)
    mapping = {"root_id": root["id"], "id": "mapping", "primary_instrument_id": "future", "related_instrument_id": "spot",
               "role": "spot_reference", "root_instrument": "future", "futures_instrument": "future", "spot_instrument": "spot",
               "effective_from": BASE, "effective_to": None}
    if mode == "missing_mapping":
        mapping["id"] = None
    elif mode in {"role", "root_instrument", "futures_instrument", "spot_instrument"}:
        mapping[mode] = "wrong"
    elif mode == "not_yet_effective":
        mapping["effective_from"] = BASE + timedelta(days=1)
    elif mode == "expired":
        mapping["effective_to"] = root["observation_time"]
    def resolve(session, **kwargs):
        assert len(kwargs["requests"]) == 2
        return ({item["id"]: item for item in source_rows},
                {(root["id"], role): [item["id"]] for role, item in zip(("futures", "spot"), source_rows)})
    monkeypatch.setattr(admission, "resolve_material_source_revisions", resolve)
    session = SimpleNamespace(execute=lambda *_: SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [mapping])))
    kwargs = dict(rows=[root], object_store=None, max_rows=10, max_logical_bytes=1024 * 1024)
    if mode == "valid":
        assert admission.resolve_derived_source_revisions(session, **kwargs) == sorted(source_rows, key=lambda row: row["id"])
    else:
        with pytest.raises(RuntimeError, match="canonical_basis_"):
            admission.resolve_derived_source_revisions(session, **kwargs)
