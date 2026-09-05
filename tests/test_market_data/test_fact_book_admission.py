from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from portal.backend.service.storage.repos import fact_book_admission as admission
from portal.backend.service.storage.repos import fact_dependencies


BASE = datetime(2026, 1, 1, tzinfo=UTC)
POSITION = {"definition_id": "definition", "session_id": "session", "connection_epoch": 0,
            "provider_product_id": "BTC-USD", "receive_ordinal": 3, "event_ordinal": 0,
            "provider_sequence_num": 1}


def _source(identity="source-1"):
    return {"id": identity, "series_id": 1, "fact_type": "market.l2_book", "row_hash": "a" * 64,
            "market_commit_seq": 10, "known_at": BASE, "observation_key": "definition:session:0:3:0",
            "provenance": {"_qt_l2_evidence": {**POSITION, "raw_record_id": "raw"}},
            "payload": {"validity_interval_id": "interval", "product_definition_version_id": "product",
                        "reconstruction_version": admission.BOOK_RECONSTRUCTION_VERSION, "after_state_hash": "state",
                        "entries": [{"provider_size_unit": "base"}]}}


def _derived():
    return {"id": "derived", "fact_type": "market.bbo", "market_commit_seq": 12, "known_at": BASE,
            "provenance": {"_qt_bbo_evidence": {"source_position": dict(POSITION), "source_l2_series_id": 1}},
            "payload": {"validity_interval_id": "interval", "product_definition_version_id": "product",
                        "source_state_hash": "state"}}


@pytest.mark.parametrize("mode", ["valid", "missing", "state", "interval", "scope", "product", "budget",
                                 "future_known", "future_commit", "wrong_key", "wrong_series"])
def test_source_admission_binds_all_causal_revisions_not_latest_aliases(monkeypatch, mode):
    rows = [_source(), _source("source-2")]
    if mode in {"state", "interval", "product"}:
        field = {"state": "after_state_hash", "interval": "validity_interval_id", "product": "product_definition_version_id"}[mode]
        for row in rows:
            row["payload"][field] = "wrong"
    if mode == "scope":
        rows[0]["provenance"]["_qt_l2_evidence"]["session_id"] = "another-session"
    elif mode == "future_known":
        rows[0]["known_at"] = BASE + timedelta(seconds=1)
    elif mode == "future_commit":
        rows[0]["market_commit_seq"] = 13
    elif mode == "wrong_key":
        rows[0]["observation_key"] = "another-observation"
    elif mode == "wrong_series":
        rows[0]["series_id"] = 99
    pairs = [] if mode == "missing" else [("derived", item["id"]) for item in rows]
    def execute(statement, params):
        assert "source.market_commit_seq<=requested.commit_seq" in str(statement)
        assert "source.known_at<=requested.known_at" in str(statement)
        assert "ORDER BY requested.root_id,source.market_commit_seq,source.id" in str(statement)
        assert json.loads(params["requests"]) == [{"root_id": "derived", "series_id": 1,
            "observation_key": "definition:session:0:3:0", "commit_seq": 12, "known_at": BASE.isoformat()}]
        return SimpleNamespace(all=lambda: pairs)
    def read(_session, ids, **kwargs):
        return {row["id"]: row for row in rows if row["id"] in ids}
    verified = []
    monkeypatch.setattr(admission, "read_canonical_dependency_rows", read)
    monkeypatch.setattr(admission, "record_from_storage_row", lambda row: verified.append(row["id"]))
    args = dict(rows=[_derived()], object_store=None, max_rows=1 if mode == "budget" else 10, max_logical_bytes=1024)
    if mode == "valid":
        assert admission.resolve_book_source_revisions(SimpleNamespace(execute=execute), **args) == rows
        assert verified == ["source-1", "source-2"]
    else:
        with pytest.raises(RuntimeError, match="canonical_book_source_"):
            admission.resolve_book_source_revisions(SimpleNamespace(execute=execute), **args)


@pytest.mark.parametrize("mode", ["valid", "missing_validity", "validity_scope", "validity_position", "version",
                                 "missing_product", "product_scope", "unit", "bound_checkpoint_missing"])
def test_book_metadata_fails_closed_without_mutable_stream_config(mode):
    row = _source()
    opening = {"series_id": 1, "status": "open_valid", "reconstruction_version": admission.BOOK_RECONSTRUCTION_VERSION,
               "opening_session_id": "session", "opening_connection_epoch": 0, "opening_receive_ordinal": 1,
               "opening_event_ordinal": 0}
    product = {"provider_product_id": "BTC-USD", "provider_size_unit": "base", "price_increment": None, "base_increment": None}
    if mode == "missing_validity":
        opening = None
    elif mode == "validity_scope":
        opening["opening_session_id"] = "wrong"
    elif mode == "validity_position":
        opening["opening_receive_ordinal"] = 4
    elif mode == "version":
        row["payload"]["reconstruction_version"] = "unsupported"
    elif mode == "missing_product":
        product = None
    elif mode == "product_scope":
        product["provider_product_id"] = "ETH-USD"
    elif mode == "unit":
        product["provider_size_unit"] = "contracts"
    def execute(statement, params):
        sql = str(statement)
        if "book_validity_interval_versions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: opening))
        if "product_definition_versions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: product))
        assert "book_checkpoint_manifests" in sql
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
    kwargs = dict(rows=[row], object_store=None, byte_verifier=None, max_objects=10,
                  bound_checkpoint_ids=["missing"] if mode == "bound_checkpoint_missing" else None)
    if mode == "valid":
        assert admission.verify_book_metadata_and_checkpoints(SimpleNamespace(execute=execute), **kwargs) == ([], [])
    else:
        with pytest.raises(RuntimeError, match="canonical_book_"):
            admission.verify_book_metadata_and_checkpoints(SimpleNamespace(execute=execute), **kwargs)


def test_every_revision_checks_validity_not_just_latest_position():
    rows = [_source(), _source("older")]
    rows[1]["provenance"]["_qt_l2_evidence"]["receive_ordinal"] = 1
    opening = {"series_id": 1, "status": "open_valid", "reconstruction_version": admission.BOOK_RECONSTRUCTION_VERSION,
               "opening_session_id": "session", "opening_connection_epoch": 0, "opening_receive_ordinal": 2,
               "opening_event_ordinal": 0}
    product = {"provider_product_id": "BTC-USD", "provider_size_unit": "base", "price_increment": None, "base_increment": None}
    values = iter([opening, product])
    session = SimpleNamespace(execute=lambda *_: SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: next(values))))
    with pytest.raises(RuntimeError, match="canonical_book_validity_position_mismatch"):
        admission.verify_book_metadata_and_checkpoints(session, rows=rows, object_store=None, byte_verifier=None, max_objects=10)


@pytest.mark.parametrize("mode", ["valid", "state", "missing", "scope", "raw_mapping"])
def test_checkpoint_requires_exact_canonical_state_as_well_as_valid_bytes(monkeypatch, mode):
    row = {**_source(), "market_commit_seq": 12}
    opening = {"series_id": 1, "status": "open_valid", "reconstruction_version": admission.BOOK_RECONSTRUCTION_VERSION,
               "opening_session_id": "session", "opening_connection_epoch": 0, "opening_receive_ordinal": 1,
               "opening_event_ordinal": 0}
    product = {"provider_product_id": "BTC-USD", "provider_size_unit": "base", "price_increment": None, "base_increment": None}
    checkpoint = {**POSITION, "id": "checkpoint", "series_id": 1, "known_at": BASE, "source_manifest_ids": ["raw"],
                  "state_hash": "wrong" if mode == "state" else "state", "validity_interval_id": "interval",
                  "product_definition_version_id": "product", "object_key": "checkpoint.parquet",
                  "object_sha256": "a" * 64, "byte_count": 100}
    if mode == "scope":
        checkpoint["provider_sequence_num"] = 99
    def execute(statement, params):
        sql = str(statement)
        if "book_validity_interval_versions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: opening))
        if "product_definition_versions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: product))
        if "book_checkpoint_manifests" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [checkpoint]))
        if "count(DISTINCT manifests.id)" in sql:
            return SimpleNamespace(scalar_one=lambda: 0 if mode == "raw_mapping" else 1)
        assert "market_commit_seq<=:seq AND known_at<=:known_at" in sql
        assert params["known_at"] == BASE and params["seq"] == 12
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [] if mode == "missing" else [row["id"]]))
    restored = []
    verified = []
    monkeypatch.setattr(admission, "restore_book_checkpoint_parquet", lambda *args, **kwargs: restored.append(kwargs["expected"]["id"]))
    kwargs = dict(rows=[row], object_store=SimpleNamespace(local_path=lambda key: key), max_objects=10,
                  byte_verifier=SimpleNamespace(verify=lambda *args, **kwargs: verified.append(args)))
    if mode == "valid":
        objects, sources = admission.verify_book_metadata_and_checkpoints(SimpleNamespace(execute=execute), **kwargs)
        assert sources == [row] and objects[0]["target_id"] == "checkpoint"
        assert restored == ["checkpoint"] and len(verified) == 1
    else:
        with pytest.raises(RuntimeError, match="canonical_book_checkpoint_source_"):
            admission.verify_book_metadata_and_checkpoints(SimpleNamespace(execute=execute), **kwargs)
        assert not restored and not verified


def test_source_byte_budget_rejects_before_fetching_payloads():
    class Session:
        def execute(self, statement, params):
            if "coalesce(sum" in str(statement):
                return SimpleNamespace(scalar_one=lambda: 2048)
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
    with pytest.raises(RuntimeError, match="canonical_source_dependency_byte_budget_exceeded"):
        fact_dependencies.read_canonical_dependency_rows(Session(), ["source"], reader=None, max_logical_bytes=1024)


@pytest.mark.parametrize("mode", ["valid", "missing", "hash", "duplicate"])
def test_source_placement_proof_checks_exact_header(mode):
    dependency = {"fact_version_id": "source", "row_hash": "a" * 64}
    rows = [] if mode == "missing" else [{"id": "source", "row_hash": "b" * 64 if mode == "hash" else "a" * 64,
                                         "storage_day": BASE.date(), "hot_present": True}]
    session = SimpleNamespace(execute=lambda *_: SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows)))
    dependencies = [dependency, deepcopy(dependency)] if mode == "duplicate" else [dependency]
    if mode == "valid":
        assert len(fact_dependencies.verify_canonical_source_placements(session, dependencies)) == 64
    else:
        with pytest.raises(RuntimeError, match="canonical_source_dependency_"):
            fact_dependencies.verify_canonical_source_placements(session, dependencies)
