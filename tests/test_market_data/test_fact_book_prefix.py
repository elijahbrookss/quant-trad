from copy import deepcopy
from types import SimpleNamespace

import pytest

from market_data.archive import RawArchiveReadLimits
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from portal.backend.service.storage.repos import fact_book_prefix, fact_lineage
from portal.backend.service.storage.repos.market_lifecycle import MarketStorageLifecycleBusyError
from tests.test_market_data.test_fact_raw_lineage import _Session, _fixture, _book_root


class _PrefixSession(_Session):
    def __init__(self, mappings):
        super().__init__(mappings)
        self.chunks, self.holds = [], []
        self.busy = False

    def add(self, record):
        values = {column.name: getattr(record, column.name) for column in record.__table__.columns}
        (self.chunks if record.__tablename__ == "fact_book_prefix_chunks" else self.holds).append(values)

    def flush(self):
        pass

    def execute(self, statement, params):
        sql = str(statement)
        if "pg_try_advisory_xact_lock" in sql:
            return SimpleNamespace(scalar_one=lambda: not self.busy)
        if "FROM market.fact_book_prefix_chunks" in sql:
            found = [item for item in self.chunks if all(item[name] == params[name]
                for name in (*fact_lineage.BOOK_SCOPE_FIELDS, "verifier_version"))]
            if "until" in params:
                found = [item for item in found if item["first_receive_ordinal"] <= params["until"]][:params["limit"]]
            else:
                found = found[-1:]
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: found,
                one_or_none=lambda: found[0] if found else None))
        if "FROM market.fact_book_prefix_dependencies" in sql:
            manifests = {item["id"]: item for item in self.mappings}
            found = []
            for hold in sorted(self.holds, key=lambda item: item["target_id"]):
                if hold["chunk_id"] != params["id"]:
                    continue
                raw = manifests[hold["target_id"]]
                found.append({**raw, "target_id": hold["target_id"], "held_key": hold["object_key"],
                    "held_sha256": hold["object_sha256"], "unavailable": False})
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: found[:params["limit"]]))
        return super().execute(statement, params)


def _prepare(session, store, root, **kwargs):
    return fact_book_prefix.prepare_next_book_prefix(session, rows=[root], object_store=store,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()), limits=RawArchiveReadLimits(),
        max_mapping_rows=8, max_objects=100, **kwargs)


def _resolve(session, store, rows, **kwargs):
    return fact_book_prefix.resolve_verified_book_prefixes(session, rows=rows,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()), max_objects=100, **kwargs)


def test_chunk_progress_exceeds_one_mapping_budget_and_reuses_deep_proof(tmp_path, monkeypatch):
    store, records, mappings, rows = _fixture(tmp_path, tuple(range(1, 13)), requested_channel="level2")
    session = _PrefixSession(mappings)
    root = _book_root(records[-1], rows[-1])
    for first in range(1, 13, 2):
        progress = _prepare(session, store, root)
        assert (progress["first_receive_ordinal"], progress["last_receive_ordinal"]) == (first, first + 1)
    assert _prepare(session, store, root) is None
    assert len(session.chunks) == 6 and len(session.holds) == 6
    def must_not_decode(*args, **kwargs):
        raise AssertionError("an admitted prefix must not decode the historical frames again")
    monkeypatch.setattr(fact_lineage, "_raw_records", must_not_decode)
    refs, bindings = _resolve(session, store, [root])
    assert set(refs) == {"raw-manifest"} and bindings == {root["id"]: {"raw-manifest"}}
    # Metadata never replaces the fresh byte check.
    with store.local_path("fixture.parquet").open("ab") as file:
        file.write(b"changed")
    with pytest.raises(RuntimeError, match="size|sha256|checksum"):
        _resolve(session, store, [root])


def test_late_interval_does_not_expand_or_query_its_earlier_history(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path, (100_001, 100_002), requested_channel="level2")
    session = _PrefixSession(mappings)
    prefix = fact_lineage.canonical_book_prefixes([_book_root(records[-1], rows[-1])])[0]
    references = fact_lineage.resolve_canonical_raw_archive_refs(session, rows=[], object_store=store,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()), max_mapping_rows=2,
        book_prefix_ranges=[{**prefix, "first_receive_ordinal": 100_001}])
    assert set(references) == {"raw-manifest"}
    assert len(session.calls) == 1 and session.calls[0][1]["limit"] == 3


@pytest.mark.parametrize("damage", ["missing_chunk", "changed_descriptor", "missing_hold", "changed_hold", "stale_verifier"])
def test_receipt_chain_or_hold_damage_is_not_admitted(tmp_path, damage):
    store, records, mappings, rows = _fixture(tmp_path, (1, 2, 3), requested_channel="level2")
    session = _PrefixSession(mappings)
    root = _book_root(records[-1], rows[-1])
    assert _prepare(session, store, root)
    assert _prepare(session, store, root)
    if damage == "missing_chunk":
        session.chunks.pop(0)
    elif damage == "changed_descriptor":
        session.chunks[0]["descriptor"]["last_receive_ordinal"] = 999
    elif damage == "missing_hold":
        session.holds.pop(0)
    elif damage == "changed_hold":
        session.holds[0]["object_key"] = "elsewhere"
    else:
        session.chunks[0]["verifier_version"] = "older-version"
    with pytest.raises(RuntimeError, match="canonical_book_prefix_"):
        _resolve(session, store, [root])


def test_missing_bound_reference_incomplete_progress_and_scope_lock_fail_loud(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path, (1, 2, 3), requested_channel="level2")
    session = _PrefixSession(mappings)
    root = _book_root(records[-1], rows[-1])
    _prepare(session, store, root)
    with pytest.raises(RuntimeError, match="not_ready"):
        _resolve(session, store, [root])
    session.busy = True
    with pytest.raises(MarketStorageLifecycleBusyError, match="book_prefix_busy"):
        _prepare(session, store, root)
    assert len(session.chunks) == 1
    session.busy = False
    _prepare(session, store, root)
    with pytest.raises(RuntimeError, match="bound_dependency_missing"):
        _resolve(session, store, [root], bound_manifest_ids=set())
    with pytest.raises(RuntimeError, match="chunk_budget_exceeded"):
        _resolve(session, store, [root], max_chunks=1)


def test_exact_root_cannot_substitute_a_new_mapping_outside_its_certified_chunk(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path, (1,), requested_channel="level2")
    root = _book_root(records[0], rows[0])
    session = _PrefixSession(mappings)
    _prepare(session, store, root)
    _, bindings = _resolve(session, store, [root])
    alternate = deepcopy(mappings[0])
    alternate["id"] = "new-placement"
    # Simulate only a new placement resolving this exact witness. A prefix
    # certificate does not allow it to replace the bound historical placement.
    with pytest.raises(RuntimeError, match="mapping_missing"):
        fact_lineage.resolve_canonical_raw_archive_refs(_Session([alternate]), rows=[root], object_store=store,
            byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()), witness_manifest_ids=bindings)


def _trade_prefix(records):
    last = records[-1]
    return {name: getattr(last, name) for name in fact_lineage.BOOK_SCOPE_FIELDS} | {
        "first_receive_ordinal": 1, "receive_ordinal": last.receive_ordinal,
        "root_fact_version_id": "flow-root",
    }


def _prepare_trade(session, store, prefix):
    return fact_book_prefix.prepare_next_trade_prefix(session, prefixes=[prefix], object_store=store,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()), limits=RawArchiveReadLimits(),
        max_mapping_rows=8, max_objects=100)


def _resolve_trade(session, store, prefix, witnesses):
    return fact_book_prefix.resolve_verified_trade_prefixes(session, prefixes=[prefix], witnesses=witnesses,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()), max_objects=100)


@pytest.mark.parametrize("progress", [0, 1, 3])
def test_trade_history_read_verifies_absent_tail_without_writing_prefix_progress(tmp_path, progress):
    store, records, mappings, _ = _fixture(tmp_path, (1, 2, 3, 4, 5))
    session = _PrefixSession(mappings)
    prefix = _trade_prefix(records)
    for _ in range(progress):
        _prepare_trade(session, store, prefix)
    witnesses = [{**prefix, "root_fact_version_id": f"flow:{record.receive_ordinal}",
        "first_receive_ordinal": record.receive_ordinal, "receive_ordinal": record.receive_ordinal,
        "raw_record_id": record.raw_record_id, "requested_channel": "market_trades"} for record in (records[0], records[-1])]
    before = deepcopy((session.chunks, session.holds))
    result = fact_book_prefix.resolve_trade_prefixes_for_read(session, prefixes=[prefix], witnesses=witnesses, rows=[],
        object_store=store, byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
        limits=RawArchiveReadLimits(), max_mapping_rows=20, max_objects=100)
    assert set(result) == {"raw-manifest"}
    assert (session.chunks, session.holds) == before
    if progress:
        session.holds.pop(0)
        with pytest.raises(RuntimeError, match="dependencies_invalid"):
            fact_book_prefix.resolve_trade_prefixes_for_read(session, prefixes=[prefix], witnesses=witnesses, rows=[],
                object_store=store, byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
                limits=RawArchiveReadLimits(), max_mapping_rows=20, max_objects=100)


def test_trade_history_read_bounds_missing_tail_and_reuses_completed_certificate(tmp_path):
    store, records, mappings, _ = _fixture(tmp_path, (1, 2, 3, 4, 5))
    session = _PrefixSession(mappings)
    prefix = _trade_prefix(records)
    kwargs = dict(prefixes=[prefix], witnesses=[], rows=[], object_store=store,
        limits=RawArchiveReadLimits(), max_mapping_rows=2, max_objects=100)
    with pytest.raises(RuntimeError, match="prefix_budget_exceeded"):
        fact_book_prefix.resolve_trade_prefixes_for_read(session,
            byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()), **kwargs)
    assert session.chunks == session.holds == []
    for _ in range(3):
        _prepare_trade(session, store, prefix)
    assert fact_book_prefix.resolve_trade_prefixes_for_read(session,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()), **kwargs)


def test_trade_prefix_resumes_separately_and_binds_exact_coverage_endpoints(tmp_path):
    store, records, mappings, rows = _fixture(tmp_path, (1, 2, 3, 4, 5), requested_channel="market_trades")
    session = _PrefixSession(mappings)
    prefix = _trade_prefix(records)
    endpoints = [{**prefix, "receive_ordinal": record.receive_ordinal,
        "root_fact_version_id": f"flow-root:{role}", "raw_record_id": record.raw_record_id}
        for role, record in (("opening", records[0]), ("closing", records[-1]))]
    for first, last in ((1, 2), (3, 4), (5, 5)):
        progress = _prepare_trade(session, store, prefix)
        assert progress["status"] == "trade_prefix_verified"
        assert (progress["first_receive_ordinal"], progress["last_receive_ordinal"]) == (first, last)
    assert _prepare_trade(session, store, prefix) is None
    assert all(chunk["verifier_version"] == fact_book_prefix.TRADE_PREFIX_VERIFIER_VERSION for chunk in session.chunks)
    assert all("requested_channel" not in chunk["descriptor"] for chunk in session.chunks), "retain the existing descriptor schema"
    refs, bindings = _resolve_trade(session, store, prefix, endpoints)
    assert set(refs) == {"raw-manifest"} and set(bindings) == {item["root_fact_version_id"] for item in endpoints}
    ranges = [{**item, "first_receive_ordinal": item["receive_ordinal"], "requested_channel": "market_trades"}
              for item in endpoints]
    exact = fact_lineage.resolve_canonical_raw_archive_refs(session, rows=[], object_store=store,
        byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
        book_prefix_ranges=ranges, witness_manifest_ids=bindings)
    assert exact == refs
    with pytest.raises(RuntimeError, match="not_ready"):
        _resolve(session, store, [_book_root(records[-1], rows[-1])])
    with pytest.raises(RuntimeError, match="mapping_missing"):
        fact_lineage.resolve_canonical_raw_archive_refs(session, rows=[], object_store=store,
            byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
            book_prefix_ranges=ranges, witness_manifest_ids={item["root_fact_version_id"]: set() for item in endpoints})
    wrong_endpoint = [{**ranges[0], "raw_record_id": "wrong-coverage-opening"}]
    with pytest.raises(RuntimeError, match="witness_mismatch"):
        fact_lineage.resolve_canonical_raw_archive_refs(session, rows=[], object_store=store,
            byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
            book_prefix_ranges=wrong_endpoint, witness_manifest_ids=bindings)


@pytest.mark.parametrize("channel", ["level2", "market_trades"])
def test_prefix_channels_cannot_certify_each_other(tmp_path, channel):
    store, records, mappings, rows = _fixture(tmp_path, (1,), requested_channel=channel)
    session = _PrefixSession(mappings)
    with pytest.raises(RuntimeError, match="witness_mismatch"):
        if channel == "level2":
            _prepare_trade(session, store, _trade_prefix(records))
        else:
            _prepare(session, store, _book_root(records[0], rows[0]))
    assert session.chunks == [] and session.holds == []
    if channel == "level2":
        _prepare(session, store, _book_root(records[0], rows[0]))
        with pytest.raises(RuntimeError, match="not_ready"):
            _resolve_trade(session, store, _trade_prefix(records), [_trade_prefix(records)])
    else:
        _prepare_trade(session, store, _trade_prefix(records))
        with pytest.raises(RuntimeError, match="not_ready"):
            _resolve(session, store, [_book_root(records[0], rows[0])])


@pytest.mark.parametrize("damage", ["scope", "range", "negative"])
def test_coverage_witness_must_belong_to_its_requested_prefix(tmp_path, damage):
    store, records, mappings, _ = _fixture(tmp_path, (1, 2), requested_channel="market_trades")
    session = _PrefixSession(mappings)
    prefix = _trade_prefix(records)
    _prepare_trade(session, store, prefix)
    changed = {"session_id": "different-session"} if damage == "scope" else {"receive_ordinal": 3 if damage == "range" else 0}
    with pytest.raises(RuntimeError, match="canonical_raw_prefix_witness_"):
        _resolve_trade(session, store, prefix, [{**prefix, **changed}])


@pytest.mark.parametrize("progress", [0, 1, 3])
def test_book_history_read_checks_exact_witness_and_tail_without_writing(tmp_path, progress):
    store, records, mappings, rows = _fixture(tmp_path, (1, 2, 3, 4, 5), requested_channel="level2")
    session = _PrefixSession(mappings)
    root = _book_root(records[-1], rows[-1])
    for _ in range(progress):
        _prepare(session, store, root)
    before = deepcopy((session.chunks, session.holds))
    def read():
        return fact_book_prefix.resolve_book_prefixes_for_read(session, rows=[root], object_store=store,
            byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
            limits=RawArchiveReadLimits(), max_mapping_rows=20, max_objects=100)
    assert set(read()) == {"raw-manifest"}
    assert (session.chunks, session.holds) == before
    root["provenance"]["_qt_l2_evidence"]["raw_record_id"] = "absent"
    with pytest.raises(RuntimeError, match="mapping_missing"):
        read()


@pytest.mark.parametrize("channel", ["level2", "market_trades"])
def test_read_identity_does_not_expand_when_a_certificate_includes_later_objects(tmp_path, channel):
    store, records, mappings, rows = _fixture(tmp_path / "first", (1,), requested_channel=channel)
    later_store, later_records, later_mappings, later_rows = _fixture(tmp_path / "later", (2,), requested_channel=channel)
    ack = store.put_verified(object_key="later.parquet", source_path=later_store.local_path("fixture.parquet"),
        expected_sha256=later_mappings[0]["object_sha256"])
    for mapping in later_mappings:
        mapping.update(id="later-manifest", object_key=ack.object_key, object_uri=ack.object_uri)
    session = _PrefixSession([*mappings, *later_mappings])
    def read():
        common = dict(object_store=store, byte_verifier=ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits()),
            limits=RawArchiveReadLimits(), max_mapping_rows=20, max_objects=100)
        if channel == "level2":
            return fact_book_prefix.resolve_book_prefixes_for_read(session, rows=[_book_root(records[0], rows[0])], **common)
        prefix = _trade_prefix(records)
        witness = {**prefix, "raw_record_id": records[0].raw_record_id, "requested_channel": channel}
        return fact_book_prefix.resolve_trade_prefixes_for_read(session, prefixes=[prefix], witnesses=[witness], rows=rows, **common)
    before = read()
    assert set(before) == {"raw-manifest"} and session.chunks == []
    if channel == "level2":
        _prepare(session, store, _book_root(later_records[0], later_rows[0]))
    else:
        _prepare_trade(session, store, _trade_prefix(later_records))
    assert len(session.chunks) == 1 and len(session.holds) == 2
    assert read() == before
    # Even a wholly later dependency of the reused certificate must still pass
    # its current byte check. Trimming identity never bypasses receipt integrity.
    store.local_path("later.parquet").write_bytes(b"corrupt larger-prefix dependency")
    with pytest.raises(RuntimeError, match="size|sha256|checksum"):
        read()
