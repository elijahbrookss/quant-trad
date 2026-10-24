from datetime import date, datetime, timedelta, timezone
from itertools import repeat
import struct
from types import SimpleNamespace
from contextlib import nullcontext

import pytest

from market_data.archive_verification import ArchiveVerificationLimits
from portal.backend.service.storage.repos import fact_handoff as handoff


def test_binary_records_keep_type_null_boundaries_and_unicode():
    text = (("left", "text"), ("right", "text"))
    assert handoff._record({"left": "ab", "right": "c"}, text) != handoff._record({"left": "a", "right": "bc"}, text)
    assert handoff._record({"left": None, "right": ""}, text) != handoff._record({"left": "", "right": None}, text)
    payload = handoff._record({"left": "猫🥚: ", "right": "\n"}, text)
    encoded = "猫🥚: ".encode()
    assert payload == (struct.pack("!iIi", 2, 25, len(encoded)) + encoded + struct.pack("!Ii", 25, 1) + b"\n")
    assert handoff._record({"a": 1}, (("a", "int"),)) != handoff._record({"a": "1"}, (("a", "text"),))


def test_binary_clocks_preserve_timezone_and_negative_microsecond():
    before = datetime(1999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    columns = (("at", "time"),)
    expected = struct.pack("!iIiq", 1, 1184, 8, -1)
    assert handoff._record({"at": before}, columns) == expected
    assert handoff._record({"at": before.astimezone(timezone(timedelta(hours=5, minutes=45)))}, columns) == expected
    assert handoff._record({"d": date(1999, 12, 31)}, (("d", "date"),)) == struct.pack("!iIii", 1, 1082, 4, -1)


@pytest.mark.parametrize("kind,value", [("int", True), ("int", "1"), ("text", 1), ("bool", 1),
                                         ("date", datetime(2000, 1, 1)), ("time", datetime(2000, 1, 1))])
def test_binary_scalar_coercions_fail_closed(kind, value):
    with pytest.raises(RuntimeError, match="scalar_invalid"):
        handoff._record({"x": value}, (("x", kind),))


@pytest.mark.parametrize("value", ["(1,0)", "(-1,2)", "(4294967296,1)", "(1,65536)", "(1,1);DROP TABLE x", None])
def test_tuple_locations_are_bounded_data(value):
    with pytest.raises(RuntimeError, match="tuple_location_invalid"):
        handoff._tid_key(value)


def test_physical_order_is_numeric_and_duplicate_locations_refuse(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    monkeypatch.setattr(proof, "_relation", lambda *a, **k: None)
    rows = [{"id": "ten", "_handoff_oid": 1, "_handoff_tid": "(10,2)"},
            {"id": "two", "_handoff_oid": 1, "_handoff_tid": "(2,10)"}]
    witness = proof._rows(None, rows, (("id", "text"),))
    assert witness.tids == '{"(2,10)","(10,2)"}'
    assert witness.digest == handoff._digest(rows[::-1], (("id", "text"),))
    with pytest.raises(RuntimeError, match="duplicate_tuple_location"):
        proof._rows(None, [rows[0], rows[0]], (("id", "text"),))


def test_duplicate_logical_keys_refuse_even_with_distinct_locations():
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    with pytest.raises(RuntimeError, match="duplicate_catalog_key"):
        proof.catalog_rows(None, "canonical_dependencies", "page", [
            {"fact_version_id": "same", "row_hash": "a"}, {"fact_version_id": "same", "row_hash": "b"}])


def test_total_retained_memory_is_bounded(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    monkeypatch.setattr(handoff, "_MAX_WITNESS_BYTES", proof.retained_bytes + 100)
    with pytest.raises(RuntimeError, match="witness_memory_budget_exceeded"):
        proof._keep("x" * 200)


@pytest.mark.parametrize("change", [{"hinted_count": 0}, {"hinted_count": 2}, {"logical_count": 2},
                                    {"digest": "changed"}])
def test_fresh_catalog_rejects_missing_added_or_reused_rows(change):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    proof.admitted = True
    page, receipt = {"id": "page"}, {"verification_hash": "old"}
    proof.pages["page"] = (page, receipt)
    proof.generations["page"] = None
    witness = handoff._Rows(1, (("manifest_id", "text"), ("fact_version_id", "text"), ("row_hash", "text")),
                            '{"(1,1)"}', 1, "expected")
    proof.catalogs["page"] = {"canonical_dependencies": witness}
    sql = []
    def execute(statement, parameters):
        sql.append(str(statement))
        if "FROM market.fact_archive_active_generations" in str(statement):
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [{
            "manifest_id": "page", "hinted_count": 1, "logical_count": 1, "digest": "expected", **change}]))
    with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
        proof.verify_catalogs(SimpleNamespace(execute=execute), page, receipt, lambda: None)
    assert len(sql) == 2 and "fact_archive_active_generations" in sql[0]
    assert "logical_count" in sql[1] and "q.ctid=ANY" in sql[1]


def test_unbound_wrong_day_and_incomplete_source_scope_refuse_before_sql():
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    with pytest.raises(RuntimeError, match="scope_invalid"):
        proof._admit(None, {"storage_day": date(2000, 1, 1)}, ArchiveVerificationLimits())
    with pytest.raises(RuntimeError, match="source_scope_invalid"):
        proof.capture_sources(None, "unknown", [], [])
    proof.pages["page"] = ({}, {})
    with pytest.raises(RuntimeError, match="incomplete_first_proof"):
        proof.bind(date(2000, 1, 1), 1, {"page_count": 1}, ArchiveVerificationLimits())


@pytest.mark.parametrize("failure", ["rewrite", "wrong_parent", "disabled_fk", "replica", "key_contract"])
def test_admission_refuses_changed_physical_or_identity_contract(monkeypatch, failure):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    day = date(2000, 1, 1)
    limits = ArchiveVerificationLimits()
    proof.bound = (day, 1, {"manifest_set_hash": "root"}, limits)
    relation = {"relname": "fact_versions_20000101", "parents": [10], "filenode": 20}
    proof.relations[1] = relation
    proof.relation_kinds[1] = "header"
    proof.header_days[1] = {day}
    monkeypatch.setattr(proof, "_metadata", lambda *a: {**relation, **({"filenode": 21} if failure == "rewrite" else {})})
    if failure == "wrong_parent":
        relation["parents"] = [11]
    def contract(_):
        if failure == "key_contract":
            raise RuntimeError("key_contract_changed")
    monkeypatch.setattr(handoff, "assert_fact_identity_contract", contract)
    monkeypatch.setattr(proof, "_assert_immutable_guards", lambda *a: None)
    monkeypatch.setattr(proof, "_assert_hot_insert_guards", lambda *a: None)
    statements = []
    def execute(statement, *_):
        value = str(statement); statements.append(value)
        scalar = (("replica" if failure == "replica" else "origin")
                  if "session_replication_role" in value else
                  20 if "fact_hot_payloads'::regclass" in value else
                  30 if "fact_archive_manifests'::regclass" in value else 10)
        checks = [{"proname": "RI_FKey_check_ins", "tgtype": 5, "tgenabled": "D" if failure == "disabled_fk" else "O", "unqualified": True},
                  {"proname": "RI_FKey_check_upd", "tgtype": 17, "tgenabled": "O", "unqualified": True}]
        return SimpleNamespace(scalar_one=lambda: scalar, mappings=lambda: SimpleNamespace(all=lambda: checks))
    session = SimpleNamespace(execute=execute, connection=lambda: None)
    with pytest.raises(RuntimeError, match="relation_changed|membership_changed|enforcement_invalid|key_contract_changed"):
        proof._admit(session, {"storage_day": day, "manifest_set_hash": "root"}, limits)
    assert proof.admitted is False
    assert all("NOWAIT" in stmt for stmt in statements if "LOCK TABLE" in stmt)


def test_nested_schema_queries_check_deadline_and_remove_listener():
    from sqlalchemy import create_engine, text
    engine = create_engine("sqlite://")
    calls = []
    def deadline():
        calls.append(True)
        if len(calls) == 5:
            raise RuntimeError("handoff deadline")
    with engine.connect() as connection:
        with pytest.raises(RuntimeError, match="handoff deadline"):
            with handoff._bounded_sql(connection, deadline):
                for _ in range(4096):
                    connection.execute(text("SELECT 1"))
        assert len(calls) == 5
        assert connection.execute(text("SELECT 2")).scalar_one() == 2
        assert len(calls) == 5
    engine.dispose()


def _cold_manifest(source_id="cold", manifest_id="manifest"):
    return {
        "source_id": source_id, "id": manifest_id,
        "storage_day": date(1999, 12, 31), "page_ordinal": 0,
        "object_key": "object", "object_sha256": "a" * 64,
        "manifest_hash": "b" * 64, "row_count": 1, "byte_count": 2,
        "first_commit_seq": 1, "first_id": "cold",
        "last_commit_seq": 1, "last_id": "cold",
        "_handoff_descriptor": "{}",
    }


def test_source_batches_retain_page_and_relation_authority_without_row_witnesses(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    monkeypatch.setattr(proof, "_relation", lambda *a, **k: None)
    proof.pages["page"] = ({}, {})
    proof.source_batches["page"] = []
    rows = [
        {"id": identity, "storage_day": date(2000, 1, 1), "row_hash": "a" * 64,
         "market_commit_seq": index, "hot_present": True,
         "_handoff_oid": 1, "_handoff_hot_oid": 2}
        for index, identity in enumerate(("first", "second"), 1)
    ]
    proof.capture_sources(None, "page", rows, [])
    proof.record_source_hash("page", "p" * 64)
    commitment = proof.source_commitments["page"]
    assert proof.source_batches == {}
    assert commitment.placement_hash == "p" * 64
    assert commitment.batches == (handoff._source_batch(rows, []),)
    assert proof.source_row_count == 2
    assert proof.header_days == {1: {date(2000, 1, 1)}}
    assert proof.hot_days == {2: {date(2000, 1, 1)}}
    assert proof.cold_manifests == {}


def test_source_capture_binds_distinct_cold_manifest_authority(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    monkeypatch.setattr(proof, "_relation", lambda *a, **k: None)
    proof.pages["page"] = ({}, {})
    proof.source_batches["page"] = []
    header = {"id": "cold", "storage_day": date(1999, 12, 31),
              "row_hash": "a" * 64, "market_commit_seq": 1,
              "hot_present": False, "_handoff_oid": 1,
              "_handoff_hot_oid": None}
    manifest = _cold_manifest()
    proof.capture_sources(None, "page", [header], [manifest])
    proof.capture_sources(None, "page", [header], [manifest])
    assert proof.cold_days == {date(1999, 12, 31)}
    assert proof.cold_manifests == {"manifest": handoff._digest([manifest], handoff._MANIFEST)}


def test_duplicate_or_out_of_order_source_batch_refuses(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    monkeypatch.setattr(proof, "_relation", lambda *a, **k: None)
    proof.pages["page"] = ({}, {})
    proof.source_batches["page"] = []
    row = {"id": "same", "storage_day": date(2000, 1, 1), "row_hash": "a" * 64,
           "market_commit_seq": 1, "hot_present": True, "_handoff_oid": 1,
           "_handoff_hot_oid": 2}
    with pytest.raises(RuntimeError, match="source_commitment_order_invalid"):
        proof.capture_sources(None, "page", [row, dict(row)], [])


def test_failed_scale_capture_path_retains_only_page_batch_commitments(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    monkeypatch.setattr(proof, "_relation", lambda *a, **k: None)
    monkeypatch.setattr(handoff, "_digest", lambda *a, **k: "d" * 64)
    monkeypatch.setattr(
        handoff,
        "_source_batch",
        lambda headers, matches: handoff._SourceBatch(
            len(headers), "h" * 64, len(matches), "c" * 64,
        ),
    )

    class RepeatedRows:
        def __init__(self, row, count):
            self.row, self.count = row, count

        def __iter__(self):
            return repeat(self.row, self.count)

        def __len__(self):
            return self.count

    pages, rows = 529, 5_221_054
    base, remainder = divmod(rows, pages)
    header = {
        "id": "source", "storage_day": date(2000, 1, 1),
        "row_hash": "a" * 64, "market_commit_seq": 1,
        "hot_present": False, "_handoff_oid": 1, "_handoff_hot_oid": None,
    }
    manifest = _cold_manifest(source_id="source")
    for page in range(pages):
        identity = f"page-{page:04d}"
        proof.pages[identity] = ({}, {})
        proof.source_batches[identity] = []
        count = base + int(page < remainder)
        proof.capture_sources(
            None, identity, RepeatedRows(header, count),
            RepeatedRows(manifest, count),
        )
        proof.record_source_hash(identity, "a" * 64)
    assert proof.source_row_count == rows
    assert len(proof.source_commitments) == pages
    assert proof.retained_bytes < 4 * 1024 * 1024
    assert proof.cold_days == {date(1999, 12, 31)}
    assert set(proof.cold_manifests) == {"manifest"}
    assert not hasattr(proof, "source_frames") and not hasattr(proof, "source_headers")


def test_prepare_validates_page_commitment_cardinality_without_rereading_sources():
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    proof.bound = (None, None, {}, None)
    proof.source_commitments["page"] = handoff._SourceCommitment(
        (handoff._SourceBatch(1, "h" * 64, 0, "c" * 64),), "a" * 64,
    )
    proof.catalogs["page"] = {"canonical_dependencies": SimpleNamespace(count=1)}
    proof.prepare_headers(None, lambda: None)
    assert proof.authority_prepared and not proof.admitted and not proof.headers_current
    with pytest.raises(RuntimeError, match="source_proof_not_current"):
        proof.source_hash("page")
    with pytest.raises(RuntimeError, match="not_admitted"):
        proof.verify_headers(None, lambda: None)


def test_prepare_refuses_mismatched_page_commitment():
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    proof.bound = (None, None, {}, None)
    proof.source_commitments["page"] = handoff._SourceCommitment(
        (handoff._SourceBatch(1, "h" * 64, 0, "c" * 64),), "a" * 64,
    )
    proof.catalogs["page"] = {"canonical_dependencies": SimpleNamespace(count=2)}
    with pytest.raises(RuntimeError, match="source_commitment_invalid"):
        proof.prepare_headers(None, lambda: None)


def test_admitted_authority_check_is_required_before_page_source_hash(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    proof.admitted = True
    proof.authority_prepared = True
    proof.source_commitments["page"] = handoff._SourceCommitment((), "p" * 64)
    calls = []
    monkeypatch.setattr(proof, "_verify_manifest_authority", lambda *a: calls.append("authority"))
    proof.verify_headers(None, lambda: calls.append("budget"))
    assert calls == ["authority", "budget"] and proof.headers_current
    proof.fresh_catalogs.add("page")
    proof.verify_sources(None, "page", lambda: None)
    assert proof.source_hash("page") == "p" * 64


def test_manifest_authority_rejects_changed_row():
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    manifest = _cold_manifest()
    proof.cold_manifests["manifest"] = handoff._digest([manifest], handoff._MANIFEST)
    proof.cold_days.add(date(1999, 12, 31))

    def execute(statement, parameters):
        sql = str(statement)
        if "FROM market.fact_archive_manifests" in sql and "WITH ordered" not in sql:
            changed = {**manifest, "manifest_hash": "c" * 64}
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [changed]))
        if "FROM market.fact_retention_partitions" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [
                {"storage_day": date(1999, 12, 31), "state": "verified"},
            ]))
        return SimpleNamespace(scalar_one_or_none=lambda: None)

    with pytest.raises(RuntimeError, match="manifest_authority_changed"):
        proof._verify_manifest_authority(SimpleNamespace(execute=execute))

def _catalog_batch_fixture(count, *, tid_chars=8):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    proof.admitted = True
    pages = []
    for index in range(count):
        identity = f"page-{index:04d}"
        page, receipt = {"id": identity}, {"verification_hash": identity}
        proof.pages[identity] = (page, receipt)
        proof.generations[identity] = None
        proof.catalogs[identity] = {name: handoff._Rows(
            1, (("manifest_id", "text"),) + columns, '{"(1,1)"}' + " " * tid_chars,
            index % 3, identity + name) for name, (_, columns) in handoff._CATALOGS.items()}
        pages.append((page, receipt))
    return proof, pages


def _batch_session(proof, *, change=None):
    calls = []
    def execute(statement, parameters):
        sql = str(statement)
        if "FROM market.fact_archive_active_generations" in sql:
            rows = [proof.generations[identity] for identity in parameters["ids"]
                    if proof.generations[identity] is not None]
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))
        name = next(name for name, (table, _) in handoff._CATALOGS.items() if f"market.{table} " in sql)
        batch_size = sum(key.startswith("id_") for key in parameters)
        parameters = {"ids": [parameters[f"id_{i}"] for i in range(batch_size)],
                      "tids": [parameters[f"tids_{i}"] for i in range(batch_size)]}
        calls.append((name, parameters))
        assert "requested.tids" not in sql
        if len(parameters["ids"]) > 1:
            assert "UNION ALL" in sql
        rows = [{"manifest_id": identity, "hinted_count": proof.catalogs[identity][name].count,
                 "logical_count": proof.catalogs[identity][name].count,
                 "digest": proof.catalogs[identity][name].digest} for identity in parameters["ids"]]
        if change is not None:
            rows = change(name, parameters["ids"], rows)
        # Result ordering must not become proof authority.
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows[::-1]))
    return SimpleNamespace(execute=execute), calls


def test_many_pages_keep_every_catalog_check_with_bounded_round_trips():
    proof, pages = _catalog_batch_fixture(464)
    session, calls = _batch_session(proof)
    # Same metadata batching as the actual partition evidence loop.
    for offset in range(0, len(pages), 100):
        proof.verify_catalog_batch(session, pages[offset:offset + 100], lambda: None)
    assert proof.fresh_catalogs == set(proof.pages)
    assert len(calls) == 128  # Formerly four calls per page = 1,856.
    for name in handoff._CATALOGS:
        checked = [identity for actual, params in calls if actual == name for identity in params["ids"]]
        assert checked == [page["id"] for page, _ in pages]
    assert all(1 <= len(params["ids"]) <= 16 for _, params in calls)


@pytest.mark.parametrize("change", ["digest", "logical_count", "hinted_count", "missing", "extra", "duplicate"])
def test_late_catalog_mismatch_never_marks_batch_current(change):
    proof, pages = _catalog_batch_fixture(17)
    def mutate(name, identities, rows):
        if name != "canonical_dependencies" or "page-0016" not in identities:
            return rows
        if change == "missing":
            return []
        if change == "extra":
            return rows + [{**rows[0], "manifest_id": "unrequested"}]
        if change == "duplicate":
            return rows + rows
        return [{**row, change: "bad" if change == "digest" else 500} for row in rows]
    session, calls = _batch_session(proof, change=mutate)
    with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
        proof.verify_catalog_batch(session, pages, lambda: None)
    assert calls and proof.fresh_catalogs == set()


def test_changed_active_generation_pointer_refuses_before_catalog_freshness():
    proof, pages = _catalog_batch_fixture(1)
    proof.generations["page-0000"] = {
        "manifest_id": "page-0000", "preparation_id": "prep-original",
        "generation": 1, "catalog_commitment": "a" * 64,
    }
    session, calls = _batch_session(proof)
    proof.generations["page-0000"] = {
        "manifest_id": "page-0000", "preparation_id": "prep-changed",
        "generation": 2, "catalog_commitment": "b" * 64,
    }
    captured = dict(proof.generations["page-0000"])
    proof.generations["page-0000"] = {
        "manifest_id": "page-0000", "preparation_id": "prep-original",
        "generation": 1, "catalog_commitment": "a" * 64,
    }

    original_execute = session.execute
    def changed_pointer(statement, parameters):
        if "FROM market.fact_archive_active_generations" in str(statement):
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [captured]))
        return original_execute(statement, parameters)
    session.execute = changed_pointer

    with pytest.raises(RuntimeError, match="generation_pointer_changed"):
        proof.verify_catalog_batch(session, pages, lambda: None)
    assert not calls and not proof.fresh_catalogs


def test_catalog_parameter_volume_is_bounded_and_large_single_page_stays_supported():
    proof, pages = _catalog_batch_fixture(3, tid_chars=150000)
    session, calls = _batch_session(proof)
    proof.verify_catalog_batch(session, pages, lambda: None)
    assert all(len(params["ids"]) == 1 for _, params in calls)
    proof, pages = _catalog_batch_fixture(1, tid_chars=300000)
    session, calls = _batch_session(proof)
    proof.verify_catalog_batch(session, pages, lambda: None)
    assert len(calls) == 4 and proof.fresh_catalogs == {"page-0000"}


def test_budget_expiry_after_batch_sql_does_not_make_catalogs_current():
    proof, pages = _catalog_batch_fixture(17)
    session, calls = _batch_session(proof)
    def deadline():
        if calls:
            raise RuntimeError("handoff deadline")
    with pytest.raises(RuntimeError, match="handoff deadline"):
        proof.verify_catalog_batch(session, pages, deadline)
    assert len(calls) == 1 and not proof.fresh_catalogs


@pytest.mark.parametrize("change", ["page", "receipt", "duplicate", "unadmitted"])
def test_batch_scope_is_exact_before_sql(change):
    proof, pages = _catalog_batch_fixture(2)
    if change == "page":
        pages[1] = ({**pages[1][0], "row_count": 99}, pages[1][1])
    elif change == "receipt":
        pages[1] = (pages[1][0], {"verification_hash": "changed"})
    elif change == "duplicate":
        pages.append(pages[0])
    else:
        proof.admitted = False
    session, calls = _batch_session(proof)
    with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
        proof.verify_catalog_batch(session, pages, lambda: None)
    assert not calls and not proof.fresh_catalogs
