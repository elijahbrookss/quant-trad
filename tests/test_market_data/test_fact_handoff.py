from datetime import date, datetime, timedelta, timezone
import hashlib
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
    witness = handoff._Rows(1, (("manifest_id", "text"), ("fact_version_id", "text"), ("row_hash", "text")),
                            '{"(1,1)"}', 1, "expected")
    proof.catalogs["page"] = {"canonical_dependencies": witness}
    sql = []
    def execute(statement, parameters):
        sql.append(str(statement))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: {
            "hinted_count": 1, "logical_count": 1, "digest": "expected", **change}))
    with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
        proof.verify_catalogs(SimpleNamespace(execute=execute), page, receipt, lambda: None)
    assert len(sql) == 1 and "logical_count" in sql[0] and "q.ctid=ANY" in sql[0]


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
    monkeypatch.setattr(proof, "_metadata", lambda *a: {**relation, **({"filenode": 21} if failure == "rewrite" else {})})
    if failure == "wrong_parent":
        relation["parents"] = [11]
    def contract(_):
        if failure == "key_contract":
            raise RuntimeError("key_contract_changed")
    monkeypatch.setattr(handoff, "assert_fact_identity_contract", contract)
    statements = []
    def execute(statement, *_):
        value = str(statement); statements.append(value)
        scalar = ("replica" if failure == "replica" else "origin") if "session_replication_role" in value else 10
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


def test_global_header_runs_deduplicate_only_identical_fresh_bindings(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    monkeypatch.setattr(proof, "_relation", lambda *a, **k: None)
    proof.pages["page"] = ({}, {})
    def row(tid, identity):
        return {"id": identity, "storage_day": date(2000, 1, 1), "row_hash": "a" * 64,
                "market_commit_seq": 1, "hot_present": True, "_handoff_oid": 1, "_handoff_tid": tid}
    first, second = row("(10,2)", "first"), row("(2,10)", "second")
    proof.capture_sources(None, "page", [first], [])
    proof.capture_sources(None, "page", [second, first], [])
    proof._compile_headers()
    assert proof.source_frames == {}
    witness, = proof.source_headers
    assert witness.count == 2 and witness.tids == '{"(2,10)","(10,2)"}'
    assert witness.digest == hashlib.sha256(b"".join(hashlib.sha256(handoff._record(item, handoff._HEADER)).digest()
                                                   for item in [second, first])).hexdigest()
    assert witness.hashed_rows is True


def test_global_duplicate_tuple_with_different_first_pass_fields_refuses(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    monkeypatch.setattr(proof, "_relation", lambda *a, **k: None)
    proof.pages["page"] = ({}, {})
    row = {"id": "same", "storage_day": date(2000, 1, 1), "row_hash": "a" * 64,
           "market_commit_seq": 1, "hot_present": True, "_handoff_oid": 1, "_handoff_tid": "(2,1)"}
    proof.capture_sources(None, "page", [row], [])
    proof.capture_sources(None, "page", [{**row, "row_hash": "b" * 64}], [])
    with pytest.raises(RuntimeError, match="inconsistent_source_tuple"):
        proof._compile_headers()


def test_duplicate_heavy_source_merge_checks_cancellation_before_distinct_flush():
    calls = []
    def cancel():
        calls.append(True)
        if len(calls) == 4:
            raise RuntimeError("cancel duplicate-heavy merge")
    proof = handoff.FactHandoffProof(check_budget=cancel)
    frame = struct.pack("!IHi", 1, 1, 0) + b"a" * 32
    proof.source_frames[1] = [frame * 10000]
    with pytest.raises(RuntimeError, match="cancel duplicate-heavy"):
        proof._compile_headers()
    assert len(calls) == 4 and proof.bound is None and not proof.source_headers


def test_preparation_does_not_admit_or_make_cached_source_hash_current(monkeypatch):
    proof = handoff.FactHandoffProof(check_budget=lambda: None)
    proof.bound = (None, None, {}, None)
    proof.source_hashes["page"] = "old"
    calls = []
    monkeypatch.setattr(proof, "_compare_headers", lambda *a: calls.append("fresh read"))
    proof.prepare_headers(None, lambda: None)
    assert calls == ["fresh read"] and not proof.admitted and not proof.headers_current
    with pytest.raises(RuntimeError, match="source_proof_not_current"):
        proof.source_hash("page")
    with pytest.raises(RuntimeError, match="not_admitted"):
        proof.verify_headers(None, lambda: None)
    with pytest.raises(RuntimeError, match="source_proof_not_current"):
        proof.verify_sources(None, "page", lambda: None)
