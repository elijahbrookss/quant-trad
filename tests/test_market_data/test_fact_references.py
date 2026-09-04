from types import SimpleNamespace

import pytest

from portal.backend.service.storage.repos.fact_references import lock_canonical_raw_references


class _Result(list):
    def scalar_one(self):
        return self[0]

    def scalars(self):
        return self

    def mappings(self):
        return self

    def all(self):
        return list(self)


class _Session:
    def __init__(self, mappings=(), expired=(), pending=(), isolation="read committed"):
        self.rows = list(mappings)
        self.expired = list(expired)
        self.pending = list(pending)
        self.isolation = isolation
        self.statements = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append((sql, params))
        if sql == "SHOW transaction_isolation":
            return _Result([self.isolation])
        if "FOR KEY SHARE" in sql:
            return _Result(params["ids"])
        if "storage_lifecycle_events" in sql:
            return _Result([{"target_id": identity, "event_type": "completed"} for identity in self.expired]
                           + [{"target_id": identity, "event_type": "planned"} for identity in self.pending])
        return _Result(self.rows)


def _trade(raw="raw-a"):
    return SimpleNamespace(fact_type="market.trade", observation_key="trade-a",
                           provenance={"_qt_trade_evidence": {"raw_record_id": raw}})


def _mapping(manifest, raw="raw-a", request="record:raw-a"):
    return {"request_key": request, "raw_record_id": raw, "manifest_id": manifest}


def test_reference_lock_precedes_fresh_expiry_check_and_sorts_manifest_locks():
    session = _Session([_mapping("manifest-z"), _mapping("manifest-a")], expired=["manifest-a"])
    lock_canonical_raw_references(session, [_trade()])
    assert "FOR KEY SHARE" in session.statements[-2][0]
    assert session.statements[-2][1]["ids"] == ["manifest-a", "manifest-z"]
    assert "storage_lifecycle_events" in session.statements[-1][0]


def test_each_raw_record_requires_a_live_copy_not_just_any_manifest_in_the_batch():
    session = _Session([_mapping("dead"), _mapping("live", raw="raw-b", request="record:raw-b")], expired=["dead"])
    with pytest.raises(RuntimeError, match="canonical_raw_reference_expired.*raw-a"):
        lock_canonical_raw_references(session, [_trade(), _trade("raw-b")])


def test_declared_but_unmapped_reference_fails_before_writing():
    session = _Session()
    with pytest.raises(RuntimeError, match="canonical_raw_reference_missing"):
        lock_canonical_raw_references(session, [_trade()])
    assert not any("FOR KEY SHARE" in sql for sql, _ in session.statements)


def test_unfinished_expiration_is_unavailable_even_without_completion_event():
    session = _Session([_mapping("uncertain")], pending=["uncertain"])
    with pytest.raises(RuntimeError, match="reference_expiration_pending.*resume"):
        lock_canonical_raw_references(session, [_trade()])


def test_live_compacted_copy_can_survive_an_unfinished_original_expiration():
    session = _Session([_mapping("uncertain"), _mapping("live")], pending=["uncertain"])
    lock_canonical_raw_references(session, [_trade()])


def test_low_level_writer_admits_return_to_original_hash_in_batch_order(monkeypatch):
    from portal.backend.service.storage.repos import fact_references
    from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository

    repo = PostgresMarketDataRepository()
    first = SimpleNamespace(observation_key="key", row_hash="hash-a")
    correction = SimpleNamespace(observation_key="key", row_hash="hash-b")
    session = _Session([{"observation_key": "key", "row_hash": "hash-a"}])
    monkeypatch.setattr(repo, "_assert_collection_fence", lambda *args, **kwargs: None)
    monkeypatch.setattr(repo, "_canonical_source_for_run", lambda *args: (1, None))
    admitted = []

    class AdmissionObserved(Exception):
        pass

    def capture_admission(session, facts):
        admitted.extend(facts)
        raise AdmissionObserved

    monkeypatch.setattr(fact_references, "lock_canonical_raw_references", capture_admission)
    with pytest.raises(AdmissionObserved):
        repo._ingest_canonical_rows_with_session(session, run_id="fixture", series_id=1,
                                                rows=[first, correction, first], allow_corrections=True)
    assert admitted == [correction, first]


def test_raw_reference_admission_rejects_a_stale_transaction_snapshot():
    session = _Session(isolation="repeatable read")
    with pytest.raises(RuntimeError, match="reference_isolation_invalid"):
        lock_canonical_raw_references(session, [_trade()])
    assert len(session.statements) == 1


def test_reference_free_facts_and_empty_pending_batch_need_no_archive_queries():
    session = _Session()
    lock_canonical_raw_references(session, [])
    lock_canonical_raw_references(session, [SimpleNamespace(fact_type="derivatives.funding_rate", provenance={})])
    assert session.statements == []


def test_mapping_budget_overflow_fails_not_partial_admission():
    session = _Session([_mapping("a"), _mapping("b"), _mapping("c")])
    with pytest.raises(RuntimeError, match="reference_budget_exceeded"):
        lock_canonical_raw_references(session, [_trade()], max_mapping_rows=2)


@pytest.mark.parametrize("value", [False, 0, 1.5])
def test_mapping_budget_requires_positive_integer(value):
    with pytest.raises(ValueError, match="reference_budget_invalid"):
        lock_canonical_raw_references(_Session(), [], max_mapping_rows=value)


@pytest.mark.parametrize("position", [{}, {"definition_id": "d", "session_id": "s", "connection_epoch": True, "receive_ordinal": 1},
                                     {"definition_id": "d", "session_id": "s", "connection_epoch": 0, "receive_ordinal": -1}])
def test_malformed_book_position_is_not_silently_ignored(position):
    fact = SimpleNamespace(fact_type="market.bbo", observation_key="bbo-a",
                           provenance={"_qt_bbo_evidence": {"source_position": position}})
    with pytest.raises(ValueError, match="reference_invalid"):
        lock_canonical_raw_references(_Session(), [fact])
