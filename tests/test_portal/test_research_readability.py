from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
import pytest
from sqlalchemy.dialects import postgresql
from portal.backend.service.research import activity_range, scope

@pytest.mark.parametrize("a,b", [("2026-01-01", "2026-01-02Z"), ("2026-01-01T00:00Z", "2026-01-01T00:00Z"), ("2024-01-01T00:00Z", "2026-01-01T00:00Z"), ("bad", "bad")])
def test_activity_rejects_ambiguous_empty_reversed_or_excessive_ranges(a,b):
    with pytest.raises(ValueError): activity_range.bounds(a,b)

def test_activity_normalizes_offsets():
    a,b=activity_range.bounds("2026-01-01T01:00:00+01:00", "2026-01-01T02:00:00+01:00")
    assert a == datetime(2026,1,1) and b == datetime(2026,1,1,1)

@pytest.mark.parametrize("kind,column,qualification", [("checks_completed", "created_at", "research_check"), ("observations_recorded", "created_at", "observation"), ("hypotheses_created", "created_at", "hypothesis"), ("backtests_completed", "ended_at", "backtest")])
def test_activity_sql_filters_all_records_and_keeps_partial_buckets(monkeypatch,kind,column,qualification):
    statements=[]
    class Session:
        def execute(self,q):
            statements.append(q)
            return SimpleNamespace(all=lambda:[SimpleNamespace(at=datetime(2026,1,1,0,0),total=600)])
    @contextmanager
    def session(): yield Session()
    monkeypatch.setattr(activity_range.browse,"read_session",session)
    result=activity_range.activity(kind,"2026-01-01T00:00:30Z","2026-01-01T00:05:30Z")
    sql=str(statements[0].compile(dialect=postgresql.dialect()))
    assert column in sql and " >= " in sql and " < " in sql and "count(" in sql
    assert qualification in statements[0].compile().params.values()
    assert result["total"]==600 and len(result["buckets"])==6
    assert result["buckets"][-1]["total"]==0
    assert result["timestamp_field"]==column and result["bucket_unit"]=="minute"

@pytest.mark.parametrize("end,unit,max_buckets", [("2026-01-02T00:00Z","hour",24),("2026-01-08T00:00Z","hour",168),("2027-01-02T00:00Z","day",366)])
def test_activity_response_is_bounded_and_zero_filled(monkeypatch,end,unit,max_buckets):
    @contextmanager
    def session(): yield SimpleNamespace(execute=lambda q:SimpleNamespace(all=lambda:[]))
    monkeypatch.setattr(activity_range.browse,"read_session",session)
    result=activity_range.activity("hypotheses_created","2026-01-01T00:00Z",end)
    assert result["bucket_unit"]==unit and len(result["buckets"])==max_buckets and result["total"]==0

def test_scope_uses_only_selected_check_ids_and_rechecks_custody(monkeypatch):
    from portal.backend.service.research import browse
    checked=[];queries=[]
    checks=[SimpleNamespace(_mapping={"id":"check-btc","instrument_id":"btc","timeframe":"1m","_result_hash":"result","_evidence_hash":"evidence"}),SimpleNamespace(_mapping={"id":"check-eth","instrument_id":"eth","timeframe":"1m","_result_hash":"result","_evidence_hash":"evidence"})]
    labels=[SimpleNamespace(id="btc",_mapping={"id":"btc","symbol":"BTC-USD"}),SimpleNamespace(id="eth",_mapping={"id":"eth","symbol":"ETH-USD"})]
    class Session:
        def execute(self,q):
            queries.append(q);return SimpleNamespace(all=lambda:checks if len(queries)==1 else labels)
    monkeypatch.setattr(browse,"_assert_item_access",lambda id,s:checked.append(id))
    result=scope.context({"id":"study"},{"references":[{"kind":"research_check","item_id":"check-btc","result_hash":"result","evidence_hash":"evidence"},{"kind":"research_check","item_id":"check-eth","result_hash":"result","evidence_hash":"evidence"},{"kind":"observation","item_id":"not-a-scope-source"}]},Session())
    assert result["basis"]=="referenced_check_metadata" and result["complete"]
    assert {r["instrument"]["symbol"] for r in result["items"]}=={"BTC-USD","ETH-USD"}
    assert set(checked)=={"check-btc","check-eth"}
    assert set(queries[0].compile().params["id_1"])=={"check-btc","check-eth"}

def test_missing_scope_is_not_inferred_from_title():
    result=scope.context({"id":"study","title":"BTC2022"},None,None)
    assert result["items"][0]["instrument_id"] is None

@pytest.mark.db
def test_disposable_activity_half_open_range_counts_all_matching_records():
    import uuid
    from sqlalchemy import delete
    from portal.backend.db.session import db
    from portal.backend.db.models import ResearchItemRecord as Item
    token=uuid.uuid4().hex
    ids=[token+str(i) for i in range(3)]
    try:
        with db.session() as s:
            for id,minute in zip(ids,[0,1,2]):
                s.add(Item(id=id,kind="hypothesis",status="draft",title="range fixture",payload={},tags=[],created_at=datetime(1999,1,1,0,minute)))
        result=activity_range.activity("hypotheses_created","1999-01-01T00:00Z","1999-01-01T00:02Z")
        assert result["total"]==2 and [x["total"] for x in result["buckets"]]==[1,1]
    finally:
        with db.session() as s:s.execute(delete(Item).where(Item.id.in_(ids)))


def test_extreme_timezone_and_terminal_year_are_safe(monkeypatch):
    with pytest.raises(ValueError):activity_range.bounds("0001-01-01T00:00:00+23:59","0001-01-02T00:00:00+23:59")
    @contextmanager
    def session():yield SimpleNamespace(execute=lambda q:SimpleNamespace(all=lambda:[]))
    monkeypatch.setattr(activity_range.browse,"read_session",session)
    result=activity_range.activity("hypotheses_created","9999-12-30T00:00Z","9999-12-31T23:59Z")
    assert result["total"]==0 and len(result["buckets"])==48

def test_scope_rejects_changed_stored_identity_and_access_failure(monkeypatch):
    from portal.backend.service.research import browse
    row=SimpleNamespace(_mapping={"id":"check","instrument_id":"btc","_result_hash":"changed","_evidence_hash":"evidence"})
    session=SimpleNamespace(execute=lambda q:SimpleNamespace(all=lambda:[row]))
    monkeypatch.setattr(browse,"_assert_item_access",lambda *args:None)
    refs={"references":[{"kind":"research_check","item_id":"check","result_hash":"original","evidence_hash":"evidence"}]}
    result=scope.context({"id":"study"},refs,session)
    assert not result["complete"] and result["items"]==[]
    def denied(*args):raise ValueError("inaccessible")
    monkeypatch.setattr(browse,"_assert_item_access",denied)
    with pytest.raises(ValueError,match="inaccessible"):scope.context({"id":"study"},refs,session)



def test_inventory_scope_keeps_per_study_identity_and_partial_state(monkeypatch):
    from portal.backend.service.research import browse
    refs=[SimpleNamespace(study_id="first",reference={"item_id":"check","result_hash":"current","evidence_hash":"e"}),SimpleNamespace(study_id="second",reference={"item_id":"check","result_hash":"old","evidence_hash":"e"})]
    row=SimpleNamespace(_mapping={"id":"check","symbol":"BTC","_result_hash":"current","_evidence_hash":"e"})
    calls=[]
    class Session:
        def execute(self,q):
            calls.append(q);return SimpleNamespace(all=lambda:refs if len(calls)==1 else [row])
    monkeypatch.setattr(browse,"_assert_item_access",lambda *args:None)
    items=[{"id":"first","kind":"study"},{"id":"second","kind":"study"}]
    scope.inventory(items,Session())
    assert items[0]["scope"]["complete"] and items[0]["scope"]["items"][0]["symbol"]=="BTC"
    assert not items[1]["scope"]["complete"] and items[1]["scope"]["items"]==[]

@pytest.mark.db
def test_disposable_latest_scope_jsonpath_preserves_incomplete_metadata():
    import uuid
    from sqlalchemy import delete
    from portal.backend.db.session import db
    from portal.backend.db.models import ResearchItemRecord as Item
    from portal.backend.service.research import browse
    token=uuid.uuid4().hex;check=token+"c";study=token+"s"
    try:
        with db.session() as session:
            session.add(Item(id=check,kind="research_check",status="tested",title="scope fixture",symbol="TEST",payload={"result":{"result_hash":"r"},"evidence":{"evidence_hash":"e"}},tags=[]))
            session.add(Item(id=study,kind="study",status="active",title="scope fixture",payload={"question_contract":{"publications":[{"references":[{"kind":"research_check","item_id":check,"result_hash":"r","evidence_hash":"e"}]}]}},tags=[]))
        with browse.read_session() as session:
            items=[{"id":study,"kind":"study"}];scope.inventory(items,session)
            assert items[0]["scope"]["complete"] and items[0]["scope"]["items"][0]["symbol"]=="TEST"
    finally:
        with db.session() as session:session.execute(delete(Item).where(Item.id.in_([check,study])))
