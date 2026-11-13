from datetime import datetime
import pytest
from portal.backend.service.research import activity_range, scope

@pytest.mark.db
def test_disposable_activity_half_open_range_counts_all_matching_records():
    import uuid
    from sqlalchemy import delete
    from portal.backend.db.session import db
    from portal.backend.db.models import ResearchItemRecord as Item
    token=uuid.uuid4().hex
    ids=[token+str(i) for i in range(3)]
    before=activity_range.activity("hypotheses_created","1999-01-01T00:00Z","1999-01-01T00:02Z")
    try:
        with db.session() as s:
            for id,minute in zip(ids,[0,1,2]):
                s.add(Item(id=id,kind="hypothesis",status="draft",title="range fixture",payload={},tags=[],created_at=datetime(1999,1,1,0,minute)))
        result=activity_range.activity("hypotheses_created","1999-01-01T00:00Z","1999-01-01T00:02Z")
        assert result["total"]==before["total"]+2
        assert [new["total"]-old["total"] for new,old in zip(result["buckets"],before["buckets"])]==[1,1]
    finally:
        with db.session() as s:s.execute(delete(Item).where(Item.id.in_(ids)))


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

