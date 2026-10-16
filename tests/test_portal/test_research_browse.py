from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from portal.backend.service.research import browse


def test_cursor_is_exact_filter_scoped_and_invalid_cursor_fails():
    import base64
    import json
    scope = json.dumps({"kind": "study"}, sort_keys=True)
    encoded = base64.urlsafe_b64encode(json.dumps({"scope": scope, "id": "item", "created_at": "2026-10-16T12:00:00"}).encode()).decode()
    assert browse._cursor(encoded, scope) == (datetime(2026, 10, 16, 12), "item")
    for cursor in ("!", encoded, "a" * 2049):
        with pytest.raises(ValueError, match="cursor"):
            browse._cursor(cursor, "other filters")


def test_inventory_selects_metadata_not_full_payload_and_bounds_page(monkeypatch):
    statements = []
    rows = [SimpleNamespace(created_at=datetime(2026, 10, 16), id=str(i), _mapping={"id": str(i), "kind": "study"}) for i in range(3)]
    class Session:
        def scalar(self, statement):
            statements.append(statement)
            return 654
        def execute(self, statement):
            statements.append(statement)
            return SimpleNamespace(all=lambda: rows)
    @contextmanager
    def session():
        yield Session()
    monkeypatch.setattr(browse, "read_session", session)
    result = browse.inventory(kind="study", limit=2, q="100%_BTC")
    assert len(result["items"]) == 2 and result["total"] == 654 and result["next_cursor"]
    sql = str(statements[-1].compile(dialect=postgresql.dialect()))
    assert "portal_research_items.payload," not in sql
    assert "portal_research_items.body" not in sql
    assert "LIMIT" in sql and "created_at DESC" in sql and "id DESC" in sql
    assert r"%100\%\_BTC%" in statements[-1].compile().params.values()
    with pytest.raises(ValueError):
        browse.inventory(kind="study", limit=501)


def test_detail_legacy_empty_and_missing_revision_are_explicit(monkeypatch):
    row = SimpleNamespace(_mapping={"id": "legacy", "kind": "study", "body": "Older Study", "question_contract": None})
    class Session:
        def execute(self, statement):
            return SimpleNamespace(one_or_none=lambda: row)
    @contextmanager
    def session():
        yield Session()
    monkeypatch.setattr(browse, "read_session", session)
    monkeypatch.setattr(browse, "_assert_item_access", lambda *args: None)
    result = browse.detail("legacy")
    assert result["publication"] is None and result["history"] == []
    with pytest.raises(KeyError, match="no interpretation"):
        browse.detail("legacy", 2)


@pytest.mark.db
def test_disposable_large_inventory_pagination_aggregates_and_compact_relations():
    import uuid
    from portal.backend.service.research import repository
    from portal.backend.db.models import ResearchItemRecord
    from portal.backend.db.session import db
    token = uuid.uuid4().hex
    with db.session() as session:
        for index in range(630):
            repository.create_item(kind="study", title=f"{token}-{index:03d}", symbol=token,
                timeframe="1m" if index % 2 else "15m", payload={"heterogeneous": {"blob": "x" * 4096}}, session=session)
    ids = []
    cursor = None
    import json
    while True:
        page = browse.inventory(kind="study", q=token, limit=25, cursor=cursor)
        assert page["total"] == 630
        assert len(json.dumps(page).encode()) < 100_000
        assert all("payload" not in item and "body" not in item for item in page["items"])
        ids.extend(item["id"] for item in page["items"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert len(ids) == len(set(ids)) == 630
    assert browse.detail(ids[0])["publication"] is None
    before = browse.overview()
    check = repository.create_item(kind="research_check", title="Synthetic linked definition", payload={"definition": {"definition_id": "synthetic-definition"}})
    repository.create_link(source_item_id=ids[0], target_type="research_item", target_id=check["id"], relation="supports")
    related = browse.relationships(ids[0])
    assert related["items"][0]["id"] == check["id"]
    summary = browse.overview()
    assert summary["studies"] >= 630 and summary["dimensions"]["procedure"]["multi_membership"] is True
    assert summary["dimensions"]["procedure"]["unknown_studies"] == before["dimensions"]["procedure"]["unknown_studies"] - 1
    assert len(json.dumps(summary).encode()) < 250_000
    with db.session() as session:
        session.query(ResearchItemRecord).filter(ResearchItemRecord.id.in_(ids)).delete(synchronize_session=False)


def test_payload_only_sealed_dependencies_are_projected_and_gate_detail(monkeypatch):
    seen = []
    class Session:
        def execute(self, statement):
            seen.append(str(statement.compile(dialect=postgresql.dialect())))
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: ["sealed-dataset"]))
    def denied(item, session):
        assert item["dependencies"] == [{"dataset_id": "sealed-dataset"}]
        raise ValueError("question_evidence_inaccessible: sealed scientific evidence")
    monkeypatch.setattr(browse.publication, "_assert_access", denied)
    with pytest.raises(ValueError, match="inaccessible"):
        browse._assert_item_access("payload-only-check", Session())
    assert "jsonb_path_query" in seen[0] and "LIMIT" in seen[0]


def test_empty_publication_fallback_binds_sql_text_array_not_json_string():
    expression = browse.PUBLICATION_COUNT
    compiled = expression.compile(dialect=postgresql.dialect())
    fallback = [bind for bind in compiled.binds.values() if bind.value == "[]"]
    assert fallback
    for bind in fallback:
        processor = bind.type.bind_processor(postgresql.dialect())
        assert (processor(bind.value) if processor else bind.value) == "[]"
