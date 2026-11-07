"""Compact, bounded research-memory reads. No scientific execution or payload scans."""
from __future__ import annotations

import base64
from contextlib import contextmanager
from datetime import datetime
import json
from typing import Any

from sqlalchemy import and_, case, cast, func, literal, or_, select, String, text
from sqlalchemy.orm import aliased

from portal.backend.db.models import ResearchItemRecord as Item, ResearchLinkRecord as Link
from portal.backend.db.session import db
from . import publication
from . import scope as scope_projection

KINDS = {"study", "research_check", "observation", "hypothesis"}
SCALARS = ("id", "kind", "title", "status", "symbol", "instrument_id", "timeframe",
           "datasource", "exchange", "window_start", "window_end", "created_at", "updated_at", "source_revision")
PUBLICATIONS = Item.payload["question_contract"]["publications"]
PUBLICATION_COUNT = func.jsonb_array_length(func.coalesce(PUBLICATIONS, cast(literal("[]", String), Item.payload.type)))


@contextmanager
def read_session():
    with db.session() as session:
        # A browser disconnect does not interrupt a synchronous worker. Bound SQL
        # independently; do not reuse scientific-job cancellation semantics.
        session.connection(execution_options={"isolation_level": "REPEATABLE READ", "postgresql_readonly": True})
        session.execute(text("SET LOCAL statement_timeout = '5000ms'"))
        yield session


def _json(value):
    if isinstance(value, datetime):
        return value.isoformat() + "Z"
    return value


def columns(model=Item):
    return [getattr(model, key) for key in SCALARS]


def compact(row):
    return {key: _json(value) for key, value in row._mapping.items()}


def _filters(*, kind="study", q="", status=None, symbol=None, timeframe=None, publication_state=None):
    if kind not in KINDS | {"all"}:
        raise ValueError("research_browse_invalid: unsupported kind")
    if len(q) > 200:
        raise ValueError("research_browse_invalid: query exceeds 200 characters")
    if publication_state not in {None, "published", "unpublished"}:
        raise ValueError("research_browse_invalid: publication state")
    filters = [] if kind == "all" else [Item.kind == kind]
    if q.strip():
        needle = "%" + q.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        filters.append(or_(*(field.ilike(needle, escape="\\") for field in (Item.title, Item.id, Item.symbol))))
    for field, value in ((Item.status, status), (Item.symbol, symbol), (Item.timeframe, timeframe)):
        if value:
            if len(value) > 64:
                raise ValueError("research_browse_invalid: filter exceeds 64 characters")
            filters.append(field == value)
    if publication_state:
        filters.append(PUBLICATION_COUNT > 0 if publication_state == "published" else PUBLICATION_COUNT == 0)
    return filters


def _cursor(value, scope):
    if not value:
        return None
    try:
        if len(value) > 2048:
            raise ValueError()
        raw = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
        if raw["scope"] != scope or not isinstance(raw["id"], str) or len(raw["id"]) > 64:
            raise ValueError()
        return datetime.fromisoformat(raw["created_at"]), raw["id"]
    except (KeyError, ValueError, TypeError, UnicodeError) as exc:
        raise ValueError("research_browse_invalid: cursor does not match filters") from exc


def inventory(*, limit=25, cursor=None, **filters):
    if not 1 <= limit <= 100:
        raise ValueError("research_browse_invalid: limit must be 1..100")
    scope = json.dumps(filters, sort_keys=True)
    where = _filters(**filters)
    anchor = _cursor(cursor, scope)
    latest = PUBLICATIONS[-1]
    query = select(*columns(), PUBLICATION_COUNT.label("publication_count"),
                   latest["revision"].astext.label("latest_revision"),
                   latest["publication_hash"].astext.label("publication_hash")).where(*where)
    if anchor:
        query = query.where(or_(Item.created_at < anchor[0], and_(Item.created_at == anchor[0], Item.id < anchor[1])))
    with read_session() as session:
        total = session.scalar(select(func.count()).select_from(Item).where(*where))
        rows = session.execute(query.order_by(Item.created_at.desc(), Item.id.desc()).limit(limit + 1)).all()
        items = [compact(row) for row in rows[:limit]]
        scope_projection.instruments(items, session)
        scope_projection.inventory(items, session)
    more = len(rows) > limit
    next_cursor = None
    if more:
        last = rows[limit - 1]
        next_cursor = base64.urlsafe_b64encode(json.dumps({"scope": scope, "created_at": last.created_at.isoformat(), "id": last.id}).encode()).decode().rstrip("=")
    return {"schema_version": "research_inventory.v1", "items": items, "total": total,
            "next_cursor": next_cursor, "order": "created_at_desc_id_desc", "counting_rule": "unique_saved_records"}


def overview():
    """Aggregate in SQL; never fetch every full Study or trail into Python."""
    with read_session() as session:
        counts = session.execute(select(Item.kind, func.count()).group_by(Item.kind)).all()
        published = session.scalar(select(func.count()).select_from(Item).where(Item.kind == "study", PUBLICATION_COUNT > 0))
        study_count = dict(counts).get("study", 0)
        dimensions = {}
        for name, field in (("symbol", Item.symbol), ("timeframe", Item.timeframe),
                            ("period", func.concat(func.coalesce(cast(Item.window_start, String), "Unknown"), " → ", func.coalesce(cast(Item.window_end, String), "Unknown")))):
            label = func.coalesce(func.nullif(field, ""), "Unknown")
            count = func.count()
            rows = session.execute(select(label.label("label"), count.label("count")).where(Item.kind == "study").group_by(label).order_by(count.desc(), label).limit(12)).all()
            dimensions[name] = {"items": [dict(row._mapping) for row in rows],
                                "other": study_count - sum(row.count for row in rows), "multi_membership": False}
        # A Study may refer to several Check definitions. Count each Study once
        # per actual linked definition; totals need not equal Study count.
        check = aliased(Item)
        family = func.coalesce(func.nullif(func.left(check.payload["definition"]["definition_id"].astext, 128), ""), "Unknown")
        joined = select(Item.id.label("study_id"), family.label("label")).select_from(Item).join(Link, or_(
            Link.source_item_id == Item.id,
            and_(Link.target_type == "research_item", Link.target_id == Item.id))).join(check, and_(check.kind == "research_check", or_(
                and_(Link.source_item_id == Item.id, Link.target_type == "research_item", Link.target_id == check.id),
                and_(Link.target_id == Item.id, Link.target_type == "research_item", Link.source_item_id == check.id)))).where(Item.kind == "study").distinct().subquery()
        groups = session.execute(select(joined.c.label, func.count().label("count")).group_by(joined.c.label).order_by(func.count().desc(), joined.c.label).limit(12)).all()
        classified = session.scalar(select(func.count(func.distinct(joined.c.study_id))))
        memberships = session.scalar(select(func.count()).select_from(joined))
        dimensions["procedure"] = {"items": [dict(row._mapping) for row in groups], "unknown_studies": study_count - classified,
                                   "other_memberships": memberships - sum(row.count for row in groups), "multi_membership": True}
    return {"schema_version": "research_overview.v1", "studies": study_count, "published": published,
            "unpublished": study_count - published, "records_by_kind": dict(counts), "dimensions": dimensions,
            "counting_rule": "saved_study_ids_not_independent_scientific_evidence",
            "period_meaning": "stored_window_start_and_end_not_continuous_source_coverage"}


def _assert_item_access(item_id, session):
    # Project only dependency identifiers, including nested payload-only bindings.
    # Current protocol custody still makes the access decision. Target IDs are
    # conservatively checked too; no arbitrary full payload crosses this seam.
    dependencies = select(func.jsonb_path_query(Item.payload, '$.**.dataset_id').label("dependency")).where(Item.id == item_id).union(
        select(func.jsonb_path_query(Item.payload, '$.**.target_id').label("dependency")).where(Item.id == item_id)).limit(513)
    values = session.execute(dependencies).scalars().all()
    if len(values) > 512:
        raise ValueError("research_browse_limit: too many dependency identities for compact access verification")
    publication._assert_access({"id": item_id, "dependencies": [{"dataset_id": str(value)} for value in values]}, session)


def detail(item_id: str, revision: int | None = None):
    with read_session() as session:
        row = session.execute(select(*columns(), Item.body,
            Item.payload[publication.KEY].label("question_contract")).where(Item.id == item_id)).one_or_none()
        if row is None:
            raise KeyError("Research item not found")
        item = compact(row)
        contract = item.pop("question_contract")
        _assert_item_access(item_id, session)
        selected = None
        history = []
        if contract:
            if len(json.dumps(contract).encode()) > publication.MAX_HISTORY_BYTES:
                raise ValueError("research_browse_limit: question contract exceeds retained bound")
            publication.verify_history(contract, item_id)
            publications = contract.get("publications", [])
            for entry in publications:
                history.append({key: entry.get(key) for key in ("revision", "publication_hash", "published_at", "completion_meaning")})
            selected = next((p for p in publications if p["revision"] == revision), None) if revision is not None else (publications[-1] if publications else None)
            if revision is not None and selected is None:
                raise KeyError("Study interpretation revision not found")
        elif revision is not None:
            raise KeyError("Study has no interpretation revision")
        scope_projection.instruments([item], session)
        scope_context = scope_projection.context(item, selected, session)
        if selected:
            selected = {**{key: selected.get(key) for key in ("revision", "publication_hash", "conclusion", "scope", "limitations", "published_at", "completion_meaning")}, "references": [{key: value for key, value in ref.items() if key not in {"snapshot", "relationships"}} for ref in selected["references"]]}
    return {"schema_version": "research_detail.v1", "item": item, "question": contract.get("question") if contract else None,
            "publication": selected, "history": history, "scope": scope_context}


def relationships(item_id: str, limit=50):
    if not 1 <= limit <= 100:
        raise ValueError("research_browse_invalid: relation limit")
    # Keep the existing full trail available explicitly; normal browsing is
    # one-hop, compact and capped, with no duplicated payload arrays.
    related_id = case((Link.source_item_id == item_id, Link.target_id), else_=Link.source_item_id)
    with read_session() as session:
        if session.scalar(select(Item.id).where(Item.id == item_id)) is None:
            raise KeyError("Research item not found")
        _assert_item_access(item_id, session)
        rows = session.execute(select(Link.id.label("link_id"), Link.relation, Link.target_type,
            Link.source_item_id, Link.target_id, *columns()).select_from(Link).outerjoin(Item,
            and_(Link.target_type == "research_item", Item.id == related_id)).where(or_(
                Link.source_item_id == item_id, and_(Link.target_type == "research_item", Link.target_id == item_id)))
            .order_by(Link.id).limit(limit + 1)).all()
        for row in rows[:limit]:
            if row.id:
                _assert_item_access(row.id, session)
        related = [compact(row) for row in rows[:limit]]
        scope_projection.instruments(related, session)
    return {"schema_version": "research_relationships.v1", "item_id": item_id,
            "items": related, "truncated": len(rows) > limit, "limit": limit}
