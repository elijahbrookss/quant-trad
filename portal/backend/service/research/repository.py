"""Persistence helpers for research memory."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping
import uuid

from portal.backend.db.models import ResearchItemRecord, ResearchLinkRecord
from portal.backend.db.session import db


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def parse_optional_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo else parsed


def create_item(
    *,
    kind: str,
    title: str,
    status: str = "draft",
    body: str | None = None,
    instrument_id: str | None = None,
    symbol: str | None = None,
    timeframe: str | None = None,
    datasource: str | None = None,
    exchange: str | None = None,
    window_start: Any = None,
    window_end: Any = None,
    tags: list[str] | None = None,
    payload: Mapping[str, Any] | None = None,
    source_revision: str | None = None,
    item_id: str | None = None,
) -> dict[str, Any]:
    now = utcnow()
    record = ResearchItemRecord(
        id=str(item_id or uuid.uuid4()),
        kind=_required(kind, "kind"),
        status=_required(status, "status"),
        title=_required(title, "title"),
        body=body,
        instrument_id=_optional(instrument_id),
        symbol=_optional(symbol),
        timeframe=_optional(timeframe),
        datasource=_optional(datasource),
        exchange=_optional(exchange),
        window_start=parse_optional_timestamp(window_start),
        window_end=parse_optional_timestamp(window_end),
        tags=list(tags or []),
        payload=dict(payload or {}),
        source_revision=_optional(source_revision),
        created_at=now,
        updated_at=now,
    )
    with db.session() as session:
        session.add(record)
        session.flush()
        return record.to_dict()


def get_item(item_id: str) -> dict[str, Any]:
    normalized = _required(item_id, "item_id")
    with db.session() as session:
        record = session.get(ResearchItemRecord, normalized)
        if record is None:
            raise KeyError("Research item not found")
        return record.to_dict()


def list_items(
    *,
    kind: str | None = None,
    status: str | None = None,
    symbol: str | None = None,
    timeframe: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with db.session() as session:
        query = session.query(ResearchItemRecord)
        if kind:
            query = query.filter(ResearchItemRecord.kind == str(kind).strip())
        if status:
            query = query.filter(ResearchItemRecord.status == str(status).strip())
        if symbol:
            query = query.filter(ResearchItemRecord.symbol == str(symbol).strip())
        if timeframe:
            query = query.filter(ResearchItemRecord.timeframe == str(timeframe).strip())
        records = (
            query.order_by(ResearchItemRecord.updated_at.desc(), ResearchItemRecord.created_at.desc())
            .limit(max(1, min(int(limit), 500)))
            .all()
        )
        return [record.to_dict() for record in records]


def create_link(
    *,
    source_item_id: str,
    target_type: str,
    target_id: str,
    relation: str,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_source = _required(source_item_id, "source_item_id")
    normalized_target_type = _required(target_type, "target_type")
    normalized_target_id = _required(target_id, "target_id")
    normalized_relation = _required(relation, "relation")
    now = utcnow()
    with db.session() as session:
        source = session.get(ResearchItemRecord, normalized_source)
        if source is None:
            raise KeyError("Source research item not found")
        existing = (
            session.query(ResearchLinkRecord)
            .filter(
                ResearchLinkRecord.source_item_id == normalized_source,
                ResearchLinkRecord.target_type == normalized_target_type,
                ResearchLinkRecord.target_id == normalized_target_id,
                ResearchLinkRecord.relation == normalized_relation,
            )
            .one_or_none()
        )
        if existing is not None:
            existing.link_metadata = dict(metadata or existing.link_metadata or {})
            existing.updated_at = now
            session.flush()
            return existing.to_dict()
        record = ResearchLinkRecord(
            id=str(uuid.uuid4()),
            source_item_id=normalized_source,
            target_type=normalized_target_type,
            target_id=normalized_target_id,
            relation=normalized_relation,
            link_metadata=dict(metadata or {}),
            created_at=now,
            updated_at=now,
        )
        session.add(record)
        session.flush()
        return record.to_dict()


def list_links(item_id: str, *, include_inbound: bool = True) -> list[dict[str, Any]]:
    normalized = _required(item_id, "item_id")
    with db.session() as session:
        outbound = session.query(ResearchLinkRecord).filter(ResearchLinkRecord.source_item_id == normalized)
        if not include_inbound:
            records = outbound.order_by(ResearchLinkRecord.created_at.desc()).all()
            return [record.to_dict() for record in records]
        inbound = session.query(ResearchLinkRecord).filter(
            ResearchLinkRecord.target_type == "research_item",
            ResearchLinkRecord.target_id == normalized,
        )
        records = list(outbound.all()) + list(inbound.all())
        records.sort(key=lambda record: record.created_at or datetime.min, reverse=True)
        return [record.to_dict() for record in records]


def _required(value: Any, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{label} is required")
    return normalized


def _optional(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None
