"""Storage repository module."""

from __future__ import annotations

from ._shared import *

def load_atm_templates() -> List[Dict[str, Any]]:
    """Return all persisted ATM templates."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(ATMTemplateRecord)).scalars().all()
        return [row.to_dict() for row in rows]


def get_atm_template(template_id: str) -> Optional[Dict[str, Any]]:
    """Return a single ATM template."""

    if not db.available:
        return None
    with db.session() as session:
        record = session.get(ATMTemplateRecord, template_id)
        return record.to_dict() if record else None


def upsert_atm_template(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Insert or update an ATM template record."""

    template_id = payload.get("id") or str(uuid.uuid4())
    if not db.available:
        return {**payload, "id": template_id}
    try:
        with db.session() as session:
            record = session.get(ATMTemplateRecord, template_id)
            now = _utcnow()
            if record is None:
                # If an ID wasn't provided, prefer an existing template with the same
                # name (templates are globally unique by name after normalization).
                name = payload.get("name") or payload.get("label") or template_id
                existing = session.execute(
                    select(ATMTemplateRecord).where(ATMTemplateRecord.name == name)
                ).scalars().first()
                if existing is not None:
                    record = existing
                else:
                    record = ATMTemplateRecord(id=template_id)
                    session.add(record)
            record.name = payload.get("name") or payload.get("label") or template_id
            record.template = dict(payload.get("template") or {})
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
            payload = record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning("atm_template_persist_failed | id=%s | error=%s", template_id, exc)
    return payload




