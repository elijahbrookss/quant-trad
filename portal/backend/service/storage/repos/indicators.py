"""Storage repository module."""

from __future__ import annotations

from ...indicators.persistence_payload import merge_indicator_payload, split_indicator_payload
from ._shared import *


def _record_to_indicator_payload(record: IndicatorRecord) -> Dict[str, Any]:
    params, dependencies = split_indicator_payload(record.params)
    return {
        "id": record.id,
        "name": record.name,
        "type": record.type,
        "params": params,
        "dependencies": dependencies,
        "color": record.color,
        "enabled": bool(record.enabled),
        "created_at": (record.created_at or _utcnow()).isoformat() + "Z",
        "updated_at": (record.updated_at or _utcnow()).isoformat() + "Z",
    }

def load_indicators() -> List[Dict[str, Any]]:
    """Return all persisted indicator records."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(IndicatorRecord)).scalars().all()
        return [_record_to_indicator_payload(row) for row in rows]


def get_indicator(indicator_id: str) -> Optional[Dict[str, Any]]:
    """Return a single indicator payload if it exists."""

    if not db.available:
        return None
    with db.session() as session:
        record = session.get(IndicatorRecord, indicator_id)
        return _record_to_indicator_payload(record) if record else None




def upsert_indicator(meta: Dict[str, Any]) -> None:
    """Create or update an indicator record based on *meta*."""

    if not db.available:
        return
    try:
        logger.info(
            "event=upsert_indicator_called indicator_id=%s meta_params_keys=%s meta_params=%s",
            meta.get("id"),
            list((meta.get("params") or {}).keys()),
            meta.get("params"),
        )
        with db.session() as session:
            record = session.get(IndicatorRecord, meta["id"])
            now = _utcnow()
            if record is None:
                record = IndicatorRecord(
                    id=meta["id"],
                    name=meta.get("name") or meta["type"],
                    type=meta["type"],
                )
                session.add(record)
            record.name = meta.get("name") or record.name
            record.type = meta.get("type") or record.type
            params_to_store = merge_indicator_payload(
                meta.get("params"),
                meta.get("dependencies"),
            )
            logger.info(
                "event=upsert_indicator_params_assignment indicator_id=%s params_keys=%s params=%s",
                meta.get("id"),
                list(params_to_store.keys()),
                params_to_store,
            )
            record.params = params_to_store
            record.color = meta.get("color")
            # datasource/exchange removed from persisted indicators
            record.enabled = bool(meta.get("enabled", True))
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("indicator_persist_failed | id=%s | error=%s", meta.get("id"), exc)




def delete_indicator(indicator_id: str) -> None:
    """Remove an indicator from persistence."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(IndicatorRecord, indicator_id)
            if record:
                session.delete(record)
            links = session.execute(
                select(StrategyIndicatorLink).where(
                    StrategyIndicatorLink.indicator_id == indicator_id
                )
            ).scalars()
            for link in links:
                session.delete(link)
    except SQLAlchemyError as exc:
        logger.warning("indicator_delete_failed | id=%s | error=%s", indicator_id, exc)




def strategies_for_indicator(indicator_id: str) -> List[Dict[str, Any]]:
    """Return strategies referencing *indicator_id*."""

    if not db.available:
        return []
    with db.session() as session:
        links = session.execute(
            select(StrategyIndicatorLink).where(StrategyIndicatorLink.indicator_id == indicator_id)
        ).scalars().all()
        strategy_ids = {link.strategy_id for link in links}
        if not strategy_ids:
            return []
        strategies = session.execute(
            select(StrategyRecord).where(StrategyRecord.id.in_(strategy_ids))
        ).scalars().all()
        rules = session.execute(
            select(StrategyRuleRecord).where(StrategyRuleRecord.strategy_id.in_(strategy_ids))
        ).scalars().all()
        rules_by_strategy: Dict[str, List[Dict[str, Any]]] = {}
        for rule in rules:
            rules_by_strategy.setdefault(rule.strategy_id, []).append(rule.to_dict())
        return [
            {
                **strategy.to_dict(),
                "rules": rules_by_strategy.get(strategy.id, []),
            }
            for strategy in strategies
        ]


