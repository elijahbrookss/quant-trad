"""Storage repository module."""

from __future__ import annotations

from ._shared import *

def load_strategies() -> List[Dict[str, Any]]:
    """Return strategies plus indicators and rules."""

    if not db.available:
        return []
    with db.session() as session:
        # Use a raw select to avoid depending on ORM model columns that may have been removed
        rows = session.execute(text("SELECT * FROM portal_strategies")).mappings().all()
        templates = {row.id: row for row in session.execute(select(ATMTemplateRecord)).scalars().all()}
        payload: List[Dict[str, Any]] = []
        for row in rows:
            # Start from the raw DB mapping
            record: Dict[str, Any] = dict(row)
            template_id = record.get("atm_template_id")
            if template_id and template_id in templates:
                record["atm_template_id"] = template_id
                record["atm_template"] = normalise_template(templates[template_id].template)
                record.setdefault("atm_template_name", templates[template_id].name)
            else:
                record["atm_template"] = None

            strategy_id = record.get("id")
            links = session.execute(
                select(StrategyIndicatorLink).where(
                    StrategyIndicatorLink.strategy_id == strategy_id
                )
            ).scalars().all()
            inst_links = session.execute(
                select(StrategyInstrumentLink).where(
                    StrategyInstrumentLink.strategy_id == strategy_id
                )
            ).scalars().all()
            rules = session.execute(
                select(StrategyRuleRecord).where(
                    StrategyRuleRecord.strategy_id == strategy_id
                )
            ).scalars().all()
            record["indicator_links"] = [link.to_dict() for link in links]
            record["instrument_links"] = [link.to_dict() for link in inst_links]
            record["rules_raw"] = [rule.to_dict() for rule in rules]
            payload.append(record)
        return payload


def upsert_strategy(payload: Dict[str, Any]) -> None:
    """Persist a strategy definition along with indicator ordering."""

    if not db.available:
        return
    strategy_id = payload["id"]
    try:
        with db.session() as session:
            record = session.get(StrategyRecord, strategy_id)
            now = _utcnow()
            if record is None:
                record = StrategyRecord(
                    id=strategy_id,
                    name=payload.get("name") or strategy_id,
                    timeframe=payload.get("timeframe") or "15m",
                )
                session.add(record)
            record.name = payload.get("name") or record.name
            record.description = payload.get("description")
            record.timeframe = payload.get("timeframe") or record.timeframe
            record.datasource = payload.get("datasource")
            record.exchange = payload.get("exchange")
            # indicator attachments are persisted in portal_strategy_indicators
            record.atm_template_id = payload.get("atm_template_id")
            record.base_risk_per_trade = payload.get("base_risk_per_trade")
            record.global_risk_multiplier = payload.get("global_risk_multiplier")
            record.risk_overrides = payload.get("risk_overrides") or {}
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("strategy_persist_failed | id=%s | error=%s", strategy_id, exc)


def delete_strategy(strategy_id: str) -> None:
    """Delete a strategy and its dependent rows."""

    if not db.available:
        return
    try:
        with db.session() as session:
            dependent_bots = session.execute(
                select(BotRecord.id, BotRecord.name).where(BotRecord.strategy_id == strategy_id)
            ).all()
            if dependent_bots:
                bot_refs = [
                    f"{str(bot_id)}({str(bot_name or bot_id)})"
                    for bot_id, bot_name in dependent_bots
                ]
                joined = ", ".join(bot_refs)
                logger.error(
                    "strategy_delete_blocked_by_bots | strategy_id=%s | bot_ids=%s",
                    strategy_id,
                    ",".join(str(bot_id) for bot_id, _bot_name in dependent_bots),
                )
                raise ValueError(
                    f"Cannot delete strategy {strategy_id}: dependent bots exist. "
                    f"Delete or reassign these bots first: {joined}"
                )
            record = session.get(StrategyRecord, strategy_id)
            if record:
                session.delete(record)
            session.query(StrategyIndicatorLink).filter(
                StrategyIndicatorLink.strategy_id == strategy_id
            ).delete(synchronize_session=False)
            session.query(StrategyInstrumentLink).filter(
                StrategyInstrumentLink.strategy_id == strategy_id
            ).delete(synchronize_session=False)
            session.query(StrategyRuleRecord).filter(
                StrategyRuleRecord.strategy_id == strategy_id
            ).delete(synchronize_session=False)
    except SQLAlchemyError as exc:
        logger.warning("strategy_delete_failed | id=%s | error=%s", strategy_id, exc)


def upsert_strategy_indicator(
    *,
    strategy_id: str,
    indicator_id: str,
    # REMOVED: snapshot parameter - no longer storing snapshots
) -> None:
    """Persist the association between a strategy and indicator (no snapshot storage)."""

    if not db.available:
        return
    try:
        with db.session() as session:
            link = session.execute(
                select(StrategyIndicatorLink).where(
                    StrategyIndicatorLink.strategy_id == strategy_id,
                    StrategyIndicatorLink.indicator_id == indicator_id,
                )
            ).scalars().first()
            now = _utcnow()
            if link is None:
                link = StrategyIndicatorLink(
                    id=f"{strategy_id}:{indicator_id}",
                    strategy_id=strategy_id,
                    indicator_id=indicator_id,
                    # REMOVED: indicator_snapshot assignment - no longer storing snapshots
                    created_at=now,
                    updated_at=now,
                )
                session.add(link)
            else:
                # REMOVED: indicator_snapshot update - no longer storing snapshots
                link.updated_at = now
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_indicator_persist_failed | strategy=%s | indicator=%s | error=%s",
            strategy_id,
            indicator_id,
            exc,
        )


def upsert_strategy_instrument(*, strategy_id: str, instrument_id: str, snapshot: Dict[str, Any]) -> None:
    """Persist association between a strategy and an instrument."""

    if not db.available:
        return
    try:
        with db.session() as session:
            link = session.execute(
                select(StrategyInstrumentLink).where(
                    StrategyInstrumentLink.strategy_id == strategy_id,
                    StrategyInstrumentLink.instrument_id == instrument_id,
                )
            ).scalars().first()
            now = _utcnow()
            if link is None:
                # Use a deterministic uuid5 for the link id so the composite key
                # (strategy_id:instrument_id) cannot exceed the column length.
                link_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{strategy_id}:{instrument_id}"))
                link = StrategyInstrumentLink(
                    id=link_id,
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    instrument_snapshot=dict(snapshot or {}),
                    created_at=now,
                    updated_at=now,
                )
                session.add(link)
            else:
                link.instrument_snapshot = dict(snapshot or {})
                link.updated_at = now
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_instrument_persist_failed | strategy=%s | instrument=%s | error=%s",
            strategy_id,
            instrument_id,
            exc,
        )


def delete_strategy_instrument(strategy_id: str, instrument_id: str) -> None:
    """Remove a strategy <-> instrument link."""

    if not db.available:
        return
    try:
        with db.session() as session:
            session.query(StrategyInstrumentLink).filter(
                StrategyInstrumentLink.strategy_id == strategy_id,
                StrategyInstrumentLink.instrument_id == instrument_id,
            ).delete(synchronize_session=False)
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_instrument_delete_failed | strategy=%s | instrument=%s | error=%s",
            strategy_id,
            instrument_id,
            exc,
        )


def delete_orphan_strategy_instrument_links(strategy_id: str) -> int:
    """Remove strategy-instrument links where the instrument no longer exists."""

    if not db.available:
        return 0
    try:
        with db.session() as session:
            orphan_links = session.execute(
                select(StrategyInstrumentLink.instrument_id)
                .outerjoin(
                    InstrumentRecord,
                    StrategyInstrumentLink.instrument_id == InstrumentRecord.id,
                )
                .where(
                    StrategyInstrumentLink.strategy_id == strategy_id,
                    InstrumentRecord.id.is_(None),
                )
            ).scalars().all()
            if not orphan_links:
                return 0
            session.query(StrategyInstrumentLink).filter(
                StrategyInstrumentLink.strategy_id == strategy_id,
                StrategyInstrumentLink.instrument_id.in_(orphan_links),
            ).delete(synchronize_session=False)
            return len(orphan_links)
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_instrument_orphan_cleanup_failed | strategy=%s | error=%s",
            strategy_id,
            exc,
        )
        return 0


def delete_strategy_indicator(strategy_id: str, indicator_id: str) -> None:
    """Remove a strategy/indicator link."""

    if not db.available:
        return
    try:
        with db.session() as session:
            session.query(StrategyIndicatorLink).filter(
                StrategyIndicatorLink.strategy_id == strategy_id,
                StrategyIndicatorLink.indicator_id == indicator_id,
            ).delete(synchronize_session=False)
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_indicator_delete_failed | strategy=%s | indicator=%s | error=%s",
            strategy_id,
            indicator_id,
            exc,
        )


def upsert_strategy_rule(payload: Dict[str, Any]) -> None:
    """Persist a strategy rule definition."""

    if not db.available:
        return
    rule_id = payload["id"]
    try:
        with db.session() as session:
            record = session.get(StrategyRuleRecord, rule_id)
            now = _utcnow()
            if record is None:
                record = StrategyRuleRecord(
                    id=rule_id,
                    strategy_id=payload["strategy_id"],
                    name=payload.get("name") or rule_id,
                    action=payload.get("action") or "buy",
                )
                session.add(record)
            record.strategy_id = payload.get("strategy_id") or record.strategy_id
            record.name = payload.get("name") or record.name
            record.action = payload.get("action") or record.action
            record.match = payload.get("match") or record.match
            record.description = payload.get("description")
            record.enabled = bool(payload.get("enabled", True))
            record.conditions = list(payload.get("conditions") or [])
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("strategy_rule_persist_failed | id=%s | error=%s", rule_id, exc)


def delete_strategy_rule(rule_id: str) -> None:
    """Remove a persisted strategy rule."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(StrategyRuleRecord, rule_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("strategy_rule_delete_failed | id=%s | error=%s", rule_id, exc)




