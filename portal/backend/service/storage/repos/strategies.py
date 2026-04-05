"""Storage repository module."""

from __future__ import annotations

from ._shared import *


def _normalize_variant_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return a storage-safe strategy variant payload."""

    strategy_id = str(payload.get("strategy_id") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not strategy_id:
        raise ValueError("strategy_id is required for strategy variants")
    if not name:
        raise ValueError("name is required for strategy variants")
    description = payload.get("description")
    return {
        "id": str(payload.get("id") or uuid.uuid4()),
        "strategy_id": strategy_id,
        "name": name,
        "description": str(description).strip() if description else None,
        "param_overrides": dict(_json_safe(payload.get("param_overrides") or {})),
        "atm_template_id": str(payload.get("atm_template_id") or "").strip() or None,
        "is_default": bool(payload.get("is_default", False)),
    }


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


def list_strategy_variants(strategy_id: str) -> List[Dict[str, Any]]:
    """Return all persisted variants for a strategy."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(
            select(StrategyVariantRecord).where(
                StrategyVariantRecord.strategy_id == strategy_id
            )
        ).scalars().all()
        ordered = sorted(
            (row.to_dict() for row in rows),
            key=lambda item: (
                0 if item.get("is_default") else 1,
                str(item.get("name") or ""),
                str(item.get("id") or ""),
            ),
        )
        return ordered


def get_strategy_variant(variant_id: str) -> Optional[Dict[str, Any]]:
    """Return a single strategy variant by id."""

    if not db.available or not variant_id:
        return None
    with db.session() as session:
        record = session.get(StrategyVariantRecord, variant_id)
        return record.to_dict() if record else None


def ensure_default_strategy_variant(strategy_id: str) -> Dict[str, Any]:
    """Ensure a strategy has one default saved variant."""

    if not db.available:
        now = _utcnow().isoformat() + "Z"
        return {
            "id": str(uuid.uuid4()),
            "strategy_id": strategy_id,
            "name": "default",
            "description": None,
            "param_overrides": {},
            "atm_template_id": None,
            "is_default": True,
            "created_at": now,
            "updated_at": now,
        }
    with db.session() as session:
        rows = session.execute(
            select(StrategyVariantRecord).where(
                StrategyVariantRecord.strategy_id == strategy_id
            )
        ).scalars().all()
        for row in rows:
            if row.is_default:
                return row.to_dict()

        default_row = next(
            (row for row in rows if str(row.name or "").strip().lower() == "default"),
            None,
        )
        now = _utcnow()
        if default_row is None:
            default_row = StrategyVariantRecord(
                id=str(uuid.uuid4()),
                strategy_id=strategy_id,
                name="default",
                description=None,
                param_overrides={},
                atm_template_id=None,
                is_default=True,
                created_at=now,
                updated_at=now,
            )
            session.add(default_row)
        else:
            default_row.is_default = True
            default_row.updated_at = now
        return default_row.to_dict()


def upsert_strategy_variant(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Create or update a persisted strategy variant."""

    normalized = _normalize_variant_payload(payload)
    if not db.available:
        return normalized
    with db.session() as session:
        record = session.get(StrategyVariantRecord, normalized["id"])
        now = _utcnow()
        if record is None:
            record = StrategyVariantRecord(
                id=normalized["id"],
                strategy_id=normalized["strategy_id"],
                created_at=now,
            )
            session.add(record)
        if normalized["is_default"]:
            siblings = session.execute(
                select(StrategyVariantRecord).where(
                    StrategyVariantRecord.strategy_id == normalized["strategy_id"]
                )
            ).scalars().all()
            for sibling in siblings:
                if sibling.id != normalized["id"] and sibling.is_default:
                    sibling.is_default = False
                    sibling.updated_at = now
        record.strategy_id = normalized["strategy_id"]
        record.name = normalized["name"]
        record.description = normalized["description"]
        record.param_overrides = normalized["param_overrides"]
        record.atm_template_id = normalized["atm_template_id"]
        record.is_default = normalized["is_default"]
        record.updated_at = now
        if record.created_at is None:
            record.created_at = now
        return record.to_dict()


def delete_strategy_variant(variant_id: str) -> None:
    """Delete a persisted non-default strategy variant."""

    if not db.available:
        return
    with db.session() as session:
        record = session.get(StrategyVariantRecord, variant_id)
        if record is None:
            return
        if record.is_default:
            raise ValueError("Default strategy variant cannot be deleted")
        session.delete(record)


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
            record.risk_config = payload.get("risk_config") or {}
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
            record.conditions = dict(_json_safe(payload.get("conditions") or {}))
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
