"""Persistence helpers bridging services and the database layer."""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import delete, select
from sqlalchemy.exc import SQLAlchemyError

from ..db import (
    BotRecord,
    BotStrategyLink,
    IndicatorRecord,
    StrategyIndicatorLink,
    StrategyRecord,
    StrategyRuleRecord,
    SymbolPresetRecord,
    db,
)


logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return a naive UTC timestamp."""

    return datetime.utcnow()


def load_indicators() -> List[Dict[str, Any]]:
    """Return all persisted indicator records."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(IndicatorRecord)).scalars().all()
        return [row.to_dict() for row in rows]


def load_bots() -> List[Dict[str, Any]]:
    """Return all persisted bot configurations."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(BotRecord)).scalars().all()
        if not rows:
            return []
        bot_ids = [row.id for row in rows]
        link_map: Dict[str, List[str]] = defaultdict(list)
        links = session.execute(
            select(BotStrategyLink).where(BotStrategyLink.bot_id.in_(bot_ids))
        ).scalars().all()
        for link in links:
            link_map[link.bot_id].append(link.strategy_id)
        payload: List[Dict[str, Any]] = []
        for row in rows:
            record = row.to_dict()
            strategies = link_map.get(row.id, [])
            if not strategies and row.strategy_id:
                strategies = [row.strategy_id]
            record["strategy_ids"] = strategies
        payload.append(record)
        return payload


def upsert_indicator(meta: Dict[str, Any]) -> None:
    """Create or update an indicator record based on *meta*."""

    if not db.available:
        return
    try:
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
            record.params = dict(meta.get("params") or {})
            record.color = meta.get("color")
            record.datasource = meta.get("datasource")
            record.exchange = meta.get("exchange")
            record.enabled = bool(meta.get("enabled", True))
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("indicator_persist_failed | id=%s | error=%s", meta.get("id"), exc)


def _sync_bot_strategies(session, bot_id: str, strategy_ids: Iterable[str]) -> None:
    """Replace bot strategy links with *strategy_ids*."""

    session.execute(delete(BotStrategyLink).where(BotStrategyLink.bot_id == bot_id))
    for strategy_id in strategy_ids:
        link = BotStrategyLink(
            id=str(uuid.uuid4()),
            bot_id=bot_id,
            strategy_id=strategy_id,
        )
        session.add(link)


def upsert_bot(payload: Dict[str, Any]) -> None:
    """Persist a bot configuration row."""

    if not db.available:
        return
    bot_id = payload["id"]
    try:
        with db.session() as session:
            record = session.get(BotRecord, bot_id)
            now = _utcnow()
            if record is None:
                record = BotRecord(id=bot_id, name=payload.get("name") or bot_id)
                session.add(record)
            record.name = payload.get("name") or record.name
            strategy_ids: Optional[Iterable[str]] = payload.get("strategy_ids")
            if strategy_ids is not None:
                strategy_list = [sid for sid in strategy_ids]
                _sync_bot_strategies(session, bot_id, strategy_list)
                first_strategy = strategy_list[0] if strategy_list else None
            else:
                first_strategy = payload.get("strategy_id")
            record.strategy_id = first_strategy
            record.datasource = payload.get("datasource")
            record.exchange = payload.get("exchange")
            record.timeframe = payload.get("timeframe") or record.timeframe
            record.mode = payload.get("mode") or record.mode
            record.fetch_seconds = int(payload.get("fetch_seconds") or record.fetch_seconds or 5)
            record.risk = dict(payload.get("risk") or {})
            record.status = payload.get("status") or record.status
            record.last_run_at = payload.get("last_run_at") or record.last_run_at
            record.last_stats = dict(payload.get("last_stats") or record.last_stats or {})
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("bot_persist_failed | id=%s | error=%s", bot_id, exc)


def delete_bot(bot_id: str) -> None:
    """Remove a bot configuration permanently."""

    if not db.available:
        return
    try:
        with db.session() as session:
            session.execute(delete(BotStrategyLink).where(BotStrategyLink.bot_id == bot_id))
            record = session.get(BotRecord, bot_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("bot_delete_failed | id=%s | error=%s", bot_id, exc)


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


def load_strategies() -> List[Dict[str, Any]]:
    """Return strategies plus indicators and rules."""

    if not db.available:
        return []
    with db.session() as session:
        strategies = session.execute(select(StrategyRecord)).scalars().all()
        payload: List[Dict[str, Any]] = []
        for strategy in strategies:
            record = strategy.to_dict()
            links = session.execute(
                select(StrategyIndicatorLink).where(
                    StrategyIndicatorLink.strategy_id == strategy.id
                )
            ).scalars().all()
            rules = session.execute(
                select(StrategyRuleRecord).where(
                    StrategyRuleRecord.strategy_id == strategy.id
                )
            ).scalars().all()
            record["indicator_links"] = [link.to_dict() for link in links]
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
            record.symbols = list(payload.get("symbols") or [])
            record.timeframe = payload.get("timeframe") or record.timeframe
            record.datasource = payload.get("datasource")
            record.exchange = payload.get("exchange")
            record.indicator_ids = list(payload.get("indicator_ids") or [])
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
            record = session.get(StrategyRecord, strategy_id)
            if record:
                session.delete(record)
            session.query(StrategyIndicatorLink).filter(
                StrategyIndicatorLink.strategy_id == strategy_id
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
    snapshot: Dict[str, Any],
) -> None:
    """Persist the association between a strategy and indicator."""

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
                    indicator_snapshot=dict(snapshot or {}),
                    created_at=now,
                    updated_at=now,
                )
                session.add(link)
            else:
                link.indicator_snapshot = dict(snapshot or {})
                link.updated_at = now
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_indicator_persist_failed | strategy=%s | indicator=%s | error=%s",
            strategy_id,
            indicator_id,
            exc,
        )


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


def upsert_symbol_preset(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Store or update a symbol preset."""

    if not db.available:
        return None
    preset_id = payload.get("id")
    try:
        with db.session() as session:
            record = session.get(SymbolPresetRecord, preset_id) if preset_id else None
            now = _utcnow()
            if record is None:
                preset_id = preset_id or payload.get("id") or payload.get("label")
                record = SymbolPresetRecord(
                    id=str(preset_id or f"preset-{now.timestamp():.0f}"),
                    label=payload.get("label") or "Preset",
                    datasource=payload.get("datasource"),
                    exchange=payload.get("exchange"),
                    timeframe=payload.get("timeframe") or "15m",
                    symbol=payload.get("symbol") or "",
                    created_at=now,
                    updated_at=now,
                )
                session.add(record)
            record.label = payload.get("label") or record.label
            record.datasource = payload.get("datasource")
            record.exchange = payload.get("exchange")
            record.timeframe = payload.get("timeframe") or record.timeframe
            record.symbol = payload.get("symbol") or record.symbol
            record.updated_at = now
            session.flush()
            return record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning("symbol_preset_persist_failed | id=%s | error=%s", preset_id, exc)
        return None


def list_symbol_presets() -> List[Dict[str, Any]]:
    """Return all saved symbol presets."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(SymbolPresetRecord)).scalars().all()
        return [row.to_dict() for row in rows]


def delete_symbol_preset(preset_id: str) -> None:
    """Delete a stored symbol preset."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(SymbolPresetRecord, preset_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("symbol_preset_delete_failed | id=%s | error=%s", preset_id, exc)
