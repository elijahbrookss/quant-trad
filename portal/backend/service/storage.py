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
    ATMTemplateRecord,
    BotRecord,
    BotStrategyLink,
    BotTradeEventRecord,
    BotTradeRecord,
    IndicatorRecord,
    InstrumentRecord,
    StrategyATMTemplateLink,
    StrategyIndicatorLink,
    StrategyRecord,
    StrategyRuleRecord,
    SymbolPresetRecord,
    db,
)
from .atm import normalise_template


logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return a naive UTC timestamp."""

    return datetime.utcnow()


def _parse_optional_timestamp(value: Any) -> Optional[datetime]:
    """Best-effort parsing of ISO8601 strings into naive UTC datetimes."""

    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def load_indicators() -> List[Dict[str, Any]]:
    """Return all persisted indicator records."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(IndicatorRecord)).scalars().all()
        return [row.to_dict() for row in rows]


def get_indicator(indicator_id: str) -> Optional[Dict[str, Any]]:
    """Return a single indicator payload if it exists."""

    if not db.available:
        return None
    with db.session() as session:
        record = session.get(IndicatorRecord, indicator_id)
        return record.to_dict() if record else None


def load_instruments() -> List[Dict[str, Any]]:
    """Return all persisted instrument metadata rows."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(InstrumentRecord)).scalars().all()
        return [row.to_dict() for row in rows]


def get_instrument(instrument_id: str) -> Optional[Dict[str, Any]]:
    """Return a single instrument by identifier."""

    if not db.available:
        return None
    with db.session() as session:
        record = session.get(InstrumentRecord, instrument_id)
        return record.to_dict() if record else None


def find_instrument(datasource: Optional[str], exchange: Optional[str], symbol: str) -> Optional[Dict[str, Any]]:
    """Look up an instrument by datasource/exchange/symbol with fallbacks."""

    if not db.available:
        return None
    symbol_key = (symbol or "").upper()
    if not symbol_key:
        return None
    datasource_key = (datasource or "").lower() or None
    exchange_key = (exchange or "").lower() or None
    with db.session() as session:
        candidates = session.execute(
            select(InstrumentRecord).where(InstrumentRecord.symbol == symbol_key)
        ).scalars().all()
        if not candidates:
            return None
        def _score(record: InstrumentRecord) -> int:
            score = 0
            if datasource_key and (record.datasource or "").lower() == datasource_key:
                score += 2
            if exchange_key and (record.exchange or "").lower() == exchange_key:
                score += 1
            return score

        ranked = sorted(candidates, key=_score, reverse=True)
        return ranked[0].to_dict() if ranked else None


def upsert_instrument(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Insert or update an instrument record."""

    if not db.available:
        return meta
    instrument_id = meta.get("id") or str(uuid.uuid4())
    try:
        with db.session() as session:
            record = session.get(InstrumentRecord, instrument_id)
            now = _utcnow()
            if record is None:
                record = InstrumentRecord(id=instrument_id)
                session.add(record)
            record.datasource = meta.get("datasource")
            record.exchange = meta.get("exchange")
            record.symbol = (meta.get("symbol") or "").upper()
            record.instrument_type = meta.get("instrument_type")
            record.tick_size = meta.get("tick_size")
            record.tick_value = meta.get("tick_value")
            record.contract_size = meta.get("contract_size")
            record.min_order_size = meta.get("min_order_size")
            record.quote_currency = meta.get("quote_currency")
            record.maker_fee_rate = meta.get("maker_fee_rate")
            record.taker_fee_rate = meta.get("taker_fee_rate")
            record.extra_metadata = dict(meta.get("metadata") or {})
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
            meta = record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning("instrument_persist_failed | id=%s | error=%s", instrument_id, exc)
    return meta


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
                record = ATMTemplateRecord(id=template_id)
                session.add(record)
            record.name = payload.get("name") or payload.get("label") or template_id
            record.owner_id = payload.get("owner_id")
            record.template = dict(payload.get("template") or {})
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
            payload = record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning("atm_template_persist_failed | id=%s | error=%s", template_id, exc)
    return payload


def link_strategy_template(strategy_id: str, template_id: str) -> None:
    """Ensure a single template link exists for the strategy."""

    if not db.available:
        return
    try:
        with db.session() as session:
            link = session.execute(
                select(StrategyATMTemplateLink).where(StrategyATMTemplateLink.strategy_id == strategy_id)
            ).scalar_one_or_none()
            now = _utcnow()
            if link is None:
                link = StrategyATMTemplateLink(id=str(uuid.uuid4()), strategy_id=strategy_id, template_id=template_id)
                session.add(link)
            else:
                link.template_id = template_id
            link.updated_at = now
            if link.created_at is None:
                link.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("strategy_template_link_failed | strategy=%s | template=%s | error=%s", strategy_id, template_id, exc)


def delete_instrument(instrument_id: str) -> None:
    """Delete an instrument metadata row."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(InstrumentRecord, instrument_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("instrument_delete_failed | id=%s | error=%s", instrument_id, exc)


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
            record.run_type = payload.get("run_type") or record.run_type
            playback_speed = payload.get("playback_speed")
            if playback_speed is None:
                playback_speed = payload.get("fetch_seconds")
            if playback_speed is not None:
                try:
                    record.playback_speed = float(playback_speed)
                except (TypeError, ValueError):
                    record.playback_speed = (
                        record.playback_speed if record.playback_speed is not None else 10.0
                    )
            if record.playback_speed is None:
                record.playback_speed = 10.0
            if "risk" in payload:
                record.risk = dict(payload.get("risk") or {})
            record.backtest_start = _parse_optional_timestamp(payload.get("backtest_start")) or record.backtest_start
            record.backtest_end = _parse_optional_timestamp(payload.get("backtest_end")) or record.backtest_end
            record.status = payload.get("status") or record.status
            record.last_run_at = _parse_optional_timestamp(payload.get("last_run_at")) or record.last_run_at
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
        template_links = session.execute(select(StrategyATMTemplateLink)).scalars().all()
        templates = {row.id: row for row in session.execute(select(ATMTemplateRecord)).scalars().all()}
        link_map = {link.strategy_id: link.template_id for link in template_links}
        payload: List[Dict[str, Any]] = []
        for strategy in strategies:
            record = strategy.to_dict()
            template_id = link_map.get(strategy.id) or strategy.atm_template_id
            if template_id and template_id in templates:
                record["atm_template_id"] = template_id
                record["atm_template"] = normalise_template(templates[template_id].template)
                record.setdefault("atm_template_name", templates[template_id].name)
            else:
                record["atm_template"] = normalise_template(record.get("atm_template"))
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
            record.atm_template = dict(payload.get("atm_template") or {})
            record.atm_template_id = payload.get("atm_template_id")
            record.base_risk_per_trade = payload.get("base_risk_per_trade")
            record.global_risk_multiplier = payload.get("global_risk_multiplier")
            record.atr_period = payload.get("atr_period")
            record.atr_multiplier = payload.get("atr_multiplier")
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


def record_bot_trade(snapshot: Dict[str, Any]) -> None:
    """Insert or update a stored trade snapshot for dashboarding."""

    if not db.available:
        return
    trade_id = snapshot.get("trade_id") or snapshot.get("id")
    bot_id = snapshot.get("bot_id")
    if not trade_id or not bot_id:
        return
    try:
        with db.session() as session:
            record = session.get(BotTradeRecord, trade_id)
            now = _utcnow()
            if record is None:
                record = BotTradeRecord(id=str(trade_id), bot_id=str(bot_id), direction=snapshot.get("direction") or "long")
                record.created_at = now
                session.add(record)
            record.bot_id = str(bot_id)
            if snapshot.get("strategy_id"):
                record.strategy_id = str(snapshot.get("strategy_id"))
            if snapshot.get("symbol"):
                record.symbol = str(snapshot.get("symbol"))
            if snapshot.get("direction"):
                record.direction = str(snapshot.get("direction")).lower()
            status = snapshot.get("status")
            if status:
                record.status = str(status)
            contracts = _coerce_int(snapshot.get("contracts"))
            if contracts is not None:
                record.contracts = contracts
            entry_time = _parse_optional_timestamp(snapshot.get("entry_time"))
            if entry_time:
                record.entry_time = entry_time
            exit_time = _parse_optional_timestamp(snapshot.get("exit_time"))
            if exit_time:
                record.exit_time = exit_time
            entry_price = _coerce_float(snapshot.get("entry_price"))
            if entry_price is not None:
                record.entry_price = entry_price
            stop_price = _coerce_float(snapshot.get("stop_price"))
            if stop_price is not None:
                record.stop_price = stop_price
            gross = _coerce_float(snapshot.get("gross_pnl"))
            if gross is not None:
                record.gross_pnl = gross
            fees = _coerce_float(snapshot.get("fees_paid"))
            if fees is not None:
                record.fees_paid = fees
            net = _coerce_float(snapshot.get("net_pnl"))
            if net is not None:
                record.net_pnl = net
            quote = snapshot.get("quote_currency")
        if quote:
            record.quote_currency = str(quote).upper()
        if snapshot.get("atm_template") is not None:
            record.atm_template = dict(snapshot.get("atm_template") or {})
        if snapshot.get("metrics") is not None:
            record.metrics = dict(snapshot.get("metrics") or {})
        record.updated_at = now
        if record.created_at is None:
            record.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("bot_trade_persist_failed | trade=%s | error=%s", trade_id, exc)


def record_bot_trade_event(event: Dict[str, Any]) -> None:
    """Persist a stop/target event for a stored trade."""

    if not db.available:
        return
    trade_id = event.get("trade_id")
    bot_id = event.get("bot_id")
    if not trade_id or not bot_id:
        return
    event_id = event.get("id") or str(uuid.uuid4())
    event_time = _parse_optional_timestamp(event.get("event_time") or event.get("time"))
    try:
        with db.session() as session:
            record = BotTradeEventRecord(
                id=event_id,
                trade_id=str(trade_id),
                bot_id=str(bot_id),
                strategy_id=event.get("strategy_id"),
                symbol=event.get("symbol"),
                event_type=str(event.get("event_type") or event.get("type") or "event"),
                leg=event.get("leg"),
                contracts=_coerce_int(event.get("contracts")),
                price=_coerce_float(event.get("price")),
                ticks=_coerce_float(event.get("ticks")),
                pnl=_coerce_float(event.get("pnl")),
                quote_currency=(event.get("currency") or event.get("quote_currency")),
                event_time=event_time,
            )
            session.add(record)
    except SQLAlchemyError as exc:
        logger.warning("bot_trade_event_persist_failed | trade=%s | error=%s", trade_id, exc)
