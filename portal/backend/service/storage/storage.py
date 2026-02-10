"""Persistence helpers bridging services and the database layer."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import delete, select, text, func
from sqlalchemy.exc import SQLAlchemyError

from ...db import (
    ATMTemplateRecord,
    BotRecord,
    BotRunRecord,
    BotRunStepRecord,
    BotTradeEventRecord,
    BotTradeRecord,
    IndicatorRecord,
    InstrumentRecord,
    RuleFilterRecord,
    StrategyIndicatorLink,
    StrategyFilterRecord,
    StrategyInstrumentLink,
    StrategyRecord,
    StrategyRuleRecord,
    SymbolPresetRecord,
    db,
)
from ..risk.atm import normalise_template


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


def list_strategy_instrument_symbols(strategy_id: str) -> List[str]:
    """Return symbols for all instruments attached to *strategy_id*.

    This queries the instrument table directly using the strategy->instrument links
    so we always derive authoritative symbol values from the persisted instrument rows.
    """

    if not db.available:
        return []
    if not strategy_id:
        return []
    with db.session() as session:
        # Join StrategyInstrumentLink -> InstrumentRecord and return symbol list
        rows = (
            session.execute(
                select(InstrumentRecord.symbol)
                .join(StrategyInstrumentLink, StrategyInstrumentLink.instrument_id == InstrumentRecord.id)
                .where(StrategyInstrumentLink.strategy_id == strategy_id)
            )
            .scalars()
            .all()
        )
        # Normalise and dedupe while preserving order
        seen: set[str] = set()
        symbols: List[str] = []
        for s in rows:
            if s is None:
                continue
            key = str(s).strip()
            if not key:
                continue
            if key in seen:
                continue
            seen.add(key)
            symbols.append(key)
    return symbols


def list_strategy_instrument_links(strategy_id: str) -> List[Dict[str, Any]]:
    """Return instrument link rows with resolved symbols for a strategy."""

    if not db.available:
        return []
    try:
        with db.session() as session:
            rows = session.execute(
                select(
                    StrategyInstrumentLink.instrument_id,
                    InstrumentRecord.symbol,
                ).join(
                    InstrumentRecord,
                    StrategyInstrumentLink.instrument_id == InstrumentRecord.id,
                ).where(
                    StrategyInstrumentLink.strategy_id == strategy_id,
                )
            ).all()
            return [
                {"instrument_id": instrument_id, "symbol": symbol}
                for instrument_id, symbol in rows
            ]
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_instrument_list_failed | strategy=%s | error=%s",
            strategy_id,
            exc,
        )
        return []


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
        # Require exact symbol match. If datasource and/or exchange are provided,
        # require those fields to match as well so we do not conflate distinct venue symbols.
        query = select(InstrumentRecord).where(InstrumentRecord.symbol == symbol_key)
        if datasource_key:
            query = query.where((InstrumentRecord.datasource or '').ilike(datasource_key))
        if exchange_key:
            query = query.where((InstrumentRecord.exchange or '').ilike(exchange_key))

        record = session.execute(query).scalars().first()
        return record.to_dict() if record else None


def upsert_instrument(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Insert or update an instrument record."""

    if not db.available:
        return meta
    # Ignore any caller-provided `id`. Always dedupe by the canonical unique key
    # (datasource, exchange, symbol). This prevents clients from bypassing the
    # uniqueness constraint by inventing IDs.
    symbol = (meta.get("symbol") or "").upper()
    datasource = meta.get("datasource")
    exchange = meta.get("exchange")

    if not symbol:
        raise ValueError("Instrument symbol is required")

    try:
        with db.session() as session:
            now = _utcnow()

            # Look for existing instrument by composite key regardless of any id.
            existing = None
            if datasource and exchange:
                existing = session.execute(
                    select(InstrumentRecord).where(
                        InstrumentRecord.symbol == symbol,
                        InstrumentRecord.datasource == datasource,
                        InstrumentRecord.exchange == exchange,
                    )
                ).scalars().first()

            if existing is not None:
                record = existing
            else:
                # Creating a new instrument requires datasource and exchange.
                if not datasource or not exchange:
                    raise ValueError("Instrument creation requires 'datasource' and 'exchange'")
                instrument_id = str(uuid.uuid4())
                record = InstrumentRecord(id=instrument_id)
                session.add(record)

            # Update fields on the found-or-created record
            record.datasource = datasource
            record.exchange = exchange
            record.symbol = symbol
            record.instrument_type = meta.get("instrument_type")
            # Instrument field values now live in the metadata JSON payload.
            # Merge metadata instead of replacing to preserve existing values
            if "metadata" in meta:
                existing_metadata = dict(record.extra_metadata or {})
                existing_metadata.update(meta.get("metadata") or {})
                record.extra_metadata = existing_metadata
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
            meta = record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning("instrument_persist_failed | id=%s | error=%s", instrument_id, exc)
        raise
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
        payload: List[Dict[str, Any]] = []
        for row in rows:
            record = row.to_dict()
            record["strategy_ids"] = [row.strategy_id] if row.strategy_id else []
            payload.append(record)
        return payload


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
            params_to_store = dict(meta.get("params") or {})
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
            first_strategy = None
            if strategy_ids:
                for strategy_id in strategy_ids:
                    if strategy_id:
                        candidate = str(strategy_id).strip()
                        if candidate:
                            first_strategy = candidate
                            break
            if not first_strategy:
                fallback = payload.get("strategy_id")
                if fallback:
                    candidate = str(fallback).strip()
                    if candidate:
                        first_strategy = candidate
            record.strategy_id = first_strategy
            # datasource/exchange/timeframe are no longer stored on bots; derive from strategy at runtime
            record.mode = payload.get("mode") or record.mode
            record.run_type = payload.get("run_type") or record.run_type
            record.playback_speed = 0.0
            if "risk" in payload:
                record.risk = dict(payload.get("risk") or {})
            if "wallet_config" in payload:
                record.wallet_config = dict(payload.get("wallet_config") or {})
            record.backtest_start = _parse_optional_timestamp(payload.get("backtest_start")) or record.backtest_start
            record.backtest_end = _parse_optional_timestamp(payload.get("backtest_end")) or record.backtest_end
            record.status = payload.get("status") or record.status
            record.last_run_at = _parse_optional_timestamp(payload.get("last_run_at")) or record.last_run_at
            record.last_stats = dict(payload.get("last_stats") or record.last_stats or {})
            if "last_run_artifact" in payload:
                record.last_run_artifact = dict(payload.get("last_run_artifact") or {})
            if "runner_id" in payload:
                record.runner_id = payload.get("runner_id")
            if "heartbeat_at" in payload:
                record.heartbeat_at = _parse_optional_timestamp(payload.get("heartbeat_at"))
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("bot_persist_failed | id=%s | error=%s", bot_id, exc)


def update_bot_run_artifact(bot_id: str, artifact: Dict[str, Any]) -> None:
    """Persist last run artifact on the bot record (fail loud)."""

    if not db.available:
        raise RuntimeError("Database not available for run artifact persistence")
    with db.session() as session:
        record = session.get(BotRecord, bot_id)
        if record is None:
            raise KeyError(f"Bot {bot_id} was not found")
        record.last_run_artifact = dict(artifact or {})
        record.updated_at = _utcnow()


def update_bot_heartbeat(bot_id: str, runner_id: str) -> None:
    """Update heartbeat timestamp for a running bot (BotWatchdog)."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(BotRecord, bot_id)
            if record is None:
                return
            now = _utcnow()
            record.heartbeat_at = now
            record.runner_id = runner_id
    except SQLAlchemyError as exc:
        logger.warning("bot_heartbeat_failed | id=%s | error=%s", bot_id, exc)


def mark_bot_crashed(bot_id: str, reason: str = "orphaned") -> bool:
    """Mark a bot as crashed and clear its runner ownership (BotWatchdog).

    Returns True if the bot was updated, False otherwise.
    """

    if not db.available:
        return False
    try:
        with db.session() as session:
            record = session.get(BotRecord, bot_id)
            if record is None:
                return False
            previous_runner = record.runner_id
            record.status = "crashed"
            record.runner_id = None
            record.heartbeat_at = None
            record.updated_at = _utcnow()
            logger.info(
                "bot_marked_crashed | id=%s | reason=%s | previous_runner=%s",
                bot_id,
                reason,
                previous_runner,
            )
            return True
    except SQLAlchemyError as exc:
        logger.warning("bot_mark_crashed_failed | id=%s | error=%s", bot_id, exc)
        return False


def find_orphaned_bots(
    stale_threshold_seconds: float,
    runner_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Find bots that appear to be orphaned (BotWatchdog).

    Args:
        stale_threshold_seconds: Consider a bot orphaned if heartbeat is older than this
        runner_id: If provided, only check bots owned by this runner

    Returns:
        List of bot dicts that are orphaned (running/paused but stale heartbeat)
    """

    if not db.available:
        return []
    try:
        from datetime import timedelta
        with db.session() as session:
            cutoff = _utcnow() - timedelta(seconds=stale_threshold_seconds)
            query = select(BotRecord).where(
                BotRecord.status.in_(["running", "paused", "starting"])
            )
            if runner_id:
                query = query.where(BotRecord.runner_id == runner_id)
            rows = session.execute(query).scalars().all()
            orphaned = []
            for row in rows:
                # Consider orphaned if:
                # 1. No heartbeat ever recorded, OR
                # 2. Heartbeat is older than threshold
                if row.heartbeat_at is None or row.heartbeat_at < cutoff:
                    record = row.to_dict()
                    record["strategy_ids"] = [row.strategy_id] if row.strategy_id else []
                    orphaned.append(record)
            return orphaned
    except SQLAlchemyError as exc:
        logger.warning("find_orphaned_bots_failed | error=%s", exc)
        return []


def clear_bot_runner(bot_id: str) -> None:
    """Clear runner ownership when a bot stops normally (BotWatchdog)."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(BotRecord, bot_id)
            if record is None:
                return
            record.runner_id = None
            record.heartbeat_at = None
    except SQLAlchemyError as exc:
        logger.warning("clear_bot_runner_failed | id=%s | error=%s", bot_id, exc)


def upsert_bot_run(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Insert or update a bot run snapshot."""

    if not db.available:
        raise RuntimeError("Database not available for run persistence")
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("run_id is required for bot run persistence")
    with db.session() as session:
        record = session.get(BotRunRecord, run_id)
        now = _utcnow()
        if record is None:
            record = BotRunRecord(run_id=run_id)
            record.created_at = now
            session.add(record)
        record.bot_id = payload.get("bot_id") or record.bot_id
        record.bot_name = payload.get("bot_name") or record.bot_name
        record.strategy_id = payload.get("strategy_id") or record.strategy_id
        record.strategy_name = payload.get("strategy_name") or record.strategy_name
        record.run_type = payload.get("run_type") or record.run_type or "backtest"
        record.status = payload.get("status") or record.status or "completed"
        record.timeframe = payload.get("timeframe") or record.timeframe
        record.datasource = payload.get("datasource") or record.datasource
        record.exchange = payload.get("exchange") or record.exchange
        symbols = payload.get("symbols")
        if symbols is not None:
            record.symbols = list(symbols)
        record.backtest_start = _parse_optional_timestamp(payload.get("backtest_start")) or record.backtest_start
        record.backtest_end = _parse_optional_timestamp(payload.get("backtest_end")) or record.backtest_end
        record.started_at = _parse_optional_timestamp(payload.get("started_at")) or record.started_at
        record.ended_at = _parse_optional_timestamp(payload.get("ended_at")) or record.ended_at
        if payload.get("summary") is not None:
            record.summary = dict(payload.get("summary") or {})
        if payload.get("config_snapshot") is not None:
            record.config_snapshot = dict(payload.get("config_snapshot") or {})
        if payload.get("decision_ledger") is not None:
            record.decision_ledger = list(payload.get("decision_ledger") or [])
        record.updated_at = now
        if record.created_at is None:
            record.created_at = now
        return record.to_dict()


def get_bot_run(run_id: str) -> Optional[Dict[str, Any]]:
    """Return a persisted bot run snapshot."""

    if not db.available:
        return None
    if not run_id:
        return None
    with db.session() as session:
        record = session.get(BotRunRecord, run_id)
        return record.to_dict() if record else None


def get_bot(bot_id: str) -> Optional[Dict[str, Any]]:
    """Return a persisted bot configuration."""

    if not db.available:
        return None
    if not bot_id:
        return None
    with db.session() as session:
        record = session.get(BotRecord, bot_id)
        return record.to_dict() if record else None


def list_bot_runs(
    *,
    run_type: Optional[str] = None,
    status: Optional[str] = None,
    bot_id: Optional[str] = None,
    timeframe: Optional[str] = None,
    started_after: Optional[str] = None,
    started_before: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return persisted bot run snapshots filtered by metadata."""

    if not db.available:
        return []
    query = select(BotRunRecord)
    if run_type:
        query = query.where(BotRunRecord.run_type == run_type)
    if status:
        query = query.where(BotRunRecord.status == status)
    if bot_id:
        query = query.where(BotRunRecord.bot_id == bot_id)
    if timeframe:
        query = query.where(BotRunRecord.timeframe == timeframe)
    start_dt = _parse_optional_timestamp(started_after)
    if start_dt:
        query = query.where(BotRunRecord.ended_at >= start_dt)
    end_dt = _parse_optional_timestamp(started_before)
    if end_dt:
        query = query.where(BotRunRecord.ended_at <= end_dt)
    try:
        with db.session() as session:
            rows = session.execute(query).scalars().all()
            return [row.to_dict() for row in rows]
    except SQLAlchemyError as exc:
        logger.error(
            "bot_run_list_failed | run_type=%s | status=%s | bot_id=%s | timeframe=%s | error=%s",
            run_type,
            status,
            bot_id,
            timeframe,
            exc,
        )
        raise


def list_bot_trades_for_run(run_id: str) -> List[Dict[str, Any]]:
    """Return trades associated with a run ID."""

    if not db.available:
        return []
    if not run_id:
        return []
    with db.session() as session:
        rows = session.execute(
            select(BotTradeRecord).where(BotTradeRecord.run_id == run_id)
        ).scalars().all()
        return [row.to_dict() for row in rows]


def list_bot_trade_events_for_trades(trade_ids: Iterable[str]) -> List[Dict[str, Any]]:
    """Return trade events for the provided trade IDs."""

    if not db.available:
        return []
    trade_ids = [trade_id for trade_id in trade_ids if trade_id]
    if not trade_ids:
        return []
    with db.session() as session:
        rows = session.execute(
            select(BotTradeEventRecord).where(BotTradeEventRecord.trade_id.in_(trade_ids))
        ).scalars().all()
        return [row.to_dict() for row in rows]


def delete_bot(bot_id: str) -> None:
    """Remove a bot configuration permanently."""

    if not db.available:
        return
    try:
        with db.session() as session:
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
            strategy_filters = session.execute(
                select(StrategyFilterRecord).where(
                    StrategyFilterRecord.strategy_id == strategy_id
                )
            ).scalars().all()
            rule_ids = [rule.id for rule in rules]
            rule_filters: List[RuleFilterRecord] = []
            if rule_ids:
                rule_filters = session.execute(
                    select(RuleFilterRecord).where(RuleFilterRecord.rule_id.in_(rule_ids))
                ).scalars().all()
            record["indicator_links"] = [link.to_dict() for link in links]
            record["instrument_links"] = [link.to_dict() for link in inst_links]
            record["rules_raw"] = [rule.to_dict() for rule in rules]
            record["strategy_filters_raw"] = [flt.to_dict() for flt in strategy_filters]
            record["rule_filters_raw"] = [flt.to_dict() for flt in rule_filters]
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
            session.query(StrategyFilterRecord).filter(
                StrategyFilterRecord.strategy_id == strategy_id
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


def list_strategy_filters(strategy_id: str) -> List[Dict[str, Any]]:
    """Return global filters for a strategy."""

    if not db.available or not strategy_id:
        return []
    with db.session() as session:
        filters = session.execute(
            select(StrategyFilterRecord).where(StrategyFilterRecord.strategy_id == strategy_id)
        ).scalars().all()
        return [flt.to_dict() for flt in filters]


def list_rule_filters(rule_id: str) -> List[Dict[str, Any]]:
    """Return filters attached to a rule."""

    if not db.available or not rule_id:
        return []
    with db.session() as session:
        filters = session.execute(
            select(RuleFilterRecord).where(RuleFilterRecord.rule_id == rule_id)
        ).scalars().all()
        return [flt.to_dict() for flt in filters]


def upsert_strategy_filter(payload: Dict[str, Any]) -> None:
    """Persist a strategy-wide filter definition."""

    if not db.available:
        return
    filter_id = payload.get("id")
    strategy_id = payload.get("strategy_id")
    if not filter_id or not strategy_id:
        raise ValueError("Strategy filter id and strategy_id are required")
    try:
        with db.session() as session:
            record = session.get(StrategyFilterRecord, filter_id)
            now = _utcnow()
            if record is None:
                record = StrategyFilterRecord(
                    id=str(filter_id),
                    strategy_id=str(strategy_id),
                    scope=str(payload.get("scope") or "GLOBAL"),
                    created_at=now,
                    updated_at=now,
                )
                session.add(record)
            record.name = payload.get("name") or record.name
            record.description = payload.get("description")
            record.enabled = bool(payload.get("enabled", True))
            record.scope = str(payload.get("scope") or record.scope or "GLOBAL")
            record.dsl = dict(payload.get("dsl") or {})
            record.updated_at = now
    except SQLAlchemyError as exc:
        logger.warning(
            "strategy_filter_persist_failed | strategy=%s | filter=%s | error=%s",
            strategy_id,
            filter_id,
            exc,
        )


def upsert_rule_filter(payload: Dict[str, Any]) -> None:
    """Persist a rule-scoped filter definition."""

    if not db.available:
        return
    filter_id = payload.get("id")
    rule_id = payload.get("rule_id")
    if not filter_id or not rule_id:
        raise ValueError("Rule filter id and rule_id are required")
    try:
        with db.session() as session:
            record = session.get(RuleFilterRecord, filter_id)
            now = _utcnow()
            if record is None:
                record = RuleFilterRecord(
                    id=str(filter_id),
                    rule_id=str(rule_id),
                    scope=str(payload.get("scope") or "RULE"),
                    created_at=now,
                    updated_at=now,
                )
                session.add(record)
            record.name = payload.get("name") or record.name
            record.description = payload.get("description")
            record.enabled = bool(payload.get("enabled", True))
            record.scope = str(payload.get("scope") or record.scope or "RULE")
            record.dsl = dict(payload.get("dsl") or {})
            record.updated_at = now
    except SQLAlchemyError as exc:
        logger.warning(
            "rule_filter_persist_failed | rule=%s | filter=%s | error=%s",
            rule_id,
            filter_id,
            exc,
        )


def delete_strategy_filter(filter_id: str) -> None:
    """Delete a strategy-wide filter."""

    if not db.available or not filter_id:
        return
    try:
        with db.session() as session:
            record = session.get(StrategyFilterRecord, filter_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("strategy_filter_delete_failed | filter=%s | error=%s", filter_id, exc)


def delete_rule_filter(filter_id: str) -> None:
    """Delete a rule-scoped filter."""

    if not db.available or not filter_id:
        return
    try:
        with db.session() as session:
            record = session.get(RuleFilterRecord, filter_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("rule_filter_delete_failed | filter=%s | error=%s", filter_id, exc)


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
            # Bot rows no longer persist datasource/exchange/timeframe; these are
            # owned by strategies. Ignore any payload values for these fields.
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
                record = BotTradeRecord(
                    id=str(trade_id),
                    bot_id=str(bot_id),
                    direction=snapshot.get("direction") or "long",
                )
                record.created_at = now
                session.add(record)
            run_id = snapshot.get("run_id")
            if run_id:
                record.run_id = str(run_id)
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
            # quote_currency is no longer stored on trades; resolve via instrument service when needed
            if snapshot.get("metrics") is not None:
                incoming_metrics = dict(snapshot.get("metrics") or {})
                existing_metrics = dict(record.metrics or {})
                existing_metrics.update(incoming_metrics)
                record.metrics = existing_metrics
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
                pnl=_coerce_float(event.get("pnl")),
                event_time=event_time,
            )
            session.add(record)
    except SQLAlchemyError as exc:
        logger.warning("bot_trade_event_persist_failed | trade=%s | error=%s", trade_id, exc)


def record_bot_run_step(payload: Dict[str, Any]) -> None:
    """Persist a timed bot runtime step for profiler dashboards."""

    if not db.available:
        return
    run_id = str(payload.get("run_id") or "").strip()
    step_name = str(payload.get("step_name") or "").strip()
    if not run_id or not step_name:
        return
    started_at = _parse_optional_timestamp(payload.get("started_at"))
    ended_at = _parse_optional_timestamp(payload.get("ended_at"))
    duration_ms = _coerce_float(payload.get("duration_ms"))
    if started_at is None or ended_at is None or duration_ms is None:
        return
    try:
        with db.session() as session:
            now = _utcnow()
            record = BotRunStepRecord(
                run_id=run_id,
                bot_id=str(payload.get("bot_id") or "") or None,
                step_name=step_name,
                started_at=started_at,
                ended_at=ended_at,
                duration_ms=float(duration_ms),
                ok=bool(payload.get("ok", True)),
                strategy_id=str(payload.get("strategy_id") or "") or None,
                symbol=str(payload.get("symbol") or "") or None,
                timeframe=str(payload.get("timeframe") or "") or None,
                error=(str(payload.get("error"))[:1024] if payload.get("error") else None),
                context=dict(payload.get("context") or {}),
                created_at=now,
            )
            session.add(record)
    except SQLAlchemyError as exc:
        logger.warning("bot_run_step_persist_failed | run_id=%s | step=%s | error=%s", run_id, step_name, exc)
