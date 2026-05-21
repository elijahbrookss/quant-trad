"""Storage repository module."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from ._shared import (
    BotTradeEventRecord,
    BotTradeRecord,
    SQLAlchemyError,
    _coerce_float,
    _coerce_int,
    _parse_optional_timestamp,
    _utcnow,
    db,
    logger,
    select,
    uuid,
)

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




def record_bot_trade(snapshot: Dict[str, Any]) -> None:
    """Insert or update a stored trade snapshot for dashboarding."""

    if not db.available:
        raise RuntimeError("Database not available for trade persistence")
    trade_id = snapshot.get("trade_id") or snapshot.get("id")
    bot_id = snapshot.get("bot_id")
    if not trade_id or not bot_id:
        raise ValueError("trade persistence requires both trade_id and bot_id")
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
        logger.error(
            "bot_trade_persist_failed | trade=%s | bot_id=%s | run_id=%s | error=%s",
            trade_id,
            bot_id,
            snapshot.get("run_id"),
            exc,
        )
        raise


def record_bot_trade_event(event: Dict[str, Any]) -> None:
    """Persist a stop/target event for a stored trade."""

    if not db.available:
        raise RuntimeError("Database not available for trade event persistence")
    trade_id = event.get("trade_id")
    bot_id = event.get("bot_id")
    if not trade_id or not bot_id:
        raise ValueError("trade event persistence requires both trade_id and bot_id")
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
        logger.error(
            "bot_trade_event_persist_failed | event=%s | trade=%s | bot_id=%s | error=%s",
            event_id,
            trade_id,
            bot_id,
            exc,
        )
        raise


