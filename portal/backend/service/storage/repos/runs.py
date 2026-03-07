"""Storage repository module."""

from __future__ import annotations

from ._shared import *

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




