"""Storage repository module."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, or_

from ._shared import BotRunRecord, SQLAlchemyError, _json_safe, _parse_optional_timestamp, _utcnow, db, func, logger, select

_NON_MATERIAL_CONFIG_KEYS = {
    "backtest_warmup_evidence",
    "generated_at",
    "report_generated_at",
    "report_warnings",
    "request_id",
    "runtime_warnings",
    "updated_at",
    "warnings",
}
_LIFECYCLE_OWNED_RUN_FIELDS = frozenset({"status", "started_at", "ended_at"})


def _merge_symbols(existing: Any, incoming: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for raw in list(existing or []) + list(incoming or []):
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        merged.append(symbol)
    return merged


def _hash_payload(payload: Any) -> Optional[str]:
    safe_payload = _json_safe(payload)
    if safe_payload in (None, "", [], {}):
        return None
    encoded = json.dumps(safe_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _material_config_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _material_config_payload(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
            if str(key) not in _NON_MATERIAL_CONFIG_KEYS
        }
    if isinstance(value, list):
        return [_material_config_payload(item) for item in value]
    return _json_safe(value)


def upsert_bot_run(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Insert or update non-lifecycle run identity, provenance, and report data."""

    if not db.available:
        raise RuntimeError("Database not available for run persistence")
    forbidden = sorted(
        field
        for field in _LIFECYCLE_OWNED_RUN_FIELDS
        if field in payload
    )
    if forbidden:
        raise ValueError(
            "bot run lifecycle fields are ledger-owned; "
            f"record a lifecycle checkpoint instead: {', '.join(forbidden)}"
        )
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("run_id is required for bot run persistence")
    with db.session() as session:
        record = session.get(BotRunRecord, run_id)
        now = _utcnow()
        if record is None:
            record = BotRunRecord(run_id=run_id, status="idle")
            record.created_at = now
            session.add(record)
        record.bot_id = payload.get("bot_id") or record.bot_id
        record.bot_name = payload.get("bot_name") or record.bot_name
        record.strategy_id = payload.get("strategy_id") or record.strategy_id
        record.strategy_name = payload.get("strategy_name") or record.strategy_name
        record.run_type = payload.get("run_type") or record.run_type or "backtest"
        record.timeframe = payload.get("timeframe") or record.timeframe
        record.datasource = payload.get("datasource") or record.datasource
        record.exchange = payload.get("exchange") or record.exchange
        symbols = payload.get("symbols")
        if symbols is not None:
            record.symbols = _merge_symbols(record.symbols, symbols)
        record.backtest_start = _parse_optional_timestamp(payload.get("backtest_start")) or record.backtest_start
        record.backtest_end = _parse_optional_timestamp(payload.get("backtest_end")) or record.backtest_end
        if payload.get("summary") is not None:
            record.summary = dict(_json_safe(payload.get("summary") or {}))
        if payload.get("config_snapshot") is not None:
            record.config_snapshot = dict(_json_safe(payload.get("config_snapshot") or {}))
            record.config_hash = (
                str(payload.get("config_hash") or "").strip()
                or _hash_payload(record.config_snapshot)
            )
            record.material_config_hash = (
                str(payload.get("material_config_hash") or payload.get("strategy_material_config_hash") or "").strip()
                or _hash_payload(_material_config_payload(record.config_snapshot))
            )
        for field in (
            "config_hash",
            "material_config_hash",
            "strategy_hash",
            "data_snapshot_hash",
            "runtime_contract_version",
            "runtime_source_revision",
            "runtime_image",
            "storage_schema_version",
        ):
            if payload.get(field) not in (None, ""):
                setattr(record, field, str(payload.get(field)).strip())
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


def list_bot_runs_by_ids(run_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Return run snapshots keyed by run id."""

    normalized = [str(run_id or "").strip() for run_id in run_ids]
    wanted = [run_id for run_id in dict.fromkeys(normalized) if run_id]
    if not wanted or not db.available:
        return {}
    with db.session() as session:
        rows = (
            session.execute(select(BotRunRecord).where(BotRunRecord.run_id.in_(wanted)))
            .scalars()
            .all()
        )
        return {str(row.run_id): row.to_dict() for row in rows}


def list_latest_bot_runs_by_bot_ids(bot_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Return the latest BotRunRecord for each bot id."""

    normalized = [str(bot_id or "").strip() for bot_id in bot_ids]
    wanted = [bot_id for bot_id in dict.fromkeys(normalized) if bot_id]
    if not wanted or not db.available:
        return {}
    with db.session() as session:
        rows = (
            session.execute(
                select(BotRunRecord)
                .where(BotRunRecord.bot_id.in_(wanted))
                .order_by(
                    BotRunRecord.bot_id.asc(),
                    BotRunRecord.started_at.desc().nullslast(),
                    BotRunRecord.updated_at.desc().nullslast(),
                    BotRunRecord.created_at.desc().nullslast(),
                    BotRunRecord.run_id.desc(),
                )
            )
            .scalars()
            .all()
        )
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        bot_id = str(row.bot_id or "")
        if bot_id and bot_id not in latest:
            latest[bot_id] = row.to_dict()
    return latest




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


def list_bot_runs_page(
    *,
    limit: int = 100,
    before_sort_at: Optional[str] = None,
    before_run_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return one stable, reverse-chronological run inventory window."""

    if not db.available:
        return []
    bounded_limit = max(1, min(int(limit or 100), 250))
    sort_at = func.coalesce(
        BotRunRecord.started_at,
        BotRunRecord.updated_at,
        BotRunRecord.created_at,
    )
    query = select(BotRunRecord)
    cursor_at = _parse_optional_timestamp(before_sort_at)
    cursor_run_id = str(before_run_id or "").strip()
    if cursor_at is not None:
        query = query.where(
            or_(
                sort_at < cursor_at,
                and_(sort_at == cursor_at, BotRunRecord.run_id < cursor_run_id),
            )
        )
    query = query.order_by(sort_at.desc(), BotRunRecord.run_id.desc()).limit(bounded_limit)
    with db.session() as session:
        rows = session.execute(query).scalars().all()
    return [row.to_dict() for row in rows]


def count_bot_runs_by_day(
    *,
    run_type: Optional[str] = None,
    status: Optional[str] = None,
    since: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Return per-day, per-status run counts bucketed by ``ended_at`` at UTC day boundaries.

    ``ended_at`` is stored as a naive UTC timestamp (see ``BotRunRecord``), so
    ``date_trunc('day', ended_at)`` already yields UTC day boundaries without
    an explicit timezone conversion.
    """

    if not db.available:
        return []
    day_bucket = func.date_trunc("day", BotRunRecord.ended_at)
    query = select(day_bucket.label("day"), BotRunRecord.status, func.count().label("total")).where(
        BotRunRecord.ended_at.is_not(None)
    )
    if run_type:
        query = query.where(BotRunRecord.run_type == run_type)
    if status:
        query = query.where(BotRunRecord.status == status)
    if since is not None:
        query = query.where(BotRunRecord.ended_at >= since)
    query = query.group_by(day_bucket, BotRunRecord.status).order_by(day_bucket)
    try:
        with db.session() as session:
            rows = session.execute(query).all()
            return [{"day": row.day, "status": row.status, "total": int(row.total)} for row in rows]
    except SQLAlchemyError as exc:
        logger.error(
            "bot_run_activity_count_failed | run_type=%s | status=%s | since=%s | error=%s",
            run_type,
            status,
            since,
            exc,
        )
        raise
