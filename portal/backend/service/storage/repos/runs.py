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


def _timestamp_iso(value: Any) -> Optional[str]:
    return value.isoformat() + "Z" if value is not None else None


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
        ranked = (
            select(
                BotRunRecord.run_id.label("run_id"),
                func.row_number()
                .over(
                    partition_by=BotRunRecord.bot_id,
                    order_by=(
                        BotRunRecord.started_at.desc().nullslast(),
                        BotRunRecord.updated_at.desc().nullslast(),
                        BotRunRecord.created_at.desc().nullslast(),
                        BotRunRecord.run_id.desc(),
                    ),
                )
                .label("row_rank"),
            )
            .where(BotRunRecord.bot_id.in_(wanted))
            .subquery()
        )
        rows = (
            session.execute(
                select(BotRunRecord)
                .join(ranked, ranked.c.run_id == BotRunRecord.run_id)
                .where(ranked.c.row_rank == 1)
            )
            .scalars()
            .all()
        )
    return {
        str(row.bot_id): row.to_dict()
        for row in rows
        if str(row.bot_id or "").strip()
    }


def list_report_catalog_candidates(
    *,
    run_type: str,
    status: str,
    bot_id: Optional[str] = None,
    timeframe: Optional[str] = None,
    started_after: Optional[str] = None,
    started_before: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return compact fields used to rank report catalog rows.

    The hot catalog must not deserialize every run configuration. Dataset
    identity and execution settings are loaded only for the bounded page
    selected by the caller.
    """

    if not db.available:
        return []
    columns = (
        BotRunRecord.run_id,
        BotRunRecord.bot_id,
        BotRunRecord.bot_name,
        BotRunRecord.strategy_id,
        BotRunRecord.strategy_name,
        BotRunRecord.run_type,
        BotRunRecord.status,
        BotRunRecord.timeframe,
        BotRunRecord.symbols,
        BotRunRecord.backtest_start,
        BotRunRecord.backtest_end,
        BotRunRecord.started_at,
        BotRunRecord.ended_at,
        BotRunRecord.summary,
        BotRunRecord.data_snapshot_hash,
        BotRunRecord.updated_at,
    )
    query = select(*columns)
    if run_type:
        query = query.where(BotRunRecord.run_type == str(run_type))
    if status:
        query = query.where(BotRunRecord.status == str(status))
    if bot_id:
        query = query.where(BotRunRecord.bot_id == str(bot_id))
    if timeframe:
        query = query.where(BotRunRecord.timeframe == str(timeframe))
    start_dt = _parse_optional_timestamp(started_after)
    if start_dt:
        query = query.where(BotRunRecord.ended_at >= start_dt)
    end_dt = _parse_optional_timestamp(started_before)
    if end_dt:
        query = query.where(BotRunRecord.ended_at <= end_dt)
    with db.session() as session:
        rows = session.execute(query).mappings().all()
    return [
        {
            **dict(row),
            "symbols": list(row.get("symbols") or []),
            "summary": dict(row.get("summary") or {}),
            "backtest_start": _timestamp_iso(row.get("backtest_start")),
            "backtest_end": _timestamp_iso(row.get("backtest_end")),
            "started_at": _timestamp_iso(row.get("started_at")),
            "ended_at": _timestamp_iso(row.get("ended_at")),
            "updated_at": _timestamp_iso(row.get("updated_at")),
        }
        for row in rows
    ]


def list_report_catalog_details(run_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Return only the JSON scalar details needed by report catalog cards."""

    wanted = [str(run_id or "").strip() for run_id in dict.fromkeys(run_ids) if str(run_id or "").strip()]
    if not wanted or not db.available:
        return {}
    config = BotRunRecord.config_snapshot
    query = select(
        BotRunRecord.run_id,
        func.coalesce(
            config["execution_mode"].as_string(),
            config["bot"]["execution_mode"].as_string(),
            config["risk_settings"]["execution_mode"].as_string(),
            config["bot"]["risk"]["execution_mode"].as_string(),
        ).label("execution_mode"),
        func.coalesce(
            config["execution_behavior"].as_string(),
            config["bot"]["execution_behavior"].as_string(),
            config["bot"]["risk"]["execution_behavior"].as_string(),
        ).label("execution_behavior"),
        config["dataset_binding"]["dataset_id"].as_string().label("dataset_id"),
        config["dataset_binding"]["dataset_hash"].as_string().label("dataset_hash"),
    ).where(BotRunRecord.run_id.in_(wanted))
    with db.session() as session:
        rows = session.execute(query).mappings().all()
    return {
        str(row["run_id"]): {
            "execution_mode": row.get("execution_mode"),
            "execution_behavior": row.get("execution_behavior"),
            "dataset_id": row.get("dataset_id"),
            "dataset_hash": row.get("dataset_hash"),
        }
        for row in rows
    }




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
    config = BotRunRecord.config_snapshot
    columns = (
        BotRunRecord.run_id,
        BotRunRecord.bot_id,
        BotRunRecord.bot_name,
        BotRunRecord.strategy_id,
        BotRunRecord.strategy_name,
        BotRunRecord.run_type,
        BotRunRecord.status,
        BotRunRecord.timeframe,
        BotRunRecord.datasource,
        BotRunRecord.exchange,
        BotRunRecord.symbols,
        BotRunRecord.backtest_start,
        BotRunRecord.backtest_end,
        BotRunRecord.started_at,
        BotRunRecord.ended_at,
        BotRunRecord.summary,
        BotRunRecord.config_hash,
        BotRunRecord.material_config_hash,
        BotRunRecord.strategy_hash,
        BotRunRecord.data_snapshot_hash,
        BotRunRecord.runtime_contract_version,
        BotRunRecord.runtime_source_revision,
        BotRunRecord.runtime_image,
        BotRunRecord.storage_schema_version,
        BotRunRecord.created_at,
        BotRunRecord.updated_at,
        config["execution_mode"].as_string().label("execution_mode"),
        config["execution_behavior"].as_string().label("execution_behavior"),
        config["dataset_binding"]["dataset_id"].as_string().label("dataset_id"),
    )
    query = select(*columns)
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
        rows = session.execute(query).mappings().all()
    projected: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        execution_mode = str(row.get("execution_mode") or "fast").strip().lower()
        if execution_mode not in {"fast", "full"}:
            execution_mode = "fast"
        execution_behavior = str(row.get("execution_behavior") or "simulated").strip().lower().replace("_", "-")
        if execution_behavior not in {"simulated", "observe-only"}:
            execution_behavior = "simulated"
        dataset_id = str(row.pop("dataset_id", "") or "").strip() or None
        row.update(
            {
                "symbols": list(row.get("symbols") or []),
                "summary": dict(row.get("summary") or {}),
                "backtest_start": _timestamp_iso(row.get("backtest_start")),
                "backtest_end": _timestamp_iso(row.get("backtest_end")),
                "started_at": _timestamp_iso(row.get("started_at")),
                "ended_at": _timestamp_iso(row.get("ended_at")),
                "created_at": _timestamp_iso(row.get("created_at")),
                "updated_at": _timestamp_iso(row.get("updated_at")),
                "execution_mode": execution_mode,
                "execution_behavior": execution_behavior,
                "dataset_id": dataset_id,
                "config_snapshot": (
                    {"dataset_binding": {"dataset_id": dataset_id}}
                    if dataset_id
                    else {}
                ),
            }
        )
        projected.append(row)
    return projected


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
