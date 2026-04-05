"""Storage repository module."""

from __future__ import annotations

from ._shared import *

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
            if "strategy_variant_id" in payload:
                variant_id = payload.get("strategy_variant_id")
                record.strategy_variant_id = str(variant_id).strip() if variant_id else None
            if "strategy_variant_name" in payload:
                variant_name = payload.get("strategy_variant_name")
                record.strategy_variant_name = str(variant_name).strip() if variant_name else None
            if "atm_template_id" in payload:
                atm_template_id = payload.get("atm_template_id")
                record.atm_template_id = str(atm_template_id).strip() if atm_template_id else None
            if "resolved_params" in payload:
                record.resolved_params = dict(_json_safe(payload.get("resolved_params") or {}))
            if "risk_config" in payload:
                record.risk_config = dict(_json_safe(payload.get("risk_config") or {}))
            # datasource/exchange/timeframe are no longer stored on bots; derive from strategy at runtime
            record.mode = payload.get("mode") or record.mode
            record.run_type = payload.get("run_type") or record.run_type
            record.playback_speed = 0.0
            if "risk" in payload:
                record.risk = dict(payload.get("risk") or {})
            if "wallet_config" in payload:
                record.wallet_config = dict(payload.get("wallet_config") or {})
            if "snapshot_interval_ms" in payload:
                record.snapshot_interval_ms = int(payload.get("snapshot_interval_ms") or 0)
            if "bot_env" in payload:
                record.bot_env = dict(payload.get("bot_env") or {})
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




def get_bot(bot_id: str) -> Optional[Dict[str, Any]]:
    """Return a persisted bot configuration."""

    if not db.available:
        return None
    if not bot_id:
        return None
    with db.session() as session:
        record = session.get(BotRecord, bot_id)
        return record.to_dict() if record else None




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


