"""Bounded UTC activity projection over persisted records; never runs research."""
from datetime import datetime, timedelta, timezone
from sqlalchemy import func, select
from portal.backend.db.models import ResearchItemRecord, BotRunRecord
from . import browse

def bounds(start, end):
    try:
        values = [datetime.fromisoformat(v.replace("Z", "+00:00")) for v in (start, end)]
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("activity_range_invalid: ISO timestamps with timezone required") from exc
    if any(v.tzinfo is None or v.utcoffset() is None for v in values):
        raise ValueError("activity_range_invalid: timezone required")
    try:
        start, end = [v.astimezone(timezone.utc).replace(tzinfo=None) for v in values]
    except (ValueError, OverflowError) as exc:
        raise ValueError("activity_range_invalid: UTC timestamp outside supported calendar") from exc
    if not timedelta(0) < end - start <= timedelta(days=366):
        raise ValueError("activity_range_invalid: start must precede end; maximum 366 days")
    return start, end

def activity(activity_type, start, end):
    from .service import RESEARCH_ACTIVITY_TYPES
    start, end = bounds(start, end)
    span = end - start
    unit = "minute" if span <= timedelta(hours=2) else "hour" if span <= timedelta(days=7) else "day"
    step = {"minute": timedelta(minutes=1), "hour": timedelta(hours=1), "day": timedelta(days=1)}[unit]
    if activity_type == "backtests_completed":
        model, clock, statuses = BotRunRecord, BotRunRecord.ended_at, ("completed",)
        filters = [BotRunRecord.run_type == "backtest", BotRunRecord.status == "completed"]
        meaning = "Completed backtests by persisted ended_at."
    elif activity_type in RESEARCH_ACTIVITY_TYPES:
        kind, statuses, meaning = RESEARCH_ACTIVITY_TYPES[activity_type]
        model, clock = ResearchItemRecord, ResearchItemRecord.created_at
        filters = [model.kind == kind]
        if statuses:
            filters.append(model.status.in_(statuses))
        meaning = ("Checks saved after evaluation (including blocked results); persisted created_at, not exact evaluator finish time."
                   if activity_type == "checks_completed" else "Recorded research items by persisted created_at.")
    else:
        raise ValueError("activity_range_invalid: unsupported activity type")
    bucket = func.date_trunc(unit, clock)
    query = select(bucket.label("at"), func.count().label("total")).where(
        *filters, clock >= start, clock < end).group_by(bucket).order_by(bucket).limit(368)
    with browse.read_session() as session:
        rows = session.execute(query).all()
    if len(rows) > 367:
        raise ValueError("activity_range_limit: aggregate exceeds bounded bucket count")
    counts = {row.at: int(row.total) for row in rows}
    cursor = start.replace(second=0, microsecond=0)
    if unit != "minute": cursor = cursor.replace(minute=0)
    if unit == "day": cursor = cursor.replace(hour=0)
    result = []
    while cursor < end:
        result.append({"at": cursor.isoformat() + "Z", "total": counts.get(cursor, 0)})
        if end - cursor <= step: break
        cursor += step
    return {"schema_version": "research_activity_range.v1", "activity_type": activity_type,
            "from": start.isoformat() + "Z", "to": end.isoformat() + "Z", "timezone": "UTC",
            "boundary": "start_inclusive_end_exclusive", "timestamp_field": clock.key,
            "description": meaning, "qualifying_statuses": list(statuses), "bucket_unit": unit,
            "buckets": result, "total": sum(x["total"] for x in result)}
