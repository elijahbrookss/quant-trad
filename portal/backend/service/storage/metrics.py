"""Read-only storage metrics; failures do not affect Storage status/settings."""
from __future__ import annotations

import logging
import math
from datetime import UTC, datetime, timedelta
from time import monotonic
from urllib.parse import urlsplit
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from core.settings import get_settings
from portal.backend.db import db

logger = logging.getLogger(__name__)
METRICS = {
    "database_allocated_bytes": "sampled_database_allocation",
    "market_allocated_bytes": "sampled_market_schema_allocation",
    "archive_compressed_bytes": "acknowledged_compressed_manifest_bytes",
    "verified_first_compressed_bytes": "first_qualified_verification_compressed_bytes",
    "reclaimed_physical_hot_bytes": "reclaimed_physical_partition_bytes",
}
STOCKS = frozenset(tuple(METRICS)[:2])
SERIES_SQL = text("""SELECT * FROM observability_metrics.storage_series_v1(
    :metric,CAST(:start AS timestamptz),CAST(:end AS timestamptz),:bucket)""")
STATUS_SQL = text("SELECT * FROM observability_metrics.storage_metric_status_v1()")


def utc_time(value: str | datetime) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise ValueError
        return parsed.astimezone(UTC)
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("storage_metrics_utc_time_required") from exc


def metric_range(start, end, resolution="auto"):
    start, end = utc_time(start), utc_time(end)
    if end <= start or end-start > timedelta(days=30):
        raise ValueError("storage_metrics_range_invalid")
    seconds = (end-start).total_seconds()
    choices = {"5m": 300, "30m": 1800, "2h": 7200}
    if resolution == "auto":
        bucket = next((x for x in choices.values() if math.ceil(seconds/x) <= 720), None)
    else:
        bucket = choices.get(resolution)
    if bucket is None or math.ceil(seconds/bucket) > 720:
        raise ValueError("storage_metrics_resolution_invalid")
    return start, end, bucket


def _iso(value):
    if value is None:
        return None
    return value.replace(tzinfo=UTC).isoformat() if value.tzinfo is None else value.astimezone(UTC).isoformat()


def grafana_link(value):
    if not isinstance(value, str) or not value:
        return {"state": "unavailable", "dashboard_url": None, "reason": "grafana_unconfigured"}
    try:
        url = urlsplit(value)
        _ = url.port  # Reject malformed/non-numeric ports before publishing a link.
        valid = (url.scheme in {"http", "https"} and bool(url.hostname)
                 and not url.username and not url.password and not url.query and not url.fragment
                 and "\\" not in value and not any(ord(c) < 33 for c in value) and len(value) <= 2048)
        if valid:
            return {"state": "available", "dashboard_url": value, "reason": None}
    except ValueError:
        pass
    return {"state": "unavailable", "dashboard_url": None, "reason": "grafana_url_invalid"}


def _series(metric, rows, *, start, end, bucket, now, failure=None):
    limit = 1000000 if metric == "market_allocated_bytes" else 2048 if metric == "reclaimed_physical_hot_bytes" else 10000
    points = [{"time": _iso(r["time"]), "value": int(r["value"]) if r["value"] is not None else None,
               "state": r["state"], "reason": r["reason"],
               "observed_at":_iso(r.get("observed_at")),"max_gap_seconds":r.get("max_gap_seconds")} for r in rows]
    good = [p for p in points if p["value"] is not None]
    truncated = any(r["truncated"] for r in rows)
    missing = sum(p["value"] is None for p in points)
    complete = bool(rows) and all(r["complete"] for r in rows) and (not missing or metric not in STOCKS)
    state = "unavailable" if failure or not good else "partial" if truncated or (metric in STOCKS and (missing or not complete)) else "ready"
    source_reason = next((r["reason"] for r in rows if r["reason"] not in (None,"no_sample","no_completion_record")),None)
    reason = failure or source_reason or ("source_limit" if truncated else "sample_gaps" if metric in STOCKS and missing else "no_observations" if not good else None)
    if any(r["state"] == "stale" for r in rows):
        state,reason,complete = "stale","capacity_sample_stale",False
    # A flow's last event is not a sampler heartbeat. Historical ranges are not live-stale.
    if metric in STOCKS and good and now-end <= timedelta(seconds=900) and now >= end:
        last_bucket = utc_time(good[-1].get("observed_at") or good[-1]["time"])
        if end-last_bucket > timedelta(seconds=900):
            state,reason,complete = "stale","capacity_sample_stale",False
    summary = {"first_at": (good[0].get("observed_at") or good[0]["time"]) if good else None,
               "last_at": (good[-1].get("observed_at") or good[-1]["time"]) if good else None,
               "net_change_bytes": None,"average_bytes_per_day": None,"total_bytes": None}
    if good and not truncated:
        if metric in STOCKS and len(good)>1:
            elapsed = (utc_time(summary["last_at"])-utc_time(summary["first_at"])).total_seconds()
            summary["net_change_bytes"] = good[-1]["value"]-good[0]["value"]
            summary["average_bytes_per_day"] = summary["net_change_bytes"]*86400/elapsed if elapsed>0 else None
        elif metric not in STOCKS:
            summary["total_bytes"] = sum(p["value"] for p in good)
    return {"metric_id":metric,"unit":"bytes","basis":METRICS[metric],"state":state,"reason":reason,
            "points":points,"coverage":{"complete":complete,"truncated":truncated,
                "source_count":max((r["source_count"] for r in rows),default=None),
                "source_limit":limit,"missing_buckets":missing},"summary":summary}


class StorageMetrics:
    def __init__(self, database=None, *, settings=None, clock=None):
        self.database = database if database is not None else db
        self.settings = settings or get_settings
        self.clock = clock or (lambda: datetime.now(UTC))

    def snapshot(self, *, start, end, resolution="auto") -> dict[str, Any]:
        start,end,bucket = metric_range(start,end,resolution)
        now = self.clock()
        payload = {"schema_version":"qt.storage_metrics.v1","generated_at":_iso(now),
                   "range":{"from":_iso(start),"to":_iso(end),"bucket_seconds":bucket},
                   "cadence_seconds":300,"retention_days":30,"series":[],"stats":[],
                   "links":{"grafana":grafana_link(getattr(self.settings().storage,"grafana_dashboard_url",None))}}
        results, statuses = {}, []
        failure = None
        deadline = monotonic()+5
        try:
            with self.database.session() as session:
                session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
                session.execute(text("SET LOCAL lock_timeout = '250ms'"))
                def query(sql, params=None):
                    remaining = int((deadline-monotonic())*1000)
                    if remaining <= 0:
                        raise TimeoutError("storage_metrics_deadline")
                    session.execute(text("SELECT set_config('statement_timeout',:timeout,true)"),
                                    {"timeout":str(min(5000,remaining))})
                    return [dict(r) for r in session.execute(sql,params or {}).mappings()]
                for metric in METRICS:
                    results[metric] = query(SERIES_SQL,{"metric":metric,"start":start,"end":end,"bucket":bucket})
                statuses = query(STATUS_SQL)
        except (SQLAlchemyError, TimeoutError) as exc:
            failure = "storage_metrics_query_unavailable"
            # Never log DB exception text: it may contain a DSN, SQL or source identifiers.
            logger.warning("storage_metrics_query_unavailable | error_type=%s",type(exc).__name__)
        payload["series"] = [_series(metric,results.get(metric,[]),start=start,end=end,bucket=bucket,now=now,
            failure=failure if metric not in results else None) for metric in METRICS]
        payload["stats"] = [{**{k:r[k] for k in ("metric_id","value","unit","basis","state","reason")},
                            "oldest_day":r["oldest_day"].isoformat() if r["oldest_day"] else None,
                            "coverage":{"complete":r["complete"],"truncated":r["truncated"]}} for r in statuses]
        if not statuses:
            payload["stats"] = [{"metric_id":m,"value":None,"unit":"bytes",
                "basis":"current_eligible_sealed_partition_catalog","state":"unavailable",
                "reason":failure or "storage_status_unavailable","oldest_day":None,
                "coverage":{"complete":False,"truncated":False}} for m in
                ("eligible_backlog_physical_bytes","verified_guarded_physical_bytes")]
        return payload


storage_metrics = StorageMetrics()
