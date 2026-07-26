"""Canonical candle storage reads used by reports and coverage diagnostics."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from data_providers.utils.ohlcv import interval_to_timedelta
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION
from sqlalchemy import text

from ._shared import _parse_optional_timestamp, db


def _seconds_to_timeframe(seconds: int) -> str:
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _timeframe_seconds(timeframe: str) -> int:
    seconds = int(interval_to_timedelta(str(timeframe or "").strip()).total_seconds())
    if seconds <= 0:
        raise ValueError("candle timeframe must be positive")
    return seconds


def get_candle_storage_summary(
    *,
    instrument_id: str,
    timeframe: str,
    start: Any,
    end: Any,
) -> Optional[Dict[str, Any]]:
    """Return canonical accepted-candle availability for an exclusive-end window."""

    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip()
    start_at = _parse_optional_timestamp(start)
    end_at = _parse_optional_timestamp(end)
    if not instrument or not interval or start_at is None or end_at is None or end_at <= start_at:
        return None
    timeframe_seconds = _timeframe_seconds(interval)
    params = {
        "instrument_id": instrument,
        "fact_type": CANDLE_FACT_TYPE,
        "contract_version": CANDLE_FACT_VERSION,
        "timeframe_seconds": timeframe_seconds,
        "start_at": start_at,
        "end_at": end_at,
    }
    with db.session() as session:
        stats = session.execute(
            text(
                """
                WITH visible AS (
                    SELECT DISTINCT ON (versions.candle_open_time)
                        versions.candle_open_time, versions.open, versions.high,
                        versions.low, versions.close
                    FROM market.series AS series
                    JOIN market.candle_versions AS versions
                      ON versions.series_id = series.id
                    WHERE series.instrument_id = :instrument_id
                      AND series.fact_type = :fact_type
                      AND series.contract_version = :contract_version
                      AND series.timeframe_seconds = :timeframe_seconds
                      AND versions.candle_open_time >= :start_at
                      AND versions.candle_open_time < :end_at
                    ORDER BY versions.candle_open_time, versions.revision DESC
                ),
                ordered AS (
                    SELECT *, lag(candle_open_time) OVER (ORDER BY candle_open_time) AS previous_time
                    FROM visible
                )
                SELECT
                    count(*) AS candle_count,
                    min(candle_open_time) AS first_candle,
                    max(candle_open_time) AS last_candle,
                    count(*) FILTER (
                        WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
                    ) AS missing_ohlc_count,
                    count(*) - count(DISTINCT candle_open_time) AS duplicate_count,
                    count(*) FILTER (
                        WHERE previous_time IS NOT NULL
                          AND extract(epoch FROM (candle_open_time - previous_time)) > :timeframe_seconds
                    ) AS gap_count,
                    coalesce(
                        sum(
                            greatest(
                                floor(
                                    extract(epoch FROM (candle_open_time - previous_time))
                                    / :timeframe_seconds
                                )::int - 1,
                                0
                            )
                        ) FILTER (WHERE previous_time IS NOT NULL),
                        0
                    ) AS missing_count
                FROM ordered
                """
            ),
            params,
        ).mappings().first()
        available = session.execute(
            text(
                """
                SELECT DISTINCT timeframe_seconds
                FROM market.series
                WHERE instrument_id = :instrument_id
                  AND fact_type = :fact_type
                  AND contract_version = :contract_version
                  AND timeframe_seconds IS NOT NULL
                ORDER BY timeframe_seconds
                """
            ),
            params,
        ).scalars().all()
    if not stats:
        return None
    return {
        "instrument_id": instrument,
        "timeframe": interval,
        "timeframe_seconds": timeframe_seconds,
        "market_data_contract": CANDLE_FACT_VERSION,
        "candle_count": int(stats.get("candle_count") or 0),
        "first_candle": stats.get("first_candle"),
        "last_candle": stats.get("last_candle"),
        "missing_ohlc_count": int(stats.get("missing_ohlc_count") or 0),
        "duplicate_count": int(stats.get("duplicate_count") or 0),
        "gap_count": int(stats.get("gap_count") or 0),
        "missing_count": int(stats.get("missing_count") or 0),
        "available_resolutions": [
            _seconds_to_timeframe(int(value)) for value in available if value
        ],
    }


def list_candle_provider_gap_evidence(
    *,
    instrument_id: str,
    timeframe: str,
    start: Any,
    end: Any,
) -> List[Dict[str, Any]]:
    """Return canonical provider-missing evidence for a series window."""

    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip()
    start_at = _parse_optional_timestamp(start)
    end_at = _parse_optional_timestamp(end)
    if not instrument or not interval or start_at is None or end_at is None or end_at <= start_at:
        return []
    timeframe_seconds = _timeframe_seconds(interval)
    with db.session() as session:
        rows = session.execute(
            text(
                """
                SELECT evidence.start_time, evidence.end_time, evidence.classification,
                       evidence.expected_count, evidence.observed_count,
                       evidence.detected_as_of_commit_seq, evidence.evidence_hash,
                       evidence.evidence
                FROM market.series AS series
                JOIN market.gap_evidence AS evidence
                  ON evidence.series_id = series.id
                WHERE series.instrument_id = :instrument_id
                  AND series.fact_type = :fact_type
                  AND series.contract_version = :contract_version
                  AND series.timeframe_seconds = :timeframe_seconds
                  AND evidence.classification = 'provider_missing_data'
                  AND evidence.end_time > :start_at
                  AND evidence.start_time < :end_at
                ORDER BY evidence.start_time, evidence.end_time, evidence.evidence_hash
                """
            ),
            {
                "instrument_id": instrument,
                "fact_type": CANDLE_FACT_TYPE,
                "contract_version": CANDLE_FACT_VERSION,
                "timeframe_seconds": timeframe_seconds,
                "start_at": start_at,
                "end_at": end_at,
            },
        ).mappings().all()
    return [
        {
            "instrument_id": instrument,
            "timeframe": interval,
            "timeframe_seconds": timeframe_seconds,
            "start": row["start_time"],
            "end": row["end_time"],
            "classification": row["classification"],
            "expected_count": int(row["expected_count"]),
            "observed_count": int(row["observed_count"]),
            "detected_as_of_commit_seq": int(row["detected_as_of_commit_seq"]),
            "evidence_hash": row["evidence_hash"],
            "metadata": dict(row["evidence"] or {}),
        }
        for row in rows
    ]


def list_candles_for_series(
    *,
    instrument_id: str,
    timeframe: str,
    start: Any,
    end: Any,
    limit: int,
    prefer_latest: bool = False,
) -> List[Dict[str, Any]]:
    """Return latest accepted revisions for a bounded chart/debug window."""

    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip()
    start_at = _parse_optional_timestamp(start)
    end_at = _parse_optional_timestamp(end)
    if not instrument or not interval or (start_at is None and end_at is None):
        return []
    if start_at is not None and end_at is not None and end_at <= start_at:
        return []
    timeframe_seconds = _timeframe_seconds(interval)
    normalized_limit = max(1, min(int(limit or 320), 2000))
    predicates = []
    params: Dict[str, Any] = {
        "instrument_id": instrument,
        "fact_type": CANDLE_FACT_TYPE,
        "contract_version": CANDLE_FACT_VERSION,
        "timeframe_seconds": timeframe_seconds,
        "limit": normalized_limit,
    }
    if start_at is not None:
        predicates.append("versions.candle_open_time >= :start_at")
        params["start_at"] = start_at
    if end_at is not None:
        predicates.append("versions.candle_open_time < :end_at")
        params["end_at"] = end_at
    window_sql = " AND ".join(predicates)
    if window_sql:
        window_sql = " AND " + window_sql
    order_sql = "DESC" if prefer_latest else "ASC"
    with db.session() as session:
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (versions.candle_open_time)
                        versions.candle_open_time, versions.open, versions.high,
                        versions.low, versions.close, versions.volume, versions.revision,
                        versions.market_commit_seq
                    FROM market.series AS series
                    JOIN market.candle_versions AS versions
                      ON versions.series_id = series.id
                    WHERE series.instrument_id = :instrument_id
                      AND series.fact_type = :fact_type
                      AND series.contract_version = :contract_version
                      AND series.timeframe_seconds = :timeframe_seconds
                      {window_sql}
                    ORDER BY versions.candle_open_time, versions.revision DESC
                )
                SELECT * FROM visible
                ORDER BY candle_open_time {order_sql}
                LIMIT :limit
                """
            ),
            params,
        ).mappings().all()
    normalized = [
        {
            "time": int(row["candle_open_time"].timestamp()),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]) if row.get("volume") is not None else None,
            "revision": int(row["revision"]),
            "market_commit_seq": int(row["market_commit_seq"]),
        }
        for row in rows
    ]
    return sorted(normalized, key=lambda row: int(row["time"]))


__all__ = [
    "get_candle_storage_summary",
    "list_candle_provider_gap_evidence",
    "list_candles_for_series",
]
