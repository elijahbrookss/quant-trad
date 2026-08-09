"""Canonical candle storage reads used by reports and coverage diagnostics."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, Dict, List, Mapping, Optional

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


def list_candles_for_series_windows(
    *,
    instrument_id: str,
    timeframe: str,
    windows: Sequence[Mapping[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Return latest accepted revisions for many independently bounded windows.

    Each requested window retains the same exclusive-end and per-window row-limit
    semantics as :func:`list_candles_for_series`. The database receives bounded
    batches so callers do not need one round trip per trade.
    """

    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip()
    if not instrument or not interval or not windows:
        return {}
    timeframe_seconds = _timeframe_seconds(interval)
    normalized_windows: List[Dict[str, Any]] = []
    results: Dict[str, List[Dict[str, Any]]] = {}
    for ordinal, window in enumerate(windows):
        window_id = str(window.get("window_id") or ordinal).strip()
        if not window_id:
            raise ValueError("candle window_id must be non-empty")
        if window_id in results:
            raise ValueError(f"duplicate candle window_id: {window_id}")
        results[window_id] = []
        start_at = _parse_optional_timestamp(window.get("start"))
        end_at = _parse_optional_timestamp(window.get("end"))
        if start_at is None or end_at is None or end_at <= start_at:
            continue
        normalized_windows.append(
            {
                "window_id": window_id,
                "ordinal": ordinal,
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "row_limit": max(1, min(int(window.get("limit") or 320), 2000)),
            }
        )

    batch_size = 500
    for offset in range(0, len(normalized_windows), batch_size):
        batch = normalized_windows[offset : offset + batch_size]
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    WITH requested AS (
                        SELECT *
                        FROM jsonb_to_recordset(CAST(:windows AS jsonb)) AS request(
                            window_id text,
                            ordinal integer,
                            start_at timestamptz,
                            end_at timestamptz,
                            row_limit integer
                        )
                    ),
                    series_match AS (
                        SELECT series.id
                        FROM market.series AS series
                        WHERE series.instrument_id = :instrument_id
                          AND series.fact_type = :fact_type
                          AND series.contract_version = :contract_version
                          AND series.timeframe_seconds = :timeframe_seconds
                    )
                    SELECT requested.window_id, requested.ordinal,
                           visible.candle_open_time, visible.open, visible.high,
                           visible.low, visible.close, visible.volume,
                           visible.revision, visible.market_commit_seq
                    FROM requested
                    CROSS JOIN series_match
                    JOIN LATERAL (
                        SELECT DISTINCT ON (versions.candle_open_time)
                            versions.candle_open_time, versions.open, versions.high,
                            versions.low, versions.close, versions.volume,
                            versions.revision, versions.market_commit_seq
                        FROM market.candle_versions AS versions
                        WHERE versions.series_id = series_match.id
                          AND versions.candle_open_time >= requested.start_at
                          AND versions.candle_open_time < requested.end_at
                        ORDER BY versions.candle_open_time, versions.revision DESC
                        LIMIT requested.row_limit
                    ) AS visible ON TRUE
                    ORDER BY requested.ordinal, visible.candle_open_time
                    """
                ),
                {
                    "windows": json.dumps(batch, sort_keys=True, separators=(",", ":")),
                    "instrument_id": instrument,
                    "fact_type": CANDLE_FACT_TYPE,
                    "contract_version": CANDLE_FACT_VERSION,
                    "timeframe_seconds": timeframe_seconds,
                },
            ).mappings().all()
        for row in rows:
            results[str(row["window_id"])].append(
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
            )
    return results


def _frozen_dataset_page_flags(
    *,
    frozen_start: Any,
    frozen_end: Any,
    effective_start: Any,
    effective_end: Any,
    has_extra: bool,
    prefer_latest: bool,
) -> tuple[bool, bool]:
    """Return dataset-relative paging flags for a bounded frozen candle page."""

    has_more_before = bool(
        effective_start > frozen_start or (has_extra and prefer_latest)
    )
    has_more_after = bool(
        effective_end < frozen_end or (has_extra and not prefer_latest)
    )
    return has_more_before, has_more_after


def read_frozen_dataset_candles(
    *,
    dataset_id: str,
    series_id: int,
    start: Any,
    end: Any,
    limit: int,
    prefer_latest: bool = False,
) -> Dict[str, Any]:
    """Read a bounded candle page at the dataset series frozen commit boundary."""

    normalized_dataset_id = str(dataset_id or "").strip()
    if not normalized_dataset_id:
        raise ValueError("market_dataset_id_required")
    normalized_limit = max(1, min(int(limit or 320), 2000))
    requested_start = _parse_optional_timestamp(start)
    requested_end = _parse_optional_timestamp(end)
    if requested_start is not None and requested_end is not None and requested_end <= requested_start:
        return {"candles": [], "has_more_before": False, "has_more_after": False}

    with db.session() as session:
        frozen = session.execute(
            text(
                """
                SELECT range_start, range_end, max_commit_seq
                FROM market.dataset_series
                WHERE dataset_id = :dataset_id AND series_id = :series_id
                """
            ),
            {"dataset_id": normalized_dataset_id, "series_id": int(series_id)},
        ).mappings().first()
        if frozen is None:
            raise ValueError(
                "market_dataset_series_unknown: "
                f"dataset_id={normalized_dataset_id} series_id={series_id}"
            )

        effective_start = max(
            value for value in (requested_start, frozen["range_start"]) if value is not None
        )
        effective_end = min(
            value for value in (requested_end, frozen["range_end"]) if value is not None
        )
        if effective_end <= effective_start:
            has_more_before, has_more_after = _frozen_dataset_page_flags(
                frozen_start=frozen["range_start"],
                frozen_end=frozen["range_end"],
                effective_start=effective_start,
                effective_end=effective_end,
                has_extra=False,
                prefer_latest=prefer_latest,
            )
            return {
                "candles": [],
                "has_more_before": has_more_before,
                "has_more_after": has_more_after,
                "range_start": frozen["range_start"],
                "range_end": frozen["range_end"],
                "max_commit_seq": int(frozen["max_commit_seq"]),
            }
        order_sql = "DESC" if prefer_latest else "ASC"
        rows = session.execute(
            text(
                f"""
                WITH visible AS (
                    SELECT DISTINCT ON (candle_open_time)
                        candle_open_time, open, high, low, close, volume, revision,
                        market_commit_seq
                    FROM market.candle_versions
                    WHERE series_id = :series_id
                      AND market_commit_seq <= :max_commit_seq
                      AND candle_open_time >= :start_at
                      AND candle_open_time < :end_at
                    ORDER BY candle_open_time, revision DESC
                )
                SELECT * FROM visible
                ORDER BY candle_open_time {order_sql}
                LIMIT :page_limit
                """
            ),
            {
                "series_id": int(series_id),
                "max_commit_seq": int(frozen["max_commit_seq"]),
                "start_at": effective_start,
                "end_at": effective_end,
                "page_limit": normalized_limit + 1,
            },
        ).mappings().all()

    has_extra = len(rows) > normalized_limit
    has_more_before, has_more_after = _frozen_dataset_page_flags(
        frozen_start=frozen["range_start"],
        frozen_end=frozen["range_end"],
        effective_start=effective_start,
        effective_end=effective_end,
        has_extra=has_extra,
        prefer_latest=prefer_latest,
    )
    selected = rows[:normalized_limit]
    candles = sorted(
        [
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
            for row in selected
        ],
        key=lambda row: int(row["time"]),
    )
    return {
        "candles": candles,
        "has_more_before": has_more_before,
        "has_more_after": has_more_after,
        "range_start": frozen["range_start"],
        "range_end": frozen["range_end"],
        "max_commit_seq": int(frozen["max_commit_seq"]),
    }


__all__ = [
    "get_candle_storage_summary",
    "list_candle_provider_gap_evidence",
    "list_candles_for_series",
    "list_candles_for_series_windows",
    "read_frozen_dataset_candles",
]
