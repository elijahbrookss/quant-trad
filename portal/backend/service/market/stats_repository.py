"""Batch retrieval helpers for candle/regime stats tables."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from data_providers.config.runtime import PersistenceConfig, runtime_config_from_env


_engine: Optional[Engine] = None
_engine_dsn: Optional[str] = None


@dataclass(frozen=True)
class StatsSnapshot:
    candle_stats_latest: Dict[Tuple[str, datetime], Dict[str, Any]]
    candle_stats_by_version: Dict[Tuple[str, datetime, str], Dict[str, Any]]
    regime_stats_latest: Dict[Tuple[str, datetime], Dict[str, Any]]
    regime_stats_by_version: Dict[Tuple[str, datetime, str], Dict[str, Any]]


def _get_engine_and_config() -> Tuple[Optional[Engine], PersistenceConfig]:
    global _engine, _engine_dsn
    persistence = runtime_config_from_env().persistence
    if not persistence.dsn:
        return None, persistence
    if _engine is None or _engine_dsn != persistence.dsn:
        _engine = create_engine(persistence.dsn)
        _engine_dsn = persistence.dsn
    return _engine, persistence


def _to_naive(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _normalise_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        if isinstance(parsed, Mapping):
            return dict(parsed)
        return {}
    return {}


def _fetch_latest(
    *,
    engine: Engine,
    table: str,
    value_field: str,
    version_field: str,
    instrument_ids: Sequence[str],
    timeframe_seconds: int,
    start: datetime,
    end: datetime,
) -> Dict[Tuple[str, datetime], Dict[str, Any]]:
    if not instrument_ids or timeframe_seconds <= 0:
        return {}
    query = text(
        f"""
        SELECT DISTINCT ON (instrument_id, timeframe_seconds, candle_time)
            instrument_id,
            candle_time,
            {version_field},
            {value_field},
            computed_at
        FROM {table}
        WHERE instrument_id IN :instrument_ids
          AND timeframe_seconds = :timeframe_seconds
          AND candle_time BETWEEN :start AND :end
        ORDER BY instrument_id, timeframe_seconds, candle_time, computed_at DESC
        """
    ).bindparams(bindparam("instrument_ids", expanding=True))
    records: Dict[Tuple[str, datetime], Dict[str, Any]] = {}
    try:
        with engine.begin() as conn:
            result = conn.execute(
                query,
                {
                    "instrument_ids": list(instrument_ids),
                    "timeframe_seconds": timeframe_seconds,
                    "start": start,
                    "end": end,
                },
            )
            for row in result.mappings():
                candle_time = _to_naive(row.get("candle_time"))
                if candle_time is None:
                    continue
                payload = {
                    "version": row.get(version_field),
                    value_field: _normalise_json(row.get(value_field)),
                }
                records[(row.get("instrument_id"), candle_time)] = payload
    except SQLAlchemyError:
        return {}
    return records


def _fetch_versions(
    *,
    engine: Engine,
    table: str,
    value_field: str,
    version_field: str,
    instrument_ids: Sequence[str],
    timeframe_seconds: int,
    start: datetime,
    end: datetime,
    versions: Sequence[str],
) -> Dict[Tuple[str, datetime, str], Dict[str, Any]]:
    if not instrument_ids or timeframe_seconds <= 0 or not versions:
        return {}
    query = text(
        f"""
        SELECT instrument_id, candle_time, {version_field}, {value_field}
        FROM {table}
        WHERE instrument_id IN :instrument_ids
          AND timeframe_seconds = :timeframe_seconds
          AND candle_time BETWEEN :start AND :end
          AND {version_field} IN :versions
        """
    ).bindparams(
        bindparam("instrument_ids", expanding=True),
        bindparam("versions", expanding=True),
    )
    records: Dict[Tuple[str, datetime, str], Dict[str, Any]] = {}
    try:
        with engine.begin() as conn:
            result = conn.execute(
                query,
                {
                    "instrument_ids": list(instrument_ids),
                    "timeframe_seconds": timeframe_seconds,
                    "start": start,
                    "end": end,
                    "versions": list(versions),
                },
            )
            for row in result.mappings():
                candle_time = _to_naive(row.get("candle_time"))
                if candle_time is None:
                    continue
                payload = _normalise_json(row.get(value_field))
                records[(row.get("instrument_id"), candle_time, row.get(version_field))] = payload
    except SQLAlchemyError:
        return {}
    return records


def build_stats_snapshot(
    *,
    instrument_ids: Iterable[str],
    timeframe_seconds: int,
    start: datetime,
    end: datetime,
    candle_versions: Optional[Sequence[str]] = None,
    regime_versions: Optional[Sequence[str]] = None,
    include_latest_candle: bool = False,
    include_latest_regime: bool = False,
) -> StatsSnapshot:
    engine, persistence = _get_engine_and_config()
    if not engine:
        return StatsSnapshot({}, {}, {}, {})

    instrument_ids = [str(item).strip() for item in instrument_ids if item]
    candle_versions = [str(v).strip() for v in (candle_versions or []) if str(v).strip()]
    regime_versions = [str(v).strip() for v in (regime_versions or []) if str(v).strip()]

    candle_latest: Dict[Tuple[str, datetime], Dict[str, Any]] = {}
    regime_latest: Dict[Tuple[str, datetime], Dict[str, Any]] = {}
    if include_latest_candle:
        candle_latest = _fetch_latest(
            engine=engine,
            table=persistence.candle_stats_table,
            value_field="stats",
            version_field="stats_version",
            instrument_ids=instrument_ids,
            timeframe_seconds=timeframe_seconds,
            start=start,
            end=end,
        )
    if include_latest_regime:
        regime_latest = _fetch_latest(
            engine=engine,
            table=persistence.regime_stats_table,
            value_field="regime",
            version_field="regime_version",
            instrument_ids=instrument_ids,
            timeframe_seconds=timeframe_seconds,
            start=start,
            end=end,
        )

    candle_versions_map = _fetch_versions(
        engine=engine,
        table=persistence.candle_stats_table,
        value_field="stats",
        version_field="stats_version",
        instrument_ids=instrument_ids,
        timeframe_seconds=timeframe_seconds,
        start=start,
        end=end,
        versions=candle_versions,
    )
    regime_versions_map = _fetch_versions(
        engine=engine,
        table=persistence.regime_stats_table,
        value_field="regime",
        version_field="regime_version",
        instrument_ids=instrument_ids,
        timeframe_seconds=timeframe_seconds,
        start=start,
        end=end,
        versions=regime_versions,
    )
    return StatsSnapshot(
        candle_stats_latest=candle_latest,
        candle_stats_by_version=candle_versions_map,
        regime_stats_latest=regime_latest,
        regime_stats_by_version=regime_versions_map,
    )
