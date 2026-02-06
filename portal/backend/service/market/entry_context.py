"""Helpers to derive per-candle entry context from candle_stats/regime_stats tables."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from data_providers.config.runtime import PersistenceConfig, runtime_config_from_env

from portal.backend.service.market.stats_queue import REGIME_VERSION, STATS_VERSION


_engine: Optional[Engine] = None
_engine_dsn: Optional[str] = None


@dataclass(frozen=True)
class EntryContext:
    stats: Optional[Dict[str, Any]]
    regime: Optional[Dict[str, Any]]
    stats_fallback: bool
    regime_fallback: bool


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


def _floor_to_interval(value: datetime, interval_seconds: int) -> Optional[datetime]:
    if interval_seconds <= 0:
        return None
    value = _to_naive(value)
    if value is None:
        return None
    epoch = datetime(1970, 1, 1)
    total_seconds = int((value - epoch).total_seconds())
    remainder = total_seconds % interval_seconds
    return epoch + timedelta(seconds=total_seconds - remainder)


def _lookup_latest_row(
    engine: Engine,
    table: str,
    version_field: str,
    version_value: str,
    value_field: str,
    instrument_id: str,
    timeframe_seconds: int,
    target: datetime,
    delta: timedelta,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    if timeframe_seconds <= 0:
        return None, False
    query = text(
        f"""
        SELECT candle_time, {value_field}
        FROM {table}
        WHERE instrument_id = :instrument_id
          AND timeframe_seconds = :timeframe_seconds
          AND {version_field} = :version_value
          AND candle_time <= :target
        ORDER BY candle_time DESC
        LIMIT 1
        """
    )
    try:
        with engine.begin() as conn:
            result = conn.execute(
                query,
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "version_value": version_value,
                    "target": target,
                },
            )
            row = result.mappings().first()
    except SQLAlchemyError:
        return None, False
    if not row:
        return None, False
    candle_time = _to_naive(row.get("candle_time"))
    if candle_time is None:
        return None, False
    diff = target - candle_time
    if diff < timedelta(0) or diff > delta:
        return None, False
    raw_value = row.get(value_field)
    if isinstance(raw_value, str):
        try:
            raw_value = json.loads(raw_value)
        except ValueError:
            raw_value = {}
    value = dict(raw_value or {})
    return value, diff > timedelta(0)


def derive_entry_context(
    *,
    instrument_id: Optional[str],
    timeframe_seconds: Optional[int],
    entry_time: Optional[datetime],
    stats_version: str = STATS_VERSION,
    regime_version: str = REGIME_VERSION,
) -> Optional[EntryContext]:
    if not instrument_id or not timeframe_seconds or not entry_time:
        return None
    target = _floor_to_interval(entry_time, timeframe_seconds)
    if target is None:
        return None
    engine, persistence = _get_engine_and_config()
    if not engine:
        return None
    delta = timedelta(seconds=timeframe_seconds)
    stats, stats_fallback = _lookup_latest_row(
        engine,
        persistence.candle_stats_table,
        "stats_version",
        stats_version,
        "stats",
        instrument_id,
        timeframe_seconds,
        target,
        delta,
    )
    regime, regime_fallback = _lookup_latest_row(
        engine,
        persistence.regime_stats_table,
        "regime_version",
        regime_version,
        "regime",
        instrument_id,
        timeframe_seconds,
        target,
        delta,
    )
    return EntryContext(
        stats=stats,
        regime=regime,
        stats_fallback=stats_fallback,
        regime_fallback=regime_fallback,
    )


def build_entry_metrics(context: Optional[EntryContext]) -> Dict[str, Any]:
    if context is None:
        return {
            "entry_regime_missing": True,
            "entry_fallback_used": False,
            "entry_stats_warmup": False,
        }

    stats = context.stats or {}
    regime = context.regime or {}
    volatility = regime.get("volatility") or {}
    structure = regime.get("structure") or {}
    expansion = regime.get("expansion") or {}
    liquidity = regime.get("liquidity") or {}

    def _stat_or_regime(stat_key: str, regime_section: Dict[str, Any], regime_key: str) -> Optional[Any]:
        value = stats.get(stat_key)
        if value is not None:
            return value
        return regime_section.get(regime_key)

    metrics: Dict[str, Any] = {
        "entry_tr_pct": _stat_or_regime("tr_pct", volatility, "tr_pct"),
        "entry_atr_ratio": _stat_or_regime("atr_ratio", volatility, "atr_ratio"),
        "entry_atr_slope": _stat_or_regime("atr_slope", expansion, "atr_slope"),
        "entry_atr_zscore": _stat_or_regime("atr_zscore", volatility, "atr_zscore"),
        "entry_overlap_pct": _stat_or_regime("overlap_pct", expansion, "overlap_pct"),
        "entry_directional_efficiency": _stat_or_regime(
            "directional_efficiency", structure, "directional_efficiency"
        ),
        "entry_range_position": _stat_or_regime("range_position", structure, "range_position"),
        "entry_stats_warmup": bool(stats.get("slope_stability_warmup")),
        "entry_volatility_state": volatility.get("state"),
        "entry_structure_state": structure.get("state"),
        "entry_expansion_state": expansion.get("state"),
        "entry_liquidity_state": liquidity.get("state"),
        "entry_regime_confidence": regime.get("confidence"),
        "entry_regime_key": regime.get("regime_key"),
        "entry_regime_block_id": regime.get("regime_block_id"),
        "entry_regime_missing": context.regime is None,
        "entry_fallback_used": context.stats_fallback or context.regime_fallback,
    }
    return metrics
