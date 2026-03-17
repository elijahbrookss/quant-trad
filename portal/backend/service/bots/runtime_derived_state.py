from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

import pandas as pd

from data_providers.config.runtime import runtime_config_from_env
from engines.bot_runtime.core.domain import Candle
from portal.backend.service.market.candle_stats_service import CandleStatsService
from portal.backend.service.market.entry_context import EntryContext, build_entry_metrics
from portal.backend.service.market.regime_stats_service import RegimeStatsService
from portal.backend.service.market.stats_contract import REGIME_VERSION, STATS_VERSION


def _to_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _floor_to_interval(value: datetime, timeframe_seconds: int) -> Optional[datetime]:
    if timeframe_seconds <= 0:
        return None
    target = _to_utc_naive(value)
    epoch = datetime(1970, 1, 1)
    total_seconds = int((target - epoch).total_seconds())
    remainder = total_seconds % timeframe_seconds
    return epoch + timedelta(seconds=total_seconds - remainder)


def _candles_to_dataframe(candles: Sequence[Candle]) -> pd.DataFrame:
    rows = []
    for candle in candles or []:
        if not isinstance(candle, Candle):
            continue
        rows.append(
            {
                "candle_time": pd.to_datetime(candle.time, utc=True),
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": getattr(candle, "volume", None),
                "trade_count": getattr(candle, "trade_count", None),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["candle_time", "open", "high", "low", "close", "volume", "trade_count"])
    frame = pd.DataFrame(rows)
    frame.sort_values("candle_time", inplace=True)
    frame.reset_index(drop=True, inplace=True)
    return frame


def _build_stats_snapshot_maps(
    *,
    stats_df: pd.DataFrame,
    instrument_id: str,
    timeframe_seconds: int,
    stats_version: str,
) -> Tuple[
    Dict[Tuple[str, datetime], Dict[str, Any]],
    Dict[Tuple[str, datetime, str], Dict[str, Any]],
    Dict[datetime, Dict[str, Any]],
]:
    latest: Dict[Tuple[str, datetime], Dict[str, Any]] = {}
    by_version: Dict[Tuple[str, datetime, str], Dict[str, Any]] = {}
    by_time: Dict[datetime, Dict[str, Any]] = {}
    rows = CandleStatsService(
        config=runtime_config_from_env().persistence,
        engine=None,
    )._serialize_stats(
        stats_df,
        instrument_id=instrument_id,
        timeframe_seconds=timeframe_seconds,
        stats_version=stats_version,
    )
    for row in rows:
        candle_time = row.get("candle_time")
        if not isinstance(candle_time, datetime):
            continue
        candle_time = _to_utc_naive(candle_time)
        stats_payload = dict(row.get("stats") or {})
        latest[(instrument_id, candle_time)] = {"version": stats_version, "stats": stats_payload}
        by_version[(instrument_id, candle_time, stats_version)] = stats_payload
        by_time[candle_time] = stats_payload
    return latest, by_version, by_time


def _build_regime_snapshot_maps(
    *,
    candles_df: pd.DataFrame,
    stats_by_time: Mapping[datetime, Mapping[str, Any]],
    instrument_id: str,
    timeframe_seconds: int,
    regime_version: str,
) -> Tuple[
    Dict[Tuple[str, datetime], Dict[str, Any]],
    Dict[Tuple[str, datetime, str], Dict[str, Any]],
    Dict[datetime, Dict[str, Any]],
]:
    stats_payload_df = pd.DataFrame(
        [
            {
                "candle_time": pd.to_datetime(candle_time, utc=True),
                "stats": dict(payload or {}),
            }
            for candle_time, payload in stats_by_time.items()
        ]
    )
    if stats_payload_df.empty:
        return {}, {}, {}
    regimes = RegimeStatsService(
        config=runtime_config_from_env().persistence,
        engine=None,
    )._build_regimes(
        candles_df,
        stats_payload_df,
        instrument_id=instrument_id,
        timeframe_seconds=timeframe_seconds,
        regime_version=regime_version,
    )
    latest: Dict[Tuple[str, datetime], Dict[str, Any]] = {}
    by_version: Dict[Tuple[str, datetime, str], Dict[str, Any]] = {}
    by_time: Dict[datetime, Dict[str, Any]] = {}
    for row in regimes:
        candle_time = row.get("candle_time")
        if not isinstance(candle_time, datetime):
            continue
        candle_time = _to_utc_naive(candle_time)
        regime_payload = dict(row.get("regime") or {})
        latest[(instrument_id, candle_time)] = {"version": regime_version, "regime": regime_payload}
        by_version[(instrument_id, candle_time, regime_version)] = regime_payload
        by_time[candle_time] = regime_payload
    return latest, by_version, by_time


@dataclass(frozen=True)
class RuntimeSeriesDerivedState:
    instrument_id: str
    timeframe_seconds: int
    stats_version: str
    regime_version: str
    candle_stats_latest: Dict[Tuple[str, datetime], Dict[str, Any]]
    candle_stats_by_version: Dict[Tuple[str, datetime, str], Dict[str, Any]]
    regime_stats_latest: Dict[Tuple[str, datetime], Dict[str, Any]]
    regime_stats_by_version: Dict[Tuple[str, datetime, str], Dict[str, Any]]
    candle_stats_by_time: Dict[datetime, Dict[str, Any]]
    regime_rows: Dict[datetime, Dict[str, Any]]

    def entry_context(self, *, entry_time: Optional[datetime]) -> Optional[EntryContext]:
        if not self.instrument_id or self.timeframe_seconds <= 0 or not isinstance(entry_time, datetime):
            return None
        target = _floor_to_interval(entry_time, self.timeframe_seconds)
        if target is None:
            return None
        stats = self.candle_stats_by_time.get(target)
        regime = self.regime_rows.get(target)
        stats_fallback = False
        regime_fallback = False
        if stats is None:
            for candle_time in sorted(self.candle_stats_by_time.keys(), reverse=True):
                if candle_time > target or (target - candle_time) > timedelta(seconds=self.timeframe_seconds):
                    continue
                stats = self.candle_stats_by_time.get(candle_time)
                stats_fallback = stats is not None and candle_time != target
                break
        if regime is None:
            for candle_time in sorted(self.regime_rows.keys(), reverse=True):
                if candle_time > target or (target - candle_time) > timedelta(seconds=self.timeframe_seconds):
                    continue
                regime = self.regime_rows.get(candle_time)
                regime_fallback = regime is not None and candle_time != target
                break
        return EntryContext(
            stats=dict(stats or {}) if isinstance(stats, Mapping) else None,
            regime=dict(regime or {}) if isinstance(regime, Mapping) else None,
            stats_fallback=stats_fallback,
            regime_fallback=regime_fallback,
        )

    def entry_metrics(self, *, entry_time: Optional[datetime]) -> Dict[str, Any]:
        return build_entry_metrics(self.entry_context(entry_time=entry_time))


def build_runtime_series_derived_state(
    *,
    candles: Sequence[Candle],
    instrument_id: str,
    timeframe_seconds: int,
    stats_version: str = STATS_VERSION,
    regime_version: str = REGIME_VERSION,
) -> RuntimeSeriesDerivedState:
    frame = _candles_to_dataframe(candles)
    if frame.empty:
        return RuntimeSeriesDerivedState(
            instrument_id=str(instrument_id or ""),
            timeframe_seconds=int(timeframe_seconds or 0),
            stats_version=str(stats_version),
            regime_version=str(regime_version),
            candle_stats_latest={},
            candle_stats_by_version={},
            regime_stats_latest={},
            regime_stats_by_version={},
            candle_stats_by_time={},
            regime_rows={},
        )

    stats_service = CandleStatsService(config=runtime_config_from_env().persistence, engine=None)
    stats_df = stats_service._compute_stats(frame)
    candle_latest, candle_by_version, candle_by_time = _build_stats_snapshot_maps(
        stats_df=stats_df,
        instrument_id=str(instrument_id or ""),
        timeframe_seconds=int(timeframe_seconds or 0),
        stats_version=str(stats_version),
    )
    regime_latest, regime_by_version, regime_rows = _build_regime_snapshot_maps(
        candles_df=frame,
        stats_by_time=candle_by_time,
        instrument_id=str(instrument_id or ""),
        timeframe_seconds=int(timeframe_seconds or 0),
        regime_version=str(regime_version),
    )
    return RuntimeSeriesDerivedState(
        instrument_id=str(instrument_id or ""),
        timeframe_seconds=int(timeframe_seconds or 0),
        stats_version=str(stats_version),
        regime_version=str(regime_version),
        candle_stats_latest=candle_latest,
        candle_stats_by_version=candle_by_version,
        regime_stats_latest=regime_latest,
        regime_stats_by_version=regime_by_version,
        candle_stats_by_time=candle_by_time,
        regime_rows=regime_rows,
    )


__all__ = ["RuntimeSeriesDerivedState", "build_runtime_series_derived_state"]
