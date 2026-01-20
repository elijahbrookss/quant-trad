from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import pandas as pd
from sqlalchemy import Engine, bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from core.logger import logger
from data_providers.config.runtime import PersistenceConfig

from .regime_engine import RegimeEngineV1


LOOKBACK_BARS = 200


@dataclass(frozen=True)
class RegimeResult:
    rows_upserted: int
    gaps: int
    last_candle_time: Optional[str]


class RegimeStatsService:
    """Compute deterministic regime stats for a range and persist them."""

    def __init__(self, *, config: PersistenceConfig, engine: Optional[Engine]) -> None:
        self._config = config
        self._engine = engine
        self._engine_impl = RegimeEngineV1()

    def compute_range(
        self,
        *,
        instrument_id: str,
        timeframe_seconds: int,
        time_min: pd.Timestamp,
        time_max: pd.Timestamp,
        stats_version: str,
        regime_version: str,
    ) -> RegimeResult:
        if not self._engine:
            raise RuntimeError("Regime stats engine unavailable.")

        time_min = pd.to_datetime(time_min, utc=True)
        time_max = pd.to_datetime(time_max, utc=True)
        lookback_start = time_min - pd.Timedelta(seconds=timeframe_seconds * LOOKBACK_BARS)
        logger.debug(
            "regime_stats_compute_start | instrument_id=%s timeframe_seconds=%s stats_version=%s regime_version=%s lookback_start=%s time_min=%s time_max=%s",
            instrument_id,
            timeframe_seconds,
            stats_version,
            regime_version,
            lookback_start.isoformat(),
            time_min.isoformat(),
            time_max.isoformat(),
        )

        candles = self._load_candles(instrument_id, timeframe_seconds, lookback_start, time_max)
        if candles.empty:
            logger.warning(
                "regime_stats_no_candles | instrument_id=%s timeframe_seconds=%s time_min=%s time_max=%s regime_version=%s",
                instrument_id,
                timeframe_seconds,
                time_min.isoformat(),
                time_max.isoformat(),
                regime_version,
            )
            return RegimeResult(rows_upserted=0, gaps=0, last_candle_time=None)

        stats_df = self._load_candle_stats(
            instrument_id,
            timeframe_seconds,
            lookback_start,
            time_max,
            stats_version,
        )

        in_window_candles = candles[
            (candles["candle_time"] >= time_min) & (candles["candle_time"] <= time_max)
        ].copy()
        in_window_stats = stats_df[
            (stats_df["candle_time"] >= time_min) & (stats_df["candle_time"] <= time_max)
        ].copy()

        missing = self._find_missing_stats(in_window_candles["candle_time"], in_window_stats["candle_time"])
        if missing:
            logger.debug(
                "regime_stats_missing_samples | instrument_id=%s timeframe_seconds=%s stats_version=%s missing_times=%s",
                instrument_id,
                timeframe_seconds,
                stats_version,
                [ts.isoformat() for ts in missing[:5]],
            )
            logger.error(
                "regime_stats_missing_dependency | instrument_id=%s timeframe_seconds=%s stats_version=%s time_min=%s time_max=%s missing_count=%s",
                instrument_id,
                timeframe_seconds,
                stats_version,
                time_min.isoformat(),
                time_max.isoformat(),
                len(missing),
            )
            raise RuntimeError("Candle stats coverage missing for regime computation.")

        rows = self._build_regimes(
            in_window_candles,
            in_window_stats,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
            regime_version=regime_version,
        )
        self._upsert(rows)

        last_candle_time = in_window_candles["candle_time"].max()
        gaps = self._count_gaps(in_window_candles["candle_time"], timeframe_seconds)

        logger.debug(
            "regime_stats_compute_end | instrument_id=%s timeframe_seconds=%s regime_version=%s rows=%s last_candle_time=%s gaps=%s",
            instrument_id,
            timeframe_seconds,
            regime_version,
            len(rows),
            last_candle_time.isoformat() if last_candle_time is not None else None,
            gaps,
        )
        return RegimeResult(
            rows_upserted=len(rows),
            gaps=gaps,
            last_candle_time=last_candle_time.isoformat() if last_candle_time is not None else None,
        )

    def _load_candles(
        self,
        instrument_id: str,
        timeframe_seconds: int,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        logger.debug(
            "regime_stats_load_candles | instrument_id=%s timeframe_seconds=%s start=%s end=%s",
            instrument_id,
            timeframe_seconds,
            start.isoformat(),
            end.isoformat(),
        )
        query = text(
            f"""
            SELECT candle_time, open, high, low, close, volume, trade_count
            FROM {self._config.candles_raw_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND candle_time BETWEEN :start AND :end
            ORDER BY candle_time
            """
        )
        return pd.read_sql(
            query,
            self._engine,
            params={
                "instrument_id": instrument_id,
                "timeframe_seconds": timeframe_seconds,
                "start": start,
                "end": end,
            },
        )

    def _load_candle_stats(
        self,
        instrument_id: str,
        timeframe_seconds: int,
        start: pd.Timestamp,
        end: pd.Timestamp,
        stats_version: str,
    ) -> pd.DataFrame:
        logger.debug(
            "regime_stats_load_stats | instrument_id=%s timeframe_seconds=%s stats_version=%s start=%s end=%s",
            instrument_id,
            timeframe_seconds,
            stats_version,
            start.isoformat(),
            end.isoformat(),
        )
        query = text(
            f"""
            SELECT candle_time, stats
            FROM {self._config.candle_stats_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND stats_version = :stats_version
              AND candle_time BETWEEN :start AND :end
            ORDER BY candle_time
            """
        )
        stats_df = pd.read_sql(
            query,
            self._engine,
            params={
                "instrument_id": instrument_id,
                "timeframe_seconds": timeframe_seconds,
                "stats_version": stats_version,
                "start": start,
                "end": end,
            },
        )
        stats_df["candle_time"] = pd.to_datetime(stats_df["candle_time"], utc=True)
        return stats_df

    @staticmethod
    def _find_missing_stats(candle_times: pd.Series, stats_times: pd.Series) -> list[pd.Timestamp]:
        candle_set = set(pd.to_datetime(candle_times, utc=True))
        stats_set = set(pd.to_datetime(stats_times, utc=True))
        return sorted(candle_set - stats_set)

    def _build_regimes(
        self,
        candles: pd.DataFrame,
        stats_df: pd.DataFrame,
        *,
        instrument_id: str,
        timeframe_seconds: int,
        regime_version: str,
    ) -> Iterable[Dict[str, Any]]:
        stats_map = {
            row["candle_time"]: row["stats"]
            for _, row in stats_df.iterrows()
        }
        rows: list[Dict[str, Any]] = []
        for _, candle in candles.iterrows():
            candle_time = candle["candle_time"]
            stats = stats_map.get(candle_time)
            if stats is None:
                continue
            candle_payload = {
                "open": _to_float(candle.get("open")),
                "high": _to_float(candle.get("high")),
                "low": _to_float(candle.get("low")),
                "close": _to_float(candle.get("close")),
                "volume": _to_float(candle.get("volume")),
                "trade_count": _to_float(candle.get("trade_count")),
            }
            regime = self._engine_impl.classify(candle_payload, stats).as_dict()
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "candle_time": candle_time,
                    "regime_version": regime_version,
                    "regime": regime,
                }
            )
        return rows

    def _upsert(self, rows: Iterable[Dict[str, Any]]) -> None:
        if not rows:
            return
        query = text(
            f"""
            INSERT INTO {self._config.regime_stats_table}
                (instrument_id, timeframe_seconds, candle_time, regime_version, regime)
            VALUES (:instrument_id, :timeframe_seconds, :candle_time, :regime_version, :regime)
            ON CONFLICT (instrument_id, timeframe_seconds, candle_time, regime_version)
            DO UPDATE SET computed_at = now(), regime = EXCLUDED.regime
            """
        ).bindparams(bindparam("regime", type_=JSONB))
        with self._engine.begin() as conn:
            conn.execute(query, list(rows))

    @staticmethod
    def _count_gaps(candle_times: pd.Series, timeframe_seconds: int) -> int:
        ordered = pd.to_datetime(candle_times, utc=True).sort_values()
        if ordered.empty or timeframe_seconds <= 0:
            return 0
        step = pd.Timedelta(seconds=timeframe_seconds)
        deltas = ordered.diff().dropna()
        return int((deltas > step * 1.5).sum())


def _to_float(value: Any) -> Optional[float]:
    try:
        if pd.isna(value):
            return None
    except Exception:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
