from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import pandas as pd
from sqlalchemy import Engine, bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from core.logger import logger
from data_providers.config.runtime import PersistenceConfig
from data_providers.utils.ohlcv import compute_tr_atr


ATR_SHORT = 14
ATR_LONG = 50
ATR_Z_WINDOW = 100
DIRECTIONAL_EFFICIENCY_WINDOW = 20
SLOPE_WINDOW = 20
RANGE_WINDOW = 20
EXPANSION_WINDOW = 20
VOLUME_WINDOW = 50
LOOKBACK_BARS = 200


@dataclass(frozen=True)
class StatsResult:
    rows_upserted: int
    gaps: int
    last_candle_time: Optional[str]


class CandleStatsService:
    """Compute deterministic candle stats for a range and persist them."""

    def __init__(self, *, config: PersistenceConfig, engine: Optional[Engine]) -> None:
        self._config = config
        self._engine = engine

    def compute_range(
        self,
        *,
        instrument_id: str,
        timeframe_seconds: int,
        time_min: pd.Timestamp,
        time_max: pd.Timestamp,
        stats_version: str,
    ) -> StatsResult:
        if not self._engine:
            raise RuntimeError("Candle stats engine unavailable.")

        time_min = pd.to_datetime(time_min, utc=True)
        time_max = pd.to_datetime(time_max, utc=True)
        lookback_start = time_min - pd.Timedelta(seconds=timeframe_seconds * LOOKBACK_BARS)

        candles = self._load_candles(instrument_id, timeframe_seconds, lookback_start, time_max)
        if candles.empty:
            logger.warning(
                "candle_stats_no_candles | instrument_id=%s timeframe_seconds=%s time_min=%s time_max=%s stats_version=%s",
                instrument_id,
                timeframe_seconds,
                time_min.isoformat(),
                time_max.isoformat(),
                stats_version,
            )
            return StatsResult(rows_upserted=0, gaps=0, last_candle_time=None)

        stats_df = self._compute_stats(candles)
        in_window = stats_df[
            (stats_df["candle_time"] >= time_min) & (stats_df["candle_time"] <= time_max)
        ].copy()
        if in_window.empty:
            return StatsResult(rows_upserted=0, gaps=0, last_candle_time=None)

        rows = self._serialize_stats(
            in_window,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
            stats_version=stats_version,
        )
        self._upsert(rows)

        last_candle_time = in_window["candle_time"].max()
        gaps = self._count_gaps(in_window["candle_time"], timeframe_seconds)

        return StatsResult(
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

    def _compute_stats(self, candles: pd.DataFrame) -> pd.DataFrame:
        candles = candles.copy()
        candles["candle_time"] = pd.to_datetime(candles["candle_time"], utc=True)
        candles.sort_values("candle_time", inplace=True)
        candles = compute_tr_atr(candles, period=ATR_SHORT)
        candles.rename(columns={"atr_wilder": "atr_short"}, inplace=True)
        tr = candles["tr"]
        candles["atr_long"] = tr.ewm(alpha=1 / ATR_LONG, adjust=False).mean()
        candles["atr_zscore"] = self._rolling_zscore(candles["atr_short"], ATR_Z_WINDOW)
        candles["tr_pct"] = tr / candles["close"]
        candles["atr_ratio"] = candles["atr_short"] / candles["atr_long"]

        close = candles["close"]
        diff_abs = close.diff().abs()
        efficiency_denom = diff_abs.rolling(DIRECTIONAL_EFFICIENCY_WINDOW).sum()
        candles["directional_efficiency"] = (close - close.shift(DIRECTIONAL_EFFICIENCY_WINDOW)).abs() / efficiency_denom
        candles["slope"] = (close - close.shift(SLOPE_WINDOW)) / SLOPE_WINDOW
        candles["slope_stability"] = candles["slope"].rolling(SLOPE_WINDOW).std()

        range_high = candles["high"].rolling(RANGE_WINDOW).max()
        range_low = candles["low"].rolling(RANGE_WINDOW).min()
        range_width = range_high - range_low
        candles["range_width"] = range_width
        candles["range_position"] = (close - range_low) / range_width.replace(0, pd.NA)

        candles["atr_slope"] = candles["atr_short"] - candles["atr_short"].shift(EXPANSION_WINDOW)
        candles["range_contraction"] = range_width / range_width.shift(EXPANSION_WINDOW)
        candles["overlap_pct"] = self._overlap_ratio(candles)

        volume = candles.get("volume")
        candles["volume_zscore"] = self._rolling_zscore(volume, VOLUME_WINDOW)
        candles["volume_vs_median"] = volume / volume.rolling(VOLUME_WINDOW).median()

        return candles

    @staticmethod
    def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
        if series is None:
            return pd.Series(dtype="float64")
        mean = series.rolling(window).mean()
        std = series.rolling(window).std()
        return (series - mean) / std.replace(0, pd.NA)

    @staticmethod
    def _overlap_ratio(df: pd.DataFrame) -> pd.Series:
        prev_high = df["high"].shift()
        prev_low = df["low"].shift()
        overlap = (pd.concat([df["high"], prev_high], axis=1).min(axis=1)) - (
            pd.concat([df["low"], prev_low], axis=1).max(axis=1)
        )
        overlap = overlap.clip(lower=0)
        span = (df["high"] - df["low"]).replace(0, pd.NA)
        return overlap / span

    @staticmethod
    def _serialize_stats(
        df: pd.DataFrame,
        *,
        instrument_id: str,
        timeframe_seconds: int,
        stats_version: str,
    ) -> Iterable[Dict[str, Any]]:
        rows: list[Dict[str, Any]] = []
        for _, row in df.iterrows():
            stats = {
                "tr": _to_float(row.get("tr")),
                "atr_short": _to_float(row.get("atr_short")),
                "atr_long": _to_float(row.get("atr_long")),
                "atr_zscore": _to_float(row.get("atr_zscore")),
                "tr_pct": _to_float(row.get("tr_pct")),
                "atr_ratio": _to_float(row.get("atr_ratio")),
                "directional_efficiency": _to_float(row.get("directional_efficiency")),
                "slope": _to_float(row.get("slope")),
                "slope_stability": _to_float(row.get("slope_stability")),
                "range_position": _to_float(row.get("range_position")),
                "range_width": _to_float(row.get("range_width")),
                "atr_slope": _to_float(row.get("atr_slope")),
                "range_contraction": _to_float(row.get("range_contraction")),
                "overlap_pct": _to_float(row.get("overlap_pct")),
                "volume_zscore": _to_float(row.get("volume_zscore")),
                "volume_vs_median": _to_float(row.get("volume_vs_median")),
            }
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "candle_time": row["candle_time"],
                    "stats_version": stats_version,
                    "stats": stats,
                }
            )
        return rows

    def _upsert(self, rows: Iterable[Dict[str, Any]]) -> None:
        if not rows:
            return
        query = text(
            f"""
            INSERT INTO {self._config.candle_stats_table}
                (instrument_id, timeframe_seconds, candle_time, stats_version, stats)
            VALUES (:instrument_id, :timeframe_seconds, :candle_time, :stats_version, :stats)
            ON CONFLICT (instrument_id, timeframe_seconds, candle_time, stats_version)
            DO UPDATE SET computed_at = now(), stats = EXCLUDED.stats
            """
        ).bindparams(bindparam("stats", type_=JSONB))
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
