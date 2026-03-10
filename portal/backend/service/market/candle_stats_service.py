from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import pandas as pd
from sqlalchemy import Engine, bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from core.logger import logger
from data_providers.config.runtime import PersistenceConfig


ATR_SHORT = 14
ATR_LONG = 50
ATR_Z_WINDOW = 100
DIRECTIONAL_EFFICIENCY_WINDOW = 20
SLOPE_WINDOW = 20
RANGE_WINDOW = 20
EXPANSION_WINDOW = 20
VOLUME_WINDOW = 50
OVERLAP_WINDOW = 8
SLOPE_STABILITY_LOOKBACK = 150
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
        logger.debug(
            "candle_stats_compute_start | instrument_id=%s timeframe_seconds=%s stats_version=%s lookback_start=%s time_min=%s time_max=%s",
            instrument_id,
            timeframe_seconds,
            stats_version,
            lookback_start.isoformat(),
            time_min.isoformat(),
            time_max.isoformat(),
        )

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
            logger.warning(
                "candle_stats_no_window | instrument_id=%s timeframe_seconds=%s stats_version=%s time_min=%s time_max=%s",
                instrument_id,
                timeframe_seconds,
                stats_version,
                time_min.isoformat(),
                time_max.isoformat(),
            )
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

        logger.debug(
            "candle_stats_compute_end | instrument_id=%s timeframe_seconds=%s stats_version=%s rows=%s last_candle_time=%s gaps=%s",
            instrument_id,
            timeframe_seconds,
            stats_version,
            len(rows),
            last_candle_time.isoformat() if last_candle_time is not None else None,
            gaps,
        )
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
        logger.debug(
            "candle_stats_load_candles | instrument_id=%s timeframe_seconds=%s start=%s end=%s",
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

    def _compute_stats(self, candles: pd.DataFrame) -> pd.DataFrame:
        candles = candles.copy()
        candles["candle_time"] = pd.to_datetime(candles["candle_time"], utc=True)
        candles.sort_values("candle_time", inplace=True)
        true_range = self._true_range(candles)
        candles["tr"] = true_range
        candles["atr_short"] = true_range.ewm(alpha=1 / ATR_SHORT, adjust=False).mean()
        candles["atr_long"] = true_range.ewm(alpha=1 / ATR_LONG, adjust=False).mean()
        candles["atr_zscore"], _ = self._rolling_zscore(candles["atr_short"], ATR_Z_WINDOW)
        prev_close = candles["close"].shift().replace(0, pd.NA)
        candles["tr_pct"] = true_range / prev_close
        # Fall back to current close if we cannot normalize against the previous close.
        fallback_close = candles["close"].replace(0, pd.NA)
        candles["tr_pct"] = candles["tr_pct"].fillna(true_range / fallback_close)
        candles["atr_ratio"] = candles["atr_short"] / candles["atr_long"]

        close = candles["close"]
        diff_abs = close.diff().abs()
        efficiency_denom = diff_abs.rolling(DIRECTIONAL_EFFICIENCY_WINDOW).sum()
        candles["directional_efficiency"] = (close - close.shift(DIRECTIONAL_EFFICIENCY_WINDOW)).abs() / efficiency_denom
        candles["slope"] = (close - close.shift(SLOPE_WINDOW)) / SLOPE_WINDOW
        slope_std = candles["slope"].rolling(SLOPE_WINDOW, min_periods=SLOPE_WINDOW).std()
        candles["slope_stability"], slope_warmup = self._rolling_zscore(
            slope_std,
            SLOPE_STABILITY_LOOKBACK,
            min_periods=SLOPE_STABILITY_LOOKBACK,
        )
        candles["slope_stability_warmup"] = slope_warmup

        range_high = candles["high"].rolling(RANGE_WINDOW).max()
        range_low = candles["low"].rolling(RANGE_WINDOW).min()
        range_width = range_high - range_low
        candles["range_width"] = range_width
        candles["range_position"] = (close - range_low) / range_width.replace(0, pd.NA)

        candles["atr_slope"] = candles["atr_short"] - candles["atr_short"].shift(EXPANSION_WINDOW)
        candles["range_contraction"] = range_width / range_width.shift(EXPANSION_WINDOW)
        candles["overlap_pct"] = self._body_overlap_pct(candles)

        volume = candles.get("volume")
        candles["volume_zscore"], _ = self._rolling_zscore(volume, VOLUME_WINDOW)
        candles["volume_vs_median"] = volume / volume.rolling(VOLUME_WINDOW).median()

        return candles

    @staticmethod
    def _true_range(df: pd.DataFrame) -> pd.Series:
        prev_close = df["close"].shift()
        high_low = df["high"] - df["low"]
        high_prev_close = (df["high"] - prev_close).abs()
        low_prev_close = (df["low"] - prev_close).abs()
        stacked = pd.concat([high_low, high_prev_close, low_prev_close], axis=1)
        return stacked.max(axis=1, skipna=True)

    @staticmethod
    def _rolling_zscore(
        series: pd.Series,
        window: int,
        min_periods: Optional[int] = None,
    ) -> tuple[pd.Series, pd.Series]:
        if series is None:
            series = pd.Series(dtype="float64")
        min_periods = window if min_periods is None else min_periods
        rolling = series.rolling(window, min_periods=min_periods)
        mean = rolling.mean()
        std = rolling.std()
        warmup = mean.isna() | std.isna()
        zscore = (series - mean) / std.replace(0, pd.NA)
        zscore = zscore.where(~warmup)
        return zscore.fillna(0), warmup.fillna(True)

    @staticmethod
    def _body_overlap_pct(df: pd.DataFrame) -> pd.Series:
        body_high = df[["open", "close"]].max(axis=1)
        body_low = df[["open", "close"]].min(axis=1)
        prev_body_high = body_high.shift()
        prev_body_low = body_low.shift()
        max_high = pd.concat([body_high, prev_body_high], axis=1).max(axis=1)
        min_low = pd.concat([body_low, prev_body_low], axis=1).min(axis=1)
        overlap = pd.concat([body_high, prev_body_high], axis=1).min(axis=1) - pd.concat(
            [body_low, prev_body_low], axis=1
        ).max(axis=1)
        overlap = overlap.clip(lower=0)
        body_range = max_high - min_low
        zero_range = body_range == 0
        identical_bodies = zero_range & (body_high == prev_body_high) & (body_low == prev_body_low)
        ratio = overlap / body_range
        ratio = ratio.clip(lower=0, upper=1)
        ratio = ratio.fillna(0)
        ratio = ratio.mask(zero_range, 0.0)
        ratio = ratio.mask(identical_bodies, 1.0)
        raw_pct = ratio.fillna(0)
        aggregated = raw_pct.rolling(OVERLAP_WINDOW).mean()
        return aggregated.clip(0, 1).fillna(0)

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
                "slope_stability_warmup": bool(row.get("slope_stability_warmup")),
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
    except Exception as exc:
        logger.warning(
            "candle_stats_to_float_isna_failed | value_type=%s error=%s",
            type(value).__name__,
            exc,
        )
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
