from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import pandas as pd
from sqlalchemy import Engine, bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from core.logger import logger
from data_providers.config.runtime import PersistenceConfig

from indicators.regime import RegimeEngineV1
from .regime_blocks import build_regime_blocks
from indicators.regime import RegimeStabilizer, default_regime_runtime_config


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
        regime_runtime = default_regime_runtime_config()
        self._stabilizer = RegimeStabilizer(regime_runtime.stabilizer)
        self._block_config = regime_runtime.blocks

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
        points: list[Dict[str, Any]] = []
        for _, candle_row in candles.iterrows():
            candle_time = candle_row["candle_time"]
            stats = stats_map.get(candle_time)
            if stats is None:
                continue
            candle_payload = {
                "open": _to_float(candle_row.get("open")),
                "high": _to_float(candle_row.get("high")),
                "low": _to_float(candle_row.get("low")),
                "close": _to_float(candle_row.get("close")),
                "volume": _to_float(candle_row.get("volume")),
                "trade_count": _to_float(candle_row.get("trade_count")),
            }
            try:
                raw_regime = self._engine_impl.classify(candle_payload, stats).as_dict()
            except ValueError as exc:
                # Skip warmup rows that lack required stats instead of failing the batch.
                logger.debug(
                    "regime_stats_skip_row | instrument_id=%s timeframe_seconds=%s candle_time=%s reason=%s",
                    instrument_id,
                    timeframe_seconds,
                    candle_time,
                    exc,
                )
                continue
            stabilized = self._stabilizer.stabilize(
                raw_regime,
                bar_time=candle_time,
                instrument_id=instrument_id,
                timeframe_seconds=timeframe_seconds,
            )
            row_idx = len(rows)
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "candle_time": candle_time,
                    "regime_version": regime_version,
                    "regime": stabilized,
                }
            )
            structure_state = (stabilized.get("structure") or {}).get("state")
            points.append(
                {
                    "idx": row_idx,
                    "time": candle_time,
                    "structure_state": structure_state,
                    "volatility_state": (stabilized.get("volatility") or {}).get("state"),
                    "liquidity_state": (stabilized.get("liquidity") or {}).get("state"),
                    "expansion_state": (stabilized.get("expansion") or {}).get("state"),
                    "confidence": stabilized.get("confidence"),
                }
            )

        if not rows:
            return rows

        blocks, block_ids = build_regime_blocks(
            points,
            timeframe_seconds=timeframe_seconds,
            config=self._block_config,
            instrument_id=instrument_id,
        )
        if blocks:
            logger.debug(
                "regime_blocks_built | instrument_id=%s timeframe_seconds=%s blocks=%s first_block_id=%s",
                instrument_id,
                timeframe_seconds,
                len(blocks),
                blocks[0].get("block_id"),
            )
            if self._engine is None:
                logger.debug(
                    "regime_blocks_persist_skipped | instrument_id=%s timeframe_seconds=%s regime_version=%s reason=engine_unavailable",
                    instrument_id,
                    timeframe_seconds,
                    regime_version,
                )
            else:
                self._upsert_blocks(
                    blocks,
                    instrument_id=instrument_id,
                    timeframe_seconds=timeframe_seconds,
                    regime_version=regime_version,
                )

        for row_idx, row in enumerate(rows):
            block_id = block_ids.get(row_idx)
            if block_id:
                row_regime = row.get("regime") or {}
                row_regime["regime_block_id"] = block_id
                row["regime"] = row_regime
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

    def _upsert_blocks(
        self,
        blocks: Iterable[Dict[str, Any]],
        *,
        instrument_id: str,
        timeframe_seconds: int,
        regime_version: str,
    ) -> None:
        if not blocks:
            return
        query = text(
            f"""
            INSERT INTO {self._config.regime_blocks_table}
                (block_id, instrument_id, timeframe_seconds, start_ts, end_ts, regime_version, block)
            VALUES (:block_id, :instrument_id, :timeframe_seconds, :start_ts, :end_ts, :regime_version, :block)
            ON CONFLICT (block_id)
            DO UPDATE SET
                computed_at = now(),
                end_ts = EXCLUDED.end_ts,
                block = EXCLUDED.block
            """
        ).bindparams(bindparam("block", type_=JSONB))
        rows = []
        for block in blocks:
            block_id = block.get("block_id")
            start_ts = block.get("start_ts")
            end_ts = block.get("end_ts")
            if not block_id or start_ts is None or end_ts is None:
                continue
            payload = _json_safe(block)
            rows.append(
                {
                    "block_id": block_id,
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "regime_version": regime_version,
                    "block": payload,
                }
            )
        if not rows:
            return
        with self._engine.begin() as conn:
            conn.execute(query, rows)
        logger.debug(
            "regime_blocks_upserted | instrument_id=%s timeframe_seconds=%s regime_version=%s blocks=%s",
            instrument_id,
            timeframe_seconds,
            regime_version,
            len(rows),
        )

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
            "regime_stats_to_float_isna_failed | value_type=%s error=%s",
            type(value).__name__,
            exc,
        )
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception as exc:
            logger.warning(
                "regime_stats_json_safe_isoformat_failed | value_type=%s error=%s",
                type(value).__name__,
                exc,
            )
            return str(value)
    return value
