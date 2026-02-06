from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from data_providers.config.runtime import PersistenceConfig, runtime_config_from_env

from .regime_blocks import build_regime_blocks
from .regime_config import RegimeBlockConfig
from .stats_queue import REGIME_VERSION


class RegimeBlocksService:
    def __init__(
        self,
        *,
        engine: Optional[Engine] = None,
        config: Optional[PersistenceConfig] = None,
        block_config: Optional[RegimeBlockConfig] = None,
    ) -> None:
        self._config = config or runtime_config_from_env().persistence
        self._engine = engine or (create_engine(self._config.dsn) if self._config.dsn else None)
        self._block_config = block_config or RegimeBlockConfig()

    def fetch_blocks(
        self,
        *,
        instrument_id: str,
        timeframe_seconds: int,
        start: datetime,
        end: datetime,
        regime_version: str = REGIME_VERSION,
    ) -> List[Dict[str, Any]]:
        if not self._engine:
            return []
        query = text(
            f"""
            SELECT candle_time, regime
            FROM {self._config.regime_stats_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND regime_version = :regime_version
              AND candle_time BETWEEN :start AND :end
            ORDER BY candle_time
            """
        )
        with self._engine.begin() as conn:
            result = conn.execute(
                query,
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "regime_version": regime_version,
                    "start": start,
                    "end": end,
                },
            )
            rows = result.mappings().all()

        points: List[Dict[str, Any]] = []
        for row in rows:
            candle_time = row.get("candle_time")
            regime = row.get("regime")
            if isinstance(regime, str):
                try:
                    regime = json.loads(regime)
                except ValueError:
                    regime = {}
            if not isinstance(regime, dict):
                continue
            structure = regime.get("structure") or {}
            volatility = regime.get("volatility") or {}
            liquidity = regime.get("liquidity") or {}
            expansion = regime.get("expansion") or {}
            points.append(
                {
                    "time": candle_time,
                    "structure_state": structure.get("state"),
                    "volatility_state": volatility.get("state"),
                    "liquidity_state": liquidity.get("state"),
                    "expansion_state": expansion.get("state"),
                    "confidence": regime.get("confidence"),
                }
            )

        if not points:
            return []
        blocks, _ = build_regime_blocks(
            points,
            timeframe_seconds=timeframe_seconds,
            config=self._block_config,
            instrument_id=instrument_id,
        )
        return [
            block
            for block in blocks
            if block.get("start_ts") <= end and block.get("end_ts") >= start
        ]


__all__ = ["RegimeBlocksService"]
