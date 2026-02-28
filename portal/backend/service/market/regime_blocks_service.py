from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from data_providers.config.runtime import PersistenceConfig, runtime_config_from_env

from .stats_contract import REGIME_VERSION


class RegimeBlocksService:
    def __init__(
        self,
        *,
        engine: Optional[Engine] = None,
        config: Optional[PersistenceConfig] = None,
    ) -> None:
        self._config = config or runtime_config_from_env().persistence
        self._engine = engine or (create_engine(self._config.dsn) if self._config.dsn else None)

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
            SELECT block
            FROM {self._config.regime_blocks_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND regime_version = :regime_version
              AND end_ts >= :start
              AND start_ts <= :end
            ORDER BY start_ts
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

        blocks: List[Dict[str, Any]] = []
        for row in rows:
            block = row.get("block")
            if isinstance(block, str):
                try:
                    block = json.loads(block)
                except ValueError:
                    block = {}
            if not isinstance(block, dict):
                continue
            blocks.append(block)

        return blocks


__all__ = ["RegimeBlocksService"]
