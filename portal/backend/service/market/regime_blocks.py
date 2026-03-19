from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import logging

from indicators.regime import RegimeBlockConfig

logger = logging.getLogger(__name__)


@dataclass
class _BlockAccumulator:
    start_idx: int
    end_idx: int
    start_time: datetime
    end_time: datetime
    structure_state: str
    volatility_state: str
    liquidity_state: str
    expansion_state: str
    entry_confidence: Optional[float]
    min_confidence: Optional[float]
    confidence_sum: float
    confidence_count: int
    prevented_flips: int = 0

    def add_point(self, idx: int, time: datetime, confidence: Optional[float]) -> None:
        self.end_idx = idx
        self.end_time = time
        if isinstance(confidence, (int, float)):
            self.confidence_sum += float(confidence)
            self.confidence_count += 1
            if self.min_confidence is None:
                self.min_confidence = float(confidence)
            else:
                self.min_confidence = min(self.min_confidence, float(confidence))

    def avg_confidence(self) -> Optional[float]:
        if self.confidence_count <= 0:
            return None
        return self.confidence_sum / self.confidence_count


def build_regime_blocks(
    points: Sequence[Mapping[str, Any]],
    *,
    timeframe_seconds: int,
    config: Optional[RegimeBlockConfig] = None,
    instrument_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
    cfg = config or RegimeBlockConfig()
    min_block_bars = max(1, cfg.min_block_bars)
    if not points:
        logger.debug(
            "regime_blocks_empty | instrument_id=%s timeframe_seconds=%s",
            instrument_id,
            timeframe_seconds,
        )
        return [], {}

    accumulators: List[_BlockAccumulator] = []
    for idx, point in enumerate(points):
        time = point.get("time")
        if not isinstance(time, datetime):
            continue
        structure_state = (point.get("structure_state") or "").strip().lower()
        if not structure_state:
            continue
        volatility_state = (point.get("volatility_state") or "").strip().lower() or "unknown"
        liquidity_state = (point.get("liquidity_state") or "").strip().lower() or "unknown"
        expansion_state = (point.get("expansion_state") or "").strip().lower() or "unknown"
        confidence = point.get("confidence")

        if not accumulators or accumulators[-1].structure_state != structure_state:
            accumulators.append(
                _BlockAccumulator(
                    start_idx=idx,
                    end_idx=idx,
                    start_time=time,
                    end_time=time,
                    structure_state=structure_state,
                    volatility_state=volatility_state,
                    liquidity_state=liquidity_state,
                    expansion_state=expansion_state,
                    entry_confidence=float(confidence) if isinstance(confidence, (int, float)) else None,
                    min_confidence=float(confidence) if isinstance(confidence, (int, float)) else None,
                    confidence_sum=float(confidence) if isinstance(confidence, (int, float)) else 0.0,
                    confidence_count=1 if isinstance(confidence, (int, float)) else 0,
                )
            )
        else:
            accumulators[-1].add_point(idx, time, confidence)

    merged: List[_BlockAccumulator] = []
    for block in accumulators:
        block_len = block.end_idx - block.start_idx + 1
        if block_len >= min_block_bars or not merged:
            merged.append(block)
            continue
        prev = merged[-1]
        prev.end_idx = block.end_idx
        prev.end_time = block.end_time
        prev.confidence_sum += block.confidence_sum
        prev.confidence_count += block.confidence_count
        if block.min_confidence is not None:
            if prev.min_confidence is None:
                prev.min_confidence = block.min_confidence
            else:
                prev.min_confidence = min(prev.min_confidence, block.min_confidence)
        prev.prevented_flips += 1

    consolidated: List[_BlockAccumulator] = []
    for block in merged:
        if not consolidated:
            consolidated.append(block)
            continue
        prev = consolidated[-1]
        if prev.structure_state == block.structure_state:
            prev.end_idx = block.end_idx
            prev.end_time = block.end_time
            prev.confidence_sum += block.confidence_sum
            prev.confidence_count += block.confidence_count
            if block.min_confidence is not None:
                if prev.min_confidence is None:
                    prev.min_confidence = block.min_confidence
                else:
                    prev.min_confidence = min(prev.min_confidence, block.min_confidence)
            prev.prevented_flips += block.prevented_flips
            continue
        consolidated.append(block)

    blocks: List[Dict[str, Any]] = []
    block_ids: Dict[int, str] = {}
    for block in consolidated:
        block_id = _block_id(
            block.start_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )
        regime_key = _regime_key(
            block.structure_state,
            block.volatility_state,
            block.liquidity_state,
            block.expansion_state,
        )
        avg_confidence = block.avg_confidence()
        bars = block.end_idx - block.start_idx + 1
        known_at = _known_at(block.start_time, timeframe_seconds, min_block_bars)
        block_payload = {
            "block_id": block_id,
            "start_ts": block.start_time,
            "end_ts": block.end_time,
            "known_at": known_at,
            "primary_state": block.structure_state,
            "structure_state": block.structure_state,
            "volatility_state": block.volatility_state,
            "liquidity_state": block.liquidity_state,
            "expansion_state": block.expansion_state,
            "entry_confidence": block.entry_confidence,
            "avg_confidence": avg_confidence,
            "min_confidence": block.min_confidence,
            "bars": bars,
            "regime_key": regime_key,
            "prevented_flips_count": block.prevented_flips,
        }
        blocks.append(block_payload)
        for idx in range(block.start_idx, block.end_idx + 1):
            block_ids[idx] = block_id
    logger.debug(
        "regime_blocks_built | instrument_id=%s timeframe_seconds=%s blocks=%s min_block_bars=%s",
        instrument_id,
        timeframe_seconds,
        len(blocks),
        min_block_bars,
    )
    return blocks, block_ids


def _block_id(start_time: datetime, *, instrument_id: Optional[str], timeframe_seconds: int) -> str:
    base = f"{start_time.isoformat()}_{timeframe_seconds}"
    if instrument_id:
        return f"regime_block:{instrument_id}:{base}"
    return f"regime_block:{base}"


def _regime_key(structure: str, volatility: str, liquidity: str, expansion: str) -> str:
    return "|".join([structure, volatility, liquidity, expansion])


def _known_at(start_time: datetime, timeframe_seconds: int, min_block_bars: int) -> Optional[datetime]:
    if timeframe_seconds <= 0:
        return start_time
    if min_block_bars <= 1:
        return start_time
    offset = timeframe_seconds * (min_block_bars - 1)
    return start_time + timedelta(seconds=offset)


__all__ = [
    "RegimeBlockConfig",
    "build_regime_blocks",
]
