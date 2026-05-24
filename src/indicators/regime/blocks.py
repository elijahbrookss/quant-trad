from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .config import RegimeBlockConfig


@dataclass
class _BlockAccumulator:
    start_idx: int
    end_idx: int
    start_time: datetime
    end_time: datetime
    structure_state: str
    trend_direction: str
    volatility_state: str
    liquidity_state: str
    expansion_state: str
    entry_confidence: Optional[float]
    min_confidence: Optional[float]
    confidence_sum: float
    confidence_count: int
    score_margin_sum: float = 0.0
    score_margin_count: int = 0
    structure_confidence_sum: float = 0.0
    structure_confidence_count: int = 0
    trust_score_sum: float = 0.0
    trust_score_count: int = 0
    last_is_known: bool = False
    last_is_mature: bool = False
    last_is_trustworthy: bool = False
    max_recent_switch_count: int = 0
    prevented_flips: int = 0

    def add_point(
        self,
        *,
        idx: int,
        time: datetime,
        confidence: Optional[float],
        score_margin: Optional[float],
        structure_confidence: Optional[float],
        trust_score: Optional[float],
        is_known: Optional[bool],
        is_mature: Optional[bool],
        is_trustworthy: Optional[bool],
        recent_switch_count: Optional[int],
    ) -> None:
        self.end_idx = idx
        self.end_time = time
        if isinstance(confidence, (int, float)):
            self.confidence_sum += float(confidence)
            self.confidence_count += 1
            if self.min_confidence is None:
                self.min_confidence = float(confidence)
            else:
                self.min_confidence = min(self.min_confidence, float(confidence))
        if isinstance(score_margin, (int, float)):
            self.score_margin_sum += float(score_margin)
            self.score_margin_count += 1
        if isinstance(structure_confidence, (int, float)):
            self.structure_confidence_sum += float(structure_confidence)
            self.structure_confidence_count += 1
        if isinstance(trust_score, (int, float)):
            self.trust_score_sum += float(trust_score)
            self.trust_score_count += 1
        if isinstance(is_known, bool):
            self.last_is_known = is_known
        if isinstance(is_mature, bool):
            self.last_is_mature = is_mature
        if isinstance(is_trustworthy, bool):
            self.last_is_trustworthy = is_trustworthy
        if isinstance(recent_switch_count, int):
            self.max_recent_switch_count = max(self.max_recent_switch_count, recent_switch_count)

    def avg_confidence(self) -> Optional[float]:
        if self.confidence_count <= 0:
            return None
        return self.confidence_sum / self.confidence_count

    def avg_score_margin(self) -> Optional[float]:
        if self.score_margin_count <= 0:
            return None
        return self.score_margin_sum / self.score_margin_count

    def avg_structure_confidence(self) -> Optional[float]:
        if self.structure_confidence_count <= 0:
            return None
        return self.structure_confidence_sum / self.structure_confidence_count

    def avg_trust_score(self) -> Optional[float]:
        if self.trust_score_count <= 0:
            return None
        return self.trust_score_sum / self.trust_score_count


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
        return [], {}

    accumulators: List[_BlockAccumulator] = []
    for idx, point in enumerate(points):
        time = point.get("time")
        if not isinstance(time, datetime):
            continue
        structure_state = str(point.get("structure_state") or "").strip().lower()
        if not structure_state:
            continue
        trend_direction = _normalized_trend_direction(
            point.get("trend_direction"),
            structure_state=structure_state,
        )
        volatility_state = str(point.get("volatility_state") or "").strip().lower() or "unknown"
        liquidity_state = str(point.get("liquidity_state") or "").strip().lower() or "unknown"
        expansion_state = str(point.get("expansion_state") or "").strip().lower() or "unknown"
        confidence = point.get("confidence")
        score_margin = point.get("score_margin")
        structure_confidence = point.get("structure_confidence")
        trust_score = point.get("trust_score")
        is_known = point.get("is_known")
        is_mature = point.get("is_mature")
        is_trustworthy = point.get("is_trustworthy")
        recent_switch_count = point.get("recent_switch_count")

        if (
            not accumulators
            or accumulators[-1].structure_state != structure_state
            or accumulators[-1].trend_direction != trend_direction
        ):
            accumulators.append(
                _BlockAccumulator(
                    start_idx=idx,
                    end_idx=idx,
                    start_time=time,
                    end_time=time,
                    structure_state=structure_state,
                    trend_direction=trend_direction,
                    volatility_state=volatility_state,
                    liquidity_state=liquidity_state,
                    expansion_state=expansion_state,
                    entry_confidence=float(confidence) if isinstance(confidence, (int, float)) else None,
                    min_confidence=float(confidence) if isinstance(confidence, (int, float)) else None,
                    confidence_sum=float(confidence) if isinstance(confidence, (int, float)) else 0.0,
                    confidence_count=1 if isinstance(confidence, (int, float)) else 0,
                    score_margin_sum=float(score_margin) if isinstance(score_margin, (int, float)) else 0.0,
                    score_margin_count=1 if isinstance(score_margin, (int, float)) else 0,
                    structure_confidence_sum=(
                        float(structure_confidence) if isinstance(structure_confidence, (int, float)) else 0.0
                    ),
                    structure_confidence_count=1 if isinstance(structure_confidence, (int, float)) else 0,
                    trust_score_sum=float(trust_score) if isinstance(trust_score, (int, float)) else 0.0,
                    trust_score_count=1 if isinstance(trust_score, (int, float)) else 0,
                    last_is_known=bool(is_known),
                    last_is_mature=bool(is_mature),
                    last_is_trustworthy=bool(is_trustworthy),
                    max_recent_switch_count=int(recent_switch_count)
                    if isinstance(recent_switch_count, int)
                    else 0,
                )
            )
        else:
            accumulators[-1].add_point(
                idx=idx,
                time=time,
                confidence=confidence,
                score_margin=score_margin,
                structure_confidence=structure_confidence,
                trust_score=trust_score,
                is_known=is_known,
                is_mature=is_mature,
                is_trustworthy=is_trustworthy,
                recent_switch_count=recent_switch_count if isinstance(recent_switch_count, int) else None,
            )

    merged: List[_BlockAccumulator] = []
    for block in accumulators:
        block_len = block.end_idx - block.start_idx + 1
        if block_len >= min_block_bars or not merged:
            merged.append(block)
            continue
        prev = merged[-1]
        _merge_block(prev, block)
        prev.prevented_flips += 1

    consolidated: List[_BlockAccumulator] = []
    for block in merged:
        if not consolidated:
            consolidated.append(block)
            continue
        prev = consolidated[-1]
        if prev.structure_state == block.structure_state and prev.trend_direction == block.trend_direction:
            _merge_block(prev, block)
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
            structure_state=block.structure_state,
            trend_direction=block.trend_direction,
        )
        regime_key = _regime_key(
            block.structure_state,
            block.volatility_state,
            block.liquidity_state,
            block.expansion_state,
        )
        avg_confidence = block.avg_confidence()
        avg_score_margin = block.avg_score_margin()
        avg_structure_confidence = block.avg_structure_confidence()
        avg_trust_score = block.avg_trust_score()
        bars = block.end_idx - block.start_idx + 1
        known_at = _known_at(block.start_time, timeframe_seconds, min_block_bars)
        block_payload = {
            "block_id": block_id,
            "start_ts": block.start_time,
            "end_ts": block.end_time,
            "known_at": known_at,
            "primary_state": block.structure_state,
            "structure_state": block.structure_state,
            "trend_direction": block.trend_direction,
            "volatility_state": block.volatility_state,
            "liquidity_state": block.liquidity_state,
            "expansion_state": block.expansion_state,
            "entry_confidence": block.entry_confidence,
            "avg_confidence": avg_confidence,
            "avg_structure_confidence": avg_structure_confidence,
            "avg_trust_score": avg_trust_score,
            "avg_score_margin": avg_score_margin,
            "min_confidence": block.min_confidence,
            "bars": bars,
            "regime_key": regime_key,
            "is_known": block.last_is_known,
            "is_mature": block.last_is_mature,
            "is_trustworthy": block.last_is_trustworthy,
            "recent_switch_count": block.max_recent_switch_count,
            "prevented_flips_count": block.prevented_flips,
        }
        blocks.append(block_payload)
        for idx in range(block.start_idx, block.end_idx + 1):
            block_ids[idx] = block_id
    return blocks, block_ids


def _merge_block(prev: _BlockAccumulator, block: _BlockAccumulator) -> None:
    prev.end_idx = block.end_idx
    prev.end_time = block.end_time
    prev.confidence_sum += block.confidence_sum
    prev.confidence_count += block.confidence_count
    prev.score_margin_sum += block.score_margin_sum
    prev.score_margin_count += block.score_margin_count
    prev.structure_confidence_sum += block.structure_confidence_sum
    prev.structure_confidence_count += block.structure_confidence_count
    prev.trust_score_sum += block.trust_score_sum
    prev.trust_score_count += block.trust_score_count
    prev.last_is_known = block.last_is_known
    prev.last_is_mature = block.last_is_mature
    prev.last_is_trustworthy = block.last_is_trustworthy
    prev.max_recent_switch_count = max(prev.max_recent_switch_count, block.max_recent_switch_count)
    if block.min_confidence is not None:
        if prev.min_confidence is None:
            prev.min_confidence = block.min_confidence
        else:
            prev.min_confidence = min(prev.min_confidence, block.min_confidence)


def _normalized_trend_direction(value: Any, *, structure_state: str) -> str:
    normalized_state = str(structure_state or "").strip().lower()
    if normalized_state.endswith("_up"):
        return "up"
    if normalized_state.endswith("_down"):
        return "down"
    if normalized_state in {"range", "transition_neutral"}:
        return "neutral"
    if normalized_state not in {"trend", "transition"}:
        return "neutral"
    direction = str(value or "neutral").strip().lower()
    if direction in {"up", "down"}:
        return direction
    return "neutral"


def _block_id(
    start_time: datetime,
    *,
    instrument_id: Optional[str],
    timeframe_seconds: int,
    structure_state: str,
    trend_direction: str,
) -> str:
    base = f"{start_time.isoformat()}_{timeframe_seconds}_{structure_state}_{trend_direction}"
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


__all__ = ["RegimeBlockConfig", "build_regime_blocks"]
