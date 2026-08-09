"""Generic causal fact alignment for frozen research inputs."""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from .study import TemporalJoinSpec, stable_hash


def _utc(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(frozen=True)
class FrozenFact:
    """One normalized fact envelope supplied from an already-frozen dataset."""

    fact_key: str
    event_time: datetime
    known_at: datetime
    sample_time: datetime
    value: Any
    evidence_hash: str

    def __post_init__(self) -> None:
        fact_key = str(self.fact_key or "").strip()
        evidence_hash = str(self.evidence_hash or "").strip()
        if not fact_key or not evidence_hash:
            raise ValueError("frozen fact identity and evidence hash are required")
        event_time = _utc(self.event_time, field="frozen_fact.event_time")
        sample_time = _utc(self.sample_time, field="frozen_fact.sample_time")
        known_at = _utc(self.known_at, field="frozen_fact.known_at")
        if event_time > known_at or sample_time > known_at:
            raise ValueError("frozen fact source time exceeds known_at")
        object.__setattr__(self, "fact_key", fact_key)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "sample_time", sample_time)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "evidence_hash", evidence_hash)


@dataclass(frozen=True)
class ResearchFrame:
    """One primary observation and its causally joined context."""

    primary_fact_key: str
    primary_time: datetime
    decision_time: datetime
    facts: Mapping[str, Any]
    evidence_hashes: tuple[str, ...]
    frame_hash: str = ""

    def __post_init__(self) -> None:
        key = str(self.primary_fact_key or "").strip()
        if not key:
            raise ValueError("research frame primary fact key is required")
        primary_time = _utc(self.primary_time, field="frame.primary_time")
        decision_time = _utc(self.decision_time, field="frame.decision_time")
        if decision_time < primary_time:
            raise ValueError("research frame decision time precedes primary time")
        facts = dict(self.facts or {})
        if key not in facts:
            raise ValueError("research frame omits its primary fact")
        hashes = tuple(sorted({str(value) for value in self.evidence_hashes if str(value)}))
        if not hashes:
            raise ValueError("research frame requires evidence hashes")
        object.__setattr__(self, "primary_fact_key", key)
        object.__setattr__(self, "primary_time", primary_time)
        object.__setattr__(self, "decision_time", decision_time)
        object.__setattr__(self, "facts", facts)
        object.__setattr__(self, "evidence_hashes", hashes)
        expected = stable_hash(self._material())
        if self.frame_hash and self.frame_hash != expected:
            raise ValueError("research_frame_hash_mismatch")
        object.__setattr__(self, "frame_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if key != "frame_hash"
        }


def primary_frames(
    facts: Sequence[FrozenFact],
    *,
    primary_fact_key: str,
) -> tuple[ResearchFrame, ...]:
    """Create ordered frames from facts whose known-at is their decision clock."""

    key = str(primary_fact_key or "").strip()
    selected = sorted(
        (fact for fact in facts if fact.fact_key == key),
        key=lambda fact: (
            fact.event_time,
            fact.known_at,
            fact.evidence_hash,
        ),
    )
    if len({fact.event_time for fact in selected}) != len(selected):
        raise ValueError("primary facts contain duplicate event times")
    return tuple(
        ResearchFrame(
            primary_fact_key=key,
            primary_time=fact.event_time,
            decision_time=fact.known_at,
            facts={key: fact.value},
            evidence_hashes=(fact.evidence_hash,),
        )
        for fact in selected
    )


def apply_temporal_joins(
    frames: Sequence[ResearchFrame],
    *,
    facts_by_key: Mapping[str, Sequence[FrozenFact]],
    joins: Sequence[TemporalJoinSpec],
) -> tuple[ResearchFrame, ...]:
    """Apply declared as-of joins without ever selecting future-known samples."""

    current = tuple(frames)
    for join in joins:
        right = tuple(
            sorted(
                facts_by_key.get(join.right_fact_key, ()),
                key=lambda fact: (
                    fact.known_at,
                    fact.sample_time,
                    fact.event_time,
                    fact.evidence_hash,
                ),
            )
        )
        if any(fact.fact_key != join.right_fact_key for fact in right):
            raise ValueError("temporal join received facts under the wrong key")
        known_times = [fact.known_at for fact in right]
        joined: list[ResearchFrame] = []
        for frame in current:
            if join.left_fact_key not in frame.facts:
                raise ValueError("temporal join left fact is absent from frame")
            index = bisect_right(known_times, frame.decision_time) - 1
            selected = right[index] if index >= 0 else None
            if selected is not None and (
                selected.known_at > frame.decision_time
                or selected.sample_time > frame.decision_time
                or selected.event_time > frame.decision_time
            ):
                raise RuntimeError("temporal join selected a future fact")
            if selected is None:
                if join.missing_policy == "reject_frame":
                    raise ValueError(
                        "temporal join has no causal fact: "
                        f"right_fact_key={join.right_fact_key} "
                        f"decision_time={frame.decision_time.isoformat()}"
                    )
                if join.missing_policy == "exclude_frame":
                    continue
                value = None
                hashes = frame.evidence_hashes
            else:
                value = selected.value
                hashes = (*frame.evidence_hashes, selected.evidence_hash)
            joined.append(
                ResearchFrame(
                    primary_fact_key=frame.primary_fact_key,
                    primary_time=frame.primary_time,
                    decision_time=frame.decision_time,
                    facts={**frame.facts, join.output_key: value},
                    evidence_hashes=hashes,
                )
            )
        current = tuple(joined)
    return current


__all__ = [
    "FrozenFact",
    "ResearchFrame",
    "apply_temporal_joins",
    "primary_frames",
]
