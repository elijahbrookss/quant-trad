"""Canonical runtime contracts for indicator-state driven bot execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, MutableMapping, Protocol, Sequence

INDICATOR_SNAPSHOT_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class IndicatorStateSnapshot:
    revision: int
    known_at: datetime
    formed_at: datetime
    source_timeframe: str
    payload: Mapping[str, Any]
    schema_version: int = INDICATOR_SNAPSHOT_SCHEMA_VERSION


@dataclass(frozen=True)
class SignalEvaluationInput:
    snapshots: Mapping[str, IndicatorStateSnapshot]


@dataclass(frozen=True)
class OverlayProjectionInput:
    snapshot: IndicatorStateSnapshot
    previous_projection_state: Mapping[str, Any]


@dataclass(frozen=True)
class ProjectionDelta:
    seq: int
    base_seq: int
    ops: Sequence[Mapping[str, Any]]
    authoritative_snapshot: bool = False


@dataclass(frozen=True)
class IndicatorStateDelta:
    changed: bool
    revision: int
    known_at: datetime


class IndicatorStateEngine(Protocol):
    def initialize(self, window_context: Mapping[str, Any]) -> MutableMapping[str, Any]:
        ...

    def apply_bar(self, state: MutableMapping[str, Any], bar: Any) -> IndicatorStateDelta:
        ...

    def snapshot(self, state: Mapping[str, Any]) -> IndicatorStateSnapshot:
        ...
