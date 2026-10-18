"""Small code-level research recognition contracts, not runtime trading signals.

Definitions are ordinary Python functions. Events are immutable value records;
existing Check evidence owns their persistence and version identity.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import math
from typing import Any, Mapping

from .study import stable_hash


def utc(value: datetime | str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("recognition_invalid: timezone-aware clock required")
    return parsed.astimezone(timezone.utc)


def _name(value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("recognition_invalid: nonempty identity required")


def _positive(value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("recognition_invalid: positive finite price required")


def plain(value: Any) -> Any:
    if isinstance(value, datetime):
        return utc(value).isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    return value


@dataclass(frozen=True)
class RecognitionCandle:
    candle_id: str
    source_identity_key: str
    open_time: datetime
    close_time: datetime
    known_at: datetime
    open: float
    high: float
    low: float
    close: float

    def __post_init__(self) -> None:
        for name in (self.candle_id, self.source_identity_key):
            _name(name)
        for key in ("open_time", "close_time", "known_at"):
            object.__setattr__(self, key, utc(getattr(self, key)))
        for key in ("open", "high", "low", "close"):
            _positive(getattr(self, key))
            object.__setattr__(self, key, float(getattr(self, key)))
        if (self.close_time <= self.open_time or self.known_at < self.close_time
                or self.high < max(self.open, self.close) or self.low > min(self.open, self.close)):
            raise ValueError("recognition_invalid: malformed candle or clock")

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, source_identity_key: str | None = None) -> RecognitionCandle:
        """One material-candle conversion for canonical Check rows and visuals.

        Frozen revision/provenance custody remains on the Check input binding;
        this identity describes the exact source, clocks and OHLC consumed here.
        """
        source = source_identity_key or row.get("source_identity_key")
        if source_identity_key and row.get("source_identity_key", source_identity_key) != source_identity_key:
            raise ValueError("recognition_invalid: conflicting source identity")
        opened = utc(row.get("open_time", row.get("timestamp")))
        closed = utc(row.get("close_time"))
        known = utc(row.get("known_at"))
        prices = []
        for key in ("open", "high", "low", "close"):
            _positive(row.get(key))
            prices.append(float(row[key]))
        identity = stable_hash({"source": source, "open_time": plain(opened), "close_time": plain(closed),
                                "known_at": plain(known), "OHLC": prices})
        return cls(identity, source, opened, closed, known, *prices)


@dataclass(frozen=True)
class RecognitionContext:
    instrument_id: str
    timeframe_seconds: int
    computation_origin: datetime
    source_identity_key: str

    def __post_init__(self) -> None:
        _name(self.instrument_id)
        _name(self.source_identity_key)
        if type(self.timeframe_seconds) is not int or self.timeframe_seconds <= 0:
            raise ValueError("recognition_invalid: positive integer interval required")
        object.__setattr__(self, "computation_origin", utc(self.computation_origin))


@dataclass(frozen=True)
class RecognitionStage:
    name: str
    observed_at: datetime
    known_at: datetime
    candle_id: str

    def __post_init__(self) -> None:
        _name(self.name)
        _name(self.candle_id)
        for key in ("observed_at", "known_at"):
            object.__setattr__(self, key, utc(getattr(self, key)))
        if self.known_at < self.observed_at:
            raise ValueError("recognition_invalid: stage known before observation")


@dataclass(frozen=True)
class RecognitionLevel:
    name: str
    price: float
    observed_at: datetime
    price_known_at: datetime
    role_known_at: datetime
    candle_id: str

    def __post_init__(self) -> None:
        _name(self.name)
        _name(self.candle_id)
        _positive(self.price)
        object.__setattr__(self, "price", float(self.price))
        for key in ("observed_at", "price_known_at", "role_known_at"):
            object.__setattr__(self, key, utc(getattr(self, key)))
        if not self.observed_at <= self.price_known_at <= self.role_known_at:
            raise ValueError("recognition_invalid: reference price/role clock order")


@dataclass(frozen=True)
class SourceEventLink:
    event_id: str
    definition_id: str
    definition_version: str
    relation: str
    occurred_at: datetime
    known_at: datetime
    consumed_at: datetime

    def __post_init__(self) -> None:
        for value in (self.event_id, self.definition_id, self.definition_version, self.relation):
            _name(value)
        for key in ("occurred_at", "known_at", "consumed_at"):
            object.__setattr__(self, key, utc(getattr(self, key)))
        if not self.occurred_at <= self.known_at <= self.consumed_at:
            raise ValueError("recognition_invalid: source event unavailable when consumed")


@dataclass(frozen=True)
class RecognitionEvent:
    definition_id: str
    definition_version: str
    configuration_hash: str
    context: RecognitionContext
    kind: str
    formation_start: datetime
    occurred_at: datetime
    known_at: datetime
    stages: tuple[RecognitionStage, ...] = ()
    levels: tuple[RecognitionLevel, ...] = ()
    source_events: tuple[SourceEventLink, ...] = ()
    event_id: str = ""

    def __post_init__(self) -> None:
        for value in (self.definition_id, self.definition_version, self.configuration_hash, self.kind):
            _name(value)
        if not isinstance(self.context, RecognitionContext):
            raise ValueError("recognition_invalid: typed context required")
        for key in ("formation_start", "occurred_at", "known_at"):
            object.__setattr__(self, key, utc(getattr(self, key)))
        if not self.context.computation_origin <= self.formation_start <= self.occurred_at <= self.known_at:
            raise ValueError("recognition_invalid: event clock order")
        for key, cls in (("stages", RecognitionStage), ("levels", RecognitionLevel), ("source_events", SourceEventLink)):
            values = getattr(self, key)
            if not isinstance(values, tuple) or any(not isinstance(x, cls) for x in values):
                raise ValueError(f"recognition_invalid: immutable typed {key} required")
        if any(x.observed_at < self.context.computation_origin or x.observed_at > self.occurred_at or x.known_at > self.known_at for x in self.stages):
            raise ValueError("recognition_invalid: future stage")
        if any(x.observed_at < self.context.computation_origin or x.observed_at > self.occurred_at or x.role_known_at > self.known_at for x in self.levels):
            raise ValueError("recognition_invalid: future reference")
        if any(x.occurred_at >= self.occurred_at or x.consumed_at > self.known_at for x in self.source_events):
            raise ValueError("recognition_invalid: source event must precede occurrence and be available")
        if len({x.event_id for x in self.source_events}) != len(self.source_events):
            raise ValueError("recognition_invalid: duplicate source event")
        material = plain({k: v for k, v in asdict(self).items() if k != "event_id"})
        expected = stable_hash(material)
        if self.event_id and self.event_id != expected:
            raise ValueError("recognition_event_hash_mismatch")
        object.__setattr__(self, "event_id", expected)

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": "research.recognition_event.v1", **plain(asdict(self))}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RecognitionEvent:
        raw = dict(value)
        if raw.pop("schema_version", None) != "research.recognition_event.v1":
            raise ValueError("recognition_invalid: unsupported event schema")
        raw["context"] = RecognitionContext(**raw["context"])
        for key, item in (("stages", RecognitionStage), ("levels", RecognitionLevel), ("source_events", SourceEventLink)):
            raw[key] = tuple(item(**x) for x in raw.get(key, ()))
        return cls(**raw)


def link_source_event(event: RecognitionEvent, source: RecognitionEvent, *,
                      consumed_at: datetime, max_age_seconds: int, relation: str) -> RecognitionEvent:
    """Explicit ordinary-function composition: no implicit first/latest join.

    The caller selects the exact source and consumption clock. Maximum age is
    measured from source occurrence to consumption. Same-clock/cross-market
    chains are deliberately unsupported; no signal conversion is performed.
    """
    consumed = utc(consumed_at)
    if type(max_age_seconds) is not int or max_age_seconds <= 0:
        raise ValueError("recognition_invalid: explicit positive source age bound required")
    if (source.context != event.context or source.event_id == event.event_id
            or consumed > event.known_at or consumed < source.known_at
            or (consumed-source.occurred_at).total_seconds() > max_age_seconds):
        raise ValueError("recognition_invalid: incompatible, future or expired source event")
    link = SourceEventLink(source.event_id, source.definition_id, source.definition_version,
                           relation, source.occurred_at, source.known_at, consumed)
    return replace(event, source_events=(*event.source_events, link), event_id="")
