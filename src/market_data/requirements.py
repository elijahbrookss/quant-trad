"""Pure market-data requirement resolution and causal consumer binding.

Consumers declare facts and instrument relationships. They never select provider
endpoints, storage tables, collector schedules, or fallback sources here.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from .contracts import (
    CandleRecord,
    InstrumentRole,
    MarketDataAlignment,
    MarketDataRecord,
    MarketDataRequirement,
    NumericFactRecord,
    NumericFactState,
    OpenInterestRecord,
)


RESOLVED_MARKET_DATA_PLAN_VERSION = "resolved_market_data_plan.v1"


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"market_data_plan_invalid: {field} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"market_data_plan_invalid: {field} must be ISO-8601"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class InstrumentResolutionContext:
    """Explicit canonical IDs available to an immutable run or check plan."""

    primary_instrument_ids: tuple[str, ...]
    underlying_by_primary: Mapping[str, str] = field(default_factory=dict)
    benchmarks: Mapping[str, str] = field(default_factory=dict)
    known_instrument_ids: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        primary = tuple(
            dict.fromkeys(
                str(value).strip()
                for value in self.primary_instrument_ids
                if str(value).strip()
            )
        )
        if not primary:
            raise ValueError(
                "market_data_instrument_resolution_invalid: at least one primary instrument is required"
            )
        underlying = {
            str(key).strip(): str(value).strip()
            for key, value in dict(self.underlying_by_primary or {}).items()
            if str(key).strip() and str(value).strip()
        }
        benchmarks = {
            str(key).strip(): str(value).strip()
            for key, value in dict(self.benchmarks or {}).items()
            if str(key).strip() and str(value).strip()
        }
        known = frozenset(
            str(value).strip()
            for value in self.known_instrument_ids
            if str(value).strip()
        )
        if known:
            selected = set(primary) | set(underlying.values()) | set(benchmarks.values())
            unknown = sorted(selected - known)
            if unknown:
                raise ValueError(
                    "market_data_instrument_resolution_invalid: unknown canonical instrument IDs "
                    + ", ".join(unknown)
                )
        object.__setattr__(self, "primary_instrument_ids", primary)
        object.__setattr__(self, "underlying_by_primary", underlying)
        object.__setattr__(self, "benchmarks", benchmarks)
        object.__setattr__(self, "known_instrument_ids", known)


@dataclass(frozen=True)
class ResolvedMarketDataRequirement:
    consumer_id: str
    requirement: MarketDataRequirement
    instrument_id: str
    primary_instrument_id: Optional[str]

    def __post_init__(self) -> None:
        consumer_id = str(self.consumer_id or "").strip()
        instrument_id = str(self.instrument_id or "").strip()
        primary_id = str(self.primary_instrument_id or "").strip() or None
        if not consumer_id or not instrument_id:
            raise ValueError(
                "market_data_plan_invalid: consumer_id and instrument_id are required"
            )
        object.__setattr__(self, "consumer_id", consumer_id)
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "primary_instrument_id", primary_id)

    @property
    def series_key(
        self,
    ) -> tuple[str, str, str, Optional[int], tuple[tuple[str, str], ...]]:
        return (
            self.instrument_id,
            self.requirement.fact_type,
            str(self.requirement.contract_version),
            self.requirement.timeframe_seconds,
            tuple(sorted(dict(self.requirement.dimensions).items())),
        )

    @property
    def binding_key(self) -> tuple[str, str, Optional[str]]:
        return (
            self.consumer_id,
            self.requirement.key,
            self.primary_instrument_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "consumer_id": self.consumer_id,
            "input": self.requirement.to_dict(),
            "instrument_id": self.instrument_id,
            "primary_instrument_id": self.primary_instrument_id,
        }


@dataclass(frozen=True)
class ResolvedMarketDataSeries:
    instrument_id: str
    fact_type: str
    contract_version: str
    timeframe_seconds: Optional[int]
    dimensions: Mapping[str, str]
    bindings: tuple[ResolvedMarketDataRequirement, ...]
    required: bool
    allow_gaps: bool
    alignment: MarketDataAlignment
    max_staleness_seconds: Optional[int]
    required_fields: tuple[str, ...]
    lookback_bars: Optional[int]
    lookback_seconds: Optional[int]

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "instrument_id": self.instrument_id,
            "fact_type": self.fact_type,
            "contract_version": self.contract_version,
            "timeframe_seconds": self.timeframe_seconds,
            "required": self.required,
            "allow_gaps": self.allow_gaps,
            "alignment": self.alignment.value,
            "max_staleness_seconds": self.max_staleness_seconds,
            "required_fields": list(self.required_fields),
            "lookback_bars": self.lookback_bars,
            "lookback_seconds": self.lookback_seconds,
            "bindings": [binding.to_dict() for binding in self.bindings],
        }
        if self.dimensions:
            payload["dimensions"] = dict(self.dimensions)
        return payload


@dataclass(frozen=True)
class ResolvedMarketDataPlan:
    series: tuple[ResolvedMarketDataSeries, ...]
    schema_version: str = RESOLVED_MARKET_DATA_PLAN_VERSION

    def __post_init__(self) -> None:
        if not self.series:
            raise ValueError("market_data_plan_invalid: at least one series is required")

    @property
    def plan_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": self.schema_version,
                "series": [series.to_dict() for series in self.series],
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "plan_hash": self.plan_hash,
            "series": [series.to_dict() for series in self.series],
        }


class MarketDataPlanResolver:
    """Resolve typed requirements without inferring symbols or provider fallbacks."""

    @staticmethod
    def _instrument_selections(
        requirement: MarketDataRequirement,
        context: InstrumentResolutionContext,
    ) -> list[tuple[str, Optional[str]]]:
        role = requirement.instrument_role
        if role is InstrumentRole.PRIMARY:
            return [(instrument_id, instrument_id) for instrument_id in context.primary_instrument_ids]
        if role is InstrumentRole.UNDERLYING:
            resolved: list[tuple[str, Optional[str]]] = []
            relation_name = requirement.instrument_ref or "underlying"
            if relation_name != "underlying":
                raise ValueError(
                    "market_data_instrument_resolution_invalid: only the canonical underlying relation is supported"
                )
            for primary_id in context.primary_instrument_ids:
                target = str(context.underlying_by_primary.get(primary_id) or "").strip()
                if not target:
                    raise ValueError(
                        "market_data_instrument_resolution_missing: "
                        f"role=underlying primary_instrument_id={primary_id}"
                    )
                resolved.append((target, primary_id))
            return resolved
        if role is InstrumentRole.BENCHMARK:
            alias = str(requirement.instrument_ref or "").strip()
            target = str(context.benchmarks.get(alias) or "").strip()
            if not target:
                raise ValueError(
                    "market_data_instrument_resolution_missing: "
                    f"role=benchmark alias={alias or '<missing>'}"
                )
            return [(target, None)]
        if role is InstrumentRole.EXPLICIT:
            target = str(requirement.instrument_ref or "").strip()
            if context.known_instrument_ids and target not in context.known_instrument_ids:
                raise ValueError(
                    "market_data_instrument_resolution_invalid: "
                    f"explicit instrument_id={target} is not canonical"
                )
            return [(target, None)]
        raise AssertionError(f"unhandled instrument role {role}")

    def resolve(
        self,
        declarations: Sequence[tuple[str, Sequence[MarketDataRequirement]]],
        *,
        instruments: InstrumentResolutionContext,
    ) -> ResolvedMarketDataPlan:
        resolved: list[ResolvedMarketDataRequirement] = []
        binding_keys: set[tuple[str, str, Optional[str]]] = set()
        for consumer_id, requirements in declarations:
            normalized_consumer = str(consumer_id or "").strip()
            if not normalized_consumer:
                raise ValueError("market_data_plan_invalid: consumer_id is required")
            local_keys: set[str] = set()
            for requirement in requirements:
                if requirement.key in local_keys:
                    raise ValueError(
                        "market_data_plan_invalid: duplicate input key "
                        f"consumer_id={normalized_consumer} key={requirement.key}"
                    )
                local_keys.add(requirement.key)
                for instrument_id, primary_id in self._instrument_selections(
                    requirement, instruments
                ):
                    binding = ResolvedMarketDataRequirement(
                        consumer_id=normalized_consumer,
                        requirement=requirement,
                        instrument_id=instrument_id,
                        primary_instrument_id=primary_id,
                    )
                    if binding.binding_key in binding_keys:
                        raise ValueError(
                            "market_data_plan_invalid: duplicate resolved binding "
                            f"consumer_id={normalized_consumer} key={requirement.key}"
                        )
                    binding_keys.add(binding.binding_key)
                    resolved.append(binding)

        grouped: dict[
            tuple[
                str,
                str,
                str,
                Optional[int],
                tuple[tuple[str, str], ...],
            ],
            list[ResolvedMarketDataRequirement],
        ] = defaultdict(list)
        for binding in resolved:
            grouped[binding.series_key].append(binding)
        series: list[ResolvedMarketDataSeries] = []
        for key in sorted(
            grouped,
            key=lambda value: (
                value[0],
                value[1],
                value[2],
                value[3] if value[3] is not None else -1,
                value[4],
            ),
        ):
            bindings = sorted(grouped[key], key=lambda item: item.binding_key)
            alignments = {item.requirement.alignment for item in bindings}
            if len(alignments) != 1:
                raise ValueError(
                    "market_data_plan_invalid: one series cannot use conflicting alignment policies"
                )
            staleness = [
                int(item.requirement.max_staleness_seconds)
                for item in bindings
                if item.requirement.max_staleness_seconds is not None
            ]
            lookback_bars = [
                int(item.requirement.lookback_bars)
                for item in bindings
                if item.requirement.lookback_bars is not None
            ]
            lookback_seconds = [
                int(item.requirement.lookback_seconds)
                for item in bindings
                if item.requirement.lookback_seconds is not None
            ]
            series.append(
                ResolvedMarketDataSeries(
                    instrument_id=key[0],
                    fact_type=key[1],
                    contract_version=key[2],
                    timeframe_seconds=key[3],
                    dimensions=dict(key[4]),
                    bindings=tuple(bindings),
                    required=any(item.requirement.required for item in bindings),
                    allow_gaps=all(item.requirement.allow_gaps for item in bindings),
                    alignment=next(iter(alignments)),
                    max_staleness_seconds=min(staleness) if staleness else None,
                    required_fields=tuple(
                        sorted(
                            {
                                field
                                for item in bindings
                                for field in item.requirement.required_fields
                            }
                        )
                    ),
                    lookback_bars=max(lookback_bars) if lookback_bars else None,
                    lookback_seconds=(
                        max(lookback_seconds) if lookback_seconds else None
                    ),
                )
            )
        return ResolvedMarketDataPlan(series=tuple(series))


@dataclass(frozen=True)
class UnavailableMarketData:
    key: str
    reason: str
    evaluation_time: datetime
    details: Mapping[str, Any]

    @property
    def available(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": False,
            "key": self.key,
            "reason": self.reason,
            "evaluation_time": _iso(self.evaluation_time),
            "details": dict(self.details),
        }


def _known_at(record: MarketDataRecord) -> datetime:
    return record.fact.known_at


def latest_known_record(
    records: Iterable[MarketDataRecord],
    *,
    evaluation_time: Any,
    max_staleness_seconds: int,
) -> MarketDataRecord | UnavailableMarketData:
    """Select only facts causally available by the decision time."""

    evaluation = _utc(evaluation_time, field="evaluation_time")
    staleness = int(max_staleness_seconds)
    if staleness <= 0:
        raise ValueError("market_data_alignment_invalid: max staleness must be positive")
    visible = [record for record in records if _known_at(record) <= evaluation]
    if not visible:
        return UnavailableMarketData(
            key="",
            reason="no_known_fact",
            evaluation_time=evaluation,
            details={},
        )
    latest = max(
        visible,
        key=lambda record: (_known_at(record), int(record.market_commit_seq)),
    )
    age = evaluation - _known_at(latest)
    if age > timedelta(seconds=staleness):
        return UnavailableMarketData(
            key="",
            reason="stale",
            evaluation_time=evaluation,
            details={
                "known_at": _iso(_known_at(latest)),
                "age_seconds": age.total_seconds(),
                "max_staleness_seconds": staleness,
            },
        )
    return latest


def causal_numeric_fact_records(
    records: Iterable[NumericFactRecord],
    *,
    evaluation_time: Any,
) -> tuple[NumericFactRecord, ...]:
    """Return the active revision of each source event known by one decision time."""

    evaluation = _utc(evaluation_time, field="evaluation_time")
    latest_by_event: dict[str, NumericFactRecord] = {}
    for record in records:
        if record.fact.known_at > evaluation:
            continue
        event_key = record.fact.source_event_key
        current = latest_by_event.get(event_key)
        if current is None or (
            int(record.revision), int(record.market_commit_seq)
        ) > (
            int(current.revision), int(current.market_commit_seq)
        ):
            latest_by_event[event_key] = record
    return tuple(
        sorted(
            (
                record
                for record in latest_by_event.values()
                if record.fact.state is NumericFactState.ACTIVE
            ),
            key=lambda record: (
                record.fact.effective_at,
                record.fact.source_event_key,
            ),
        )
    )


class BoundMarketDataContext:
    """Read exact resolved series without consumer-owned joins or fallback calls."""

    def __init__(
        self,
        *,
        plan: ResolvedMarketDataPlan,
        records_by_series: Mapping[
            tuple[str, str, str, Optional[int]], Sequence[MarketDataRecord]
        ],
    ) -> None:
        self.plan = plan
        self._records = {
            tuple(key): tuple(value) for key, value in records_by_series.items()
        }
        self._bindings: dict[
            tuple[str, str, Optional[str]], ResolvedMarketDataSeries
        ] = {}
        for series in plan.series:
            for binding in series.bindings:
                self._bindings[binding.binding_key] = series

    def latest(
        self,
        key: str,
        *,
        consumer_id: str,
        evaluation_time: Any,
        primary_instrument_id: Optional[str] = None,
    ) -> MarketDataRecord | UnavailableMarketData:
        binding_key = (
            str(consumer_id or "").strip(),
            str(key or "").strip(),
            str(primary_instrument_id or "").strip() or None,
        )
        series = self._bindings.get(binding_key)
        if series is None:
            raise KeyError(
                "market_data_binding_missing: "
                f"consumer_id={binding_key[0]} key={binding_key[1]} "
                f"primary_instrument_id={binding_key[2]}"
            )
        if series.alignment is not MarketDataAlignment.LATEST_KNOWN:
            raise ValueError(
                "market_data_alignment_invalid: latest() requires latest_known alignment"
            )
        assert series.max_staleness_seconds is not None
        result = latest_known_record(
            self._records.get(
                (
                    series.instrument_id,
                    series.fact_type,
                    series.contract_version,
                    series.timeframe_seconds,
                ),
                (),
            ),
            evaluation_time=evaluation_time,
            max_staleness_seconds=series.max_staleness_seconds,
        )
        if isinstance(result, UnavailableMarketData):
            unavailable = UnavailableMarketData(
                key=binding_key[1],
                reason=result.reason,
                evaluation_time=result.evaluation_time,
                details={
                    **dict(result.details),
                    "instrument_id": series.instrument_id,
                    "fact_type": series.fact_type,
                    "required": series.required,
                },
            )
            if series.required:
                raise RuntimeError(
                    "market_data_required_unavailable: "
                    f"consumer_id={binding_key[0]} key={binding_key[1]} "
                    f"reason={unavailable.reason}"
                )
            return unavailable
        return result


__all__ = [
    "BoundMarketDataContext",
    "InstrumentResolutionContext",
    "MarketDataPlanResolver",
    "RESOLVED_MARKET_DATA_PLAN_VERSION",
    "ResolvedMarketDataPlan",
    "ResolvedMarketDataRequirement",
    "ResolvedMarketDataSeries",
    "UnavailableMarketData",
    "causal_numeric_fact_records",
    "latest_known_record",
]
