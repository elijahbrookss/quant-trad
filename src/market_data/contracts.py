"""Pure contracts for versioned, causally safe market-data facts.

This module intentionally knows nothing about providers, PostgreSQL, pandas,
indicators, or runtime orchestration. It defines the source facts that every
acquisition and replay path must agree on.
"""

from __future__ import annotations

import hashlib
import json
import math
import struct
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Optional

from .fact_registry import get_fact_contract


CANDLE_FACT_TYPE = "candle.ohlcv"
CANDLE_FACT_VERSION = "candle.ohlcv.v1"
CANDLE_MATERIAL_HASH_VERSION = "candle_material_hash.v1"
OPEN_INTEREST_FACT_TYPE = "derivatives.open_interest"
OPEN_INTEREST_FACT_VERSION = "derivatives.open_interest.v1"
OPEN_INTEREST_MATERIAL_HASH_VERSION = "open_interest_material_hash.v1"
FUNDING_RATE_FACT_TYPE = "derivatives.funding_rate"
FUNDING_RATE_FACT_VERSION = "derivatives.funding_rate.v1"
FUNDING_RATE_MATERIAL_HASH_VERSION = "funding_rate_material_hash.v1"
DATASET_IDENTITY_HASH_VERSION = "market_dataset.v1"
QUALITY_HASH_VERSION = "market_data_quality_hash.v1"
NUMERIC_FACT_MATERIAL_HASH_VERSION = "numeric_fact_material_hash.v1"

_RECEIPT_KNOWN_AT_METHODS = frozenset(
    {"platform_acceptance", "platform_receipt", "stream_receipt"}
)


class InstrumentRole(str, Enum):
    """How a requested fact's instrument relates to the run's traded instrument."""

    PRIMARY = "primary"
    UNDERLYING = "underlying"
    BENCHMARK = "benchmark"
    EXPLICIT = "explicit"


class MarketDataAlignment(str, Enum):
    """Supported causal alignment policies for source facts."""

    EXACT_INTERVAL = "exact_interval"
    LATEST_KNOWN = "latest_known"


class NumericFactState(str, Enum):
    """Validity state of one append-only numeric source-event revision."""

    ACTIVE = "active"
    INVALIDATED = "invalidated"


def _utc_datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"market_data_invalid: {field} must be finite")
        if abs(numeric) > 2e10:
            numeric /= 1000.0
        parsed = datetime.fromtimestamp(numeric, tz=timezone.utc)
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"market_data_invalid: {field} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"market_data_invalid: {field} is not an ISO-8601 timestamp"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_utc_datetime(value: Any, *, field: str) -> Optional[datetime]:
    if value in (None, ""):
        return None
    return _utc_datetime(value, field=field)


def _finite_number(
    value: Any,
    *,
    field: str,
    required: bool = True,
    nonnegative: bool = False,
) -> Optional[float]:
    if value is None and not required:
        return None
    if isinstance(value, bool):
        raise ValueError(f"market_data_invalid: {field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"market_data_invalid: {field} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"market_data_invalid: {field} must be finite")
    if nonnegative and result < 0:
        raise ValueError(f"market_data_invalid: {field} must be nonnegative")
    return 0.0 if result == 0.0 else result


def _canonical_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _canonical_number(value: Optional[float]) -> Optional[str]:
    return None if value is None else struct.pack("!d", float(value)).hex()


def _exact_decimal(value: Any, *, field: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError(
            f"market_data_invalid: {field} must be Decimal, integer, or decimal string"
        )
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"market_data_invalid: {field} must be an exact decimal") from exc
    if not result.is_finite():
        raise ValueError(f"market_data_invalid: {field} must be finite")
    return Decimal(0) if result.is_zero() else result


def _canonical_decimal(value: Decimal) -> str:
    if value.is_zero():
        return "0"
    normalized = format(value.normalize(), "f")
    return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SourceIdentity:
    """Stable identity for one acquisition source and venue."""

    provider: str
    venue: str
    source_kind: str
    adapter_version: str

    def __post_init__(self) -> None:
        for field in ("provider", "source_kind", "adapter_version"):
            value = str(getattr(self, field) or "").strip()
            if not value:
                raise ValueError(f"market_data_source_invalid: {field} is required")
            object.__setattr__(self, field, value)
        object.__setattr__(self, "venue", str(self.venue or "").strip())

    @property
    def identity_key(self) -> str:
        return _stable_hash(
            {
                "provider": self.provider.lower(),
                "venue": self.venue.lower(),
                "source_kind": self.source_kind.lower(),
                "adapter_version": self.adapter_version,
            }
        )


@dataclass(frozen=True)
class MarketDataRequirement:
    """Typed source-fact requirement declared by a consumer."""

    fact_type: str = CANDLE_FACT_TYPE
    timeframe_seconds: Optional[int] = None
    contract_version: Optional[str] = None
    required: bool = True
    key: str = "primary_bars"
    instrument_role: InstrumentRole | str = InstrumentRole.PRIMARY
    instrument_ref: Optional[str] = None
    dimensions: Mapping[str, Any] = field(default_factory=dict)
    alignment: MarketDataAlignment | str | None = None
    max_staleness_seconds: Optional[int] = None
    allow_gaps: bool = False
    known_at_required: bool = True
    required_fields: tuple[str, ...] = ()
    lookback_bars: Optional[int] = None
    lookback_seconds: Optional[int] = None

    def __post_init__(self) -> None:
        fact_type = str(self.fact_type or "").strip().lower()
        contract = get_fact_contract(fact_type)
        default_version = contract.contract_version
        contract_version = str(self.contract_version or default_version or "").strip()
        key = str(self.key or "").strip()
        if not fact_type:
            raise ValueError("market_data_requirement_invalid: fact_type is required")
        if not contract_version:
            raise ValueError(
                "market_data_requirement_invalid: contract_version is required"
            )
        if not key:
            raise ValueError("market_data_requirement_invalid: key is required")
        if self.timeframe_seconds is not None and int(self.timeframe_seconds) <= 0:
            raise ValueError(
                "market_data_requirement_invalid: timeframe_seconds must be positive"
            )
        contract.validate(contract_version=contract_version, timeframe_seconds=self.timeframe_seconds)
        dimensions = contract.normalize_dimensions(self.dimensions)
        try:
            raw_instrument_role = (
                self.instrument_role.value
                if isinstance(self.instrument_role, InstrumentRole)
                else self.instrument_role
            )
            instrument_role = InstrumentRole(str(raw_instrument_role).strip().lower())
        except ValueError as exc:
            raise ValueError(
                "market_data_requirement_invalid: instrument_role must be "
                "primary, underlying, benchmark, or explicit"
            ) from exc
        instrument_ref = str(self.instrument_ref or "").strip() or None
        if instrument_role in {InstrumentRole.BENCHMARK, InstrumentRole.EXPLICIT}:
            if instrument_ref is None:
                raise ValueError(
                    "market_data_requirement_invalid: benchmark and explicit roles require instrument_ref"
                )
        elif instrument_role is InstrumentRole.PRIMARY and instrument_ref is not None:
            raise ValueError(
                "market_data_requirement_invalid: primary role cannot declare instrument_ref"
            )

        default_alignment = (
            MarketDataAlignment.EXACT_INTERVAL
            if contract.default_alignment == MarketDataAlignment.EXACT_INTERVAL.value
            else MarketDataAlignment.LATEST_KNOWN
        )
        try:
            raw_alignment = (
                self.alignment.value
                if isinstance(self.alignment, MarketDataAlignment)
                else self.alignment
            )
            alignment = MarketDataAlignment(
                str(raw_alignment or default_alignment.value).strip().lower()
            )
        except ValueError as exc:
            raise ValueError(
                "market_data_requirement_invalid: unsupported alignment"
            ) from exc
        if contract.timeframe_mode == "required" and alignment is not MarketDataAlignment.EXACT_INTERVAL:
            raise ValueError(
                "market_data_requirement_invalid: bucketed facts require exact_interval alignment"
            )
        max_staleness = self.max_staleness_seconds
        if max_staleness is not None and int(max_staleness) <= 0:
            raise ValueError(
                "market_data_requirement_invalid: max_staleness_seconds must be positive"
            )
        if alignment is MarketDataAlignment.LATEST_KNOWN and max_staleness is None:
            raise ValueError(
                "market_data_requirement_invalid: latest_known alignment requires max_staleness_seconds"
            )
        lookback_bars = self.lookback_bars
        if lookback_bars is not None and int(lookback_bars) <= 0:
            raise ValueError(
                "market_data_requirement_invalid: lookback_bars must be positive"
            )
        lookback_seconds = self.lookback_seconds
        if lookback_seconds is not None and int(lookback_seconds) <= 0:
            raise ValueError(
                "market_data_requirement_invalid: lookback_seconds must be positive"
            )
        required_fields = tuple(
            dict.fromkeys(
                str(field).strip()
                for field in self.required_fields
                if str(field).strip()
            )
        )
        object.__setattr__(self, "fact_type", fact_type)
        object.__setattr__(self, "contract_version", contract_version)
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "instrument_role", instrument_role)
        object.__setattr__(self, "instrument_ref", instrument_ref)
        object.__setattr__(self, "dimensions", dimensions)
        object.__setattr__(self, "alignment", alignment)
        object.__setattr__(self, "required", bool(self.required))
        object.__setattr__(self, "allow_gaps", bool(self.allow_gaps))
        object.__setattr__(self, "known_at_required", bool(self.known_at_required))
        object.__setattr__(self, "required_fields", required_fields)
        if self.timeframe_seconds is not None:
            object.__setattr__(self, "timeframe_seconds", int(self.timeframe_seconds))
        if max_staleness is not None:
            object.__setattr__(self, "max_staleness_seconds", int(max_staleness))
        if lookback_bars is not None:
            object.__setattr__(self, "lookback_bars", int(lookback_bars))
        if lookback_seconds is not None:
            object.__setattr__(self, "lookback_seconds", int(lookback_seconds))

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "key": self.key,
            "fact_type": self.fact_type,
            "contract_version": self.contract_version,
            "instrument_role": self.instrument_role.value,
            "instrument_ref": self.instrument_ref,
            "timeframe_seconds": self.timeframe_seconds,
            "required": self.required,
            "alignment": self.alignment.value,
            "max_staleness_seconds": self.max_staleness_seconds,
            "allow_gaps": self.allow_gaps,
            "known_at_required": self.known_at_required,
            "required_fields": list(self.required_fields),
            "lookback_bars": self.lookback_bars,
            "lookback_seconds": self.lookback_seconds,
        }
        if self.dimensions:
            payload["dimensions"] = dict(self.dimensions)
        return payload


@dataclass(frozen=True)
class MarketDataWindow:
    """Half-open query window for one canonical fact series."""

    instrument_id: str
    fact_type: str
    start: datetime
    end: datetime
    timeframe_seconds: Optional[int] = None
    as_of_commit_seq: Optional[int] = None

    def __post_init__(self) -> None:
        instrument_id = str(self.instrument_id or "").strip()
        fact_type = str(self.fact_type or "").strip().lower()
        if not instrument_id:
            raise ValueError("market_data_window_invalid: instrument_id is required")
        if not fact_type:
            raise ValueError("market_data_window_invalid: fact_type is required")
        start = _utc_datetime(self.start, field="start")
        end = _utc_datetime(self.end, field="end")
        if end <= start:
            raise ValueError("market_data_window_invalid: end must be after start")
        if self.timeframe_seconds is not None and int(self.timeframe_seconds) <= 0:
            raise ValueError(
                "market_data_window_invalid: timeframe_seconds must be positive"
            )
        contract = get_fact_contract(fact_type)
        if contract.timeframe_mode == "required" and self.timeframe_seconds is None:
            raise ValueError(
                "market_data_window_invalid: bucketed facts require timeframe_seconds"
            )
        if contract.timeframe_mode == "forbidden" and self.timeframe_seconds is not None:
            raise ValueError("market_data_window_invalid: unbucketed fact forbids timeframe")
        if self.as_of_commit_seq is not None and int(self.as_of_commit_seq) < 0:
            raise ValueError(
                "market_data_window_invalid: as_of_commit_seq must be nonnegative"
            )
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "fact_type", fact_type)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        if self.timeframe_seconds is not None:
            object.__setattr__(self, "timeframe_seconds", int(self.timeframe_seconds))
        if self.as_of_commit_seq is not None:
            object.__setattr__(self, "as_of_commit_seq", int(self.as_of_commit_seq))


@dataclass(frozen=True)
class DatasetSeriesRequest:
    """One series and half-open range requested for an immutable dataset."""

    series_id: int
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if int(self.series_id) <= 0:
            raise ValueError("market_dataset_invalid: series_id must be positive")
        start = _utc_datetime(self.start, field="start")
        end = _utc_datetime(self.end, field="end")
        if end <= start:
            raise ValueError("market_dataset_invalid: end must be after start")
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)


@dataclass(frozen=True)
class NumericFact:
    """Provider-neutral exact-decimal fact keyed by one logical source event."""

    fact_type: str
    contract_version: str
    value: Decimal | int | str
    raw_value: str
    unit: str
    dimensions: Mapping[str, Any]
    effective_at: datetime
    effective_at_method: str
    accepted_at: datetime
    known_at: datetime
    known_at_method: str
    source_event_key: str
    source_published_at: Optional[datetime] = None
    received_at: Optional[datetime] = None
    source_event_group_key: Optional[str] = None
    source_event_component_key: Optional[str] = None
    state: NumericFactState | str = NumericFactState.ACTIVE
    source_event_material_hash: Optional[str] = None

    def __post_init__(self) -> None:
        fact_type = str(self.fact_type or "").strip().lower()
        contract_version = str(self.contract_version or "").strip()
        contract = get_fact_contract(fact_type)
        contract.validate(contract_version=contract_version, timeframe_seconds=None)
        value = _exact_decimal(self.value, field="value")
        unit, dimensions = contract.validate_numeric_value(
            value=value,
            unit=self.unit,
            dimensions=self.dimensions,
        )
        raw_value = str(self.raw_value or "").strip()
        if not raw_value:
            raise ValueError("numeric_fact_invalid: raw_value is required")

        effective_at = _utc_datetime(self.effective_at, field="effective_at")
        accepted_at = _utc_datetime(self.accepted_at, field="accepted_at")
        known_at = _utc_datetime(self.known_at, field="known_at")
        source_published_at = _optional_utc_datetime(
            self.source_published_at, field="source_published_at"
        )
        received_at = _optional_utc_datetime(self.received_at, field="received_at")
        effective_at_method = str(self.effective_at_method or "").strip().lower()
        known_at_method = str(self.known_at_method or "").strip().lower()
        if not effective_at_method:
            raise ValueError("numeric_fact_invalid: effective_at_method is required")
        if not known_at_method:
            raise ValueError("numeric_fact_invalid: known_at_method is required")
        if known_at < effective_at:
            raise ValueError("numeric_fact_invalid: known_at must not precede effective_at")
        if source_published_at is not None and known_at < source_published_at:
            raise ValueError(
                "numeric_fact_invalid: known_at must not precede source publication"
            )
        if received_at is not None and accepted_at < received_at:
            raise ValueError(
                "numeric_fact_invalid: accepted_at must not precede received_at"
            )
        if known_at_method in _RECEIPT_KNOWN_AT_METHODS:
            if received_at is not None and known_at < received_at:
                raise ValueError(
                    "numeric_fact_invalid: receipt-based known_at must not precede receipt"
                )
            if known_at < accepted_at:
                raise ValueError(
                    "numeric_fact_invalid: receipt-based known_at must not precede acceptance"
                )

        source_event_key = str(self.source_event_key or "").strip()
        group_key = str(self.source_event_group_key or "").strip() or None
        component_key = str(self.source_event_component_key or "").strip() or None
        if not source_event_key:
            raise ValueError("numeric_fact_invalid: source_event_key is required")
        source_event_material_hash = str(
            self.source_event_material_hash or ""
        ).strip().lower()
        if not source_event_material_hash:
            source_event_material_hash = _stable_hash(
                {
                    "schema_version": "market.source_event_material.v1",
                    "material": {},
                }
            )
        if len(source_event_material_hash) != 64 or any(
            character not in "0123456789abcdef"
            for character in source_event_material_hash
        ):
            raise ValueError(
                "numeric_fact_invalid: source_event_material_hash must be sha256"
            )
        try:
            raw_state = self.state.value if isinstance(self.state, NumericFactState) else self.state
            state = NumericFactState(str(raw_state).strip().lower())
        except ValueError as exc:
            raise ValueError(
                "numeric_fact_invalid: state must be active or invalidated"
            ) from exc

        object.__setattr__(self, "fact_type", fact_type)
        object.__setattr__(self, "contract_version", contract_version)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "raw_value", raw_value)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "dimensions", dimensions)
        object.__setattr__(self, "effective_at", effective_at)
        object.__setattr__(self, "effective_at_method", effective_at_method)
        object.__setattr__(self, "source_published_at", source_published_at)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "accepted_at", accepted_at)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "known_at_method", known_at_method)
        object.__setattr__(self, "source_event_key", source_event_key)
        object.__setattr__(self, "source_event_group_key", group_key)
        object.__setattr__(self, "source_event_component_key", component_key)
        object.__setattr__(
            self, "source_event_material_hash", source_event_material_hash
        )
        object.__setattr__(self, "state", state)

    @property
    def row_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": self.contract_version,
                "fact_type": self.fact_type,
                "value": _canonical_decimal(self.value),
                "raw_value": self.raw_value,
                "unit": self.unit,
                "dimensions": dict(self.dimensions),
                "effective_at": _canonical_time(self.effective_at),
                "effective_at_method": self.effective_at_method,
                "source_published_at": (
                    _canonical_time(self.source_published_at)
                    if self.source_published_at is not None
                    else None
                ),
                "received_at": (
                    _canonical_time(self.received_at)
                    if self.received_at is not None
                    else None
                ),
                "known_at": _canonical_time(self.known_at),
                "known_at_method": self.known_at_method,
                "source_event_key": self.source_event_key,
                "source_event_group_key": self.source_event_group_key,
                "source_event_component_key": self.source_event_component_key,
                "source_event_material_hash": self.source_event_material_hash,
                "state": self.state.value,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "fact_type": self.fact_type,
            "contract_version": self.contract_version,
            "value": _canonical_decimal(self.value),
            "raw_value": self.raw_value,
            "unit": self.unit,
            "dimensions": dict(self.dimensions),
            "effective_at": self.effective_at,
            "effective_at_method": self.effective_at_method,
            "source_published_at": self.source_published_at,
            "received_at": self.received_at,
            "accepted_at": self.accepted_at,
            "known_at": self.known_at,
            "known_at_method": self.known_at_method,
            "source_event_key": self.source_event_key,
            "source_event_group_key": self.source_event_group_key,
            "source_event_component_key": self.source_event_component_key,
            "source_event_material_hash": self.source_event_material_hash,
            "state": self.state.value,
            "row_hash": self.row_hash,
        }


@dataclass(frozen=True)
class NumericFactRecord:
    """Immutable storage revision of a provider-neutral numeric fact."""

    series_id: int
    revision: int
    market_commit_seq: int
    ingestion_run_id: str
    source_identity_key: str
    source: SourceIdentity
    provenance: Mapping[str, Any]
    fact: NumericFact

    def __post_init__(self) -> None:
        if int(self.series_id) <= 0:
            raise ValueError("numeric_fact_record_invalid: series_id must be positive")
        if int(self.revision) <= 0:
            raise ValueError("numeric_fact_record_invalid: revision must be positive")
        if int(self.market_commit_seq) <= 0:
            raise ValueError(
                "numeric_fact_record_invalid: market_commit_seq must be positive"
            )
        ingestion_run_id = str(self.ingestion_run_id or "").strip()
        source_identity_key = str(self.source_identity_key or "").strip()
        if not ingestion_run_id:
            raise ValueError("numeric_fact_record_invalid: ingestion_run_id is required")
        if not source_identity_key:
            raise ValueError(
                "numeric_fact_record_invalid: source_identity_key is required"
            )
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "revision", int(self.revision))
        object.__setattr__(self, "market_commit_seq", int(self.market_commit_seq))
        object.__setattr__(self, "ingestion_run_id", ingestion_run_id)
        object.__setattr__(self, "source_identity_key", source_identity_key)
        object.__setattr__(self, "provenance", dict(self.provenance or {}))


@dataclass(frozen=True)
class CandleFact:
    """Closed source candle with explicit causal and provenance timestamps."""

    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float]
    known_at: datetime
    known_at_method: str
    accepted_at: datetime
    trade_count: Optional[int] = None
    source_published_at: Optional[datetime] = None
    received_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        open_time = _utc_datetime(self.open_time, field="open_time")
        close_time = _utc_datetime(self.close_time, field="close_time")
        known_at = _utc_datetime(self.known_at, field="known_at")
        accepted_at = _utc_datetime(self.accepted_at, field="accepted_at")
        source_published_at = _optional_utc_datetime(
            self.source_published_at, field="source_published_at"
        )
        received_at = _optional_utc_datetime(self.received_at, field="received_at")
        method = str(self.known_at_method or "").strip().lower()
        if close_time <= open_time:
            raise ValueError("candle_fact_invalid: close_time must be after open_time")
        if known_at < close_time:
            raise ValueError("candle_fact_invalid: known_at must not precede close_time")
        if source_published_at is not None and known_at < source_published_at:
            raise ValueError(
                "candle_fact_invalid: known_at must not precede source_published_at"
            )
        if received_at is not None:
            if accepted_at < received_at:
                raise ValueError(
                    "candle_fact_invalid: accepted_at must not precede received_at"
                )
            if known_at < received_at:
                raise ValueError(
                    "candle_fact_invalid: known_at must not precede received_at"
                )
        if method in _RECEIPT_KNOWN_AT_METHODS and known_at < accepted_at:
            raise ValueError(
                "candle_fact_invalid: receipt-based known_at must not precede accepted_at"
            )
        if not method:
            raise ValueError("candle_fact_invalid: known_at_method is required")

        open_price = _finite_number(self.open, field="open")
        high = _finite_number(self.high, field="high")
        low = _finite_number(self.low, field="low")
        close_price = _finite_number(self.close, field="close")
        volume = _finite_number(
            self.volume, field="volume", required=False, nonnegative=True
        )
        assert open_price is not None
        assert high is not None
        assert low is not None
        assert close_price is not None
        if high < low:
            raise ValueError("candle_fact_invalid: high must not be below low")
        if high < max(open_price, close_price):
            raise ValueError("candle_fact_invalid: high must not be below open or close")
        if low > min(open_price, close_price):
            raise ValueError("candle_fact_invalid: low must not be above open or close")
        trade_count = self.trade_count
        if trade_count is not None:
            if isinstance(trade_count, bool):
                raise ValueError(
                    "candle_fact_invalid: trade_count must be a nonnegative integer"
                )
            try:
                trade_count = int(trade_count)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(
                    "candle_fact_invalid: trade_count must be a nonnegative integer"
                ) from exc
            if trade_count < 0:
                raise ValueError(
                    "candle_fact_invalid: trade_count must be a nonnegative integer"
                )

        object.__setattr__(self, "open_time", open_time)
        object.__setattr__(self, "close_time", close_time)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "accepted_at", accepted_at)
        object.__setattr__(self, "source_published_at", source_published_at)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "known_at_method", method)
        object.__setattr__(self, "open", open_price)
        object.__setattr__(self, "high", high)
        object.__setattr__(self, "low", low)
        object.__setattr__(self, "close", close_price)
        object.__setattr__(self, "volume", volume)
        object.__setattr__(self, "trade_count", trade_count)

    @property
    def row_hash(self) -> str:
        """Hash source values and causal availability, excluding ingest time."""

        return _stable_hash(
            {
                "schema_version": CANDLE_FACT_VERSION,
                "open_time": _canonical_time(self.open_time),
                "close_time": _canonical_time(self.close_time),
                "open": _canonical_number(self.open),
                "high": _canonical_number(self.high),
                "low": _canonical_number(self.low),
                "close": _canonical_number(self.close),
                "volume": _canonical_number(self.volume),
                "trade_count": self.trade_count,
                "source_published_at": (
                    _canonical_time(self.source_published_at)
                    if self.source_published_at is not None
                    else None
                ),
                "received_at": (
                    _canonical_time(self.received_at)
                    if self.received_at is not None
                    else None
                ),
                "known_at": _canonical_time(self.known_at),
                "known_at_method": self.known_at_method,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "open_time": self.open_time,
            "close_time": self.close_time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "source_published_at": self.source_published_at,
            "received_at": self.received_at,
            "known_at": self.known_at,
            "known_at_method": self.known_at_method,
            "accepted_at": self.accepted_at,
            "row_hash": self.row_hash,
        }


@dataclass(frozen=True)
class CandleRecord:
    """A candle fact plus its immutable storage revision identity."""

    series_id: int
    revision: int
    market_commit_seq: int
    ingestion_run_id: str
    source_identity_key: str
    source: SourceIdentity
    provenance: Mapping[str, Any]
    fact: CandleFact

    def __post_init__(self) -> None:
        if int(self.series_id) <= 0:
            raise ValueError("candle_record_invalid: series_id must be positive")
        if int(self.revision) <= 0:
            raise ValueError("candle_record_invalid: revision must be positive")
        if int(self.market_commit_seq) <= 0:
            raise ValueError("candle_record_invalid: market_commit_seq must be positive")
        ingestion_run_id = str(self.ingestion_run_id or "").strip()
        if not ingestion_run_id:
            raise ValueError("candle_record_invalid: ingestion_run_id is required")
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "revision", int(self.revision))
        object.__setattr__(self, "market_commit_seq", int(self.market_commit_seq))
        source_identity_key = str(self.source_identity_key or "").strip()
        if not source_identity_key:
            raise ValueError("candle_record_invalid: source_identity_key is required")
        object.__setattr__(self, "ingestion_run_id", ingestion_run_id)
        object.__setattr__(self, "source_identity_key", source_identity_key)
        object.__setattr__(self, "provenance", dict(self.provenance or {}))


@dataclass(frozen=True)
class OpenInterestFact:
    """One scheduled open-interest observation with explicit receipt semantics.

    Coinbase Advanced Trade does not publish an event timestamp for open interest.
    ``sample_time`` is therefore the collector's scheduled observation identity,
    never a fabricated provider event time. Causal availability is governed only
    by ``known_at``.
    """

    sample_time: datetime
    value: float
    known_at: datetime
    known_at_method: str
    accepted_at: datetime
    unit: str = "contracts"
    sample_time_method: str = "collector_schedule"
    source_published_at: Optional[datetime] = None
    received_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        sample_time = _utc_datetime(self.sample_time, field="sample_time")
        known_at = _utc_datetime(self.known_at, field="known_at")
        accepted_at = _utc_datetime(self.accepted_at, field="accepted_at")
        source_published_at = _optional_utc_datetime(
            self.source_published_at, field="source_published_at"
        )
        received_at = _optional_utc_datetime(self.received_at, field="received_at")
        known_at_method = str(self.known_at_method or "").strip().lower()
        sample_time_method = str(self.sample_time_method or "").strip().lower()
        unit = str(self.unit or "").strip().lower()
        value = _finite_number(
            self.value, field="open_interest", required=True, nonnegative=True
        )
        assert value is not None
        if not known_at_method:
            raise ValueError("open_interest_fact_invalid: known_at_method is required")
        if sample_time_method != "collector_schedule":
            raise ValueError(
                "open_interest_fact_invalid: sample_time_method must be collector_schedule"
            )
        if unit != "contracts":
            raise ValueError(
                "open_interest_fact_invalid: unit must be contracts for derivatives.open_interest.v1"
            )
        if known_at < sample_time:
            raise ValueError(
                "open_interest_fact_invalid: known_at must not precede sample_time"
            )
        if source_published_at is not None and known_at < source_published_at:
            raise ValueError(
                "open_interest_fact_invalid: known_at must not precede source publication"
            )
        if received_at is not None:
            if accepted_at < received_at:
                raise ValueError(
                    "open_interest_fact_invalid: accepted_at must not precede received_at"
                )
            if known_at < received_at:
                raise ValueError(
                    "open_interest_fact_invalid: known_at must not precede received_at"
                )
        if known_at_method in _RECEIPT_KNOWN_AT_METHODS and known_at < accepted_at:
            raise ValueError(
                "open_interest_fact_invalid: receipt-based known_at must not precede accepted_at"
            )
        object.__setattr__(self, "sample_time", sample_time)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "accepted_at", accepted_at)
        object.__setattr__(self, "known_at_method", known_at_method)
        object.__setattr__(self, "sample_time_method", sample_time_method)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "source_published_at", source_published_at)
        object.__setattr__(self, "received_at", received_at)

    @property
    def row_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": OPEN_INTEREST_FACT_VERSION,
                "sample_time": _canonical_time(self.sample_time),
                "sample_time_method": self.sample_time_method,
                "value": _canonical_number(self.value),
                "unit": self.unit,
                "source_published_at": (
                    _canonical_time(self.source_published_at)
                    if self.source_published_at is not None
                    else None
                ),
                "received_at": (
                    _canonical_time(self.received_at)
                    if self.received_at is not None
                    else None
                ),
                "known_at": _canonical_time(self.known_at),
                "known_at_method": self.known_at_method,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_time": self.sample_time,
            "sample_time_method": self.sample_time_method,
            "value": self.value,
            "unit": self.unit,
            "source_published_at": self.source_published_at,
            "received_at": self.received_at,
            "accepted_at": self.accepted_at,
            "known_at": self.known_at,
            "known_at_method": self.known_at_method,
            "row_hash": self.row_hash,
        }


@dataclass(frozen=True)
class OpenInterestRecord:
    """An open-interest fact plus immutable storage revision identity."""

    series_id: int
    revision: int
    market_commit_seq: int
    ingestion_run_id: str
    source_identity_key: str
    source: SourceIdentity
    provenance: Mapping[str, Any]
    fact: OpenInterestFact

    def __post_init__(self) -> None:
        if int(self.series_id) <= 0:
            raise ValueError("open_interest_record_invalid: series_id must be positive")
        if int(self.revision) <= 0:
            raise ValueError("open_interest_record_invalid: revision must be positive")
        if int(self.market_commit_seq) <= 0:
            raise ValueError(
                "open_interest_record_invalid: market_commit_seq must be positive"
            )
        ingestion_run_id = str(self.ingestion_run_id or "").strip()
        source_identity_key = str(self.source_identity_key or "").strip()
        if not ingestion_run_id:
            raise ValueError(
                "open_interest_record_invalid: ingestion_run_id is required"
            )
        if not source_identity_key:
            raise ValueError(
                "open_interest_record_invalid: source_identity_key is required"
            )
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "revision", int(self.revision))
        object.__setattr__(self, "market_commit_seq", int(self.market_commit_seq))
        object.__setattr__(self, "ingestion_run_id", ingestion_run_id)
        object.__setattr__(self, "source_identity_key", source_identity_key)
        object.__setattr__(self, "provenance", dict(self.provenance or {}))


@dataclass(frozen=True)
class FundingRateFact:
    """One scheduled observation of a provider-reported perpetual funding rate.

    sample_time is the collector schedule and therefore the observation
    identity. Coinbase documents funding_time as part of the product payload
    without defining it as a publication timestamp, so it is preserved verbatim
    and never substituted for causal known_at.
    """

    sample_time: datetime
    rate: float
    funding_time: datetime
    interval_seconds: int
    known_at: datetime
    known_at_method: str
    accepted_at: datetime
    unit: str = "fraction"
    sample_time_method: str = "collector_schedule"
    source_published_at: Optional[datetime] = None
    received_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        sample_time = _utc_datetime(self.sample_time, field="sample_time")
        funding_time = _utc_datetime(self.funding_time, field="funding_time")
        known_at = _utc_datetime(self.known_at, field="known_at")
        accepted_at = _utc_datetime(self.accepted_at, field="accepted_at")
        source_published_at = _optional_utc_datetime(
            self.source_published_at, field="source_published_at"
        )
        received_at = _optional_utc_datetime(self.received_at, field="received_at")
        known_at_method = str(self.known_at_method or "").strip().lower()
        sample_time_method = str(self.sample_time_method or "").strip().lower()
        unit = str(self.unit or "").strip().lower()
        rate = _finite_number(self.rate, field="funding_rate")
        assert rate is not None
        if isinstance(self.interval_seconds, bool):
            raise ValueError(
                "funding_rate_fact_invalid: interval_seconds must be a positive integer"
            )
        if (
            isinstance(self.interval_seconds, float)
            and not self.interval_seconds.is_integer()
        ):
            raise ValueError(
                "funding_rate_fact_invalid: interval_seconds must be a positive integer"
            )
        try:
            interval_seconds = int(self.interval_seconds)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "funding_rate_fact_invalid: interval_seconds must be a positive integer"
            ) from exc
        if interval_seconds <= 0:
            raise ValueError(
                "funding_rate_fact_invalid: interval_seconds must be a positive integer"
            )
        if not known_at_method:
            raise ValueError("funding_rate_fact_invalid: known_at_method is required")
        if sample_time_method != "collector_schedule":
            raise ValueError(
                "funding_rate_fact_invalid: sample_time_method must be collector_schedule"
            )
        if unit != "fraction":
            raise ValueError(
                "funding_rate_fact_invalid: unit must be fraction for derivatives.funding_rate.v1"
            )
        if known_at < sample_time:
            raise ValueError(
                "funding_rate_fact_invalid: known_at must not precede sample_time"
            )
        if source_published_at is not None and known_at < source_published_at:
            raise ValueError(
                "funding_rate_fact_invalid: known_at must not precede source publication"
            )
        if received_at is not None:
            if accepted_at < received_at:
                raise ValueError(
                    "funding_rate_fact_invalid: accepted_at must not precede received_at"
                )
            if known_at < received_at:
                raise ValueError(
                    "funding_rate_fact_invalid: known_at must not precede received_at"
                )
        if known_at_method in _RECEIPT_KNOWN_AT_METHODS and known_at < accepted_at:
            raise ValueError(
                "funding_rate_fact_invalid: receipt-based known_at must not precede accepted_at"
            )
        object.__setattr__(self, "sample_time", sample_time)
        object.__setattr__(self, "rate", rate)
        object.__setattr__(self, "funding_time", funding_time)
        object.__setattr__(self, "interval_seconds", interval_seconds)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "accepted_at", accepted_at)
        object.__setattr__(self, "known_at_method", known_at_method)
        object.__setattr__(self, "sample_time_method", sample_time_method)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "source_published_at", source_published_at)
        object.__setattr__(self, "received_at", received_at)

    @property
    def row_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": FUNDING_RATE_FACT_VERSION,
                "sample_time": _canonical_time(self.sample_time),
                "sample_time_method": self.sample_time_method,
                "rate": _canonical_number(self.rate),
                "funding_time": _canonical_time(self.funding_time),
                "interval_seconds": self.interval_seconds,
                "unit": self.unit,
                "source_published_at": (
                    _canonical_time(self.source_published_at)
                    if self.source_published_at is not None
                    else None
                ),
                "received_at": (
                    _canonical_time(self.received_at)
                    if self.received_at is not None
                    else None
                ),
                "known_at": _canonical_time(self.known_at),
                "known_at_method": self.known_at_method,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_time": self.sample_time,
            "sample_time_method": self.sample_time_method,
            "rate": self.rate,
            "funding_time": self.funding_time,
            "interval_seconds": self.interval_seconds,
            "unit": self.unit,
            "source_published_at": self.source_published_at,
            "received_at": self.received_at,
            "accepted_at": self.accepted_at,
            "known_at": self.known_at,
            "known_at_method": self.known_at_method,
            "row_hash": self.row_hash,
        }


@dataclass(frozen=True)
class FundingRateRecord:
    """A funding-rate fact plus immutable storage revision identity."""

    series_id: int
    revision: int
    market_commit_seq: int
    ingestion_run_id: str
    source_identity_key: str
    source: SourceIdentity
    provenance: Mapping[str, Any]
    fact: FundingRateFact

    def __post_init__(self) -> None:
        if int(self.series_id) <= 0:
            raise ValueError("funding_rate_record_invalid: series_id must be positive")
        if int(self.revision) <= 0:
            raise ValueError("funding_rate_record_invalid: revision must be positive")
        if int(self.market_commit_seq) <= 0:
            raise ValueError(
                "funding_rate_record_invalid: market_commit_seq must be positive"
            )
        ingestion_run_id = str(self.ingestion_run_id or "").strip()
        source_identity_key = str(self.source_identity_key or "").strip()
        if not ingestion_run_id:
            raise ValueError(
                "funding_rate_record_invalid: ingestion_run_id is required"
            )
        if not source_identity_key:
            raise ValueError(
                "funding_rate_record_invalid: source_identity_key is required"
            )
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "revision", int(self.revision))
        object.__setattr__(self, "market_commit_seq", int(self.market_commit_seq))
        object.__setattr__(self, "ingestion_run_id", ingestion_run_id)
        object.__setattr__(self, "source_identity_key", source_identity_key)
        object.__setattr__(self, "provenance", dict(self.provenance or {}))


def build_candle_material_hash(
    *, series_identity: Mapping[str, Any], records: Iterable[CandleRecord]
) -> str:
    """Hash exact visible facts without coupling identity to row revisions."""

    rows = sorted(records, key=lambda item: item.fact.open_time)
    seen: set[datetime] = set()
    material: list[dict[str, Any]] = []
    for record in rows:
        if record.fact.open_time in seen:
            raise ValueError("candle_material_hash_invalid: duplicate candle time")
        seen.add(record.fact.open_time)
        material.append(
            {
                "open_time": _canonical_time(record.fact.open_time),
                "row_hash": record.fact.row_hash,
            }
        )
    return _stable_hash(
        {
            "schema_version": CANDLE_MATERIAL_HASH_VERSION,
            "series": dict(series_identity),
            "rows": material,
        }
    )


def build_open_interest_material_hash(
    *,
    series_identity: Mapping[str, Any],
    records: Iterable[OpenInterestRecord],
) -> str:
    """Hash exact scheduled OI samples without coupling identity to revisions."""

    rows = sorted(records, key=lambda item: item.fact.sample_time)
    seen: set[datetime] = set()
    material: list[dict[str, Any]] = []
    for record in rows:
        if record.fact.sample_time in seen:
            raise ValueError(
                "open_interest_material_hash_invalid: duplicate sample_time"
            )
        seen.add(record.fact.sample_time)
        material.append(
            {
                "sample_time": _canonical_time(record.fact.sample_time),
                "row_hash": record.fact.row_hash,
            }
        )
    return _stable_hash(
        {
            "schema_version": OPEN_INTEREST_MATERIAL_HASH_VERSION,
            "series": dict(series_identity),
            "rows": material,
        }
    )


def build_funding_rate_material_hash(
    *,
    series_identity: Mapping[str, Any],
    records: Iterable[FundingRateRecord],
) -> str:
    """Hash exact scheduled funding observations without revision coupling."""

    rows = sorted(records, key=lambda item: item.fact.sample_time)
    seen: set[datetime] = set()
    material: list[dict[str, Any]] = []
    for record in rows:
        if record.fact.sample_time in seen:
            raise ValueError(
                "funding_rate_material_hash_invalid: duplicate sample_time"
            )
        seen.add(record.fact.sample_time)
        material.append(
            {
                "sample_time": _canonical_time(record.fact.sample_time),
                "row_hash": record.fact.row_hash,
            }
        )
    return _stable_hash(
        {
            "schema_version": FUNDING_RATE_MATERIAL_HASH_VERSION,
            "series": dict(series_identity),
            "rows": material,
        }
    )


def build_numeric_fact_material_hash(
    *,
    series_identity: Mapping[str, Any],
    records: Iterable[NumericFactRecord],
) -> str:
    """Hash every causal event revision without coupling identity to DB ordinals."""

    rows = sorted(
        records,
        key=lambda item: (item.fact.source_event_key, int(item.revision)),
    )
    event_ordinals: dict[str, int] = {}
    material: list[dict[str, Any]] = []
    for record in rows:
        event_key = record.fact.source_event_key
        event_ordinal = event_ordinals.get(event_key, 0) + 1
        event_ordinals[event_key] = event_ordinal
        material.append(
            {
                "effective_at": _canonical_time(record.fact.effective_at),
                "source_event_key": event_key,
                "event_revision_ordinal": event_ordinal,
                "row_hash": record.fact.row_hash,
            }
        )
    return _stable_hash(
        {
            "schema_version": NUMERIC_FACT_MATERIAL_HASH_VERSION,
            "series": dict(series_identity),
            "rows": material,
        }
    )


from .structure import MarketTradeRecord, TradeFlowAggregateRecord


DERIVED_FEATURE_SERIES_MATERIAL_HASH_VERSION = "market.derived_feature_series.v1"


@dataclass(frozen=True)
class TypedFeatureRecord:
    """Storage envelope for one typed derived or normalized fact revision."""

    version_id: str
    series_id: int
    revision: int
    market_commit_seq: int
    provenance_hash: str
    quality: Mapping[str, Any]
    fact: Any

    def __post_init__(self) -> None:
        version_id = str(self.version_id or "").strip()
        provenance_hash = str(self.provenance_hash or "").strip().lower()
        if not version_id or int(self.series_id) <= 0:
            raise ValueError("market_feature_record_invalid: identity")
        if int(self.revision) <= 0 or int(self.market_commit_seq) <= 0:
            raise ValueError("market_feature_record_invalid: revision")
        if len(provenance_hash) != 64:
            raise ValueError("market_feature_record_invalid: provenance hash")
        if int(getattr(self.fact, "series_id", 0)) != int(self.series_id):
            raise ValueError("market_feature_record_invalid: fact series disagreement")
        material_hash = str(getattr(self.fact, "material_hash", "") or "").lower()
        if len(material_hash) != 64:
            raise ValueError("market_feature_record_invalid: fact material hash")
        object.__setattr__(self, "version_id", version_id)
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "revision", int(self.revision))
        object.__setattr__(self, "market_commit_seq", int(self.market_commit_seq))
        object.__setattr__(self, "provenance_hash", provenance_hash)
        object.__setattr__(self, "quality", dict(self.quality))

MarketDataRecord = (
    CandleRecord
    | OpenInterestRecord
    | FundingRateRecord
    | NumericFactRecord
    | MarketTradeRecord
    | TradeFlowAggregateRecord
    | TypedFeatureRecord
)


def _record_time(record: MarketDataRecord) -> datetime:
    if isinstance(record, CandleRecord):
        return record.fact.open_time
    if isinstance(record, OpenInterestRecord):
        return record.fact.sample_time
    if isinstance(record, FundingRateRecord):
        return record.fact.sample_time
    if isinstance(record, NumericFactRecord):
        return record.fact.effective_at
    if isinstance(record, MarketTradeRecord):
        return record.fact.provider_event_time
    if isinstance(record, TradeFlowAggregateRecord):
        return record.fact.bucket_start
    if isinstance(record, TypedFeatureRecord):
        for field in ("bucket_start", "effective_at", "sample_time", "provider_event_time"):
            value = getattr(record.fact, field, None)
            if isinstance(value, datetime):
                return value
        raise TypeError(
            f"market_data_record_invalid: feature has no effective time {type(record.fact).__name__}"
        )
    raise TypeError(
        f"market_data_record_invalid: unsupported record type {type(record).__name__}"
    )


def record_effective_time(record: MarketDataRecord) -> datetime:
    """Return the canonical event/effective time for any typed record."""

    return _record_time(record)


def build_typed_feature_material_hash(
    *,
    series_identity: Mapping[str, Any],
    records: Iterable[TypedFeatureRecord],
) -> str:
    """Hash exact typed feature values independently of storage revisions."""

    rows = sorted(
        records,
        key=lambda record: (_record_time(record), record.fact.material_hash),
    )
    if not rows:
        raise ValueError("market_feature_material_hash_invalid: records are required")
    seen: set[str] = set()
    material: list[dict[str, Any]] = []
    for record in rows:
        if record.version_id in seen:
            raise ValueError("market_feature_material_hash_invalid: duplicate version")
        seen.add(record.version_id)
        material.append(
            {
                "fact_time": _canonical_time(_record_time(record)),
                "material_hash": record.fact.material_hash,
            }
        )
    return _stable_hash(
        {
            "schema_version": DERIVED_FEATURE_SERIES_MATERIAL_HASH_VERSION,
            "series": dict(series_identity),
            "rows": material,
        }
    )


def build_provenance_hash(records: Iterable[MarketDataRecord]) -> str:
    """Hash acquisition lineage for exact visible typed-fact revisions."""

    rows = sorted(
        records,
        key=lambda record: (
            _record_time(record),
            str(getattr(record.fact, "source_event_key", "")),
            (
                int(record.revision)
                if isinstance(record, NumericFactRecord)
                else 0
            ),
        ),
    )
    normalized: list[dict[str, Any]] = []
    market_structure_records = any(
        isinstance(record, (MarketTradeRecord, TradeFlowAggregateRecord, TypedFeatureRecord))
        for record in rows
    )
    for record in rows:
        if isinstance(record, TypedFeatureRecord):
            normalized.append(
                {
                    "fact_time": _canonical_time(_record_time(record)),
                    "material_hash": record.fact.material_hash,
                    "version_id": record.version_id,
                    "provenance_hash": record.provenance_hash,
                    "quality": dict(record.quality),
                }
            )
            continue

        if isinstance(record, MarketTradeRecord):
            normalized.append(
                {
                    "fact_time": _canonical_time(_record_time(record)),
                    "row_hash": record.fact.row_hash,
                    "source_id": record.source_id,
                    "version_id": record.version_id,
                    "provenance_hash": record.provenance_hash,
                    "quality": dict(record.quality),
                }
            )
            continue
        if isinstance(record, TradeFlowAggregateRecord):
            normalized.append(
                {
                    "fact_time": _canonical_time(_record_time(record)),
                    "material_hash": record.fact.material_hash,
                    "version_id": record.version_id,
                    "aggregation_version": record.aggregation_version,
                    "provenance_hash": record.provenance_hash,
                    "quality": dict(record.quality),
                }
            )
            continue
        provenance = json.loads(
            json.dumps(
                dict(record.provenance),
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
        )
        normalized.append(
            {
                "fact_time": _canonical_time(_record_time(record)),
                "row_hash": record.fact.row_hash,
                "source_identity_key": record.source_identity_key,
                "ingestion_run_id": record.ingestion_run_id,
                "provenance": provenance,
            }
        )
    return _stable_hash(
        {
            "schema_version": (
                "market_data_provenance_hash.v3"
                if market_structure_records
                else "market_data_provenance_hash.v2"
            ),
            "records": normalized,
        }
    )


def build_dataset_identity_hash(series: Iterable[Mapping[str, Any]]) -> str:
    """Hash exact series material, provenance, and quality, not DB watermarks."""

    normalized: list[dict[str, Any]] = []
    for raw in series:
        entry = {
            str(key): value
            for key, value in dict(raw).items()
            if str(key) != "max_commit_seq"
        }
        normalized.append(
            json.loads(
                json.dumps(
                    entry, sort_keys=True, separators=(",", ":"), default=str
                )
            )
        )
    normalized.sort(
        key=lambda item: (
            int(item.get("series_id") or 0),
            str(item.get("range_start") or ""),
            str(item.get("range_end") or ""),
        )
    )
    if not normalized:
        raise ValueError("market_dataset_invalid: at least one series is required")
    return _stable_hash(
        {"schema_version": DATASET_IDENTITY_HASH_VERSION, "series": normalized}
    )


def dataset_series_identity_payload(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Project the stable v1 Dataset-series identity fields.

    Full quality evidence is pinned beside the manifest. Its material identity
    is represented by ``quality_hash`` so historical v1 Dataset hashes remain
    reconstructable after the evidence array became directly persisted.
    """

    return {
        "series_id": int(entry["series_id"]),
        "range_start": _canonical_time(
            _utc_datetime(entry["range_start"], field="range_start")
        ),
        "range_end": _canonical_time(
            _utc_datetime(entry["range_end"], field="range_end")
        ),
        "max_commit_seq": int(entry["max_commit_seq"]),
        "row_count": int(entry["row_count"]),
        "material_hash": str(entry["material_hash"]),
        "provenance_hash": str(entry["provenance_hash"]),
        "source_summary": dict(entry.get("source_summary") or {}),
        "quality_hash": str(entry["quality_hash"]),
        "quality_summary": dict(entry.get("quality_summary") or {}),
    }


def build_quality_hash(evidence: Iterable[Mapping[str, Any]]) -> str:
    """Hash quality evidence separately from exact candle material."""

    normalized = [
        json.loads(
            json.dumps(
                dict(item), sort_keys=True, separators=(",", ":"), default=str
            )
        )
        for item in evidence
    ]
    normalized.sort(
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"))
    )
    return _stable_hash(
        {"schema_version": QUALITY_HASH_VERSION, "evidence": normalized}
    )


__all__ = [
    "CANDLE_FACT_TYPE",
    "CANDLE_FACT_VERSION",
    "DATASET_IDENTITY_HASH_VERSION",
    "FUNDING_RATE_FACT_TYPE",
    "FUNDING_RATE_FACT_VERSION",
    "OPEN_INTEREST_FACT_TYPE",
    "OPEN_INTEREST_FACT_VERSION",
    "NUMERIC_FACT_MATERIAL_HASH_VERSION",
    "CandleFact",
    "CandleRecord",
    "DatasetSeriesRequest",
    "FundingRateFact",
    "FundingRateRecord",
    "InstrumentRole",
    "MarketDataAlignment",
    "MarketDataRecord",
    "MarketDataRequirement",
    "MarketDataWindow",
    "NumericFact",
    "NumericFactRecord",
    "NumericFactState",
    "OpenInterestFact",
    "OpenInterestRecord",
    "SourceIdentity",
    "build_candle_material_hash",
    "build_dataset_identity_hash",
    "dataset_series_identity_payload",
    "build_funding_rate_material_hash",
    "build_open_interest_material_hash",
    "build_numeric_fact_material_hash",
    "build_provenance_hash",
    "build_quality_hash",
]
