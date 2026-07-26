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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional


CANDLE_FACT_TYPE = "candle.ohlcv"
CANDLE_FACT_VERSION = "candle.ohlcv.v1"
CANDLE_MATERIAL_HASH_VERSION = "candle_material_hash.v1"
DATASET_IDENTITY_HASH_VERSION = "market_dataset.v1"
QUALITY_HASH_VERSION = "market_data_quality_hash.v1"

_RECEIPT_KNOWN_AT_METHODS = frozenset(
    {"platform_acceptance", "platform_receipt", "stream_receipt"}
)


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

    fact_type: str
    timeframe_seconds: Optional[int] = None
    contract_version: str = CANDLE_FACT_VERSION
    required: bool = True

    def __post_init__(self) -> None:
        fact_type = str(self.fact_type or "").strip().lower()
        contract_version = str(self.contract_version or "").strip()
        if not fact_type:
            raise ValueError("market_data_requirement_invalid: fact_type is required")
        if not contract_version:
            raise ValueError(
                "market_data_requirement_invalid: contract_version is required"
            )
        if self.timeframe_seconds is not None and int(self.timeframe_seconds) <= 0:
            raise ValueError(
                "market_data_requirement_invalid: timeframe_seconds must be positive"
            )
        if fact_type == CANDLE_FACT_TYPE and self.timeframe_seconds is None:
            raise ValueError(
                "market_data_requirement_invalid: candle facts require timeframe_seconds"
            )
        object.__setattr__(self, "fact_type", fact_type)
        object.__setattr__(self, "contract_version", contract_version)
        if self.timeframe_seconds is not None:
            object.__setattr__(self, "timeframe_seconds", int(self.timeframe_seconds))


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
        if fact_type == CANDLE_FACT_TYPE and self.timeframe_seconds is None:
            raise ValueError(
                "market_data_window_invalid: candle facts require timeframe_seconds"
            )
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


def build_provenance_hash(records: Iterable[CandleRecord]) -> str:
    """Hash the acquisition lineage for exact visible candle revisions."""

    rows = sorted(records, key=lambda item: item.fact.open_time)
    normalized: list[dict[str, Any]] = []
    for record in rows:
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
                "open_time": _canonical_time(record.fact.open_time),
                "row_hash": record.fact.row_hash,
                "source_identity_key": record.source_identity_key,
                "ingestion_run_id": record.ingestion_run_id,
                "provenance": provenance,
            }
        )
    return _stable_hash(
        {
            "schema_version": "market_data_provenance_hash.v1",
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
    "CandleFact",
    "CandleRecord",
    "DatasetSeriesRequest",
    "MarketDataRequirement",
    "MarketDataWindow",
    "SourceIdentity",
    "build_candle_material_hash",
    "build_dataset_identity_hash",
    "build_provenance_hash",
    "build_quality_hash",
]
