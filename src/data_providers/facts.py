"""Normalized acquisition-only contracts emitted by provider adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class ProviderOpenInterestSnapshot:
    """One provider response normalized without assigning platform known-at time."""

    provider_product_id: str
    value: float
    received_at: datetime
    response_hash: str
    source_path: str
    provider_event_at: Optional[datetime] = None
    unit: str = "contracts"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        product_id = str(self.provider_product_id or "").strip()
        response_hash = str(self.response_hash or "").strip().lower()
        source_path = str(self.source_path or "").strip()
        if not product_id or not source_path:
            raise ValueError(
                "provider_open_interest_invalid: product ID and source path are required"
            )
        if len(response_hash) != 64 or any(
            character not in "0123456789abcdef" for character in response_hash
        ):
            raise ValueError(
                "provider_open_interest_invalid: response_hash must be SHA-256 hex"
            )
        if isinstance(self.value, bool):
            raise ValueError(
                "provider_open_interest_invalid: value must be finite and nonnegative"
            )
        try:
            value = float(self.value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "provider_open_interest_invalid: value must be finite and nonnegative"
            ) from exc
        if not math.isfinite(value) or value < 0:
            raise ValueError(
                "provider_open_interest_invalid: value must be finite and nonnegative"
            )
        unit = str(self.unit or "").strip().lower()
        if unit != "contracts":
            raise ValueError(
                "provider_open_interest_invalid: unit must be contracts"
            )
        received_at = self.received_at
        if received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=timezone.utc)
        received_at = received_at.astimezone(timezone.utc)
        provider_event_at = self.provider_event_at
        if provider_event_at is not None:
            if provider_event_at.tzinfo is None:
                provider_event_at = provider_event_at.replace(tzinfo=timezone.utc)
            provider_event_at = provider_event_at.astimezone(timezone.utc)
            if provider_event_at > received_at:
                raise ValueError(
                    "provider_open_interest_invalid: provider event cannot follow receipt"
                )
        object.__setattr__(self, "provider_product_id", product_id)
        object.__setattr__(self, "value", 0.0 if value == 0.0 else value)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "response_hash", response_hash)
        object.__setattr__(self, "source_path", source_path)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "provider_event_at", provider_event_at)
        object.__setattr__(self, "metadata", dict(self.metadata or {}))


@dataclass(frozen=True)
class ProviderFundingRateSnapshot:
    """One provider funding-rate observation before platform known-at assignment."""

    provider_product_id: str
    rate: float
    funding_time: datetime
    interval_seconds: int
    received_at: datetime
    response_hash: str
    source_path: str
    unit: str = "fraction"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        product_id = str(self.provider_product_id or "").strip()
        response_hash = str(self.response_hash or "").strip().lower()
        source_path = str(self.source_path or "").strip()
        if not product_id or not source_path:
            raise ValueError(
                "provider_funding_rate_invalid: product ID and source path are required"
            )
        if len(response_hash) != 64 or any(
            character not in "0123456789abcdef" for character in response_hash
        ):
            raise ValueError(
                "provider_funding_rate_invalid: response_hash must be SHA-256 hex"
            )
        if isinstance(self.rate, bool):
            raise ValueError("provider_funding_rate_invalid: rate must be finite")
        try:
            rate = float(self.rate)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "provider_funding_rate_invalid: rate must be finite"
            ) from exc
        if not math.isfinite(rate):
            raise ValueError("provider_funding_rate_invalid: rate must be finite")
        if isinstance(self.interval_seconds, bool):
            raise ValueError(
                "provider_funding_rate_invalid: interval_seconds must be positive"
            )
        try:
            interval_seconds = int(self.interval_seconds)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "provider_funding_rate_invalid: interval_seconds must be positive"
            ) from exc
        if interval_seconds <= 0:
            raise ValueError(
                "provider_funding_rate_invalid: interval_seconds must be positive"
            )
        unit = str(self.unit or "").strip().lower()
        if unit != "fraction":
            raise ValueError(
                "provider_funding_rate_invalid: unit must be fraction"
            )
        received_at = self.received_at
        if received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=timezone.utc)
        received_at = received_at.astimezone(timezone.utc)
        funding_time = self.funding_time
        if funding_time.tzinfo is None:
            funding_time = funding_time.replace(tzinfo=timezone.utc)
        funding_time = funding_time.astimezone(timezone.utc)
        object.__setattr__(self, "provider_product_id", product_id)
        object.__setattr__(self, "rate", 0.0 if rate == 0.0 else rate)
        object.__setattr__(self, "funding_time", funding_time)
        object.__setattr__(self, "interval_seconds", interval_seconds)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "response_hash", response_hash)
        object.__setattr__(self, "source_path", source_path)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "metadata", dict(self.metadata or {}))


@dataclass(frozen=True)
class ProviderReserveStateSnapshot:
    """One atomic reserve report before canonical QT identity is assigned."""

    subject_id: str
    report_id: str
    reserve_asset: str
    reserve_quantity: Decimal | int | str
    raw_reserve_quantity: str
    observation_time: datetime
    received_at: datetime
    response_hash: str
    source_path: str
    source_event_key: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        subject_id = str(self.subject_id or "").strip()
        report_id = str(self.report_id or "").strip()
        reserve_asset = str(self.reserve_asset or "").strip().upper()
        raw_quantity = str(self.raw_reserve_quantity or "").strip()
        source_path = str(self.source_path or "").strip()
        source_event_key = str(self.source_event_key or "").strip()
        response_hash = str(self.response_hash or "").strip().lower()
        if not all(
            (
                subject_id,
                report_id,
                reserve_asset,
                raw_quantity,
                source_path,
                source_event_key,
            )
        ):
            raise ValueError("provider_reserve_state_invalid: identity is incomplete")
        if len(response_hash) != 64 or any(
            character not in "0123456789abcdef" for character in response_hash
        ):
            raise ValueError(
                "provider_reserve_state_invalid: response_hash must be SHA-256 hex"
            )
        if isinstance(self.reserve_quantity, (bool, float)):
            raise ValueError(
                "provider_reserve_state_invalid: reserve quantity must be exact"
            )
        try:
            quantity = (
                self.reserve_quantity
                if isinstance(self.reserve_quantity, Decimal)
                else Decimal(str(self.reserve_quantity))
            )
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(
                "provider_reserve_state_invalid: reserve quantity must be exact"
            ) from exc
        if not quantity.is_finite() or quantity < 0:
            raise ValueError(
                "provider_reserve_state_invalid: reserve quantity must be finite and nonnegative"
            )
        observation_time = self.observation_time
        received_at = self.received_at
        for field_name, value in (
            ("observation_time", observation_time),
            ("received_at", received_at),
        ):
            if not isinstance(value, datetime):
                raise ValueError(
                    f"provider_reserve_state_invalid: {field_name} must be datetime"
                )
        if observation_time.tzinfo is None:
            observation_time = observation_time.replace(tzinfo=timezone.utc)
        if received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=timezone.utc)
        observation_time = observation_time.astimezone(timezone.utc)
        received_at = received_at.astimezone(timezone.utc)
        if observation_time > received_at:
            raise ValueError(
                "provider_reserve_state_invalid: observation cannot follow receipt"
            )
        object.__setattr__(self, "subject_id", subject_id)
        object.__setattr__(self, "report_id", report_id)
        object.__setattr__(self, "reserve_asset", reserve_asset)
        object.__setattr__(self, "reserve_quantity", Decimal(0) if quantity.is_zero() else quantity)
        object.__setattr__(self, "raw_reserve_quantity", raw_quantity)
        object.__setattr__(self, "observation_time", observation_time)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "response_hash", response_hash)
        object.__setattr__(self, "source_path", source_path)
        object.__setattr__(self, "source_event_key", source_event_key)
        object.__setattr__(self, "metadata", dict(self.metadata or {}))


__all__ = [
    "ProviderFundingRateSnapshot",
    "ProviderOpenInterestSnapshot",
    "ProviderReserveStateSnapshot",
]
