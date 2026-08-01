"""Normalized acquisition-only contracts emitted by provider adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
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


__all__ = ["ProviderOpenInterestSnapshot"]
