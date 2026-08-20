"""Versioned fee schedules and notional primitives for runtime execution."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_UP
from typing import Any, Mapping, Optional


FEE_SCHEDULE_SCHEMA_VERSION = "fee_schedule.v1"
_FEE_ROUNDING = {
    "down": ROUND_DOWN,
    "half_even": ROUND_HALF_EVEN,
    "half_up": ROUND_HALF_UP,
    "up": ROUND_UP,
}
_FEE_BASES = {"quote_notional", "base_quantity"}


@dataclass(frozen=True)
class FeeSchedule:
    """Immutable, versioned fee facts used by execution models.

    The first four fields preserve the legacy constructor. Resolved execution
    contexts populate the remaining identity, currency, rounding, tier, and
    provenance fields explicitly and pin ``schedule_hash`` in the run bundle.
    """

    maker_rate: float
    taker_rate: float
    source: str
    version: Optional[str] = None
    schema_version: str = FEE_SCHEDULE_SCHEMA_VERSION
    schedule_id: str = "legacy_unscoped"
    venue_profile_id: str = "legacy_unscoped"
    fee_currency: str = "quote"
    calculation_basis: str = "quote_notional"
    rounding_mode: str = "unrounded"
    precision: Optional[int] = None
    tier: str = "default"
    configured: bool = True
    verified_zero: bool = False
    schedule_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != FEE_SCHEDULE_SCHEMA_VERSION:
            raise ValueError(f"unsupported fee schedule schema_version: {self.schema_version}")
        for field_name in (
            "schedule_id",
            "venue_profile_id",
            "source",
            "fee_currency",
            "tier",
        ):
            if not str(getattr(self, field_name) or "").strip():
                raise ValueError(f"fee schedule {field_name} is required")
        for field_name, value in (("maker_rate", self.maker_rate), ("taker_rate", self.taker_rate)):
            if isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError(f"fee schedule {field_name} must be finite")
        if self.calculation_basis not in _FEE_BASES:
            raise ValueError(
                "fee schedule calculation_basis must be quote_notional or base_quantity"
            )
        if self.rounding_mode != "unrounded" and self.rounding_mode not in _FEE_ROUNDING:
            raise ValueError(
                "fee schedule rounding_mode must be unrounded, down, half_even, half_up, or up"
            )
        if self.rounding_mode == "unrounded" and self.precision is not None:
            raise ValueError("unrounded fee schedules must not declare precision")
        if self.rounding_mode != "unrounded":
            if isinstance(self.precision, bool) or not isinstance(self.precision, int) or self.precision < 0:
                raise ValueError("rounded fee schedules require a non-negative integer precision")
        if not isinstance(self.configured, bool) or not isinstance(self.verified_zero, bool):
            raise ValueError("fee schedule configured and verified_zero must be booleans")
        if not self.configured and self.verified_zero:
            raise ValueError("an unconfigured fee schedule cannot be verified zero")
        if self.maker_rate == 0.0 and self.taker_rate == 0.0 and self.configured and not self.verified_zero:
            # The schedule remains constructible for exploratory compatibility,
            # but callers cannot treat it as verified economic evidence.
            pass
        expected_hash = _stable_hash(self._material())
        if self.schedule_hash and self.schedule_hash != expected_hash:
            raise ValueError("fee_schedule_hash_mismatch")
        object.__setattr__(self, "schedule_hash", expected_hash)

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "schedule_id": str(self.schedule_id),
            "venue_profile_id": str(self.venue_profile_id),
            "version": str(self.version or ""),
            "maker_rate": float(self.maker_rate),
            "taker_rate": float(self.taker_rate),
            "source": str(self.source),
            "fee_currency": str(self.fee_currency),
            "calculation_basis": str(self.calculation_basis),
            "rounding_mode": str(self.rounding_mode),
            "precision": self.precision,
            "tier": str(self.tier),
            "configured": self.configured,
            "verified_zero": self.verified_zero,
        }

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "FeeSchedule":
        if not isinstance(raw, Mapping) or not raw:
            raise ValueError("fee schedule manifest must be a non-empty object")
        return cls(
            maker_rate=float(raw.get("maker_rate")),
            taker_rate=float(raw.get("taker_rate")),
            source=str(raw.get("source") or "").strip(),
            version=str(raw.get("version") or "").strip() or None,
            schema_version=str(raw.get("schema_version") or "").strip(),
            schedule_id=str(raw.get("schedule_id") or "").strip(),
            venue_profile_id=str(raw.get("venue_profile_id") or "").strip(),
            fee_currency=str(raw.get("fee_currency") or "").strip(),
            calculation_basis=str(raw.get("calculation_basis") or "").strip(),
            rounding_mode=str(raw.get("rounding_mode") or "").strip(),
            precision=raw.get("precision"),
            tier=str(raw.get("tier") or "").strip(),
            configured=raw.get("configured"),
            verified_zero=raw.get("verified_zero"),
            schedule_hash=str(raw.get("schedule_hash") or "").strip(),
        )


@dataclass(frozen=True)
class FeeDetail:
    """Resolved fee information from an execution outcome."""

    role: str
    fee_rate: float
    notional: float
    fee_paid: float
    source: str
    version: Optional[str] = None
    currency: str = "quote"
    calculation_basis: str = "quote_notional"
    rounding_mode: str = "unrounded"
    precision: Optional[int] = None
    tier: str = "default"
    schedule_hash: Optional[str] = None


def executed_notional(*, price: float, quantity: float, contract_size: float) -> float:
    """Canonical executed notional for all fee, fill, and reservation paths."""

    return abs(float(price) * float(quantity) * float(contract_size))


def executed_fee(
    *,
    price: float,
    quantity: float,
    contract_size: float,
    fee_rate: float,
) -> float:
    """Canonical fee calculation for an executed fill."""

    return float(fee_rate or 0.0) * executed_notional(
        price=price,
        quantity=quantity,
        contract_size=contract_size,
    )


def rounded_fee(value: float, *, mode: str, precision: Optional[int]) -> float:
    """Apply one deterministic schedule-owned fee rounding rule."""

    if mode == "unrounded":
        return float(value)
    if mode not in _FEE_ROUNDING or precision is None:
        raise ValueError("rounded fee calculation requires a supported mode and precision")
    quantum = Decimal(1).scaleb(-int(precision))
    return float(Decimal(str(value)).quantize(quantum, rounding=_FEE_ROUNDING[mode]))


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class FeeResolver:
    """Centralized fee resolver for maker/taker classification."""

    def __init__(self, schedule: FeeSchedule) -> None:
        self.schedule = schedule

    def resolve(
        self,
        *,
        role: str,
        price: float,
        quantity: float,
        contract_size: float,
    ) -> FeeDetail:
        normalized_role = "maker" if str(role or "").lower() == "maker" else "taker"
        rate = self.schedule.maker_rate if normalized_role == "maker" else self.schedule.taker_rate
        notional = executed_notional(
            price=price,
            quantity=quantity,
            contract_size=contract_size,
        )
        fee_basis = (
            abs(float(quantity) * float(contract_size))
            if self.schedule.calculation_basis == "base_quantity"
            else notional
        )
        fee_paid = rounded_fee(
            float(rate or 0.0) * fee_basis,
            mode=self.schedule.rounding_mode,
            precision=self.schedule.precision,
        )
        return FeeDetail(
            role=normalized_role,
            fee_rate=float(rate or 0.0),
            notional=float(notional),
            fee_paid=float(fee_paid),
            source=self.schedule.source,
            version=self.schedule.version,
            currency=self.schedule.fee_currency,
            calculation_basis=self.schedule.calculation_basis,
            rounding_mode=self.schedule.rounding_mode,
            precision=self.schedule.precision,
            tier=self.schedule.tier,
            schedule_hash=self.schedule.schedule_hash,
        )


__all__ = [
    "FEE_SCHEDULE_SCHEMA_VERSION",
    "FeeSchedule",
    "FeeDetail",
    "FeeResolver",
    "executed_fee",
    "executed_notional",
    "rounded_fee",
]
