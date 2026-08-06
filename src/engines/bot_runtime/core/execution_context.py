"""Immutable venue-neutral contracts resolved for deterministic execution.

Phase 2A separates product facts, venue rules, fees, and fill-model evidence.
The resulting context is pinned per series in the run snapshot. Generic
execution code consumes this contract and never switches on a venue name.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

from .amount_constraints import AmountConstraints, normalize_qty_with_constraints
from .execution_assumptions import ResolvedExecutionAssumptions
from .fees import FEE_SCHEDULE_SCHEMA_VERSION, FeeSchedule


INSTRUMENT_EXECUTION_CONTRACT_SCHEMA_VERSION = "instrument_execution_contract.v1"
VENUE_EXECUTION_PROFILE_SCHEMA_VERSION = "venue_execution_profile.v1"
EXECUTION_MODEL_ARTIFACT_SCHEMA_VERSION = "execution_model_artifact.v1"
RESOLVED_EXECUTION_CONTEXT_SCHEMA_VERSION = "resolved_execution_context.v1"
RESOLVED_EXECUTION_CONTEXT_BUNDLE_SCHEMA_VERSION = "resolved_execution_context_bundle.v1"

_ORDER_TYPES = {
    "market",
    "limit_aggressive",
    "limit_maker",
    "limit_resting",
    "stop_market",
}
_TIME_IN_FORCE = {"gtc", "ioc", "fok", "day"}
_BOOK_CAPABILITIES = {"bars", "l1", "l2", "l3"}
_INCREMENT_POLICIES = {"reject", "round_down"}
_POST_ONLY_BEHAVIORS = {"reject_would_cross", "cancel_would_cross"}


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _finite_number(value: Any, *, field: str, optional: bool = False) -> float | None:
    if value in (None, ""):
        if optional:
            return None
        raise ValueError(f"{field} is required")
    if isinstance(value, bool):
        raise ValueError(f"{field} must be finite")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be finite")
    return parsed


def _positive_number(value: Any, *, field: str, optional: bool = False) -> float | None:
    parsed = _finite_number(value, field=field, optional=optional)
    if parsed is not None and parsed <= 0:
        raise ValueError(f"{field} must be positive")
    return parsed


def _optional_int(value: Any, *, field: str) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _required_text(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _text_tuple(raw: Any, *, field: str, allowed: set[str]) -> tuple[str, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError(f"{field} must be a list")
    values = tuple(sorted({_required_text(item, field=field).lower() for item in raw}))
    if not values:
        raise ValueError(f"{field} must not be empty")
    unsupported = sorted(set(values) - allowed)
    if unsupported:
        raise ValueError(f"{field} contains unsupported values: {','.join(unsupported)}")
    return values


def _pair_tuple(raw: Any, *, field: str) -> tuple[tuple[str, str], ...]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"{field} must be an object")
    pairs: list[tuple[str, str]] = []
    for key, value in raw.items():
        left = _required_text(key, field=f"{field}.key").lower()
        right = _required_text(value, field=f"{field}.{left}").lower()
        pairs.append((left, right))
    return tuple(sorted(pairs))


@dataclass(frozen=True)
class InstrumentExecutionContract:
    """Static instrument and product facts used by execution/accounting."""

    schema_version: str
    instrument_id: str | None
    symbol: str
    instrument_type: str
    source_instrument_type: str
    execution_semantics: str
    research_market_role: str
    datasource: str | None
    exchange: str | None
    base_currency: str
    quote_currency: str
    tick_size: float
    contract_size: float
    tick_value: float
    min_order_size: float | None
    qty_step: float | None
    max_qty: float | None
    min_notional: float | None
    amount_precision: int | None
    supports_margin: bool
    supports_short: bool
    short_requires_borrow: bool
    has_funding: bool
    has_expiry: bool
    accounting_mode: str | None
    margin_calc_type: str
    margin_intraday_long: float | None
    margin_intraday_short: float | None
    margin_overnight_long: float | None
    margin_overnight_short: float | None
    contract_hash: str

    def __post_init__(self) -> None:
        if self.schema_version != INSTRUMENT_EXECUTION_CONTRACT_SCHEMA_VERSION:
            raise ValueError(f"unsupported instrument execution schema: {self.schema_version}")
        _required_text(self.symbol, field="instrument_execution_contract.symbol")
        _required_text(self.instrument_type, field="instrument_execution_contract.instrument_type")
        _required_text(
            self.source_instrument_type,
            field="instrument_execution_contract.source_instrument_type",
        )
        _required_text(self.execution_semantics, field="instrument_execution_contract.execution_semantics")
        _required_text(
            self.research_market_role,
            field="instrument_execution_contract.research_market_role",
        )
        _required_text(self.base_currency, field="instrument_execution_contract.base_currency")
        _required_text(self.quote_currency, field="instrument_execution_contract.quote_currency")
        _required_text(self.margin_calc_type, field="instrument_execution_contract.margin_calc_type")
        _positive_number(self.tick_size, field="instrument_execution_contract.tick_size")
        _positive_number(self.contract_size, field="instrument_execution_contract.contract_size")
        _positive_number(self.tick_value, field="instrument_execution_contract.tick_value")
        _positive_number(self.min_order_size, field="instrument_execution_contract.min_order_size", optional=True)
        _positive_number(self.qty_step, field="instrument_execution_contract.qty_step", optional=True)
        _positive_number(self.max_qty, field="instrument_execution_contract.max_qty", optional=True)
        _positive_number(self.min_notional, field="instrument_execution_contract.min_notional", optional=True)
        _optional_int(self.amount_precision, field="instrument_execution_contract.amount_precision")
        for name in (
            "supports_margin",
            "supports_short",
            "short_requires_borrow",
            "has_funding",
            "has_expiry",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"instrument_execution_contract.{name} must be boolean")
        expected = _stable_hash(self._material())
        if self.contract_hash and self.contract_hash != expected:
            raise ValueError("instrument_execution_contract_hash_mismatch")
        object.__setattr__(self, "contract_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "contract_hash"}

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "InstrumentExecutionContract":
        if not isinstance(raw, Mapping) or not raw:
            raise ValueError("instrument execution contract must be a non-empty object")
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            instrument_id=str(raw.get("instrument_id") or "").strip() or None,
            symbol=str(raw.get("symbol") or "").strip(),
            instrument_type=str(raw.get("instrument_type") or "unknown").strip(),
            source_instrument_type=str(raw.get("source_instrument_type") or "unknown").strip(),
            execution_semantics=str(raw.get("execution_semantics") or "").strip(),
            research_market_role=str(raw.get("research_market_role") or "").strip(),
            datasource=str(raw.get("datasource") or "").strip() or None,
            exchange=str(raw.get("exchange") or "").strip() or None,
            base_currency=str(raw.get("base_currency") or "").strip(),
            quote_currency=str(raw.get("quote_currency") or "").strip(),
            tick_size=float(raw.get("tick_size")),
            contract_size=float(raw.get("contract_size")),
            tick_value=float(raw.get("tick_value")),
            min_order_size=_finite_number(raw.get("min_order_size"), field="min_order_size", optional=True),
            qty_step=_finite_number(raw.get("qty_step"), field="qty_step", optional=True),
            max_qty=_finite_number(raw.get("max_qty"), field="max_qty", optional=True),
            min_notional=_finite_number(raw.get("min_notional"), field="min_notional", optional=True),
            amount_precision=raw.get("amount_precision"),
            supports_margin=raw.get("supports_margin"),
            supports_short=raw.get("supports_short"),
            short_requires_borrow=raw.get("short_requires_borrow"),
            has_funding=raw.get("has_funding"),
            has_expiry=raw.get("has_expiry"),
            accounting_mode=str(raw.get("accounting_mode") or "").strip() or None,
            margin_calc_type=str(raw.get("margin_calc_type") or "").strip(),
            margin_intraday_long=_finite_number(raw.get("margin_intraday_long"), field="margin_intraday_long", optional=True),
            margin_intraday_short=_finite_number(raw.get("margin_intraday_short"), field="margin_intraday_short", optional=True),
            margin_overnight_long=_finite_number(raw.get("margin_overnight_long"), field="margin_overnight_long", optional=True),
            margin_overnight_short=_finite_number(raw.get("margin_overnight_short"), field="margin_overnight_short", optional=True),
            contract_hash=str(raw.get("contract_hash") or ""),
        )

    @property
    def amount_constraints(self) -> AmountConstraints:
        return AmountConstraints(
            min_qty=self.min_order_size,
            max_qty=self.max_qty,
            qty_step=self.qty_step,
            min_notional=self.min_notional,
            precision=self.amount_precision,
            step_source="instrument_execution_contract",
            min_qty_source="instrument_execution_contract",
            max_qty_source="instrument_execution_contract",
            precision_source="instrument_execution_contract",
        )


@dataclass(frozen=True)
class VenueExecutionProfile:
    """Venue capabilities and rules, independent of provider data adapters."""

    schema_version: str
    profile_id: str
    version: str
    venue_id: str
    supported_order_types: tuple[str, ...]
    supported_time_in_force: tuple[str, ...]
    post_only_supported: bool
    post_only_behavior: str
    liquidity_role_by_order_type: tuple[tuple[str, str], ...]
    price_increment_policy: str
    quantity_increment_policy: str
    max_market_order_notional: float | None
    market_price_collar_bps: float | None
    book_data_capability: str
    lifecycle_event_mapping: tuple[tuple[str, str], ...]
    external_order_submission_enabled: bool
    source: str
    profile_hash: str

    def __post_init__(self) -> None:
        if self.schema_version != VENUE_EXECUTION_PROFILE_SCHEMA_VERSION:
            raise ValueError(f"unsupported venue execution profile schema: {self.schema_version}")
        _required_text(self.profile_id, field="venue_execution_profile.profile_id")
        _required_text(self.version, field="venue_execution_profile.version")
        _required_text(self.venue_id, field="venue_execution_profile.venue_id")
        _required_text(self.source, field="venue_execution_profile.source")
        if not self.supported_order_types or set(self.supported_order_types) - _ORDER_TYPES:
            raise ValueError("venue execution profile has unsupported order types")
        if not self.supported_time_in_force or set(self.supported_time_in_force) - _TIME_IN_FORCE:
            raise ValueError("venue execution profile has unsupported time-in-force values")
        if not isinstance(self.post_only_supported, bool):
            raise ValueError("venue execution profile post_only_supported must be boolean")
        if self.post_only_behavior not in _POST_ONLY_BEHAVIORS:
            raise ValueError("venue execution profile has unsupported post_only_behavior")
        roles = dict(self.liquidity_role_by_order_type)
        if set(roles) != set(self.supported_order_types):
            raise ValueError("venue profile liquidity classification must cover every supported order type")
        if set(roles.values()) - {"maker", "taker"}:
            raise ValueError("venue profile liquidity roles must be maker or taker")
        if self.price_increment_policy not in _INCREMENT_POLICIES:
            raise ValueError("venue profile has unsupported price_increment_policy")
        if self.quantity_increment_policy not in _INCREMENT_POLICIES:
            raise ValueError("venue profile has unsupported quantity_increment_policy")
        _positive_number(self.max_market_order_notional, field="max_market_order_notional", optional=True)
        collar_bps = _finite_number(
            self.market_price_collar_bps,
            field="market_price_collar_bps",
            optional=True,
        )
        if collar_bps is not None and collar_bps < 0:
            raise ValueError("market_price_collar_bps must be non-negative")
        if self.book_data_capability not in _BOOK_CAPABILITIES:
            raise ValueError("venue profile book_data_capability must be bars, l1, l2, or l3")
        if self.external_order_submission_enabled is not False:
            raise ValueError("Phase 2A venue profiles cannot enable external order submission")
        expected = _stable_hash(self._material())
        if self.profile_hash and self.profile_hash != expected:
            raise ValueError("venue_execution_profile_hash_mismatch")
        object.__setattr__(self, "profile_hash", expected)

    def _material(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("profile_hash", None)
        payload["liquidity_role_by_order_type"] = dict(self.liquidity_role_by_order_type)
        payload["lifecycle_event_mapping"] = dict(self.lifecycle_event_mapping)
        return payload

    def to_dict(self) -> dict[str, Any]:
        payload = self._material()
        payload["profile_hash"] = self.profile_hash
        return payload

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "VenueExecutionProfile":
        if not isinstance(raw, Mapping) or not raw:
            raise ValueError("venue execution profile must be a non-empty object")
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            profile_id=str(raw.get("profile_id") or "").strip(),
            version=str(raw.get("version") or "").strip(),
            venue_id=str(raw.get("venue_id") or "").strip(),
            supported_order_types=_text_tuple(
                raw.get("supported_order_types"), field="supported_order_types", allowed=_ORDER_TYPES
            ),
            supported_time_in_force=_text_tuple(
                raw.get("supported_time_in_force"), field="supported_time_in_force", allowed=_TIME_IN_FORCE
            ),
            post_only_supported=raw.get("post_only_supported"),
            post_only_behavior=str(raw.get("post_only_behavior") or "").strip(),
            liquidity_role_by_order_type=_pair_tuple(
                raw.get("liquidity_role_by_order_type"), field="liquidity_role_by_order_type"
            ),
            price_increment_policy=str(raw.get("price_increment_policy") or "").strip(),
            quantity_increment_policy=str(raw.get("quantity_increment_policy") or "").strip(),
            max_market_order_notional=_finite_number(
                raw.get("max_market_order_notional"), field="max_market_order_notional", optional=True
            ),
            market_price_collar_bps=_finite_number(
                raw.get("market_price_collar_bps"), field="market_price_collar_bps", optional=True
            ),
            book_data_capability=str(raw.get("book_data_capability") or "").strip().lower(),
            lifecycle_event_mapping=_pair_tuple(
                raw.get("lifecycle_event_mapping") or {}, field="lifecycle_event_mapping"
            ),
            external_order_submission_enabled=raw.get("external_order_submission_enabled"),
            source=str(raw.get("source") or "").strip(),
            profile_hash=str(raw.get("profile_hash") or "").strip(),
        )

    def liquidity_role(self, order_type: str) -> str:
        normalized = str(order_type or "").strip().lower()
        role = dict(self.liquidity_role_by_order_type).get(normalized)
        if not role:
            raise ValueError(f"unsupported order type for venue profile: {normalized}")
        return role


@dataclass(frozen=True)
class ExecutionModelArtifact:
    """Versioned fill-model evidence independently pinned from venue rules."""

    schema_version: str
    artifact_id: str
    version: str
    assumption_manifest_hash: str
    input_capability: str
    execution_quality_ceiling: str
    supports_partial_fills: bool
    supports_resting_orders: bool
    supports_latency: bool
    calibration_artifact_hash: str | None
    source: str
    artifact_hash: str

    def __post_init__(self) -> None:
        if self.schema_version != EXECUTION_MODEL_ARTIFACT_SCHEMA_VERSION:
            raise ValueError(f"unsupported execution model artifact schema: {self.schema_version}")
        _required_text(self.artifact_id, field="execution_model_artifact.artifact_id")
        _required_text(self.version, field="execution_model_artifact.version")
        _required_text(self.assumption_manifest_hash, field="execution_model_artifact.assumption_manifest_hash")
        _required_text(self.source, field="execution_model_artifact.source")
        if self.execution_quality_ceiling not in {"X0", "X1", "X2", "X3", "X4"}:
            raise ValueError("execution model quality ceiling must be X0 through X4")
        if self.input_capability not in _BOOK_CAPABILITIES:
            raise ValueError("execution model input_capability must be bars, l1, l2, or l3")
        for name in ("supports_partial_fills", "supports_resting_orders", "supports_latency"):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"execution_model_artifact.{name} must be boolean")
        expected = _stable_hash(self._material())
        if self.artifact_hash and self.artifact_hash != expected:
            raise ValueError("execution_model_artifact_hash_mismatch")
        object.__setattr__(self, "artifact_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "artifact_hash"}

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionModelArtifact":
        if not isinstance(raw, Mapping) or not raw:
            raise ValueError("execution model artifact must be a non-empty object")
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            artifact_id=str(raw.get("artifact_id") or "").strip(),
            version=str(raw.get("version") or "").strip(),
            assumption_manifest_hash=str(raw.get("assumption_manifest_hash") or "").strip(),
            input_capability=str(raw.get("input_capability") or "").strip().lower(),
            execution_quality_ceiling=str(raw.get("execution_quality_ceiling") or "").strip().upper(),
            supports_partial_fills=raw.get("supports_partial_fills"),
            supports_resting_orders=raw.get("supports_resting_orders"),
            supports_latency=raw.get("supports_latency"),
            calibration_artifact_hash=str(raw.get("calibration_artifact_hash") or "").strip() or None,
            source=str(raw.get("source") or "").strip(),
            artifact_hash=str(raw.get("artifact_hash") or "").strip(),
        )


@dataclass(frozen=True)
class OrderConformance:
    accepted: bool
    reason: str | None
    liquidity_role: str | None
    normalized_qty: float | None
    metadata: Mapping[str, Any]
    normalized_price: float | None = None


@dataclass(frozen=True)
class ResolvedExecutionContext:
    """Complete immutable execution contract for one runtime series."""

    schema_version: str
    instrument: InstrumentExecutionContract
    venue: VenueExecutionProfile
    fee_schedule: FeeSchedule
    model: ExecutionModelArtifact
    source: str
    context_hash: str

    def __post_init__(self) -> None:
        if self.schema_version != RESOLVED_EXECUTION_CONTEXT_SCHEMA_VERSION:
            raise ValueError(f"unsupported resolved execution context schema: {self.schema_version}")
        if self.fee_schedule.schema_version != FEE_SCHEDULE_SCHEMA_VERSION:
            raise ValueError("resolved execution context has unsupported fee schedule")
        if self.fee_schedule.venue_profile_id != self.venue.profile_id:
            raise ValueError("fee_schedule_venue_profile_mismatch")
        if self.fee_schedule.fee_currency.casefold() != self.instrument.quote_currency.casefold():
            raise ValueError("phase_2a_non_quote_fee_currency_unsupported")
        if self.fee_schedule.calculation_basis != "quote_notional":
            raise ValueError("phase_2a_fee_calculation_basis_unsupported")
        if self.fee_schedule.maker_rate < 0 or self.fee_schedule.taker_rate < 0:
            raise ValueError("phase_2a_fee_rebate_unsupported")
        capability_rank = {"bars": 0, "l1": 1, "l2": 2, "l3": 3}
        if capability_rank[self.model.input_capability] > capability_rank[self.venue.book_data_capability]:
            raise ValueError("execution_model_input_exceeds_venue_book_capability")
        if self.model.execution_quality_ceiling == "X3" and self.model.input_capability not in {"l1", "l2", "l3"}:
            raise ValueError("x3_execution_model_requires_spread_capability")
        if self.model.execution_quality_ceiling == "X4":
            if self.model.input_capability not in {"l2", "l3"}:
                raise ValueError("x4_execution_model_requires_l2_or_l3")
            if not self.model.supports_partial_fills:
                raise ValueError("x4_execution_model_requires_partial_fills")
        _required_text(self.source, field="resolved_execution_context.source")
        if not str(self.fee_schedule.version or "").strip():
            raise ValueError("resolved execution context requires a versioned fee schedule")
        expected = _stable_hash(self._material())
        if self.context_hash and self.context_hash != expected:
            raise ValueError("resolved_execution_context_hash_mismatch")
        object.__setattr__(self, "context_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "instrument_execution_contract_hash": self.instrument.contract_hash,
            "venue_execution_profile_hash": self.venue.profile_hash,
            "fee_schedule_hash": self.fee_schedule.schedule_hash,
            "execution_model_artifact_hash": self.model.artifact_hash,
            "source": self.source,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **self._material(),
            "instrument": self.instrument.to_dict(),
            "venue": self.venue.to_dict(),
            "fee_schedule": self.fee_schedule.to_dict(),
            "model": self.model.to_dict(),
            "context_hash": self.context_hash,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ResolvedExecutionContext":
        if not isinstance(raw, Mapping) or not raw:
            raise ValueError("resolved execution context must be a non-empty object")
        context = cls(
            schema_version=str(raw.get("schema_version") or ""),
            instrument=InstrumentExecutionContract.from_dict(raw.get("instrument") or {}),
            venue=VenueExecutionProfile.from_dict(raw.get("venue") or {}),
            fee_schedule=FeeSchedule.from_dict(raw.get("fee_schedule") or {}),
            model=ExecutionModelArtifact.from_dict(raw.get("model") or {}),
            source=str(raw.get("source") or "").strip(),
            context_hash=str(raw.get("context_hash") or "").strip(),
        )
        expected_refs = {
            "instrument_execution_contract_hash": context.instrument.contract_hash,
            "venue_execution_profile_hash": context.venue.profile_hash,
            "fee_schedule_hash": context.fee_schedule.schedule_hash,
            "execution_model_artifact_hash": context.model.artifact_hash,
        }
        for field, expected in expected_refs.items():
            if str(raw.get(field) or "") != expected:
                raise ValueError(f"resolved_execution_context_{field}_mismatch")
        return context

    def evidence_metadata(self) -> dict[str, Any]:
        return {
            "resolved_execution_context_hash": self.context_hash,
            "instrument_execution_contract_hash": self.instrument.contract_hash,
            "venue_execution_profile_hash": self.venue.profile_hash,
            "venue_execution_profile_id": self.venue.profile_id,
            "venue_execution_profile_version": self.venue.version,
            "fee_schedule_hash": self.fee_schedule.schedule_hash,
            "fee_schedule_id": self.fee_schedule.schedule_id,
            "fee_schedule_version": self.fee_schedule.version,
            "fee_currency": self.fee_schedule.fee_currency,
            "fee_rounding_mode": self.fee_schedule.rounding_mode,
            "fee_precision": self.fee_schedule.precision,
            "fee_tier": self.fee_schedule.tier,
            "execution_model_artifact_hash": self.model.artifact_hash,
            "execution_model_artifact_id": self.model.artifact_id,
            "book_data_capability": self.venue.book_data_capability,
        }

    def validate_policy_capabilities(
        self,
        *,
        required_order_types: Iterable[Any],
        required_time_in_force: Iterable[Any],
        post_only_order_types: Iterable[Any] = (),
    ) -> dict[str, Any]:
        """Fail closed when a compiled strategy policy exceeds venue capabilities."""

        order_types = tuple(
            sorted({_required_text(item, field="required_order_types").lower() for item in required_order_types})
        )
        time_in_force = tuple(
            sorted(
                {
                    _required_text(item, field="required_time_in_force").lower()
                    for item in required_time_in_force
                }
            )
        )
        post_only_types = tuple(
            sorted(
                {
                    _required_text(item, field="post_only_order_types").lower()
                    for item in post_only_order_types
                }
            )
        )
        if not order_types:
            raise ValueError("required_order_types must not be empty")
        if not time_in_force:
            raise ValueError("required_time_in_force must not be empty")

        unsupported_orders = sorted(set(order_types) - set(self.venue.supported_order_types))
        if unsupported_orders:
            raise ValueError(
                "venue_profile_unsupported_order_types "
                f"profile_id={self.venue.profile_id} missing={','.join(unsupported_orders)}"
            )
        unsupported_tif = sorted(set(time_in_force) - set(self.venue.supported_time_in_force))
        if unsupported_tif:
            raise ValueError(
                "venue_profile_unsupported_time_in_force "
                f"profile_id={self.venue.profile_id} missing={','.join(unsupported_tif)}"
            )
        invalid_post_only = sorted(set(post_only_types) - set(order_types))
        if invalid_post_only:
            raise ValueError(
                "post_only_order_types_not_required "
                f"order_types={','.join(invalid_post_only)}"
            )
        if post_only_types and not self.venue.post_only_supported:
            raise ValueError(
                "venue_profile_post_only_unsupported "
                f"profile_id={self.venue.profile_id} order_types={','.join(post_only_types)}"
            )
        for order_type in order_types:
            self.venue.liquidity_role(order_type)

        return {
            "status": "passed",
            "required_order_types": list(order_types),
            "required_time_in_force": list(time_in_force),
            "post_only_order_types": list(post_only_types),
            "venue_execution_profile_id": self.venue.profile_id,
            "venue_execution_profile_version": self.venue.version,
            "venue_execution_profile_hash": self.venue.profile_hash,
        }

    def validate_order(
        self,
        *,
        order_type: Any,
        time_in_force: Any,
        post_only: Any,
        side: Any,
        quantity: Any,
        price: Any,
        liquidity_role: Any = None,
    ) -> OrderConformance:
        normalized_type = str(order_type or "").strip().lower()
        normalized_tif = str(time_in_force or "gtc").strip().lower()
        metadata = {
            **self.evidence_metadata(),
            "order_type": normalized_type,
            "time_in_force": normalized_tif,
            "post_only": post_only,
            "side": str(side or "").strip().lower(),
            "requested_qty": quantity,
            "requested_price": price,
        }
        if normalized_type not in self.venue.supported_order_types:
            return OrderConformance(False, "UNSUPPORTED_ORDER_TYPE", None, None, metadata)
        if normalized_tif not in self.venue.supported_time_in_force:
            return OrderConformance(False, "UNSUPPORTED_TIME_IN_FORCE", None, None, metadata)
        if not isinstance(post_only, bool):
            return OrderConformance(False, "POST_ONLY_FLAG_INVALID", None, None, metadata)
        if post_only and not self.venue.post_only_supported:
            return OrderConformance(False, "POST_ONLY_UNSUPPORTED", None, None, metadata)
        if normalized_type == "limit_maker" and not post_only:
            return OrderConformance(False, "POST_ONLY_REQUIRED", None, None, metadata)
        try:
            expected_role = self.venue.liquidity_role(normalized_type)
        except ValueError:
            return OrderConformance(False, "LIQUIDITY_CLASSIFICATION_UNRESOLVED", None, None, metadata)
        supplied_role = str(liquidity_role or "").strip().lower()
        if supplied_role and supplied_role != expected_role:
            return OrderConformance(False, "LIQUIDITY_ROLE_MISMATCH", expected_role, None, metadata)
        try:
            qty = float(quantity)
            order_price = float(price)
        except (TypeError, ValueError):
            return OrderConformance(False, "ORDER_NUMERIC_VALUE_INVALID", expected_role, None, metadata)
        if not math.isfinite(qty) or not math.isfinite(order_price) or qty <= 0 or order_price <= 0:
            return OrderConformance(False, "ORDER_NUMERIC_VALUE_INVALID", expected_role, None, metadata)
        normalization = normalize_qty_with_constraints(self.instrument.amount_constraints, qty)
        if not normalization.ok:
            return OrderConformance(
                False,
                normalization.rejected_reason or "QTY_CONSTRAINT_FAILED",
                expected_role,
                None,
                {**metadata, **normalization.to_log_dict()},
            )
        normalized_qty = float(normalization.qty_final)
        if self.venue.quantity_increment_policy == "reject" and not math.isclose(
            normalized_qty, qty, rel_tol=0.0, abs_tol=1e-12
        ):
            return OrderConformance(
                False,
                "QTY_INCREMENT_MISMATCH",
                expected_role,
                normalized_qty,
                {**metadata, **normalization.to_log_dict()},
            )
        normalized_price = order_price
        if normalized_type in {"limit_aggressive", "limit_maker", "limit_resting", "stop_market"}:
            ticks = order_price / self.instrument.tick_size
            if self.venue.price_increment_policy == "reject" and not math.isclose(
                ticks, round(ticks), rel_tol=0.0, abs_tol=1e-9
            ):
                return OrderConformance(False, "PRICE_INCREMENT_MISMATCH", expected_role, normalized_qty, metadata)
            if self.venue.price_increment_policy == "round_down":
                normalized_price = (
                    math.floor(ticks + 1e-12) * self.instrument.tick_size
                )
                if normalized_price <= 0:
                    return OrderConformance(
                        False,
                        "PRICE_ROUNDS_TO_ZERO",
                        expected_role,
                        normalized_qty,
                        metadata,
                    )
        metadata = {
            **metadata,
            "normalized_qty": normalized_qty,
            "normalized_price": normalized_price,
        }
        notional = abs(normalized_price * normalized_qty * self.instrument.contract_size)
        if self.instrument.min_notional is not None and notional < self.instrument.min_notional:
            return OrderConformance(False, "MIN_NOTIONAL_NOT_MET", expected_role, normalized_qty, metadata)
        if (
            normalized_type == "market"
            and self.venue.max_market_order_notional is not None
            and notional > self.venue.max_market_order_notional
        ):
            return OrderConformance(False, "MARKET_PROTECTION_NOTIONAL_EXCEEDED", expected_role, normalized_qty, metadata)
        return OrderConformance(
            True,
            None,
            expected_role,
            normalized_qty,
            metadata,
            normalized_price=normalized_price,
        )

    def validate_fill_protections(
        self,
        *,
        order_type: Any,
        side: Any,
        requested_price: Any,
        fill_price: Any,
        filled_qty: Any,
    ) -> OrderConformance:
        """Validate venue market protections against the simulated fill."""

        normalized_type = str(order_type or "").strip().lower()
        normalized_side = str(side or "").strip().lower()
        metadata = {
            **self.evidence_metadata(),
            "order_type": normalized_type,
            "side": normalized_side,
            "requested_price": requested_price,
            "fill_price": fill_price,
            "filled_qty": filled_qty,
            "market_price_collar_bps": self.venue.market_price_collar_bps,
            "max_market_order_notional": self.venue.max_market_order_notional,
        }
        try:
            requested = float(requested_price)
            filled = float(fill_price)
            quantity = float(filled_qty)
        except (TypeError, ValueError):
            return OrderConformance(False, "FILL_NUMERIC_VALUE_INVALID", None, None, metadata)
        if (
            not math.isfinite(requested)
            or not math.isfinite(filled)
            or not math.isfinite(quantity)
            or requested <= 0
            or filled <= 0
            or quantity <= 0
        ):
            return OrderConformance(False, "FILL_NUMERIC_VALUE_INVALID", None, None, metadata)
        if normalized_type not in {"market", "stop_market"}:
            return OrderConformance(True, None, None, quantity, metadata, normalized_price=filled)

        adverse_move = (
            max(0.0, filled - requested)
            if normalized_side in {"buy", "long"}
            else max(0.0, requested - filled)
        )
        adverse_bps = adverse_move / requested * 10_000.0
        metadata = {**metadata, "adverse_fill_bps": adverse_bps}
        collar = self.venue.market_price_collar_bps
        if collar is not None and adverse_bps > float(collar) + 1e-12:
            return OrderConformance(
                False,
                "MARKET_PROTECTION_PRICE_COLLAR_EXCEEDED",
                None,
                quantity,
                metadata,
                normalized_price=filled,
            )
        actual_notional = abs(filled * quantity * self.instrument.contract_size)
        metadata = {**metadata, "actual_fill_notional": actual_notional}
        maximum = self.venue.max_market_order_notional
        if maximum is not None and actual_notional > float(maximum) + 1e-12:
            return OrderConformance(
                False,
                "MARKET_PROTECTION_NOTIONAL_EXCEEDED",
                None,
                quantity,
                metadata,
                normalized_price=filled,
            )
        return OrderConformance(
            True,
            None,
            None,
            quantity,
            metadata,
            normalized_price=filled,
        )


@dataclass(frozen=True)
class ResolvedExecutionContextBundle:
    schema_version: str
    contexts: tuple[ResolvedExecutionContext, ...]
    bundle_hash: str

    def __post_init__(self) -> None:
        if self.schema_version != RESOLVED_EXECUTION_CONTEXT_BUNDLE_SCHEMA_VERSION:
            raise ValueError(f"unsupported resolved execution context bundle schema: {self.schema_version}")
        if not self.contexts:
            raise ValueError("resolved execution context bundle must not be empty")
        identities: set[tuple[str | None, str, str]] = set()
        for context in self.contexts:
            identity = (
                context.instrument.instrument_id,
                context.instrument.symbol.upper(),
                context.instrument.execution_semantics,
            )
            if identity in identities:
                raise ValueError("duplicate resolved execution context identity")
            identities.add(identity)
        expected = _stable_hash(self._material())
        if self.bundle_hash and self.bundle_hash != expected:
            raise ValueError("resolved_execution_context_bundle_hash_mismatch")
        object.__setattr__(self, "bundle_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "context_hashes": sorted(context.context_hash for context in self.contexts),
        }

    def to_dict(self) -> dict[str, Any]:
        ordered = sorted(
            self.contexts,
            key=lambda item: (
                item.instrument.instrument_id or "",
                item.instrument.symbol,
                item.instrument.execution_semantics,
            ),
        )
        return {
            "schema_version": self.schema_version,
            "contexts": [context.to_dict() for context in ordered],
            "bundle_hash": self.bundle_hash,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ResolvedExecutionContextBundle":
        if not isinstance(raw, Mapping) or not raw:
            raise ValueError("resolved execution context bundle must be a non-empty object")
        rows = raw.get("contexts")
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise ValueError("resolved execution context bundle contexts must be a list")
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            contexts=tuple(ResolvedExecutionContext.from_dict(row) for row in rows),
            bundle_hash=str(raw.get("bundle_hash") or "").strip(),
        )

    def context_for(
        self,
        *,
        instrument_id: Any,
        symbol: Any,
        execution_semantics: Any,
    ) -> ResolvedExecutionContext:
        normalized_id = str(instrument_id or "").strip()
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_semantics = str(execution_semantics or "").strip().lower()
        candidates = [
            context
            for context in self.contexts
            if context.instrument.execution_semantics == normalized_semantics
            and (
                (normalized_id and context.instrument.instrument_id == normalized_id)
                or (not normalized_id and context.instrument.symbol.upper() == normalized_symbol)
            )
        ]
        if len(candidates) != 1:
            raise ValueError(
                "resolved_execution_context_not_unique "
                f"instrument_id={normalized_id or None} symbol={normalized_symbol or None} "
                f"execution_semantics={normalized_semantics or None} matches={len(candidates)}"
            )
        return candidates[0]


def instrument_execution_contract_from_profile(profile: Any) -> InstrumentExecutionContract:
    """Project the existing runtime profile into the immutable instrument slice."""

    margin_rates = getattr(profile, "margin_rates", None)
    return InstrumentExecutionContract(
        schema_version=INSTRUMENT_EXECUTION_CONTRACT_SCHEMA_VERSION,
        instrument_id=profile.instrument.instrument_id,
        symbol=profile.instrument.symbol,
        instrument_type=profile.instrument.instrument_type,
        source_instrument_type=profile.instrument.source_instrument_type,
        execution_semantics=profile.instrument.execution_semantics,
        research_market_role=profile.instrument.research_market_role,
        datasource=profile.instrument.datasource,
        exchange=profile.instrument.exchange,
        base_currency=profile.instrument.base_currency,
        quote_currency=profile.instrument.quote_currency,
        tick_size=float(profile.constraints.tick_size),
        contract_size=float(profile.constraints.contract_size),
        tick_value=float(profile.constraints.tick_value),
        min_order_size=profile.constraints.min_order_size,
        qty_step=profile.constraints.qty_step,
        max_qty=profile.constraints.max_qty,
        min_notional=profile.constraints.min_notional,
        amount_precision=profile.constraints.amount_precision,
        supports_margin=profile.capabilities.supports_margin,
        supports_short=profile.capabilities.supports_short,
        short_requires_borrow=profile.capabilities.short_requires_borrow,
        has_funding=profile.capabilities.has_funding,
        has_expiry=profile.capabilities.has_expiry,
        accounting_mode=profile.accounting_mode,
        margin_calc_type=profile.margin_calc_type,
        margin_intraday_long=getattr(margin_rates, "intraday_long", None),
        margin_intraday_short=getattr(margin_rates, "intraday_short", None),
        margin_overnight_long=getattr(margin_rates, "overnight_long", None),
        margin_overnight_short=getattr(margin_rates, "overnight_short", None),
        contract_hash="",
    )


def _raw_contract(instrument: Mapping[str, Any], key: str) -> Mapping[str, Any] | None:
    value = instrument.get(key)
    if isinstance(value, Mapping) and value:
        return value
    metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
    fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
    value = fields.get(key)
    return value if isinstance(value, Mapping) and value else None


def resolve_venue_execution_profile(
    instrument: InstrumentExecutionContract,
    raw: Mapping[str, Any] | None = None,
) -> VenueExecutionProfile:
    """Resolve explicit venue rules or the pinned bar-simulation compatibility profile."""

    if raw:
        payload = dict(raw)
        payload.setdefault("schema_version", VENUE_EXECUTION_PROFILE_SCHEMA_VERSION)
        payload.setdefault("venue_id", instrument.exchange or "unscoped")
        payload.setdefault("external_order_submission_enabled", False)
        payload.setdefault("source", "instrument_venue_execution_profile")
        payload.setdefault("profile_hash", "")
        return VenueExecutionProfile.from_dict(payload)
    return VenueExecutionProfile(
        schema_version=VENUE_EXECUTION_PROFILE_SCHEMA_VERSION,
        profile_id="canonical_bar_simulation",
        version="canonical_bar_simulation.v1",
        venue_id=instrument.exchange or "unscoped",
        supported_order_types=tuple(sorted(_ORDER_TYPES)),
        supported_time_in_force=("gtc",),
        post_only_supported=True,
        post_only_behavior="reject_would_cross",
        liquidity_role_by_order_type=tuple(
            sorted(
                {
                    "market": "taker",
                    "limit_aggressive": "taker",
                    "limit_maker": "maker",
                    "limit_resting": "maker",
                    "stop_market": "taker",
                }.items()
            )
        ),
        price_increment_policy="reject",
        quantity_increment_policy="round_down",
        max_market_order_notional=None,
        market_price_collar_bps=None,
        book_data_capability="bars",
        lifecycle_event_mapping=tuple(
            sorted(
                {
                    "submitted": "submitted",
                    "open": "open",
                    "filled": "filled",
                    "rejected": "rejected",
                }.items()
            )
        ),
        external_order_submission_enabled=False,
        source="canonical_bar_compatibility_profile",
        profile_hash="",
    )


def resolve_fee_schedule(
    *,
    instrument: InstrumentExecutionContract,
    venue: VenueExecutionProfile,
    execution_fee_contract: Any,
    raw: Mapping[str, Any] | None = None,
) -> FeeSchedule:
    """Resolve a schedule separately from venue capability and instrument facts."""

    if raw:
        payload = dict(raw)
        payload.setdefault("schema_version", FEE_SCHEDULE_SCHEMA_VERSION)
        payload.setdefault("venue_profile_id", venue.profile_id)
        payload.setdefault("fee_currency", instrument.quote_currency)
        payload.setdefault("calculation_basis", "quote_notional")
        payload.setdefault("rounding_mode", "unrounded")
        payload.setdefault("precision", None)
        payload.setdefault("tier", "default")
        payload.setdefault("configured", bool(execution_fee_contract.configured))
        payload.setdefault("verified_zero", bool(execution_fee_contract.verified_zero))
        payload.setdefault("schedule_hash", "")
        return FeeSchedule.from_dict(payload)
    return FeeSchedule(
        maker_rate=float(execution_fee_contract.maker_fee_rate),
        taker_rate=float(execution_fee_contract.taker_fee_rate),
        source=str(execution_fee_contract.source),
        version=str(execution_fee_contract.version or "") or None,
        schedule_id=f"{venue.profile_id}:instrument_resolved",
        venue_profile_id=venue.profile_id,
        fee_currency=instrument.quote_currency,
        calculation_basis="quote_notional",
        rounding_mode="unrounded",
        precision=None,
        tier="instrument_default",
        configured=bool(execution_fee_contract.configured),
        verified_zero=bool(execution_fee_contract.verified_zero),
    )


def execution_model_artifact_from_assumptions(
    assumptions: ResolvedExecutionAssumptions,
) -> ExecutionModelArtifact:
    return ExecutionModelArtifact(
        schema_version=EXECUTION_MODEL_ARTIFACT_SCHEMA_VERSION,
        artifact_id="deterministic_bar_execution",
        version=assumptions.model_version,
        assumption_manifest_hash=assumptions.manifest_hash,
        input_capability="bars",
        execution_quality_ceiling=assumptions.execution_quality_ceiling,
        supports_partial_fills=False,
        supports_resting_orders=False,
        supports_latency=False,
        calibration_artifact_hash=None,
        source="execution_assumptions_manifest",
        artifact_hash="",
    )


def execution_model_artifact_from_book_tape(
    assumptions: ResolvedExecutionAssumptions,
    *,
    source_capability: str,
) -> ExecutionModelArtifact:
    """Build the pinned deterministic X3/X4 model contract for a book tape."""

    capability = str(source_capability or "").strip().lower()
    if capability not in {"l1", "l2", "l3"}:
        raise ValueError("book execution model requires l1, l2, or l3 capability")
    from .book_execution import AGGREGATED_L2_MODEL_VERSION, SPREAD_AWARE_MODEL_VERSION

    l2_capable = capability in {"l2", "l3"}
    return ExecutionModelArtifact(
        schema_version=EXECUTION_MODEL_ARTIFACT_SCHEMA_VERSION,
        artifact_id=("deterministic_aggregated_l2_execution" if l2_capable else "deterministic_spread_execution"),
        version=(AGGREGATED_L2_MODEL_VERSION if l2_capable else SPREAD_AWARE_MODEL_VERSION),
        assumption_manifest_hash=assumptions.manifest_hash,
        input_capability=("l2" if l2_capable else "l1"),
        execution_quality_ceiling=("X4" if l2_capable else "X3"),
        # L1 is still visibility-bounded: the top level may satisfy only part
        # of an order, so residual disposition remains explicit at X3 too.
        supports_partial_fills=True,
        supports_resting_orders=False,
        supports_latency=False,
        calibration_artifact_hash=None,
        source="replay_certified_execution_book_tape",
        artifact_hash="",
    )


def resolve_execution_context(
    profile: Any,
    assumptions: ResolvedExecutionAssumptions,
    *,
    instrument_payload: Mapping[str, Any] | None = None,
    execution_model_artifact: ExecutionModelArtifact | None = None,
    source: str = "runtime_profile_resolution",
) -> ResolvedExecutionContext:
    """Resolve the complete venue-neutral execution bundle for one series."""

    instrument = instrument_execution_contract_from_profile(profile)
    raw_instrument = dict(instrument_payload or getattr(profile.instrument, "raw", {}) or {})
    venue = resolve_venue_execution_profile(
        instrument,
        _raw_contract(raw_instrument, "venue_execution_profile"),
    )
    fee_schedule = resolve_fee_schedule(
        instrument=instrument,
        venue=venue,
        execution_fee_contract=profile.fees,
        raw=_raw_contract(raw_instrument, "fee_schedule"),
    )
    model = execution_model_artifact or execution_model_artifact_from_assumptions(assumptions)
    if model.assumption_manifest_hash != assumptions.manifest_hash:
        raise ValueError("execution_model_assumption_manifest_mismatch")
    return ResolvedExecutionContext(
        schema_version=RESOLVED_EXECUTION_CONTEXT_SCHEMA_VERSION,
        instrument=instrument,
        venue=venue,
        fee_schedule=fee_schedule,
        model=model,
        source=str(source),
        context_hash="",
    )


def build_execution_context_bundle(
    contexts: Iterable[ResolvedExecutionContext],
) -> ResolvedExecutionContextBundle:
    return ResolvedExecutionContextBundle(
        schema_version=RESOLVED_EXECUTION_CONTEXT_BUNDLE_SCHEMA_VERSION,
        contexts=tuple(contexts),
        bundle_hash="",
    )


def validate_context_against_runtime(
    context: ResolvedExecutionContext,
    *,
    profile: Any,
    assumptions: ResolvedExecutionAssumptions,
    instrument_payload: Mapping[str, Any] | None = None,
) -> None:
    """Prove runtime-resolved facts match the immutable startup context exactly."""

    recomputed = resolve_execution_context(
        profile,
        assumptions,
        instrument_payload=instrument_payload,
        execution_model_artifact=context.model,
        source=context.source,
    )
    if recomputed.context_hash != context.context_hash:
        raise ValueError(
            "resolved_execution_context_runtime_mismatch "
            f"expected={context.context_hash} actual={recomputed.context_hash} "
            f"symbol={context.instrument.symbol}"
        )


__all__ = [
    "EXECUTION_MODEL_ARTIFACT_SCHEMA_VERSION",
    "INSTRUMENT_EXECUTION_CONTRACT_SCHEMA_VERSION",
    "RESOLVED_EXECUTION_CONTEXT_BUNDLE_SCHEMA_VERSION",
    "RESOLVED_EXECUTION_CONTEXT_SCHEMA_VERSION",
    "VENUE_EXECUTION_PROFILE_SCHEMA_VERSION",
    "ExecutionModelArtifact",
    "InstrumentExecutionContract",
    "OrderConformance",
    "ResolvedExecutionContext",
    "ResolvedExecutionContextBundle",
    "VenueExecutionProfile",
    "build_execution_context_bundle",
    "execution_model_artifact_from_assumptions",
    "execution_model_artifact_from_book_tape",
    "instrument_execution_contract_from_profile",
    "resolve_execution_context",
    "resolve_fee_schedule",
    "resolve_venue_execution_profile",
    "validate_context_against_runtime",
]
