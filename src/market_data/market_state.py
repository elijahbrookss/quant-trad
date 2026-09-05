"""Typed causal market-state feature contracts and deterministic transforms."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Iterable, Mapping, Optional, Sequence

from .contracts import FundingRateRecord, OpenInterestRecord
from .order_book import BookSourcePosition, BookStateView
from .structure import (
    MarketSide,
    MarketTradeFact,
    ProviderSizeUnit,
    TradeFlowAggregateFact,
)


BBO_FACT_TYPE = "market.bbo"
BBO_FACT_VERSION = "market.bbo.v1"
DEPTH_FACT_TYPE = "market.depth_observation"
DEPTH_FACT_VERSION = "market.depth_band.v1"
TRADE_FLOW_FEATURE_FACT_TYPE = "market.trade_flow_feature"
TRADE_FLOW_FEATURE_FACT_VERSION = "market.trade_flow_feature.v1"
BASIS_FACT_TYPE = "market.futures_spot_relationship"
BASIS_FACT_VERSION = "market.futures_spot_basis.v1"
DERIVATIVE_STATE_FACT_TYPE = "market.derivative_state"
DERIVATIVE_STATE_FACT_VERSION = "market.derivative_state.v1"
RESPONSE_FACT_TYPE = "market.market_response"
RESPONSE_FACT_VERSION = "market.market_response.v1"
BOOK_FEATURE_INTERVAL_SECONDS = 1
APPROVED_DEPTH_BANDS_BPS = (5, 10, 25)
BASIS_MAX_STALENESS = timedelta(seconds=2)
DERIVATIVE_OI_INTERVAL_SECONDS = 60
RESPONSE_MAX_STALENESS = timedelta(seconds=2)
ONE_MILLION = Decimal("1000000")
TEN_THOUSAND = Decimal("10000")


def _utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"market_state_invalid: {field_name} must be datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(
    value: Any,
    *,
    field_name: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"market_state_invalid: {field_name} must be decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"market_state_invalid: {field_name} must be decimal"
        ) from exc
    if not result.is_finite():
        raise ValueError(f"market_state_invalid: {field_name} must be finite")
    if positive and result <= 0:
        raise ValueError(f"market_state_invalid: {field_name} must be positive")
    if nonnegative and result < 0:
        raise ValueError(f"market_state_invalid: {field_name} must be nonnegative")
    return Decimal(0) if result == 0 else result


def _optional_decimal(
    value: Any,
    *,
    field_name: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> Optional[Decimal]:
    if value is None:
        return None
    return _decimal(
        value,
        field_name=field_name,
        positive=positive,
        nonnegative=nonnegative,
    )


def _canonical_decimal(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _canonical_time(value: datetime) -> str:
    return _utc(value, field_name="time").isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _bucket_start(value: datetime, *, interval_seconds: int) -> datetime:
    timestamp = _utc(value, field_name="effective_at")
    seconds = int(interval_seconds)
    epoch = int(timestamp.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % seconds), tz=UTC)


def _position_material(position: BookSourcePosition) -> Mapping[str, Any]:
    return position.material()


def _position_key(position: BookSourcePosition) -> tuple[Any, ...]:
    return (
        position.connection_epoch,
        position.receive_ordinal,
        position.event_ordinal,
        position.provider_sequence_num
        if position.provider_sequence_num is not None
        else 2**63,
    )


def _validate_hash(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"market_state_invalid: {field_name} must be sha256")
    return normalized


@dataclass(frozen=True)
class MarketStateValuationContract:
    product_definition_version_id: str
    provider_size_unit: ProviderSizeUnit | str
    base_currency: str
    quote_currency: str
    contract_size: Optional[Decimal] = None
    linear_quote_notional: bool = True

    def __post_init__(self) -> None:
        definition = str(self.product_definition_version_id or "").strip()
        base = str(self.base_currency or "").strip().upper()
        quote = str(self.quote_currency or "").strip().upper()
        if not definition or not base or not quote:
            raise ValueError("market_state_contract_invalid: identity is required")
        try:
            unit = ProviderSizeUnit(
                str(getattr(self.provider_size_unit, "value", self.provider_size_unit))
            )
        except ValueError as exc:
            raise ValueError(
                "market_state_contract_invalid: provider size unit is unknown"
            ) from exc
        contract_size = _optional_decimal(
            self.contract_size,
            field_name="contract_size",
            positive=True,
        )
        if unit is ProviderSizeUnit.CONTRACTS and contract_size is None:
            raise ValueError(
                "market_state_contract_invalid: contract unit requires multiplier"
            )
        if unit is ProviderSizeUnit.BASE and contract_size is not None:
            raise ValueError(
                "market_state_contract_invalid: base unit forbids multiplier"
            )
        object.__setattr__(self, "product_definition_version_id", definition)
        object.__setattr__(self, "provider_size_unit", unit)
        object.__setattr__(self, "base_currency", base)
        object.__setattr__(self, "quote_currency", quote)
        object.__setattr__(self, "contract_size", contract_size)
        object.__setattr__(
            self, "linear_quote_notional", bool(self.linear_quote_notional)
        )

    @property
    def base_multiplier(self) -> Decimal:
        if self.provider_size_unit is ProviderSizeUnit.BASE:
            return Decimal(1)
        assert self.contract_size is not None
        return self.contract_size

    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": "market.state_valuation_contract.v1",
                "product_definition_version_id": self.product_definition_version_id,
                "provider_size_unit": self.provider_size_unit.value,
                "base_currency": self.base_currency,
                "quote_currency": self.quote_currency,
                "contract_size": _canonical_decimal(self.contract_size),
                "linear_quote_notional": self.linear_quote_notional,
            }
        )


@dataclass(frozen=True)
class BboFeatureFact:
    series_id: int
    source_l2_series_id: int
    bucket_start: datetime
    bucket_end: datetime
    source_effective_at: datetime
    known_at: datetime
    source_position: BookSourcePosition
    validity_interval_id: str
    product_definition_version_id: str
    provider_size_unit: ProviderSizeUnit
    source_state_hash: str
    bid_price: Decimal
    bid_quantity: Decimal
    bid_base_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    ask_base_quantity: Decimal
    mid_price: Decimal
    spread: Decimal
    spread_bps: Decimal
    input_fingerprint: str

    def __post_init__(self) -> None:
        if int(self.series_id) <= 0 or int(self.source_l2_series_id) <= 0:
            raise ValueError("market_bbo_invalid: series identities must be positive")
        start = _utc(self.bucket_start, field_name="bucket_start")
        end = _utc(self.bucket_end, field_name="bucket_end")
        source_time = _utc(self.source_effective_at, field_name="source_effective_at")
        known = _utc(self.known_at, field_name="known_at")
        if end != start + timedelta(seconds=BOOK_FEATURE_INTERVAL_SECONDS):
            raise ValueError("market_bbo_invalid: bucket must be one second")
        if not start <= source_time < end or known < end:
            raise ValueError("market_bbo_invalid: causal bucket timing is invalid")
        bid = _decimal(self.bid_price, field_name="bid_price", positive=True)
        ask = _decimal(self.ask_price, field_name="ask_price", positive=True)
        if bid >= ask:
            raise ValueError("market_bbo_invalid: book is crossed or locked")
        bid_quantity = _decimal(
            self.bid_quantity, field_name="bid_quantity", positive=True
        )
        ask_quantity = _decimal(
            self.ask_quantity, field_name="ask_quantity", positive=True
        )
        bid_base = _decimal(
            self.bid_base_quantity, field_name="bid_base_quantity", positive=True
        )
        ask_base = _decimal(
            self.ask_base_quantity, field_name="ask_base_quantity", positive=True
        )
        mid = _decimal(self.mid_price, field_name="mid_price", positive=True)
        spread = _decimal(self.spread, field_name="spread", positive=True)
        spread_bps = _decimal(
            self.spread_bps, field_name="spread_bps", positive=True
        )
        if mid != (bid + ask) / Decimal(2) or spread != ask - bid:
            raise ValueError("market_bbo_invalid: price formulas do not reconcile")
        if spread_bps != TEN_THOUSAND * spread / mid:
            raise ValueError("market_bbo_invalid: spread_bps does not reconcile")
        object.__setattr__(self, "series_id", int(self.series_id))
        try:
            size_unit = ProviderSizeUnit(
                str(getattr(self.provider_size_unit, "value", self.provider_size_unit))
            )
        except ValueError as exc:
            raise ValueError("market_bbo_invalid: provider size unit is unknown") from exc
        if not str(self.validity_interval_id or "").strip() or not str(
            self.product_definition_version_id or ""
        ).strip():
            raise ValueError(
                "market_bbo_invalid: validity and product definition are required"
            )
        object.__setattr__(self, "bucket_start", start)
        object.__setattr__(self, "bucket_end", end)
        object.__setattr__(self, "source_effective_at", source_time)
        object.__setattr__(self, "source_l2_series_id", int(self.source_l2_series_id))
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "bid_price", bid)
        object.__setattr__(self, "ask_price", ask)
        object.__setattr__(self, "bid_quantity", bid_quantity)
        object.__setattr__(self, "ask_quantity", ask_quantity)
        object.__setattr__(self, "bid_base_quantity", bid_base)
        object.__setattr__(self, "ask_base_quantity", ask_base)
        object.__setattr__(self, "mid_price", mid)
        object.__setattr__(self, "spread", spread)
        object.__setattr__(self, "spread_bps", spread_bps)
        object.__setattr__(
            self, "source_state_hash", _validate_hash(self.source_state_hash, field_name="state_hash")
        )
        object.__setattr__(self, "provider_size_unit", size_unit)
        object.__setattr__(
            self,
            "input_fingerprint",
            _validate_hash(self.input_fingerprint, field_name="input_fingerprint"),
        )

    @property
    def effective_at(self) -> datetime:
        return self.bucket_end

    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": BBO_FACT_VERSION,
                "series_id": self.series_id,
                "bucket_start": _canonical_time(self.bucket_start),
                "source_effective_at": _canonical_time(self.source_effective_at),
                "source_position": _position_material(self.source_position),
                "source_l2_series_id": self.source_l2_series_id,
                "validity_interval_id": self.validity_interval_id,
                "product_definition_version_id": self.product_definition_version_id,
                "provider_size_unit": self.provider_size_unit.value,
                "source_state_hash": self.source_state_hash,
                "bid_price": _canonical_decimal(self.bid_price),
                "bid_quantity": _canonical_decimal(self.bid_quantity),
                "bid_base_quantity": _canonical_decimal(self.bid_base_quantity),
                "ask_price": _canonical_decimal(self.ask_price),
                "ask_quantity": _canonical_decimal(self.ask_quantity),
                "ask_base_quantity": _canonical_decimal(self.ask_base_quantity),
                "mid_price": _canonical_decimal(self.mid_price),
                "spread": _canonical_decimal(self.spread),
                "spread_bps": _canonical_decimal(self.spread_bps),
                "input_fingerprint": self.input_fingerprint,
            }
        )


@dataclass(frozen=True)
class DepthFeatureFact:
    series_id: int
    source_l2_series_id: int
    bucket_start: datetime
    bucket_end: datetime
    source_effective_at: datetime
    known_at: datetime
    source_position: BookSourcePosition
    validity_interval_id: str
    source_state_hash: str
    bbo_input_fingerprint: str
    provider_size_unit: ProviderSizeUnit
    band_bps: int
    mid_price: Decimal
    bid_quantity: Decimal
    ask_quantity: Decimal
    bid_base_quantity: Decimal
    ask_base_quantity: Decimal
    bid_notional: Optional[Decimal]
    ask_notional: Optional[Decimal]
    imbalance: Optional[Decimal]
    input_fingerprint: str

    def __post_init__(self) -> None:
        if (
            int(self.series_id) <= 0
            or int(self.source_l2_series_id) <= 0
            or int(self.band_bps) not in APPROVED_DEPTH_BANDS_BPS
        ):
            raise ValueError("market_depth_invalid: series or band is unsupported")
        start = _utc(self.bucket_start, field_name="bucket_start")
        end = _utc(self.bucket_end, field_name="bucket_end")
        source_time = _utc(self.source_effective_at, field_name="source_effective_at")
        known = _utc(self.known_at, field_name="known_at")
        if end != start + timedelta(seconds=1) or not start <= source_time < end:
            raise ValueError("market_depth_invalid: bucket timing is invalid")
        if known < end:
            raise ValueError("market_depth_invalid: known_at precedes bucket end")
        for name in (
            "bid_quantity",
            "ask_quantity",
            "bid_base_quantity",
            "ask_base_quantity",
        ):
            object.__setattr__(
                self,
                name,
                _decimal(getattr(self, name), field_name=name, nonnegative=True),
            )
        for name in ("bid_notional", "ask_notional"):
            object.__setattr__(
                self,
                name,
                _optional_decimal(
                    getattr(self, name), field_name=name, nonnegative=True
                ),
            )
        imbalance = _optional_decimal(self.imbalance, field_name="imbalance")
        denominator = self.bid_base_quantity + self.ask_base_quantity
        if denominator == 0:
            if imbalance is not None:
                raise ValueError("market_depth_invalid: empty band must suppress imbalance")
        else:
            expected = (self.bid_base_quantity - self.ask_base_quantity) / denominator
            if imbalance != expected or expected < -1 or expected > 1:
                raise ValueError("market_depth_invalid: imbalance does not reconcile")
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "band_bps", int(self.band_bps))
        object.__setattr__(self, "source_l2_series_id", int(self.source_l2_series_id))
        object.__setattr__(self, "bucket_start", start)
        object.__setattr__(self, "bucket_end", end)
        object.__setattr__(self, "source_effective_at", source_time)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(
            self, "mid_price", _decimal(self.mid_price, field_name="mid_price", positive=True)
        )
        object.__setattr__(self, "imbalance", imbalance)
        try:
            size_unit = ProviderSizeUnit(
                str(getattr(self.provider_size_unit, "value", self.provider_size_unit))
            )
        except ValueError as exc:
            raise ValueError("market_depth_invalid: provider size unit is unknown") from exc
        object.__setattr__(self, "provider_size_unit", size_unit)
        if not str(self.validity_interval_id or "").strip():
            raise ValueError("market_depth_invalid: validity interval is required")
        for name in ("source_state_hash", "bbo_input_fingerprint", "input_fingerprint"):
            object.__setattr__(
                self, name, _validate_hash(getattr(self, name), field_name=name)
            )

    @property
    def effective_at(self) -> datetime:
        return self.bucket_end

    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": DEPTH_FACT_VERSION,
                "series_id": self.series_id,
                "bucket_start": _canonical_time(self.bucket_start),
                "source_l2_series_id": self.source_l2_series_id,
                "source_effective_at": _canonical_time(self.source_effective_at),
                "source_position": _position_material(self.source_position),
                "validity_interval_id": self.validity_interval_id,
                "source_state_hash": self.source_state_hash,
                "band_bps": self.band_bps,
                "mid_price": _canonical_decimal(self.mid_price),
                "provider_size_unit": self.provider_size_unit.value,
                "bid_quantity": _canonical_decimal(self.bid_quantity),
                "ask_quantity": _canonical_decimal(self.ask_quantity),
                "bid_base_quantity": _canonical_decimal(self.bid_base_quantity),
                "ask_base_quantity": _canonical_decimal(self.ask_base_quantity),
                "bid_notional": _canonical_decimal(self.bid_notional),
                "ask_notional": _canonical_decimal(self.ask_notional),
                "imbalance": _canonical_decimal(self.imbalance),
                "input_fingerprint": self.input_fingerprint,
            }
        )


@dataclass(frozen=True)
class TradeFlowFeatureFact:
    series_id: int
    source_trade_flow_series_id: int
    interval_seconds: int
    bucket_start: datetime
    bucket_end: datetime
    known_at: datetime
    aggregate_material_hash: str
    aggregate_input_fingerprint: str
    trade_count: int
    quote_notional: Decimal
    aggressor_buy_base_volume: Decimal
    aggressor_sell_base_volume: Decimal
    aggressor_buy_notional: Decimal
    aggressor_sell_notional: Decimal
    cvd_base: Decimal
    cvd_notional: Decimal
    cvd_volume_share: Optional[Decimal]
    input_fingerprint: str

    def __post_init__(self) -> None:
        interval = int(self.interval_seconds)
        if (
            interval not in {1, 60}
            or int(self.series_id) <= 0
            or int(self.source_trade_flow_series_id) <= 0
        ):
            raise ValueError("market_flow_feature_invalid: series or interval is unsupported")
        start = _utc(self.bucket_start, field_name="bucket_start")
        end = _utc(self.bucket_end, field_name="bucket_end")
        known = _utc(self.known_at, field_name="known_at")
        if end != start + timedelta(seconds=interval) or known < end:
            raise ValueError("market_flow_feature_invalid: causal timing is invalid")
        if int(self.trade_count) <= 0:
            raise ValueError("market_flow_feature_invalid: a populated complete bucket is required")
        for name in (
            "quote_notional",
            "aggressor_buy_base_volume",
            "aggressor_sell_base_volume",
            "aggressor_buy_notional",
            "aggressor_sell_notional",
        ):
            object.__setattr__(
                self,
                name,
                _decimal(getattr(self, name), field_name=name, nonnegative=True),
            )
        cvd_base = _decimal(self.cvd_base, field_name="cvd_base")
        cvd_notional = _decimal(self.cvd_notional, field_name="cvd_notional")
        if cvd_base != self.aggressor_buy_base_volume - self.aggressor_sell_base_volume:
            raise ValueError("market_flow_feature_invalid: base CVD does not reconcile")
        if cvd_notional != self.aggressor_buy_notional - self.aggressor_sell_notional:
            raise ValueError("market_flow_feature_invalid: notional CVD does not reconcile")
        denominator = self.aggressor_buy_base_volume + self.aggressor_sell_base_volume
        share = _optional_decimal(self.cvd_volume_share, field_name="cvd_volume_share")
        if denominator == 0:
            if share is not None:
                raise ValueError("market_flow_feature_invalid: zero volume suppresses share")
        elif share != cvd_base / denominator or share < -1 or share > 1:
            raise ValueError("market_flow_feature_invalid: CVD share does not reconcile")
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "interval_seconds", interval)
        object.__setattr__(
            self, "source_trade_flow_series_id", int(self.source_trade_flow_series_id)
        )
        object.__setattr__(self, "trade_count", int(self.trade_count))
        object.__setattr__(self, "bucket_start", start)
        object.__setattr__(self, "bucket_end", end)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "cvd_base", cvd_base)
        object.__setattr__(self, "cvd_notional", cvd_notional)
        object.__setattr__(self, "cvd_volume_share", share)
        for name in (
            "aggregate_material_hash",
            "aggregate_input_fingerprint",
            "input_fingerprint",
        ):
            object.__setattr__(
                self, name, _validate_hash(getattr(self, name), field_name=name)
            )

    @property
    def effective_at(self) -> datetime:
        return self.bucket_end

    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": TRADE_FLOW_FEATURE_FACT_VERSION,
                "series_id": self.series_id,
                "interval_seconds": self.interval_seconds,
                "source_trade_flow_series_id": self.source_trade_flow_series_id,
                "bucket_start": _canonical_time(self.bucket_start),
                "aggregate_material_hash": self.aggregate_material_hash,
                "trade_count": self.trade_count,
                "quote_notional": _canonical_decimal(self.quote_notional),
                "aggressor_buy_base_volume": _canonical_decimal(
                    self.aggressor_buy_base_volume
                ),
                "aggressor_sell_base_volume": _canonical_decimal(
                    self.aggressor_sell_base_volume
                ),
                "aggressor_buy_notional": _canonical_decimal(
                    self.aggressor_buy_notional
                ),
                "aggressor_sell_notional": _canonical_decimal(
                    self.aggressor_sell_notional
                ),
                "cvd_base": _canonical_decimal(self.cvd_base),
                "cvd_notional": _canonical_decimal(self.cvd_notional),
                "cvd_volume_share": _canonical_decimal(self.cvd_volume_share),
                "input_fingerprint": self.input_fingerprint,
            }
        )


@dataclass(frozen=True)
class BasisFeatureFact:
    mapping_id: str
    futures_series_id: int
    series_id: int
    spot_series_id: int
    effective_at: datetime
    known_at: datetime
    futures_bbo_material_hash: str
    spot_bbo_material_hash: str
    futures_mid: Decimal
    spot_mid: Decimal
    futures_staleness_seconds: Decimal
    spot_staleness_seconds: Decimal
    basis: Decimal
    basis_bps: Decimal
    input_fingerprint: str
    def __post_init__(self) -> None:
        if (
            int(self.series_id) <= 0
            or int(self.futures_series_id) <= 0
            or int(self.spot_series_id) <= 0
            or not str(self.mapping_id or "").strip()
        ):
            raise ValueError("market_basis_invalid: identities are required")
        effective = _utc(self.effective_at, field_name="effective_at")
        known = _utc(self.known_at, field_name="known_at")
        if known < effective:
            raise ValueError("market_basis_invalid: known_at precedes effective_at")
        futures_mid = _decimal(self.futures_mid, field_name="futures_mid", positive=True)
        spot_mid = _decimal(self.spot_mid, field_name="spot_mid", positive=True)
        basis = _decimal(self.basis, field_name="basis")
        basis_bps = _decimal(self.basis_bps, field_name="basis_bps")
        if basis != futures_mid - spot_mid or basis_bps != TEN_THOUSAND * basis / spot_mid:
            raise ValueError("market_basis_invalid: formulas do not reconcile")
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "futures_series_id", int(self.futures_series_id))
        object.__setattr__(self, "spot_series_id", int(self.spot_series_id))
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "futures_mid", futures_mid)
        object.__setattr__(self, "spot_mid", spot_mid)
        object.__setattr__(self, "basis", basis)
        object.__setattr__(self, "basis_bps", basis_bps)
        for name in ("futures_staleness_seconds", "spot_staleness_seconds"):
            object.__setattr__(
                self, name, _decimal(getattr(self, name), field_name=name, nonnegative=True)
            )
        for name in (
            "futures_bbo_material_hash",
            "spot_bbo_material_hash",
            "input_fingerprint",
        ):
            object.__setattr__(
                self, name, _validate_hash(getattr(self, name), field_name=name)
            )


    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": BASIS_FACT_VERSION,
                "mapping_id": self.mapping_id,
                "series_id": self.series_id,
                "futures_series_id": self.futures_series_id,
                "spot_series_id": self.spot_series_id,
                "effective_at": _canonical_time(self.effective_at),
                "futures_bbo_material_hash": self.futures_bbo_material_hash,
                "spot_bbo_material_hash": self.spot_bbo_material_hash,
                "futures_mid": _canonical_decimal(self.futures_mid),
                "spot_mid": _canonical_decimal(self.spot_mid),
                "futures_staleness_seconds": _canonical_decimal(
                    self.futures_staleness_seconds
                ),
                "spot_staleness_seconds": _canonical_decimal(
                    self.spot_staleness_seconds
                ),
                "basis": _canonical_decimal(self.basis),
                "basis_bps": _canonical_decimal(self.basis_bps),
                "input_fingerprint": self.input_fingerprint,
            }
        )


@dataclass(frozen=True)
class DerivativeStateFeatureFact:
    instrument_id: str
    effective_at: datetime
    series_id: int
    known_at: datetime
    oi_series_id: Optional[int]
    oi_sample_time: Optional[datetime]
    oi_market_commit_seq: Optional[int]
    oi_value: Optional[Decimal]
    oi_previous_value: Optional[Decimal]
    oi_log_change: Optional[Decimal]
    funding_series_id: Optional[int]
    funding_sample_time: Optional[datetime]
    funding_market_commit_seq: Optional[int]
    funding_rate: Optional[Decimal]
    funding_time: Optional[datetime]
    funding_interval_seconds: Optional[int]
    funding_semantics: Optional[str]
    input_fingerprint: str
    def __post_init__(self) -> None:
        if int(self.series_id) <= 0 or not str(self.instrument_id or "").strip():
            raise ValueError("market_derivative_state_invalid: identity is required")
        effective = _utc(self.effective_at, field_name="effective_at")
        known = _utc(self.known_at, field_name="known_at")
        if known < effective:
            raise ValueError(
                "market_derivative_state_invalid: known_at precedes effective_at"
            )
        oi_value = _optional_decimal(self.oi_value, field_name="oi_value", positive=True)
        previous = _optional_decimal(
            self.oi_previous_value, field_name="oi_previous_value", positive=True
        )
        log_change = _optional_decimal(self.oi_log_change, field_name="oi_log_change")
        funding_rate = _optional_decimal(self.funding_rate, field_name="funding_rate")
        if (oi_value is None) != (self.oi_series_id is None):
            raise ValueError(
                "market_derivative_state_invalid: OI identity/value mismatch"
            )
        if log_change is not None:
            if oi_value is None or previous is None:
                raise ValueError(
                    "market_derivative_state_invalid: OI change requires two values"
                )
            with localcontext() as context:
                context.prec = 38
                expected = oi_value.ln() - previous.ln()
            # The retained v1 canonical decimal serializer normalizes at the
            # default 28-digit precision, while this calculation uses 38.
            # Admit that exact historical representation as well as the full
            # in-memory result; never rewrite it or accept a numeric tolerance.
            with localcontext() as retained_context:
                retained_context.prec = 28
                retained_context.rounding = "ROUND_HALF_EVEN"
                retained_v1 = expected.normalize()
            if log_change not in (expected, retained_v1):
                raise ValueError(
                    "market_derivative_state_invalid: OI log change does not reconcile"
                )
        if (funding_rate is None) != (self.funding_series_id is None):
            raise ValueError(
                "market_derivative_state_invalid: funding identity/value mismatch"
            )
        if funding_rate is not None and self.funding_semantics != "provider_reported":
            raise ValueError(
                "market_derivative_state_invalid: funding semantics are unsupported"
            )
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "oi_value", oi_value)
        object.__setattr__(self, "oi_previous_value", previous)
        object.__setattr__(self, "oi_log_change", log_change)
        object.__setattr__(self, "funding_rate", funding_rate)
        for name in ("oi_sample_time", "funding_sample_time", "funding_time"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value, field_name=name))
        object.__setattr__(
            self,
            "input_fingerprint",
            _validate_hash(self.input_fingerprint, field_name="input_fingerprint"),
        )


    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": DERIVATIVE_STATE_FACT_VERSION,
                "instrument_id": self.instrument_id,
                "series_id": self.series_id,
                "effective_at": _canonical_time(self.effective_at),
                "oi_series_id": self.oi_series_id,
                "oi_sample_time": (
                    _canonical_time(self.oi_sample_time)
                    if self.oi_sample_time is not None
                    else None
                ),
                "oi_market_commit_seq": self.oi_market_commit_seq,
                "oi_value": _canonical_decimal(self.oi_value),
                "oi_previous_value": _canonical_decimal(self.oi_previous_value),
                "oi_log_change": _canonical_decimal(self.oi_log_change),
                "funding_series_id": self.funding_series_id,
                "funding_sample_time": (
                    _canonical_time(self.funding_sample_time)
                    if self.funding_sample_time is not None
                    else None
                ),
                "funding_market_commit_seq": self.funding_market_commit_seq,
                "funding_rate": _canonical_decimal(self.funding_rate),
                "funding_time": (
                    _canonical_time(self.funding_time)
                    if self.funding_time is not None
                    else None
                ),
                "funding_interval_seconds": self.funding_interval_seconds,
                "funding_semantics": self.funding_semantics,
                "input_fingerprint": self.input_fingerprint,
            }
        )


@dataclass(frozen=True)
class ResponseFeatureFact:
    series_id: int
    bucket_start: datetime
    source_flow_feature_series_id: int
    source_l2_series_id: int
    source_flow_material_hash: str
    pre_state_hash: str
    trough_state_hash: str
    post_state_hash: str
    bucket_end: datetime
    effective_at: datetime
    known_at: datetime
    direction: MarketSide
    first_trade_id: str
    last_trade_id: str
    first_trade_source_position: Mapping[str, Any]
    last_trade_source_position: Mapping[str, Any]
    pre_book_source_position: BookSourcePosition
    trough_book_source_position: BookSourcePosition
    post_book_source_position: BookSourcePosition
    validity_interval_id: str
    aggressive_notional: Decimal
    signed_aggressive_notional: Decimal
    response_bps: Decimal
    pre_depth_notional: Decimal
    consumed_depth_notional: Decimal
    replenished_depth_notional: Decimal
    depth_replenishment: Decimal
    liquidity_adjusted_impact: Decimal
    price_response_per_flow: Decimal
    input_fingerprint: str
    def __post_init__(self) -> None:
        if (
            int(self.series_id) <= 0
            or int(self.source_flow_feature_series_id) <= 0
            or int(self.source_l2_series_id) <= 0
        ):
            raise ValueError(
                "market_response_invalid: series identities must be positive"
            )
        start = _utc(self.bucket_start, field_name="bucket_start")
        end = _utc(self.bucket_end, field_name="bucket_end")
        effective = _utc(self.effective_at, field_name="effective_at")
        known = _utc(self.known_at, field_name="known_at")
        if end != start + timedelta(seconds=1) or effective < end or known < effective:
            raise ValueError("market_response_invalid: causal timing is invalid")
        try:
            direction = MarketSide(
                str(getattr(self.direction, "value", self.direction))
            )
        except ValueError as exc:
            raise ValueError("market_response_invalid: direction is unsupported") from exc
        aggressive = _decimal(
            self.aggressive_notional,
            field_name="aggressive_notional",
            positive=True,
        )
        signed = _decimal(
            self.signed_aggressive_notional,
            field_name="signed_aggressive_notional",
        )
        if signed != (aggressive if direction is MarketSide.BUY else -aggressive):
            raise ValueError(
                "market_response_invalid: signed notional does not reconcile"
            )
        pre_depth = _decimal(
            self.pre_depth_notional,
            field_name="pre_depth_notional",
            positive=True,
        )
        consumed = _decimal(
            self.consumed_depth_notional,
            field_name="consumed_depth_notional",
            positive=True,
        )
        replenished = _decimal(
            self.replenished_depth_notional,
            field_name="replenished_depth_notional",
            nonnegative=True,
        )
        replenishment = _decimal(
            self.depth_replenishment,
            field_name="depth_replenishment",
            nonnegative=True,
        )
        response = _decimal(self.response_bps, field_name="response_bps")
        liquidity_impact = _decimal(
            self.liquidity_adjusted_impact,
            field_name="liquidity_adjusted_impact",
        )
        response_per_flow = _decimal(
            self.price_response_per_flow,
            field_name="price_response_per_flow",
        )
        if replenishment != replenished / consumed:
            raise ValueError(
                "market_response_invalid: replenishment does not reconcile"
            )
        if liquidity_impact != response / (pre_depth / ONE_MILLION):
            raise ValueError(
                "market_response_invalid: liquidity impact does not reconcile"
            )
        if response_per_flow != response / (aggressive / ONE_MILLION):
            raise ValueError(
                "market_response_invalid: price response does not reconcile"
            )
        if not all(
            str(value or "").strip()
            for value in (
                self.first_trade_id,
                self.last_trade_id,
                self.validity_interval_id,
            )
        ):
            raise ValueError("market_response_invalid: source identities are required")
        if _position_key(self.trough_book_source_position) < _position_key(
            self.pre_book_source_position
        ) or _position_key(self.post_book_source_position) < _position_key(
            self.trough_book_source_position
        ):
            raise ValueError("market_response_invalid: book positions are not ordered")
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(
            self,
            "source_flow_feature_series_id",
            int(self.source_flow_feature_series_id),
        )
        object.__setattr__(self, "source_l2_series_id", int(self.source_l2_series_id))
        object.__setattr__(self, "bucket_start", start)
        object.__setattr__(self, "bucket_end", end)
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "aggressive_notional", aggressive)
        object.__setattr__(self, "signed_aggressive_notional", signed)
        object.__setattr__(self, "pre_depth_notional", pre_depth)
        object.__setattr__(self, "consumed_depth_notional", consumed)
        object.__setattr__(self, "replenished_depth_notional", replenished)
        object.__setattr__(self, "depth_replenishment", replenishment)
        object.__setattr__(self, "response_bps", response)
        object.__setattr__(self, "liquidity_adjusted_impact", liquidity_impact)
        object.__setattr__(self, "price_response_per_flow", response_per_flow)
        for name in (
            "source_flow_material_hash",
            "pre_state_hash",
            "trough_state_hash",
            "post_state_hash",
            "input_fingerprint",
        ):
            object.__setattr__(self, name, _validate_hash(getattr(self, name), field_name=name))


    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": RESPONSE_FACT_VERSION,
                "series_id": self.series_id,
                "bucket_start": _canonical_time(self.bucket_start),
                "source_flow_feature_series_id": self.source_flow_feature_series_id,
                "source_l2_series_id": self.source_l2_series_id,
                "source_flow_material_hash": self.source_flow_material_hash,
                "pre_state_hash": self.pre_state_hash,
                "trough_state_hash": self.trough_state_hash,
                "post_state_hash": self.post_state_hash,
                "direction": self.direction.value,
                "first_trade_id": self.first_trade_id,
                "last_trade_id": self.last_trade_id,
                "first_trade_source_position": dict(self.first_trade_source_position),
                "last_trade_source_position": dict(self.last_trade_source_position),
                "pre_book_source_position": self.pre_book_source_position.material(),
                "trough_book_source_position": self.trough_book_source_position.material(),
                "post_book_source_position": self.post_book_source_position.material(),
                "validity_interval_id": self.validity_interval_id,
                "aggressive_notional": _canonical_decimal(self.aggressive_notional),
                "signed_aggressive_notional": _canonical_decimal(
                    self.signed_aggressive_notional
                ),
                "response_bps": _canonical_decimal(self.response_bps),
                "pre_depth_notional": _canonical_decimal(self.pre_depth_notional),
                "consumed_depth_notional": _canonical_decimal(
                    self.consumed_depth_notional
                ),
                "replenished_depth_notional": _canonical_decimal(
                    self.replenished_depth_notional
                ),
                "depth_replenishment": _canonical_decimal(self.depth_replenishment),
                "liquidity_adjusted_impact": _canonical_decimal(
                    self.liquidity_adjusted_impact
                ),
                "price_response_per_flow": _canonical_decimal(
                    self.price_response_per_flow
                ),
                "input_fingerprint": self.input_fingerprint,
            }
        )


def _depth_values(
    state: BookStateView,
    *,
    contract: MarketStateValuationContract,
    band_bps: int,
) -> Mapping[str, Any]:
    bid_price = state.bids[-1][0]
    ask_price = state.asks[0][0]
    mid = (bid_price + ask_price) / Decimal(2)
    fraction = Decimal(int(band_bps)) / TEN_THOUSAND
    bid_floor = mid * (Decimal(1) - fraction)
    ask_ceiling = mid * (Decimal(1) + fraction)
    bids = [(price, quantity) for price, quantity in state.bids if price >= bid_floor]
    asks = [(price, quantity) for price, quantity in state.asks if price <= ask_ceiling]
    bid_quantity = sum((quantity for _price, quantity in bids), Decimal(0))
    ask_quantity = sum((quantity for _price, quantity in asks), Decimal(0))
    bid_base = bid_quantity * contract.base_multiplier
    ask_base = ask_quantity * contract.base_multiplier
    if contract.linear_quote_notional:
        bid_notional = sum(
            (price * quantity * contract.base_multiplier for price, quantity in bids),
            Decimal(0),
        )
        ask_notional = sum(
            (price * quantity * contract.base_multiplier for price, quantity in asks),
            Decimal(0),
        )
    else:
        bid_notional = None
        ask_notional = None
    denominator = bid_base + ask_base
    imbalance = (bid_base - ask_base) / denominator if denominator > 0 else None
    return {
        "mid": mid,
        "bid_quantity": bid_quantity,
        "ask_quantity": ask_quantity,
        "bid_base": bid_base,
        "ask_base": ask_base,
        "bid_notional": bid_notional,
        "ask_notional": ask_notional,
        "imbalance": imbalance,
    }


def derive_book_features(
    states: Iterable[BookStateView],
    *,
    contract: MarketStateValuationContract,
    bbo_series_id: int,
    depth_series_id: int,
    computed_at: datetime,
) -> tuple[tuple[BboFeatureFact, ...], tuple[DepthFeatureFact, ...]]:
    """Downsample valid post-event states to one deterministic row per UTC second."""

    computed = _utc(computed_at, field_name="computed_at")
    selected: dict[datetime, BookStateView] = {}
    for state in states:
        if state.product_definition_version_id != contract.product_definition_version_id:
            raise ValueError("market_state_contract_mismatch: product definition differs")
        if state.provider_size_unit is not contract.provider_size_unit:
            raise ValueError("market_state_contract_mismatch: provider size unit differs")
        if state.known_at > computed:
            continue
        start = _bucket_start(state.effective_at, interval_seconds=1)
        end = start + timedelta(seconds=1)
        if end > computed:
            continue
        prior = selected.get(start)
        identity = (state.effective_at, _position_key(state.source_position))
        if prior is None or identity > (
            prior.effective_at,
            _position_key(prior.source_position),
        ):
            selected[start] = state

    bbo_rows: list[BboFeatureFact] = []
    depth_rows: list[DepthFeatureFact] = []
    for start, state in sorted(selected.items()):
        bid_price, bid_quantity = state.bids[-1]
        ask_price, ask_quantity = state.asks[0]
        mid = (bid_price + ask_price) / Decimal(2)
        spread = ask_price - bid_price
        fingerprint = _stable_hash(
            {
                "schema_version": "market.book_feature_input.v1",
                "bucket_start": _canonical_time(start),
                "state_hash": state.state_hash,
                "source_position": state.source_position.material(),
                "contract_hash": contract.material_hash,
            }
        )
        known = max(state.known_at, start + timedelta(seconds=1))
        bbo = BboFeatureFact(
            series_id=int(bbo_series_id),
            source_l2_series_id=state.series_id,
            bucket_start=start,
            bucket_end=start + timedelta(seconds=1),
            source_effective_at=state.effective_at,
            known_at=known,
            source_position=state.source_position,
            validity_interval_id=state.validity_interval_id,
            product_definition_version_id=state.product_definition_version_id,
            provider_size_unit=state.provider_size_unit,
            source_state_hash=state.state_hash,
            bid_price=bid_price,
            bid_quantity=bid_quantity,
            bid_base_quantity=bid_quantity * contract.base_multiplier,
            ask_price=ask_price,
            ask_quantity=ask_quantity,
            ask_base_quantity=ask_quantity * contract.base_multiplier,
            mid_price=mid,
            spread=spread,
            spread_bps=TEN_THOUSAND * spread / mid,
            input_fingerprint=fingerprint,
        )
        bbo_rows.append(bbo)
        for band in APPROVED_DEPTH_BANDS_BPS:
            values = _depth_values(state, contract=contract, band_bps=band)
            depth_fingerprint = _stable_hash(
                {
                    "schema_version": "market.depth_feature_input.v1",
                    "bbo_input_fingerprint": fingerprint,
                    "band_bps": band,
                    "state_hash": state.state_hash,
                }
            )
            depth_rows.append(
                DepthFeatureFact(
                    series_id=int(depth_series_id),
                    source_l2_series_id=state.series_id,
                    bucket_start=start,
                    bucket_end=start + timedelta(seconds=1),
                    source_effective_at=state.effective_at,
                    known_at=known,
                    source_position=state.source_position,
                    validity_interval_id=state.validity_interval_id,
                    source_state_hash=state.state_hash,
                    bbo_input_fingerprint=fingerprint,
                    provider_size_unit=state.provider_size_unit,
                    band_bps=band,
                    mid_price=values["mid"],
                    bid_quantity=values["bid_quantity"],
                    ask_quantity=values["ask_quantity"],
                    bid_base_quantity=values["bid_base"],
                    ask_base_quantity=values["ask_base"],
                    bid_notional=values["bid_notional"],
                    ask_notional=values["ask_notional"],
                    imbalance=values["imbalance"],
                    input_fingerprint=depth_fingerprint,
                )
            )
    return tuple(bbo_rows), tuple(depth_rows)


def derive_trade_flow_feature(
    *,
    series_id: int,
    source_trade_flow_series_id: int,
    aggregate: TradeFlowAggregateFact,
    trades: Iterable[MarketTradeFact],
    computed_at: datetime,
) -> Optional[TradeFlowFeatureFact]:
    """Derive side notional and normalized CVD only from a complete proven bucket."""

    if not aggregate.aggregate_complete or aggregate.trade_count == 0:
        return None
    computed = _utc(computed_at, field_name="computed_at")
    if computed < aggregate.bucket_end:
        raise ValueError("market_flow_feature_invalid: computed before bucket end")
    identities: dict[tuple[str, str], MarketTradeFact] = {}
    for trade in trades:
        if not aggregate.bucket_start <= trade.provider_event_time < aggregate.bucket_end:
            continue
        key = (trade.provider_product_id, trade.provider_trade_id)
        prior = identities.get(key)
        if prior is not None and prior.material_hash != trade.material_hash:
            raise ValueError("market_flow_feature_invalid: conflicting trade identity")
        identities[key] = trade
    rows = tuple(identities.values())
    if len(rows) != aggregate.trade_count:
        raise ValueError("market_flow_feature_invalid: trade count does not reconcile")
    if any(
        trade.aggressor_side is None
        or trade.base_quantity is None
        or trade.quote_notional is None
        for trade in rows
    ):
        return None
    buys = [trade for trade in rows if trade.aggressor_side is MarketSide.BUY]
    sells = [trade for trade in rows if trade.aggressor_side is MarketSide.SELL]
    buy_base = sum((trade.base_quantity for trade in buys if trade.base_quantity is not None), Decimal(0))
    sell_base = sum((trade.base_quantity for trade in sells if trade.base_quantity is not None), Decimal(0))
    buy_notional = sum((trade.quote_notional for trade in buys if trade.quote_notional is not None), Decimal(0))
    sell_notional = sum((trade.quote_notional for trade in sells if trade.quote_notional is not None), Decimal(0))
    total_notional = buy_notional + sell_notional
    if aggregate.quote_notional is None or total_notional != aggregate.quote_notional:
        raise ValueError("market_flow_feature_invalid: quote notional does not reconcile")
    cvd_base = buy_base - sell_base
    denominator = buy_base + sell_base
    fingerprint = _stable_hash(
        {
            "schema_version": "market.trade_flow_feature_input.v1",
            "aggregate_material_hash": aggregate.material_hash,
            "trade_material_hashes": sorted(trade.material_hash for trade in rows),
        }
    )
    return TradeFlowFeatureFact(
        series_id=int(series_id),
        interval_seconds=aggregate.interval_seconds,
        bucket_start=aggregate.bucket_start,
        source_trade_flow_series_id=int(source_trade_flow_series_id),
        bucket_end=aggregate.bucket_end,
        known_at=max([aggregate.bucket_end, aggregate.known_at] + [trade.known_at for trade in rows]),
        aggregate_material_hash=aggregate.material_hash,
        aggregate_input_fingerprint=aggregate.input_fingerprint,
        trade_count=len(rows),
        quote_notional=total_notional,
        aggressor_buy_base_volume=buy_base,
        aggressor_sell_base_volume=sell_base,
        aggressor_buy_notional=buy_notional,
        aggressor_sell_notional=sell_notional,
        cvd_base=cvd_base,
        cvd_notional=buy_notional - sell_notional,
        cvd_volume_share=(cvd_base / denominator if denominator > 0 else None),
        input_fingerprint=fingerprint,
    )


def derive_basis_features(
    futures_rows: Iterable[BboFeatureFact],
    spot_rows: Iterable[BboFeatureFact],
    *,
    mapping_id: str,
    computed_at: datetime,
    max_staleness: timedelta = BASIS_MAX_STALENESS,
    series_id: int,
) -> tuple[BasisFeatureFact, ...]:
    """Align latest causal spot BBO to each futures BBO without forward fill beyond 2s."""

    computed = _utc(computed_at, field_name="computed_at")
    futures = sorted(futures_rows, key=lambda row: row.effective_at)
    spot = sorted(spot_rows, key=lambda row: row.effective_at)
    results: list[BasisFeatureFact] = []
    spot_index = 0
    latest_spot: Optional[BboFeatureFact] = None
    for future in futures:
        if future.known_at > computed:
            continue
        while spot_index < len(spot) and spot[spot_index].effective_at <= future.effective_at:
            if spot[spot_index].known_at <= computed:
                latest_spot = spot[spot_index]
            spot_index += 1
        if latest_spot is None:
            continue
        spot_age = future.effective_at - latest_spot.effective_at
        if spot_age < timedelta(0) or spot_age > max_staleness:
            continue
        basis = future.mid_price - latest_spot.mid_price
        input_fingerprint = _stable_hash(
            {
                "schema_version": "market.futures_spot_basis_input.v1",
                "mapping_id": mapping_id,
                "futures_material_hash": future.material_hash,
                "spot_material_hash": latest_spot.material_hash,
            }
        )
        results.append(
            BasisFeatureFact(
                mapping_id=str(mapping_id),
                futures_series_id=future.series_id,
                spot_series_id=latest_spot.series_id,
                series_id=int(series_id),
                effective_at=future.effective_at,
                known_at=max(future.known_at, latest_spot.known_at),
                futures_bbo_material_hash=future.material_hash,
                spot_bbo_material_hash=latest_spot.material_hash,
                futures_mid=future.mid_price,
                spot_mid=latest_spot.mid_price,
                futures_staleness_seconds=Decimal(0),
                spot_staleness_seconds=Decimal(str(spot_age.total_seconds())),
                basis=basis,
                basis_bps=TEN_THOUSAND * basis / latest_spot.mid_price,
                input_fingerprint=input_fingerprint,
            )
        )
    return tuple(results)


def _gap_intersects(
    gaps: Sequence[Mapping[str, Any]], *, start: datetime, end: datetime
) -> bool:
    for gap in gaps:
        gap_start = datetime.fromisoformat(str(gap["start"]).replace("Z", "+00:00"))
        gap_end = datetime.fromisoformat(str(gap["end"]).replace("Z", "+00:00"))
        if _utc(gap_end, field_name="gap_end") > start and _utc(
            gap_start, field_name="gap_start"
        ) < end:
            return True
    return False


def derivative_state_input_fingerprint(
    *, instrument_id: str, effective_at: datetime,
    oi_record: Optional[OpenInterestRecord], previous_oi_record: Optional[OpenInterestRecord],
    funding_record: Optional[FundingRateRecord],
) -> str:
    """One owner for the retained v1 derivative input identity.

    Previous OI is identified by commit sequence even when another delivery has
    identical value. Archive admission and derivation must use this same hash.
    """
    return _stable_hash({
        "schema_version": "market.derivative_state_input.v1",
        "instrument_id": instrument_id,
        "effective_at": _canonical_time(effective_at),
        "oi": ({
            "series_id": oi_record.series_id,
            "commit_seq": oi_record.market_commit_seq,
            "row_hash": oi_record.fact.row_hash,
            "previous_commit_seq": previous_oi_record.market_commit_seq if previous_oi_record is not None else None,
        } if oi_record is not None else None),
        "funding": ({
            "series_id": funding_record.series_id,
            "commit_seq": funding_record.market_commit_seq,
            "row_hash": funding_record.fact.row_hash,
        } if funding_record is not None else None),
    })


def derive_derivative_state_features(
    *,
    instrument_id: str,
    oi_records: Sequence[OpenInterestRecord],
    funding_records: Sequence[FundingRateRecord],
    oi_gaps: Sequence[Mapping[str, Any]],
    series_id: int,
    expected_oi_interval_seconds: int,
    computed_at: datetime,
) -> tuple[DerivativeStateFeatureFact, ...]:
    """Causally align proven OI changes and provider-reported funding levels."""

    computed = _utc(computed_at, field_name="computed_at")
    oi = sorted(
        (record for record in oi_records if record.fact.known_at <= computed),
        key=lambda record: record.fact.sample_time,
    )
    funding = sorted(
        (record for record in funding_records if record.fact.known_at <= computed),
        key=lambda record: record.fact.sample_time,
    )
    effective_times = sorted(
        {
            _bucket_start(record.fact.sample_time, interval_seconds=60)
            + timedelta(seconds=60)
            for record in (*oi, *funding)
            if record.fact.sample_time < computed
        }
    )
    results: list[DerivativeStateFeatureFact] = []
    for effective in effective_times:
        current_oi = next(
            (record for record in reversed(oi) if record.fact.sample_time < effective),
            None,
        )
        previous_oi = None
        if current_oi is not None:
            current_index = oi.index(current_oi)
            if current_index > 0:
                candidate = oi[current_index - 1]
                if (
                    current_oi.fact.sample_time - candidate.fact.sample_time
                    == timedelta(seconds=int(expected_oi_interval_seconds))
                    and candidate.fact.value > 0
                    and current_oi.fact.value > 0
                    and not _gap_intersects(
                        oi_gaps,
                        start=candidate.fact.sample_time,
                        end=current_oi.fact.sample_time,
                    )
                ):
                    previous_oi = candidate
        current_funding = next(
            (
                record
                for record in reversed(funding)
                if record.fact.sample_time < effective
            ),
            None,
        )
        oi_log_change = None
        if current_oi is not None and previous_oi is not None:
            with localcontext() as context:
                context.prec = 38
                oi_log_change = Decimal(str(current_oi.fact.value)).ln() - Decimal(
                    str(previous_oi.fact.value)
                ).ln()
        funding_rate = (
            Decimal(str(current_funding.fact.rate))
            if current_funding is not None
            else None
        )
        if oi_log_change is None and funding_rate is None:
            continue
        fingerprint = derivative_state_input_fingerprint(instrument_id=instrument_id, effective_at=effective,
            oi_record=current_oi, previous_oi_record=previous_oi, funding_record=current_funding)
        known_values = [effective]
        if current_oi is not None:
            known_values.append(current_oi.fact.known_at)
        if previous_oi is not None:
            known_values.append(previous_oi.fact.known_at)
        if current_funding is not None:
            known_values.append(current_funding.fact.known_at)
        results.append(
            DerivativeStateFeatureFact(
                series_id=int(series_id),
                instrument_id=str(instrument_id),
                effective_at=effective,
                known_at=max(known_values),
                oi_series_id=current_oi.series_id if current_oi is not None else None,
                oi_sample_time=(
                    current_oi.fact.sample_time if current_oi is not None else None
                ),
                oi_market_commit_seq=(
                    current_oi.market_commit_seq if current_oi is not None else None
                ),
                oi_value=(
                    Decimal(str(current_oi.fact.value))
                    if current_oi is not None
                    else None
                ),
                oi_previous_value=(
                    Decimal(str(previous_oi.fact.value))
                    if previous_oi is not None
                    else None
                ),
                oi_log_change=oi_log_change,
                funding_series_id=(
                    current_funding.series_id if current_funding is not None else None
                ),
                funding_sample_time=(
                    current_funding.fact.sample_time
                    if current_funding is not None
                    else None
                ),
                funding_market_commit_seq=(
                    current_funding.market_commit_seq
                    if current_funding is not None
                    else None
                ),
                funding_rate=funding_rate,
                funding_time=(
                    current_funding.fact.funding_time
                    if current_funding is not None
                    else None
                ),
                funding_interval_seconds=(
                    current_funding.fact.interval_seconds
                    if current_funding is not None
                    else None
                ),
                funding_semantics=(
                    "provider_reported" if current_funding is not None else None
                ),
                input_fingerprint=fingerprint,
            )
        )
    return tuple(results)


def _trade_position(trade: MarketTradeFact) -> Mapping[str, Any]:
    return {
        "connection_epoch": trade.connection_epoch,
        "provider_sequence_num": trade.provider_sequence_num,
        "receive_ordinal": trade.receive_ordinal,
        "event_ordinal": trade.event_ordinal,
        "trade_ordinal": trade.trade_ordinal,
        "raw_record_id": trade.raw_record_id,
    }


def derive_response_features(
    states: Sequence[BookStateView],
    trades: Sequence[MarketTradeFact],
    flow_features: Sequence[TradeFlowFeatureFact],
    *,
    contract: MarketStateValuationContract,
    series_id: int,
    computed_at: datetime,
) -> tuple[ResponseFeatureFact, ...]:
    """Derive exact directional 1s replenishment and response facts."""

    computed = _utc(computed_at, field_name="computed_at")
    ordered_states = sorted(states, key=lambda row: (row.effective_at, _position_key(row.source_position)))
    results: list[ResponseFeatureFact] = []
    for flow in flow_features:
        if flow.interval_seconds != 1 or flow.known_at > computed:
            continue
        bucket_trades = [
            trade
            for trade in trades
            if flow.bucket_start <= trade.provider_event_time < flow.bucket_end
            and trade.known_at <= computed
        ]
        for direction in (MarketSide.BUY, MarketSide.SELL):
            directed = sorted(
                (
                    trade
                    for trade in bucket_trades
                    if trade.aggressor_side is direction
                    and trade.quote_notional is not None
                ),
                key=lambda trade: (
                    trade.provider_event_time,
                    trade.connection_epoch,
                    trade.receive_ordinal,
                    trade.event_ordinal,
                    trade.trade_ordinal,
                ),
            )
            if not directed:
                continue
            first_trade = directed[0]
            horizon = first_trade.provider_event_time + timedelta(seconds=1)
            pre_candidates = [
                state
                for state in ordered_states
                if state.effective_at <= first_trade.provider_event_time
                and first_trade.provider_event_time - state.effective_at
                <= RESPONSE_MAX_STALENESS
            ]
            if not pre_candidates:
                continue
            pre = pre_candidates[-1]
            interval_states = [
                state
                for state in ordered_states
                if state.validity_interval_id == pre.validity_interval_id
                and first_trade.provider_event_time < state.effective_at <= horizon
            ]
            post_candidates = [
                state
                for state in ordered_states
                if state.validity_interval_id == pre.validity_interval_id
                and state.effective_at >= horizon
                and state.effective_at - horizon <= RESPONSE_MAX_STALENESS
            ]
            if not interval_states or not post_candidates:
                continue
            post = post_candidates[0]
            side_key = "ask_notional" if direction is MarketSide.BUY else "bid_notional"
            pre_values = _depth_values(pre, contract=contract, band_bps=10)
            if pre_values[side_key] is None or pre_values[side_key] <= 0:
                continue
            depth_states = [pre, *interval_states, post]
            depth_pairs = [
                (
                    state,
                    _depth_values(state, contract=contract, band_bps=10)[side_key],
                )
                for state in depth_states
            ]
            if any(value is None for _state, value in depth_pairs):
                continue
            trough, trough_depth = min(
                ((state, value) for state, value in depth_pairs if value is not None),
                key=lambda item: item[1],
            )
            pre_depth = pre_values[side_key]
            post_depth = _depth_values(post, contract=contract, band_bps=10)[side_key]
            assert pre_depth is not None and post_depth is not None
            consumed = max(pre_depth - trough_depth, Decimal(0))
            if consumed <= 0:
                continue
            replenished = max(post_depth - trough_depth, Decimal(0))
            replenishment = replenished / consumed
            pre_mid = _depth_values(pre, contract=contract, band_bps=10)["mid"]
            post_mid = _depth_values(post, contract=contract, band_bps=10)["mid"]
            raw_response = TEN_THOUSAND * (post_mid - pre_mid) / pre_mid
            directional_response = (
                raw_response if direction is MarketSide.BUY else -raw_response
            )
            aggressive_notional = sum(
                (
                    trade.quote_notional
                    for trade in directed
                    if trade.quote_notional is not None
                ),
                Decimal(0),
            )
            if aggressive_notional <= 0:
                continue
            signed_notional = (
                aggressive_notional
                if direction is MarketSide.BUY
                else -aggressive_notional
            )
            input_fingerprint = _stable_hash(
                {
                    "schema_version": "market.response_feature_input.v1",
                    "flow_material_hash": flow.material_hash,
                    "direction": direction.value,
                    "trade_material_hashes": [
                        trade.material_hash for trade in directed
                    ],
                    "pre_state_hash": pre.state_hash,
                    "trough_state_hash": trough.state_hash,
                    "post_state_hash": post.state_hash,
                    "contract_hash": contract.material_hash,
                }
            )
            results.append(
                ResponseFeatureFact(
                    series_id=int(series_id),
                    source_flow_feature_series_id=flow.series_id,
                    source_l2_series_id=pre.series_id,
                    source_flow_material_hash=flow.material_hash,
                    pre_state_hash=pre.state_hash,
                    trough_state_hash=trough.state_hash,
                    post_state_hash=post.state_hash,
                    bucket_start=flow.bucket_start,
                    bucket_end=flow.bucket_end,
                    effective_at=post.effective_at,
                    known_at=max(
                        [flow.known_at, pre.known_at, trough.known_at, post.known_at]
                        + [trade.known_at for trade in directed]
                    ),
                    direction=direction,
                    first_trade_id=first_trade.provider_trade_id,
                    last_trade_id=directed[-1].provider_trade_id,
                    first_trade_source_position=_trade_position(first_trade),
                    last_trade_source_position=_trade_position(directed[-1]),
                    pre_book_source_position=pre.source_position,
                    trough_book_source_position=trough.source_position,
                    post_book_source_position=post.source_position,
                    validity_interval_id=pre.validity_interval_id,
                    aggressive_notional=aggressive_notional,
                    signed_aggressive_notional=signed_notional,
                    response_bps=directional_response,
                    pre_depth_notional=pre_depth,
                    consumed_depth_notional=consumed,
                    replenished_depth_notional=replenished,
                    depth_replenishment=replenishment,
                    liquidity_adjusted_impact=(
                        directional_response / (pre_depth / ONE_MILLION)
                    ),
                    price_response_per_flow=(
                        raw_response / (signed_notional / ONE_MILLION)
                    ),
                    input_fingerprint=input_fingerprint,
                )
            )
    return tuple(results)


__all__ = [
    "APPROVED_DEPTH_BANDS_BPS",
    "BASIS_FACT_TYPE",
    "BASIS_FACT_VERSION",
    "BBO_FACT_TYPE",
    "BBO_FACT_VERSION",
    "BOOK_FEATURE_INTERVAL_SECONDS",
    "DEPTH_FACT_TYPE",
    "DEPTH_FACT_VERSION",
    "DERIVATIVE_STATE_FACT_TYPE",
    "DERIVATIVE_STATE_FACT_VERSION",
    "DERIVATIVE_OI_INTERVAL_SECONDS",
    "RESPONSE_FACT_TYPE",
    "RESPONSE_FACT_VERSION",
    "TRADE_FLOW_FEATURE_FACT_TYPE",
    "TRADE_FLOW_FEATURE_FACT_VERSION",
    "BasisFeatureFact",
    "BboFeatureFact",
    "DepthFeatureFact",
    "DerivativeStateFeatureFact",
    "MarketStateValuationContract",
    "ResponseFeatureFact",
    "TradeFlowFeatureFact",
    "derive_basis_features",
    "derive_book_features",
    "derive_derivative_state_features",
    "derivative_state_input_fingerprint",
    "derive_response_features",
    "derive_trade_flow_feature",
]
