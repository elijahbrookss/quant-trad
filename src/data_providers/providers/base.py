from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime as dt
from enum import Enum
from typing import Optional

import pandas as pd



class DataSource(str, Enum):
    YFINANCE = "YFINANCE"
    ALPACA = "ALPACA"
    IBKR = "IBKR"
    CCXT = "CCXT"
    COINBASE = "COINBASE"
    UNKNOWN = "UNKNOWN"


class InstrumentType(str, Enum):
    SPOT = "SPOT"
    FUTURE = "FUTURE"


@dataclass(frozen=True)
class InstrumentMetadata:
    """Standardized instrument metadata expressed per trading unit."""

    tick_size: Optional[float]
    contract_size: Optional[float]
    tick_value: Optional[float]
    min_order_size: Optional[float]
    qty_step: Optional[float]
    max_qty: Optional[float]
    min_notional: Optional[float]
    maker_fee_rate: Optional[float]
    taker_fee_rate: Optional[float]
    margin_rates: Optional[dict]
    can_short: Optional[bool]
    short_requires_borrow: Optional[bool]
    has_funding: Optional[bool]
    expiry_ts: Optional[dt.datetime]
    base_currency: Optional[str]
    quote_currency: Optional[str]
    metadata: Optional[dict]

    def as_dict(self) -> dict:
        expiry_value = self.expiry_ts.isoformat() if self.expiry_ts else None
        instrument_fields = {
            "tick_size": self.tick_size,
            "contract_size": self.contract_size,
            "tick_value": self.tick_value,
            "min_order_size": self.min_order_size,
            "qty_step": self.qty_step,
            "max_qty": self.max_qty,
            "min_notional": self.min_notional,
            "maker_fee_rate": self.maker_fee_rate,
            "taker_fee_rate": self.taker_fee_rate,
            "margin_rates": dict(self.margin_rates or {}),
            "can_short": self.can_short,
            "short_requires_borrow": self.short_requires_borrow,
            "has_funding": self.has_funding,
            "expiry_ts": expiry_value,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
        }
        return {
            "instrument_fields": instrument_fields,
            "provider_metadata": dict(self.metadata or {}),
        }


class ProviderInterface(ABC):
    """Minimal provider contract covering market metadata and fetch operations."""

    @abstractmethod
    def get_datasource(self) -> str:
        pass

    @abstractmethod
    def fetch_from_api(self, symbol: str, start: dt.datetime, end: dt.datetime, interval: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Return a binary instrument classification (spot vs futures/perps)."""

    @abstractmethod
    def validate_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Raise if the instrument type cannot be confirmed."""

    @abstractmethod
    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        """Return tick_size, contract_size, and tick_value for a trading unit."""

    @abstractmethod
    def validate_symbol(self, venue: str, symbol: str) -> None:
        """Raise if the symbol does not exist for the provider/venue."""


class BaseDataProvider(ProviderInterface):
    """Acquisition-only provider base; persistence belongs to market-data services."""

    @staticmethod
    def _normalize_metadata(
        *,
        tick_size: Optional[float] = None,
        contract_size: Optional[float] = None,
        tick_value: Optional[float] = None,
        min_order_size: Optional[float] = None,
        qty_step: Optional[float] = None,
        max_qty: Optional[float] = None,
        min_notional: Optional[float] = None,
        maker_fee_rate: Optional[float] = None,
        taker_fee_rate: Optional[float] = None,
        margin_rates: Optional[dict] = None,
        can_short: Optional[bool] = None,
        short_requires_borrow: Optional[bool] = None,
        has_funding: Optional[bool] = None,
        expiry_ts: Optional[dt.datetime] = None,
        base_currency: Optional[str] = None,
        quote_currency: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> InstrumentMetadata:
        """Derive a consistent metadata triple from the provided inputs."""

        ts = float(tick_size) if tick_size is not None else None
        cs = float(contract_size) if contract_size is not None else None
        tv = float(tick_value) if tick_value is not None else None

        if ts is None and tv is None:
            raise ValueError("At least tick_size or tick_value must be provided")

        if tv is None and ts is not None and cs is not None:
            tv = ts * cs

        if cs is None and ts is not None and tv is not None and ts != 0:
            cs = tv / ts

        if ts is None and cs is not None and tv is not None and cs != 0:
            ts = tv / cs

        missing = []
        if can_short is None:
            missing.append("can_short")
        if short_requires_borrow is None:
            missing.append("short_requires_borrow")
        if has_funding is None:
            missing.append("has_funding")
        if not base_currency:
            missing.append("base_currency")
        if not quote_currency:
            missing.append("quote_currency")
        if missing:
            raise ValueError(f"Instrument metadata missing fields: {', '.join(missing)}")

        return InstrumentMetadata(
            ts,
            cs,
            tv,
            float(min_order_size) if min_order_size is not None else None,
            float(qty_step) if qty_step is not None else None,
            float(max_qty) if max_qty is not None else None,
            float(min_notional) if min_notional is not None else None,
            float(maker_fee_rate) if maker_fee_rate is not None else None,
            float(taker_fee_rate) if taker_fee_rate is not None else None,
            dict(margin_rates or {}),
            can_short,
            short_requires_borrow,
            has_funding,
            expiry_ts,
            str(base_currency).upper() if base_currency else None,
            str(quote_currency).upper() if quote_currency else None,
            dict(metadata or {}),
        )
