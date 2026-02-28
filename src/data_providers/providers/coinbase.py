"""Coinbase Advanced Trade provider (minimal SDK-backed implementation)."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

try:
    from coinbase.rest import RESTClient
    COINBASE_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover - handled in __init__
    RESTClient = None
    COINBASE_SDK_AVAILABLE = False

from core.logger import logger
from data_providers.registry import _REGISTRY
from data_providers.services.credential_store import load_credentials
from .base import BaseDataProvider, InstrumentMetadata, InstrumentType


# Register provider/venue metadata via decorator-friendly registry.
@_REGISTRY.provider(
    id="COINBASE",
    label="Coinbase Direct API",
    supported_venues=["COINBASE_DIRECT"],
    capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["crypto"]},
)
def _register_coinbase_provider():
    return CoinbaseProvider


@_REGISTRY.venue(
    id="COINBASE_DIRECT",
    label="Coinbase Direct API",
    provider_id="COINBASE",
    adapter_id=None,
    asset_class="crypto",
    required_secrets=["COINBASE_API_KEY", "COINBASE_API_SECRET"],
)
def _register_coinbase_direct():
    return "COINBASE_DIRECT"

class CoinbaseAPIError(Exception):
    """Generic Coinbase provider error."""


class Granularity(str, Enum):
    """Candle granularities supported by Coinbase Advanced Trade API."""

    ONE_MINUTE = "ONE_MINUTE"
    FIVE_MINUTE = "FIVE_MINUTE"
    FIFTEEN_MINUTE = "FIFTEEN_MINUTE"
    THIRTY_MINUTE = "THIRTY_MINUTE"
    ONE_HOUR = "ONE_HOUR"
    TWO_HOUR = "TWO_HOUR"
    FOUR_HOUR = "FOUR_HOUR"
    SIX_HOUR = "SIX_HOUR"
    ONE_DAY = "ONE_DAY"


@dataclass
class CoinbaseProduct:
    """Subset of product fields required for metadata mapping."""

    product_id: str
    product_type: Optional[str]
    price_increment: Optional[str]
    quote_increment: Optional[str]
    base_currency_id: Optional[str]
    quote_currency_id: Optional[str]
    base_display_symbol: Optional[str]
    quote_display_symbol: Optional[str]
    base_min_size: Optional[str]
    future_product_details: Optional[Dict[str, Any]]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CoinbaseProduct":
        return cls(
            product_id=data.get("product_id", ""),
            product_type=data.get("product_type"),
            price_increment=data.get("price_increment"),
            quote_increment=data.get("quote_increment"),
            base_currency_id=data.get("base_currency_id"),
            quote_currency_id=data.get("quote_currency_id"),
            base_display_symbol=data.get("base_display_symbol"),
            quote_display_symbol=data.get("quote_display_symbol"),
            base_min_size=data.get("base_min_size"),
            future_product_details=data.get("future_product_details"),
        )


class CoinbaseProvider(BaseDataProvider):
    """SDK-backed Coinbase Advanced Trade provider."""

    GRANULARITY_MAP = {
        "1m": Granularity.ONE_MINUTE,
        "5m": Granularity.FIVE_MINUTE,
        "15m": Granularity.FIFTEEN_MINUTE,
        "30m": Granularity.THIRTY_MINUTE,
        "1h": Granularity.ONE_HOUR,
        "2h": Granularity.TWO_HOUR,
        "4h": Granularity.FOUR_HOUR,
        "6h": Granularity.SIX_HOUR,
        "1d": Granularity.ONE_DAY,
    }

    GRANULARITY_SECONDS = {
        Granularity.ONE_MINUTE: 60,
        Granularity.FIVE_MINUTE: 300,
        Granularity.FIFTEEN_MINUTE: 900,
        Granularity.THIRTY_MINUTE: 1800,
        Granularity.ONE_HOUR: 3600,
        Granularity.TWO_HOUR: 7200,
        Granularity.FOUR_HOUR: 14400,
        Granularity.SIX_HOUR: 21600,
        Granularity.ONE_DAY: 86400,
    }

    MAX_CANDLES_PER_REQUEST = 300

    def __init__(
        self,
        *,
        persistence=None,
        settings=None,
        timeout: int = 30,
    ) -> None:
        super().__init__(persistence=persistence, settings=settings)

        if not COINBASE_SDK_AVAILABLE:
            raise ImportError(
                "CoinbaseProvider requires coinbase-advanced-py to be installed."
            )

        self._api_key, self._api_secret = self._resolve_credentials()
        self._client = RESTClient(
            api_key=self._api_key,
            api_secret=self._api_secret,
            timeout=timeout,
        )

        self._last_product_payload: Dict[str, Any] = {}

    # Credentials / helpers -------------------------------------------------
    def _resolve_credentials(self) -> Tuple[str, str]:
        # DB-backed provider credentials are the only supported path.
        try:
            stored = load_credentials("COINBASE", "COINBASE_DIRECT")
        except Exception as exc:
            logger.error("coinbase_credentials_store_error | error=%s", exc)
            raise RuntimeError(
                "Coinbase credentials unavailable from provider credential store. "
                "Fix PROVIDER_CREDENTIAL_KEY and re-save COINBASE credentials."
            ) from exc

        if not stored:
            raise RuntimeError(
                "Coinbase credentials missing from provider credential store for COINBASE/COINBASE_DIRECT. "
                "Save credentials via provider settings before starting Coinbase data operations."
            )

        api_key = str(stored.get("COINBASE_API_KEY") or "").strip()
        api_secret = str(stored.get("COINBASE_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            raise RuntimeError(
                "Coinbase credentials incomplete in provider credential store; "
                "both COINBASE_API_KEY and COINBASE_API_SECRET are required."
            )
        return api_key, api_secret

    @staticmethod
    def _response_to_dict(response: Any) -> Dict[str, Any]:
        if response is None:
            return {}
        if isinstance(response, dict):
            return response
        if hasattr(response, "to_dict"):
            return response.to_dict()
        if hasattr(response, "__dict__"):
            return dict(response.__dict__)
        return {}

    def _load_product(self, symbol: str) -> CoinbaseProduct:
        if not symbol:
            raise ValueError("Symbol is required for Coinbase lookup.")
        if not self._api_key or not self._api_secret:
            raise ValueError("Coinbase API credentials are missing. Add API keys to continue.")

        try:
            response = self._client.get_product(product_id=symbol)
        except Exception as exc:
            logger.warning("coinbase_product_lookup_failed | symbol=%s | error=%s", symbol, exc)
            raise ValueError(f"Coinbase product lookup failed: {exc}") from exc

        data = self._response_to_dict(response)
        self._last_product_payload = data
        if not data:
            raise ValueError(
                f"Coinbase did not return product metadata for '{symbol}'. "
                "Ensure the symbol exists and API keys have sufficient access."
            )
        return CoinbaseProduct.from_dict(data)

    def get_datasource(self) -> str:
        return "COINBASE"

    def validate_symbol(self, venue: str, symbol: str) -> None:
        if not symbol:
            raise ValueError("symbol is required for Coinbase validation")
        try:
            self._load_product(symbol)
        except Exception as exc:
            raise ValueError(str(exc)) from exc

    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        try:
            product = self._load_product(symbol)
        except Exception as exc:
            logger.warning(
                "coinbase_instrument_type_failed | symbol=%s | error=%s",
                symbol,
                exc,
            )
            return None  # type: ignore[return-value]
        if not product:
            return None  # type: ignore[return-value]
        product_type = str(product.product_type or "").upper()
        if product_type == "FUTURE":
            return InstrumentType.FUTURE
        if product_type == "SPOT":
            return InstrumentType.SPOT
        return None  # type: ignore[return-value]

    def validate_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        product = self._load_product(symbol)
        if not product:
            raise ValueError(f"Symbol '{symbol}' not returned by Coinbase. Check API keys and spelling.")

        product_type = str(product.product_type or "").upper()
        if product_type == "FUTURE":
            return InstrumentType.FUTURE
        if product_type == "SPOT":
            return InstrumentType.SPOT

        raise ValueError(
            f"Unsupported Coinbase product type '{product.product_type}' for symbol '{symbol}'"
        )

    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        product = self._load_product(symbol)
        if not product:
            raise ValueError(f"Symbol '{symbol}' not returned by Coinbase. Verify API keys and symbol.")

        future_details = (
            product.future_product_details
            if isinstance(product.future_product_details, dict)
            else {}
        )
        future_base = future_details.get("contract_root_unit")
        base_currency = product.base_currency_id or future_base or product.base_display_symbol
        quote_currency = product.quote_currency_id or product.quote_display_symbol
        tick_size_value = product.price_increment or product.quote_increment
        min_order_size_value = product.base_min_size

        missing_fields: List[str] = []
        if tick_size_value is None:
            missing_fields.append("tick_size")
        if min_order_size_value is None:
            missing_fields.append("min_order_size")
        if not base_currency:
            missing_fields.append("base_currency")
        if not quote_currency:
            missing_fields.append("quote_currency")

        try:
            tick_size = Decimal(tick_size_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Coinbase product '{symbol}' invalid price increment '{tick_size_value}'"
            ) from exc

        min_order_size = None
        if min_order_size_value is not None:
            try:
                min_order_size = Decimal(min_order_size_value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Coinbase product '{symbol}' invalid base_min_size '{min_order_size_value}'"
                ) from exc
        qty_step = float(min_order_size) if min_order_size is not None else None
        max_qty = None
        raw_base_max_size = (
            (self._last_product_payload or {}).get("base_max_size")
            if isinstance(self._last_product_payload, dict)
            else None
        )
        if raw_base_max_size is not None:
            try:
                max_qty = float(Decimal(raw_base_max_size))
            except (TypeError, ValueError):
                max_qty = None

        product_type = str(product.product_type or "").upper()
        is_future = product_type == "FUTURE"

        contract_size = None
        if is_future:
            raw_contract_size = future_details.get("contract_size")
            if raw_contract_size is None:
                missing_fields.append("contract_size")
            else:
                try:
                    contract_size = Decimal(raw_contract_size)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Coinbase product '{symbol}' invalid contract size '{raw_contract_size}'"
                    ) from exc

        expiry_ts = None
        contract_expiry = future_details.get("contract_expiry")
        if contract_expiry:
            try:
                expiry_ts = pd.to_datetime(contract_expiry, utc=True).to_pydatetime()
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Coinbase product '{symbol}' invalid contract expiry '{contract_expiry}'"
                ) from exc

        funding_fields = {
            "funding_interval": future_details.get("funding_interval"),
            "funding_rate": future_details.get("funding_rate"),
            "funding_time": future_details.get("funding_time"),
        }
        has_funding = is_future and any(funding_fields.values())

        margin_rates = {}
        intraday_margin = future_details.get("intraday_margin_rate")
        if isinstance(intraday_margin, dict):
            margin_rates["intraday"] = {
                "long_margin_rate": intraday_margin.get("long_margin_rate"),
                "short_margin_rate": intraday_margin.get("short_margin_rate"),
            }
        overnight_margin = future_details.get("overnight_margin_rate")
        if isinstance(overnight_margin, dict):
            margin_rates["overnight"] = {
                "long_margin_rate": overnight_margin.get("long_margin_rate"),
                "short_margin_rate": overnight_margin.get("short_margin_rate"),
            }

        fee_tier_payload: Dict[str, Any] = {}
        maker_fee_rate = None
        taker_fee_rate = None
        try:
            summary_response = self._client.get_transaction_summary(
                product_type=product_type or None
            )
            summary_payload = self._response_to_dict(summary_response)
            fee_tier_payload = (
                summary_payload.get("fee_tier")
                if isinstance(summary_payload.get("fee_tier"), dict)
                else {}
            )
        except Exception as exc:
            logger.error(
                "coinbase_transaction_summary_failed | symbol=%s | product_type=%s | error=%s",
                symbol,
                product_type,
                exc,
            )
            raise ValueError(
                f"Coinbase transaction summary failed for '{symbol}': {exc}"
            ) from exc

        maker_fee_rate_value = fee_tier_payload.get("maker_fee_rate")
        if maker_fee_rate_value is None:
            missing_fields.append("maker_fee_rate")
        else:
            try:
                maker_fee_rate = Decimal(maker_fee_rate_value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Coinbase fee tier invalid maker_fee_rate '{maker_fee_rate_value}'"
                ) from exc

        taker_fee_rate_value = fee_tier_payload.get("taker_fee_rate")
        if taker_fee_rate_value is None:
            missing_fields.append("taker_fee_rate")
        else:
            try:
                taker_fee_rate = Decimal(taker_fee_rate_value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Coinbase fee tier invalid taker_fee_rate '{taker_fee_rate_value}'"
                ) from exc

        tick_value = tick_size * contract_size if contract_size is not None else None

        mapped_payload = {
            "symbol": symbol,
            "product_type": product_type,
            "tick_size": str(tick_size_value) if tick_size_value is not None else None,
            "contract_size": str(contract_size) if contract_size is not None else None,
            "tick_value": str(tick_value) if tick_value is not None else None,
            "min_order_size": str(min_order_size) if min_order_size is not None else None,
            "qty_step": str(qty_step) if qty_step is not None else None,
            "max_qty": str(max_qty) if max_qty is not None else None,
            "maker_fee_rate": str(maker_fee_rate) if maker_fee_rate is not None else None,
            "taker_fee_rate": str(taker_fee_rate) if taker_fee_rate is not None else None,
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "has_funding": has_funding,
            "expiry_ts": expiry_ts.isoformat() if expiry_ts else None,
        }
        logger.debug(
            "coinbase_instrument_metadata_mapped | symbol=%s product_type=%s tick_size=%s contract_size=%s min_order_size=%s has_funding=%s expiry_ts=%s",
            symbol,
            product_type,
            mapped_payload.get("tick_size"),
            mapped_payload.get("contract_size"),
            mapped_payload.get("min_order_size"),
            mapped_payload.get("has_funding"),
            mapped_payload.get("expiry_ts"),
        )

        if missing_fields:
            raise ValueError(
                "Coinbase product missing instrument metadata fields "
                f"({', '.join(sorted(set(missing_fields)))}) "
                f"for '{symbol}' | mapped={mapped_payload} | payload={self._last_product_payload}"
            )

        metadata_payload: Dict[str, Any] = {
            "product": dict(self._last_product_payload or {}),
            "fees": {
                "maker_fee_rate": float(maker_fee_rate) if maker_fee_rate is not None else None,
                "taker_fee_rate": float(taker_fee_rate) if taker_fee_rate is not None else None,
            },
        }
        if future_details:
            metadata_payload["future_product_details"] = {
                "venue": future_details.get("venue"),
                "contract_code": future_details.get("contract_code"),
                "contract_root_unit": future_details.get("contract_root_unit"),
                "contract_expiry": future_details.get("contract_expiry"),
                "contract_expiry_type": future_details.get("contract_expiry_type"),
                "contract_size": future_details.get("contract_size"),
                "margin_rates": margin_rates,
                "funding": funding_fields,
            }

        return self._normalize_metadata(
            tick_size=float(tick_size),
            contract_size=float(contract_size) if contract_size is not None else None,
            tick_value=float(tick_value) if tick_value is not None else None,
            min_order_size=float(min_order_size) if min_order_size is not None else None,
            qty_step=qty_step,
            max_qty=max_qty,
            maker_fee_rate=float(maker_fee_rate) if maker_fee_rate is not None else None,
            taker_fee_rate=float(taker_fee_rate) if taker_fee_rate is not None else None,
            margin_rates=margin_rates if margin_rates else None,
            can_short=is_future,
            short_requires_borrow=False,
            has_funding=has_funding,
            expiry_ts=expiry_ts,
            base_currency=base_currency,
            quote_currency=quote_currency,
            metadata=metadata_payload,
        )

    @staticmethod
    def _parse_datetime(value: Union[dt.datetime, str]) -> pd.Timestamp:
        ts = pd.to_datetime(value, utc=True)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts

    @staticmethod
    def _to_unix_seconds(value: Union[dt.datetime, str]) -> int:
        ts = CoinbaseProvider._parse_datetime(value)
        return int(ts.timestamp())

    @staticmethod
    def _coerce_unix_seconds(value: Union[int, str, dt.datetime]) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return CoinbaseProvider._to_unix_seconds(value)

    def _interval_to_granularity(self, interval: str) -> Granularity:
        granularity = self.GRANULARITY_MAP.get(interval.lower())
        if granularity is None:
            raise ValueError(
                f"Unsupported interval '{interval}'. "
                f"Coinbase supports: {list(self.GRANULARITY_MAP.keys())}"
            )
        return granularity

    def fetch_from_api(
        self,
        symbol: str,
        start: Union[dt.datetime, str],
        end: Union[dt.datetime, str],
        interval: str,
    ) -> pd.DataFrame:
        if not symbol:
            raise ValueError("symbol is required for Coinbase candles")

        granularity = self._interval_to_granularity(interval)
        interval_seconds = self.GRANULARITY_SECONDS[granularity]
        start_ts = self._coerce_unix_seconds(start)
        end_ts = self._coerce_unix_seconds(end)

        if end_ts <= start_ts:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        chunk_seconds = interval_seconds * self.MAX_CANDLES_PER_REQUEST
        rows: List[Dict[str, Any]] = []

        window_start = start_ts
        while window_start < end_ts:
            window_end = min(end_ts, window_start + chunk_seconds)
            response = self._client.get_candles(
                product_id=symbol,
                start=str(window_start),
                end=str(window_end),
                granularity=granularity.value,
                limit=self.MAX_CANDLES_PER_REQUEST,
            )
            payload = self._response_to_dict(response)
            for candle in payload.get("candles", []) or []:
                candle_payload = (
                    candle if isinstance(candle, dict) else self._response_to_dict(candle)
                )
                start_value = candle_payload.get("start")
                if start_value is None:
                    continue
                rows.append(
                    {
                        "timestamp": pd.to_datetime(
                            int(start_value), unit="s", utc=True
                        ),
                        "open": float(candle_payload.get("open"))
                        if candle_payload.get("open") is not None
                        else None,
                        "high": float(candle_payload.get("high"))
                        if candle_payload.get("high") is not None
                        else None,
                        "low": float(candle_payload.get("low"))
                        if candle_payload.get("low") is not None
                        else None,
                        "close": float(candle_payload.get("close"))
                        if candle_payload.get("close") is not None
                        else None,
                        "volume": float(candle_payload.get("volume"))
                        if candle_payload.get("volume") is not None
                        else None,
                    }
                )
            window_start = window_end

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        if df.empty:
            return df

        df = df.dropna(subset=["timestamp"])
        df = df.drop_duplicates(subset=["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

        logger.info(
            "coinbase_fetch_complete | symbol=%s | interval=%s | rows=%d | start=%s | end=%s",
            symbol,
            interval,
            len(df),
            pd.to_datetime(start_ts, unit="s", utc=True).isoformat(),
            pd.to_datetime(end_ts, unit="s", utc=True).isoformat(),
        )

        return df
