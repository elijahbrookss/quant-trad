import datetime as dt

import pandas as pd
import yfinance as yf

from core.logger import logger
from .base import BaseDataProvider, DataSource, InstrumentMetadata, InstrumentType

class YahooFinanceProvider(BaseDataProvider):
    def fetch_from_api(
        self,
        symbol: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str
    ) -> pd.DataFrame:
        try:
            df = yf.download(
                symbol,
                start=start,
                end=end,
                interval=interval,
                progress=False,
                threads=False,
            )
        except Exception as e:
            raise RuntimeError(f"YahooFinance download failed: {e}")

        if df is None or df.empty:
            return pd.DataFrame()
        
        # Flatten if MultiIndex or Ticker-based headers
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        elif isinstance(df.columns.name, str) and df.columns.name.lower() == "ticker":
            df.columns.name = None  # Drop the 'Ticker' name 

        df = df.tz_convert(None).reset_index()
        df.columns = [col.lower() for col in df.columns]
        logger.debug("DataFrame columns after cleanup - YFINANCE: %s", df.columns)

        df["timestamp"] = df["datetime"]
        
        return df[["timestamp", "open", "high", "low", "close", "volume"]]
    
    def get_datasource(self):
        return DataSource.YFINANCE.value

    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Return the Yahoo instrument type derived from upstream metadata."""

        return self.validate_instrument_type(venue, symbol)

    def validate_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Confirm the symbol exists and return its instrument type."""

        ticker, fast_info = self._lookup(symbol)
        instrument_type = self._instrument_type_from_quote_type(ticker, fast_info)

        if instrument_type:
            return instrument_type

        # Fall back to a data probe to ensure the symbol is real before raising.
        probe = self._probe_history(ticker)
        if probe is None or probe.empty:
            raise ValueError(f"Symbol '{symbol}' not found on Yahoo Finance")

        raise ValueError(f"Unable to determine instrument type for '{symbol}' on Yahoo Finance")

    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        """Return spot metadata expressed per share/coin."""

        return self._normalize_metadata(tick_size=0.01, contract_size=1.0)

    def validate_symbol(self, venue: str, symbol: str) -> None:
        """Confirm Yahoo Finance recognizes the symbol by probing for data."""

        ticker, fast_info = self._lookup(symbol)

        price = getattr(fast_info, "last_price", None) if fast_info else None
        if price is not None:
            return

        probe = self._probe_history(ticker)
        if probe is None or probe.empty:
            raise ValueError(f"Symbol '{symbol}' not found on Yahoo Finance")

    @staticmethod
    def _probe_history(ticker: yf.Ticker):
        try:
            return ticker.history(period="1d", interval="1d")
        except Exception as exc:
            raise ValueError(f"Yahoo Finance lookup failed for '{ticker.ticker}': {exc}") from exc

    def _lookup(self, symbol: str):
        if not symbol:
            raise ValueError("symbol is required for Yahoo Finance validation")

        ticker = yf.Ticker(symbol)
        fast_info = None
        try:
            fast_info = ticker.fast_info
        except Exception as exc:
            logger.warning(
                "yfinance_fast_info_failed | symbol=%s | error=%s",
                symbol,
                exc,
            )
        return ticker, fast_info

    @staticmethod
    def _instrument_type_from_quote_type(ticker: yf.Ticker, fast_info) -> InstrumentType | None:
        quote_type = None
        candidates = []
        for key in ("quote_type", "quoteType"):
            if hasattr(fast_info, key):
                candidates.append(getattr(fast_info, key))
            if isinstance(fast_info, dict) and key in fast_info:
                candidates.append(fast_info.get(key))

        if not quote_type:
            quote_type = next((candidate for candidate in candidates if candidate), None)

        if not quote_type:
            try:
                info = ticker.get_info()
                quote_type = (info or {}).get("quoteType")
            except Exception:
                quote_type = None

        normalized = str(quote_type or "").strip().upper()
        if not normalized:
            return None

        if normalized in {"FUTURE", "OPTION"}:
            return InstrumentType.FUTURE
        if normalized in {"EQUITY", "ETF", "MUTUALFUND", "INDEX", "CRYPTOCURRENCY", "CURRENCY"}:
            return InstrumentType.SPOT

        return None
