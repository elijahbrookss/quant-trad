import os
import datetime as dt
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.common.exceptions import APIError
from core.logger import logger
from .base_provider import DataSource, BaseDataProvider, InstrumentMetadata, InstrumentType


load_dotenv("secrets.env")

class AlpacaProvider(BaseDataProvider):
    def __init__(self):
        self._api_key = os.getenv("ALPACA_API_KEY")
        self._secret_key = os.getenv("ALPACA_SECRET_KEY")

        self.client = StockHistoricalDataClient(
            self._api_key,
            self._secret_key,
        )
        self._trading_client = None
        if self._api_key and self._secret_key:
            self._trading_client = TradingClient(
                self._api_key,
                self._secret_key,
                paper=self._paper_trading_enabled(),
            )

    def fetch_from_api(
        self,
        symbol: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str
    ) -> pd.DataFrame:
        tf = {
            "1m": TimeFrame.Minute,
            "5m": TimeFrame(5, TimeFrameUnit.Minute),
            "15m": TimeFrame(15, TimeFrameUnit.Minute),
            "30m": TimeFrame(30, TimeFrameUnit.Minute),
            "1h": TimeFrame.Hour,
            "4h": TimeFrame(4, TimeFrameUnit.Hour),
            "1d": TimeFrame.Day
        }.get(interval)

        
        if tf is None:
            raise ValueError(f"Unsupported interval for Alpaca: {interval}")


        logger.debug("Timeframe for Alpaca: %s", tf)
        bars = self.client.get_stock_bars(
            StockBarsRequest(
                symbol_or_symbols=[symbol],
                start=start,
                end=end,
                timeframe=tf,
                feed=DataFeed.IEX,  # IEX feed is free for Alpaca users
            # feed=DataFeed.SIP,  # SIP feed is paid
            )
        )

        df = bars.df
        if df.empty:
            return pd.DataFrame()
        
        df.reset_index(inplace=True) #Bring indexes to columns

        logger.debug("DataFrame columns after cleanup - ALPACA: %s", df.columns)
        return df[["timestamp", "open", "high", "low", "close", "volume"]]
    
    def get_datasource(self):
        return DataSource.ALPACA.value

    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Alpaca's equities API only delivers spot instruments."""

        return InstrumentType.SPOT

    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        """Return tick and contract details for Alpaca equities."""

        # US equities trade in $0.01 increments; one share is one trading unit.
        return self._normalize_metadata(tick_size=0.01, contract_size=1.0)

    def validate_symbol(self, venue: str, symbol: str) -> None:
        """Confirm the symbol exists via Alpaca's asset lookup."""

        if not symbol:
            raise ValueError("Symbol is required for Alpaca validation")

        if not self._trading_client:
            raise ValueError("Alpaca credentials are required for symbol validation")

        try:
            asset = self._trading_client.get_asset(symbol)
        except APIError as exc:
            logger.warning(
                "alpaca_symbol_lookup_failed | symbol=%s | error=%s",
                symbol,
                exc,
            )
            raise ValueError(f"Alpaca symbol '{symbol}' not found") from exc
        except Exception as exc:
            logger.warning(
                "alpaca_symbol_lookup_error | symbol=%s | error=%s",
                symbol,
                exc,
            )
            raise ValueError(f"Alpaca symbol lookup failed: {exc}") from exc

        if not asset or getattr(asset, "symbol", None) is None:
            raise ValueError(f"Alpaca symbol '{symbol}' not found")

    @staticmethod
    def _paper_trading_enabled() -> bool:
        flag = os.getenv("ALPACA_PAPER", "true").strip().lower()
        return flag in {"1", "true", "yes", "on"}
