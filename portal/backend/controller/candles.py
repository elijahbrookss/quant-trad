from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel
from ..service.market.candle_service import fetch_ohlcv
from ..service.providers.provider_service import translate_market
from ..service.providers.data_provider_resolver import DataProviderResolver
import pandas as pd
from datetime import datetime, timezone
import logging

router = APIRouter()
_resolver = DataProviderResolver()
logger = logging.getLogger(__name__)

def _normalize_time(value: str) -> str:
    """Normalize timestamp input to ISO8601 if numeric epochs are provided."""
    if value is None:
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return value
    if not numeric:
        return value
    # Treat large values as milliseconds.
    if numeric > 2e10:
        numeric /= 1000
    dt = datetime.fromtimestamp(numeric, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

class CandleRequest(BaseModel):
    symbol: str
    start: str
    end: str
    timeframe: str
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None

@router.post("/")
def get_candles(req: CandleRequest):
    datasource = req.datasource
    exchange = req.exchange
    if req.provider_id or req.venue_id:
        provider, venue_exchange = translate_market(req.provider_id, req.venue_id)
        datasource = datasource or provider
        exchange = exchange or venue_exchange
    datasource = _resolver.normalize_datasource(datasource)
    exchange = _resolver.normalize_exchange(exchange)
    start = _normalize_time(req.start)
    end = _normalize_time(req.end)

    logger.info(
        "candle_fetch_request | symbol=%s interval=%s start=%s end=%s datasource=%s exchange=%s",
        req.symbol,
        req.timeframe,
        start,
        end,
        datasource,
        exchange,
    )
    df = fetch_ohlcv(
        req.symbol,
        start,
        end,
        req.timeframe,
        datasource=datasource,
        exchange=exchange,
    )

    if df is None or df.empty:
       logger.warning(
           "candle_fetch_empty | symbol=%s interval=%s start=%s end=%s datasource=%s exchange=%s",
           req.symbol,
           req.timeframe,
           start,
           end,
           datasource,
           exchange,
       )
       return {"candles": []}

    logger.info(
        "candle_fetch_success | symbol=%s interval=%s start=%s end=%s candles=%d datasource=%s exchange=%s",
        req.symbol,
        req.timeframe,
        start,
        end,
        len(df),
        datasource,
        exchange,
    )
    return {
        "candles": [
            {
                "time": int(pd.to_datetime(row.name).timestamp()),
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
            }
            for _, row in df.iterrows()
        ]
    }
