from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel
from ..service.candle_service import fetch_ohlcv
from ..service.provider_service import translate_market
import pandas as pd

router = APIRouter()

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

    df = fetch_ohlcv(
        req.symbol,
        req.start,
        req.end,
        req.timeframe,
        datasource=datasource,
        exchange=exchange,
    )

    if df is None or df.empty:
       return {"candles": []}

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
