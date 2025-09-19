from fastapi import APIRouter
from pydantic import BaseModel
from ..service.candle_service import fetch_ohlcv
import pandas as pd

router = APIRouter()

class CandleRequest(BaseModel):
    symbol: str
    start: str
    end: str
    timeframe: str

@router.post("/")
def get_candles(req: CandleRequest):
    df = fetch_ohlcv(req.symbol, req.start, req.end, req.timeframe)

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
