from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..service.market.candle_service import fetch_ohlcv_by_instrument
import pandas as pd
from datetime import datetime, timezone
import logging

router = APIRouter()
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
    instrument_id: str
    symbol: Optional[str] = None
    start: str
    end: str
    timeframe: str

@router.post("/")
def get_candles(req: CandleRequest):
    start = _normalize_time(req.start)
    end = _normalize_time(req.end)

    logger.info(
        "candle_fetch_request | instrument_id=%s symbol=%s interval=%s start=%s end=%s",
        req.instrument_id,
        req.symbol,
        req.timeframe,
        start,
        end,
    )
    try:
        df = fetch_ohlcv_by_instrument(
            req.instrument_id,
            start,
            end,
            req.timeframe,
        )
    except ValueError as exc:
        logger.warning(
            "candle_fetch_instrument_missing | instrument_id=%s symbol=%s interval=%s start=%s end=%s error=%s",
            req.instrument_id,
            req.symbol,
            req.timeframe,
            start,
            end,
            exc,
        )
        raise HTTPException(status_code=400, detail=str(exc))

    if df is None or df.empty:
       logger.warning(
           "candle_fetch_empty | instrument_id=%s symbol=%s interval=%s start=%s end=%s",
           req.instrument_id,
           req.symbol,
           req.timeframe,
           start,
           end,
       )
       return {"candles": []}

    logger.info(
        "candle_fetch_success | instrument_id=%s symbol=%s interval=%s start=%s end=%s candles=%d",
        req.instrument_id,
        req.symbol,
        req.timeframe,
        start,
        end,
        len(df),
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
