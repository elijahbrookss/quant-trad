from dataclasses import asdict
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..service.market import instrument_service
from ..service.market.candle_service import fetch_ohlcv_by_instrument, preflight_candle_coverage_by_instrument
from ..service.market.feed_service import historical_candle_ingestor
from ..service.storage.repos.market_data import market_data_repo
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, DatasetSeriesRequest
from data_providers.utils.ohlcv import interval_to_timedelta
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


class CandleCoverageRequest(BaseModel):
    instrument_id: Optional[str] = None
    symbol: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    start: str
    end: str
    timeframe: str


class CandleIngestionRequest(BaseModel):
    instrument_id: str
    start: str
    end: str
    timeframe: str
    source_revision: Optional[str] = None


class CandleDatasetSeriesRequest(BaseModel):
    instrument_id: str
    start: str
    end: str
    timeframe: str


class CandleDatasetFreezeRequest(BaseModel):
    series: List[CandleDatasetSeriesRequest]
    name: Optional[str] = None
    purpose: str = "research"
    created_by: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


def _dataset_payload(dataset) -> Dict[str, Any]:
    return {
        "schema_version": "market_dataset.v1",
        "contract_version": dataset.contract_version,
        "name": dataset.name,
        "purpose": dataset.purpose,
        "metadata": dict(dataset.metadata),
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "max_commit_seq": dataset.max_commit_seq,
        "series": [dict(row) for row in dataset.series],
    }


@router.post("/ingest")
def ingest_candles(req: CandleIngestionRequest) -> Dict[str, Any]:
    """Explicitly acquire provider candles and persist accepted source facts."""

    try:
        instrument = instrument_service.get_instrument_record(req.instrument_id)
        result = historical_candle_ingestor.ingest_by_instrument(
            instrument,
            start=_normalize_time(req.start),
            end=_normalize_time(req.end),
            interval=req.timeframe,
            source_revision=req.source_revision,
        )
    except (KeyError, ValueError, RuntimeError) as exc:
        logger.warning(
            "candle_ingestion_rejected | instrument_id=%s timeframe=%s start=%s end=%s error=%s",
            req.instrument_id,
            req.timeframe,
            req.start,
            req.end,
            exc,
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "schema_version": "market_candle_ingestion_result.v1",
        "source_id": result.source_id,
        "series_id": result.series_id,
        "gap_evidence_count": result.gap_evidence_count,
        "outcome": asdict(result.outcome),
    }


@router.get("/series")
def list_market_data_series(instrument_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "schema_version": "market_series_catalog.v1",
        "series": market_data_repo.list_series(instrument_id=instrument_id),
    }


@router.post("/datasets/freeze")
def freeze_candle_dataset(req: CandleDatasetFreezeRequest) -> Dict[str, Any]:
    try:
        requests: list[DatasetSeriesRequest] = []
        resolved: list[dict[str, Any]] = []
        for item in req.series:
            timeframe_seconds = int(
                interval_to_timedelta(item.timeframe).total_seconds()
            )
            series_id = market_data_repo.resolve_series_id(
                instrument_id=item.instrument_id,
                fact_type=CANDLE_FACT_TYPE,
                timeframe_seconds=timeframe_seconds,
                contract_version=CANDLE_FACT_VERSION,
            )
            request = DatasetSeriesRequest(
                series_id=series_id,
                start=_normalize_time(item.start),
                end=_normalize_time(item.end),
            )
            requests.append(request)
            resolved.append(
                {
                    "series_id": series_id,
                    "instrument_id": item.instrument_id,
                    "timeframe": item.timeframe,
                    "start": item.start,
                    "end": item.end,
                }
            )
        dataset = market_data_repo.freeze_dataset(
            requests,
            name=req.name,
            purpose=req.purpose,
            created_by=req.created_by,
            metadata={
                **dict(req.metadata or {}),
                "schema_version": "market_dataset_request.v1",
                "resolved_requests": resolved,
            },
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _dataset_payload(dataset)


@router.get("/datasets/{dataset_id}")
def get_candle_dataset(dataset_id: str) -> Dict[str, Any]:
    try:
        return _dataset_payload(market_data_repo.get_dataset(dataset_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/coverage")
def candle_coverage(req: CandleCoverageRequest) -> Dict[str, Any]:
    instrument_id = str(req.instrument_id or "").strip()
    try:
        if not instrument_id:
            instrument_id = instrument_service.require_instrument_id(req.datasource, req.exchange, req.symbol)
        payload = preflight_candle_coverage_by_instrument(
            instrument_id,
            _normalize_time(req.start),
            _normalize_time(req.end),
            req.timeframe,
        )
        logger.info(
            "candle_coverage_preflight | instrument_id=%s symbol=%s datasource=%s exchange=%s timeframe=%s start=%s end=%s status=%s",
            instrument_id,
            req.symbol,
            req.datasource,
            req.exchange,
            req.timeframe,
            req.start,
            req.end,
            payload.get("status"),
        )
        return payload
    except ValueError as exc:
        logger.warning(
            "candle_coverage_preflight_invalid | instrument_id=%s symbol=%s datasource=%s exchange=%s timeframe=%s start=%s end=%s error=%s",
            req.instrument_id,
            req.symbol,
            req.datasource,
            req.exchange,
            req.timeframe,
            req.start,
            req.end,
            exc,
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc

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
