"""FastAPI router exposing instrument metadata CRUD endpoints."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, Field
from datetime import datetime, timezone

from ..service.market import instrument_service
from ..service.market.candle_service import preflight_candle_coverage_by_instrument

router = APIRouter()


class InstrumentPayload(BaseModel):
    """Shared instrument attributes."""

    symbol: str = Field(..., description="Symbol ticker (e.g., LINKUSDT)")
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    instrument_type: Optional[str] = None
    tick_size: Optional[float] = Field(default=None, gt=0)
    tick_value: Optional[float] = None
    contract_size: Optional[float] = Field(default=None, gt=0)
    min_order_size: Optional[float] = Field(default=None, gt=0)
    qty_step: Optional[float] = Field(default=None, gt=0)
    max_qty: Optional[float] = Field(default=None, gt=0)
    min_notional: Optional[float] = Field(default=None, ge=0)
    base_currency: Optional[str] = None
    quote_currency: Optional[str] = None
    can_short: Optional[bool] = None
    short_requires_borrow: Optional[bool] = None
    has_funding: Optional[bool] = None
    expiry_ts: Optional[datetime] = None
    maker_fee_rate: Optional[float] = Field(default=None, ge=0)
    taker_fee_rate: Optional[float] = Field(default=None, ge=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class InstrumentResponse(InstrumentPayload):
    """Response payload enriched with identifiers and timestamps."""

    id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    research_ready: Optional[bool] = None
    runtime_ready: Optional[bool] = None
    runtime_message: Optional[str] = None
    runtime_policy: Optional[str] = None
    runtime_policy_version: Optional[str] = None
    execution_semantics: Optional[str] = None


class InstrumentResolveRequest(BaseModel):
    symbol: str
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    force_refresh: bool = False


class InstrumentCoverageMatrixRequest(BaseModel):
    start: str
    end: str
    timeframe: str
    instrument_ids: List[str] = Field(default_factory=list)
    symbol: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    instrument_type: Optional[str] = None
    runtime_ready: Optional[bool] = None
    research_ready: Optional[bool] = None
    execution_semantics: Optional[str] = None


def _normalize_time(value: str) -> str:
    if value is None:
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return value
    if not numeric:
        return value
    if numeric > 2e10:
        numeric /= 1000
    return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _matches_text(value: Any, expected: Optional[str], *, uppercase: bool = False, lowercase: bool = False) -> bool:
    if expected is None:
        return True
    left = str(value or "").strip()
    right = str(expected or "").strip()
    if uppercase:
        left = left.upper()
        right = right.upper()
    if lowercase:
        left = left.lower()
        right = right.lower()
    return left == right


@router.get("/", response_model=List[InstrumentResponse])
def list_instruments() -> List[Dict[str, Any]]:
    """Return all stored instruments."""

    return [instrument_service.instrument_api_payload(record) for record in instrument_service.list_instruments()]


@router.get("/health")
def instrument_health(datasource: Optional[str] = None, exchange: Optional[str] = None) -> Dict[str, Any]:
    """Return spot instrument metadata health report."""

    return instrument_service.instrument_health_report(datasource=datasource, exchange=exchange)


@router.post("/coverage-matrix")
def coverage_matrix(request: InstrumentCoverageMatrixRequest) -> Dict[str, Any]:
    """Return candle coverage for a filtered set of canonical instruments."""

    start = _normalize_time(request.start)
    end = _normalize_time(request.end)
    requested_ids = {str(item).strip() for item in (request.instrument_ids or []) if str(item).strip()}
    rows: List[Dict[str, Any]] = []
    severity_counts: Dict[str, int] = {}

    for record in instrument_service.list_instruments():
        payload = instrument_service.instrument_api_payload(record)
        instrument_id = str(payload.get("id") or "").strip()
        if requested_ids and instrument_id not in requested_ids:
            continue
        if not _matches_text(payload.get("symbol"), request.symbol, uppercase=True):
            continue
        if not _matches_text(payload.get("datasource"), request.datasource, uppercase=True):
            continue
        if not _matches_text(payload.get("exchange"), request.exchange, lowercase=True):
            continue
        if not _matches_text(payload.get("instrument_type"), request.instrument_type, lowercase=True):
            continue
        if request.runtime_ready is not None and bool(payload.get("runtime_ready")) != bool(request.runtime_ready):
            continue
        if request.research_ready is not None and bool(payload.get("research_ready")) != bool(request.research_ready):
            continue
        if request.execution_semantics is not None and str(payload.get("execution_semantics") or "").strip() != str(request.execution_semantics).strip():
            continue

        coverage = preflight_candle_coverage_by_instrument(
            instrument_id,
            start,
            end,
            request.timeframe,
        )
        severity = str(coverage.get("severity") or coverage.get("status") or "unknown")
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        rows.append(
            {
                "instrument": {
                    "id": instrument_id,
                    "symbol": payload.get("symbol"),
                    "datasource": payload.get("datasource"),
                    "exchange": payload.get("exchange"),
                    "instrument_type": payload.get("instrument_type"),
                    "research_ready": payload.get("research_ready"),
                    "runtime_ready": payload.get("runtime_ready"),
                    "runtime_policy": payload.get("runtime_policy"),
                    "execution_semantics": payload.get("execution_semantics"),
                },
                "coverage": coverage,
            }
        )

    return {
        "schema_version": "instrument_coverage_matrix.v1",
        "requested_window": {
            "start": start,
            "end": end,
            "timeframe": request.timeframe,
        },
        "filters": {
            "instrument_ids": sorted(requested_ids),
            "symbol": request.symbol,
            "datasource": request.datasource,
            "exchange": request.exchange,
            "instrument_type": request.instrument_type,
            "runtime_ready": request.runtime_ready,
            "research_ready": request.research_ready,
            "execution_semantics": request.execution_semantics,
        },
        "items": rows,
        "summary": {
            "instrument_count": len(rows),
            "severity_counts": severity_counts,
        },
    }


@router.post("/resolve", response_model=InstrumentResponse)
def resolve_instrument(request: InstrumentResolveRequest) -> Dict[str, Any]:
    """Validate provider/venue/symbol and return a canonical instrument record."""

    record, error = instrument_service.resolve_or_create_instrument(
        request.datasource,
        request.exchange,
        request.symbol,
        provider_id=request.provider_id,
        venue_id=request.venue_id,
        force_refresh=request.force_refresh,
    )
    if error:
        raise HTTPException(400, error)
    if not record:
        raise HTTPException(404, "Instrument could not be resolved.")
    return instrument_service.instrument_api_payload(record)


@router.post("/", response_model=InstrumentResponse, status_code=201)
def create_instrument(payload: InstrumentPayload) -> Dict[str, Any]:
    """Create a new instrument definition."""

    try:
        return instrument_service.instrument_api_payload(instrument_service.create_instrument(**payload.dict()))
    except ValueError as exc:  # pragma: no cover - FastAPI plumbing
        raise HTTPException(400, str(exc)) from exc


@router.get("/{instrument_id}/runtime-profile")
def get_instrument_runtime_profile(
    instrument_id: str,
    execution_semantics: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Return the compiled runtime execution profile for an instrument."""

    try:
        record = instrument_service.get_instrument_record(instrument_id)
        return instrument_service.instrument_runtime_profile(
            record,
            execution_semantics=execution_semantics,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/{instrument_id}", response_model=InstrumentResponse)
def get_instrument(instrument_id: str) -> Dict[str, Any]:
    """Return a single instrument."""

    try:
        return instrument_service.instrument_api_payload(instrument_service.get_instrument_record(instrument_id))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/{instrument_id}", response_model=InstrumentResponse)
def update_instrument(instrument_id: str, payload: InstrumentPayload) -> Dict[str, Any]:
    """Update an existing instrument."""

    try:
        return instrument_service.instrument_api_payload(
            instrument_service.update_instrument(instrument_id, **payload.dict())
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{instrument_id}", status_code=204, response_class=Response)
def delete_instrument(instrument_id: str) -> Response:
    """Delete an instrument record."""

    instrument_service.delete_instrument_record(instrument_id)

    return Response(status_code=204)
