# routers/indicators.py
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from portal.backend.service.indicators.async_dispatch import (
    AsyncJobFailedError,
    AsyncJobNotFoundError,
    AsyncJobTimeoutError,
    enqueue_overlay_job,
    enqueue_signal_job,
    wait_for_job,
)
from portal.backend.service.indicators.indicator_service import (
    bulk_delete_instances,
    bulk_set_enabled,
    create_instance,
    delete_instance,
    duplicate_instance,
    get_instance_meta,
    get_type_details,
    list_indicator_strategies,
    list_instances_meta,
    list_types,
    set_instance_enabled,
    update_instance,
)
from portal.backend.service.indicators.indicator_service.runtime_contract import (
    assert_engine_signal_runtime_path,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _raise_failed_job(error: str) -> None:
    message = str(error or "async job failed")
    lower = message.lower()
    if "keyerror" in lower or "not found" in lower:
        raise HTTPException(404, message)
    if "lookuperror" in lower or "no overlays computed" in lower or "no candles" in lower:
        raise HTTPException(404, message)
    if "valueerror" in lower or "invalid" in lower:
        raise HTTPException(400, message)
    raise HTTPException(500, message)

# ===== Schemas =====
class IndicatorInstanceIn(BaseModel):
    type: str
    name: Optional[str] = None
    params: Dict[str, Any]  # must include symbol/start/end/interval on create
    color: Optional[str] = None

class IndicatorInstanceOut(BaseModel):
    id: str
    type: str
    name: str
    params: Dict[str, Any]
    enabled: bool
    color: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    signal_rules: Optional[List[Dict[str, Any]]] = None

class OverlayRequest(BaseModel):
    start: str
    end: str
    interval: str
    symbol: Optional[str] = None  # optional override; defaults to stored
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    instrument_id: Optional[str] = None
    visibility_epoch: Optional[Any] = None

class SignalRequest(BaseModel):
    start: str
    end: str
    interval: str
    symbol: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)


class IndicatorDuplicateRequest(BaseModel):
    name: Optional[str] = None


class IndicatorToggleRequest(BaseModel):
    enabled: bool


class IndicatorBulkToggleRequest(BaseModel):
    ids: List[str] = Field(default_factory=list)
    enabled: bool


class IndicatorBulkDeleteRequest(BaseModel):
    ids: List[str] = Field(default_factory=list)

# ===== Instances =====
@router.get("/", response_model=List[IndicatorInstanceOut])
async def list_instances():
    return list_instances_meta()

@router.post("/", response_model=IndicatorInstanceOut, status_code=201)
async def create(body: IndicatorInstanceIn):
    try:
        return create_instance(body.type, body.name, dict(body.params), body.color)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))

@router.put("/{inst_id}", response_model=IndicatorInstanceOut)
async def update(inst_id: str, body: IndicatorInstanceIn):
    try:
        color_provided = "color" in body.__fields_set__
        return update_instance(
            inst_id,
            body.type,
            dict(body.params),
            body.name,
            color=body.color,
            color_provided=color_provided,
        )
    except KeyError:
        raise HTTPException(404, "Indicator not found")
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))

@router.get("/{inst_id}", response_model=IndicatorInstanceOut)
async def get_one(inst_id: str):
    try:
        return get_instance_meta(inst_id)
    except KeyError:
        raise HTTPException(404, "Indicator not found")


@router.get("/{inst_id}/strategies")
async def get_indicator_strategies(inst_id: str):
    try:
        get_instance_meta(inst_id)
    except KeyError:
        raise HTTPException(404, "Indicator not found")
    return list_indicator_strategies(inst_id)


@router.delete("/{inst_id}", status_code=204, response_class=Response)
async def delete(inst_id: str) -> Response:
    try:
        delete_instance(inst_id)
    except KeyError:
        raise HTTPException(404, "Indicator not found")

    return Response(status_code=204)


@router.post("/{inst_id}/duplicate", response_model=IndicatorInstanceOut)
async def duplicate(inst_id: str, body: Optional[IndicatorDuplicateRequest] = None):
    try:
        return duplicate_instance(inst_id, name=body.name if body else None)
    except KeyError:
        raise HTTPException(404, "Indicator not found")


@router.patch("/{inst_id}/enabled", response_model=IndicatorInstanceOut)
async def toggle_enabled(inst_id: str, body: IndicatorToggleRequest):
    try:
        return set_instance_enabled(inst_id, body.enabled)
    except KeyError:
        raise HTTPException(404, "Indicator not found")


@router.post("/bulk/toggle", response_model=List[IndicatorInstanceOut])
async def bulk_toggle(body: IndicatorBulkToggleRequest):
    if not body.ids:
        return []
    return bulk_set_enabled(body.ids, body.enabled)


@router.post("/bulk/delete")
async def bulk_delete(body: IndicatorBulkDeleteRequest):
    removed = bulk_delete_instances(body.ids or [])
    return {"deleted": removed}

# ===== Types =====
@router.get("-types", response_model=List[str])
async def list_indicator_types():
    return list_types()

@router.get("-types/{type_id}")
async def get_indicator_type(type_id: str):
    try:
        return get_type_details(type_id)
    except KeyError as e:
        raise HTTPException(404, str(e))

# ===== Overlays by UUID =====
@router.post("/{inst_id}/overlays")
async def overlays(inst_id: str, req: OverlayRequest):
    """
    Returns TradingView Lightweight-Charts overlays for a stored indicator UUID
    over the requested chart window. Does not accept indicator params.
    """
    job_id: Optional[str] = None
    try:
        started = time.perf_counter()
        logger.info(
            "event=overlay_request_received indicator_id=%s instrument_id=%s symbol=%s interval=%s datasource=%s exchange=%s start=%s end=%s",
            inst_id,
            req.instrument_id,
            req.symbol,
            req.interval,
            req.datasource,
            req.exchange,
            req.start,
            req.end,
        )
        job_id = enqueue_overlay_job(
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            overlay_options={"visibility_epoch": req.visibility_epoch}
            if req.visibility_epoch is not None
            else None,
        )
        logger.info("event=overlay_request_enqueued indicator_id=%s job_id=%s", inst_id, job_id)
        payload = await wait_for_job(job_id)
        elapsed_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
        payload_obj = payload.get("payload") if isinstance(payload, dict) else None
        logger.info(
            "event=overlay_request_completed indicator_id=%s job_id=%s duration_ms=%.3f payload_keys=%s",
            inst_id,
            job_id,
            elapsed_ms,
            list(payload_obj.keys()) if isinstance(payload_obj, dict) else [],
        )
        return payload
    except AsyncJobNotFoundError:
        logger.error("event=overlay_request_failed indicator_id=%s job_id=%s reason=job_not_found", inst_id, job_id)
        raise HTTPException(500, "Overlay job disappeared before completion")
    except AsyncJobTimeoutError as e:
        logger.error(
            "event=overlay_request_failed indicator_id=%s job_id=%s reason=job_timeout error=%s",
            inst_id,
            job_id,
            str(e),
        )
        raise HTTPException(504, str(e))
    except AsyncJobFailedError as e:
        logger.error(
            "event=overlay_request_failed indicator_id=%s job_id=%s reason=job_failed error=%s",
            inst_id,
            job_id,
            str(e),
        )
        _raise_failed_job(str(e))
    except KeyError:
        logger.error("event=overlay_request_failed indicator_id=%s job_id=%s reason=indicator_not_found", inst_id, job_id)
        raise HTTPException(404, "Indicator not found")
    except LookupError as e:
        # no candles or no overlays
        logger.warning(
            "event=overlay_request_failed indicator_id=%s job_id=%s reason=lookup_error error=%s",
            inst_id,
            job_id,
            str(e),
        )
        raise HTTPException(404, str(e))
    except ValueError as e:
        logger.warning(
            "event=overlay_request_failed indicator_id=%s job_id=%s reason=bad_request error=%s",
            inst_id,
            job_id,
            str(e),
        )
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        logger.error(
            "event=overlay_request_failed indicator_id=%s job_id=%s reason=runtime_error error=%s",
            inst_id,
            job_id,
            str(e),
        )
        raise HTTPException(500, str(e))
    except Exception as e:
        logger.exception("Unexpected overlay error")
        logger.error(
            "event=overlay_request_failed indicator_id=%s job_id=%s reason=unexpected error=%s",
            inst_id,
            job_id,
            str(e),
        )
        raise HTTPException(500, "Unexpected error computing overlays")


@router.post("/{inst_id}/signals")
async def signals(inst_id: str, req: SignalRequest):
    logger.info(
        "event=signals_endpoint_called inst_id=%s req_datasource=%s req_exchange=%s req_symbol=%s req_interval=%s",
        inst_id,
        req.datasource,
        req.exchange,
        req.symbol,
        req.interval,
    )
    try:
        job_id = enqueue_signal_job(
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            config=req.config,
        )
        payload = await wait_for_job(job_id)
        if isinstance(payload, dict):
            assert_engine_signal_runtime_path(
                payload,
                context="signals_endpoint_runtime_path_mismatch",
                indicator_id=inst_id,
            )
        return payload
    except AsyncJobNotFoundError:
        raise HTTPException(500, "Signal job disappeared before completion")
    except AsyncJobTimeoutError as e:
        raise HTTPException(504, str(e))
    except AsyncJobFailedError as e:
        _raise_failed_job(str(e))
    except KeyError:
        raise HTTPException(404, "Indicator not found")
    except LookupError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))
    except Exception:
        logger.exception("Unexpected signal generation error")
        raise HTTPException(500, "Unexpected error generating signals")
