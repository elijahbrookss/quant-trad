# routers/indicators.py
import logging
from time import perf_counter
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from portal.backend.service.indicators.async_dispatch import (
    AsyncJobFailedError,
    AsyncJobNotFoundError,
    AsyncJobTimeoutError,
    JOB_TYPE_OVERLAYS,
    JOB_TYPE_SIGNALS,
    enqueue_overlay_job,
    enqueue_signal_job,
    quantlab_partition_key,
    quantlab_request_fingerprint,
    reuse_quantlab_job,
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


def _raise_indicator_http_error(
    *,
    event: str,
    status_code: int,
    detail: str,
    inst_id: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    interval: Optional[str] = None,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    instrument_id: Optional[str] = None,
    duration_ms: Optional[float] = None,
) -> None:
    log = logger.error if int(status_code) >= 500 else logger.warning
    log(
        "event=%s indicator_id=%s status_code=%s duration_ms=%s detail=%s start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s instrument_id=%s",
        event,
        inst_id,
        status_code,
        f"{duration_ms:.3f}" if duration_ms is not None else None,
        detail,
        start,
        end,
        interval,
        symbol,
        datasource,
        exchange,
        instrument_id,
    )
    raise HTTPException(status_code, detail)


def _raise_failed_job(
    error: str,
    *,
    event: str,
    inst_id: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    interval: Optional[str] = None,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    instrument_id: Optional[str] = None,
) -> None:
    message = str(error or "async job failed")
    lower = message.lower()
    if "keyerror" in lower or "not found" in lower:
        _raise_indicator_http_error(
            event=event,
            status_code=404,
            detail=message,
            inst_id=inst_id,
            start=start,
            end=end,
            interval=interval,
            symbol=symbol,
            datasource=datasource,
            exchange=exchange,
            instrument_id=instrument_id,
        )
    if "lookuperror" in lower or "no overlays computed" in lower or "no candles" in lower:
        _raise_indicator_http_error(
            event=event,
            status_code=404,
            detail=message,
            inst_id=inst_id,
            start=start,
            end=end,
            interval=interval,
            symbol=symbol,
            datasource=datasource,
            exchange=exchange,
            instrument_id=instrument_id,
        )
    if "valueerror" in lower or "invalid" in lower:
        _raise_indicator_http_error(
            event=event,
            status_code=400,
            detail=message,
            inst_id=inst_id,
            start=start,
            end=end,
            interval=interval,
            symbol=symbol,
            datasource=datasource,
            exchange=exchange,
            instrument_id=instrument_id,
        )
    _raise_indicator_http_error(
        event=event,
        status_code=500,
        detail=message,
        inst_id=inst_id,
        start=start,
        end=end,
        interval=interval,
        symbol=symbol,
        datasource=datasource,
        exchange=exchange,
        instrument_id=instrument_id,
    )

# ===== Schemas =====
class IndicatorInstanceIn(BaseModel):
    type: str
    name: Optional[str] = None
    params: Dict[str, Any]
    dependencies: List[Dict[str, Any]] = Field(default_factory=list)
    output_prefs: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    color: Optional[str] = None

class IndicatorInstanceOut(BaseModel):
    id: str
    type: str
    name: str
    params: Dict[str, Any]
    dependencies: List[Dict[str, Any]] = Field(default_factory=list)
    enabled: bool
    color: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    output_prefs: Optional[Dict[str, Dict[str, Any]]] = None
    typed_outputs: Optional[List[Dict[str, Any]]] = None
    overlay_outputs: Optional[List[Dict[str, Any]]] = None
    runtime_supported: Optional[bool] = None

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
    instrument_id: str
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

# ===== Types =====
@router.get("-types", response_model=List[str])
async def list_indicator_types():
    return list_types()


@router.get("/types", response_model=List[str])
async def list_indicator_types_alias():
    return list_types()


@router.get("-types/{type_id}")
async def get_indicator_type(type_id: str):
    try:
        return get_type_details(type_id)
    except KeyError as e:
        raise HTTPException(404, str(e))

# ===== Instances =====
@router.get("/", response_model=List[IndicatorInstanceOut])
async def list_instances():
    return list_instances_meta()

@router.post("/", response_model=IndicatorInstanceOut, status_code=201)
async def create(body: IndicatorInstanceIn):
    try:
        return create_instance(
            body.type,
            body.name,
            dict(body.params),
            dependencies=list(body.dependencies or []),
            color=body.color,
            output_prefs=dict(body.output_prefs or {}),
        )
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
            dependencies=list(body.dependencies or []),
            output_prefs=dict(body.output_prefs or {}),
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
    except RuntimeError as e:
        raise HTTPException(409, str(e))

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
    try:
        removed = bulk_delete_instances(body.ids or [])
        return {"deleted": removed}
    except KeyError:
        raise HTTPException(404, "Indicator not found")
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.get("/types/{type_id}")
async def get_indicator_type_alias(type_id: str):
    try:
        return get_type_details(type_id)
    except KeyError as e:
        raise HTTPException(404, str(e))

# ===== Overlays by UUID =====
@router.post("/{inst_id}/overlays")
async def overlays(inst_id: str, req: OverlayRequest):
    t0 = perf_counter()
    logger.info(
        "event=indicator_overlay_request_started indicator_id=%s start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s instrument_id=%s visibility_epoch=%s",
        inst_id,
        req.start,
        req.end,
        req.interval,
        req.symbol,
        req.datasource,
        req.exchange,
        req.instrument_id,
        req.visibility_epoch,
    )
    try:
        meta = get_instance_meta(inst_id)
        request_fingerprint = quantlab_request_fingerprint(
            job_type=JOB_TYPE_OVERLAYS,
            indicator_id=inst_id,
            indicator_updated_at=str(meta.get("updated_at") or ""),
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            visibility_epoch=req.visibility_epoch,
        )
        request_payload = {
            "inst_id": inst_id,
            "start": req.start,
            "end": req.end,
            "interval": req.interval,
            "symbol": req.symbol,
            "datasource": req.datasource,
            "exchange": req.exchange,
            "instrument_id": req.instrument_id,
            "visibility_epoch": req.visibility_epoch,
        }
        partition_key = quantlab_partition_key(request_payload)
        reusable = reuse_quantlab_job(
            job_type=JOB_TYPE_OVERLAYS,
            partition_key=partition_key,
            request_fingerprint=request_fingerprint,
        )
        if reusable and reusable.get("status") == "succeeded":
            payload = reusable.get("result")
            logger.info(
                "event=indicator_overlay_request_finished indicator_id=%s status_code=200 duration_ms=%.3f overlays=%s runtime_path=%s cache_hit=true start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s instrument_id=%s",
                inst_id,
                (perf_counter() - t0) * 1000.0,
                len(payload.get("overlays")) if isinstance(payload, dict) and isinstance(payload.get("overlays"), list) else None,
                payload.get("runtime_path") if isinstance(payload, dict) else None,
                req.start,
                req.end,
                req.interval,
                req.symbol,
                req.datasource,
                req.exchange,
                req.instrument_id,
            )
            return payload

        if reusable and reusable.get("id") and reusable.get("status") in {"queued", "running", "retry"}:
            job_id = str(reusable["id"])
        else:
            job_id = enqueue_overlay_job(
                inst_id=inst_id,
                start=req.start,
                end=req.end,
                interval=req.interval,
                symbol=req.symbol,
                datasource=req.datasource,
                exchange=req.exchange,
                instrument_id=req.instrument_id,
                visibility_epoch=req.visibility_epoch,
                request_fingerprint=request_fingerprint,
            )
        payload = await wait_for_job(job_id)
        overlays = payload.get("overlays") if isinstance(payload, dict) else None
        logger.info(
            "event=indicator_overlay_request_finished indicator_id=%s status_code=200 duration_ms=%.3f overlays=%s runtime_path=%s cache_hit=false start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s instrument_id=%s",
            inst_id,
            (perf_counter() - t0) * 1000.0,
            len(overlays) if isinstance(overlays, list) else None,
            payload.get("runtime_path") if isinstance(payload, dict) else None,
            req.start,
            req.end,
            req.interval,
            req.symbol,
            req.datasource,
            req.exchange,
            req.instrument_id,
        )
        return payload
    except AsyncJobNotFoundError:
        _raise_indicator_http_error(
            event="indicator_overlay_request_failed",
            status_code=500,
            detail="Overlay job disappeared before completion",
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            duration_ms=(perf_counter() - t0) * 1000.0,
        )
    except AsyncJobTimeoutError as e:
        _raise_indicator_http_error(
            event="indicator_overlay_request_failed",
            status_code=504,
            detail=str(e),
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            duration_ms=(perf_counter() - t0) * 1000.0,
        )
    except AsyncJobFailedError as e:
        _raise_failed_job(
            str(e),
            event="indicator_overlay_request_failed",
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
        )
    except KeyError:
        _raise_indicator_http_error(
            event="indicator_overlay_request_failed",
            status_code=404,
            detail="Indicator not found",
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            duration_ms=(perf_counter() - t0) * 1000.0,
        )
    except LookupError as e:
        _raise_indicator_http_error(
            event="indicator_overlay_request_failed",
            status_code=404,
            detail=str(e),
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            duration_ms=(perf_counter() - t0) * 1000.0,
        )
    except ValueError as e:
        _raise_indicator_http_error(
            event="indicator_overlay_request_failed",
            status_code=400,
            detail=str(e),
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            duration_ms=(perf_counter() - t0) * 1000.0,
        )
    except RuntimeError as e:
        _raise_indicator_http_error(
            event="indicator_overlay_request_failed",
            status_code=500,
            detail=str(e),
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            duration_ms=(perf_counter() - t0) * 1000.0,
        )
    except Exception as e:
        logger.exception(
            "event=indicator_overlay_request_failed indicator_id=%s status_code=500 duration_ms=%.3f detail=%s start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s instrument_id=%s",
            inst_id,
            (perf_counter() - t0) * 1000.0,
            str(e),
            req.start,
            req.end,
            req.interval,
            req.symbol,
            req.datasource,
            req.exchange,
            req.instrument_id,
        )
        raise HTTPException(500, "Unexpected error computing overlays")


@router.post("/{inst_id}/signals")
async def signals(inst_id: str, req: SignalRequest):
    logger.info(
        "event=signals_endpoint_called inst_id=%s req_datasource=%s req_exchange=%s req_symbol=%s req_interval=%s req_instrument_id=%s",
        inst_id,
        req.datasource,
        req.exchange,
        req.symbol,
        req.interval,
        req.instrument_id,
    )
    try:
        meta = get_instance_meta(inst_id)
        request_fingerprint = quantlab_request_fingerprint(
            job_type=JOB_TYPE_SIGNALS,
            indicator_id=inst_id,
            indicator_updated_at=str(meta.get("updated_at") or ""),
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
            config=req.config,
        )
        request_payload = {
            "inst_id": inst_id,
            "start": req.start,
            "end": req.end,
            "interval": req.interval,
            "symbol": req.symbol,
            "datasource": req.datasource,
            "exchange": req.exchange,
            "instrument_id": req.instrument_id,
            "config": dict(req.config or {}),
        }
        partition_key = quantlab_partition_key(request_payload)
        reusable = reuse_quantlab_job(
            job_type=JOB_TYPE_SIGNALS,
            partition_key=partition_key,
            request_fingerprint=request_fingerprint,
        )
        if reusable and reusable.get("status") == "succeeded":
            payload = reusable.get("result")
            if isinstance(payload, dict):
                assert_engine_signal_runtime_path(
                    payload,
                    context="signals_endpoint_runtime_path_mismatch",
                    indicator_id=inst_id,
                )
            logger.info(
                "event=signals_endpoint_complete inst_id=%s cache_hit=true runtime_path=%s signals=%s",
                inst_id,
                payload.get("runtime_path") if isinstance(payload, dict) else None,
                len(payload.get("signals")) if isinstance(payload, dict) and isinstance(payload.get("signals"), list) else None,
            )
            return payload

        if reusable and reusable.get("id") and reusable.get("status") in {"queued", "running", "retry"}:
            job_id = str(reusable["id"])
        else:
            job_id = enqueue_signal_job(
                inst_id=inst_id,
                start=req.start,
                end=req.end,
                interval=req.interval,
                symbol=req.symbol,
                datasource=req.datasource,
                exchange=req.exchange,
                instrument_id=req.instrument_id,
                config=req.config,
                request_fingerprint=request_fingerprint,
            )
        payload = await wait_for_job(job_id)
        if isinstance(payload, dict):
            assert_engine_signal_runtime_path(
                payload,
                context="signals_endpoint_runtime_path_mismatch",
                indicator_id=inst_id,
            )
        logger.info(
            "event=signals_endpoint_complete inst_id=%s cache_hit=false runtime_path=%s signals=%s",
            inst_id,
            payload.get("runtime_path") if isinstance(payload, dict) else None,
            len(payload.get("signals")) if isinstance(payload, dict) and isinstance(payload.get("signals"), list) else None,
        )
        return payload
    except AsyncJobNotFoundError:
        _raise_indicator_http_error(
            event="indicator_signal_request_failed",
            status_code=500,
            detail="Signal job disappeared before completion",
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
        )
    except AsyncJobTimeoutError as e:
        _raise_indicator_http_error(
            event="indicator_signal_request_failed",
            status_code=504,
            detail=str(e),
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
        )
    except AsyncJobFailedError as e:
        _raise_failed_job(
            str(e),
            event="indicator_signal_request_failed",
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
        )
    except KeyError:
        _raise_indicator_http_error(
            event="indicator_signal_request_failed",
            status_code=404,
            detail="Indicator not found",
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
        )
    except LookupError as e:
        _raise_indicator_http_error(
            event="indicator_signal_request_failed",
            status_code=404,
            detail=str(e),
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
        )
    except ValueError as e:
        _raise_indicator_http_error(
            event="indicator_signal_request_failed",
            status_code=400,
            detail=str(e),
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
        )
    except RuntimeError as e:
        _raise_indicator_http_error(
            event="indicator_signal_request_failed",
            status_code=500,
            detail=str(e),
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            datasource=req.datasource,
            exchange=req.exchange,
            instrument_id=req.instrument_id,
        )
    except Exception:
        logger.exception(
            "event=indicator_signal_request_failed indicator_id=%s status_code=500 detail=unexpected_error start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s instrument_id=%s",
            inst_id,
            req.start,
            req.end,
            req.interval,
            req.symbol,
            req.datasource,
            req.exchange,
            req.instrument_id,
        )
        raise HTTPException(500, "Unexpected error generating signals")
