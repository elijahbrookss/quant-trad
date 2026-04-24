"""FastAPI router exposing bot CRUD and runtime controls."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import Mapping as AbcMapping
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from fastapi import APIRouter, HTTPException, Query, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from starlette.websockets import WebSocketState

from ..service.bots import bot_service
from ..service.bots.bot_run_diagnostics_projection import project_bot_run_diagnostics
from ..service.bots.botlens_bootstrap_service import get_active_botlens_run_bootstrap, resolve_active_botlens_stream
from ..service.bots.botlens_chart_service import get_symbol_chart_history
from ..service.bots.botlens_forensics_service import get_run_signal_forensics, list_run_forensic_events
from ..service.bots.botlens_symbol_service import get_selected_symbol_snapshot, get_symbol_detail, list_run_symbols
from ..service.bots.telemetry_stream import telemetry_hub
from ..service.observability import BackendObserver
from ..service.storage.repos.lifecycle import get_bot_run_lifecycle, list_bot_run_lifecycle_events
from ..service.storage.repos.runtime_events import get_latest_bot_runtime_event
router = APIRouter()
logger = logging.getLogger(__name__)
_INGEST_OBSERVER = BackendObserver(component="botlens_ingest_ws", event_logger=logger)


def _sanitize_json(value: Any) -> Any:
    """Recursively make the payload JSON-friendly."""

    if isinstance(value, AbcMapping):
        return {key: _sanitize_json(payload_value) for key, payload_value in value.items()}
    if isinstance(value, datetime):
        try:
            return value.replace(tzinfo=None).isoformat() + "Z"
        except Exception:
            return str(value)
    if isinstance(value, list):
        return [_sanitize_json(entry) for entry in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_json(entry) for entry in value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _format_sse(event: str, payload: Mapping[str, Any]) -> str:
    """Return a formatted server-sent event chunk."""

    body = json.dumps(_sanitize_json(payload))
    return f"event: {event}\ndata: {body}\n\n"


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _websocket_is_connected(websocket: WebSocket) -> bool:
    return (
        websocket.application_state == WebSocketState.CONNECTED
        and websocket.client_state == WebSocketState.CONNECTED
    )


def _is_expected_websocket_runtime_error(exc: RuntimeError) -> bool:
    return "websocket is not connected" in str(exc).strip().lower()


class BotBase(BaseModel):
    """Shared bot attributes."""

    name: str
    strategy_id: str
    strategy_variant_id: Optional[str] = None
    strategy_variant_name: Optional[str] = None
    resolved_params: Dict[str, Any] = Field(default_factory=dict)
    risk_config: Dict[str, Any] = Field(default_factory=dict)
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    mode: str = Field(default="instant", pattern="^(instant|walk-forward)$")
    run_type: str = Field(default="backtest", pattern="^(backtest|sim_trade|paper|live)$")
    playback_speed: float = Field(default=0.0, ge=0)
    backtest_start: Optional[str] = None
    backtest_end: Optional[str] = None
    wallet_config: Dict[str, Any] = Field(default_factory=dict)
    snapshot_interval_ms: int = Field(..., gt=0)
    bot_env: Dict[str, str] = Field(default_factory=dict)
    instrument_type: Optional[str] = None


class BotCreateRequest(BotBase):
    """Payload for creating a bot."""


class BotUpdateRequest(BaseModel):
    """Patch payload for updating a bot."""

    name: Optional[str] = None
    strategy_id: Optional[str] = None
    strategy_variant_id: Optional[str] = None
    strategy_variant_name: Optional[str] = None
    resolved_params: Optional[Dict[str, Any]] = None
    risk_config: Optional[Dict[str, Any]] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    run_type: Optional[str] = Field(default=None, pattern="^(backtest|sim_trade|paper|live)$")
    mode: Optional[str] = Field(default=None, pattern="^(instant|walk-forward)$")
    playback_speed: Optional[float] = Field(default=None, ge=0)
    focus_symbol: Optional[str] = None
    wallet_config: Optional[Dict[str, Any]] = None
    snapshot_interval_ms: Optional[int] = Field(default=None, gt=0)
    bot_env: Optional[Dict[str, str]] = None
    instrument_type: Optional[str] = None


class BotResponse(BotBase):
    """Response payload describing a bot."""

    id: str
    atm_template_id: Optional[str] = None
    status: str
    last_run_at: Optional[str] = None
    last_stats: Dict[str, Any] = Field(default_factory=dict)
    last_run_artifact: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    runtime: Optional[Dict[str, Any]] = None
    lifecycle: Optional[Dict[str, Any]] = None
    controls: Optional[Dict[str, Any]] = None
    active_run_id: Optional[str] = None
    run: Optional[Dict[str, Any]] = None


@router.get("", response_model=List[BotResponse], include_in_schema=False)
@router.get("/", response_model=List[BotResponse])
async def list_bots() -> List[Dict[str, Any]]:
    return bot_service.list_bots()


@router.post("", response_model=BotResponse, status_code=201, include_in_schema=False)
@router.post("/", response_model=BotResponse, status_code=201)
async def create_bot(body: BotCreateRequest) -> Dict[str, Any]:
    try:
        return bot_service.create_bot(**body.dict())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/settings-catalog")
async def bot_settings_catalog() -> Dict[str, Any]:
    return bot_service.bot_settings_catalog()


@router.get("/watchdog")
async def bot_watchdog_status() -> Dict[str, Any]:
    return bot_service.watchdog_status()


@router.get("/runtime-capacity")
async def bot_runtime_capacity() -> Dict[str, Any]:
    return bot_service.runtime_capacity()


@router.get("/stream")
async def stream_bots() -> StreamingResponse:
    release, channel, initial = bot_service.bots_stream()

    async def event_iterator():
        try:
            yield _format_sse(initial.get("type", "snapshot"), initial)
            while True:
                try:
                    payload = await asyncio.to_thread(channel.get)
                except asyncio.CancelledError:
                    break
                if not payload:
                    continue
                event_type = payload.get("type", "update")
                yield _format_sse(event_type, payload)
        finally:
            release()

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_iterator(), media_type="text/event-stream", headers=headers)


@router.get("/{bot_id}", response_model=BotResponse)
async def get_bot(bot_id: str) -> Dict[str, Any]:
    try:
        return bot_service.get_bot(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/{bot_id}", response_model=BotResponse)
async def update_bot(bot_id: str, body: BotUpdateRequest) -> Dict[str, Any]:
    try:
        payload = body.dict(exclude_unset=True)
        return bot_service.update_bot(bot_id, **payload)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{bot_id}", status_code=204, response_class=Response)
async def delete_bot(bot_id: str) -> Response:
    bot_service.delete_bot_record(bot_id)
    return Response(status_code=204)


@router.post("/{bot_id}/start", response_model=BotResponse)
async def start_bot(bot_id: str) -> Dict[str, Any]:
    try:
        return bot_service.start_bot(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/{bot_id}/stop", response_model=BotResponse)
async def stop_bot(bot_id: str, preserve_container: bool = False) -> Dict[str, Any]:
    try:
        return bot_service.stop_bot(bot_id, preserve_container=preserve_container)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/{bot_id}/active-run")
def bot_active_run(bot_id: str) -> Dict[str, Any]:
    try:
        bot = bot_service.get_bot(str(bot_id))
        run_id = bot.get("active_run_id")
        return {"bot_id": str(bot_id), "run_id": run_id}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{bot_id}/runs")
def bot_runs(
    bot_id: str,
    limit: int = 25,
) -> Dict[str, Any]:
    try:
        return bot_service.list_bot_runs_for_bot(
            str(bot_id),
            limit=max(1, min(int(limit or 25), 100)),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{bot_id}/runs/{run_id}/lifecycle-events")
def bot_run_lifecycle_events(bot_id: str, run_id: str) -> Dict[str, Any]:
    normalized_run_id = str(run_id)
    lifecycle = get_bot_run_lifecycle(normalized_run_id)
    events = list_bot_run_lifecycle_events(normalized_run_id)
    run_snapshot = telemetry_hub.get_run_snapshot(run_id=normalized_run_id)
    run_health = run_snapshot.health.to_dict() if run_snapshot is not None else None
    latest_runtime_event = get_latest_bot_runtime_event(bot_id=str(bot_id), run_id=normalized_run_id)
    diagnostics = project_bot_run_diagnostics(
        run_id=normalized_run_id,
        lifecycle=lifecycle,
        events=events,
        run_health=run_health,
    )
    latest_event = events[-1] if events else {}
    consistency = {
        "read_completed_at": _utc_now_iso(),
        "lifecycle_checkpoint_at": (lifecycle or {}).get("checkpoint_at"),
        "lifecycle_event_seq": latest_event.get("seq"),
        "runtime_available": run_snapshot is not None,
        "runtime_reason": None if run_snapshot is not None else "snapshot_unavailable",
        "runtime_seq": int(run_snapshot.seq or 0) if run_snapshot is not None else None,
        "runtime_known_at": (run_health or {}).get("last_event_at"),
        "runtime_event_time": (latest_runtime_event or {}).get("event_time"),
    }
    return {
        "bot_id": str(bot_id),
        "run_id": normalized_run_id,
        "run_status": diagnostics.get("run_status"),
        "summary": diagnostics.get("summary"),
        "runtime": diagnostics.get("runtime"),
        "consistency": consistency,
        "checkpoints": diagnostics.get("checkpoints"),
        "events": diagnostics.get("events"),
    }


@router.get("/{bot_id}/runs/{run_id}/forensics/signals/{signal_id}")
async def bot_run_signal_forensics(
    bot_id: str,
    run_id: str,
    signal_id: str,
) -> Dict[str, Any]:
    try:
        return get_run_signal_forensics(
            bot_id=str(bot_id),
            run_id=str(run_id),
            signal_id=str(signal_id),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/{bot_id}/runs/{run_id}/forensics/events")
async def bot_run_forensic_events(
    bot_id: str,
    run_id: str,
    after_seq: int = 0,
    after_row_id: int = 0,
    limit: int = 200,
    event_name: Optional[List[str]] = Query(default=None),
    series_key: Optional[str] = None,
    root_event_id: Optional[str] = None,
    parent_event_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        return list_run_forensic_events(
            bot_id=str(bot_id),
            run_id=str(run_id),
            after_seq=max(0, int(after_seq or 0)),
            after_row_id=max(0, int(after_row_id or 0)),
            limit=max(1, int(limit or 200)),
            event_names=event_name or None,
            series_key=series_key,
            root_event_id=root_event_id,
            parent_event_id=parent_event_id,
            correlation_id=correlation_id,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc






@router.get("/runs/{run_id}/series")
async def bot_lens_symbol_catalog(run_id: str) -> Dict[str, Any]:
    try:
        return await list_run_symbols(run_id=str(run_id))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/{bot_id}/botlens/bootstrap/run")
async def bot_lens_run_bootstrap(bot_id: str) -> Dict[str, Any]:
    try:
        return await get_active_botlens_run_bootstrap(bot_id=str(bot_id))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/series/{series_key}/snapshot")
async def bot_lens_selected_symbol_snapshot(
    run_id: str,
    series_key: str,
    limit: int = 320,
) -> Dict[str, Any]:
    try:
        return await get_selected_symbol_snapshot(
            run_id=str(run_id),
            symbol_key=str(series_key),
            limit=max(1, min(int(limit or 320), 2000)),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/series/{series_key}/bootstrap")
async def bot_lens_selected_symbol_bootstrap(
    run_id: str,
    series_key: str,
    limit: int = 320,
) -> Dict[str, Any]:
    try:
        return await get_selected_symbol_snapshot(
            run_id=str(run_id),
            symbol_key=str(series_key),
            limit=max(1, min(int(limit or 320), 2000)),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/series/{series_key}/detail")
async def bot_lens_symbol_detail(
    run_id: str,
    series_key: str,
    limit: int = 320,
) -> Dict[str, Any]:
    try:
        return await get_symbol_detail(
            run_id=str(run_id),
            symbol_key=str(series_key),
            limit=max(1, min(int(limit or 320), 2000)),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/series/{series_key}/chart")
async def bot_lens_series_chart_history(
    run_id: str,
    series_key: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 320,
) -> Dict[str, Any]:
    try:
        return get_symbol_chart_history(
            run_id=str(run_id),
            symbol_key=str(series_key),
            start_time=start_time,
            end_time=end_time,
            limit=max(1, min(int(limit or 320), 2000)),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

@router.websocket("/ws/telemetry/ingest")
async def bot_telemetry_ingest(websocket: WebSocket) -> None:
    await websocket.accept()
    while True:
        decode_started = time.perf_counter()
        try:
            payload = await websocket.receive_json()
        except WebSocketDisconnect:
            break
        except Exception as exc:
            _INGEST_OBSERVER.increment(
                "ingest_messages_invalid_total",
                failure_mode="decode_error",
                message_kind="ingest_ws",
            )
            _INGEST_OBSERVER.event(
                "intake_invalid_envelope",
                level=logging.WARN,
                failure_mode="decode_error",
                message_kind="ingest_ws",
                error=str(exc),
            )
            continue
        _INGEST_OBSERVER.observe(
            "ingest_decode_ms",
            max((time.perf_counter() - decode_started) * 1000.0, 0.0),
            message_kind=str(payload.get("kind") or "unknown") if isinstance(payload, dict) else "unknown",
            run_id=str(payload.get("run_id") or "").strip() or None if isinstance(payload, dict) else None,
            bot_id=str(payload.get("bot_id") or "").strip() or None if isinstance(payload, dict) else None,
            worker_id=str(payload.get("worker_id") or "").strip() or None if isinstance(payload, dict) else None,
        )
        await telemetry_hub.ingest(payload)




@router.websocket("/ws/{bot_id}/botlens/live")
async def bot_lens_active_live(
    bot_id: str,
    websocket: WebSocket,
) -> None:
    try:
        resolved = resolve_active_botlens_stream(bot_id=str(bot_id))
    except (KeyError, ValueError) as exc:
        await websocket.accept()
        logger.warning("botlens_live_ws_open_failed | bot_id=%s | error=%s", bot_id, str(exc))
        await websocket.close(code=1011)
        return

    run_id = str(resolved.get("run_id") or "")
    selected_symbol_key = str(websocket.query_params.get("selected_symbol_key") or "").strip() or None
    requested_stream_session_id = str(websocket.query_params.get("stream_session_id") or "").strip() or None
    try:
        resume_from_seq = max(int(websocket.query_params.get("resume_from_seq") or 0), 0)
    except (TypeError, ValueError):
        resume_from_seq = 0
    await telemetry_hub.add_run_viewer(
        run_id=run_id,
        ws=websocket,
        selected_symbol_key=selected_symbol_key,
        resume_from_seq=resume_from_seq,
        stream_session_id=requested_stream_session_id,
    )
    if not _websocket_is_connected(websocket):
        return
    try:
        while True:
            if not _websocket_is_connected(websocket):
                break
            payload = await websocket.receive_json()
            if isinstance(payload, dict):
                await telemetry_hub.update_run_viewer(run_id=run_id, ws=websocket, payload=payload)
    except WebSocketDisconnect:
        pass
    except RuntimeError as exc:
        if not _is_expected_websocket_runtime_error(exc):
            raise
    finally:
        await telemetry_hub.remove_run_viewer(run_id=run_id, ws=websocket)
