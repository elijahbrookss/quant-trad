"""FastAPI router exposing bot CRUD and runtime controls."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Mapping as AbcMapping
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from fastapi import APIRouter, HTTPException, Query, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..service.bots import bot_service
from ..service.bots.ledger_service import list_run_ledger_events
from ..service.bots.telemetry_stream import telemetry_hub
from ..service.bots.botlens_series_service import get_series_history, get_series_window, list_series_keys
router = APIRouter()


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


class BotBase(BaseModel):
    """Shared bot attributes."""

    name: str
    strategy_id: str
    strategy_variant_id: Optional[str] = None
    strategy_variant_name: Optional[str] = None
    resolved_params: Dict[str, Any] = Field(default_factory=dict)
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
    status: str
    last_run_at: Optional[str] = None
    last_stats: Dict[str, Any] = Field(default_factory=dict)
    last_run_artifact: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    runtime: Optional[Dict[str, Any]] = None


@router.get("/", response_model=List[BotResponse])
async def list_bots() -> List[Dict[str, Any]]:
    return bot_service.list_bots()


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
async def stop_bot(bot_id: str) -> Dict[str, Any]:
    try:
        return bot_service.stop_bot(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/{bot_id}/active-run")
async def bot_active_run(bot_id: str) -> Dict[str, Any]:
    try:
        bot = await asyncio.to_thread(bot_service.get_bot, str(bot_id))
        run_id = bot.get("active_run_id")
        return {"bot_id": str(bot_id), "run_id": run_id}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{bot_id}/runs")
async def bot_runs(
    bot_id: str,
    limit: int = 25,
) -> Dict[str, Any]:
    try:
        return await asyncio.to_thread(
            bot_service.list_bot_runs_for_bot,
            str(bot_id),
            limit=max(1, min(int(limit or 25), 100)),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{bot_id}/runs/{run_id}/events")
async def bot_run_ledger_events(
    bot_id: str,
    run_id: str,
    after_seq: int = 0,
    limit: int = 500,
    event_name: Optional[List[str]] = Query(default=None),
) -> Dict[str, Any]:
    try:
        return list_run_ledger_events(
            bot_id=str(bot_id),
            run_id=str(run_id),
            after_seq=max(0, int(after_seq or 0)),
            limit=max(1, int(limit or 500)),
            event_names=event_name or None,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc






@router.get("/runs/{run_id}/series")
async def bot_lens_series_catalog(run_id: str) -> Dict[str, Any]:
    try:
        return list_series_keys(run_id=str(run_id))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

@router.get("/runs/{run_id}/series/{series_key}/window")
async def bot_lens_series_window(
    run_id: str,
    series_key: str,
    to: Optional[str] = "now",
    limit: int = 320,
) -> Dict[str, Any]:
    try:
        return get_series_window(run_id=str(run_id), series_key=str(series_key), to=to, limit=max(1, min(int(limit or 320), 2000)))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/series/{series_key}/history")
async def bot_lens_series_history(
    run_id: str,
    series_key: str,
    before_ts: Optional[str] = None,
    limit: int = 320,
) -> Dict[str, Any]:
    try:
        return get_series_history(
            run_id=str(run_id),
            series_key=str(series_key),
            before_ts=before_ts,
            limit=max(1, min(int(limit or 320), 2000)),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

@router.websocket("/ws/telemetry/ingest")
async def bot_telemetry_ingest(websocket: WebSocket) -> None:
    await websocket.accept()
    while True:
        try:
            payload = await websocket.receive_json()
        except WebSocketDisconnect:
            break
        await telemetry_hub.ingest(payload)




@router.websocket("/ws/runs/{run_id}/series/{series_key}/live")
async def bot_lens_series_live(
    run_id: str,
    series_key: str,
    websocket: WebSocket,
    limit: int = 320,
) -> None:
    await telemetry_hub.add_series_viewer(
        run_id=str(run_id),
        series_key=str(series_key),
        ws=websocket,
        limit=max(1, min(int(limit or 320), 2000)),
    )
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await telemetry_hub.remove_series_viewer(run_id=str(run_id), series_key=str(series_key), ws=websocket)
