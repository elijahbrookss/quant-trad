from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import queue
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Sequence

from core.settings import get_settings
from engines.bot_runtime.runtime.runtime import BotRuntime
from engines.bot_runtime.core.runtime_events import (
    RuntimeEventName,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
)
from sqlalchemy import and_, func, or_, select

from portal.backend.db.models import BotRunEventRecord
from portal.backend.db.session import db
from portal.backend.service.bots.container_runtime_projection import (
    coerce_float,
    json_safe,
    utc_now_iso,
)
from portal.backend.service.bots.container_runtime_telemetry import TelemetryEmitter
from portal.backend.service.bots.container_runtime_telemetry import emit_telemetry_ephemeral_message
from portal.backend.service.bots.botlens_contract import normalize_series_key
from portal.backend.service.bots.botlens_runtime_state import (
    BotLensRuntimeState,
    InvalidRuntimeStateTransition,
    guard_runtime_state_transition,
    runtime_state_value,
    startup_bootstrap_admission,
)
from portal.backend.service.bots.runtime_dependencies import build_bot_runtime_deps
from portal.backend.service.reports.artifacts import finalize_run_artifact_bundle_from_workers
from portal.backend.service.bots.startup_lifecycle import (
    BotLifecyclePhase,
    BotLifecycleStatus,
    LifecycleOwner,
    TERMINAL_PHASES,
    build_failure_payload,
    build_series_progress_metadata,
    lifecycle_checkpoint_payload,
    status_for_phase,
    terminal_status_after_supervision,
)
from portal.backend.service.observability_exporter import (
    start_observability_exporter,
    stop_observability_exporter,
)
from portal.backend.service.observability import BackendObserver
from portal.backend.service.bots.startup_validation import validate_wallet_config
from portal.backend.service.bots.strategy_loader import StrategyLoader
from portal.backend.service.storage.storage import (
    load_bots,
    record_bot_run_lifecycle_checkpoint,
    update_bot_runtime_status,
)

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="container_runtime", event_logger=logger)
_TERMINAL_STATUSES = {"completed", "stopped", "error", "failed", "startup_failed", "crashed"}
_MAX_SYMBOL_WORKERS = 8
_VIEW_STATE_SCHEMA_VERSION = 1
_CHURN_NO_PROGRESS_MS = 15_000.0
_CHURN_ACTIVITY_THRESHOLD = 3
_SETTINGS = get_settings()
_LOGGING_SETTINGS = _SETTINGS.logging
_BOT_RUNTIME_SETTINGS = _SETTINGS.bot_runtime
_TELEMETRY_SETTINGS = _BOT_RUNTIME_SETTINGS.telemetry
_BOTLENS_SETTINGS = _BOT_RUNTIME_SETTINGS.botlens
_PUSH_SETTINGS = _BOT_RUNTIME_SETTINGS.push
_MAX_SYMBOLS_PER_STRATEGY = _BOT_RUNTIME_SETTINGS.max_symbols_per_strategy
_INFO_LIFECYCLE_PHASES = {
    BotLifecyclePhase.STARTUP_FAILED.value,
    BotLifecyclePhase.CRASHED.value,
    BotLifecyclePhase.LIVE.value,
    BotLifecyclePhase.CONTAINER_LAUNCHED.value,
    BotLifecyclePhase.AWAITING_CONTAINER_BOOT.value,
    BotLifecyclePhase.WAITING_FOR_SERIES_BOOTSTRAP.value,
}
_INFO_LIFECYCLE_STATUSES = {
    BotLifecycleStatus.STARTING.value,
    BotLifecycleStatus.RUNNING.value,
    BotLifecycleStatus.DEGRADED.value,
    BotLifecycleStatus.TELEMETRY_DEGRADED.value,
    BotLifecycleStatus.STARTUP_FAILED.value,
    BotLifecycleStatus.CRASHED.value,
    BotLifecycleStatus.STOPPED.value,
    BotLifecycleStatus.COMPLETED.value,
}


def _configure_logging() -> None:
    logging.basicConfig(level=_LOGGING_SETTINGS.level)


_TELEMETRY_EMIT_QUEUE_MAX = _TELEMETRY_SETTINGS.emit_queue_max
_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS = _TELEMETRY_SETTINGS.emit_queue_timeout_ms
_TELEMETRY_EMIT_RETRY_MS = _TELEMETRY_SETTINGS.emit_retry_ms
_CANONICAL_FIRST_LIVE_EVENT_NAMES = ("CANDLE_OBSERVED",)
_CANONICAL_FIRST_LIVE_RECONCILE_INTERVAL_S = 0.5


def _materialize_bot_config(bot_payload: Mapping[str, Any]) -> Dict[str, Any]:
    materialized = dict(bot_payload or {})
    snapshot_interval = materialized.get("snapshot_interval_ms")
    if "SNAPSHOT_INTERVAL_MS" not in materialized and isinstance(snapshot_interval, int) and snapshot_interval > 0:
        materialized["SNAPSHOT_INTERVAL_MS"] = snapshot_interval
    bot_env = bot_payload.get("bot_env") if isinstance(bot_payload, Mapping) else None
    if isinstance(bot_env, Mapping):
        for raw_key, raw_value in bot_env.items():
            key = str(raw_key or "").strip()
            if not key:
                continue
            materialized[key] = raw_value
    return materialized


def _normalise_balances(raw_balances: Mapping[str, Any]) -> Dict[str, float]:
    balances: Dict[str, float] = {}
    for currency, amount in (raw_balances or {}).items():
        code = str(currency or "").strip().upper()
        if not code:
            continue
        balances[code] = coerce_float(amount, 0.0)
    return balances


def _build_shared_wallet_proxy(
    manager: mp.Manager,
    *,
    run_id: str,
    bot_id: str,
    balances: Mapping[str, float],
    initial_seq: int = 0,
) -> Dict[str, Any]:
    runtime_events = manager.list()
    init_event = new_runtime_event(
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(
            run_id=str(run_id),
            symbol=None,
            timeframe=None,
            bar_ts=None,
        ),
        context=WalletInitializedContext(
            run_id=str(run_id),
            bot_id=str(bot_id),
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            balances=dict(balances),
            source="run_start",
        ),
    )
    serialized_init = init_event.serialize()
    serialized_init["seq"] = 0
    runtime_events.append(serialized_init)
    return {
        "runtime_events": runtime_events,
        "runtime_event_seq": manager.Value("i", max(int(initial_seq or 0), 0)),
        "reservations": manager.dict(),
        "lock": manager.RLock(),
    }


def _next_run_event_seq(shared_wallet_proxy: Mapping[str, Any]) -> int:
    seq_counter = shared_wallet_proxy.get("runtime_event_seq")
    if seq_counter is None:
        raise RuntimeError("shared runtime_event_seq counter is required for bot runtime event sequencing")
    proxy_lock = shared_wallet_proxy.get("lock")
    if proxy_lock is not None:
        proxy_lock.acquire()
    try:
        if hasattr(seq_counter, "get"):
            current_value = int(seq_counter.get())
        elif hasattr(seq_counter, "value"):
            current_value = int(getattr(seq_counter, "value"))
        else:
            raise RuntimeError(f"shared runtime_event_seq counter is unsupported | type={type(seq_counter)!r}")
        next_value = current_value + 1
        if hasattr(seq_counter, "set"):
            seq_counter.set(next_value)
        elif hasattr(seq_counter, "value"):
            setattr(seq_counter, "value", next_value)
        else:
            raise RuntimeError(f"shared runtime_event_seq counter is unsupported | type={type(seq_counter)!r}")
        return int(next_value)
    finally:
        if proxy_lock is not None:
            proxy_lock.release()


def _load_strategy_symbols(strategy_id: str) -> List[str]:
    strategy = StrategyLoader.fetch_strategy(strategy_id)
    symbols: List[str] = []
    seen: set[str] = set()
    for link in strategy.instrument_links:
        symbol = str(getattr(link, "symbol", "") or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    if not symbols:
        raise RuntimeError(f"Strategy {strategy_id} has no instrument symbols configured")
    return symbols


def _assign_symbols_to_workers(symbols: Sequence[str], *, max_workers: int) -> List[List[str]]:
    if not symbols:
        return []
    normalized = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    if not normalized:
        return []
    if int(max_workers) < len(normalized):
        raise RuntimeError(
            f"process-per-series requires at least one worker per symbol "
            f"(symbols={len(normalized)}, max_workers={int(max_workers)}). "
            "Increase BOT_SYMBOL_PROCESS_MAX."
        )
    return [[symbol] for symbol in normalized]


@dataclass
class ContainerStartupContext:
    bot_id: str
    run_id: str
    bot: Dict[str, Any]
    runtime_bot_config: Dict[str, Any]
    strategy_id: str
    symbols: List[str]
    symbol_shards: List[List[str]]
    wallet_config: Dict[str, Any]
    manager: Any
    shared_wallet_proxy: Dict[str, Any]
    telemetry_sender: TelemetryEmitter | None = None
    parent_event_queue: "mp.Queue[Dict[str, Any]] | None" = None
    parent_control_queue: "mp.Queue[Dict[str, Any]] | None" = None
    children: Dict[str, mp.Process] = field(default_factory=dict)
    worker_symbols: Dict[str, List[str]] = field(default_factory=dict)
    degraded_symbols: set[str] = field(default_factory=set)
    series_states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    workers_spawned: int = 0
    startup_live_emitted: bool = False
    first_snapshot_series: set[str] = field(default_factory=set)
    reported_worker_failures: set[str] = field(default_factory=set)
    reported_worker_terminal_statuses: Dict[str, str] = field(default_factory=dict)
    runtime_state: str = BotLensRuntimeState.INITIALIZING.value
    recent_runtime_transitions: List[Dict[str, Any]] = field(default_factory=list)
    last_useful_progress_at: str | None = None
    progress_state: str = "starting"
    degraded_loop_started_at: str | None = None
    last_degraded_started_at: str | None = None
    last_degraded_duration_ms: int | None = None
    degraded_reason_code: str | None = None
    degraded_trigger_event: str | None = None
    degraded_cleared_at: str | None = None
    degraded_recovery_reason: str | None = None
    activity_without_progress_count: int = 0
    churn_detected_at: str | None = None
    churn_reason: str | None = None
    latest_pressure_snapshot: Dict[str, Any] = field(default_factory=dict)
    terminal_actor: str | None = None
    terminal_reason_text: str | None = None
    terminal_status_source: str | None = None
    terminal_status_value: str | None = None
    telemetry_degraded_emitted: bool = False


def _resolve_backend_run_id(bot_id: str) -> str:
    run_id = str(os.environ.get("QT_BOT_RUNTIME_RUN_ID") or "").strip()
    if run_id:
        return run_id
    fallback = str(uuid.uuid4())
    logger.warning(
        "bot_runtime_run_id_missing | bot_id=%s | generated_fallback_run_id=%s",
        bot_id,
        fallback,
    )
    return fallback


def _persist_lifecycle_phase(
    *,
    bot_id: str,
    run_id: str,
    phase: str,
    owner: str,
    message: str,
    metadata: Mapping[str, Any] | None = None,
    failure: Mapping[str, Any] | None = None,
    status: str | None = None,
    telemetry_sender: TelemetryEmitter | None = None,
    shared_wallet_proxy: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    seq = _next_run_event_seq(shared_wallet_proxy) if isinstance(shared_wallet_proxy, Mapping) else 0
    checkpoint = lifecycle_checkpoint_payload(
        bot_id=bot_id,
        run_id=run_id,
        phase=phase,
        owner=owner,
        message=message,
        metadata=metadata,
        failure=failure,
        status=status,
        seq=seq or None,
    )
    lifecycle_state = record_bot_run_lifecycle_checkpoint(checkpoint)
    resolved_status = str(lifecycle_state.get("status") or checkpoint["status"]).strip()
    if resolved_status in {
        BotLifecycleStatus.STOPPED.value,
        "failed",
        BotLifecycleStatus.STARTUP_FAILED.value,
        BotLifecycleStatus.CRASHED.value,
        BotLifecycleStatus.COMPLETED.value,
    }:
        update_bot_runtime_status(
            bot_id=bot_id,
            run_id=run_id,
            status=resolved_status,
            telemetry_degraded=resolved_status == BotLifecycleStatus.TELEMETRY_DEGRADED.value,
        )
    lifecycle_event_delivered = _notify_backend_lifecycle_event(
        lifecycle_state={
            **dict(lifecycle_state or {}),
            "bot_id": bot_id,
            "run_id": run_id,
            "phase": phase,
            "owner": owner,
            "message": message,
            "status": resolved_status,
            "metadata": dict(metadata or lifecycle_state.get("metadata") or {}),
            "failure": dict(failure or lifecycle_state.get("failure") or {}),
        },
        telemetry_sender=telemetry_sender,
    )
    log_fn = logger.info if phase in _INFO_LIFECYCLE_PHASES or resolved_status in _INFO_LIFECYCLE_STATUSES else logger.debug
    log_fn(
        "bot_runtime_lifecycle_checkpoint_persisted | bot_id=%s | run_id=%s | phase=%s | owner=%s | status=%s | lifecycle_event_delivered=%s | message=%s",
        bot_id,
        run_id,
        phase,
        owner,
        resolved_status,
        lifecycle_event_delivered,
        message,
    )
    return lifecycle_state


def _notify_backend_lifecycle_event(
    *,
    lifecycle_state: Mapping[str, Any],
    telemetry_sender: TelemetryEmitter | None = None,
) -> bool:
    telemetry_url = str(_TELEMETRY_SETTINGS.ws_url or "").strip()
    bot_id = str(lifecycle_state.get("bot_id") or "").strip()
    run_id = str(lifecycle_state.get("run_id") or "").strip()
    phase = str(lifecycle_state.get("phase") or "").strip()
    status = str(lifecycle_state.get("status") or "").strip()
    if not telemetry_url:
        logger.warning(
            "bot_runtime_lifecycle_event_skipped | bot_id=%s | run_id=%s | phase=%s | status=%s | reason=telemetry_url_missing",
            bot_id,
            run_id,
            phase,
            status,
        )
        return False
    payload = {
        "kind": "botlens_lifecycle_event",
        "bot_id": bot_id,
        "run_id": run_id,
        "source_emitter": "container_runtime",
        "source_reason": "ingest",
        "seq": int(lifecycle_state.get("seq") or 0),
        "phase": phase,
        "owner": str(lifecycle_state.get("owner") or "").strip() or None,
        "message": str(lifecycle_state.get("message") or "").strip() or None,
        "status": status,
        "metadata": dict(lifecycle_state.get("metadata") or {}),
        "failure": dict(lifecycle_state.get("failure") or {}),
        "checkpoint_at": lifecycle_state.get("checkpoint_at") or lifecycle_state.get("updated_at"),
        "updated_at": lifecycle_state.get("updated_at") or lifecycle_state.get("checkpoint_at"),
        "known_at": lifecycle_state.get("checkpoint_at") or lifecycle_state.get("updated_at") or utc_now_iso(),
    }
    safe_payload = json_safe(payload)
    is_terminal_lifecycle = phase in TERMINAL_PHASES or status in _TERMINAL_STATUSES
    delivered = False
    if is_terminal_lifecycle:
        delivered = emit_telemetry_ephemeral_message(telemetry_url, json.dumps(safe_payload))
        if not delivered and telemetry_sender is not None:
            logger.warning(
                "bot_runtime_lifecycle_event_direct_delivery_failed | bot_id=%s | run_id=%s | phase=%s | status=%s | fallback=queued_sender",
                bot_id,
                run_id,
                phase,
                status,
            )
            delivered = telemetry_sender.send(safe_payload)
    elif telemetry_sender is not None:
        delivered = telemetry_sender.send(safe_payload)
    else:
        delivered = emit_telemetry_ephemeral_message(telemetry_url, json.dumps(safe_payload))
    if not delivered:
        logger.warning(
            "bot_runtime_lifecycle_event_delivery_failed | bot_id=%s | run_id=%s | phase=%s | status=%s | telemetry_url=%s",
            bot_id,
            run_id,
            phase,
            status,
            telemetry_url,
        )
    return delivered


def _series_progress_metadata(ctx: ContainerStartupContext) -> Dict[str, Any]:
    return build_series_progress_metadata(
        total_series=len(ctx.symbols),
        workers_planned=len(ctx.symbol_shards),
        workers_spawned=ctx.workers_spawned,
        series=ctx.series_states,
    )


def _runtime_observability_metadata(ctx: ContainerStartupContext) -> Dict[str, Any]:
    metadata = _series_progress_metadata(ctx)
    runtime_meta: Dict[str, Any] = {
        "runtime_state": ctx.runtime_state,
        "progress_state": ctx.progress_state,
    }
    if ctx.recent_runtime_transitions:
        runtime_meta["recent_transitions"] = [dict(entry) for entry in ctx.recent_runtime_transitions[-12:]]
        runtime_meta["transition"] = dict(ctx.recent_runtime_transitions[-1])
    if ctx.last_useful_progress_at:
        runtime_meta["last_useful_progress_at"] = ctx.last_useful_progress_at
    degraded_payload = _degraded_condition_payload(ctx)
    if degraded_payload:
        runtime_meta["degraded"] = degraded_payload
    churn_payload = _churn_payload(ctx)
    if churn_payload:
        runtime_meta["churn"] = churn_payload
    if ctx.latest_pressure_snapshot:
        runtime_meta["pressure"] = dict(ctx.latest_pressure_snapshot)
    terminal_payload = _terminal_payload(ctx)
    if terminal_payload:
        runtime_meta["terminal"] = terminal_payload
    if runtime_meta:
        metadata["runtime_observability"] = runtime_meta
    return metadata


def _parse_iso_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_utc_iso(value: Any) -> str:
    if isinstance(value, datetime):
        timestamp = value
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)
        return timestamp.isoformat().replace("+00:00", "Z")
    text = str(value or "").strip()
    return text or utc_now_iso()


def _transition_runtime_state(
    ctx: ContainerStartupContext,
    *,
    next_state: str,
    reason: str,
    source_component: str,
    observed_at: Any = None,
    allow_restart: bool = False,
) -> Dict[str, Any] | None:
    try:
        transition = guard_runtime_state_transition(
            current_state=ctx.runtime_state,
            next_state=next_state,
            transition_reason=reason,
            source_component=source_component,
            timestamp=observed_at,
            allow_restart=allow_restart,
        )
    except InvalidRuntimeStateTransition:
        current_state = runtime_state_value(ctx.runtime_state)
        attempted_state = runtime_state_value(next_state)
        _OBSERVER.event(
            "runtime_state_transition_rejected",
            level=logging.ERROR,
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            runtime_state=current_state,
            next_runtime_state=attempted_state,
            transition_reason=reason,
            source_component=source_component,
        )
        _persist_runtime_state_transition_rejected(
            ctx,
            from_state=current_state,
            attempted_to_state=attempted_state,
            reason=reason,
            source_component=source_component,
            observed_at=observed_at,
        )
        raise
    current_state = runtime_state_value(ctx.runtime_state)
    target_state = transition.to_state
    ctx.runtime_state = target_state
    transition_payload = transition.to_dict()
    if current_state != target_state:
        ctx.recent_runtime_transitions.append(dict(transition_payload))
        if len(ctx.recent_runtime_transitions) > 12:
            ctx.recent_runtime_transitions = ctx.recent_runtime_transitions[-12:]
        _OBSERVER.event(
            "runtime_state_transition",
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            from_state=current_state,
            to_state=target_state,
            transition_reason=reason,
            source_component=source_component,
            timestamp=transition.timestamp,
        )
        return transition_payload
    return None


def _lifecycle_phase_for_runtime_state(current_state: str | None) -> str:
    normalized = runtime_state_value(current_state)
    if normalized == BotLensRuntimeState.LIVE.value:
        return BotLifecyclePhase.LIVE.value
    if normalized == BotLensRuntimeState.DEGRADED.value:
        return BotLifecyclePhase.DEGRADED.value
    if normalized == BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value:
        return BotLifecyclePhase.AWAITING_FIRST_SNAPSHOT.value
    if normalized == BotLensRuntimeState.STARTUP_FAILED.value:
        return BotLifecyclePhase.STARTUP_FAILED.value
    if normalized == BotLensRuntimeState.CRASHED.value:
        return BotLifecyclePhase.CRASHED.value
    if normalized == BotLensRuntimeState.STOPPED.value:
        return BotLifecyclePhase.STOPPED.value
    return BotLifecyclePhase.WAITING_FOR_SERIES_BOOTSTRAP.value


def _persist_runtime_state_transition_rejected(
    ctx: ContainerStartupContext,
    *,
    from_state: str | None,
    attempted_to_state: str | None,
    reason: str,
    source_component: str,
    observed_at: Any = None,
) -> None:
    phase = _lifecycle_phase_for_runtime_state(from_state or ctx.runtime_state)
    event_time = str(observed_at or "").strip() or utc_now_iso()
    message = (
        f"Rejected runtime state transition {from_state or 'none'} -> {attempted_to_state or 'none'} "
        f"| reason={reason} | source_component={source_component}"
    )
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=phase,
        owner=LifecycleOwner.RUNTIME.value,
        message=message,
        metadata=_runtime_observability_metadata(ctx),
        failure={
            "phase": phase,
            "message": message,
            "at": event_time,
            "type": "runtime_state_transition_rejected",
            "reason_code": "runtime_state_transition_rejected",
            "owner": LifecycleOwner.RUNTIME.value,
            "from_state": from_state,
            "attempted_to_state": attempted_to_state,
            "transition_reason": reason,
            "source_component": source_component,
        },
        status=status_for_phase(phase),
        telemetry_sender=ctx.telemetry_sender,
        shared_wallet_proxy=ctx.shared_wallet_proxy,
    )


def _derive_top_pressure(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    queue_depth = int(snapshot.get("telemetry_queue_depth") or 0)
    queue_capacity = max(int(snapshot.get("telemetry_queue_capacity") or 0), 1)
    queue_ratio = queue_depth / queue_capacity
    control_queue_depth = int(snapshot.get("telemetry_control_queue_depth") or 0)
    control_queue_capacity = max(int(snapshot.get("telemetry_control_queue_capacity") or 0), 1)
    control_queue_ratio = control_queue_depth / control_queue_capacity
    emit_queue_depth = int(snapshot.get("telemetry_emit_queue_depth") or 0)
    emit_queue_capacity = max(int(snapshot.get("telemetry_emit_queue_capacity") or 0), 1)
    emit_queue_ratio = emit_queue_depth / emit_queue_capacity
    if bool(snapshot.get("telemetry_backpressure_active")):
        candidates.append({"reason_code": "telemetry_backpressure", "value": queue_ratio, "unit": "ratio"})
    if bool(snapshot.get("telemetry_control_backpressure_active")):
        candidates.append({"reason_code": "telemetry_control_backpressure", "value": control_queue_ratio, "unit": "ratio"})
    if bool(snapshot.get("telemetry_emit_backpressure_active")):
        candidates.append({"reason_code": "telemetry_emit_backpressure", "value": emit_queue_ratio, "unit": "ratio"})
    if bool(snapshot.get("telemetry_retry_pending")):
        candidates.append({"reason_code": "telemetry_retry_pending", "value": 1, "unit": "flag"})
    if float(snapshot.get("payload_bytes") or 0.0) >= 128_000:
        candidates.append({"reason_code": "payload_bytes", "value": int(snapshot.get("payload_bytes") or 0), "unit": "bytes"})
    if float(snapshot.get("telemetry_emit_ms") or 0.0) >= 500.0:
        candidates.append({"reason_code": "telemetry_emit_ms", "value": round(float(snapshot.get("telemetry_emit_ms") or 0.0), 3), "unit": "ms"})
    if float(snapshot.get("parent_event_queue_depth") or 0.0) > 0.0:
        candidates.append({"reason_code": "parent_event_queue_depth", "value": int(snapshot.get("parent_event_queue_depth") or 0), "unit": "count"})
    if float(snapshot.get("parent_control_queue_depth") or 0.0) > 0.0:
        candidates.append({"reason_code": "parent_control_queue_depth", "value": int(snapshot.get("parent_control_queue_depth") or 0), "unit": "count"})
    if not candidates:
        return {}
    return max(candidates, key=lambda entry: float(entry.get("value") or 0.0))


def _capture_pressure_snapshot(
    ctx: ContainerStartupContext,
    *,
    trigger: str,
    observed_at: Any = None,
    payload_bytes: int | None = None,
    telemetry_emit_ms: float | None = None,
    queue_drain_ms: float | None = None,
    trigger_event: str | None = None,
) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "captured_at": str(observed_at or "").strip() or utc_now_iso(),
        "trigger": str(trigger or "").strip() or "runtime_activity",
    }
    if trigger_event:
        snapshot["trigger_event"] = str(trigger_event).strip()
    if payload_bytes is not None:
        snapshot["payload_bytes"] = int(payload_bytes)
    if telemetry_emit_ms is not None:
        snapshot["telemetry_emit_ms"] = round(float(telemetry_emit_ms), 3)
    if queue_drain_ms is not None:
        snapshot["queue_drain_ms"] = round(float(queue_drain_ms), 3)
    if ctx.parent_event_queue is not None:
        try:
            snapshot["parent_event_queue_depth"] = int(ctx.parent_event_queue.qsize())
        except Exception:
            snapshot["parent_event_queue_depth"] = 0
    if ctx.parent_control_queue is not None:
        try:
            snapshot["parent_control_queue_depth"] = int(ctx.parent_control_queue.qsize())
        except Exception:
            snapshot["parent_control_queue_depth"] = 0
    if ctx.telemetry_sender is not None:
        sender_snapshot = ctx.telemetry_sender.pressure_snapshot()
        snapshot["telemetry_queue_depth"] = int(sender_snapshot.get("queue_depth") or 0)
        snapshot["telemetry_queue_capacity"] = int(sender_snapshot.get("queue_capacity") or 0)
        snapshot["telemetry_queue_oldest_age_ms"] = float(sender_snapshot.get("queue_oldest_age_ms") or 0.0)
        snapshot["telemetry_backpressure_active"] = bool(sender_snapshot.get("backpressure_active"))
        snapshot["telemetry_control_queue_depth"] = int(sender_snapshot.get("control_queue_depth") or 0)
        snapshot["telemetry_control_queue_capacity"] = int(sender_snapshot.get("control_queue_capacity") or 0)
        snapshot["telemetry_control_queue_oldest_age_ms"] = float(sender_snapshot.get("control_queue_oldest_age_ms") or 0.0)
        snapshot["telemetry_control_backpressure_active"] = bool(sender_snapshot.get("control_backpressure_active"))
        snapshot["telemetry_emit_queue_depth"] = int(sender_snapshot.get("emit_queue_depth") or 0)
        snapshot["telemetry_emit_queue_capacity"] = int(sender_snapshot.get("emit_queue_capacity") or 0)
        snapshot["telemetry_emit_queue_oldest_age_ms"] = float(sender_snapshot.get("emit_queue_oldest_age_ms") or 0.0)
        snapshot["telemetry_emit_backpressure_active"] = bool(sender_snapshot.get("emit_backpressure_active"))
        snapshot["telemetry_retry_pending"] = bool(sender_snapshot.get("transport_retry_pending"))
        snapshot["telemetry_transport_connected"] = bool(sender_snapshot.get("transport_connected"))
        snapshot["telemetry_suppressed_bootstrap_duplicates"] = int(sender_snapshot.get("suppressed_bootstrap_duplicates") or 0)
        snapshot["telemetry_suppressed_large_fact_duplicates"] = int(sender_snapshot.get("suppressed_large_fact_duplicates") or 0)
        if payload_bytes is None:
            snapshot["payload_bytes"] = int(sender_snapshot.get("last_payload_bytes") or 0)
        if telemetry_emit_ms is None:
            snapshot["telemetry_emit_ms"] = float(sender_snapshot.get("last_send_ms") or 0.0)
    top_pressure = _derive_top_pressure(snapshot)
    if top_pressure:
        snapshot["top_pressure"] = top_pressure
    ctx.latest_pressure_snapshot = dict(snapshot)
    return snapshot


def _degraded_condition_payload(ctx: ContainerStartupContext) -> Dict[str, Any]:
    if not ctx.degraded_loop_started_at and not ctx.degraded_reason_code and not ctx.degraded_cleared_at:
        return {}
    started_at = ctx.degraded_loop_started_at or ctx.last_degraded_started_at
    payload: Dict[str, Any] = {
        "active": bool(ctx.degraded_loop_started_at),
        "started_at": started_at,
        "reason_code": ctx.degraded_reason_code,
        "trigger_event": ctx.degraded_trigger_event,
        "cleared_at": ctx.degraded_cleared_at,
        "recovery_reason": ctx.degraded_recovery_reason,
    }
    if started_at and ctx.degraded_cleared_at:
        started = _parse_iso_timestamp(started_at)
        cleared = _parse_iso_timestamp(ctx.degraded_cleared_at)
        if started is not None and cleared is not None:
            payload["duration_ms"] = max(int((cleared - started).total_seconds() * 1000.0), 0)
    elif ctx.last_degraded_duration_ms is not None:
        payload["duration_ms"] = int(ctx.last_degraded_duration_ms)
    return {key: value for key, value in payload.items() if value not in (None, "", [])}


def _churn_payload(ctx: ContainerStartupContext) -> Dict[str, Any]:
    if not ctx.churn_detected_at and ctx.activity_without_progress_count <= 0:
        return {}
    payload: Dict[str, Any] = {
        "active": bool(ctx.churn_detected_at),
        "detected_at": ctx.churn_detected_at,
        "reason_code": ctx.churn_reason,
        "activity_without_progress_count": int(ctx.activity_without_progress_count or 0),
    }
    if ctx.last_useful_progress_at:
        payload["last_useful_progress_at"] = ctx.last_useful_progress_at
    return {key: value for key, value in payload.items() if value not in (None, "", [])}


def _terminal_payload(ctx: ContainerStartupContext) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if ctx.terminal_status_value:
        payload["status"] = ctx.terminal_status_value
    if ctx.terminal_status_source:
        payload["source"] = ctx.terminal_status_source
    if ctx.terminal_actor:
        payload["actor"] = ctx.terminal_actor
    if ctx.terminal_reason_text:
        payload["reason"] = ctx.terminal_reason_text
    if ctx.reported_worker_terminal_statuses:
        payload["worker_terminal_statuses"] = dict(ctx.reported_worker_terminal_statuses)
    payload["expected_workers"] = len(ctx.worker_symbols)
    payload["reported_workers"] = len(ctx.reported_worker_terminal_statuses)
    return payload


def _mark_useful_progress(ctx: ContainerStartupContext, observed_at: Any) -> None:
    timestamp = str(observed_at or "").strip() or utc_now_iso()
    ctx.last_useful_progress_at = timestamp
    ctx.progress_state = "progressing"
    ctx.activity_without_progress_count = 0
    ctx.churn_detected_at = None
    ctx.churn_reason = None


def _mark_degraded_loop(
    ctx: ContainerStartupContext,
    *,
    started_at: Any = None,
    reason: str | None = None,
    trigger_event: str | None = None,
) -> None:
    timestamp = str(started_at or "").strip() or utc_now_iso()
    is_new_degraded_condition = not ctx.degraded_loop_started_at
    if not ctx.degraded_loop_started_at:
        ctx.degraded_loop_started_at = timestamp
        ctx.last_degraded_started_at = timestamp
        ctx.last_degraded_duration_ms = None
    ctx.progress_state = "degraded"
    ctx.degraded_cleared_at = None
    ctx.degraded_recovery_reason = None
    if reason:
        ctx.degraded_reason_code = str(reason).strip()
    if trigger_event:
        ctx.degraded_trigger_event = str(trigger_event).strip()
    if is_new_degraded_condition:
        _OBSERVER.event(
            "degraded_condition_started",
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            runtime_state=ctx.runtime_state,
            reason_code=ctx.degraded_reason_code,
            trigger_event=ctx.degraded_trigger_event,
            timestamp=timestamp,
        )


def _clear_degraded_loop(ctx: ContainerStartupContext, *, cleared_at: Any = None, recovery_reason: str | None = None) -> None:
    if not ctx.degraded_loop_started_at:
        return
    started_at = ctx.degraded_loop_started_at
    ctx.degraded_cleared_at = str(cleared_at or "").strip() or utc_now_iso()
    ctx.degraded_recovery_reason = str(recovery_reason or "").strip() or "runtime_progress_resumed"
    ctx.degraded_loop_started_at = None
    ctx.degraded_reason_code = None
    ctx.degraded_trigger_event = None
    ctx.progress_state = "progressing"
    started_ts = _parse_iso_timestamp(started_at)
    cleared_ts = _parse_iso_timestamp(ctx.degraded_cleared_at)
    duration_ms = None
    if started_ts is not None and cleared_ts is not None:
        duration_ms = max(int((cleared_ts - started_ts).total_seconds() * 1000.0), 0)
    ctx.last_degraded_started_at = started_at
    ctx.last_degraded_duration_ms = duration_ms
    _OBSERVER.event(
        "degraded_condition_cleared",
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        runtime_state=ctx.runtime_state,
        duration_ms=duration_ms,
        recovery_reason=ctx.degraded_recovery_reason,
        timestamp=ctx.degraded_cleared_at,
    )


def _record_non_progress_activity(
    ctx: ContainerStartupContext,
    *,
    observed_at: Any = None,
    reason: str,
) -> None:
    timestamp = str(observed_at or "").strip() or utc_now_iso()
    churn_was_active = bool(ctx.churn_detected_at)
    ctx.activity_without_progress_count = int(ctx.activity_without_progress_count or 0) + 1
    last_progress_at = _parse_iso_timestamp(ctx.last_useful_progress_at)
    observed_ts = _parse_iso_timestamp(timestamp)
    elapsed_ms = 0.0
    if last_progress_at is not None and observed_ts is not None:
        elapsed_ms = max((observed_ts - last_progress_at).total_seconds() * 1000.0, 0.0)
    if (
        elapsed_ms >= _CHURN_NO_PROGRESS_MS
        and ctx.activity_without_progress_count >= _CHURN_ACTIVITY_THRESHOLD
        and not ctx.churn_detected_at
    ):
        ctx.churn_detected_at = timestamp
        ctx.churn_reason = str(reason or "activity_without_progress").strip()
        ctx.progress_state = "churning"
    if not churn_was_active and ctx.churn_detected_at:
        _OBSERVER.event(
            "churn_detected",
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            runtime_state=ctx.runtime_state,
            reason_code=ctx.churn_reason,
            detected_at=ctx.churn_detected_at,
            last_useful_progress_at=ctx.last_useful_progress_at,
            activity_without_progress_count=ctx.activity_without_progress_count,
        )


def _worker_failure_payload(
    *,
    ctx: ContainerStartupContext,
    worker_id: str,
    phase: str,
    message: str,
    exit_code: int | None = None,
    exception_type: str | None = None,
    traceback_text: str | None = None,
    stderr_tail: str | None = None,
    component: str | None = None,
    operation: str | None = None,
    path: str | None = None,
    errno: int | None = None,
    reason_code: str | None = None,
) -> Dict[str, Any]:
    symbols = [str(symbol).strip().upper() for symbol in (ctx.worker_symbols.get(worker_id) or []) if str(symbol).strip()]
    failure_type = "worker_exit" if exit_code is not None else "worker_exception" if exception_type or traceback_text else "worker_failure"
    resolved_reason_code = reason_code or (
        "worker_exit_non_zero"
        if exit_code is not None
        else "runtime_worker_exception"
        if exception_type or traceback_text
        else "worker_failure"
    )
    return build_failure_payload(
        phase=phase,
        message=message,
        type=failure_type,
        reason_code=resolved_reason_code,
        owner=LifecycleOwner.RUNTIME.value,
        worker_id=worker_id,
        symbol=symbols[0] if len(symbols) == 1 else None,
        exit_code=exit_code,
        stderr_tail=stderr_tail,
        exception_type=exception_type,
        traceback=traceback_text.strip() if traceback_text else None,
        symbols=symbols or None,
        component=component,
        operation=operation,
        path=path,
        errno=errno,
    )


def _classify_worker_exception(exc: Exception) -> Dict[str, Any]:
    if isinstance(exc, OSError) and int(getattr(exc, "errno", 0) or 0) == 39:
        path = str(getattr(exc, "filename", "") or "").strip() or None
        if "Directory not empty" in str(exc):
            return {
                "reason_code": "artifact_cleanup_race",
                "component": "report_artifacts",
                "operation": "spool_cleanup",
                "path": path,
                "errno": 39,
            }
    return {}


def _runtime_state_for_phase(phase: str, *, current_state: str | None = None) -> str | None:
    normalized_phase = str(phase or "").strip().lower()
    if normalized_phase in {
        BotLifecyclePhase.CONTAINER_BOOTING.value,
        BotLifecyclePhase.LOADING_BOT_CONFIG.value,
        BotLifecyclePhase.CLAIMING_RUN.value,
        BotLifecyclePhase.LOADING_STRATEGY_SNAPSHOT.value,
        BotLifecyclePhase.PREPARING_WALLET.value,
        BotLifecyclePhase.PLANNING_SERIES_WORKERS.value,
        BotLifecyclePhase.SPAWNING_SERIES_WORKERS.value,
        BotLifecyclePhase.WAITING_FOR_SERIES_BOOTSTRAP.value,
        BotLifecyclePhase.WARMING_UP_RUNTIME.value,
        BotLifecyclePhase.RUNTIME_SUBSCRIBING.value,
    }:
        return BotLensRuntimeState.INITIALIZING.value
    if normalized_phase == BotLifecyclePhase.AWAITING_FIRST_SNAPSHOT.value:
        return BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value
    if normalized_phase == BotLifecyclePhase.LIVE.value:
        return BotLensRuntimeState.LIVE.value
    if normalized_phase in {BotLifecyclePhase.DEGRADED.value, BotLifecyclePhase.TELEMETRY_DEGRADED.value}:
        return BotLensRuntimeState.DEGRADED.value
    if normalized_phase == BotLifecyclePhase.STARTUP_FAILED.value:
        return BotLensRuntimeState.STARTUP_FAILED.value
    if normalized_phase == BotLifecyclePhase.CRASHED.value:
        return BotLensRuntimeState.CRASHED.value
    if normalized_phase == BotLifecyclePhase.STOPPED.value:
        return BotLensRuntimeState.STOPPED.value
    if normalized_phase == BotLifecyclePhase.COMPLETED.value:
        return BotLensRuntimeState.STOPPED.value
    return runtime_state_value(current_state)


def _transition_for_phase(
    ctx: ContainerStartupContext,
    *,
    phase: str,
    reason: str,
    source_component: str,
    observed_at: Any = None,
) -> Dict[str, Any] | None:
    target_state = _runtime_state_for_phase(phase, current_state=ctx.runtime_state)
    if not target_state:
        return None
    return _transition_runtime_state(
        ctx,
        next_state=target_state,
        reason=reason,
        source_component=source_component,
        observed_at=observed_at,
    )


def _handle_continuity_gap_event(
    ctx: ContainerStartupContext,
    event: Mapping[str, Any],
) -> None:
    reason = str(event.get("reason") or "continuity_gap").strip() or "continuity_gap"
    trigger_event = str(event.get("trigger_event") or "continuity_gap").strip() or "continuity_gap"
    observed_at = event.get("known_at") or event.get("event_time") or utc_now_iso()
    previous_reason = ctx.degraded_reason_code
    previous_trigger = ctx.degraded_trigger_event
    previous_churn_at = ctx.churn_detected_at
    previous_degraded_started_at = ctx.degraded_loop_started_at
    _mark_degraded_loop(
        ctx,
        started_at=observed_at,
        reason=reason,
        trigger_event=trigger_event,
    )
    _record_non_progress_activity(
        ctx,
        observed_at=observed_at,
        reason=reason,
    )
    _capture_pressure_snapshot(
        ctx,
        trigger="continuity_gap",
        observed_at=observed_at,
        trigger_event=trigger_event,
    )
    transition = _transition_runtime_state(
        ctx,
        next_state=BotLensRuntimeState.DEGRADED.value,
        reason=f"continuity_gap:{reason}",
        source_component="worker_bridge",
        observed_at=observed_at,
    )
    should_persist = (
        transition is not None
        or previous_reason != ctx.degraded_reason_code
        or previous_trigger != ctx.degraded_trigger_event
        or (previous_churn_at is None and ctx.churn_detected_at is not None)
        or previous_degraded_started_at != ctx.degraded_loop_started_at
    )
    if not should_persist:
        return
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=BotLifecyclePhase.DEGRADED.value,
        owner=LifecycleOwner.RUNTIME.value,
        message=f"Runtime continuity degraded after live progress stopped advancing ({reason}).",
        metadata=_runtime_observability_metadata(ctx),
        status=BotLifecycleStatus.DEGRADED.value,
        telemetry_sender=ctx.telemetry_sender,
        shared_wallet_proxy=ctx.shared_wallet_proxy,
    )


def _set_series_state(
    ctx: ContainerStartupContext,
    *,
    symbol: str,
    status: str,
    worker_id: str | None = None,
    message: str | None = None,
    series_key: str | None = None,
    bootstrap_seq: int | None = None,
    error: str | None = None,
) -> None:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return
    current = dict(ctx.series_states.get(normalized_symbol) or {})
    current["symbol"] = normalized_symbol
    current["status"] = str(status or "").strip()
    if worker_id:
        current["worker_id"] = worker_id
    if message:
        current["message"] = message
    if series_key:
        current["series_key"] = series_key
    if bootstrap_seq is not None:
        current["bootstrap_seq"] = int(bootstrap_seq)
    if error:
        current["error"] = error
    current["updated_at"] = utc_now_iso()
    ctx.series_states[normalized_symbol] = current


def _parent_event_queue_maxsize(*, worker_count: int) -> int:
    per_worker_capacity = max(8, int(_TELEMETRY_EMIT_QUEUE_MAX or 0))
    return per_worker_capacity * max(int(worker_count or 0), 1)


def load_container_startup_context(
    bot_id: str,
    *,
    telemetry_sender: TelemetryEmitter | None = None,
) -> ContainerStartupContext:
    run_id = _resolve_backend_run_id(bot_id)
    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.CONTAINER_BOOTING.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Container process booting with backend-owned startup contract.",
        telemetry_sender=telemetry_sender,
    )

    bot = next((b for b in load_bots() if b.get("id") == bot_id), None)
    if bot is None:
        raise RuntimeError(f"Bot not found: {bot_id}")
    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.LOADING_BOT_CONFIG.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Loading bot config snapshot in runtime container.",
        telemetry_sender=telemetry_sender,
    )

    runtime_bot_config = _materialize_bot_config(bot)
    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.CLAIMING_RUN.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Container claimed backend-owned run_id.",
        metadata={"run_id": run_id},
        telemetry_sender=telemetry_sender,
    )
    strategy_id = str(bot.get("strategy_id") or "").strip()
    if not strategy_id:
        raise RuntimeError(f"Bot {bot_id} has no strategy_id configured")
    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.LOADING_STRATEGY_SNAPSHOT.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Loading strategy snapshot for worker planning.",
        metadata={"strategy_id": strategy_id},
        telemetry_sender=telemetry_sender,
    )
    all_symbols = _load_strategy_symbols(strategy_id)
    max_symbols = _MAX_SYMBOLS_PER_STRATEGY
    if len(all_symbols) > max_symbols:
        raise RuntimeError(
            f"Strategy {strategy_id} has {len(all_symbols)} symbols but runtime limit is {max_symbols}. "
            "Reduce symbols or increase BOT_MAX_SYMBOLS_PER_STRATEGY."
        )

    preparing_wallet_state = _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.PREPARING_WALLET.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Preparing shared wallet for worker processes.",
        telemetry_sender=telemetry_sender,
    )
    wallet_config = validate_wallet_config(bot.get("wallet_config") if isinstance(bot.get("wallet_config"), Mapping) else None)
    balances = wallet_config.get("balances")
    manager = mp.Manager()
    shared_wallet_proxy = _build_shared_wallet_proxy(
        manager,
        run_id=run_id,
        bot_id=bot_id,
        balances=_normalise_balances(balances),
        initial_seq=int(preparing_wallet_state.get("seq") or 0),
    )

    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.PLANNING_SERIES_WORKERS.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Planning one worker shard per strategy symbol.",
        metadata={"symbols": list(all_symbols), "symbol_count": len(all_symbols)},
        telemetry_sender=telemetry_sender,
        shared_wallet_proxy=shared_wallet_proxy,
    )
    default_max_workers = max(_MAX_SYMBOL_WORKERS, len(all_symbols))
    max_workers = (
        _BOT_RUNTIME_SETTINGS.symbol_process_max
        if _BOT_RUNTIME_SETTINGS.symbol_process_max is not None
        else default_max_workers
    )
    symbol_shards = _assign_symbols_to_workers(
        all_symbols,
        max_workers=max(1, max_workers),
    )
    if not symbol_shards:
        raise RuntimeError(f"Strategy {strategy_id} resolved no symbol shards")

    ctx = ContainerStartupContext(
        bot_id=bot_id,
        run_id=run_id,
        bot=dict(bot),
        runtime_bot_config=runtime_bot_config,
        strategy_id=strategy_id,
        symbols=list(all_symbols),
        symbol_shards=[list(symbols) for symbols in symbol_shards],
        wallet_config=wallet_config,
        manager=manager,
        shared_wallet_proxy=shared_wallet_proxy,
        telemetry_sender=telemetry_sender,
    )
    for symbols in ctx.symbol_shards:
        for symbol in symbols:
            _set_series_state(ctx, symbol=symbol, status="planned", message="Worker plan created.")
    return ctx


def spawn_workers(ctx: ContainerStartupContext) -> None:
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=BotLifecyclePhase.SPAWNING_SERIES_WORKERS.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Spawning runtime workers for planned symbol shards.",
        metadata=_series_progress_metadata(ctx),
        telemetry_sender=ctx.telemetry_sender,
        shared_wallet_proxy=ctx.shared_wallet_proxy,
    )
    parent_event_queue: "mp.Queue[Dict[str, Any]]" = mp.Queue(
        maxsize=_parent_event_queue_maxsize(worker_count=len(ctx.symbol_shards))
    )
    parent_control_queue: "mp.Queue[Dict[str, Any]]" = mp.Queue(
        maxsize=max(32, len(ctx.symbol_shards) * 16)
    )
    ctx.parent_event_queue = parent_event_queue
    ctx.parent_control_queue = parent_control_queue
    for index, symbols in enumerate(ctx.symbol_shards):
        worker_id = f"worker-{index + 1}"
        ctx.worker_symbols[worker_id] = list(symbols)
        for symbol in symbols:
            _set_series_state(ctx, symbol=symbol, status="spawned", worker_id=worker_id, message="Worker process spawned.")
        proc = mp.Process(
            target=_series_worker,
            kwargs={
                "run_id": ctx.run_id,
                "bot_id": ctx.bot_id,
                "worker_id": worker_id,
                "strategy_id": ctx.strategy_id,
                "symbols": list(symbols),
                "bot_config": ctx.runtime_bot_config,
                "shared_wallet_proxy": ctx.shared_wallet_proxy,
                "event_queue": parent_event_queue,
                "control_queue": parent_control_queue,
            },
            daemon=False,
        )
        proc.start()
        ctx.children[worker_id] = proc
        ctx.workers_spawned += 1
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=BotLifecyclePhase.WAITING_FOR_SERIES_BOOTSTRAP.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Waiting for all planned series workers to report bootstrap state.",
        metadata=_series_progress_metadata(ctx),
        telemetry_sender=ctx.telemetry_sender,
        shared_wallet_proxy=ctx.shared_wallet_proxy,
    )



def _series_worker(
    *,
    run_id: str,
    bot_id: str,
    worker_id: str,
    strategy_id: str,
    symbols: Sequence[str],
    bot_config: Mapping[str, Any],
    shared_wallet_proxy: Mapping[str, Any],
    event_queue: "mp.Queue[Dict[str, Any]]",
    control_queue: "mp.Queue[Dict[str, Any]]",
) -> None:
    # Child processes inherit parent engine/pool state after fork; force a clean DB
    # bootstrap in-process to avoid libpq/ORM corruption across processes.
    db.reset_for_fork()

    logger.debug(
        "bot_symbol_worker_started | run_id=%s | bot_id=%s | worker_id=%s | symbols=%s | cache_owner=series_process",
        run_id,
        bot_id,
        worker_id,
        list(symbols),
    )
    child_config = dict(bot_config)
    child_config["strategy_ids"] = [strategy_id]
    child_config["strategy_id"] = strategy_id
    child_config["run_id"] = str(run_id)
    child_config["runtime_symbols"] = list(symbols)
    child_config["degrade_series_on_error"] = True
    child_config["shared_wallet_proxy"] = dict(shared_wallet_proxy)
    child_config["series_runner"] = "inline"
    child_config["worker_id"] = worker_id
    child_config["report_artifact_role"] = "worker"
    if (
        "BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY" not in child_config
        and "push_payload_bytes_sample_every" not in child_config
    ):
        child_config["BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY"] = _PUSH_SETTINGS.payload_bytes_sample_every
    runtime_error: Dict[str, str] = {}
    runtime = BotRuntime(bot_id=bot_id, config=child_config, deps=build_bot_runtime_deps())

    def _queue_worker_event(payload: Mapping[str, Any], *, timeout_s: float = 0.25) -> bool:
        try:
            event_queue.put(dict(payload), timeout=timeout_s)
            return True
        except queue.Full:
            logger.warning(
                "bot_runtime_worker_bridge_queue_full | bot_id=%s | run_id=%s | worker_id=%s | kind=%s | queue_max=%s",
                bot_id,
                run_id,
                worker_id,
                str(payload.get("kind") or ""),
                max(8, _TELEMETRY_EMIT_QUEUE_MAX),
            )
            return False

    def _queue_control_event(payload: Mapping[str, Any], *, timeout_s: float = 0.25) -> bool:
        try:
            control_queue.put(dict(payload), timeout=timeout_s)
            return True
        except queue.Full:
            logger.warning(
                "bot_runtime_worker_control_queue_full | bot_id=%s | run_id=%s | worker_id=%s | kind=%s | queue_max=%s",
                bot_id,
                run_id,
                worker_id,
                str(payload.get("kind") or ""),
                max(32, len(symbols) * 16),
            )
            return False

    def _build_bootstrap_payload() -> Dict[str, Any]:
        bootstrap_payload = runtime.botlens_bootstrap_payload()
        facts = bootstrap_payload.get("facts") if isinstance(bootstrap_payload.get("facts"), list) else []
        series_key_local = normalize_series_key(bootstrap_payload.get("series_key"))
        known_at_local = str(bootstrap_payload.get("known_at") or "").strip() or utc_now_iso()
        if not facts:
            raise RuntimeError(f"worker bootstrap missing facts | worker_id={worker_id} | symbols={list(symbols)}")
        if not series_key_local:
            raise RuntimeError(f"worker bootstrap missing series key | worker_id={worker_id} | symbols={list(symbols)}")
        return {
            **dict(bootstrap_payload),
            "series_key": series_key_local,
            "known_at": known_at_local,
            "event_time": bootstrap_payload.get("event_time") or known_at_local,
            "facts": [dict(fact) for fact in facts if isinstance(fact, Mapping)],
        }

    bridge_session_id = uuid.uuid4().hex
    bridge_seq = 0
    bridge_resync_reason: str | None = None
    first_live_facts_emitted = False
    first_live_progress_signaled = False

    def _schedule_bridge_resync(reason: str) -> None:
        nonlocal bridge_session_id, bridge_seq, bridge_resync_reason
        logger.warning(
            "bot_runtime_bridge_resync_scheduled | bot_id=%s | run_id=%s | worker_id=%s | reason=%s | previous_bridge_session_id=%s | previous_bridge_seq=%s",
            bot_id,
            run_id,
            worker_id,
            reason,
            bridge_session_id,
            bridge_seq,
        )
        bridge_session_id = uuid.uuid4().hex
        bridge_seq = 0
        bridge_resync_reason = str(reason or "bridge_resync")

    committed_bootstrap_payload: Dict[str, Any] | None = None

    def _emit_series_bootstrap(*, reason: str | None = None) -> bool:
        nonlocal bridge_seq, bridge_resync_reason
        if committed_bootstrap_payload is None:
            raise RuntimeError(f"worker bootstrap facts were not committed before bridge emit | worker_id={worker_id}")
        bridge_seq += 1
        emitted = _queue_control_event(
            {
                "kind": "series_bootstrap",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "run_seq": int(committed_bootstrap_payload.get("run_seq") or committed_bootstrap_payload.get("seq") or 0),
                "series_key": str(committed_bootstrap_payload.get("series_key") or ""),
                "bridge_session_id": bridge_session_id,
                "bridge_seq": bridge_seq,
                "reason": reason or bridge_resync_reason,
                "facts": list(committed_bootstrap_payload.get("facts") or []),
                "known_at": committed_bootstrap_payload.get("known_at") or utc_now_iso(),
                "event_time": committed_bootstrap_payload.get("event_time") or utc_now_iso(),
            }
        )
        if emitted:
            bridge_resync_reason = None
        return emitted

    stream_stop = threading.Event()
    subscription_token: str | None = None
    emitter_thread: threading.Thread | None = None
    series_key = ""

    def _queue_first_live_progress_signal(*, reason: str, trigger_event: str) -> bool:
        nonlocal first_live_progress_signaled
        if first_live_progress_signaled:
            return True
        payload = {
            "kind": "runtime_facts_started",
            "worker_id": worker_id,
            "symbols": list(symbols),
            "series_key": series_key,
            "reason": str(reason or "subscriber_gap"),
            "trigger_event": str(trigger_event or "runtime_facts_gap_before_live"),
            "bridge_session_id": bridge_session_id,
            "event_time": utc_now_iso(),
            "known_at": utc_now_iso(),
        }
        while not stream_stop.is_set():
            if _queue_control_event(payload, timeout_s=0.5):
                first_live_progress_signaled = True
                return True
            logger.warning(
                "bot_runtime_first_live_progress_signal_retry | bot_id=%s | run_id=%s | worker_id=%s | symbols=%s | reason=%s | trigger_event=%s",
                bot_id,
                run_id,
                worker_id,
                list(symbols),
                payload["reason"],
                payload["trigger_event"],
            )
            time.sleep(0.05)
        return False

    def _runtime_facts_loop() -> None:
        nonlocal bridge_seq, bridge_resync_reason, first_live_facts_emitted, first_live_progress_signaled
        if subscription_token is None:
            return
        while not stream_stop.is_set() or not subscription_queue.empty():
            if bridge_resync_reason:
                if first_live_facts_emitted or first_live_progress_signaled:
                    _queue_control_event(
                        {
                            "kind": "continuity_gap",
                            "worker_id": worker_id,
                            "symbols": list(symbols),
                            "series_key": series_key,
                            "reason": bridge_resync_reason,
                            "trigger_event": "bridge_resync_suppressed_after_live",
                            "bridge_session_id": bridge_session_id,
                            "event_time": utc_now_iso(),
                            "known_at": utc_now_iso(),
                        }
                    )
                    bridge_resync_reason = None
                    continue
                if not _emit_series_bootstrap(reason=bridge_resync_reason):
                    time.sleep(0.01)
                    continue
            try:
                message = subscription_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if not isinstance(message, Mapping):
                continue
            message_type = str(message.get("type") or "").strip().lower()
            if message_type == "gap":
                try:
                    while True:
                        subscription_queue.get_nowait()
                except queue.Empty:
                    pass
                if first_live_facts_emitted:
                    _queue_control_event(
                        {
                            "kind": "continuity_gap",
                            "worker_id": worker_id,
                            "symbols": list(symbols),
                            "series_key": series_key,
                            "reason": str(message.get("reason") or "subscriber_gap"),
                            "trigger_event": str(message.get("event") or "subscriber_gap"),
                            "bridge_session_id": bridge_session_id,
                            "event_time": utc_now_iso(),
                            "known_at": utc_now_iso(),
                        }
                    )
                    continue
                if first_live_progress_signaled:
                    continue
                emitted = _queue_first_live_progress_signal(
                    reason=str(message.get("reason") or "subscriber_gap"),
                    trigger_event=str(message.get("event") or "runtime_facts_gap_before_live"),
                )
                if emitted:
                    continue
                _schedule_bridge_resync(str(message.get("reason") or "subscriber_gap"))
                continue
            if message_type != "facts":
                continue
            facts = message.get("facts") if isinstance(message.get("facts"), list) else []
            if not facts:
                continue
            known_at = message.get("known_at") or utc_now_iso()
            run_seq = int(message.get("run_seq") or message.get("seq") or 0)
            if run_seq <= 0:
                raise RuntimeError(
                    f"runtime live facts missing committed run_seq | worker_id={worker_id} | series_key={series_key or '<missing>'}"
                )
            if not first_live_progress_signaled:
                signaled = _queue_first_live_progress_signal(
                    reason="runtime_facts_observed",
                    trigger_event="facts",
                )
                if not signaled:
                    _schedule_bridge_resync("runtime_facts_observed_before_live")
                    continue
            bridge_seq_local = bridge_seq + 1
            emitted = _queue_worker_event(
                {
                    "kind": "runtime_facts",
                    "worker_id": worker_id,
                    "symbols": list(symbols),
                    "run_seq": run_seq,
                    "series_key": series_key,
                    "bridge_session_id": bridge_session_id,
                    "bridge_seq": bridge_seq_local,
                    "facts": json_safe(list(facts)),
                    "known_at": known_at,
                    "event_time": utc_now_iso(),
                },
                timeout_s=0.02,
            )
            if emitted:
                bridge_seq = bridge_seq_local
                first_live_facts_emitted = True
                first_live_progress_signaled = True
                continue
            if first_live_facts_emitted:
                _queue_control_event(
                    {
                        "kind": "continuity_gap",
                        "worker_id": worker_id,
                        "symbols": list(symbols),
                        "series_key": series_key,
                        "reason": "bridge_queue_backpressure",
                        "trigger_event": "runtime_facts_drop_after_live",
                        "bridge_session_id": bridge_session_id,
                        "event_time": utc_now_iso(),
                        "known_at": utc_now_iso(),
                    }
                )
                continue
            if not first_live_progress_signaled:
                signaled = _queue_first_live_progress_signal(
                    reason="bridge_queue_backpressure",
                    trigger_event="runtime_facts_drop_before_live",
                )
                if signaled:
                    continue
            _schedule_bridge_resync("bridge_queue_backpressure")

    try:
        if not _queue_control_event(
            {
                "kind": "worker_phase",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "phase": BotLifecyclePhase.WARMING_UP_RUNTIME.value,
                "message": "Worker warming runtime state.",
                "event_time": utc_now_iso(),
            }
        ):
            raise RuntimeError(f"worker lifecycle bridge unavailable during warm-up | worker_id={worker_id}")
        runtime.reset_if_finished()
        runtime.warm_up()
        bootstrap_append_outcome = runtime.commit_botlens_fact_payload(
            _build_bootstrap_payload(),
            batch_kind="botlens_runtime_bootstrap_facts",
            dispatch=False,
        )
        if bootstrap_append_outcome is None:
            raise RuntimeError(f"worker bootstrap missing canonical facts | worker_id={worker_id} | symbols={list(symbols)}")
        committed_bootstrap_payload = dict(bootstrap_append_outcome.batch.live_payload)
        if not _emit_series_bootstrap():
            raise RuntimeError(f"worker bootstrap bridge unavailable | worker_id={worker_id}")
        series_key = str(committed_bootstrap_payload.get("series_key") or "").strip()
        if not _queue_control_event(
            {
                "kind": "worker_phase",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "phase": BotLifecyclePhase.RUNTIME_SUBSCRIBING.value,
                "message": "Worker runtime subscribing to live facts stream.",
                "event_time": utc_now_iso(),
            }
        ):
            raise RuntimeError(f"worker lifecycle bridge unavailable during subscription | worker_id={worker_id}")
        subscription_token, subscription_queue = runtime.subscribe(overflow_policy="drop_and_signal")
        emitter_thread = threading.Thread(target=_runtime_facts_loop, name=f"bot-facts-stream-{worker_id}", daemon=True)
        emitter_thread.start()
        runtime.start()
    except Exception as exc:  # noqa: BLE001
        runtime_error.update(_classify_worker_exception(exc))
        runtime_error["message"] = str(exc)
        runtime_error["exception"] = repr(exc)
        runtime_error["exception_type"] = type(exc).__name__
        runtime_error["traceback"] = traceback.format_exc().strip()
    finally:
        stream_stop.set()
        if emitter_thread is not None:
            emitter_thread.join(timeout=1.0)
        if subscription_token is not None:
            runtime.unsubscribe(subscription_token)
    status = str((runtime.snapshot() or {}).get("status") or "").strip().lower() or ("error" if runtime_error else "stopped")
    if runtime_error:
        queued = _queue_control_event(
            {
                "kind": "worker_error",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "error": runtime_error.get("message"),
                "exception": runtime_error.get("exception"),
                "exception_type": runtime_error.get("exception_type"),
                "traceback": runtime_error.get("traceback"),
                "reason_code": runtime_error.get("reason_code"),
                "component": runtime_error.get("component"),
                "operation": runtime_error.get("operation"),
                "path": runtime_error.get("path"),
                "errno": runtime_error.get("errno"),
                "at": utc_now_iso(),
            }
        )
        _queue_control_event(
            {
                "kind": "worker_terminal",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "status": "error",
                "message": f"Worker runtime exited with terminal status error.",
                "event_time": utc_now_iso(),
            }
        )
        if not queued:
            raise RuntimeError(
                f"symbol worker failed | worker_id={worker_id} | symbols={list(symbols)} | error={runtime_error.get('message')}"
            )
        return
    _queue_control_event(
        {
            "kind": "worker_terminal",
            "worker_id": worker_id,
            "symbols": list(symbols),
            "status": status,
            "message": f"Worker runtime exited with terminal status {status}.",
            "event_time": utc_now_iso(),
        }
    )
    if status in {"error", "failed", "crashed", "degraded"}:
        _queue_control_event(
            {
                "kind": "worker_error",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "error": f"Worker runtime exited with terminal status {status}.",
                "reason_code": f"worker_terminal_{status}",
                "at": utc_now_iso(),
            }
        )
        return


def _handle_worker_phase_event(ctx: ContainerStartupContext, event: Mapping[str, Any]) -> None:
    phase = str(event.get("phase") or "").strip()
    message = str(event.get("message") or "").strip() or "Worker startup phase update."
    worker_id = str(event.get("worker_id") or "").strip()
    for symbol in event.get("symbols") or []:
        _set_series_state(
            ctx,
            symbol=str(symbol),
            status="warming_up" if phase == BotLifecyclePhase.WARMING_UP_RUNTIME.value else "awaiting_first_snapshot",
            worker_id=worker_id or None,
            message=message,
        )
    current_runtime_state = runtime_state_value(ctx.runtime_state)
    if phase in {BotLifecyclePhase.WARMING_UP_RUNTIME.value, BotLifecyclePhase.RUNTIME_SUBSCRIBING.value} and current_runtime_state not in {
        None,
        BotLensRuntimeState.INITIALIZING.value,
    }:
        logger.info(
            "bot_runtime_worker_phase_ignored | bot_id=%s | run_id=%s | worker_id=%s | phase=%s | runtime_state=%s | reason=stale_startup_phase",
            ctx.bot_id,
            ctx.run_id,
            worker_id,
            phase,
            current_runtime_state,
        )
        return
    if phase in {BotLifecyclePhase.WARMING_UP_RUNTIME.value, BotLifecyclePhase.RUNTIME_SUBSCRIBING.value}:
        _transition_for_phase(
            ctx,
            phase=phase,
            reason=f"worker_phase:{phase}",
            source_component="runtime_worker",
            observed_at=event.get("event_time"),
        )
        _persist_lifecycle_phase(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            phase=phase,
            owner=LifecycleOwner.RUNTIME.value,
            message=message,
            metadata=_runtime_observability_metadata(ctx),
            telemetry_sender=ctx.telemetry_sender,
            shared_wallet_proxy=ctx.shared_wallet_proxy,
        )


def _handle_worker_terminal_event(ctx: ContainerStartupContext, event: Mapping[str, Any]) -> None:
    worker_id = str(event.get("worker_id") or "").strip()
    status = str(event.get("status") or "").strip().lower()
    if not worker_id or not status:
        return
    ctx.reported_worker_terminal_statuses[worker_id] = status
    for symbol in event.get("symbols") or []:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            continue
        series_status = status if status in {"completed", "stopped", "degraded"} else "failed"
        _set_series_state(
            ctx,
            symbol=normalized_symbol,
            status=series_status,
            worker_id=worker_id or None,
            message=str(event.get("message") or "").strip() or f"Worker reported terminal status {status}.",
        )


def _handle_series_bootstrap_event(
    ctx: ContainerStartupContext,
    event: Mapping[str, Any],
    *,
    telemetry_sender: TelemetryEmitter,
) -> tuple[int, float, int, bool]:
    worker_id = str(event.get("worker_id") or "").strip()
    reason = str(event.get("reason") or "").strip() or None
    observed_at = event.get("known_at") or event.get("event_time") or utc_now_iso()
    admission = startup_bootstrap_admission(runtime_state=ctx.runtime_state)
    if not admission.allowed:
        rejection_reason = f"startup_bootstrap_after_{ctx.runtime_state or 'unknown'}"
        _OBSERVER.event(
            "startup_bootstrap_rejected",
            level=logging.ERROR,
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            runtime_state=admission.runtime_state or ctx.runtime_state,
            worker_id=worker_id or None,
            series_key=normalize_series_key(event.get("series_key")),
            reason=reason or rejection_reason,
            event_time=observed_at,
        )
        if ctx.runtime_state in {
            BotLensRuntimeState.LIVE.value,
            BotLensRuntimeState.DEGRADED.value,
        }:
            _mark_degraded_loop(
                ctx,
                started_at=observed_at,
                reason=f"{rejection_reason}:{reason}" if reason else rejection_reason,
                trigger_event="startup_bootstrap_rejected",
            )
            _record_non_progress_activity(
                ctx,
                observed_at=observed_at,
                reason=rejection_reason,
            )
            _capture_pressure_snapshot(
                ctx,
                trigger="startup_bootstrap_rejected",
                observed_at=observed_at,
                trigger_event="startup_bootstrap_rejected",
            )
            _transition_runtime_state(
                ctx,
                next_state=BotLensRuntimeState.DEGRADED.value,
                reason=rejection_reason,
                source_component="container_runtime",
                observed_at=observed_at,
            )
            _persist_lifecycle_phase(
                bot_id=ctx.bot_id,
                run_id=ctx.run_id,
                phase=BotLifecyclePhase.DEGRADED.value,
                owner=LifecycleOwner.RUNTIME.value,
                message="Rejected startup bootstrap request after runtime had already left startup states.",
                metadata=_runtime_observability_metadata(ctx),
                status=BotLifecycleStatus.DEGRADED.value,
                telemetry_sender=ctx.telemetry_sender,
                shared_wallet_proxy=ctx.shared_wallet_proxy,
            )
        logger.warning(
            "bot_runtime_series_bootstrap_suppressed_after_live | bot_id=%s | run_id=%s | worker_id=%s | series_key=%s | reason=%s",
            ctx.bot_id,
            ctx.run_id,
            worker_id,
            normalize_series_key(event.get("series_key")),
            reason or rejection_reason,
        )
        return 0, 0.0, 0, True
    _transition_runtime_state(
        ctx,
        next_state=BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value,
        reason="startup_bootstrap_completed",
        source_component="runtime_worker",
        observed_at=observed_at,
    )
    run_seq = int(event.get("run_seq") or event.get("seq") or 0)
    if run_seq <= 0:
        raise RuntimeError(
            "startup bootstrap payload is missing committed run_seq "
            f"bot_id={ctx.bot_id} run_id={ctx.run_id} worker_id={worker_id or '<missing>'}"
        )
    telemetry_emit_ms = 0.0
    payload_bytes = 0
    sent = True
    for symbol in ctx.worker_symbols.get(worker_id) or []:
        _set_series_state(
            ctx,
            symbol=symbol,
            status="bootstrapped",
            worker_id=worker_id or None,
            series_key=str(event.get("series_key") or "").strip() or None,
            bootstrap_seq=run_seq,
            message="Worker produced bootstrap fact batch.",
        )
    series_key = normalize_series_key(event.get("series_key"))
    facts = event.get("facts") if isinstance(event.get("facts"), list) else []
    if series_key:
        telemetry_payload = {
            "kind": "botlens_runtime_bootstrap_facts",
            "bot_id": ctx.bot_id,
            "run_id": ctx.run_id,
            "worker_id": worker_id,
            "source_emitter": "container_runtime",
            "source_reason": "bootstrap",
            "run_seq": run_seq,
            "series_key": series_key,
            "bridge_session_id": str(event.get("bridge_session_id") or "").strip() or None,
            "bridge_seq": int(event.get("bridge_seq") or 0),
            "known_at": event.get("known_at") or utc_now_iso(),
            "event_time": event.get("event_time") or utc_now_iso(),
            "facts": list(facts),
            "summary": {},
        }
        telemetry_message = json.dumps(json_safe(telemetry_payload))
        payload_bytes = len(telemetry_message.encode("utf-8"))
        telemetry_payload["summary"]["payload_bytes"] = payload_bytes
        telemetry_started = time.monotonic()
        sent = telemetry_sender.send(telemetry_payload)
        telemetry_emit_ms = max((time.monotonic() - telemetry_started) * 1000.0, 0.0)
        _capture_pressure_snapshot(
            ctx,
            trigger="startup_bootstrap",
            observed_at=observed_at,
            payload_bytes=payload_bytes,
            telemetry_emit_ms=telemetry_emit_ms,
        )
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=BotLifecyclePhase.AWAITING_FIRST_SNAPSHOT.value,
        owner=LifecycleOwner.RUNTIME.value,
        message="Series bootstrap completed; waiting for first live runtime facts.",
        metadata=_runtime_observability_metadata(ctx),
        telemetry_sender=ctx.telemetry_sender,
        shared_wallet_proxy=ctx.shared_wallet_proxy,
    )
    return run_seq, telemetry_emit_ms, payload_bytes, sent


def _handle_runtime_facts_event(
    ctx: ContainerStartupContext,
    event: Mapping[str, Any],
    *,
    telemetry_sender: TelemetryEmitter,
) -> tuple[int, float, int, bool]:
    series_key = normalize_series_key(event.get("series_key"))
    facts = event.get("facts") if isinstance(event.get("facts"), list) else []
    if not series_key or not facts:
        return 0, 0.0, 0, True
    observed_at = event.get("known_at") or event.get("event_time") or utc_now_iso()
    _mark_useful_progress(ctx, observed_at)
    worker_id = str(event.get("worker_id") or "").strip()
    run_seq = int(event.get("run_seq") or event.get("seq") or 0)
    if run_seq <= 0:
        raise RuntimeError(
            "runtime facts payload is missing committed run_seq "
            f"bot_id={ctx.bot_id} run_id={ctx.run_id} worker_id={worker_id or '<missing>'} series_key={series_key}"
        )
    for symbol in ctx.worker_symbols.get(worker_id) or []:
        _set_series_state(
            ctx,
            symbol=symbol,
            status="live",
            worker_id=worker_id or None,
            series_key=series_key,
            message="Series emitted first live runtime facts." if symbol not in ctx.first_snapshot_series else "Series remains live.",
        )
        ctx.first_snapshot_series.add(str(symbol).strip().upper())
    telemetry_payload = {
        "kind": "botlens_runtime_facts",
        "bot_id": ctx.bot_id,
        "run_id": ctx.run_id,
        "worker_id": worker_id,
        "source_emitter": "container_runtime",
        "source_reason": "ingest",
        "run_seq": run_seq,
        "series_key": series_key,
        "bridge_session_id": str(event.get("bridge_session_id") or "").strip() or None,
        "bridge_seq": int(event.get("bridge_seq") or 0),
        "known_at": event.get("known_at") or utc_now_iso(),
        "event_time": event.get("event_time") or utc_now_iso(),
        "facts": list(facts),
        "summary": {},
    }
    telemetry_message = json.dumps(json_safe(telemetry_payload))
    payload_bytes = len(telemetry_message.encode("utf-8"))
    telemetry_payload["summary"]["payload_bytes"] = payload_bytes
    telemetry_started = time.monotonic()
    sent = telemetry_sender.send(telemetry_payload)
    telemetry_emit_ms = max((time.monotonic() - telemetry_started) * 1000.0, 0.0)
    _capture_pressure_snapshot(
        ctx,
        trigger="runtime_facts",
        observed_at=observed_at,
        payload_bytes=payload_bytes,
        telemetry_emit_ms=telemetry_emit_ms,
    )
    if (
        sent
        and ctx.runtime_state == BotLensRuntimeState.DEGRADED.value
        and str(ctx.degraded_reason_code or "").strip().lower() != "worker_error"
    ):
        _clear_degraded_loop(
            ctx,
            cleared_at=observed_at,
            recovery_reason="runtime_facts_resumed",
        )
        _persist_lifecycle_phase(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            phase=BotLifecyclePhase.LIVE.value,
            owner=LifecycleOwner.RUNTIME.value,
            message="Runtime continuity recovered and live facts resumed.",
            metadata=_runtime_observability_metadata(ctx),
            status=BotLifecycleStatus.RUNNING.value,
            telemetry_sender=ctx.telemetry_sender,
            shared_wallet_proxy=ctx.shared_wallet_proxy,
        )
        _transition_runtime_state(
            ctx,
            next_state=BotLensRuntimeState.LIVE.value,
            reason="continuity_recovered:runtime_facts_resumed",
            source_component="runtime_worker",
            observed_at=observed_at,
        )
    _persist_startup_live_if_ready(
        ctx,
        observed_at=observed_at,
        reason="startup_live_ready",
        source_component="runtime_worker",
        message="All planned series emitted first runtime snapshot; bot is live.",
    )
    return run_seq, telemetry_emit_ms, payload_bytes, sent


def _persist_startup_live_if_ready(
    ctx: ContainerStartupContext,
    *,
    observed_at: Any,
    reason: str,
    source_component: str,
    message: str,
) -> bool:
    if len(ctx.first_snapshot_series) == len(ctx.symbols) and not ctx.startup_live_emitted:
        _persist_lifecycle_phase(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            phase=BotLifecyclePhase.LIVE.value,
            owner=LifecycleOwner.RUNTIME.value,
            message=message,
            metadata=_runtime_observability_metadata(ctx),
            status=BotLifecycleStatus.RUNNING.value,
            telemetry_sender=ctx.telemetry_sender,
            shared_wallet_proxy=ctx.shared_wallet_proxy,
        )
        ctx.startup_live_emitted = True
        _transition_runtime_state(
            ctx,
            next_state=BotLensRuntimeState.LIVE.value,
            reason=reason,
            source_component=source_component,
            observed_at=observed_at,
        )
        return True
    return False


def _handle_runtime_facts_started_event(
    ctx: ContainerStartupContext,
    event: Mapping[str, Any],
) -> None:
    if ctx.startup_live_emitted:
        return
    worker_id = str(event.get("worker_id") or "").strip()
    if not worker_id:
        return
    observed_at = event.get("known_at") or event.get("event_time") or utc_now_iso()
    series_key = normalize_series_key(event.get("series_key"))
    worker_symbols = ctx.worker_symbols.get(worker_id)
    if worker_symbols is None:
        event_symbols = event.get("symbols") if isinstance(event.get("symbols"), list) else []
        worker_symbols = [str(symbol).strip().upper() for symbol in event_symbols if str(symbol).strip()]
    if not worker_symbols:
        return
    _mark_useful_progress(ctx, observed_at)
    for symbol in worker_symbols:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            continue
        _set_series_state(
            ctx,
            symbol=normalized_symbol,
            status="awaiting_first_snapshot",
            worker_id=worker_id,
            series_key=series_key or None,
            message="Runtime facts bridge signaled first live progress; reconciling canonical facts.",
        )
    reason = str(event.get("reason") or "subscriber_gap").strip() or "subscriber_gap"
    trigger_event = str(event.get("trigger_event") or "runtime_facts_gap_before_live").strip()
    _capture_pressure_snapshot(
        ctx,
        trigger="runtime_facts_gap_before_live",
        observed_at=observed_at,
        trigger_event=trigger_event,
    )
    logger.warning(
        "bot_runtime_first_facts_gap_reconcile_requested | bot_id=%s | run_id=%s | worker_id=%s | symbols=%s | series_key=%s | reason=%s | trigger_event=%s",
        ctx.bot_id,
        ctx.run_id,
        worker_id,
        list(worker_symbols),
        series_key,
        reason,
        trigger_event,
    )
    reconciled = _reconcile_startup_live_from_canonical_facts(ctx)
    if not reconciled:
        logger.warning(
            "bot_runtime_first_facts_gap_without_canonical_facts | bot_id=%s | run_id=%s | worker_id=%s | symbols=%s | series_key=%s | reason=%s | trigger_event=%s",
            ctx.bot_id,
            ctx.run_id,
            worker_id,
            list(worker_symbols),
            series_key,
            reason,
            trigger_event,
        )


def _pending_first_live_series_by_key(ctx: ContainerStartupContext) -> Dict[str, str]:
    pending: Dict[str, str] = {}
    for symbol in ctx.symbols:
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol or normalized_symbol in ctx.first_snapshot_series:
            continue
        state = dict(ctx.series_states.get(normalized_symbol) or {})
        series_key = normalize_series_key(state.get("series_key"))
        if not series_key:
            continue
        if int(state.get("bootstrap_seq") or 0) <= 0:
            continue
        pending[series_key] = normalized_symbol
    return pending


def _load_canonical_first_live_observations(
    ctx: ContainerStartupContext,
    *,
    pending_series_by_key: Mapping[str, str],
) -> Dict[str, Dict[str, Any]]:
    series_keys = [key for key in pending_series_by_key.keys() if key]
    if not series_keys:
        return {}
    live_fact_conditions = []
    for series_key, symbol in pending_series_by_key.items():
        state = dict(ctx.series_states.get(str(symbol or "").strip().upper()) or {})
        bootstrap_seq = int(state.get("bootstrap_seq") or 0)
        if bootstrap_seq <= 0:
            continue
        live_fact_conditions.append(
            and_(
                BotRunEventRecord.series_key == series_key,
                BotRunEventRecord.seq > bootstrap_seq,
            )
        )
    if not live_fact_conditions:
        return {}
    try:
        with db.session() as session:
            rows = session.execute(
                select(
                    BotRunEventRecord.series_key.label("series_key"),
                    func.min(BotRunEventRecord.seq).label("first_seq"),
                    func.max(BotRunEventRecord.seq).label("latest_seq"),
                    func.max(BotRunEventRecord.known_at).label("known_at"),
                    func.max(BotRunEventRecord.event_time).label("event_time"),
                    func.max(BotRunEventRecord.symbol).label("symbol"),
                    func.max(BotRunEventRecord.timeframe).label("timeframe"),
                )
                .where(BotRunEventRecord.bot_id == ctx.bot_id)
                .where(BotRunEventRecord.run_id == ctx.run_id)
                .where(or_(*live_fact_conditions))
                .where(BotRunEventRecord.event_name.in_(_CANONICAL_FIRST_LIVE_EVENT_NAMES))
                .group_by(BotRunEventRecord.series_key)
            ).all()
    except Exception as exc:  # noqa: BLE001 - canonical reconciliation is secondary to bridge delivery.
        logger.warning(
            "bot_runtime_canonical_first_live_reconcile_failed | bot_id=%s | run_id=%s | pending_series=%s | error=%s",
            ctx.bot_id,
            ctx.run_id,
            series_keys,
            exc,
            exc_info=True,
        )
        return {}

    observations: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        series_key = normalize_series_key(row.series_key)
        if not series_key:
            continue
        observations[series_key] = {
            "series_key": series_key,
            "symbol": str(row.symbol or pending_series_by_key.get(series_key) or "").strip().upper() or None,
            "timeframe": str(row.timeframe or "").strip() or None,
            "first_seq": int(row.first_seq or 0),
            "latest_seq": int(row.latest_seq or 0),
            "known_at": _format_utc_iso(row.known_at),
            "event_time": _format_utc_iso(row.event_time or row.known_at),
            "event_names": list(_CANONICAL_FIRST_LIVE_EVENT_NAMES),
        }
    return observations


def _apply_canonical_first_live_observations(
    ctx: ContainerStartupContext,
    observations: Mapping[str, Mapping[str, Any]],
) -> bool:
    if ctx.startup_live_emitted or ctx.runtime_state != BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value:
        return False
    pending_series_by_key = _pending_first_live_series_by_key(ctx)
    if not pending_series_by_key:
        return False

    newly_live: List[Dict[str, Any]] = []
    latest_observed_at = utc_now_iso()
    for series_key, symbol in pending_series_by_key.items():
        observation = dict(observations.get(series_key) or {})
        if not observation:
            continue
        observed_at = _format_utc_iso(observation.get("known_at") or observation.get("event_time"))
        latest_observed_at = observed_at
        _mark_useful_progress(ctx, observed_at)
        _set_series_state(
            ctx,
            symbol=symbol,
            status="live",
            series_key=series_key,
            message="Series committed first canonical runtime facts.",
        )
        ctx.first_snapshot_series.add(symbol)
        newly_live.append(
            {
                "symbol": symbol,
                "series_key": series_key,
                "first_seq": int(observation.get("first_seq") or 0),
                "latest_seq": int(observation.get("latest_seq") or observation.get("first_seq") or 0),
                "known_at": observed_at,
            }
        )

    if not newly_live:
        return False

    _capture_pressure_snapshot(
        ctx,
        trigger="canonical_runtime_facts_observed",
        observed_at=latest_observed_at,
        trigger_event="canonical_first_live_observed",
    )
    ready_for_live = len(ctx.first_snapshot_series) == len(ctx.symbols) and not ctx.startup_live_emitted
    log_fn = logger.warning if ready_for_live else logger.info
    log_fn(
        "bot_runtime_live_reconciled_from_canonical_facts | bot_id=%s | run_id=%s | newly_live_series=%s | live_series=%s | total_series=%s | reason=bridge_first_live_signal_missing",
        ctx.bot_id,
        ctx.run_id,
        newly_live,
        len(ctx.first_snapshot_series),
        len(ctx.symbols),
    )
    _persist_startup_live_if_ready(
        ctx,
        observed_at=latest_observed_at,
        reason="startup_live_ready:canonical_runtime_facts_observed",
        source_component="canonical_runtime_events",
        message="All planned series committed first canonical runtime facts; bot is live.",
    )
    return True


def _reconcile_startup_live_from_canonical_facts(ctx: ContainerStartupContext) -> bool:
    if ctx.startup_live_emitted or ctx.runtime_state != BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value:
        return False
    pending_series_by_key = _pending_first_live_series_by_key(ctx)
    if not pending_series_by_key:
        return False
    observations = _load_canonical_first_live_observations(
        ctx,
        pending_series_by_key=pending_series_by_key,
    )
    if not observations:
        return False
    return _apply_canonical_first_live_observations(ctx, observations)


def _drain_parent_event_queue(
    *,
    event_queue: "mp.Queue[Dict[str, Any]] | None",
    handle_event: Any,
    max_events: int | None = None,
) -> Dict[str, int]:
    drained_counts: Dict[str, int] = {}
    drained_total = 0
    while True:
        if max_events is not None and drained_total >= int(max_events):
            break
        if event_queue is None:
            break
        try:
            event = event_queue.get_nowait()
        except queue.Empty:
            break
        worker_id = str(event.get("worker_id") or "").strip()
        handle_event(worker_id, event)
        drained_total += 1
        if worker_id:
            drained_counts[worker_id] = int(drained_counts.get(worker_id, 0)) + 1
    return drained_counts


def _handle_worker_error(
    ctx: ContainerStartupContext,
    worker_id: str,
    *,
    error: str | None,
    observed_at: Any = None,
    exit_code: int | None = None,
    exception_type: str | None = None,
    traceback_text: str | None = None,
    stderr_tail: str | None = None,
    component: str | None = None,
    operation: str | None = None,
    path: str | None = None,
    errno: int | None = None,
    reason_code: str | None = None,
) -> None:
    for symbol in ctx.worker_symbols.get(worker_id) or []:
        normalized_symbol = str(symbol).strip().upper()
        ctx.degraded_symbols.add(normalized_symbol)
        _set_series_state(
            ctx,
            symbol=normalized_symbol,
            status="failed",
            worker_id=worker_id or None,
            error=error or "worker_error",
            message="Worker reported startup/runtime failure.",
        )
    remaining_live_workers = any(
        candidate_id != worker_id and getattr(proc, "exitcode", None) is None
        for candidate_id, proc in ctx.children.items()
    )
    # Worker failures stay in startup semantics until the run has actually reached
    # live. Other workers still existing, or some early snapshots existing, do not
    # mean startup completed.
    partial_runtime_alive = bool(ctx.startup_live_emitted)
    failure_phase = BotLifecyclePhase.DEGRADED.value if partial_runtime_alive else BotLifecyclePhase.STARTUP_FAILED.value
    failure_status = BotLifecycleStatus.DEGRADED.value if partial_runtime_alive else BotLifecycleStatus.STARTUP_FAILED.value
    if worker_id in ctx.reported_worker_failures:
        return
    ctx.reported_worker_failures.add(worker_id)
    failure_message = error or (f"Worker {worker_id} exited with code {exit_code}" if exit_code is not None else "Worker failure reported by runtime container.")
    failure_at = observed_at or utc_now_iso()
    if partial_runtime_alive:
        _mark_degraded_loop(
            ctx,
            started_at=failure_at,
            reason="worker_error",
            trigger_event="worker_error",
        )
        _record_non_progress_activity(
            ctx,
            observed_at=failure_at,
            reason="worker_error",
        )
        _capture_pressure_snapshot(
            ctx,
            trigger="worker_error",
            observed_at=failure_at,
            trigger_event="worker_error",
        )
        _transition_runtime_state(
            ctx,
            next_state=BotLensRuntimeState.DEGRADED.value,
            reason="worker_error",
            source_component="runtime_worker",
            observed_at=failure_at,
        )
    else:
        ctx.terminal_actor = "runtime_worker"
        ctx.terminal_status_source = "worker_error"
        ctx.terminal_status_value = failure_status
        ctx.terminal_reason_text = failure_message
        _transition_runtime_state(
            ctx,
            next_state=BotLensRuntimeState.STARTUP_FAILED.value,
            reason="startup_failed:worker_error",
            source_component="runtime_worker",
            observed_at=failure_at,
        )
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=failure_phase,
        owner=LifecycleOwner.RUNTIME.value,
        message=failure_message,
        metadata=_runtime_observability_metadata(ctx),
        failure=_worker_failure_payload(
            ctx=ctx,
            worker_id=worker_id,
            phase=failure_phase,
            message=failure_message,
            exit_code=exit_code,
            exception_type=exception_type,
            traceback_text=traceback_text,
            stderr_tail=stderr_tail,
            component=component,
            operation=operation,
            path=path,
            errno=errno,
            reason_code=reason_code,
        ),
        status=failure_status,
        telemetry_sender=ctx.telemetry_sender,
        shared_wallet_proxy=ctx.shared_wallet_proxy,
    )
    logger.error(
        "bot_runtime_worker_failure_recorded | bot_id=%s | run_id=%s | worker_id=%s | symbols=%s | phase=%s | status=%s | exit_code=%s | exception_type=%s | message=%s",
        ctx.bot_id,
        ctx.run_id,
        worker_id,
        ctx.worker_symbols.get(worker_id) or [],
        failure_phase,
        failure_status,
        exit_code,
        exception_type,
        failure_message,
    )


def _final_terminal_reason(
    ctx: ContainerStartupContext,
    *,
    final_phase: str,
    final_status: str,
) -> tuple[str, str, str]:
    expected_workers = len(ctx.worker_symbols)
    reported_workers = len(ctx.reported_worker_terminal_statuses)
    if final_status == BotLifecycleStatus.COMPLETED.value:
        return (
            "runtime_worker",
            "worker_terminal_statuses",
            f"All {reported_workers}/{expected_workers} workers reported completed.",
        )
    if final_status == BotLifecycleStatus.STOPPED.value:
        return (
            "runtime_worker",
            "worker_terminal_statuses",
            f"One or more workers reported stopped ({reported_workers}/{expected_workers} workers reported terminal state).",
        )
    if reported_workers < expected_workers:
        if final_phase == BotLifecyclePhase.CRASHED.value:
            return (
                "container_runtime",
                "supervision_guard",
                f"Workers exited without explicit terminal status ({reported_workers}/{expected_workers} reported).",
            )
        return (
            "container_runtime",
            "supervision_guard",
            f"Startup ended before all workers reported explicit terminal status ({reported_workers}/{expected_workers} reported).",
        )
    if final_status == BotLifecycleStatus.TELEMETRY_DEGRADED.value:
        return (
            "telemetry_sender",
            "runtime_guard",
            "Telemetry transport degraded after worker terminal reports.",
        )
    if final_status == BotLifecycleStatus.DEGRADED.value:
        return (
            "runtime_worker",
            "worker_terminal_statuses",
            "At least one worker reported degraded terminal state.",
        )
    if final_status == BotLifecycleStatus.STARTUP_FAILED.value:
        return (
            "runtime_worker",
            "worker_terminal_statuses",
            "Runtime never reached a complete live terminal outcome.",
        )
    if final_status == BotLifecycleStatus.CRASHED.value:
        return (
            "runtime_worker",
            "worker_terminal_statuses",
            "One or more workers reported crash/failure terminal state.",
        )
    return (
        ctx.terminal_actor or "container_runtime",
        ctx.terminal_status_source or "supervision_guard",
        ctx.terminal_reason_text or f"Runtime ended with terminal status {final_status}.",
    )


def supervise_startup_and_runtime(ctx: ContainerStartupContext) -> None:
    run_seq = 0
    telemetry_sender = ctx.telemetry_sender
    owns_telemetry_sender = False
    if telemetry_sender is None:
        telemetry_url = str(_TELEMETRY_SETTINGS.ws_url or "").strip()
        telemetry_sender = TelemetryEmitter(
            telemetry_url,
            queue_max=_TELEMETRY_EMIT_QUEUE_MAX,
            queue_timeout_ms=_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS,
            retry_ms=_TELEMETRY_EMIT_RETRY_MS,
        )
        ctx.telemetry_sender = telemetry_sender
        owns_telemetry_sender = True
    telemetry_degraded = False
    event_poll_ms = _TELEMETRY_SETTINGS.event_poll_ms
    last_canonical_live_reconcile_at = 0.0
    try:
        try:
            while ctx.children:
                loop_started = time.monotonic()
                run_seq = 0
                queue_drain_ms = 0.0
                worker_reconcile_ms = 0.0
                telemetry_emit_ms = 0.0
                payload_bytes = 0
                emitted_events_in_cycle = 0
                cadence_mode = "event_driven"
                queue_drain_started = time.monotonic()

                def _handle_parent_queue_event(worker_id: str, event: Mapping[str, Any]) -> None:
                    nonlocal run_seq, telemetry_emit_ms, payload_bytes, telemetry_degraded, emitted_events_in_cycle
                    kind = str(event.get("kind") or "").strip().lower()
                    if kind == "worker_phase":
                        _handle_worker_phase_event(ctx, event)
                        return
                    if kind == "worker_terminal":
                        _handle_worker_terminal_event(ctx, event)
                        emitted_events_in_cycle += 1
                        return
                    if kind == "continuity_gap":
                        _handle_continuity_gap_event(ctx, event)
                        emitted_events_in_cycle += 1
                        return
                    if kind == "runtime_facts_started":
                        _handle_runtime_facts_started_event(ctx, event)
                        emitted_events_in_cycle += 1
                        return
                    if kind == "series_bootstrap":
                        run_seq, emitted_ms, event_payload_bytes, sent = _handle_series_bootstrap_event(
                            ctx,
                            event,
                            telemetry_sender=telemetry_sender,
                        )
                        telemetry_emit_ms += emitted_ms
                        payload_bytes = event_payload_bytes
                        if not sent:
                            telemetry_degraded = True
                            if ctx.runtime_state in {
                                BotLensRuntimeState.LIVE.value,
                                BotLensRuntimeState.DEGRADED.value,
                            }:
                                observed_at = event.get("known_at") or event.get("event_time") or utc_now_iso()
                                _mark_degraded_loop(
                                    ctx,
                                    started_at=observed_at,
                                    reason="telemetry_transport_send_failed",
                                    trigger_event="series_bootstrap_send_failed",
                                )
                                _record_non_progress_activity(
                                    ctx,
                                    observed_at=observed_at,
                                    reason="telemetry_transport_send_failed",
                                )
                                _capture_pressure_snapshot(
                                    ctx,
                                    trigger="telemetry_send_failed",
                                    observed_at=observed_at,
                                    payload_bytes=event_payload_bytes,
                                    telemetry_emit_ms=emitted_ms,
                                    trigger_event="series_bootstrap_send_failed",
                                )
                                _transition_runtime_state(
                                    ctx,
                                    next_state=BotLensRuntimeState.DEGRADED.value,
                                    reason="telemetry_transport_send_failed",
                                    source_component="telemetry_sender",
                                    observed_at=observed_at,
                                )
                        emitted_events_in_cycle += 1
                        return
                    if kind == "runtime_facts":
                        run_seq, emitted_ms, event_payload_bytes, sent = _handle_runtime_facts_event(
                            ctx,
                            event,
                            telemetry_sender=telemetry_sender,
                        )
                        telemetry_emit_ms += emitted_ms
                        payload_bytes = event_payload_bytes
                        if not sent:
                            telemetry_degraded = True
                            if ctx.runtime_state in {
                                BotLensRuntimeState.LIVE.value,
                                BotLensRuntimeState.DEGRADED.value,
                            }:
                                observed_at = event.get("known_at") or event.get("event_time") or utc_now_iso()
                                _mark_degraded_loop(
                                    ctx,
                                    started_at=observed_at,
                                    reason="telemetry_transport_send_failed",
                                    trigger_event="runtime_facts_send_failed",
                                )
                                _record_non_progress_activity(
                                    ctx,
                                    observed_at=observed_at,
                                    reason="telemetry_transport_send_failed",
                                )
                                _capture_pressure_snapshot(
                                    ctx,
                                    trigger="telemetry_send_failed",
                                    observed_at=observed_at,
                                    payload_bytes=event_payload_bytes,
                                    telemetry_emit_ms=emitted_ms,
                                    trigger_event="runtime_facts_send_failed",
                                )
                                _transition_runtime_state(
                                    ctx,
                                    next_state=BotLensRuntimeState.DEGRADED.value,
                                    reason="telemetry_transport_send_failed",
                                    source_component="telemetry_sender",
                                    observed_at=observed_at,
                                )
                        else:
                            if (
                                telemetry_degraded
                                and not bool(ctx.latest_pressure_snapshot.get("telemetry_backpressure_active"))
                                and not bool(ctx.latest_pressure_snapshot.get("telemetry_retry_pending"))
                            ):
                                telemetry_degraded = False
                                ctx.telemetry_degraded_emitted = False
                        emitted_events_in_cycle += 1
                        return
                    if kind == "worker_error":
                        _handle_worker_error(
                            ctx,
                            worker_id,
                            error=str(event.get("error") or "").strip() or None,
                            observed_at=event.get("at") or event.get("event_time"),
                            exception_type=str(event.get("exception_type") or "").strip() or None,
                            traceback_text=str(event.get("traceback") or "").strip() or None,
                            component=str(event.get("component") or "").strip() or None,
                            operation=str(event.get("operation") or "").strip() or None,
                            path=str(event.get("path") or "").strip() or None,
                            errno=int(event.get("errno")) if event.get("errno") is not None else None,
                            reason_code=str(event.get("reason_code") or "").strip() or None,
                        )

                control_drained_counts = _drain_parent_event_queue(
                    event_queue=ctx.parent_control_queue,
                    handle_event=_handle_parent_queue_event,
                )
                drained_counts = _drain_parent_event_queue(
                    event_queue=ctx.parent_event_queue,
                    handle_event=_handle_parent_queue_event,
                    max_events=max(16, len(ctx.worker_symbols) * 8),
                )
                for worker_id, count in control_drained_counts.items():
                    drained_counts[worker_id] = int(drained_counts.get(worker_id, 0)) + int(count)
                queue_drain_ms = max((time.monotonic() - queue_drain_started) * 1000.0, 0.0)

                worker_reconcile_started = time.monotonic()
                for worker_id, proc in list(ctx.children.items()):
                    if proc.exitcode is None:
                        continue
                    if proc.exitcode != 0:
                        _handle_worker_error(
                            ctx,
                            worker_id,
                            error=f"Worker {worker_id} exited with code {proc.exitcode}",
                            observed_at=utc_now_iso(),
                            exit_code=proc.exitcode,
                        )
                    del ctx.children[worker_id]
                worker_reconcile_ms = max((time.monotonic() - worker_reconcile_started) * 1000.0, 0.0)

                if (
                    ctx.runtime_state == BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value
                    and not ctx.startup_live_emitted
                ):
                    now_monotonic = time.monotonic()
                    if now_monotonic - last_canonical_live_reconcile_at >= _CANONICAL_FIRST_LIVE_RECONCILE_INTERVAL_S:
                        last_canonical_live_reconcile_at = now_monotonic
                        _reconcile_startup_live_from_canonical_facts(ctx)

                if ctx.runtime_state == BotLensRuntimeState.STARTUP_FAILED.value:
                    status = BotLifecycleStatus.STARTUP_FAILED.value
                elif ctx.runtime_state == BotLensRuntimeState.CRASHED.value:
                    status = BotLifecycleStatus.CRASHED.value
                elif ctx.children:
                    status = BotLifecycleStatus.RUNNING.value
                elif ctx.runtime_state == BotLensRuntimeState.DEGRADED.value:
                    status = (
                        BotLifecycleStatus.TELEMETRY_DEGRADED.value
                        if telemetry_degraded
                        else BotLifecycleStatus.DEGRADED.value
                    )
                else:
                    status = BotLifecycleStatus.STOPPED.value
                if ctx.runtime_state == BotLensRuntimeState.DEGRADED.value and status in {
                    BotLifecycleStatus.RUNNING.value,
                    BotLifecycleStatus.STOPPED.value,
                }:
                    status = (
                        BotLifecycleStatus.TELEMETRY_DEGRADED.value
                        if telemetry_degraded
                        else BotLifecycleStatus.DEGRADED.value
                    )
                if telemetry_degraded and ctx.runtime_state == BotLensRuntimeState.DEGRADED.value:
                    _capture_pressure_snapshot(
                        ctx,
                        trigger="telemetry_degraded",
                        observed_at=utc_now_iso(),
                        trigger_event="telemetry_transport_degraded",
                    )
                    _transition_for_phase(
                        ctx,
                        phase=BotLifecyclePhase.TELEMETRY_DEGRADED.value,
                        reason="telemetry_transport_degraded",
                        source_component="telemetry_sender",
                        observed_at=ctx.latest_pressure_snapshot.get("captured_at"),
                    )
                    if not ctx.telemetry_degraded_emitted:
                        ctx.telemetry_degraded_emitted = True
                        _persist_lifecycle_phase(
                            bot_id=ctx.bot_id,
                            run_id=ctx.run_id,
                            phase=BotLifecyclePhase.TELEMETRY_DEGRADED.value,
                            owner=LifecycleOwner.RUNTIME.value,
                            message="Telemetry transport degraded after live progress stopped cleanly advancing.",
                            metadata=_runtime_observability_metadata(ctx),
                            status=BotLifecycleStatus.TELEMETRY_DEGRADED.value,
                            telemetry_sender=ctx.telemetry_sender,
                            shared_wallet_proxy=ctx.shared_wallet_proxy,
                        )
                sleep_for = 0.0 if not ctx.children else max((event_poll_ms / 1000.0) - (time.monotonic() - loop_started), 0.005)
                if sleep_for > 0:
                    time.sleep(sleep_for)
        except Exception as exc:  # noqa: BLE001
            final_phase = BotLifecyclePhase.CRASHED.value if ctx.startup_live_emitted else BotLifecyclePhase.STARTUP_FAILED.value
            final_status = BotLifecycleStatus.CRASHED.value if ctx.startup_live_emitted else BotLifecycleStatus.STARTUP_FAILED.value
            ctx.terminal_actor = "container_exception"
            ctx.terminal_status_source = "container_exception"
            ctx.terminal_status_value = final_status
            ctx.terminal_reason_text = str(exc)
            _transition_for_phase(
                ctx,
                phase=final_phase,
                reason="container_supervision_exception",
                source_component="container_runtime",
                observed_at=utc_now_iso(),
            )
            _persist_lifecycle_phase(
                bot_id=ctx.bot_id,
                run_id=ctx.run_id,
                phase=final_phase,
                owner=LifecycleOwner.CONTAINER.value,
                message=str(exc),
                metadata=_runtime_observability_metadata(ctx),
                failure=build_failure_payload(
                    phase=final_phase,
                    message=str(exc),
                    error_type=type(exc).__name__,
                    type="container_exception",
                    reason_code="container_supervision_exception",
                    owner=LifecycleOwner.CONTAINER.value,
                    exception_type=type(exc).__name__,
                    traceback=traceback.format_exc().strip(),
                ),
                status=final_status,
                telemetry_sender=telemetry_sender,
                shared_wallet_proxy=ctx.shared_wallet_proxy,
            )
            raise
        finally:
            for proc in ctx.children.values():
                if proc.is_alive():
                    proc.terminate()
                proc.join(timeout=0.5)

        final_phase, final_status = terminal_status_after_supervision(
            startup_live_emitted=ctx.startup_live_emitted,
            degraded_symbols_count=len(ctx.degraded_symbols),
            telemetry_degraded=telemetry_degraded,
            expected_worker_count=len(ctx.worker_symbols),
            worker_terminal_statuses=ctx.reported_worker_terminal_statuses,
        )
        try:
            finalize_run_artifact_bundle_from_workers(
                bot_id=ctx.bot_id,
                run_id=ctx.run_id,
                config=ctx.runtime_bot_config,
                runtime_status=final_status,
            )
        except Exception as exc:  # noqa: BLE001
            failure_phase = BotLifecyclePhase.CRASHED.value if ctx.startup_live_emitted else BotLifecyclePhase.STARTUP_FAILED.value
            failure_status = BotLifecycleStatus.CRASHED.value if ctx.startup_live_emitted else BotLifecycleStatus.STARTUP_FAILED.value
            ctx.terminal_actor = "report_artifacts"
            ctx.terminal_status_source = "report_artifacts_finalize"
            ctx.terminal_status_value = failure_status
            ctx.terminal_reason_text = str(exc)
            _persist_lifecycle_phase(
                bot_id=ctx.bot_id,
                run_id=ctx.run_id,
                phase=failure_phase,
                owner=LifecycleOwner.CONTAINER.value,
                message=str(exc),
                metadata=_runtime_observability_metadata(ctx),
                failure=build_failure_payload(
                    phase=failure_phase,
                    message=str(exc),
                    error_type=type(exc).__name__,
                    type="artifact_finalize_exception",
                    reason_code="report_artifacts_finalize_failed",
                    owner=LifecycleOwner.CONTAINER.value,
                    component="report_artifacts",
                    operation="run_finalize",
                    exception_type=type(exc).__name__,
                    traceback=traceback.format_exc().strip(),
                ),
                status=failure_status,
                telemetry_sender=telemetry_sender,
                shared_wallet_proxy=ctx.shared_wallet_proxy,
            )
            raise
        terminal_actor, terminal_source, terminal_reason = _final_terminal_reason(
            ctx,
            final_phase=final_phase,
            final_status=final_status,
        )
        ctx.terminal_actor = terminal_actor
        ctx.terminal_status_source = terminal_source
        ctx.terminal_status_value = final_status
        ctx.terminal_reason_text = terminal_reason
        _transition_for_phase(
            ctx,
            phase=final_phase,
            reason="container_supervision_completed",
            source_component="container_runtime",
            observed_at=utc_now_iso(),
        )
        _persist_lifecycle_phase(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            phase=final_phase,
            owner=LifecycleOwner.CONTAINER.value,
            message=terminal_reason,
            metadata=_runtime_observability_metadata(ctx),
            status=final_status,
            telemetry_sender=telemetry_sender,
            shared_wallet_proxy=ctx.shared_wallet_proxy,
        )
    finally:
        try:
            if owns_telemetry_sender:
                telemetry_sender.close()
        finally:
            # Final lifecycle persistence still sequences against shared_wallet_proxy.
            ctx.manager.shutdown()


def main() -> int:
    _configure_logging()
    bot_id = str(_BOT_RUNTIME_SETTINGS.bot_id or "").strip()
    if not bot_id:
        raise RuntimeError("QT_BOT_RUNTIME_BOT_ID is required")

    telemetry_url = str(_TELEMETRY_SETTINGS.ws_url or "").strip()
    telemetry_sender = TelemetryEmitter(
        telemetry_url,
        queue_max=_TELEMETRY_EMIT_QUEUE_MAX,
        queue_timeout_ms=_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS,
        retry_ms=_TELEMETRY_EMIT_RETRY_MS,
    )
    try:
        ctx = load_container_startup_context(bot_id, telemetry_sender=telemetry_sender)
        logger.info("bot_runtime_run_started | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)
        spawn_workers(ctx)
        start_observability_exporter()
        supervise_startup_and_runtime(ctx)
        return 0
    finally:
        stop_observability_exporter()
        telemetry_sender.close()


if __name__ == "__main__":
    raise SystemExit(main())
