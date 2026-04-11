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
from engines.bot_runtime.core.runtime_events import RuntimeEventName, build_correlation_id, new_runtime_event
from portal.backend.db.session import db
from portal.backend.service.bots.container_runtime_projection import (
    coerce_float,
    json_safe,
    utc_now_iso,
)
from portal.backend.service.bots.container_runtime_telemetry import TelemetryEmitter
from portal.backend.service.bots.container_runtime_telemetry import emit_telemetry_ephemeral_message
from portal.backend.service.bots.botlens_contract import normalize_series_key
from portal.backend.service.bots.runtime_dependencies import build_bot_runtime_deps
from portal.backend.service.bots.startup_lifecycle import (
    BotLifecyclePhase,
    BotLifecycleStatus,
    LifecycleOwner,
    build_failure_payload,
    build_series_progress_metadata,
    lifecycle_checkpoint_payload,
    terminal_status_after_supervision,
)
from portal.backend.service.bots.startup_validation import validate_wallet_config
from portal.backend.service.bots.strategy_loader import StrategyLoader
from portal.backend.service.storage.storage import (
    load_bots,
    record_bot_run_lifecycle_checkpoint,
    record_bot_run_step,
    update_bot_runtime_status,
)

logger = logging.getLogger(__name__)
_TERMINAL_STATUSES = {"completed", "stopped", "error", "failed", "crashed"}
_MAX_SYMBOL_WORKERS = 8
_VIEW_STATE_SCHEMA_VERSION = 1
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
) -> Dict[str, Any]:
    runtime_events = manager.list()
    init_event = new_runtime_event(
        run_id=str(run_id),
        bot_id=str(bot_id),
        strategy_id="__runtime__",
        symbol=None,
        timeframe=None,
        bar_ts=None,
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(
            run_id=str(run_id),
            symbol=None,
            timeframe=None,
            bar_ts=None,
        ),
        payload={"balances": dict(balances), "source": "run_start"},
    )
    serialized_init = init_event.serialize()
    serialized_init["seq"] = 0
    runtime_events.append(serialized_init)
    return {
        "runtime_events": runtime_events,
        "runtime_event_seq": manager.Value("i", 0),
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
    parent_event_queue: "mp.Queue[Dict[str, Any]] | None" = None
    children: Dict[str, mp.Process] = field(default_factory=dict)
    worker_symbols: Dict[str, List[str]] = field(default_factory=dict)
    degraded_symbols: set[str] = field(default_factory=set)
    series_states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    workers_spawned: int = 0
    startup_live_emitted: bool = False
    first_snapshot_series: set[str] = field(default_factory=set)
    reported_worker_failures: set[str] = field(default_factory=set)


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
) -> Dict[str, Any]:
    checkpoint = lifecycle_checkpoint_payload(
        bot_id=bot_id,
        run_id=run_id,
        phase=phase,
        owner=owner,
        message=message,
        metadata=metadata,
        failure=failure,
        status=status,
    )
    lifecycle_state = record_bot_run_lifecycle_checkpoint(checkpoint)
    resolved_status = str(lifecycle_state.get("status") or checkpoint["status"]).strip()
    update_bot_runtime_status(
        bot_id=bot_id,
        run_id=run_id,
        status=resolved_status,
        telemetry_degraded=resolved_status == BotLifecycleStatus.TELEMETRY_DEGRADED.value,
    )
    projection_refresh_delivered = _notify_backend_lifecycle_event(
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
        }
    )
    log_fn = logger.info if phase in _INFO_LIFECYCLE_PHASES or resolved_status in _INFO_LIFECYCLE_STATUSES else logger.debug
    log_fn(
        "bot_runtime_lifecycle_checkpoint_persisted | bot_id=%s | run_id=%s | phase=%s | owner=%s | status=%s | projection_refresh_delivered=%s | message=%s",
        bot_id,
        run_id,
        phase,
        owner,
        resolved_status,
        projection_refresh_delivered,
        message,
    )
    return lifecycle_state


def _notify_backend_lifecycle_event(*, lifecycle_state: Mapping[str, Any]) -> bool:
    telemetry_url = str(_TELEMETRY_SETTINGS.ws_url or "").strip()
    bot_id = str(lifecycle_state.get("bot_id") or "").strip()
    run_id = str(lifecycle_state.get("run_id") or "").strip()
    phase = str(lifecycle_state.get("phase") or "").strip()
    status = str(lifecycle_state.get("status") or "").strip()
    if not telemetry_url:
        logger.warning(
            "bot_runtime_projection_refresh_skipped | bot_id=%s | run_id=%s | phase=%s | status=%s | reason=telemetry_url_missing",
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
    delivered = emit_telemetry_ephemeral_message(telemetry_url, json.dumps(json_safe(payload)))
    if not delivered:
        logger.warning(
            "bot_runtime_projection_refresh_delivery_failed | bot_id=%s | run_id=%s | phase=%s | status=%s | telemetry_url=%s",
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
) -> Dict[str, Any]:
    symbols = [str(symbol).strip().upper() for symbol in (ctx.worker_symbols.get(worker_id) or []) if str(symbol).strip()]
    failure_type = "worker_exit" if exit_code is not None else "worker_exception" if exception_type or traceback_text else "worker_failure"
    reason_code = (
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
        reason_code=reason_code,
        owner=LifecycleOwner.RUNTIME.value,
        worker_id=worker_id,
        symbol=symbols[0] if len(symbols) == 1 else None,
        exit_code=exit_code,
        stderr_tail=stderr_tail,
        exception_type=exception_type,
        traceback=traceback_text.strip() if traceback_text else None,
        symbols=symbols or None,
    )


def _set_series_state(
    ctx: ContainerStartupContext,
    *,
    symbol: str,
    status: str,
    worker_id: str | None = None,
    message: str | None = None,
    series_key: str | None = None,
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
    if error:
        current["error"] = error
    current["updated_at"] = utc_now_iso()
    ctx.series_states[normalized_symbol] = current


def _parent_event_queue_maxsize(*, worker_count: int) -> int:
    per_worker_capacity = max(8, int(_TELEMETRY_EMIT_QUEUE_MAX or 0))
    return per_worker_capacity * max(int(worker_count or 0), 1)


def load_container_startup_context(bot_id: str) -> ContainerStartupContext:
    run_id = _resolve_backend_run_id(bot_id)
    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.CONTAINER_BOOTING.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Container process booting with backend-owned startup contract.",
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
    )

    runtime_bot_config = _materialize_bot_config(bot)
    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.CLAIMING_RUN.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Container claimed backend-owned run_id.",
        metadata={"run_id": run_id},
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
    )
    all_symbols = _load_strategy_symbols(strategy_id)
    max_symbols = _MAX_SYMBOLS_PER_STRATEGY
    if len(all_symbols) > max_symbols:
        raise RuntimeError(
            f"Strategy {strategy_id} has {len(all_symbols)} symbols but runtime limit is {max_symbols}. "
            "Reduce symbols or increase BOT_MAX_SYMBOLS_PER_STRATEGY."
        )

    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.PREPARING_WALLET.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Preparing shared wallet for worker processes.",
    )
    wallet_config = validate_wallet_config(bot.get("wallet_config") if isinstance(bot.get("wallet_config"), Mapping) else None)
    balances = wallet_config.get("balances")
    manager = mp.Manager()
    shared_wallet_proxy = _build_shared_wallet_proxy(
        manager,
        run_id=run_id,
        bot_id=bot_id,
        balances=_normalise_balances(balances),
    )

    _persist_lifecycle_phase(
        bot_id=bot_id,
        run_id=run_id,
        phase=BotLifecyclePhase.PLANNING_SERIES_WORKERS.value,
        owner=LifecycleOwner.CONTAINER.value,
        message="Planning one worker shard per strategy symbol.",
        metadata={"symbols": list(all_symbols), "symbol_count": len(all_symbols)},
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
    )
    parent_event_queue: "mp.Queue[Dict[str, Any]]" = mp.Queue(
        maxsize=_parent_event_queue_maxsize(worker_count=len(ctx.symbol_shards))
    )
    ctx.parent_event_queue = parent_event_queue
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

    def _build_bootstrap_facts() -> tuple[list[Dict[str, Any]], str, str]:
        bootstrap_payload = runtime.botlens_bootstrap_payload()
        facts = bootstrap_payload.get("facts") if isinstance(bootstrap_payload.get("facts"), list) else []
        series_key_local = normalize_series_key(bootstrap_payload.get("series_key"))
        known_at_local = str(bootstrap_payload.get("known_at") or "").strip() or utc_now_iso()
        if not facts:
            raise RuntimeError(f"worker bootstrap missing facts | worker_id={worker_id} | symbols={list(symbols)}")
        if not series_key_local:
            raise RuntimeError(f"worker bootstrap missing series key | worker_id={worker_id} | symbols={list(symbols)}")
        return [dict(fact) for fact in facts if isinstance(fact, Mapping)], series_key_local, known_at_local

    bridge_session_id = uuid.uuid4().hex
    bridge_seq = 0
    bridge_resync_reason: str | None = None

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

    def _emit_series_bootstrap(*, reason: str | None = None) -> bool:
        nonlocal bridge_seq, bridge_resync_reason
        facts, series_key_local, known_at_local = _build_bootstrap_facts()
        bridge_seq += 1
        emitted = _queue_worker_event(
            {
                "kind": "series_bootstrap",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "series_key": series_key_local,
                "bridge_session_id": bridge_session_id,
                "bridge_seq": bridge_seq,
                "reason": reason or bridge_resync_reason,
                "facts": facts,
                "known_at": known_at_local,
                "event_time": utc_now_iso(),
            }
        )
        if emitted:
            bridge_resync_reason = None
        return emitted

    if not _queue_worker_event(
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
    if not _emit_series_bootstrap():
        raise RuntimeError(f"worker bootstrap bridge unavailable | worker_id={worker_id}")
    _facts, series_key, _known_at = _build_bootstrap_facts()
    if not _queue_worker_event(
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
    stream_stop = threading.Event()

    def _runtime_facts_loop() -> None:
        nonlocal bridge_seq
        while not stream_stop.is_set() or not subscription_queue.empty():
            if bridge_resync_reason:
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
                _schedule_bridge_resync(str(message.get("reason") or "subscriber_gap"))
                continue
            if message_type != "facts":
                continue
            facts = message.get("facts") if isinstance(message.get("facts"), list) else []
            if not facts:
                continue
            known_at = message.get("known_at") or utc_now_iso()
            bridge_seq_local = bridge_seq + 1
            emitted = _queue_worker_event(
                {
                    "kind": "runtime_facts",
                    "worker_id": worker_id,
                    "symbols": list(symbols),
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
                continue
            _schedule_bridge_resync("bridge_queue_backpressure")

    emitter_thread = threading.Thread(target=_runtime_facts_loop, name=f"bot-facts-stream-{worker_id}", daemon=True)
    emitter_thread.start()
    try:
        runtime.start()
    except Exception as exc:  # noqa: BLE001
        runtime_error["message"] = str(exc)
        runtime_error["exception"] = repr(exc)
        runtime_error["exception_type"] = type(exc).__name__
        runtime_error["traceback"] = traceback.format_exc().strip()
    finally:
        stream_stop.set()
        emitter_thread.join(timeout=1.0)
        runtime.unsubscribe(subscription_token)
    status = str((runtime.snapshot() or {}).get("status") or "").strip().lower() or ("error" if runtime_error else "stopped")
    if runtime_error:
        event_queue.put(
            {
                "kind": "worker_error",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "error": runtime_error.get("message"),
                "exception": runtime_error.get("exception"),
                "exception_type": runtime_error.get("exception_type"),
                "traceback": runtime_error.get("traceback"),
                "at": utc_now_iso(),
            }
        )
        raise RuntimeError(
            f"symbol worker failed | worker_id={worker_id} | symbols={list(symbols)} | error={runtime_error.get('message')}"
        )
    if status in {"error", "failed", "crashed", "degraded"}:
        raise RuntimeError(
            f"symbol worker runtime status failed | worker_id={worker_id} | symbols={list(symbols)} | status={status}"
        )


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
    if phase in {BotLifecyclePhase.WARMING_UP_RUNTIME.value, BotLifecyclePhase.RUNTIME_SUBSCRIBING.value}:
        _persist_lifecycle_phase(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            phase=phase,
            owner=LifecycleOwner.RUNTIME.value,
            message=message,
            metadata=_series_progress_metadata(ctx),
        )


def _handle_series_bootstrap_event(
    ctx: ContainerStartupContext,
    event: Mapping[str, Any],
    *,
    telemetry_sender: TelemetryEmitter,
) -> tuple[int, float, int, bool]:
    worker_id = str(event.get("worker_id") or "").strip()
    run_seq = _next_run_event_seq(ctx.shared_wallet_proxy)
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
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=BotLifecyclePhase.AWAITING_FIRST_SNAPSHOT.value,
        owner=LifecycleOwner.RUNTIME.value,
        message="Series bootstrap completed; awaiting first live runtime snapshot.",
        metadata=_series_progress_metadata(ctx),
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
    worker_id = str(event.get("worker_id") or "").strip()
    run_seq = _next_run_event_seq(ctx.shared_wallet_proxy)
    for symbol in ctx.worker_symbols.get(worker_id) or []:
        _set_series_state(
            ctx,
            symbol=symbol,
            status="live",
            worker_id=worker_id or None,
            series_key=series_key,
            message="Series emitted first live runtime snapshot." if symbol not in ctx.first_snapshot_series else "Series remains live.",
        )
        ctx.first_snapshot_series.add(str(symbol).strip().upper())
    telemetry_payload = {
        "kind": "botlens_runtime_facts",
        "bot_id": ctx.bot_id,
        "run_id": ctx.run_id,
        "worker_id": worker_id,
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
    if len(ctx.first_snapshot_series) == len(ctx.symbols):
        ctx.startup_live_emitted = True
        _persist_lifecycle_phase(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            phase=BotLifecyclePhase.LIVE.value,
            owner=LifecycleOwner.RUNTIME.value,
            message="All planned series emitted first runtime snapshot; bot is live.",
            metadata=_series_progress_metadata(ctx),
            status=BotLifecycleStatus.RUNNING.value,
    )
    return run_seq, telemetry_emit_ms, payload_bytes, sent


def _drain_parent_event_queue(
    *,
    event_queue: "mp.Queue[Dict[str, Any]] | None",
    handle_event: Any,
) -> Dict[str, int]:
    drained_counts: Dict[str, int] = {}
    while True:
        if event_queue is None:
            break
        try:
            event = event_queue.get_nowait()
        except queue.Empty:
            break
        worker_id = str(event.get("worker_id") or "").strip()
        handle_event(worker_id, event)
        if worker_id:
            drained_counts[worker_id] = int(drained_counts.get(worker_id, 0)) + 1
    return drained_counts


def _handle_worker_error(
    ctx: ContainerStartupContext,
    worker_id: str,
    *,
    error: str | None,
    exit_code: int | None = None,
    exception_type: str | None = None,
    traceback_text: str | None = None,
    stderr_tail: str | None = None,
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
    partial_runtime_alive = ctx.startup_live_emitted or bool(ctx.first_snapshot_series) or remaining_live_workers
    failure_phase = BotLifecyclePhase.DEGRADED.value if partial_runtime_alive else BotLifecyclePhase.STARTUP_FAILED.value
    failure_status = BotLifecycleStatus.DEGRADED.value if partial_runtime_alive else BotLifecycleStatus.STARTUP_FAILED.value
    if worker_id in ctx.reported_worker_failures:
        return
    ctx.reported_worker_failures.add(worker_id)
    failure_message = error or (f"Worker {worker_id} exited with code {exit_code}" if exit_code is not None else "Worker failure reported by runtime container.")
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=failure_phase,
        owner=LifecycleOwner.RUNTIME.value,
        message=failure_message,
        metadata=_series_progress_metadata(ctx),
        failure=_worker_failure_payload(
            ctx=ctx,
            worker_id=worker_id,
            phase=failure_phase,
            message=failure_message,
            exit_code=exit_code,
            exception_type=exception_type,
            traceback_text=traceback_text,
            stderr_tail=stderr_tail,
        ),
        status=failure_status,
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


def supervise_startup_and_runtime(ctx: ContainerStartupContext) -> None:
    run_seq = 0
    telemetry_url = str(_TELEMETRY_SETTINGS.ws_url or "").strip()
    telemetry_sender = TelemetryEmitter(
        telemetry_url,
        queue_max=_TELEMETRY_EMIT_QUEUE_MAX,
        queue_timeout_ms=_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS,
        retry_ms=_TELEMETRY_EMIT_RETRY_MS,
    )
    telemetry_degraded = False
    event_poll_ms = _TELEMETRY_SETTINGS.event_poll_ms
    try:
        while ctx.children:
            loop_started_at = datetime.now(timezone.utc)
            loop_started = time.monotonic()
            run_seq = 0
            queue_drain_ms = 0.0
            worker_reconcile_ms = 0.0
            telemetry_emit_ms = 0.0
            status_write_ms = 0.0
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
                    emitted_events_in_cycle += 1
                    return
                if kind == "worker_error":
                    telemetry_degraded = True
                    _handle_worker_error(
                        ctx,
                        worker_id,
                        error=str(event.get("error") or "").strip() or None,
                        exception_type=str(event.get("exception_type") or "").strip() or None,
                        traceback_text=str(event.get("traceback") or "").strip() or None,
                    )

            drained_counts = _drain_parent_event_queue(
                event_queue=ctx.parent_event_queue,
                handle_event=_handle_parent_queue_event,
            )
            queue_drain_ms = max((time.monotonic() - queue_drain_started) * 1000.0, 0.0)

            worker_reconcile_started = time.monotonic()
            for worker_id, proc in list(ctx.children.items()):
                if proc.exitcode is None:
                    continue
                if proc.exitcode != 0:
                    telemetry_degraded = True
                    _handle_worker_error(
                        ctx,
                        worker_id,
                        error=f"Worker {worker_id} exited with code {proc.exitcode}",
                        exit_code=proc.exitcode,
                    )
                del ctx.children[worker_id]
            worker_reconcile_ms = max((time.monotonic() - worker_reconcile_started) * 1000.0, 0.0)

            status = BotLifecycleStatus.RUNNING.value if ctx.children else BotLifecycleStatus.STOPPED.value
            if telemetry_degraded and status == BotLifecycleStatus.RUNNING.value:
                status = BotLifecycleStatus.TELEMETRY_DEGRADED.value
            status_write_started = time.monotonic()
            update_bot_runtime_status(
                bot_id=ctx.bot_id,
                run_id=ctx.run_id,
                status=status,
                telemetry_degraded=telemetry_degraded,
            )
            status_write_ms = max((time.monotonic() - status_write_started) * 1000.0, 0.0)
            sleep_for = 0.0 if not ctx.children else max((event_poll_ms / 1000.0) - (time.monotonic() - loop_started), 0.005)
            loop_ended_at = datetime.now(timezone.utc)
            loop_total_ms = max((time.monotonic() - loop_started) * 1000.0, 0.0)
            record_bot_run_step(
                {
                    "run_id": ctx.run_id,
                    "bot_id": ctx.bot_id,
                    "step_name": "container_runtime_event_cycle",
                    "started_at": loop_started_at,
                    "ended_at": loop_ended_at,
                    "duration_ms": loop_total_ms,
                    "ok": True,
                    "context": {
                        "run_seq": run_seq,
                        "worker_count": len(ctx.symbol_shards),
                        "active_workers": len(ctx.children),
                        "degraded_symbols_count": len(ctx.degraded_symbols),
                        "event_poll_ms": event_poll_ms,
                        "emitted_events_in_cycle": emitted_events_in_cycle,
                        "drained_events_in_cycle": sum(drained_counts.values()),
                        "drain_worker_count": len(drained_counts),
                        "drain_mode": "shared_fanin",
                        "cadence_mode": cadence_mode,
                        "queue_drain_ms": queue_drain_ms,
                        "worker_reconcile_ms": worker_reconcile_ms,
                        "telemetry_emit_ms": telemetry_emit_ms,
                        "payload_bytes": payload_bytes,
                        "status_write_ms": status_write_ms,
                        "sleep_ms": sleep_for * 1000.0,
                    },
                }
            )
            sleep_started_at = datetime.now(timezone.utc)
            sleep_ended_at = sleep_started_at + timedelta(seconds=max(sleep_for, 0.0))
            try:
                record_bot_run_step(
                    {
                        "run_id": ctx.run_id,
                        "bot_id": ctx.bot_id,
                        "step_name": "container_runtime_event_sleep",
                        "started_at": sleep_started_at,
                        "ended_at": sleep_ended_at,
                        "duration_ms": max(sleep_for * 1000.0, 0.0),
                        "ok": True,
                        "context": {
                            "run_seq": run_seq,
                            "event_poll_ms": event_poll_ms,
                            "emitted_events_in_cycle": emitted_events_in_cycle,
                            "cadence_mode": cadence_mode,
                            "active_workers": len(ctx.children),
                            "worker_count": len(ctx.symbol_shards),
                        },
                    }
                )
            except Exception:
                logger.exception(
                    "bot_runtime_container_sleep_step_trace_failed | bot_id=%s | run_id=%s | seq=%s",
                    ctx.bot_id,
                    ctx.run_id,
                    run_seq,
                )
            if sleep_for > 0:
                time.sleep(sleep_for)
    except Exception as exc:  # noqa: BLE001
        final_phase = BotLifecyclePhase.CRASHED.value if ctx.startup_live_emitted else BotLifecyclePhase.STARTUP_FAILED.value
        final_status = BotLifecycleStatus.CRASHED.value if ctx.startup_live_emitted else BotLifecycleStatus.STARTUP_FAILED.value
        _persist_lifecycle_phase(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            phase=final_phase,
            owner=LifecycleOwner.CONTAINER.value,
            message=str(exc),
            metadata=_series_progress_metadata(ctx),
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
        )
        raise
    finally:
        telemetry_sender.close()
        for proc in ctx.children.values():
            if proc.is_alive():
                proc.terminate()
            proc.join(timeout=0.5)
        ctx.manager.shutdown()

    final_phase, final_status = terminal_status_after_supervision(
        startup_live_emitted=ctx.startup_live_emitted,
        degraded_symbols_count=len(ctx.degraded_symbols),
        telemetry_degraded=telemetry_degraded,
    )
    _persist_lifecycle_phase(
        bot_id=ctx.bot_id,
        run_id=ctx.run_id,
        phase=final_phase,
        owner=LifecycleOwner.CONTAINER.value,
        message="Container runtime supervision completed.",
        metadata=_series_progress_metadata(ctx),
        status=final_status,
    )


def main() -> int:
    _configure_logging()
    bot_id = str(_BOT_RUNTIME_SETTINGS.bot_id or "").strip()
    if not bot_id:
        raise RuntimeError("QT_BOT_RUNTIME_BOT_ID is required")

    ctx = load_container_startup_context(bot_id)
    logger.info("bot_runtime_run_started | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)
    spawn_workers(ctx)
    supervise_startup_and_runtime(ctx)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
