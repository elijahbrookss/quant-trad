from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import multiprocessing as mp
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Sequence

from engines.bot_runtime.runtime.runtime import BotRuntime
from engines.bot_runtime.core.runtime_events import RuntimeEventName, build_correlation_id, new_runtime_event
from portal.backend.db.session import db
from portal.backend.service.bots.container_runtime_projection import (
    compact_view_state_payload,
    coerce_float,
    coerce_int,
    json_safe,
    merge_worker_view_state,
    runtime_bar_marker,
    runtime_trade_count,
    utc_now_iso,
)
from portal.backend.service.bots.container_runtime_telemetry import TelemetryEmitter
from portal.backend.service.bots.botlens_projection import canonical_series_key_from_entry, normalize_series_key
from portal.backend.service.bots.runtime_dependencies import build_bot_runtime_deps
from portal.backend.service.bots.strategy_loader import StrategyLoader
from portal.backend.service.storage.storage import (
    list_bot_runtime_events,
    load_bots,
    record_bot_run_step,
    update_bot_runtime_status,
)

logger = logging.getLogger(__name__)
_TERMINAL_STATUSES = {"completed", "stopped", "error", "failed", "crashed"}
_MAX_SYMBOLS_PER_STRATEGY = 10
_MAX_SYMBOL_WORKERS = 8
_VIEW_STATE_SCHEMA_VERSION = 1


def _configure_logging() -> None:
    logging.basicConfig(level=getattr(logging, os.getenv("PORTAL_LOG_LEVEL", "INFO").upper(), logging.INFO))


_TELEMETRY_EMIT_QUEUE_MAX = max(8, coerce_int(os.getenv("BOT_TELEMETRY_EMIT_QUEUE_MAX"), 256))
_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS = max(10, coerce_int(os.getenv("BOT_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS"), 1000))
_TELEMETRY_EMIT_RETRY_MS = max(50, coerce_int(os.getenv("BOT_TELEMETRY_EMIT_RETRY_MS"), 250))


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

    logger.info(
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
        child_config["BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY"] = max(
            1,
            coerce_int(os.getenv("BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY"), 10),
        )
    runtime_error: Dict[str, str] = {}
    stream_max_series = max(1, coerce_int(os.getenv("BOTLENS_STREAM_MAX_SERIES"), 12))
    stream_max_candles = max(50, coerce_int(os.getenv("BOTLENS_STREAM_MAX_CANDLES"), 320))
    stream_max_overlays = max(50, coerce_int(os.getenv("BOTLENS_STREAM_MAX_OVERLAYS"), 400))
    stream_max_overlay_points = max(20, coerce_int(os.getenv("BOTLENS_STREAM_MAX_OVERLAY_POINTS"), 160))
    stream_max_closed_trades = max(20, coerce_int(os.getenv("BOTLENS_STREAM_MAX_CLOSED_TRADES"), 240))
    stream_max_logs = max(50, coerce_int(os.getenv("BOTLENS_STREAM_MAX_LOGS"), 300))
    stream_max_decisions = max(100, coerce_int(os.getenv("BOTLENS_STREAM_MAX_DECISIONS"), 600))
    stream_max_warnings = max(20, coerce_int(os.getenv("BOTLENS_STREAM_MAX_WARNINGS"), 120))
    runtime = BotRuntime(bot_id=bot_id, config=child_config, deps=build_bot_runtime_deps())
    runtime.reset_if_finished()
    runtime.warm_up()
    initial_chart_snapshot = runtime.chart_payload()
    compact_snapshot = compact_view_state_payload(
        initial_chart_snapshot if isinstance(initial_chart_snapshot, Mapping) else {},
        max_series=stream_max_series,
        max_candles=stream_max_candles,
        max_overlays=stream_max_overlays,
        max_overlay_points=stream_max_overlay_points,
        max_closed_trades=stream_max_closed_trades,
        max_logs=stream_max_logs,
        max_decisions=stream_max_decisions,
        max_warnings=stream_max_warnings,
    )
    series_entries = compact_snapshot.get("series") if isinstance(compact_snapshot.get("series"), list) else []
    primary_series = series_entries[0] if series_entries else {}
    if not isinstance(primary_series, Mapping):
        raise RuntimeError(f"worker bootstrap missing series payload | worker_id={worker_id} | symbols={list(symbols)}")
    series_key = canonical_series_key_from_entry(primary_series)
    if not series_key:
        raise RuntimeError(f"worker bootstrap missing series key | worker_id={worker_id} | symbols={list(symbols)}")
    event_queue.put(
        {
            "kind": "series_bootstrap",
            "worker_id": worker_id,
            "symbols": list(symbols),
            "series_key": series_key,
            "projection": compact_snapshot,
            "known_at": utc_now_iso(),
            "event_time": utc_now_iso(),
        }
    )
    subscription_token, subscription_queue = runtime.subscribe()
    stream_stop = threading.Event()

    def _runtime_delta_loop() -> None:
        while not stream_stop.is_set() or not subscription_queue.empty():
            try:
                message = subscription_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if not isinstance(message, Mapping):
                continue
            if str(message.get("type") or "").strip().lower() != "delta":
                continue
            runtime_payload = message.get("runtime") if isinstance(message.get("runtime"), Mapping) else {}
            known_at = runtime_payload.get("last_snapshot_at") or runtime_payload.get("known_at") or utc_now_iso()
            event_queue.put(
                {
                    "kind": "runtime_delta",
                    "worker_id": worker_id,
                    "symbols": list(symbols),
                    "series_key": series_key,
                    "runtime_delta": json_safe(dict(message)),
                    "known_at": known_at,
                    "event_time": utc_now_iso(),
                }
            )

    emitter_thread = threading.Thread(target=_runtime_delta_loop, name=f"bot-delta-stream-{worker_id}", daemon=True)
    emitter_thread.start()
    try:
        runtime.start()
    except Exception as exc:  # noqa: BLE001
        runtime_error["message"] = str(exc)
        runtime_error["exception"] = repr(exc)
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
                "at": utc_now_iso(),
            }
        )
        raise RuntimeError(
            f"symbol worker failed | worker_id={worker_id} | symbols={list(symbols)} | error={runtime_error.get('message')}"
        )
    if status in {"error", "failed", "crashed"}:
        raise RuntimeError(
            f"symbol worker runtime status failed | worker_id={worker_id} | symbols={list(symbols)} | status={status}"
        )


def main() -> int:
    _configure_logging()
    bot_id = str(os.getenv("BOT_ID") or "").strip()
    if not bot_id:
        raise RuntimeError("BOT_ID is required")

    telemetry_url = str(os.getenv("BACKEND_TELEMETRY_WS_URL") or "").strip()
    event_poll_ms = max(10, coerce_int(os.getenv("BOT_TELEMETRY_EVENT_POLL_MS"), 50))

    bot = next((b for b in load_bots() if b.get("id") == bot_id), None)
    if bot is None:
        raise RuntimeError(f"Bot not found: {bot_id}")

    run_id = str(uuid.uuid4())
    if list_bot_runtime_events(bot_id=bot_id, run_id=run_id, after_seq=0, limit=1):
        raise RuntimeError(f"run_id collision in runtime events for bot {bot_id}: {run_id}")
    logger.info("bot_runtime_run_started | bot_id=%s | run_id=%s", bot_id, run_id)
    update_bot_runtime_status(bot_id=bot_id, run_id=run_id, status="running")
    strategy_id = str(bot.get("strategy_id") or "").strip()
    if not strategy_id:
        raise RuntimeError(f"Bot {bot_id} has no strategy_id configured")
    all_symbols = _load_strategy_symbols(strategy_id)
    max_symbols = coerce_int(os.getenv("BOT_MAX_SYMBOLS_PER_STRATEGY"), _MAX_SYMBOLS_PER_STRATEGY)
    if len(all_symbols) > max_symbols:
        raise RuntimeError(
            f"Strategy {strategy_id} has {len(all_symbols)} symbols but runtime limit is {max_symbols}. "
            "Reduce symbols or increase BOT_MAX_SYMBOLS_PER_STRATEGY."
        )

    default_max_workers = max(_MAX_SYMBOL_WORKERS, len(all_symbols))
    max_workers = coerce_int(os.getenv("BOT_SYMBOL_PROCESS_MAX"), default_max_workers)
    symbol_shards = _assign_symbols_to_workers(
        all_symbols,
        max_workers=max(1, max_workers),
    )
    if not symbol_shards:
        raise RuntimeError(f"Strategy {strategy_id} resolved no symbol shards")

    wallet_config = bot.get("wallet_config")
    if not isinstance(wallet_config, Mapping):
        raise RuntimeError("wallet_config is required for symbol-sharded runtime")
    balances = wallet_config.get("balances")
    if not isinstance(balances, Mapping) or not balances:
        raise RuntimeError("wallet_config.balances is required for symbol-sharded runtime")
    manager = mp.Manager()
    shared_wallet_proxy = _build_shared_wallet_proxy(
        manager,
        run_id=run_id,
        bot_id=bot_id,
        balances=_normalise_balances(balances),
    )

    child_queues: Dict[str, "mp.Queue[Dict[str, Any]]"] = {}
    children: Dict[str, mp.Process] = {}
    worker_symbols: Dict[str, List[str]] = {}
    degraded_symbols: set[str] = set()

    for index, symbols in enumerate(symbol_shards):
        worker_id = f"worker-{index + 1}"
        event_queue: "mp.Queue[Dict[str, Any]]" = mp.Queue()
        child_queues[worker_id] = event_queue
        worker_symbols[worker_id] = list(symbols)
        proc = mp.Process(
            target=_series_worker,
            kwargs={
                "run_id": run_id,
                "bot_id": bot_id,
                "worker_id": worker_id,
                "strategy_id": strategy_id,
                "symbols": list(symbols),
                "bot_config": bot,
                "shared_wallet_proxy": shared_wallet_proxy,
                "event_queue": event_queue,
            },
            daemon=False,
        )
        proc.start()
        children[worker_id] = proc

    run_seq = 0
    series_seq_by_key: Dict[str, int] = {}
    telemetry_sender = TelemetryEmitter(
        telemetry_url,
        queue_max=_TELEMETRY_EMIT_QUEUE_MAX,
        queue_timeout_ms=_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS,
        retry_ms=_TELEMETRY_EMIT_RETRY_MS,
    )
    telemetry_degraded = False
    try:
        while children:
            loop_started_at = datetime.now(timezone.utc)
            loop_started = time.monotonic()
            queue_drain_ms = 0.0
            worker_reconcile_ms = 0.0
            telemetry_emit_ms = 0.0
            status_write_ms = 0.0
            payload_bytes = 0
            emitted_events_in_cycle = 0
            cadence_mode = "event_driven"
            queue_drain_started = time.monotonic()
            for worker_id, event_queue in list(child_queues.items()):
                while True:
                    try:
                        event = event_queue.get_nowait()
                    except queue.Empty:
                        break
                    kind = str(event.get("kind") or "").strip().lower()
                    if kind == "series_bootstrap":
                        series_key = normalize_series_key(event.get("series_key"))
                        if not series_key:
                            continue
                        run_seq = _next_run_event_seq(shared_wallet_proxy)
                        series_seq = series_seq_by_key.get(series_key, 0) + 1
                        series_seq_by_key[series_key] = series_seq
                        telemetry_payload = {
                            "kind": "botlens_series_bootstrap",
                            "bot_id": bot_id,
                            "run_id": run_id,
                            "worker_id": worker_id,
                            "run_seq": run_seq,
                            "series_seq": series_seq,
                            "series_key": series_key,
                            "known_at": event.get("known_at") or utc_now_iso(),
                            "event_time": event.get("event_time") or utc_now_iso(),
                            "projection": dict(event.get("projection") or {}),
                            "summary": {},
                        }
                        telemetry_message = json.dumps(json_safe(telemetry_payload))
                        payload_bytes = len(telemetry_message.encode("utf-8"))
                        telemetry_payload["summary"]["payload_bytes"] = payload_bytes
                        telemetry_started = time.monotonic()
                        sent = telemetry_sender.send(telemetry_payload)
                        telemetry_emit_ms += max((time.monotonic() - telemetry_started) * 1000.0, 0.0)
                        if not sent:
                            telemetry_degraded = True
                        emitted_events_in_cycle += 1
                        continue
                    if kind == "runtime_delta":
                        series_key = normalize_series_key(event.get("series_key"))
                        runtime_delta = event.get("runtime_delta") if isinstance(event.get("runtime_delta"), Mapping) else {}
                        if not series_key or not runtime_delta:
                            continue
                        run_seq = _next_run_event_seq(shared_wallet_proxy)
                        series_seq = series_seq_by_key.get(series_key, 0) + 1
                        series_seq_by_key[series_key] = series_seq
                        telemetry_payload = {
                            "kind": "botlens_series_delta",
                            "bot_id": bot_id,
                            "run_id": run_id,
                            "worker_id": worker_id,
                            "run_seq": run_seq,
                            "series_seq": series_seq,
                            "series_key": series_key,
                            "known_at": event.get("known_at") or utc_now_iso(),
                            "event_time": event.get("event_time") or utc_now_iso(),
                            "runtime_delta": dict(runtime_delta),
                            "summary": {},
                        }
                        telemetry_message = json.dumps(json_safe(telemetry_payload))
                        payload_bytes = len(telemetry_message.encode("utf-8"))
                        telemetry_payload["summary"]["payload_bytes"] = payload_bytes
                        telemetry_started = time.monotonic()
                        sent = telemetry_sender.send(telemetry_payload)
                        telemetry_emit_ms += max((time.monotonic() - telemetry_started) * 1000.0, 0.0)
                        if not sent:
                            telemetry_degraded = True
                        emitted_events_in_cycle += 1
                        continue
                    if kind == "worker_error":
                        symbols = event.get("symbols")
                        if isinstance(symbols, list):
                            degraded_symbols.update(str(symbol).upper() for symbol in symbols if str(symbol).strip())
                        telemetry_degraded = True
                        logger.error(
                            "bot_symbol_worker_error_event | run_id=%s | bot_id=%s | worker_id=%s | symbols=%s | error=%s",
                            run_id,
                            bot_id,
                            worker_id,
                            symbols,
                            event.get("error"),
                        )
            queue_drain_ms = max((time.monotonic() - queue_drain_started) * 1000.0, 0.0)

            worker_reconcile_started = time.monotonic()
            for worker_id, proc in list(children.items()):
                if proc.exitcode is None:
                    continue
                if proc.exitcode != 0:
                    failed_symbols = worker_symbols.get(worker_id) or []
                    degraded_symbols.update(str(symbol).upper() for symbol in failed_symbols if str(symbol).strip())
                    telemetry_degraded = True
                    logger.error(
                        "bot_symbol_worker_failed | run_id=%s | bot_id=%s | worker_id=%s | symbols=%s | exitcode=%s",
                        run_id,
                        bot_id,
                        worker_id,
                        failed_symbols,
                        proc.exitcode,
                    )
                del children[worker_id]
                child_queues.pop(worker_id, None)
            worker_reconcile_ms = max((time.monotonic() - worker_reconcile_started) * 1000.0, 0.0)

            status = "running" if children else "stopped"
            status_write_started = time.monotonic()
            update_bot_runtime_status(
                bot_id=bot_id,
                run_id=run_id,
                status=status,
                telemetry_degraded=telemetry_degraded,
            )
            status_write_ms = max((time.monotonic() - status_write_started) * 1000.0, 0.0)
            sleep_for = 0.0 if not children else max((event_poll_ms / 1000.0) - (time.monotonic() - loop_started), 0.005)
            loop_ended_at = datetime.now(timezone.utc)
            loop_total_ms = max((time.monotonic() - loop_started) * 1000.0, 0.0)
            record_bot_run_step(
                {
                    "run_id": run_id,
                    "bot_id": bot_id,
                    "step_name": "container_runtime_event_cycle",
                    "started_at": loop_started_at,
                    "ended_at": loop_ended_at,
                    "duration_ms": loop_total_ms,
                    "ok": True,
                    "context": {
                        "run_seq": run_seq,
                        "worker_count": len(symbol_shards),
                        "active_workers": len(children),
                        "degraded_symbols_count": len(degraded_symbols),
                        "event_poll_ms": event_poll_ms,
                        "emitted_events_in_cycle": emitted_events_in_cycle,
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
                        "run_id": run_id,
                        "bot_id": bot_id,
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
                            "active_workers": len(children),
                            "worker_count": len(symbol_shards),
                        },
                    }
                )
            except Exception:
                logger.exception(
                    "bot_runtime_container_sleep_step_trace_failed | bot_id=%s | run_id=%s | seq=%s",
                    bot_id,
                    run_id,
                    run_seq,
                )
            if sleep_for > 0:
                time.sleep(sleep_for)
    except Exception:
        update_bot_runtime_status(bot_id=bot_id, run_id=run_id, status="failed", telemetry_degraded=telemetry_degraded)
        raise
    finally:
        telemetry_sender.close()
        for proc in children.values():
            if proc.is_alive():
                proc.terminate()
            proc.join(timeout=0.5)
        manager.shutdown()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
