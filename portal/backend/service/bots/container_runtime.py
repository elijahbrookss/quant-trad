from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing as mp
import os
import queue
import time
import uuid
from datetime import datetime, timedelta, timezone
import math
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence

from engines.bot_runtime.core.runtime_events import RuntimeEventName, build_correlation_id, new_runtime_event
from portal.backend.db.session import db
from portal.backend.service.bots.bot_runtime import BotRuntime
from portal.backend.service.bots.bot_runtime.strategy.strategy_loader import StrategyLoader
from portal.backend.service.storage.storage import (
    get_latest_bot_run_snapshot,
    list_bot_runtime_events,
    load_bots,
    record_bot_run_step,
    record_bot_run_snapshot,
    update_bot_runtime_status,
)

logger = logging.getLogger(__name__)
_TERMINAL_STATUSES = {"completed", "stopped", "error", "failed", "crashed"}
_MAX_SYMBOLS_PER_STRATEGY = 10
_MAX_SYMBOL_WORKERS = 8
_SNAPSHOT_SCHEMA_VERSION = 1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _configure_logging() -> None:
    logging.basicConfig(level=getattr(logging, os.getenv("PORTAL_LOG_LEVEL", "INFO").upper(), logging.INFO))


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat() + "Z"
        return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _emit_telemetry(url: str, payload: Mapping[str, Any]) -> bool:
    if not url:
        return False
    try:
        import websockets  # type: ignore
    except Exception:
        logger.warning("bot_telemetry_library_missing | package=websockets")
        return False

    async def _send() -> None:
        async with websockets.connect(url, open_timeout=2, close_timeout=1) as ws:
            await ws.send(json.dumps(_json_safe(payload)))

    try:
        asyncio.run(_send())
    except Exception as exc:  # noqa: BLE001
        logger.warning("bot_telemetry_send_failed | error=%s", exc)
        return False
    return True


def _normalise_balances(raw_balances: Mapping[str, Any]) -> Dict[str, float]:
    balances: Dict[str, float] = {}
    for currency, amount in (raw_balances or {}).items():
        code = str(currency or "").strip().upper()
        if not code:
            continue
        balances[code] = _coerce_float(amount, 0.0)
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


def _merge_runtime_stats(runtime_payloads: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    if not runtime_payloads:
        return {}
    counter_keys = (
        "total_trades",
        "completed_trades",
        "legs_closed",
        "wins",
        "losses",
        "breakeven_trades",
        "long_trades",
        "short_trades",
    )
    pnl_keys = ("gross_pnl", "fees_paid", "total_fees", "net_pnl")
    summary: Dict[str, Any] = {key: 0 for key in counter_keys}
    for key in pnl_keys:
        summary[key] = 0.0
    avg_win_weighted = 0.0
    avg_loss_weighted = 0.0
    max_drawdown = 0.0
    largest_win = None
    largest_loss = None
    quote_currency: str | None = None
    multi_currency = False

    for runtime in runtime_payloads:
        stats = runtime.get("stats")
        if not isinstance(stats, Mapping):
            continue
        for key in counter_keys:
            summary[key] += _coerce_int(stats.get(key), 0)
        for key in pnl_keys:
            summary[key] += _coerce_float(stats.get(key), 0.0)
        wins = max(_coerce_int(stats.get("wins"), 0), 0)
        losses = max(_coerce_int(stats.get("losses"), 0), 0)
        avg_win_weighted += _coerce_float(stats.get("avg_win"), 0.0) * wins
        avg_loss_weighted += _coerce_float(stats.get("avg_loss"), 0.0) * losses
        max_drawdown = max(max_drawdown, _coerce_float(stats.get("max_drawdown"), 0.0))
        current_largest_win = _coerce_float(stats.get("largest_win"), 0.0)
        current_largest_loss = _coerce_float(stats.get("largest_loss"), 0.0)
        largest_win = current_largest_win if largest_win is None else max(largest_win, current_largest_win)
        largest_loss = current_largest_loss if largest_loss is None else min(largest_loss, current_largest_loss)
        current_quote = stats.get("quote_currency")
        if isinstance(current_quote, str) and current_quote:
            if quote_currency is None:
                quote_currency = current_quote
            elif quote_currency != current_quote:
                multi_currency = True

    completed = max(_coerce_int(summary.get("completed_trades"), 0), 0)
    wins = max(_coerce_int(summary.get("wins"), 0), 0)
    losses = max(_coerce_int(summary.get("losses"), 0), 0)
    summary["win_rate"] = round((wins / completed), 4) if completed else 0.0
    summary["avg_win"] = round(avg_win_weighted / wins, 4) if wins else 0.0
    summary["avg_loss"] = round(avg_loss_weighted / losses, 4) if losses else 0.0
    summary["largest_win"] = round(float(largest_win or 0.0), 4)
    summary["largest_loss"] = round(float(largest_loss or 0.0), 4)
    summary["max_drawdown"] = round(max_drawdown, 4)
    for key in pnl_keys:
        summary[key] = round(_coerce_float(summary.get(key), 0.0), 4)
    if quote_currency:
        summary["quote_currency"] = "MULTI" if multi_currency else quote_currency
    return summary


def _merge_runtime_payloads(
    runtime_payloads: Sequence[Mapping[str, Any]],
    *,
    worker_count: int,
    active_workers: int,
    degraded_symbols: Sequence[str],
) -> Dict[str, Any]:
    progress_values = [_coerce_float(payload.get("progress"), 0.0) for payload in runtime_payloads]
    status_values = [str(payload.get("status") or "").lower() for payload in runtime_payloads]
    paused = any(bool(payload.get("paused")) for payload in runtime_payloads)
    if active_workers > 0:
        status = "running"
    elif degraded_symbols:
        status = "degraded"
    elif status_values and all(value == "completed" for value in status_values):
        status = "completed"
    elif any(value in {"error", "failed", "crashed"} for value in status_values):
        status = "error"
    else:
        status = "stopped"

    runtime: Dict[str, Any] = {
        "status": status,
        "progress": round(sum(progress_values) / len(progress_values), 6) if progress_values else 0.0,
        "paused": paused,
        "worker_count": int(worker_count),
        "active_workers": int(active_workers),
        "degraded_symbols": sorted({str(symbol) for symbol in degraded_symbols if str(symbol).strip()}),
        "stats": _merge_runtime_stats(runtime_payloads),
    }
    return runtime


def _merge_chart_snapshots(
    latest_worker_snapshots: Mapping[str, Mapping[str, Any]],
    *,
    worker_count: int,
    active_workers: int,
    degraded_symbols: Sequence[str],
) -> Dict[str, Any]:
    series_by_key: MutableMapping[str, Dict[str, Any]] = {}
    trades_by_key: MutableMapping[str, Dict[str, Any]] = {}
    logs_by_key: MutableMapping[str, Dict[str, Any]] = {}
    decisions_by_key: MutableMapping[str, Dict[str, Any]] = {}
    warnings: List[Any] = []
    runtime_payloads: List[Mapping[str, Any]] = []

    for envelope in latest_worker_snapshots.values():
        chart = envelope.get("snapshot")
        if not isinstance(chart, Mapping):
            continue
        raw_series = chart.get("series")
        if isinstance(raw_series, list):
            for entry in raw_series:
                if not isinstance(entry, Mapping):
                    continue
                key = "|".join(
                    [
                        str(entry.get("strategy_id") or ""),
                        str(entry.get("symbol") or ""),
                        str(entry.get("timeframe") or ""),
                    ]
                )
                series_by_key[key] = dict(entry)
        raw_trades = chart.get("trades")
        if isinstance(raw_trades, list):
            for index, trade in enumerate(raw_trades):
                if not isinstance(trade, Mapping):
                    continue
                trade_id = str(trade.get("trade_id") or trade.get("id") or "").strip()
                if not trade_id:
                    trade_id = "|".join(
                        [
                            str(trade.get("symbol") or ""),
                            str(trade.get("entry_time") or ""),
                            str(trade.get("direction") or ""),
                            str(index),
                        ]
                    )
                trades_by_key[trade_id] = dict(trade)
        raw_warnings = chart.get("warnings")
        if isinstance(raw_warnings, list):
            warnings.extend(raw_warnings)
        raw_logs = chart.get("logs")
        if isinstance(raw_logs, list):
            for index, log_entry in enumerate(raw_logs):
                if not isinstance(log_entry, Mapping):
                    continue
                log_key = str(log_entry.get("id") or "").strip()
                if not log_key:
                    log_key = "|".join(
                        [
                            str(log_entry.get("timestamp") or log_entry.get("event_time") or ""),
                            str(log_entry.get("event") or log_entry.get("message") or ""),
                            str(log_entry.get("symbol") or ""),
                            str(index),
                        ]
                    )
                logs_by_key[log_key] = dict(log_entry)
        raw_decisions = chart.get("decisions")
        if isinstance(raw_decisions, list):
            for index, decision in enumerate(raw_decisions):
                if not isinstance(decision, Mapping):
                    continue
                event_id = str(decision.get("event_id") or "").strip()
                if not event_id:
                    event_id = "|".join(
                        [
                            str(decision.get("event_ts") or decision.get("created_at") or ""),
                            str(decision.get("event_type") or ""),
                            str(decision.get("event_subtype") or ""),
                            str(decision.get("symbol") or ""),
                            str(decision.get("trade_id") or ""),
                            str(index),
                        ]
                    )
                decisions_by_key[event_id] = dict(decision)
        runtime_payload = chart.get("runtime")
        if isinstance(runtime_payload, Mapping):
            runtime_payloads.append(runtime_payload)

    runtime = _merge_runtime_payloads(
        runtime_payloads,
        worker_count=worker_count,
        active_workers=active_workers,
        degraded_symbols=degraded_symbols,
    )
    if degraded_symbols:
        warnings.append(
            {
                "id": f"degraded:{','.join(sorted(set(degraded_symbols)))}",
                "type": "symbol_degraded",
                "message": "One or more symbols were degraded due to worker failure.",
                "context": {"symbols": sorted(set(degraded_symbols))},
                "level": "warning",
                "source": "container_runtime",
                "timestamp": _utc_now_iso(),
            }
        )

    merged_series = list(series_by_key.values())
    merged_series.sort(key=lambda entry: (str(entry.get("symbol") or ""), str(entry.get("timeframe") or "")))
    merged_trades = list(trades_by_key.values())
    merged_trades.sort(key=lambda entry: str(entry.get("entry_time") or entry.get("time") or ""))
    merged_logs = list(logs_by_key.values())
    merged_logs.sort(key=lambda entry: str(entry.get("timestamp") or entry.get("event_time") or ""))
    merged_decisions = list(decisions_by_key.values())
    merged_decisions.sort(key=lambda entry: str(entry.get("event_ts") or entry.get("created_at") or ""))
    return {
        "series": merged_series,
        "trades": merged_trades,
        "logs": merged_logs[-400:],
        "decisions": merged_decisions[-800:],
        "warnings": warnings[-200:],
        "runtime": runtime,
    }


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
            _coerce_int(os.getenv("BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY"), 10),
        )
    runtime: BotRuntime
    runtime_error: Dict[str, str] = {}
    full_snapshot_interval_ms = max(
        50,
        _coerce_int(os.getenv("BOT_WORKER_FULL_SNAPSHOT_INTERVAL_MS"), 1000),
    )
    last_snapshot_emit_mono = 0.0

    def _emit_snapshot() -> str:
        chart_snapshot = runtime.chart_payload()
        runtime_snapshot = chart_snapshot.get("runtime") if isinstance(chart_snapshot, Mapping) else {}
        status_value = str((runtime_snapshot or {}).get("status") or "").lower() or "running"
        event_queue.put(
            {
                "kind": "snapshot",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "status": status_value,
                "snapshot": chart_snapshot,
                "at": _utc_now_iso(),
            }
        )
        return status_value

    def _state_callback(payload: Dict[str, Any]) -> None:
        nonlocal last_snapshot_emit_mono
        if not isinstance(payload, Mapping):
            return
        runtime_payload = payload.get("runtime")
        if not isinstance(runtime_payload, Mapping):
            return
        status_value = str(runtime_payload.get("status") or "").strip().lower()
        now_mono = time.monotonic()
        due = (
            last_snapshot_emit_mono <= 0.0
            or (now_mono - last_snapshot_emit_mono) * 1000.0 >= float(full_snapshot_interval_ms)
        )
        if due or status_value in _TERMINAL_STATUSES:
            _emit_snapshot()
            last_snapshot_emit_mono = now_mono

    runtime = BotRuntime(bot_id=bot_id, config=child_config, state_callback=_state_callback)
    runtime.reset_if_finished()
    try:
        runtime.start()
    except Exception as exc:  # noqa: BLE001
        runtime_error["message"] = str(exc)
        runtime_error["exception"] = repr(exc)
    status = _emit_snapshot()
    if runtime_error:
        event_queue.put(
            {
                "kind": "worker_error",
                "worker_id": worker_id,
                "symbols": list(symbols),
                "error": runtime_error.get("message"),
                "exception": runtime_error.get("exception"),
                "at": _utc_now_iso(),
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

    snapshot_interval_ms = int(os.getenv("SNAPSHOT_INTERVAL_MS") or "0")
    if snapshot_interval_ms <= 0:
        raise RuntimeError("SNAPSHOT_INTERVAL_MS must be > 0")
    fast_snapshot_interval_ms = _coerce_int(
        os.getenv("SNAPSHOT_FAST_INTERVAL_MS"),
        min(snapshot_interval_ms, 250),
    )
    if fast_snapshot_interval_ms <= 0:
        raise RuntimeError("SNAPSHOT_FAST_INTERVAL_MS must be > 0")
    idle_snapshot_interval_ms = _coerce_int(
        os.getenv("SNAPSHOT_IDLE_INTERVAL_MS"),
        snapshot_interval_ms,
    )
    if idle_snapshot_interval_ms <= 0:
        raise RuntimeError("SNAPSHOT_IDLE_INTERVAL_MS must be > 0")
    fast_snapshot_interval_ms = max(25, fast_snapshot_interval_ms)
    idle_snapshot_interval_ms = max(fast_snapshot_interval_ms, idle_snapshot_interval_ms)
    idle_cycle_threshold = max(1, _coerce_int(os.getenv("SNAPSHOT_IDLE_CYCLES"), 2))

    telemetry_url = str(os.getenv("BACKEND_TELEMETRY_WS_URL") or "").strip()
    telemetry_include_snapshot = str(os.getenv("BOT_TELEMETRY_INCLUDE_SNAPSHOT") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    bot = next((b for b in load_bots() if b.get("id") == bot_id), None)
    if bot is None:
        raise RuntimeError(f"Bot not found: {bot_id}")

    run_id = str(uuid.uuid4())
    if list_bot_runtime_events(bot_id=bot_id, run_id=run_id, after_seq=0, limit=1):
        raise RuntimeError(f"run_id collision in runtime events for bot {bot_id}: {run_id}")
    if get_latest_bot_run_snapshot(bot_id=bot_id, run_id=run_id, series_key="bot") is not None:
        raise RuntimeError(f"run_id collision in runtime snapshots for bot {bot_id}: {run_id}")
    logger.info("bot_runtime_run_started | bot_id=%s | run_id=%s", bot_id, run_id)
    update_bot_runtime_status(bot_id=bot_id, run_id=run_id, status="running")
    strategy_id = str(bot.get("strategy_id") or "").strip()
    if not strategy_id:
        raise RuntimeError(f"Bot {bot_id} has no strategy_id configured")
    all_symbols = _load_strategy_symbols(strategy_id)
    max_symbols = _coerce_int(os.getenv("BOT_MAX_SYMBOLS_PER_STRATEGY"), _MAX_SYMBOLS_PER_STRATEGY)
    if len(all_symbols) > max_symbols:
        raise RuntimeError(
            f"Strategy {strategy_id} has {len(all_symbols)} symbols but runtime limit is {max_symbols}. "
            "Reduce symbols or increase BOT_MAX_SYMBOLS_PER_STRATEGY."
        )

    default_max_workers = max(_MAX_SYMBOL_WORKERS, len(all_symbols))
    max_workers = _coerce_int(os.getenv("BOT_SYMBOL_PROCESS_MAX"), default_max_workers)
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
    latest_snapshots: Dict[str, Dict[str, Any]] = {}
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

    seq = 0
    telemetry_degraded = False
    idle_cycles_without_snapshot_events = 0
    try:
        while children or latest_snapshots:
            loop_started_at = datetime.now(timezone.utc)
            loop_started = time.monotonic()
            queue_drain_ms = 0.0
            worker_reconcile_ms = 0.0
            merge_ms = 0.0
            snapshot_write_ms = 0.0
            telemetry_emit_ms = 0.0
            status_write_ms = 0.0
            sleep_for = 0.0
            target_interval_ms = idle_snapshot_interval_ms
            cadence_mode = "idle"
            snapshot_events_in_cycle = 0
            queue_drain_started = time.monotonic()
            for worker_id, event_queue in list(child_queues.items()):
                while True:
                    try:
                        event = event_queue.get_nowait()
                    except queue.Empty:
                        break
                    kind = str(event.get("kind") or "").strip().lower()
                    if kind == "snapshot":
                        snapshot_events_in_cycle += 1
                        latest_snapshots[worker_id] = dict(event)
                    elif kind == "worker_error":
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

            seq += 1
            now_iso = _utc_now_iso()
            merge_started = time.monotonic()
            merged_chart = _merge_chart_snapshots(
                latest_snapshots,
                worker_count=len(symbol_shards),
                active_workers=len(children),
                degraded_symbols=sorted(degraded_symbols),
            )
            merge_ms = max((time.monotonic() - merge_started) * 1000.0, 0.0)
            snapshot = {
                "kind": "snapshot",
                "schema_version": _SNAPSHOT_SCHEMA_VERSION,
                "run_id": run_id,
                "snapshot_seq": seq,
                "series_key": "bot",
                "status": str((merged_chart.get("runtime") or {}).get("status") or ""),
                "snapshot": merged_chart,
                "known_at": now_iso,
                "at": now_iso,
            }
            snapshot_write_started = time.monotonic()
            record_bot_run_snapshot(
                {
                    "run_id": run_id,
                    "bot_id": bot_id,
                    "series_key": "bot",
                    "snapshot_seq": seq,
                    "snapshot_payload": snapshot,
                }
            )
            snapshot_write_ms = max((time.monotonic() - snapshot_write_started) * 1000.0, 0.0)
            telemetry_payload = {
                "run_id": run_id,
                "bot_id": bot_id,
                "series_key": "bot",
                "snapshot_seq": seq,
                "status": str(snapshot.get("status") or ""),
                "at": now_iso,
                "known_at": now_iso,
                "summary": {
                    "series_count": len(merged_chart.get("series") or []),
                    "trade_count": len(merged_chart.get("trades") or []),
                    "warning_count": len(merged_chart.get("warnings") or []),
                },
            }
            if telemetry_include_snapshot:
                telemetry_payload["snapshot"] = snapshot
            telemetry_started = time.monotonic()
            sent = _emit_telemetry(telemetry_url, telemetry_payload)
            telemetry_emit_ms = max((time.monotonic() - telemetry_started) * 1000.0, 0.0)
            if not sent:
                telemetry_degraded = True
            status = "running" if children else "stopped"
            status_write_started = time.monotonic()
            update_bot_runtime_status(
                bot_id=bot_id,
                run_id=run_id,
                status=status,
                telemetry_degraded=telemetry_degraded,
            )
            status_write_ms = max((time.monotonic() - status_write_started) * 1000.0, 0.0)
            if children:
                if snapshot_events_in_cycle > 0:
                    idle_cycles_without_snapshot_events = 0
                    target_interval_ms = fast_snapshot_interval_ms
                    cadence_mode = "hot"
                else:
                    idle_cycles_without_snapshot_events += 1
                    if idle_cycles_without_snapshot_events >= idle_cycle_threshold:
                        target_interval_ms = idle_snapshot_interval_ms
                        cadence_mode = "idle"
                    else:
                        target_interval_ms = fast_snapshot_interval_ms
                        cadence_mode = "warmup"
                elapsed = time.monotonic() - loop_started
                sleep_for = max((target_interval_ms / 1000.0) - elapsed, 0.05)
            else:
                sleep_for = 0.0
                idle_cycles_without_snapshot_events = 0
                cadence_mode = "stopped"
            loop_ended_at = datetime.now(timezone.utc)
            loop_total_ms = max((time.monotonic() - loop_started) * 1000.0, 0.0)
            record_bot_run_step(
                {
                    "run_id": run_id,
                    "bot_id": bot_id,
                    "step_name": "container_snapshot_cycle",
                    "started_at": loop_started_at,
                    "ended_at": loop_ended_at,
                    "duration_ms": loop_total_ms,
                    "ok": True,
                    "context": {
                        "snapshot_seq": seq,
                        "worker_count": len(symbol_shards),
                        "active_workers": len(children),
                        "degraded_symbols_count": len(degraded_symbols),
                        "snapshot_interval_ms": snapshot_interval_ms,
                        "target_interval_ms": target_interval_ms,
                        "fast_snapshot_interval_ms": fast_snapshot_interval_ms,
                        "idle_snapshot_interval_ms": idle_snapshot_interval_ms,
                        "idle_cycle_threshold": idle_cycle_threshold,
                        "idle_cycles_without_snapshot_events": idle_cycles_without_snapshot_events,
                        "snapshot_events_in_cycle": snapshot_events_in_cycle,
                        "cadence_mode": cadence_mode,
                        "queue_drain_ms": queue_drain_ms,
                        "worker_reconcile_ms": worker_reconcile_ms,
                        "merge_ms": merge_ms,
                        "snapshot_write_ms": snapshot_write_ms,
                        "telemetry_emit_ms": telemetry_emit_ms,
                        "status_write_ms": status_write_ms,
                        "sleep_ms": sleep_for * 1000.0,
                    },
                }
            )
            if not children:
                latest_snapshots.clear()
                break

            sleep_started_at = datetime.now(timezone.utc)
            sleep_ended_at = sleep_started_at + timedelta(seconds=max(sleep_for, 0.0))
            try:
                record_bot_run_step(
                    {
                        "run_id": run_id,
                        "bot_id": bot_id,
                        "step_name": "container_snapshot_sleep",
                        "started_at": sleep_started_at,
                        "ended_at": sleep_ended_at,
                        "duration_ms": max(sleep_for * 1000.0, 0.0),
                        "ok": True,
                        "context": {
                            "snapshot_seq": seq,
                            "snapshot_interval_ms": snapshot_interval_ms,
                            "target_interval_ms": target_interval_ms,
                            "fast_snapshot_interval_ms": fast_snapshot_interval_ms,
                            "idle_snapshot_interval_ms": idle_snapshot_interval_ms,
                            "idle_cycle_threshold": idle_cycle_threshold,
                            "idle_cycles_without_snapshot_events": idle_cycles_without_snapshot_events,
                            "snapshot_events_in_cycle": snapshot_events_in_cycle,
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
                    seq,
                )
            time.sleep(sleep_for)
    except Exception:
        update_bot_runtime_status(bot_id=bot_id, run_id=run_id, status="failed", telemetry_degraded=telemetry_degraded)
        raise
    finally:
        for proc in children.values():
            if proc.is_alive():
                proc.terminate()
            proc.join(timeout=0.5)
        manager.shutdown()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
