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
import math
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence

try:
    import websockets  # type: ignore
    from websockets.sync.client import connect as sync_connect  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    websockets = None
    sync_connect = None

from engines.bot_runtime.runtime.runtime import BotRuntime
from engines.bot_runtime.core.runtime_events import RuntimeEventName, build_correlation_id, new_runtime_event
from portal.backend.db.session import db
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


def _runtime_bar_marker(runtime_payload: Mapping[str, Any]) -> str:
    last_bar = runtime_payload.get("last_bar")
    if not isinstance(last_bar, Mapping):
        return ""
    marker = last_bar.get("end") or last_bar.get("time")
    if marker is None:
        return ""
    return str(marker).strip()


def _runtime_trade_count(runtime_payload: Mapping[str, Any]) -> int:
    stats = runtime_payload.get("stats")
    if not isinstance(stats, Mapping):
        return -1
    return _coerce_int(stats.get("total_trades"), -1)


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


def _tail_window(entries: Any, max_items: int) -> List[Any]:
    if not isinstance(entries, list):
        return []
    if int(max_items) <= 0:
        return list(entries)
    if len(entries) <= int(max_items):
        return list(entries)
    return list(entries[-int(max_items) :])


def _compact_overlay_geometry(value: Any, *, max_points: int) -> Any:
    if isinstance(value, Mapping):
        compact: Dict[str, Any] = {}
        for key, entry in value.items():
            compact[str(key)] = _compact_overlay_geometry(entry, max_points=max_points)
        return compact
    if isinstance(value, list):
        if int(max_points) <= 0:
            subset = list(value)
        elif len(value) <= int(max_points):
            subset = list(value)
        else:
            subset = list(value[-int(max_points) :])
        return [_compact_overlay_geometry(entry, max_points=max_points) for entry in subset]
    return value


def _compact_overlay_window(overlays: Any, *, max_overlays: int, max_points: int) -> List[Dict[str, Any]]:
    if not isinstance(overlays, list):
        return []
    if int(max_overlays) <= 0:
        subset = list(overlays)
    elif len(overlays) <= int(max_overlays):
        subset = list(overlays)
    else:
        subset = list(overlays[-int(max_overlays) :])
    compacted: List[Dict[str, Any]] = []
    for overlay in subset:
        if not isinstance(overlay, Mapping):
            continue
        compacted.append(dict(_compact_overlay_geometry(overlay, max_points=max_points)))
    return compacted


def _compact_trades_window(trades: Any, *, max_closed: int) -> List[Dict[str, Any]]:
    if not isinstance(trades, list):
        return []
    if int(max_closed) <= 0:
        return [dict(entry) for entry in trades if isinstance(entry, Mapping)]

    keep_mask: List[bool] = [False] * len(trades)
    closed_indices: List[int] = []
    for index, trade in enumerate(trades):
        if not isinstance(trade, Mapping):
            continue
        closed_at = trade.get("closed_at")
        if closed_at:
            closed_indices.append(index)
            continue
        keep_mask[index] = True
    for index in closed_indices[-int(max_closed) :]:
        keep_mask[index] = True

    compacted: List[Dict[str, Any]] = []
    for index, keep in enumerate(keep_mask):
        if not keep:
            continue
        trade = trades[index]
        if isinstance(trade, Mapping):
            compacted.append(dict(trade))
    return compacted


def _compact_view_state_payload(
    chart_snapshot: Mapping[str, Any],
    *,
    max_series: int,
    max_candles: int,
    max_overlays: int,
    max_overlay_points: int,
    max_closed_trades: int,
    max_logs: int,
    max_decisions: int,
    max_warnings: int,
) -> Dict[str, Any]:
    raw_series = chart_snapshot.get("series")
    compact_series: List[Dict[str, Any]] = []
    if isinstance(raw_series, list):
        for entry in raw_series[: max(int(max_series), 0) or None]:
            if not isinstance(entry, Mapping):
                continue
            compact_series.append(
                {
                    "strategy_id": entry.get("strategy_id"),
                    "symbol": entry.get("symbol"),
                    "timeframe": entry.get("timeframe"),
                    "datasource": entry.get("datasource"),
                    "exchange": entry.get("exchange"),
                    "instrument": entry.get("instrument"),
                    "candles": _tail_window(entry.get("candles"), max_candles),
                    "overlays": _compact_overlay_window(
                        entry.get("overlays"),
                        max_overlays=max_overlays,
                        max_points=max_overlay_points,
                    ),
                    "stats": dict(entry.get("stats") or {}) if isinstance(entry.get("stats"), Mapping) else {},
                }
            )

    runtime_payload = chart_snapshot.get("runtime")
    compact_runtime = dict(runtime_payload) if isinstance(runtime_payload, Mapping) else {}

    return {
        "series": compact_series,
        "trades": _compact_trades_window(chart_snapshot.get("trades"), max_closed=max_closed_trades),
        "logs": _tail_window(chart_snapshot.get("logs"), max_logs),
        "decisions": _tail_window(chart_snapshot.get("decisions"), max_decisions),
        "warnings": _tail_window(chart_snapshot.get("warnings"), max_warnings),
        "runtime": compact_runtime,
    }


def _emit_telemetry_ephemeral_message(url: str, message: str) -> bool:
    if not url:
        return False
    if websockets is None:
        logger.warning("bot_telemetry_library_missing | package=websockets")
        return False

    async def _send() -> None:
        async with websockets.connect(url, open_timeout=2, close_timeout=1) as ws:
            await ws.send(message)

    try:
        asyncio.run(_send())
    except Exception as exc:  # noqa: BLE001
        logger.warning("bot_telemetry_send_failed | error=%s", exc)
        return False
    return True


def _telemetry_message_context(message: str) -> Dict[str, Any]:
    try:
        payload = json.loads(str(message or "{}"))
    except Exception:
        return {}
    if not isinstance(payload, Mapping):
        return {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    return {
        "kind": str(payload.get("kind") or ""),
        "bot_id": str(payload.get("bot_id") or ""),
        "run_id": str(payload.get("run_id") or ""),
        "run_seq": _coerce_int(payload.get("run_seq"), 0),
        "series_seq": _coerce_int(payload.get("series_seq"), 0),
        "series_key": str(payload.get("series_key") or ""),
        "payload_bytes": _coerce_int(summary.get("payload_bytes"), 0),
        "known_at": payload.get("known_at"),
    }


_TELEMETRY_EMIT_QUEUE_MAX = max(8, _coerce_int(os.getenv("BOT_TELEMETRY_EMIT_QUEUE_MAX"), 256))
_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS = max(10, _coerce_int(os.getenv("BOT_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS"), 1000))
_TELEMETRY_EMIT_RETRY_MS = max(50, _coerce_int(os.getenv("BOT_TELEMETRY_EMIT_RETRY_MS"), 250))


class _TelemetryEmitter:
    def __init__(self, url: str) -> None:
        self.url = str(url or "").strip()
        self._sync_connect = None
        self._sync_ws = None
        self._state_lock = threading.Condition()
        self._pending_messages: deque[Dict[str, Any]] = deque()
        self._stop = False
        self._worker_thread: threading.Thread | None = None
        if not self.url:
            return
        self._sync_connect = sync_connect
        self._worker_thread = threading.Thread(target=self._worker_loop, name="bot-telemetry-emitter", daemon=True)
        self._worker_thread.start()

    def _close_sync_ws(self) -> None:
        ws = self._sync_ws
        self._sync_ws = None
        if ws is None:
            return
        try:
            ws.close()
        except Exception:
            pass

    def _send_sync_message(self, message: str, context: Optional[Mapping[str, Any]] = None) -> bool:
        if self._sync_connect is None:
            return False
        for attempt in range(2):
            try:
                if self._sync_ws is None:
                    self._sync_ws = self._sync_connect(self.url, open_timeout=2, close_timeout=1)
                started = time.monotonic()
                self._sync_ws.send(message)
                elapsed_ms = max((time.monotonic() - started) * 1000.0, 0.0)
                logger.debug(
                    "bot_telemetry_send_succeeded | mode=sync | attempt=%s | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | send_ms=%.3f",
                    attempt + 1,
                    (context or {}).get("kind"),
                    (context or {}).get("bot_id"),
                    (context or {}).get("run_id"),
                    (context or {}).get("run_seq"),
                    (context or {}).get("series_key"),
                    (context or {}).get("series_seq"),
                    (context or {}).get("payload_bytes"),
                    elapsed_ms,
                )
                return True
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "bot_telemetry_send_failed | mode=sync | attempt=%s | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | error=%s",
                    attempt + 1,
                    (context or {}).get("kind"),
                    (context or {}).get("bot_id"),
                    (context or {}).get("run_id"),
                    (context or {}).get("run_seq"),
                    (context or {}).get("series_key"),
                    (context or {}).get("series_seq"),
                    (context or {}).get("payload_bytes"),
                    exc,
                )
                self._close_sync_ws()
        return False

    def _deliver_message(self, message: str, context: Optional[Mapping[str, Any]] = None) -> bool:
        if self._sync_connect is not None:
            return self._send_sync_message(message, context=context)
        return _emit_telemetry_ephemeral_message(self.url, message)

    def _worker_loop(self) -> None:
        while True:
            entry = None
            with self._state_lock:
                while not self._stop and not self._pending_messages:
                    self._state_lock.wait(timeout=0.25)
                if self._stop and not self._pending_messages:
                    break
                entry = dict(self._pending_messages[0]) if self._pending_messages else None
            if not isinstance(entry, Mapping):
                continue
            message = str(entry.get("message") or "")
            context = entry.get("context") if isinstance(entry.get("context"), Mapping) else {}
            enqueued_at = float(entry.get("enqueued_monotonic") or time.monotonic())
            delivered = self._deliver_message(message, context=context)
            if delivered:
                queue_wait_ms = max((time.monotonic() - enqueued_at) * 1000.0, 0.0)
                with self._state_lock:
                    if self._pending_messages:
                        self._pending_messages.popleft()
                    queue_depth = len(self._pending_messages)
                    self._state_lock.notify_all()
                logger.debug(
                    "bot_telemetry_emit_dequeued | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | queue_wait_ms=%.3f | queue_depth=%s",
                    context.get("kind"),
                    context.get("bot_id"),
                    context.get("run_id"),
                    context.get("run_seq"),
                    context.get("series_key"),
                    context.get("series_seq"),
                    context.get("payload_bytes"),
                    queue_wait_ms,
                    queue_depth,
                )
                continue

            with self._state_lock:
                queue_depth = len(self._pending_messages)
            logger.warning(
                "bot_telemetry_emit_retry_scheduled | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | queue_depth=%s | retry_ms=%s",
                context.get("kind"),
                context.get("bot_id"),
                context.get("run_id"),
                context.get("run_seq"),
                context.get("series_key"),
                context.get("series_seq"),
                context.get("payload_bytes"),
                queue_depth,
                _TELEMETRY_EMIT_RETRY_MS,
            )
            time.sleep(_TELEMETRY_EMIT_RETRY_MS / 1000.0)

    def send_message(self, message: str) -> bool:
        if not self.url:
            return False
        context = _telemetry_message_context(message)
        deadline = time.monotonic() + (_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS / 1000.0)
        with self._state_lock:
            while len(self._pending_messages) >= _TELEMETRY_EMIT_QUEUE_MAX and not self._stop:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        "bot_telemetry_emit_queue_backpressure | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | queue_depth=%s | queue_max=%s | enqueue_timeout_ms=%s",
                        context.get("kind"),
                        context.get("bot_id"),
                        context.get("run_id"),
                        context.get("run_seq"),
                        context.get("series_key"),
                        context.get("series_seq"),
                        context.get("payload_bytes"),
                        len(self._pending_messages),
                        _TELEMETRY_EMIT_QUEUE_MAX,
                        _TELEMETRY_EMIT_QUEUE_TIMEOUT_MS,
                    )
                    return False
                self._state_lock.wait(timeout=remaining)
            if self._stop:
                return False
            self._pending_messages.append(
                {
                    "message": str(message),
                    "context": context,
                    "enqueued_monotonic": time.monotonic(),
                }
            )
            queue_depth = len(self._pending_messages)
            self._state_lock.notify_all()
        logger.debug(
            "bot_telemetry_emit_enqueued | kind=%s | bot_id=%s | run_id=%s | run_seq=%s | series_key=%s | series_seq=%s | payload_bytes=%s | queue_depth=%s | queue_max=%s",
            context.get("kind"),
            context.get("bot_id"),
            context.get("run_id"),
            context.get("run_seq"),
            context.get("series_key"),
            context.get("series_seq"),
            context.get("payload_bytes"),
            queue_depth,
            _TELEMETRY_EMIT_QUEUE_MAX,
        )
        return True

    def send(self, payload: Mapping[str, Any]) -> bool:
        message = json.dumps(_json_safe(payload))
        return self.send_message(message)

    def close(self) -> None:
        with self._state_lock:
            self._stop = True
            self._pending_messages.clear()
            self._state_lock.notify_all()
        thread = self._worker_thread
        self._worker_thread = None
        if thread is not None:
            thread.join(timeout=0.5)
        self._close_sync_ws()


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


def _merge_worker_view_state(
    latest_worker_view_state: Mapping[str, Mapping[str, Any]],
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

    for envelope in latest_worker_view_state.values():
        chart = envelope.get("view_state")
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
    runtime_error: Dict[str, str] = {}
    stream_max_series = max(1, _coerce_int(os.getenv("BOTLENS_STREAM_MAX_SERIES"), 12))
    stream_max_candles = max(50, _coerce_int(os.getenv("BOTLENS_STREAM_MAX_CANDLES"), 320))
    stream_max_overlays = max(50, _coerce_int(os.getenv("BOTLENS_STREAM_MAX_OVERLAYS"), 400))
    stream_max_overlay_points = max(20, _coerce_int(os.getenv("BOTLENS_STREAM_MAX_OVERLAY_POINTS"), 160))
    stream_max_closed_trades = max(20, _coerce_int(os.getenv("BOTLENS_STREAM_MAX_CLOSED_TRADES"), 240))
    stream_max_logs = max(50, _coerce_int(os.getenv("BOTLENS_STREAM_MAX_LOGS"), 300))
    stream_max_decisions = max(100, _coerce_int(os.getenv("BOTLENS_STREAM_MAX_DECISIONS"), 600))
    stream_max_warnings = max(20, _coerce_int(os.getenv("BOTLENS_STREAM_MAX_WARNINGS"), 120))
    runtime = BotRuntime(bot_id=bot_id, config=child_config, deps=build_bot_runtime_deps())
    runtime.reset_if_finished()
    runtime.warm_up()
    initial_chart_snapshot = runtime.chart_payload()
    compact_snapshot = _compact_view_state_payload(
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
            "known_at": _utc_now_iso(),
            "event_time": _utc_now_iso(),
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
            known_at = runtime_payload.get("last_snapshot_at") or runtime_payload.get("known_at") or _utc_now_iso()
            event_queue.put(
                {
                    "kind": "runtime_delta",
                    "worker_id": worker_id,
                    "symbols": list(symbols),
                    "series_key": series_key,
                    "runtime_delta": _json_safe(dict(message)),
                    "known_at": known_at,
                    "event_time": _utc_now_iso(),
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

    telemetry_url = str(os.getenv("BACKEND_TELEMETRY_WS_URL") or "").strip()
    event_poll_ms = max(10, _coerce_int(os.getenv("BOT_TELEMETRY_EVENT_POLL_MS"), 50))

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
    telemetry_sender = _TelemetryEmitter(telemetry_url)
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
                            "known_at": event.get("known_at") or _utc_now_iso(),
                            "event_time": event.get("event_time") or _utc_now_iso(),
                            "projection": dict(event.get("projection") or {}),
                            "summary": {},
                        }
                        telemetry_message = json.dumps(_json_safe(telemetry_payload))
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
                            "known_at": event.get("known_at") or _utc_now_iso(),
                            "event_time": event.get("event_time") or _utc_now_iso(),
                            "runtime_delta": dict(runtime_delta),
                            "summary": {},
                        }
                        telemetry_message = json.dumps(_json_safe(telemetry_payload))
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
