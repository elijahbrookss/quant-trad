"""Runtime subscriber broadcast and live delta payload assembly."""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from queue import Empty, Full, Queue
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import Candle
from utils.log_context import with_log_context

from ..core import _isoformat
from ..components.overlay_delta import (
    build_overlay_delta,
    entry_fingerprint,
    overlay_cache_key,
    overlay_change_metrics,
    overlay_delta_op_counts,
    overlay_payload_fingerprint,
    overlay_payload_metrics,
)

logger = logging.getLogger(__name__)


class RuntimePushStreamMixin:
    def subscribe(self) -> Tuple[str, Queue]:
        channel: Queue = Queue(maxsize=256)
        token = str(uuid.uuid4())
        with self._lock:
            self._subscribers[token] = channel
        return token, channel

    def unsubscribe(self, token: str) -> None:
        with self._lock:
            channel = self._subscribers.pop(token, None)
        if not channel:
            return
        try:
            while True:
                channel.get_nowait()
        except Empty:
            pass

    def _broadcast(self, event: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[int, int]:
        message = dict(payload or {})
        message.setdefault("type", event)
        with self._lock:
            channels = list(self._subscribers.items())
        for token, queue_ref in channels:
            try:
                queue_ref.put_nowait(message)
            except Full:
                context = self._runtime_log_context(
                    subscriber_token=token,
                    queue_max=getattr(queue_ref, "maxsize", None),
                    event=event,
                )
                logger.warning(with_log_context("bot_runtime_stream_backpressure", context))
                try:
                    queue_ref.put(message, timeout=0.25)
                except Full as exc:
                    raise RuntimeError(
                        f"runtime stream subscriber backpressure exceeded | event={event} subscriber={token}"
                    ) from exc
        return len(channels), 0

    @staticmethod
    def _trade_revision(series: StrategySeries) -> Tuple[Any, ...]:
        engine = getattr(series, "risk_engine", None)
        engine_revision = getattr(engine, "trade_revision", None)
        if isinstance(engine_revision, int):
            return (int(engine_revision),)
        trades = list(getattr(engine, "trades", []) or [])
        if not trades:
            return (0, None, None, None, None, None, None)
        last = trades[-1]
        legs = list(getattr(last, "legs", []) or [])
        open_legs = sum(1 for leg in legs if str(getattr(leg, "status", "")) == "open")
        active_trade = getattr(engine, "active_trade", None)
        last_closed_at = _isoformat(getattr(last, "closed_at", None))
        last_net_pnl = round(float(getattr(last, "net_pnl", 0.0) or 0.0), 4)
        return (
            len(trades),
            str(getattr(last, "trade_id", "") or ""),
            last_closed_at,
            int(getattr(last, "bars_held", 0) or 0),
            open_legs,
            last_net_pnl,
            str(getattr(active_trade, "trade_id", "") or ""),
        )

    @staticmethod
    def _payload_size_bytes(payload: Mapping[str, Any]) -> int:
        total = 0
        encoder = json.JSONEncoder(separators=(",", ":"), default=str, ensure_ascii=False)
        for chunk in encoder.iterencode(payload):
            total += len(chunk.encode("utf-8"))
        return total

    def _should_probe_payload_size(self) -> bool:
        with self._lock:
            self._push_payload_size_probe_count += 1
            probe_count = self._push_payload_size_probe_count
        sample_every = max(int(getattr(self, "_push_payload_bytes_sample_every", 10) or 10), 1)
        return probe_count % sample_every == 0

    def _series_visible_overlays(
        self,
        series: StrategySeries,
        *,
        status: str,
    ) -> List[Dict[str, Any]]:
        overlays = list(series.overlays or [])
        if series.trade_overlay:
            overlays.append(series.trade_overlay)
        return self._chart_state_builder.visible_overlays(
            overlays,
            status,
            self._current_epoch_for(series),
        )

    def _series_overlay_revision(
        self,
        series: StrategySeries,
        *,
        status: str,
    ) -> Tuple[Any, ...]:
        overlays = list(series.overlays or [])
        if series.trade_overlay:
            overlays.append(series.trade_overlay)
        members = [
            (
                overlay_cache_key(overlay, index),
                overlay_payload_fingerprint(overlay),
            )
            for index, overlay in enumerate(overlays)
            if isinstance(overlay, Mapping)
        ]
        members.sort(key=lambda entry: entry[0])
        return (
            str(status or ""),
            self._current_epoch_for(series),
            tuple(members),
        )

    def _overlay_delta_op_counts(self, delta: Mapping[str, Any]) -> Dict[str, int]:
        return overlay_delta_op_counts(delta)

    def _build_overlay_delta(
        self,
        cache: Dict[str, Any],
        overlays: Sequence[Mapping[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        return build_overlay_delta(cache, overlays)

    def _overlay_change_metrics(
        self,
        before: Sequence[Mapping[str, Any]],
        after: Sequence[Mapping[str, Any]],
    ) -> Tuple[float, float]:
        return overlay_change_metrics(before, after)

    def _overlay_payload_metrics(self, payload: Mapping[str, Any]) -> Tuple[int, int]:
        return overlay_payload_metrics(payload)

    def _entry_fingerprint(self, entries: Sequence[Mapping[str, Any]]) -> Tuple[int, Optional[str], Optional[str]]:
        return entry_fingerprint(entries)

    def _push_update(
        self,
        event: str,
        *,
        series: Optional[StrategySeries] = None,
        candle: Optional[Candle] = None,
        replace_last: bool = False,
        precomputed_stats: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Optional[float]]:
        push_started = datetime.now(timezone.utc)
        push_started_perf = time.perf_counter()
        ok = True
        payload_context: Dict[str, Any] = {
            "event": event,
            "payload_bytes": None,
            "payload_bytes_sampled": None,
            "payload_bytes_sample_every": int(getattr(self, "_push_payload_bytes_sample_every", 10) or 10),
            "build_state_ms": None,
            "delta_build_ms": None,
            "serialize_ms": None,
            "delta_serialize_ms": None,
            "enqueue_ms": None,
            "stream_emit_ms": None,
            "subscriber_count": None,
            "subscribers_count": None,
            "dropped_messages": None,
            "overlay_count": None,
            "overlay_points": None,
            "stats_update_ms": None,
            "stats_reused": None,
        }
        error_message: Optional[str] = None
        trace_persist_ms: Optional[float] = None
        build_state_ms: Optional[float] = None
        serialize_ms: Optional[float] = None
        enqueue_ms: Optional[float] = None
        stats_update_ms: Optional[float] = None
        overlay_count: Optional[int] = None
        overlay_points: Optional[int] = None
        subscriber_count: Optional[int] = None
        dropped_messages: Optional[int] = None
        logs_revision: int = 0
        decisions_revision: int = 0
        logs_count: int = 0
        decisions_count: int = 0
        with self._lock:
            subscriber_count = len(self._subscribers)
            logs_revision = int(getattr(self, "_log_revision", 0))
            decisions_revision = int(getattr(self, "_decision_revision", 0))
            logs_count = len(self._logs)
            decisions_count = len(self._decision_events)
        if subscriber_count <= 0 and event in {"bar", "intrabar"}:
            dropped_messages = 0
            build_state_ms = 0.0
            serialize_ms = 0.0
            enqueue_ms = 0.0
            payload_context.update(
                {
                    "build_state_ms": build_state_ms,
                    "delta_build_ms": build_state_ms,
                    "serialize_ms": serialize_ms,
                    "delta_serialize_ms": serialize_ms,
                    "payload_bytes_sampled": False,
                    "enqueue_ms": enqueue_ms,
                    "stream_emit_ms": enqueue_ms,
                    "subscriber_count": subscriber_count,
                    "subscribers_count": subscriber_count,
                    "dropped_messages": dropped_messages,
                    "stats_reused": isinstance(precomputed_stats, Mapping),
                    "skip_reason": "no_subscribers",
                }
            )
            if event == "bar":
                trace_persist_ms = self._record_step_trace(
                    "step_push_update",
                    started_at=push_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    context=payload_context,
                )
            push_duration_ms = max((time.perf_counter() - push_started_perf) * 1000.0, 0.0)
            return {
                "duration_ms": push_duration_ms,
                "build_state_ms": build_state_ms,
                "delta_build_ms": build_state_ms,
                "serialize_ms": serialize_ms,
                "delta_serialize_ms": serialize_ms,
                "enqueue_ms": enqueue_ms,
                "stream_emit_ms": enqueue_ms,
                "stats_update_ms": stats_update_ms,
                "subscriber_count": float(subscriber_count),
                "subscribers_count": float(subscriber_count),
                "dropped_messages": float(dropped_messages),
                "overlay_count": None,
                "overlay_points": None,
                "trace_persist_ms": trace_persist_ms,
            }
        try:
            build_started = time.perf_counter()
            payload: Dict[str, Any] = {
                "type": "delta",
                "event": event,
                "runtime": self.snapshot(),
                "stats": None,
            }
            logs_entries: List[Dict[str, Any]] = []
            if logs_revision != int(getattr(self, "_push_logs_revision", -1)):
                logs_entries = self.logs()
                payload["logs"] = logs_entries
                self._push_logs_revision = logs_revision
            decisions_entries: List[Dict[str, Any]] = []
            if decisions_revision != int(getattr(self, "_push_decisions_revision", -1)):
                decisions_entries = self.decision_events()
                payload["decisions"] = decisions_entries
                self._push_decisions_revision = decisions_revision
            if isinstance(precomputed_stats, Mapping):
                payload["stats"] = dict(precomputed_stats)
                stats_update_ms = 0.0
                payload_context["stats_reused"] = True
            else:
                stats_started = time.perf_counter()
                payload["stats"] = self._aggregate_stats()
                stats_update_ms = max((time.perf_counter() - stats_started) * 1000.0, 0.0)
                payload_context["stats_reused"] = False
            candles_count: Optional[int] = None
            trades_count: Optional[int] = None
            if series is not None:
                series_key = self._strategy_key(series)
                cache = self._push_series_cache.setdefault(series_key, {})
                status = str(self.state.get("status") or "").lower()
                series_state = self._series_state_for(series)
                bar_index = series_state.bar_index if series_state else 0
                candles_count = min(bar_index + 1, len(series.candles))
                series_delta: Dict[str, Any] = {
                    "strategy_id": series.strategy_id,
                    "symbol": series.symbol,
                    "timeframe": series.timeframe,
                    "bar_index": bar_index,
                    "replace_last": bool(replace_last),
                }
                include_heavy_series_data = event != "intrabar"
                if include_heavy_series_data or "visible_overlays" not in cache:
                    overlay_revision = self._series_overlay_revision(series, status=status)
                    if cache.get("overlay_revision") != overlay_revision:
                        cache["visible_overlays"] = self._series_visible_overlays(series, status=status)
                        cache["overlay_revision"] = overlay_revision
                    visible_overlays = cache.get("visible_overlays")
                    if isinstance(visible_overlays, list):
                        overlay_summary = self._overlay_summary(visible_overlays)
                        overlay_delta = build_overlay_delta(cache, visible_overlays)
                        logger.debug(
                            with_log_context(
                                "bot_overlay_emit_attempt",
                                self._series_log_context(
                                    series,
                                    bar_index=bar_index,
                                    status=status,
                                    event=event,
                                    overlays=overlay_summary.get("total_overlays"),
                                    overlay_types=overlay_summary.get("type_counts"),
                                    overlay_payloads=overlay_summary.get("payload_counts"),
                                    overlay_profile_params=overlay_summary.get("profile_params_samples"),
                                    emitted_delta=isinstance(overlay_delta, Mapping),
                                ),
                            )
                        )
                        if isinstance(overlay_delta, Mapping):
                            series_delta["overlay_delta"] = dict(overlay_delta)
                            logger.debug(
                                with_log_context(
                                    "bot_overlay_delta_sent",
                                    self._series_log_context(
                                        series,
                                        bar_index=bar_index,
                                        seq=overlay_delta.get("seq"),
                                        base_seq=overlay_delta.get("base_seq"),
                                        overlay_ops=len(overlay_delta.get("ops") or []),
                                        overlay_op_counts=self._overlay_delta_op_counts(overlay_delta),
                                        overlays=overlay_summary.get("total_overlays"),
                                        overlay_types=overlay_summary.get("type_counts"),
                                        overlay_payloads=overlay_summary.get("payload_counts"),
                                        overlay_profile_params=overlay_summary.get("profile_params_samples"),
                                    ),
                                )
                            )
                trades_revision = self._trade_revision(series)
                if cache.get("trades_revision") != trades_revision:
                    trades = series.risk_engine.serialise_trades()
                    trades_count = len(trades)
                    cache["trades"] = trades
                    cache["trades_revision"] = trades_revision
                    series_stats = series.risk_engine.stats()
                    series_stats["total_fees"] = series_stats.get("fees_paid", 0.0)
                    cache["series_stats"] = series_stats
                    series_delta["trades"] = trades
                    series_delta["stats"] = series_stats
                else:
                    cached_trades = cache.get("trades")
                    if isinstance(cached_trades, list):
                        trades_count = len(cached_trades)
                if candle is not None:
                    series_delta["candle"] = candle.to_dict()
                payload["series"] = [series_delta]
            build_state_ms = max((time.perf_counter() - build_started) * 1000.0, 0.0)
            overlay_count, overlay_points = self._overlay_payload_metrics(payload)
            payload_context.update(
                {
                    "candles_count": candles_count,
                    "trades_count": trades_count,
                    "logs_count": logs_count,
                    "decisions_count": decisions_count,
                    "series_count": len(self._series or []),
                    "build_state_ms": build_state_ms,
                    "delta_build_ms": build_state_ms,
                    "overlay_count": overlay_count,
                    "overlay_points": overlay_points,
                    "stats_update_ms": stats_update_ms,
                }
            )
            if self._obs_enabled:
                should_probe = self._should_probe_payload_size()
                payload_context["payload_bytes_sampled"] = should_probe
                if should_probe:
                    serialize_started = time.perf_counter()
                    try:
                        payload_context["payload_bytes"] = self._payload_size_bytes(payload)
                    except Exception:
                        payload_context["payload_bytes"] = None
                    finally:
                        serialize_ms = max((time.perf_counter() - serialize_started) * 1000.0, 0.0)
                        payload_context["serialize_ms"] = serialize_ms
                        payload_context["delta_serialize_ms"] = serialize_ms
            enqueue_started = time.perf_counter()
            subscriber_count, dropped_messages = self._broadcast("delta", payload)
            enqueue_ms = max((time.perf_counter() - enqueue_started) * 1000.0, 0.0)
            payload_context["enqueue_ms"] = enqueue_ms
            payload_context["stream_emit_ms"] = enqueue_ms
            payload_context["subscriber_count"] = subscriber_count
            payload_context["subscribers_count"] = subscriber_count
            payload_context["dropped_messages"] = dropped_messages
        except Exception as exc:
            ok = False
            error_message = str(exc)
            raise
        finally:
            if event == "bar":
                trace_persist_ms = self._record_step_trace(
                    "step_push_update",
                    started_at=push_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=ok,
                    error=error_message,
                    context=payload_context,
                )
        push_duration_ms = max((time.perf_counter() - push_started_perf) * 1000.0, 0.0)
        return {
            "duration_ms": push_duration_ms,
            "build_state_ms": build_state_ms,
            "delta_build_ms": build_state_ms,
            "serialize_ms": serialize_ms,
            "delta_serialize_ms": serialize_ms,
            "enqueue_ms": enqueue_ms,
            "stream_emit_ms": enqueue_ms,
            "stats_update_ms": stats_update_ms,
            "subscriber_count": float(subscriber_count) if subscriber_count is not None else None,
            "subscribers_count": float(subscriber_count) if subscriber_count is not None else None,
            "dropped_messages": float(dropped_messages) if dropped_messages is not None else None,
            "overlay_count": float(overlay_count) if overlay_count is not None else None,
            "overlay_points": float(overlay_points) if overlay_points is not None else None,
            "trace_persist_ms": trace_persist_ms,
        }
