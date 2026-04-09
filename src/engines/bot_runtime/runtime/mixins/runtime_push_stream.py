"""Runtime subscriber broadcast and BotLens fact-batch payload assembly."""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Mapping as AbcMapping
from datetime import datetime, timezone
from queue import Empty, Full, Queue
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.series_identity import canonical_series_key
from utils.log_context import with_log_context

from ..core import _isoformat
from ..components.overlay_delta import (
    build_overlay_delta,
    entry_fingerprint,
    overlay_cache_key,
    overlay_delta_op_counts,
    overlay_payload_fingerprint,
    overlay_payload_metrics,
)

logger = logging.getLogger(__name__)

BOTLENS_FACT_RUNTIME_STATE = "runtime_state_observed"
BOTLENS_FACT_SERIES_STATE = "series_state_observed"
BOTLENS_FACT_CANDLE_UPSERTED = "candle_upserted"
BOTLENS_FACT_OVERLAY_OPS = "overlay_ops_emitted"
BOTLENS_FACT_SERIES_STATS = "series_stats_updated"
BOTLENS_FACT_TRADE_UPSERTED = "trade_upserted"
BOTLENS_FACT_LOG_EMITTED = "log_emitted"
BOTLENS_FACT_DECISION_EMITTED = "decision_emitted"


class RuntimePushStreamMixin:
    def subscribe(self, *, overflow_policy: str = "fail") -> Tuple[str, Queue]:
        channel: Queue = Queue(maxsize=256)
        token = str(uuid.uuid4())
        with self._lock:
            self._subscribers[token] = {
                "queue": channel,
                "overflow_policy": str(overflow_policy or "fail"),
                "overflowed": False,
            }
        return token, channel

    def unsubscribe(self, token: str) -> None:
        with self._lock:
            subscriber = self._subscribers.pop(token, None)
        channel = subscriber.get("queue") if isinstance(subscriber, AbcMapping) else subscriber
        if not channel:
            return
        try:
            while True:
                channel.get_nowait()
        except Empty:
            pass

    @staticmethod
    def _signal_gap(queue_ref: Queue, *, event: str) -> bool:
        try:
            while True:
                queue_ref.get_nowait()
        except Empty:
            pass
        try:
            queue_ref.put_nowait(
                {
                    "type": "gap",
                    "reason": "subscriber_backpressure",
                    "event": str(event or ""),
                }
            )
            return True
        except Full:
            return False

    def _broadcast(self, event: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[int, int]:
        message = dict(payload or {})
        message.setdefault("type", event)
        with self._lock:
            channels = list(self._subscribers.items())
        for token, subscriber in channels:
            queue_ref = subscriber.get("queue") if isinstance(subscriber, AbcMapping) else subscriber
            overflow_policy = (
                str(subscriber.get("overflow_policy") or "fail")
                if isinstance(subscriber, AbcMapping)
                else "fail"
            )
            try:
                queue_ref.put_nowait(message)
                if isinstance(subscriber, AbcMapping):
                    subscriber["overflowed"] = False
            except Full:
                if overflow_policy == "drop_and_signal" and self._signal_gap(queue_ref, event=event):
                    context = self._runtime_log_context(
                        subscriber_token=token,
                        queue_max=getattr(queue_ref, "maxsize", None),
                        event=event,
                    )
                    logger.warning(with_log_context("bot_runtime_stream_gap_signaled", context))
                    if isinstance(subscriber, AbcMapping):
                        subscriber["overflowed"] = True
                    continue
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

    def _overlay_payload_metrics(self, payload: Mapping[str, Any]) -> Tuple[int, int]:
        return overlay_payload_metrics(payload)

    def _entry_fingerprint(self, entries: Sequence[Mapping[str, Any]]) -> Tuple[int, Optional[str], Optional[str]]:
        return entry_fingerprint(entries)

    @staticmethod
    def _entries_after_marker(
        entries: Sequence[Mapping[str, Any]],
        *,
        marker_field: str,
        previous_marker: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        normalized = [dict(entry) for entry in entries if isinstance(entry, AbcMapping)]
        if not normalized:
            return [], previous_marker
        latest_marker = str(normalized[-1].get(marker_field) or "").strip() or previous_marker
        if not previous_marker:
            return normalized, latest_marker
        for index, entry in enumerate(normalized):
            if str(entry.get(marker_field) or "").strip() == previous_marker:
                return normalized[index + 1 :], latest_marker
        return normalized, latest_marker

    @staticmethod
    def _series_identity(series: StrategySeries) -> Dict[str, Any]:
        instrument = series.instrument if isinstance(series.instrument, Mapping) else {}
        instrument_id = str(instrument.get("id") or "").strip()
        series_key = canonical_series_key(instrument_id, series.timeframe)
        if not series_key:
            raise RuntimeError(
                f"bot_runtime_push_invalid_series_identity: missing instrument_id/timeframe for strategy={series.strategy_id} symbol={series.symbol}"
            )
        return {
            "series_key": series_key,
            "strategy_id": series.strategy_id,
            "instrument_id": instrument_id,
            "symbol": series.symbol,
            "timeframe": series.timeframe,
            "datasource": getattr(series, "datasource", None),
            "exchange": getattr(series, "exchange", None),
            "instrument": dict(instrument) if isinstance(instrument, Mapping) else {},
        }

    def _runtime_state_fact(self, *, runtime_snapshot: Mapping[str, Any], event: str) -> Dict[str, Any]:
        return {
            "fact_type": BOTLENS_FACT_RUNTIME_STATE,
            "event": str(event or ""),
            "runtime": dict(runtime_snapshot or {}),
        }

    def _series_state_fact(
        self,
        *,
        series: StrategySeries,
        bar_index: int,
        replace_last: bool = False,
    ) -> Dict[str, Any]:
        identity = self._series_identity(series)
        return {
            "fact_type": BOTLENS_FACT_SERIES_STATE,
            **identity,
            "bar_index": int(bar_index),
            "replace_last": bool(replace_last),
        }

    def _log_facts(self) -> List[Dict[str, Any]]:
        entries = self.logs()
        new_entries, marker = self._entries_after_marker(
            entries,
            marker_field="id",
            previous_marker=getattr(self, "_push_log_marker", None),
        )
        self._push_log_marker = marker
        return [{"fact_type": BOTLENS_FACT_LOG_EMITTED, "log": entry} for entry in new_entries]

    def _decision_facts(self) -> List[Dict[str, Any]]:
        entries = self.decision_events()
        new_entries, marker = self._entries_after_marker(
            entries,
            marker_field="event_id",
            previous_marker=getattr(self, "_push_decision_marker", None),
        )
        self._push_decision_marker = marker
        return [{"fact_type": BOTLENS_FACT_DECISION_EMITTED, "decision": entry} for entry in new_entries]

    def _trade_facts(
        self,
        *,
        series: StrategySeries,
        cache: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int]:
        trades_revision = self._trade_revision(series)
        if cache.get("trades_revision") == trades_revision:
            cached_trades = cache.get("trades")
            return [], cache.get("series_stats"), len(cached_trades) if isinstance(cached_trades, list) else 0

        identity = self._series_identity(series)
        trades = series.risk_engine.serialise_trades()
        series_stats = series.risk_engine.stats()
        series_stats["total_fees"] = series_stats.get("fees_paid", 0.0)
        cached_trade_map = (
            dict(cache.get("trade_fingerprints"))
            if isinstance(cache.get("trade_fingerprints"), AbcMapping)
            else {}
        )
        next_trade_map: Dict[str, str] = {}
        trade_facts: List[Dict[str, Any]] = []
        for trade in trades:
            if not isinstance(trade, AbcMapping):
                continue
            trade_payload = dict(trade)
            trade_id = str(trade_payload.get("trade_id") or "").strip()
            if not trade_id:
                continue
            trade_fingerprint = json.dumps(trade_payload, sort_keys=True, default=str, separators=(",", ":"))
            next_trade_map[trade_id] = trade_fingerprint
            if cached_trade_map.get(trade_id) == trade_fingerprint:
                continue
            trade_facts.append(
                {
                    "fact_type": BOTLENS_FACT_TRADE_UPSERTED,
                    "series_key": identity["series_key"],
                    "trade": trade_payload,
                }
            )

        cache["trades"] = trades
        cache["trades_revision"] = trades_revision
        cache["series_stats"] = series_stats
        cache["trade_fingerprints"] = next_trade_map
        return trade_facts, series_stats, len(trades)

    def botlens_bootstrap_payload(self) -> Dict[str, Any]:
        if not self._series:
            raise RuntimeError("botlens bootstrap requires at least one prepared runtime series")
        series = self._series[0]
        identity = self._series_identity(series)
        runtime_snapshot = self.snapshot()
        chart_snapshot = self.chart_payload()
        selected_series = None
        for entry in chart_snapshot.get("series") if isinstance(chart_snapshot.get("series"), list) else []:
            if not isinstance(entry, AbcMapping):
                continue
            entry_key = canonical_series_key(
                str(entry.get("instrument_id") or entry.get("instrument", {}).get("id") or "").strip(),
                entry.get("timeframe"),
            )
            if entry_key == identity["series_key"]:
                selected_series = dict(entry)
                break
        if not isinstance(selected_series, AbcMapping):
            raise RuntimeError(
                f"botlens bootstrap missing selected series payload | series_key={identity['series_key']} symbol={series.symbol}"
            )

        bar_index = int(selected_series.get("bar_index") or max(len(selected_series.get("candles") or []) - 1, 0))
        facts: List[Dict[str, Any]] = [
            self._runtime_state_fact(runtime_snapshot=runtime_snapshot, event="bootstrap"),
            self._series_state_fact(series=series, bar_index=bar_index, replace_last=False),
        ]
        for candle in selected_series.get("candles") if isinstance(selected_series.get("candles"), list) else []:
            if isinstance(candle, AbcMapping):
                facts.append(
                    {
                        "fact_type": BOTLENS_FACT_CANDLE_UPSERTED,
                        "series_key": identity["series_key"],
                        "candle": dict(candle),
                        "replace_last": False,
                    }
                )
        overlays = selected_series.get("overlays") if isinstance(selected_series.get("overlays"), list) else []
        overlay_ops = []
        for index, overlay in enumerate(overlays):
            if not isinstance(overlay, AbcMapping):
                continue
            overlay_ops.append(
                {
                    "op": "upsert",
                    "key": overlay_cache_key(overlay, index),
                    "overlay": dict(overlay),
                }
            )
        if overlay_ops:
            facts.append(
                {
                    "fact_type": BOTLENS_FACT_OVERLAY_OPS,
                    "series_key": identity["series_key"],
                    "overlay_delta": {"base_seq": 0, "seq": 1, "ops": overlay_ops},
                }
            )
        series_stats = selected_series.get("stats") if isinstance(selected_series.get("stats"), AbcMapping) else {}
        facts.append(
            {
                "fact_type": BOTLENS_FACT_SERIES_STATS,
                "series_key": identity["series_key"],
                "stats": dict(series_stats or {}),
            }
        )
        for trade in chart_snapshot.get("trades") if isinstance(chart_snapshot.get("trades"), list) else []:
            if isinstance(trade, AbcMapping):
                facts.append(
                    {
                        "fact_type": BOTLENS_FACT_TRADE_UPSERTED,
                        "series_key": identity["series_key"],
                        "trade": dict(trade),
                    }
                )
        for entry in chart_snapshot.get("logs") if isinstance(chart_snapshot.get("logs"), list) else []:
            if isinstance(entry, AbcMapping):
                facts.append({"fact_type": BOTLENS_FACT_LOG_EMITTED, "log": dict(entry)})
        for entry in chart_snapshot.get("decisions") if isinstance(chart_snapshot.get("decisions"), list) else []:
            if isinstance(entry, AbcMapping):
                facts.append({"fact_type": BOTLENS_FACT_DECISION_EMITTED, "decision": dict(entry)})
        return {
            "type": "facts",
            "event": "bootstrap",
            "known_at": runtime_snapshot.get("last_snapshot_at") or runtime_snapshot.get("known_at") or _isoformat(datetime.now(timezone.utc)),
            "series_key": identity["series_key"],
            "facts": facts,
        }

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
        logs_count: int = 0
        decisions_count: int = 0
        with self._lock:
            subscriber_count = len(self._subscribers)
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
            runtime_snapshot = self.snapshot()
            payload: Dict[str, Any] = {
                "type": "facts",
                "event": event,
                "known_at": runtime_snapshot.get("last_snapshot_at")
                or runtime_snapshot.get("known_at")
                or _isoformat(datetime.now(timezone.utc)),
                "facts": [self._runtime_state_fact(runtime_snapshot=runtime_snapshot, event=event)],
            }
            if isinstance(precomputed_stats, Mapping):
                payload["facts"][0]["runtime"]["stats"] = dict(precomputed_stats)
                stats_update_ms = 0.0
                payload_context["stats_reused"] = True
            else:
                stats_started = time.perf_counter()
                self._aggregate_stats()
                stats_update_ms = max((time.perf_counter() - stats_started) * 1000.0, 0.0)
                payload_context["stats_reused"] = False
            payload["facts"].extend(self._log_facts())
            payload["facts"].extend(self._decision_facts())
            candles_count: Optional[int] = None
            trades_count: Optional[int] = None
            if series is not None:
                identity = self._series_identity(series)
                public_series_key = identity["series_key"]
                cache = self._push_series_cache.setdefault(public_series_key, {})
                status = str(self.state.get("status") or "").lower()
                series_state = self._series_state_for(series)
                bar_index = series_state.bar_index if series_state else 0
                candles_count = min(bar_index + 1, len(series.candles))
                payload["series_key"] = public_series_key
                payload["facts"].append(
                    self._series_state_fact(
                        series=series,
                        bar_index=bar_index,
                        replace_last=bool(replace_last),
                    )
                )
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
                            payload["facts"].append(
                                {
                                    "fact_type": BOTLENS_FACT_OVERLAY_OPS,
                                    "series_key": public_series_key,
                                    "overlay_delta": dict(overlay_delta),
                                }
                            )
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
                trade_facts, series_stats, trades_count = self._trade_facts(series=series, cache=cache)
                payload["facts"].extend(trade_facts)
                if isinstance(series_stats, AbcMapping):
                    payload["facts"].append(
                        {
                            "fact_type": BOTLENS_FACT_SERIES_STATS,
                            "series_key": public_series_key,
                            "stats": dict(series_stats),
                        }
                    )
                if candle is not None:
                    payload["facts"].append(
                        {
                            "fact_type": BOTLENS_FACT_CANDLE_UPSERTED,
                            "series_key": public_series_key,
                            "candle": candle.to_dict(),
                            "replace_last": bool(replace_last),
                        }
                    )
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
            subscriber_count, dropped_messages = self._broadcast("facts", payload)
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
