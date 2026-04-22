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
BOTLENS_FACT_TRADE_OPENED = "trade_opened"
BOTLENS_FACT_TRADE_UPDATED = "trade_updated"
BOTLENS_FACT_TRADE_CLOSED = "trade_closed"
BOTLENS_FACT_LOG_EMITTED = "log_emitted"
BOTLENS_FACT_DECISION_EMITTED = "decision_emitted"
BOTLENS_RUNTIME_BOOTSTRAP_KIND = "botlens_runtime_bootstrap_facts"
BOTLENS_RUNTIME_FACTS_KIND = "botlens_runtime_facts"
BOTLENS_SELECTED_SYMBOL_VISUAL_REFRESH_INTERVAL_MS = 4_000
BOTLENS_RUNTIME_HEALTH_EMIT_INTERVAL_MS = 15_000
BOTLENS_RUNTIME_HEALTH_DYNAMIC_FIELDS = frozenset(
    {
        "count",
        "timestamp",
        "updated_at",
        "last_seen_at",
        "last_snapshot_at",
        "last_useful_progress_at",
        "next_bar_at",
        "next_bar_in_seconds",
        "known_at",
        "checkpoint_at",
        "observed_at",
    }
)


class RuntimePushStreamMixin:
    def _canonical_start_context(self) -> Any:
        start_context = getattr(self, "_start_context", None)
        if start_context is not None:
            return start_context
        ensure_start_context = getattr(self, "_ensure_start_context", None)
        if callable(ensure_start_context):
            return ensure_start_context()
        return None

    def _canonical_run_id(self) -> str:
        start_context = self._canonical_start_context()
        if start_context is not None:
            run_id = str(getattr(start_context, "run_id", "") or "").strip()
            if run_id:
                return run_id
        if self._run_context is not None:
            run_id = str(getattr(self._run_context, "run_id", "") or "").strip()
            if run_id:
                return run_id
        config = self.config if isinstance(getattr(self, "config", None), AbcMapping) else {}
        configured_run_id = str(config.get("run_id") or "").strip()
        if configured_run_id:
            return configured_run_id
        raise ValueError("run_id is required before canonical BotLens fact append")

    @staticmethod
    def _broadcast_metrics_from_consumer_results(consumer_results: Sequence[Any]) -> tuple[Optional[int], Optional[int]]:
        for result in consumer_results:
            consumer_name = str(getattr(result, "consumer_name", "") or "")
            if consumer_name != "LiveFactsBroadcastConsumer" or getattr(result, "error", None):
                continue
            value = getattr(result, "result", None)
            if (
                isinstance(value, tuple)
                and len(value) == 2
                and all(isinstance(entry, int) for entry in value)
            ):
                return int(value[0]), int(value[1])
        return None, None

    def commit_botlens_fact_payload(
        self,
        payload: Mapping[str, Any],
        *,
        batch_kind: str,
        dispatch: bool = True,
    ) -> Any:
        run_id = self._canonical_run_id()
        start_context = self._canonical_start_context()
        appender = getattr(self, "_canonical_fact_appender", None)
        if appender is None:
            raise RuntimeError("bot runtime canonical fact appender is not configured")
        return appender.append_fact_batch(
            bot_id=self.bot_id,
            run_id=run_id,
            batch_kind=batch_kind,
            payload=payload,
            context={
                "worker_id": (
                    getattr(start_context, "worker_id", None)
                    if start_context is not None
                    else self.config.get("worker_id")
                ),
                "source_emitter": "bot_runtime",
                "source_reason": "producer",
            },
            dispatch=dispatch,
        )

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
    def _trade_payload_is_open(trade_payload: Mapping[str, Any]) -> bool:
        if trade_payload.get("closed_at"):
            return False
        trade_state = str(trade_payload.get("trade_state") or "").strip().lower()
        if trade_state:
            return trade_state not in {"closed"}
        status = str(trade_payload.get("status") or "").strip().lower()
        if status in {"closed", "completed", "complete"}:
            trade_id = str(trade_payload.get("trade_id") or "").strip() or "<missing>"
            raise RuntimeError(
                "bot_runtime_trade_snapshot_invalid: closed trade snapshot missing closed_at "
                f"trade_id={trade_id} status={status}"
            )
        return True

    @staticmethod
    def _assert_trade_fact_completeness(
        *,
        trade_ids: set[str],
        closed_trade_ids: set[str],
        emitted_trade_ids: set[str],
        emitted_open_trade_ids: set[str],
        emitted_closed_trade_ids: set[str],
        series: StrategySeries,
    ) -> None:
        missing_trade_ids = sorted(trade_ids - emitted_trade_ids)
        if missing_trade_ids:
            raise RuntimeError(
                "bot_runtime_trade_fact_missing: runtime observed trades without any emitted canonical trade fact "
                f"strategy_id={series.strategy_id} symbol={series.symbol} timeframe={series.timeframe} "
                f"trade_ids={','.join(missing_trade_ids)}"
            )
        missing_open_trade_ids = sorted(trade_ids - emitted_open_trade_ids)
        if missing_open_trade_ids:
            raise RuntimeError(
                "bot_runtime_trade_open_fact_missing: observed trades missing emitted TRADE_OPENED fact "
                f"strategy_id={series.strategy_id} symbol={series.symbol} timeframe={series.timeframe} "
                f"trade_ids={','.join(missing_open_trade_ids)}"
            )
        missing_closed_trade_ids = sorted(closed_trade_ids - emitted_closed_trade_ids)
        if missing_closed_trade_ids:
            raise RuntimeError(
                "bot_runtime_trade_close_fact_missing: closed trades missing emitted TRADE_CLOSED fact "
                f"strategy_id={series.strategy_id} symbol={series.symbol} timeframe={series.timeframe} "
                f"trade_ids={','.join(missing_closed_trade_ids)}"
            )

    @staticmethod
    def _trade_fact_type(
        *,
        trade_id: str,
        trade_is_open: bool,
        cached_trade_map: Mapping[str, str],
        emitted_closed_trade_ids: set[str],
    ) -> str:
        if trade_is_open:
            if trade_id not in cached_trade_map:
                return BOTLENS_FACT_TRADE_OPENED
            return BOTLENS_FACT_TRADE_UPDATED
        if trade_id not in emitted_closed_trade_ids:
            return BOTLENS_FACT_TRADE_CLOSED
        return BOTLENS_FACT_TRADE_UPDATED

    @staticmethod
    def _trade_payload_timestamp(trade_payload: Mapping[str, Any], *keys: str) -> Optional[Any]:
        for key in keys:
            value = trade_payload.get(key)
            if value not in (None, ""):
                return value
        return None

    @staticmethod
    def _open_trade_payload_from_closed_trade(trade_payload: Mapping[str, Any]) -> Dict[str, Any]:
        opened = dict(trade_payload)
        for key in (
            "closed_at",
            "exit_time",
            "exit_price",
            "realized_pnl",
            "event_impact_pnl",
            "trade_net_pnl",
            "net_pnl",
            "pnl",
            "bars_held",
            "close_reason",
            "closed_reason",
            "exit_kind",
        ):
            opened.pop(key, None)
        status = str(opened.get("status") or "").strip().lower()
        if status in {"closed", "completed", "complete"}:
            opened["status"] = "open"
        opened["trade_state"] = "open"
        opened.pop("event_time", None)
        entry_time = RuntimePushStreamMixin._trade_payload_timestamp(opened, "opened_at", "entry_time")
        if entry_time not in (None, ""):
            opened["bar_time"] = entry_time
        return opened

    def _trade_lineage_fields(self, *, trade_id: str, series: StrategySeries) -> Dict[str, Any]:
        run_context = getattr(self, "_run_context", None)
        if run_context is None:
            return {}
        for event in reversed(getattr(run_context, "runtime_events", ())):
            if getattr(getattr(event, "event_name", None), "value", None) != "DECISION_ACCEPTED":
                continue
            context = getattr(event, "context", None)
            if str(getattr(context, "trade_id", "") or "") != trade_id:
                continue
            return {
                "strategy_id": getattr(context, "strategy_id", None),
                "signal_id": getattr(context, "signal_id", None),
                "decision_id": getattr(context, "decision_id", None),
            }
        return {"strategy_id": getattr(series, "strategy_id", None)}

    def _enriched_trade_payload(
        self,
        *,
        trade_payload: Mapping[str, Any],
        fact_type: str,
        series: StrategySeries,
        bar_time: Optional[Any],
    ) -> Dict[str, Any]:
        enriched = dict(trade_payload)
        trade_id = str(enriched.get("trade_id") or "").strip()
        if fact_type == BOTLENS_FACT_TRADE_OPENED:
            event_bar_time = self._trade_payload_timestamp(enriched, "opened_at", "entry_time", "bar_time") or bar_time
        elif fact_type == BOTLENS_FACT_TRADE_CLOSED:
            event_bar_time = self._trade_payload_timestamp(enriched, "closed_at", "exit_time", "bar_time") or bar_time
        else:
            event_bar_time = (
                self._trade_payload_timestamp(enriched, "bar_time", "updated_at", "closed_at", "exit_time")
                or bar_time
                or self._trade_payload_timestamp(enriched, "opened_at", "entry_time")
            )
        if event_bar_time not in (None, ""):
            enriched["bar_time"] = event_bar_time
            enriched.setdefault("event_time", event_bar_time)
        lineage = self._trade_lineage_fields(trade_id=trade_id, series=series) if trade_id else {}
        enriched.setdefault("strategy_id", lineage.get("strategy_id") or getattr(series, "strategy_id", None))
        if lineage.get("signal_id"):
            enriched.setdefault("signal_id", lineage["signal_id"])
        if lineage.get("decision_id"):
            enriched.setdefault("decision_id", lineage["decision_id"])
        return enriched

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
        series_state = self._series_state_for(series)
        refresh_overlays = getattr(self, "_refresh_indicator_overlays_for_state", None)
        if series_state is not None and callable(refresh_overlays):
            refresh_overlays(
                series_state,
                reason="visible_overlays",
            )
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

    @staticmethod
    def _is_empty_health_value(value: Any) -> bool:
        return value in (None, "", [], {}, ())

    def _stable_health_value(self, value: Any) -> Any:
        if isinstance(value, AbcMapping):
            normalized: Dict[str, Any] = {}
            for raw_key in sorted(value.keys(), key=lambda entry: str(entry)):
                key = str(raw_key or "")
                if not key or key in BOTLENS_RUNTIME_HEALTH_DYNAMIC_FIELDS:
                    continue
                normalized_value = self._stable_health_value(value.get(raw_key))
                if self._is_empty_health_value(normalized_value):
                    continue
                normalized[key] = normalized_value
            return normalized
        if isinstance(value, list):
            normalized_items = [self._stable_health_value(entry) for entry in value]
            normalized_items = [entry for entry in normalized_items if not self._is_empty_health_value(entry)]
            if normalized_items and all(isinstance(entry, AbcMapping) for entry in normalized_items):
                normalized_items.sort(key=lambda entry: json.dumps(entry, sort_keys=True, default=str, separators=(",", ":")))
            return normalized_items
        return value

    def _normalized_runtime_warning(self, warning: Mapping[str, Any]) -> Dict[str, Any]:
        normalized = {
            "warning_id": str(warning.get("warning_id") or warning.get("id") or "").strip(),
            "warning_type": str(warning.get("warning_type") or warning.get("type") or "").strip().lower() or "runtime_warning",
            "severity": str(warning.get("severity") or warning.get("level") or "").strip().lower() or "warning",
            "source": str(warning.get("source") or "runtime").strip().lower() or "runtime",
            "indicator_id": str(warning.get("indicator_id") or "").strip(),
            "symbol_key": str(warning.get("symbol_key") or "").strip(),
            "symbol": str(warning.get("symbol") or "").strip(),
            "timeframe": str(warning.get("timeframe") or "").strip(),
            "title": str(warning.get("title") or "").strip(),
            "message": str(warning.get("message") or "").strip(),
        }
        context = self._stable_health_value(warning.get("context"))
        if not self._is_empty_health_value(context):
            normalized["context"] = context
        return {key: value for key, value in normalized.items() if not self._is_empty_health_value(value)}

    def _runtime_health_fingerprint(self, runtime_snapshot: Mapping[str, Any]) -> str:
        warnings = runtime_snapshot.get("warnings") if isinstance(runtime_snapshot.get("warnings"), list) else []
        normalized_warnings = [
            self._normalized_runtime_warning(entry)
            for entry in warnings
            if isinstance(entry, AbcMapping)
        ]
        normalized_warnings.sort(
            key=lambda entry: (
                str(entry.get("warning_id") or ""),
                str(entry.get("warning_type") or ""),
                str(entry.get("symbol_key") or ""),
                str(entry.get("symbol") or ""),
                str(entry.get("message") or ""),
            )
        )
        payload: Dict[str, Any] = {
            "status": str(runtime_snapshot.get("status") or "").strip().lower(),
            "runtime_state": str(runtime_snapshot.get("runtime_state") or "").strip().lower(),
            "progress_state": str(runtime_snapshot.get("progress_state") or "").strip().lower(),
            "warning_count": len(normalized_warnings),
            "warnings": normalized_warnings,
        }
        for field in ("degraded", "churn", "terminal"):
            normalized = self._stable_health_value(runtime_snapshot.get(field))
            if not self._is_empty_health_value(normalized):
                payload[field] = normalized
        return json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))

    def _mark_runtime_health_emitted(self, fingerprint: str, *, emitted_monotonic: float) -> None:
        self._push_runtime_health_fingerprint = str(fingerprint or "")
        self._push_runtime_health_emitted_monotonic = max(float(emitted_monotonic or 0.0), 0.0)

    def _should_emit_runtime_state_fact(self, *, runtime_snapshot: Mapping[str, Any], event: str) -> bool:
        normalized_event = str(event or "").strip().lower()
        emitted_monotonic = time.monotonic()
        fingerprint = self._runtime_health_fingerprint(runtime_snapshot)
        if normalized_event not in {"bar", "intrabar"}:
            self._mark_runtime_health_emitted(fingerprint, emitted_monotonic=emitted_monotonic)
            return True
        previous_fingerprint = str(getattr(self, "_push_runtime_health_fingerprint", "") or "")
        last_emitted = float(getattr(self, "_push_runtime_health_emitted_monotonic", 0.0) or 0.0)
        heartbeat_ms = float(
            getattr(self, "_runtime_health_emit_interval_ms", BOTLENS_RUNTIME_HEALTH_EMIT_INTERVAL_MS)
            or BOTLENS_RUNTIME_HEALTH_EMIT_INTERVAL_MS
        )
        if not previous_fingerprint or previous_fingerprint != fingerprint:
            self._mark_runtime_health_emitted(fingerprint, emitted_monotonic=emitted_monotonic)
            return True
        elapsed_ms = max((emitted_monotonic - last_emitted) * 1000.0, 0.0)
        if elapsed_ms >= heartbeat_ms:
            self._mark_runtime_health_emitted(fingerprint, emitted_monotonic=emitted_monotonic)
            return True
        return False

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
        bar_time: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, bool]:
        trades_revision = self._trade_revision(series)
        if cache.get("trades_revision") == trades_revision:
            cached_trades = cache.get("trades")
            return [], cache.get("series_stats"), len(cached_trades) if isinstance(cached_trades, list) else 0, False

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
        emitted_trade_ids = {
            str(entry)
            for entry in cache.get("emitted_trade_ids", ())
            if str(entry).strip()
        }
        emitted_closed_trade_ids = {
            str(entry)
            for entry in cache.get("emitted_closed_trade_ids", ())
            if str(entry).strip()
        }
        emitted_open_trade_ids = {
            str(entry)
            for entry in cache.get("emitted_open_trade_ids", ())
            if str(entry).strip()
        }
        next_emitted_trade_ids = set(emitted_trade_ids)
        next_emitted_open_trade_ids = set(emitted_open_trade_ids)
        next_emitted_closed_trade_ids = set(emitted_closed_trade_ids)
        next_open_trade_ids: set[str] = set()
        current_trade_ids: set[str] = set()
        current_closed_trade_ids: set[str] = set()
        trade_facts: List[Dict[str, Any]] = []
        trade_entry_refresh_required = False
        for trade in trades:
            if not isinstance(trade, AbcMapping):
                continue
            trade_payload = dict(trade)
            trade_id = str(trade_payload.get("trade_id") or "").strip()
            if not trade_id:
                continue
            current_trade_ids.add(trade_id)
            trade_is_open = self._trade_payload_is_open(trade_payload)
            if trade_is_open:
                next_open_trade_ids.add(trade_id)
            else:
                current_closed_trade_ids.add(trade_id)
            fact_type = self._trade_fact_type(
                trade_id=trade_id,
                trade_is_open=trade_is_open,
                cached_trade_map=cached_trade_map,
                emitted_closed_trade_ids=emitted_closed_trade_ids,
            )
            enriched_trade_payload = self._enriched_trade_payload(
                trade_payload=trade_payload,
                fact_type=fact_type,
                series=series,
                bar_time=bar_time,
            )
            trade_fingerprint = json.dumps(enriched_trade_payload, sort_keys=True, default=str, separators=(",", ":"))
            next_trade_map[trade_id] = trade_fingerprint
            trade_changed = cached_trade_map.get(trade_id) != trade_fingerprint

            if trade_is_open:
                if trade_id in emitted_open_trade_ids and not trade_changed:
                    continue
                if trade_id not in emitted_open_trade_ids:
                    fact_type = BOTLENS_FACT_TRADE_OPENED
                    enriched_trade_payload = self._enriched_trade_payload(
                        trade_payload=trade_payload,
                        fact_type=fact_type,
                        series=series,
                        bar_time=bar_time,
                    )
                    next_emitted_open_trade_ids.add(trade_id)
                else:
                    fact_type = BOTLENS_FACT_TRADE_UPDATED
            else:
                if trade_id not in emitted_open_trade_ids:
                    opened_payload = self._open_trade_payload_from_closed_trade(trade_payload)
                    opened_payload = self._enriched_trade_payload(
                        trade_payload=opened_payload,
                        fact_type=BOTLENS_FACT_TRADE_OPENED,
                        series=series,
                        bar_time=bar_time,
                    )
                    trade_facts.append(
                        {
                            "fact_type": BOTLENS_FACT_TRADE_OPENED,
                            "series_key": identity["series_key"],
                            "trade": opened_payload,
                        }
                    )
                    trade_entry_refresh_required = True
                    next_emitted_trade_ids.add(trade_id)
                    next_emitted_open_trade_ids.add(trade_id)
                if trade_id in emitted_closed_trade_ids and not trade_changed:
                    continue
                if trade_id not in emitted_closed_trade_ids:
                    fact_type = BOTLENS_FACT_TRADE_CLOSED
                    next_emitted_closed_trade_ids.add(trade_id)
                else:
                    fact_type = BOTLENS_FACT_TRADE_UPDATED

            if fact_type == BOTLENS_FACT_TRADE_OPENED:
                trade_entry_refresh_required = True
            next_emitted_trade_ids.add(trade_id)
            trade_facts.append(
                {
                    "fact_type": fact_type,
                    "series_key": identity["series_key"],
                    "trade": enriched_trade_payload,
                }
            )

        self._assert_trade_fact_completeness(
            trade_ids=current_trade_ids,
            closed_trade_ids=current_closed_trade_ids,
            emitted_trade_ids=next_emitted_trade_ids,
            emitted_open_trade_ids=next_emitted_open_trade_ids,
            emitted_closed_trade_ids=next_emitted_closed_trade_ids,
            series=series,
        )
        cache["trades"] = trades
        cache["trades_revision"] = trades_revision
        cache["series_stats"] = series_stats
        cache["trade_fingerprints"] = next_trade_map
        cache["open_trade_ids"] = tuple(sorted(next_open_trade_ids))
        cache["emitted_trade_ids"] = tuple(sorted(next_emitted_trade_ids))
        cache["emitted_open_trade_ids"] = tuple(sorted(next_emitted_open_trade_ids))
        cache["emitted_closed_trade_ids"] = tuple(sorted(next_emitted_closed_trade_ids))
        return trade_facts, series_stats, len(trades), trade_entry_refresh_required

    def _should_emit_visual_overlay_facts(
        self,
        cache: Dict[str, Any],
        *,
        event: str,
        overlay_revision: Tuple[Any, ...] | None,
        trade_entry_refresh_required: bool,
    ) -> bool:
        if str(event or "").strip().lower() == "intrabar":
            return False
        if overlay_revision is None:
            return False
        if cache.get("visual_overlay_revision") == overlay_revision:
            return False
        if trade_entry_refresh_required:
            return True
        last_emit_monotonic = float(cache.get("visual_overlay_emit_monotonic") or 0.0)
        if last_emit_monotonic <= 0.0:
            cache["visual_overlay_emit_monotonic"] = time.monotonic()
            return False
        elapsed_ms = max((time.monotonic() - last_emit_monotonic) * 1000.0, 0.0)
        return elapsed_ms >= float(BOTLENS_SELECTED_SYMBOL_VISUAL_REFRESH_INTERVAL_MS)

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
                trade_payload = dict(trade)
                trade_is_open = self._trade_payload_is_open(trade_payload)
                if not trade_is_open:
                    opened_payload = self._open_trade_payload_from_closed_trade(trade_payload)
                    facts.append(
                        {
                            "fact_type": BOTLENS_FACT_TRADE_OPENED,
                            "series_key": identity["series_key"],
                            "trade": self._enriched_trade_payload(
                                trade_payload=opened_payload,
                                fact_type=BOTLENS_FACT_TRADE_OPENED,
                                series=series,
                                bar_time=None,
                            ),
                        }
                    )
                fact_type = BOTLENS_FACT_TRADE_OPENED if trade_is_open else BOTLENS_FACT_TRADE_CLOSED
                facts.append(
                    {
                        "fact_type": fact_type,
                        "series_key": identity["series_key"],
                        "trade": self._enriched_trade_payload(
                            trade_payload=trade_payload,
                            fact_type=fact_type,
                            series=series,
                            bar_time=None,
                        ),
                    }
                )
        for entry in chart_snapshot.get("logs") if isinstance(chart_snapshot.get("logs"), list) else []:
            if isinstance(entry, AbcMapping):
                facts.append({"fact_type": BOTLENS_FACT_LOG_EMITTED, "log": dict(entry)})
        for entry in chart_snapshot.get("decisions") if isinstance(chart_snapshot.get("decisions"), list) else []:
            if isinstance(entry, AbcMapping):
                facts.append({"fact_type": BOTLENS_FACT_DECISION_EMITTED, "decision": dict(entry)})
        observed_at = _isoformat(datetime.now(timezone.utc))
        last_candle_time = None
        candles = selected_series.get("candles") if isinstance(selected_series.get("candles"), list) else []
        if candles:
            last_candle = candles[-1]
            if isinstance(last_candle, AbcMapping):
                last_candle_time = last_candle.get("time")
        return {
            "type": "facts",
            "event": "bootstrap",
            "known_at": last_candle_time or runtime_snapshot.get("known_at") or runtime_snapshot.get("last_snapshot_at") or observed_at,
            "observed_at": observed_at,
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
        canonical_append_ms: Optional[float] = None
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
            emit_runtime_state = self._should_emit_runtime_state_fact(runtime_snapshot=runtime_snapshot, event=event)
            observed_at = _isoformat(push_started)
            last_bar = runtime_snapshot.get("last_bar") if isinstance(runtime_snapshot.get("last_bar"), AbcMapping) else {}
            candle_time = getattr(candle, "time", None) if candle is not None else None
            payload: Dict[str, Any] = {
                "type": "facts",
                "event": event,
                "known_at": (_isoformat(candle_time) if candle_time is not None else None)
                or last_bar.get("time")
                or runtime_snapshot.get("known_at")
                or runtime_snapshot.get("last_snapshot_at")
                or observed_at,
                "observed_at": observed_at,
                "facts": [],
            }
            runtime_state_fact: Optional[Dict[str, Any]] = None
            if emit_runtime_state:
                runtime_state_fact = self._runtime_state_fact(runtime_snapshot=runtime_snapshot, event=event)
                payload["facts"].append(runtime_state_fact)
            if isinstance(precomputed_stats, Mapping):
                if runtime_state_fact is not None:
                    runtime_state_fact["runtime"]["stats"] = dict(precomputed_stats)
                stats_update_ms = 0.0
                payload_context["stats_reused"] = True
            elif emit_runtime_state:
                stats_started = time.perf_counter()
                self._aggregate_stats()
                stats_update_ms = max((time.perf_counter() - stats_started) * 1000.0, 0.0)
                payload_context["stats_reused"] = False
            else:
                stats_update_ms = 0.0
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
                refresh_overlays = getattr(self, "_refresh_indicator_overlays_for_state", None)
                if series_state is not None and callable(refresh_overlays):
                    refresh_overlays(
                        series_state,
                        candle=candle,
                        reason="push_update",
                    )
                candles_count = min(bar_index + 1, len(series.candles))
                payload["series_key"] = public_series_key
                payload["facts"].append(
                    self._series_state_fact(
                        series=series,
                        bar_index=bar_index,
                        replace_last=bool(replace_last),
                    )
                )
                overlay_revision = self._series_overlay_revision(series, status=status)
                if cache.get("overlay_revision") != overlay_revision or "visible_overlays" not in cache:
                    cache["visible_overlays"] = self._series_visible_overlays(series, status=status)
                    cache["overlay_revision"] = overlay_revision
                visible_overlays = cache.get("visible_overlays")
                trade_facts, series_stats, trades_count, trade_entry_refresh_required = self._trade_facts(
                    series=series,
                    cache=cache,
                    bar_time=candle_time,
                )
                if isinstance(visible_overlays, list):
                    overlay_summary = self._overlay_summary(visible_overlays)
                    emit_visual_overlay_facts = self._should_emit_visual_overlay_facts(
                        cache,
                        event=event,
                        overlay_revision=overlay_revision,
                        trade_entry_refresh_required=trade_entry_refresh_required,
                    )
                    if emit_visual_overlay_facts:
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
                                    emitted_delta=True,
                                    trade_entry_refresh_required=trade_entry_refresh_required,
                                ),
                            )
                        )
                        overlay_delta = build_overlay_delta(cache, visible_overlays)
                        if isinstance(overlay_delta, Mapping):
                            payload["facts"].append(
                                {
                                    "fact_type": BOTLENS_FACT_OVERLAY_OPS,
                                    "series_key": public_series_key,
                                    "overlay_delta": dict(overlay_delta),
                                }
                            )
                            cache["visual_overlay_revision"] = overlay_revision
                            cache["visual_overlay_emit_monotonic"] = time.monotonic()
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
                                        trade_entry_refresh_required=trade_entry_refresh_required,
                                    ),
                                )
                            )
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
                    "runtime_state_emitted": emit_runtime_state,
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
            append_outcome = None
            if payload.get("facts"):
                append_started = time.perf_counter()
                append_outcome = self.commit_botlens_fact_payload(
                    payload,
                    batch_kind=BOTLENS_RUNTIME_FACTS_KIND,
                    dispatch=False,
                )
                canonical_append_ms = max((time.perf_counter() - append_started) * 1000.0, 0.0)
                payload_context["canonical_append_ms"] = canonical_append_ms
            if append_outcome is not None:
                dispatch_started = time.perf_counter()
                consumer_results = self._canonical_fact_appender.dispatch(append_outcome.batch)
                enqueue_ms = max((time.perf_counter() - dispatch_started) * 1000.0, 0.0)
                payload = append_outcome.batch.live_payload
                subscriber_count, dropped_messages = self._broadcast_metrics_from_consumer_results(consumer_results)
            else:
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
