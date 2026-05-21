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

from ..components.canonical_facts import canonical_fact_payload
from ..core import _isoformat
from ..components.overlay_delta import (
    build_overlay_delta,
    compact_overlay_for_transport,
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
BOTLENS_FACT_WALLET_EVENT = "wallet_ledger_event"
BOTLENS_FACT_LOG_EMITTED = "log_emitted"
BOTLENS_FACT_DECISION_EMITTED = "decision_emitted"
BOTLENS_RUNTIME_BOOTSTRAP_KIND = "botlens_runtime_bootstrap_facts"
BOTLENS_RUNTIME_FACTS_KIND = "botlens_runtime_facts"
BOTLENS_FACT_STREAM_SURFACES = (
    "candles",
    "overlays",
    "health_runtime_state",
    "trades",
    "decisions",
    "wallet",
    "diagnostics",
    "symbol_summary",
    "run_summary",
)
BOTLENS_FACT_STREAM_SURFACE_BY_FACT_TYPE = {
    BOTLENS_FACT_CANDLE_UPSERTED: "candles",
    BOTLENS_FACT_OVERLAY_OPS: "overlays",
    BOTLENS_FACT_RUNTIME_STATE: "health_runtime_state",
    BOTLENS_FACT_TRADE_OPENED: "trades",
    BOTLENS_FACT_TRADE_UPDATED: "trades",
    BOTLENS_FACT_TRADE_CLOSED: "trades",
    BOTLENS_FACT_DECISION_EMITTED: "decisions",
    BOTLENS_FACT_WALLET_EVENT: "wallet",
    BOTLENS_FACT_LOG_EMITTED: "diagnostics",
    BOTLENS_FACT_SERIES_STATE: "symbol_summary",
    BOTLENS_FACT_SERIES_STATS: "symbol_summary",
}
BOTLENS_SELECTED_SYMBOL_VISUAL_REFRESH_INTERVAL_MS = 4_000
BOTLENS_RUNTIME_HEALTH_EMIT_INTERVAL_MS = 15_000
BOTLENS_FACT_STREAM_LOG_FACT_LIMIT = 32
BOTLENS_FACT_STREAM_DECISION_FACT_LIMIT = 64
BOTLENS_FACT_STREAM_OVERLAY_POINT_LIMIT = 160
BOTLENS_BOOTSTRAP_CLOSED_TRADE_LIMIT = 240
BOTLENS_COMPACT_SERIES_STATS_KEYS = frozenset(
    {
        "total_trades",
        "completed_trades",
        "wins",
        "losses",
        "win_rate",
        "gross_pnl",
        "fees_paid",
        "total_fees",
        "net_pnl",
        "avg_win",
        "avg_loss",
        "largest_win",
        "largest_loss",
        "max_drawdown",
        "quote_currency",
    }
)
BOTLENS_RUNTIME_HEALTH_TRANSITION_LIMIT = 4
_WALLET_LEDGER_EVENT_ORDER = {
    "WALLET_INITIALIZED": 0,
    "MARGIN_RESERVED": 10,
    "MARGIN_REJECTED": 10,
    "MARGIN_RELEASED": 10,
    "FEE_APPLIED": 20,
    "REALIZED_PNL_APPLIED": 30,
    "POSITION_OPENED": 40,
    "POSITION_CLOSED": 40,
    "EQUITY_UPDATED": 50,
}
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
    @staticmethod
    def _botlens_fact_stream_surface_for_fact(fact: Mapping[str, Any]) -> str:
        fact_type = str(fact.get("fact_type") or "").strip().lower()
        return BOTLENS_FACT_STREAM_SURFACE_BY_FACT_TYPE.get(fact_type, "diagnostics")

    @staticmethod
    def _botlens_fact_stream_surface_metrics(
        payload: Mapping[str, Any],
        *,
        include_bytes: bool = False,
    ) -> Dict[str, Any]:
        facts = [entry for entry in payload.get("facts", []) if isinstance(entry, AbcMapping)]
        counts = {surface: 0 for surface in BOTLENS_FACT_STREAM_SURFACES}
        byte_counts = {surface: 0 for surface in BOTLENS_FACT_STREAM_SURFACES}
        for fact in facts:
            surface = RuntimePushStreamMixin._botlens_fact_stream_surface_for_fact(fact)
            counts[surface] = counts.get(surface, 0) + 1
            if include_bytes:
                byte_counts[surface] = byte_counts.get(surface, 0) + len(
                    json.dumps(fact, separators=(",", ":"), default=str).encode("utf-8")
                )
        metrics: Dict[str, Any] = {
            "botlens_fact_stream_fact_count": len(facts),
            "botlens_fact_stream_surface_count": sum(1 for value in counts.values() if value > 0),
        }
        for surface in BOTLENS_FACT_STREAM_SURFACES:
            metrics[f"botlens_fact_stream_{surface}_fact_count"] = counts.get(surface, 0)
            if include_bytes:
                metrics[f"botlens_fact_stream_{surface}_payload_bytes"] = byte_counts.get(surface, 0)
        return metrics

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
        leg_state = tuple(
            (
                str(getattr(leg, "leg_id", "") or ""),
                str(getattr(leg, "status", "") or ""),
                round(float(getattr(leg, "contracts", 0.0) or 0.0), 12),
                round(float(getattr(leg, "pnl", 0.0) or 0.0), 4),
                str(getattr(leg, "exit_time", "") or ""),
            )
            for leg in legs
        )
        return (
            len(trades),
            str(getattr(last, "trade_id", "") or ""),
            last_closed_at,
            open_legs,
            last_net_pnl,
            round(float(getattr(last, "stop_price", 0.0) or 0.0), 12),
            bool(getattr(last, "moved_to_breakeven", False)),
            bool(getattr(last, "trailing_active", False)),
            leg_state,
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
    def _trade_payload_legs(trade_payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
        legs = trade_payload.get("legs")
        if not isinstance(legs, list):
            return []
        return [dict(leg) for leg in legs if isinstance(leg, AbcMapping)]

    @staticmethod
    def _finite_trade_float(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not numeric == numeric or numeric in (float("inf"), float("-inf")):
            return None
        return numeric

    @classmethod
    def _trade_quantity_from_legs(cls, legs: Sequence[Mapping[str, Any]]) -> Optional[float]:
        total = 0.0
        observed = False
        for leg in legs:
            contracts = cls._finite_trade_float(leg.get("contracts"))
            if contracts is None:
                continue
            total += max(contracts, 0.0)
            observed = True
        return total if observed else None

    @classmethod
    def _weighted_exit_price_from_legs(cls, legs: Sequence[Mapping[str, Any]]) -> Optional[float]:
        weighted = 0.0
        contracts_total = 0.0
        for leg in legs:
            exit_price = cls._finite_trade_float(leg.get("exit_price"))
            contracts = cls._finite_trade_float(leg.get("contracts"))
            if exit_price is None or contracts is None:
                continue
            contracts = max(contracts, 0.0)
            weighted += exit_price * contracts
            contracts_total += contracts
        if contracts_total <= 0:
            return None
        return weighted / contracts_total

    @staticmethod
    def _close_reason_from_legs(legs: Sequence[Mapping[str, Any]]) -> Optional[str]:
        statuses = {
            str(leg.get("status") or "").strip().lower()
            for leg in legs
            if str(leg.get("status") or "").strip().lower() and str(leg.get("status") or "").strip().lower() != "open"
        }
        if not statuses:
            return None
        if statuses <= {"target"}:
            return "TARGET"
        if statuses <= {"stop"}:
            return "STOP"
        if statuses <= {"backtest_end"}:
            return "BACKTEST_END"
        return "MIXED"

    @staticmethod
    def _open_trade_payload_from_closed_trade(trade_payload: Mapping[str, Any]) -> Dict[str, Any]:
        opened = dict(trade_payload)
        final_position_seq = RuntimePushStreamMixin._trade_payload_int(opened.get("position_commit_seq"))
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
            "reason_code",
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
        if final_position_seq is None or final_position_seq <= 0:
            raise RuntimeError(
                "bot_runtime_trade_lifecycle_position_clock_missing: "
                f"closed trade cannot synthesize open event without position_commit_seq "
                f"trade_id={opened.get('trade_id') or opened.get('id')}"
            )
        opened["position_commit_seq"] = int(opened.get("position_open_commit_seq") or 1)
        opened["position_commit_seq_status"] = str(
            opened.get("position_commit_seq_status") or "position_scoped"
        )
        return opened

    @staticmethod
    def _trade_payload_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

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
        position_commit_seq = self._trade_payload_int(enriched.get("position_commit_seq"))
        if position_commit_seq is None or position_commit_seq <= 0:
            raise RuntimeError(
                "bot_runtime_trade_lifecycle_position_clock_missing: "
                f"position_commit_seq is required for trade lifecycle fact trade_id={trade_id or 'unknown'}"
            )
        enriched["position_commit_seq"] = position_commit_seq
        enriched["position_commit_seq_status"] = str(
            enriched.get("position_commit_seq_status") or "position_scoped"
        )
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
        direction = str(enriched.get("direction") or enriched.get("side") or "").strip().lower()
        if direction and not enriched.get("side"):
            enriched["side"] = direction
        if enriched.get("entry_time") and not enriched.get("opened_at"):
            enriched["opened_at"] = enriched.get("entry_time")

        legs = self._trade_payload_legs(enriched)
        quantity = self._finite_trade_float(enriched.get("quantity"))
        if quantity is None:
            quantity = self._finite_trade_float(enriched.get("qty"))
        if quantity is None:
            quantity = self._trade_quantity_from_legs(legs)
        if quantity is not None:
            enriched.setdefault("quantity", round(quantity, 12))
            enriched.setdefault("qty", round(quantity, 12))

        exit_time = self._trade_payload_timestamp(enriched, "exit_time", "closed_at")
        if exit_time not in (None, ""):
            enriched.setdefault("exit_time", exit_time)
        if fact_type == BOTLENS_FACT_TRADE_CLOSED and not enriched.get("exit_price"):
            weighted_exit = self._weighted_exit_price_from_legs(legs)
            if weighted_exit is not None:
                enriched["exit_price"] = round(weighted_exit, 4)
        if fact_type == BOTLENS_FACT_TRADE_CLOSED and not enriched.get("close_reason"):
            close_reason = str(enriched.get("reason_code") or "").strip().upper() or self._close_reason_from_legs(legs)
            if close_reason:
                enriched["close_reason"] = close_reason
                enriched.setdefault("reason_code", close_reason)
        if legs:
            enriched["legs"] = [
                {
                    **leg,
                    **(
                        {"gross_pnl": round(float(leg.get("pnl")), 4)}
                        if self._finite_trade_float(leg.get("pnl")) is not None and leg.get("gross_pnl") in (None, "")
                        else {}
                    ),
                }
                for leg in legs
            ]
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
        refresh: bool = True,
    ) -> List[Dict[str, Any]]:
        series_state = self._series_state_for(series)
        refresh_overlays = getattr(self, "_refresh_indicator_overlays_for_state", None)
        if refresh and series_state is not None and callable(refresh_overlays):
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
                overlay_payload_fingerprint(
                    compact_overlay_for_transport(
                        overlay,
                        key=overlay_cache_key(overlay, index),
                        max_payload_items=int(
                            getattr(
                                self,
                                "_botlens_fact_stream_overlay_point_limit",
                                BOTLENS_FACT_STREAM_OVERLAY_POINT_LIMIT,
                            )
                            or BOTLENS_FACT_STREAM_OVERLAY_POINT_LIMIT
                        ),
                    )
                ),
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
        return build_overlay_delta(
            cache,
            overlays,
            max_payload_items=int(
                getattr(
                    self,
                    "_botlens_fact_stream_overlay_point_limit",
                    BOTLENS_FACT_STREAM_OVERLAY_POINT_LIMIT,
                )
                or BOTLENS_FACT_STREAM_OVERLAY_POINT_LIMIT
            ),
        )

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

    def _bounded_entries_after_marker(
        self,
        entries: Sequence[Mapping[str, Any]],
        *,
        marker_field: str,
        previous_marker: Optional[str],
        limit: int,
        fact_type: str,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], int]:
        new_entries, marker = self._entries_after_marker(
            entries,
            marker_field=marker_field,
            previous_marker=previous_marker,
        )
        resolved_limit = max(int(limit or 0), 1)
        dropped = max(len(new_entries) - resolved_limit, 0)
        if dropped > 0:
            logger.warning(
                with_log_context(
                    "botlens_fact_stream_batch_truncated",
                    self._runtime_log_context(
                        fact_type=fact_type,
                        emitted_entries=resolved_limit,
                        dropped_entries=dropped,
                        limit=resolved_limit,
                    ),
                )
            )
            new_entries = new_entries[-resolved_limit:]
        return new_entries, marker, dropped

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
        }

    def _runtime_state_fact(self, *, runtime_snapshot: Mapping[str, Any], event: str) -> Dict[str, Any]:
        return {
            "fact_type": BOTLENS_FACT_RUNTIME_STATE,
            "event": str(event or ""),
            "runtime": self._compact_runtime_state_snapshot(runtime_snapshot),
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
        return {key: value for key, value in normalized.items() if not self._is_empty_health_value(value)}

    @staticmethod
    def _compact_runtime_state_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return max(int(value), 0)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _compact_runtime_state_text(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None

    def _compact_runtime_state_fields(self, value: Any, fields: Sequence[str]) -> Dict[str, Any]:
        if not isinstance(value, AbcMapping):
            return {}
        compacted: Dict[str, Any] = {}
        for field in fields:
            raw_value = value.get(field)
            if raw_value in (None, "", [], {}, ()):
                continue
            if field.endswith("_count") or field in {"expected_workers", "reported_workers", "duration_ms", "value"}:
                int_value = self._compact_runtime_state_int(raw_value)
                if int_value is not None:
                    compacted[field] = int_value
                continue
            compacted[field] = raw_value
        return compacted

    def _compact_runtime_pressure(self, value: Any) -> Dict[str, Any]:
        if not isinstance(value, AbcMapping):
            return {}
        compacted: Dict[str, Any] = {}
        for key in ("trigger", "trigger_event"):
            text = self._compact_runtime_state_text(value.get(key))
            if text:
                compacted[key] = text
        top_pressure = value.get("top_pressure")
        if isinstance(top_pressure, AbcMapping):
            pressure: Dict[str, Any] = {}
            reason_code = self._compact_runtime_state_text(top_pressure.get("reason_code"))
            unit = self._compact_runtime_state_text(top_pressure.get("unit"))
            if reason_code:
                pressure["reason_code"] = reason_code
            raw_value = top_pressure.get("value")
            if raw_value not in (None, ""):
                try:
                    pressure["value"] = float(raw_value)
                except (TypeError, ValueError):
                    pass
            if unit:
                pressure["unit"] = unit
            if pressure:
                compacted["top_pressure"] = pressure
        return compacted

    def _compact_runtime_transitions(self, value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        compacted: List[Dict[str, Any]] = []
        for entry in value:
            if not isinstance(entry, AbcMapping):
                continue
            transition = {
                key: self._compact_runtime_state_text(entry.get(key))
                for key in ("from_state", "to_state", "transition_reason", "source_component", "timestamp")
            }
            transition = {key: item for key, item in transition.items() if item}
            if transition:
                compacted.append(transition)
        return compacted[-BOTLENS_RUNTIME_HEALTH_TRANSITION_LIMIT:]

    def _compact_runtime_state_snapshot(self, runtime_snapshot: Mapping[str, Any]) -> Dict[str, Any]:
        snapshot = runtime_snapshot if isinstance(runtime_snapshot, AbcMapping) else {}
        warnings = snapshot.get("warnings") if isinstance(snapshot.get("warnings"), list) else []
        normalized_warnings = [
            self._normalized_runtime_warning(entry)
            for entry in warnings
            if isinstance(entry, AbcMapping)
        ]
        normalized_warnings = [
            entry
            for entry in normalized_warnings
            if not self._is_empty_health_value(entry)
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
        runtime: Dict[str, Any] = {}
        for field in ("status", "runtime_state", "progress_state", "last_useful_progress_at"):
            value = self._compact_runtime_state_text(snapshot.get(field))
            if value:
                runtime[field] = value
        for field in ("worker_count", "active_workers"):
            value = self._compact_runtime_state_int(snapshot.get(field))
            if value is not None:
                runtime[field] = value
        if normalized_warnings:
            runtime["warnings"] = normalized_warnings
        runtime["warning_count"] = len(normalized_warnings)
        degraded = self._compact_runtime_state_fields(
            snapshot.get("degraded"),
            ("active", "started_at", "reason_code", "trigger_event", "cleared_at", "recovery_reason", "duration_ms"),
        )
        if degraded:
            runtime["degraded"] = degraded
        churn = self._compact_runtime_state_fields(
            snapshot.get("churn"),
            ("active", "detected_at", "reason_code", "activity_without_progress_count", "last_useful_progress_at"),
        )
        if churn:
            runtime["churn"] = churn
        pressure = self._compact_runtime_pressure(snapshot.get("pressure"))
        if pressure:
            runtime["pressure"] = pressure
        transitions = self._compact_runtime_transitions(snapshot.get("recent_transitions"))
        if transitions:
            runtime["recent_transitions"] = transitions
        terminal = self._compact_runtime_state_fields(
            snapshot.get("terminal"),
            ("status", "source", "actor", "reason", "expected_workers", "reported_workers"),
        )
        worker_terminal_statuses = (
            snapshot.get("terminal", {}).get("worker_terminal_statuses")
            if isinstance(snapshot.get("terminal"), AbcMapping)
            else None
        )
        if isinstance(worker_terminal_statuses, AbcMapping) and worker_terminal_statuses:
            terminal["worker_terminal_status_count"] = len(worker_terminal_statuses)
        if terminal:
            runtime["terminal"] = terminal
        return {
            key: value
            for key, value in runtime.items()
            if value not in (None, "", [], {}, ())
        }

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
        warning_revision = getattr(self, "_warning_revision", None)
        if isinstance(warning_revision, int):
            self._push_runtime_health_warning_revision = int(warning_revision)

    def _should_probe_runtime_state_fact(self, *, event: str) -> bool:
        normalized_event = str(event or "").strip().lower()
        if normalized_event not in {"bar", "intrabar"}:
            return True
        previous_fingerprint = str(getattr(self, "_push_runtime_health_fingerprint", "") or "")
        if not previous_fingerprint:
            return True

        warning_revision = getattr(self, "_warning_revision", None)
        if not isinstance(warning_revision, int):
            return True
        previous_warning_revision = getattr(self, "_push_runtime_health_warning_revision", None)
        if previous_warning_revision != warning_revision:
            return True

        state = getattr(self, "state", None)
        status = None
        if isinstance(state, AbcMapping):
            status = str(state.get("status") or "").strip().lower()
        previous_status = str(getattr(self, "_push_runtime_health_status", "") or "")
        if status and previous_status and status != previous_status:
            return True

        last_emitted = float(getattr(self, "_push_runtime_health_emitted_monotonic", 0.0) or 0.0)
        heartbeat_ms = float(
            getattr(self, "_runtime_health_emit_interval_ms", BOTLENS_RUNTIME_HEALTH_EMIT_INTERVAL_MS)
            or BOTLENS_RUNTIME_HEALTH_EMIT_INTERVAL_MS
        )
        elapsed_ms = max((time.monotonic() - last_emitted) * 1000.0, 0.0)
        return elapsed_ms >= heartbeat_ms

    def _should_emit_runtime_state_fact(self, *, runtime_snapshot: Mapping[str, Any], event: str) -> bool:
        normalized_event = str(event or "").strip().lower()
        emitted_monotonic = time.monotonic()
        fingerprint = self._runtime_health_fingerprint(runtime_snapshot)
        status = str(runtime_snapshot.get("status") or "").strip().lower()
        if normalized_event not in {"bar", "intrabar"}:
            self._mark_runtime_health_emitted(fingerprint, emitted_monotonic=emitted_monotonic)
            self._push_runtime_health_status = status
            return True
        previous_fingerprint = str(getattr(self, "_push_runtime_health_fingerprint", "") or "")
        last_emitted = float(getattr(self, "_push_runtime_health_emitted_monotonic", 0.0) or 0.0)
        heartbeat_ms = float(
            getattr(self, "_runtime_health_emit_interval_ms", BOTLENS_RUNTIME_HEALTH_EMIT_INTERVAL_MS)
            or BOTLENS_RUNTIME_HEALTH_EMIT_INTERVAL_MS
        )
        if not previous_fingerprint or previous_fingerprint != fingerprint:
            self._mark_runtime_health_emitted(fingerprint, emitted_monotonic=emitted_monotonic)
            self._push_runtime_health_status = status
            return True
        elapsed_ms = max((emitted_monotonic - last_emitted) * 1000.0, 0.0)
        if elapsed_ms >= heartbeat_ms:
            self._mark_runtime_health_emitted(fingerprint, emitted_monotonic=emitted_monotonic)
            self._push_runtime_health_status = status
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

    @staticmethod
    def _compact_series_stats(stats: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(stats or {}) if isinstance(stats, AbcMapping) else {}
        durable: Dict[str, Any] = {}
        for key in sorted(BOTLENS_COMPACT_SERIES_STATS_KEYS):
            raw_value = payload.get(key)
            if raw_value in (None, "", [], {}, ()):
                continue
            if key == "quote_currency":
                currency = str(raw_value or "").strip().upper()
                if currency:
                    durable[key] = currency
                continue
            try:
                numeric = float(raw_value)
            except (TypeError, ValueError):
                continue
            if key in {"total_trades", "completed_trades", "wins", "losses"}:
                durable[key] = int(numeric)
            else:
                durable[key] = round(numeric, 4)
        return durable

    def _log_facts(self) -> Tuple[List[Dict[str, Any]], int]:
        entries = self.logs()
        new_entries, marker, dropped = self._bounded_entries_after_marker(
            entries,
            marker_field="id",
            previous_marker=getattr(self, "_push_log_marker", None),
            limit=int(
                getattr(self, "_botlens_fact_stream_log_fact_limit", BOTLENS_FACT_STREAM_LOG_FACT_LIMIT)
                or BOTLENS_FACT_STREAM_LOG_FACT_LIMIT
            ),
            fact_type=BOTLENS_FACT_LOG_EMITTED,
        )
        self._push_log_marker = marker
        return [{"fact_type": BOTLENS_FACT_LOG_EMITTED, "log": entry} for entry in new_entries], dropped

    def _decision_facts(self) -> Tuple[List[Dict[str, Any]], int]:
        entries = self.decision_events()
        new_entries, marker, dropped = self._bounded_entries_after_marker(
            entries,
            marker_field="event_id",
            previous_marker=getattr(self, "_push_decision_marker", None),
            limit=int(
                getattr(
                    self,
                    "_botlens_fact_stream_decision_fact_limit",
                    BOTLENS_FACT_STREAM_DECISION_FACT_LIMIT,
                )
                or BOTLENS_FACT_STREAM_DECISION_FACT_LIMIT
            ),
            fact_type=BOTLENS_FACT_DECISION_EMITTED,
        )
        self._push_decision_marker = marker
        return [{"fact_type": BOTLENS_FACT_DECISION_EMITTED, "decision": entry} for entry in new_entries], dropped

    @staticmethod
    def _wallet_event_context(entry: Mapping[str, Any]) -> Dict[str, Any]:
        context = entry.get("context")
        return dict(context) if isinstance(context, AbcMapping) else {}

    @staticmethod
    def _wallet_event_name(entry: Mapping[str, Any]) -> str:
        return str(entry.get("event_name") or "").strip().upper()

    @classmethod
    def _wallet_entry_with_wallet_before(
        cls,
        entry: Mapping[str, Any],
        wallet_before: Mapping[str, Any],
    ) -> Dict[str, Any]:
        payload = dict(entry or {})
        context = cls._wallet_event_context(payload)
        context["wallet_before"] = cls._wallet_copy_snapshot(wallet_before)
        payload["context"] = context
        return payload

    @classmethod
    def _wallet_margin_position(
        cls,
        snapshot: Mapping[str, Any],
        trade_id: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        trade_key = str(trade_id or "").strip()
        if not trade_key:
            return None
        positions = snapshot.get("margin_positions") if isinstance(snapshot, AbcMapping) else None
        if not isinstance(positions, AbcMapping):
            return None
        position = positions.get(trade_key)
        return dict(position) if isinstance(position, AbcMapping) else None

    @classmethod
    def _wallet_should_rebase_exit_before(
        cls,
        *,
        context: Mapping[str, Any],
        projected_wallet: Mapping[str, Any],
    ) -> bool:
        trade_id = str(context.get("trade_id") or "").strip()
        if not trade_id:
            return False
        wallet_before = context.get("wallet_before")
        if not isinstance(wallet_before, AbcMapping):
            return False
        context_position = cls._wallet_margin_position(wallet_before, trade_id)
        projected_position = cls._wallet_margin_position(projected_wallet, trade_id)
        if not context_position or not projected_position:
            return False
        for field_name in ("open_qty", "locked_margin"):
            context_value = cls._wallet_float(context_position.get(field_name), None)
            projected_value = cls._wallet_float(projected_position.get(field_name), None)
            if context_value is None or projected_value is None:
                continue
            if abs(context_value - projected_value) > 1e-9:
                return True
        return False

    @classmethod
    def _wallet_projection_after_facts(
        cls,
        facts: Sequence[Mapping[str, Any]],
        previous: Optional[Mapping[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        projected = cls._wallet_copy_snapshot(previous or {}) if isinstance(previous, AbcMapping) else None
        for fact in facts:
            if not isinstance(fact, AbcMapping):
                continue
            if str(fact.get("fact_type") or "") != BOTLENS_FACT_WALLET_EVENT:
                continue
            wallet_event = fact.get("wallet_event")
            if not isinstance(wallet_event, AbcMapping):
                continue
            wallet_after = wallet_event.get("wallet_after")
            if isinstance(wallet_after, AbcMapping):
                projected = cls._wallet_copy_snapshot(wallet_after)
        return projected

    @staticmethod
    def _wallet_int(value: Any, default: Optional[int] = None) -> Optional[int]:
        if value in (None, ""):
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _wallet_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        if value in (None, ""):
            return default
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return default
        if numeric != numeric or numeric in (float("inf"), float("-inf")):
            return default
        return round(numeric, 12)

    @classmethod
    def _wallet_snapshot_amount(
        cls,
        snapshot: Mapping[str, Any],
        section: str,
        currency: str,
        *,
        default: Optional[float] = None,
    ) -> Optional[float]:
        values = snapshot.get(section) if isinstance(snapshot.get(section), AbcMapping) else {}
        if not isinstance(values, AbcMapping):
            return default
        code = str(currency or "").strip().upper()
        if not code:
            return default
        return cls._wallet_float(values.get(code), default)

    @staticmethod
    def _wallet_currency(context: Mapping[str, Any]) -> str:
        for key in ("quote_currency", "currency", "asset"):
            value = str(context.get(key) or "").strip().upper()
            if value:
                return value
        required_delta = context.get("required_delta")
        if isinstance(required_delta, AbcMapping):
            value = str(required_delta.get("currency") or "").strip().upper()
            if value:
                return value
        margin_requirement = context.get("margin_requirement")
        if isinstance(margin_requirement, AbcMapping):
            value = str(margin_requirement.get("currency") or "").strip().upper()
            if value:
                return value
        balances = context.get("balances")
        if isinstance(balances, AbcMapping):
            for key in balances.keys():
                value = str(key or "").strip().upper()
                if value:
                    return value
        return "USD"

    @classmethod
    def _wallet_margin_required(
        cls,
        context: Mapping[str, Any],
        *,
        release: bool = False,
    ) -> float:
        wallet_delta = context.get("wallet_delta") if isinstance(context.get("wallet_delta"), AbcMapping) else {}
        if release:
            value = cls._wallet_float(wallet_delta.get("collateral_released"), 0.0)
            return float(value or 0.0)
        required_delta = context.get("required_delta") if isinstance(context.get("required_delta"), AbcMapping) else {}
        margin_requirement = context.get("margin_requirement") if isinstance(context.get("margin_requirement"), AbcMapping) else {}
        for source, key in (
            (required_delta, "collateral_reserved"),
            (required_delta, "total_required_collateral"),
            (required_delta, "margin_total_required"),
            (required_delta, "required_margin"),
            (required_delta, "collateral_to_lock"),
            (required_delta, "required_collateral"),
            (margin_requirement, "collateral_reserved"),
            (margin_requirement, "total_required_collateral"),
            (margin_requirement, "margin_total_required"),
            (margin_requirement, "required_margin"),
            (margin_requirement, "collateral_to_lock"),
            (margin_requirement, "required_collateral"),
            (wallet_delta, "collateral_reserved"),
        ):
            value = cls._wallet_float(source.get(key), None) if isinstance(source, AbcMapping) else None
            if value is not None:
                return float(max(value, 0.0))
        risk_qty = cls._wallet_float(context.get("risk_qty") or context.get("qty_raw") or context.get("selected_quantity"), None)
        cost_per_contract = cls._wallet_float(context.get("cost_per_contract") or context.get("margin_per_contract"), None)
        if risk_qty is not None and cost_per_contract is not None:
            return float(max(risk_qty * cost_per_contract, 0.0))
        return 0.0

    @staticmethod
    def _wallet_copy_snapshot(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
        def _float_mapping(value: Any) -> Dict[str, float]:
            result: Dict[str, float] = {}
            if not isinstance(value, AbcMapping):
                return result
            for key, raw in value.items():
                try:
                    result[str(key)] = float(raw or 0.0)
                except (TypeError, ValueError):
                    continue
            return result

        positions: Dict[str, Dict[str, Any]] = {}
        raw_positions = snapshot.get("margin_positions") if isinstance(snapshot, AbcMapping) else {}
        if isinstance(raw_positions, AbcMapping):
            for trade_id, raw_position in raw_positions.items():
                if not isinstance(raw_position, AbcMapping):
                    continue
                positions[str(trade_id)] = {
                    str(key): (float(value) if isinstance(value, (int, float)) else value)
                    for key, value in raw_position.items()
                }
        return {
            "balances": _float_mapping(snapshot.get("balances") if isinstance(snapshot, AbcMapping) else {}),
            "locked_margin": _float_mapping(snapshot.get("locked_margin") if isinstance(snapshot, AbcMapping) else {}),
            "free_collateral": _float_mapping(snapshot.get("free_collateral") if isinstance(snapshot, AbcMapping) else {}),
            "margin_positions": positions,
        }

    @classmethod
    def _wallet_has_balance(cls, snapshot: Mapping[str, Any], currency: str) -> bool:
        balances = snapshot.get("balances") if isinstance(snapshot, AbcMapping) else {}
        return isinstance(balances, AbcMapping) and str(currency or "").strip().upper() in balances

    @classmethod
    def _wallet_set_snapshot_amount(
        cls,
        snapshot: Dict[str, Any],
        section: str,
        currency: str,
        value: Optional[float],
    ) -> None:
        code = str(currency or "").strip().upper()
        if not code or value is None:
            return
        values = snapshot.setdefault(section, {})
        if not isinstance(values, dict):
            values = {}
            snapshot[section] = values
        amount = float(value)
        if section == "locked_margin" and abs(amount) <= 1e-9:
            values.pop(code, None)
        else:
            values[code] = amount

    @classmethod
    def _wallet_normalize_free_collateral(cls, snapshot: Dict[str, Any], currency: str) -> None:
        code = str(currency or "").strip().upper()
        if not code or not cls._wallet_has_balance(snapshot, code):
            return
        balance = cls._wallet_snapshot_amount(snapshot, "balances", code, default=0.0) or 0.0
        locked = cls._wallet_snapshot_amount(snapshot, "locked_margin", code, default=0.0) or 0.0
        cls._wallet_set_snapshot_amount(snapshot, "free_collateral", code, balance - locked)

    @classmethod
    def _wallet_apply_balance_delta(
        cls,
        snapshot: Dict[str, Any],
        currency: str,
        delta: float,
    ) -> Dict[str, Any]:
        after = cls._wallet_copy_snapshot(snapshot)
        balance = cls._wallet_snapshot_amount(after, "balances", currency, default=None)
        if balance is None:
            return after
        cls._wallet_set_snapshot_amount(after, "balances", currency, balance + float(delta or 0.0))
        cls._wallet_normalize_free_collateral(after, currency)
        return after

    @classmethod
    def _wallet_apply_margin_reserve(
        cls,
        snapshot: Dict[str, Any],
        *,
        currency: str,
        trade_id: Optional[str],
        qty: Optional[float],
        margin_reserved: float,
    ) -> Dict[str, Any]:
        after = cls._wallet_copy_snapshot(snapshot)
        amount = float(max(margin_reserved or 0.0, 0.0))
        locked = cls._wallet_snapshot_amount(after, "locked_margin", currency, default=0.0) or 0.0
        cls._wallet_set_snapshot_amount(after, "locked_margin", currency, locked + amount)
        if trade_id and amount:
            positions = after.setdefault("margin_positions", {})
            if not isinstance(positions, dict):
                positions = {}
                after["margin_positions"] = positions
            position = dict(positions.get(str(trade_id)) or {})
            position["currency"] = currency
            position["open_qty"] = float(position.get("open_qty") or 0.0) + float(qty or 0.0)
            position["locked_margin"] = float(position.get("locked_margin") or 0.0) + amount
            positions[str(trade_id)] = position
        cls._wallet_normalize_free_collateral(after, currency)
        return after

    @classmethod
    def _wallet_apply_margin_release(
        cls,
        snapshot: Dict[str, Any],
        *,
        currency: str,
        trade_id: Optional[str],
        qty: Optional[float],
        margin_released: float,
    ) -> Dict[str, Any]:
        after = cls._wallet_copy_snapshot(snapshot)
        amount = float(max(margin_released or 0.0, 0.0))
        locked = cls._wallet_snapshot_amount(after, "locked_margin", currency, default=0.0) or 0.0
        cls._wallet_set_snapshot_amount(after, "locked_margin", currency, max(locked - amount, 0.0))
        positions = after.get("margin_positions")
        if trade_id and isinstance(positions, dict) and str(trade_id) in positions:
            position = dict(positions.get(str(trade_id)) or {})
            position["open_qty"] = max(float(position.get("open_qty") or 0.0) - float(qty or 0.0), 0.0)
            position["locked_margin"] = max(float(position.get("locked_margin") or 0.0) - amount, 0.0)
            if position["open_qty"] <= 1e-9 or position["locked_margin"] <= 1e-9:
                positions.pop(str(trade_id), None)
            else:
                positions[str(trade_id)] = position
        cls._wallet_normalize_free_collateral(after, currency)
        return after

    @classmethod
    def _wallet_source_refs(cls, *, source: Mapping[str, Any], context: Mapping[str, Any]) -> List[Dict[str, Any]]:
        refs: List[Dict[str, Any]] = []
        for section, key in (
            ("runtime_events", "event_id"),
            ("signals", "signal_id"),
            ("decisions", "decision_id"),
            ("trades", "trade_id"),
        ):
            value = source.get(key) if key == "event_id" else context.get(key)
            text = str(value or "").strip()
            if text:
                refs.append({"section": section, key: text})
        return refs

    @classmethod
    def _wallet_fact(
        cls,
        *,
        source: Mapping[str, Any],
        context: Mapping[str, Any],
        event_name: str,
        currency: str,
        balance_before: Optional[float],
        balance_after: Optional[float],
        equity_before: Optional[float],
        equity_after: Optional[float],
        margin_required: Optional[float],
        margin_available: Optional[float],
        fee: Optional[float],
        reason: str,
        wallet_before: Mapping[str, Any],
        wallet_after: Optional[Mapping[str, Any]] = None,
        free_collateral_before: Optional[float] = None,
        free_collateral_after: Optional[float] = None,
        locked_margin_before: Optional[float] = None,
        locked_margin_after: Optional[float] = None,
        margin_reserved: Optional[float] = None,
        margin_released: Optional[float] = None,
        realized_pnl: Optional[float] = None,
        selected_quantity: Optional[float] = None,
        source_refs: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> Dict[str, Any]:
        source_event_id = str(source.get("event_id") or "").strip()
        source_seq_int = cls._wallet_int(source.get("seq"), None)
        wallet_commit_seq_int = cls._wallet_int(context.get("wallet_commit_seq"), None)
        if wallet_commit_seq_int is None:
            raise ValueError(
                "wallet_commit_seq is required for wallet ledger fact "
                f"| event_name={event_name} | source_event_id={source_event_id}"
            )
        wallet_eval_seq_int = cls._wallet_int(context.get("wallet_eval_seq"), None)
        if wallet_eval_seq_int is None:
            wallet_eval_seq_int = max(wallet_commit_seq_int - 1, 0)
        position_commit_seq_int = cls._wallet_int(context.get("position_commit_seq"), None)
        event_order = int(_WALLET_LEDGER_EVENT_ORDER.get(event_name, 99))
        wallet_event_id = (
            f"{wallet_commit_seq_int:012d}:{source_event_id}:{event_order:02d}:{event_name.lower()}"
            if source_event_id
            else None
        )
        event_id = (
            f"botlens:wallet:{wallet_commit_seq_int:012d}:{source_event_id}:{event_order:02d}:{event_name.lower()}"
            if source_event_id
            else None
        )
        wallet_event = {
            "event_name": event_name,
            "event_id": event_id,
            "event_ts": source.get("event_ts") or context.get("bar_ts") or context.get("bar_time"),
            "known_at": context.get("bar_ts") or context.get("bar_time") or source.get("event_ts"),
            "run_id": context.get("run_id"),
            "bot_id": context.get("bot_id"),
            "run_seq": source_seq_int,
            "run_seq_status": "runtime_assigned" if source_seq_int is not None else None,
            "source_run_seq": source_seq_int,
            "source_run_seq_status": "runtime_assigned" if source_seq_int is not None else None,
            "wallet_commit_seq": wallet_commit_seq_int,
            "wallet_commit_seq_status": str(context.get("wallet_commit_seq_status") or "runtime_assigned"),
            "wallet_eval_seq": wallet_eval_seq_int,
            "position_commit_seq": position_commit_seq_int,
            "position_commit_seq_status": (
                str(context.get("position_commit_seq_status") or "position_scoped")
                if position_commit_seq_int is not None
                else None
            ),
            "wallet_event_order": event_order,
            "series_key": context.get("series_key"),
            "strategy_id": context.get("strategy_id"),
            "instrument_id": context.get("instrument_id"),
            "symbol": context.get("symbol"),
            "timeframe": context.get("timeframe"),
            "trade_id": context.get("trade_id"),
            "decision_id": context.get("decision_id"),
            "bar_time": context.get("bar_ts") or context.get("bar_time") or source.get("event_ts"),
            "source_event_id": source_event_id,
            "wallet_event_id": wallet_event_id,
            "correlation_id": source.get("correlation_id"),
            "root_id": source.get("root_id"),
            "parent_id": source_event_id or source.get("parent_id"),
            "currency": currency,
            "balance_before": cls._wallet_float(balance_before),
            "balance_after": cls._wallet_float(balance_after),
            "equity_before": cls._wallet_float(equity_before),
            "equity_after": cls._wallet_float(equity_after),
            "free_collateral_before": cls._wallet_float(free_collateral_before),
            "free_collateral_after": cls._wallet_float(free_collateral_after),
            "locked_margin_before": cls._wallet_float(locked_margin_before),
            "locked_margin_after": cls._wallet_float(locked_margin_after),
            "margin_required": cls._wallet_float(margin_required, 0.0),
            "margin_reserved": cls._wallet_float(margin_reserved),
            "margin_released": cls._wallet_float(margin_released),
            "margin_available": cls._wallet_float(margin_available),
            "fee": cls._wallet_float(fee, 0.0),
            "realized_pnl": cls._wallet_float(realized_pnl),
            "reason": reason,
            "qty": cls._wallet_float(context.get("qty")),
            "selected_quantity": cls._wallet_float(selected_quantity),
            "price": cls._wallet_float(context.get("price") or context.get("signal_price")),
            "notional": cls._wallet_float(context.get("notional")),
            "side": context.get("side"),
            "direction": context.get("direction"),
            "signal_id": context.get("signal_id"),
            "wallet_delta": dict(context.get("wallet_delta") or {}) if isinstance(context.get("wallet_delta"), AbcMapping) else None,
            "margin_requirement": (
                dict(context.get("required_delta"))
                if isinstance(context.get("required_delta"), AbcMapping)
                else dict(context.get("margin_requirement") or {})
                if isinstance(context.get("margin_requirement"), AbcMapping)
                else None
            ),
            "wallet_before": dict(wallet_before or {}),
            "wallet_after": dict(wallet_after or {}) if isinstance(wallet_after, AbcMapping) else None,
            "source_refs": [dict(ref) for ref in source_refs or [] if isinstance(ref, AbcMapping)],
        }
        return {
            "fact_type": BOTLENS_FACT_WALLET_EVENT,
            "series_key": context.get("series_key"),
            "wallet_event": {key: value for key, value in wallet_event.items() if value not in (None, "", [], {})},
        }

    @classmethod
    def _wallet_facts_from_runtime_event(
        cls,
        entry: Mapping[str, Any],
        *,
        wallet_before_override: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        name = cls._wallet_event_name(entry)
        context = cls._wallet_event_context(entry)
        if not context:
            return []
        if isinstance(wallet_before_override, AbcMapping):
            context["wallet_before"] = cls._wallet_copy_snapshot(wallet_before_override)
        if name == "DECISION_REJECTED":
            rejection_artifact = context.get("rejection_artifact")
            if isinstance(rejection_artifact, AbcMapping):
                artifact_context = rejection_artifact.get("context")
                if isinstance(artifact_context, AbcMapping):
                    merged_context = dict(artifact_context)
                    merged_context.update({key: value for key, value in context.items() if value not in (None, "", [], {})})
                    context = merged_context
        currency = cls._wallet_currency(context)
        wallet_before = (
            dict(context.get("wallet_before"))
            if isinstance(context.get("wallet_before"), AbcMapping)
            else dict(context.get("wallet_snapshot"))
            if isinstance(context.get("wallet_snapshot"), AbcMapping)
            else {}
        )
        wallet_before = cls._wallet_copy_snapshot(wallet_before)

        def amount(snapshot: Mapping[str, Any], section: str) -> Optional[float]:
            return cls._wallet_snapshot_amount(snapshot, section, currency, default=None)

        def fact(
            *,
            event_name: str,
            before: Mapping[str, Any],
            after: Mapping[str, Any],
            margin_required: Optional[float],
            fee: Optional[float],
            reason: str,
            margin_reserved: Optional[float] = None,
            margin_released: Optional[float] = None,
            realized_pnl: Optional[float] = None,
            selected_quantity: Optional[float] = None,
        ) -> Dict[str, Any]:
            before_snapshot = cls._wallet_copy_snapshot(before)
            after_snapshot = cls._wallet_copy_snapshot(after)
            balance_before = amount(before_snapshot, "balances")
            balance_after = amount(after_snapshot, "balances")
            free_before = amount(before_snapshot, "free_collateral")
            free_after = amount(after_snapshot, "free_collateral")
            locked_before = amount(before_snapshot, "locked_margin")
            locked_after = amount(after_snapshot, "locked_margin")
            if locked_before is None:
                locked_before = 0.0 if balance_before is not None else None
            if locked_after is None:
                locked_after = 0.0 if balance_after is not None else None
            if free_before is None:
                free_before = balance_before
            if free_after is None:
                free_after = balance_after
            return cls._wallet_fact(
                source=entry,
                context=context,
                event_name=event_name,
                currency=currency,
                balance_before=balance_before,
                balance_after=balance_after,
                equity_before=balance_before,
                equity_after=balance_after,
                free_collateral_before=free_before,
                free_collateral_after=free_after,
                locked_margin_before=locked_before,
                locked_margin_after=locked_after,
                margin_required=margin_required,
                margin_available=free_before,
                fee=fee,
                reason=reason,
                wallet_before=before_snapshot,
                wallet_after=after_snapshot,
                margin_reserved=margin_reserved,
                margin_released=margin_released,
                realized_pnl=realized_pnl,
                selected_quantity=selected_quantity,
                source_refs=cls._wallet_source_refs(source=entry, context=context),
            )

        facts: List[Dict[str, Any]] = []
        if name == "WALLET_INITIALIZED":
            balances = context.get("balances") if isinstance(context.get("balances"), AbcMapping) else {}
            wallet_after = context.get("wallet_after") if isinstance(context.get("wallet_after"), AbcMapping) else {}
            if not balances and isinstance(wallet_after, AbcMapping):
                raw_balances = wallet_after.get("balances")
                balances = raw_balances if isinstance(raw_balances, AbcMapping) else {}
            normalized_balances = {
                str(code or "").strip().upper(): float(value or 0.0)
                for code, value in dict(balances or {}).items()
                if str(code or "").strip()
            }
            init_before = {
                "balances": {code: 0.0 for code in normalized_balances},
                "locked_margin": {},
                "free_collateral": {code: 0.0 for code in normalized_balances},
                "margin_positions": {},
            }
            init_after = {
                "balances": normalized_balances,
                "locked_margin": {},
                "free_collateral": dict(normalized_balances),
                "margin_positions": {},
            }
            facts.append(
                fact(
                    event_name="WALLET_INITIALIZED",
                    before=init_before,
                    after=init_after,
                    margin_required=0.0,
                    fee=0.0,
                    reason=str(context.get("source") or "run_start"),
                )
            )
            return facts
        if name == "DECISION_REJECTED":
            reason = str(context.get("reason_code") or context.get("message") or "").strip().upper()
            if not (reason.startswith("WALLET_") or "MARGIN" in reason):
                return []
            selected_quantity = cls._wallet_float(
                context.get("selected_quantity")
                or context.get("qty_final")
                or context.get("requested_qty")
                or context.get("risk_qty"),
                None,
            )
            facts.append(
                fact(
                    event_name="MARGIN_REJECTED",
                    before=wallet_before,
                    after=wallet_before,
                    margin_required=cls._wallet_margin_required(context),
                    fee=0.0,
                    reason=reason or "WALLET_REJECTED",
                    selected_quantity=selected_quantity,
                )
            )
            return facts
        if name == "ENTRY_FILLED":
            margin_required = cls._wallet_margin_required(context)
            fee = cls._wallet_float(context.get("fee_paid"), 0.0) or 0.0
            qty = cls._wallet_float(context.get("qty"), 0.0) or 0.0
            trade_id = str(context.get("trade_id") or "").strip() or None
            after_reserve = cls._wallet_apply_margin_reserve(
                wallet_before,
                currency=currency,
                trade_id=trade_id,
                qty=qty,
                margin_reserved=margin_required,
            )
            after_fee = cls._wallet_apply_balance_delta(after_reserve, currency, -fee) if fee else after_reserve
            facts.append(
                fact(
                    event_name="MARGIN_RESERVED",
                    before=wallet_before,
                    after=after_reserve,
                    margin_required=margin_required,
                    fee=0.0,
                    reason="entry_fill",
                    margin_reserved=margin_required,
                    selected_quantity=qty,
                )
            )
            if fee:
                facts.append(
                    fact(
                        event_name="FEE_APPLIED",
                        before=after_reserve,
                        after=after_fee,
                        margin_required=0.0,
                        fee=fee,
                        reason="entry_fee",
                        selected_quantity=qty,
                    )
                )
            facts.append(
                fact(
                    event_name="POSITION_OPENED",
                    before=after_fee,
                    after=after_fee,
                    margin_required=margin_required,
                    fee=fee,
                    reason="entry_fill",
                    margin_reserved=margin_required,
                    selected_quantity=qty,
                )
            )
            facts.append(
                fact(
                    event_name="EQUITY_UPDATED",
                    before=after_fee,
                    after=after_fee,
                    margin_required=margin_required,
                    fee=fee,
                    reason="entry_fill",
                    margin_reserved=margin_required,
                    selected_quantity=qty,
                )
            )
            return facts
        if name == "EXIT_FILLED":
            margin_released = cls._wallet_margin_required(context, release=True)
            fee = cls._wallet_float(context.get("fee_paid"), 0.0) or 0.0
            realized_pnl = cls._wallet_float(context.get("realized_pnl"), 0.0) or 0.0
            qty = cls._wallet_float(context.get("qty"), 0.0) or 0.0
            trade_id = str(context.get("trade_id") or "").strip() or None
            after_release = cls._wallet_apply_margin_release(
                wallet_before,
                currency=currency,
                trade_id=trade_id,
                qty=qty,
                margin_released=margin_released,
            )
            after_fee = cls._wallet_apply_balance_delta(after_release, currency, -fee) if fee else after_release
            after_pnl = (
                cls._wallet_apply_balance_delta(after_fee, currency, realized_pnl)
                if realized_pnl
                else after_fee
            )
            facts.append(
                fact(
                    event_name="MARGIN_RELEASED",
                    before=wallet_before,
                    after=after_release,
                    margin_required=margin_released,
                    fee=0.0,
                    reason=str(context.get("exit_kind") or context.get("event_subtype") or "exit_fill").lower(),
                    margin_released=margin_released,
                    selected_quantity=qty,
                )
            )
            if fee:
                facts.append(
                    fact(
                        event_name="FEE_APPLIED",
                        before=after_release,
                        after=after_fee,
                        margin_required=0.0,
                        fee=fee,
                        reason="exit_fee",
                        selected_quantity=qty,
                    )
                )
            if realized_pnl:
                facts.append(
                    fact(
                        event_name="REALIZED_PNL_APPLIED",
                        before=after_fee,
                        after=after_pnl,
                        margin_required=0.0,
                        fee=0.0,
                        reason="exit_pnl",
                        realized_pnl=realized_pnl,
                        selected_quantity=qty,
                    )
                )
            facts.append(
                fact(
                    event_name="POSITION_CLOSED",
                    before=after_pnl,
                    after=after_pnl,
                    margin_required=margin_released,
                    fee=fee,
                    reason=str(context.get("exit_kind") or context.get("event_subtype") or "exit_fill").lower(),
                    margin_released=margin_released,
                    realized_pnl=realized_pnl,
                    selected_quantity=qty,
                )
            )
            facts.append(
                fact(
                    event_name="EQUITY_UPDATED",
                    before=after_pnl,
                    after=after_pnl,
                    margin_required=margin_released,
                    fee=fee,
                    reason=str(context.get("exit_kind") or context.get("event_subtype") or "exit_fill").lower(),
                    margin_released=margin_released,
                    realized_pnl=realized_pnl,
                    selected_quantity=qty,
                )
            )
            return facts
        return []

    def _wallet_facts(self) -> List[Dict[str, Any]]:
        run_context = getattr(self, "_run_context", None)
        if run_context is None:
            return []
        stream = getattr(run_context, "runtime_event_stream", None)
        if stream is None:
            return []
        entries = list(stream)
        new_entries, marker = self._entries_after_marker(
            entries,
            marker_field="event_id",
            previous_marker=getattr(self, "_push_wallet_marker", None),
        )
        self._push_wallet_marker = marker
        facts: List[Dict[str, Any]] = []
        projected_wallet = getattr(self, "_push_wallet_projection", None)
        if isinstance(projected_wallet, AbcMapping):
            projected_wallet = self._wallet_copy_snapshot(projected_wallet)
        else:
            projected_wallet = None
        for entry in new_entries:
            if not isinstance(entry, AbcMapping):
                continue
            context = self._wallet_event_context(entry)
            wallet_before_override = None
            if (
                self._wallet_event_name(entry) == "EXIT_FILLED"
                and isinstance(projected_wallet, AbcMapping)
                and self._wallet_should_rebase_exit_before(
                    context=context,
                    projected_wallet=projected_wallet,
                )
            ):
                wallet_before_override = projected_wallet
            entry_facts = self._wallet_facts_from_runtime_event(
                entry,
                wallet_before_override=wallet_before_override,
            )
            facts.extend(entry_facts)
            projected_wallet = self._wallet_projection_after_facts(entry_facts, projected_wallet)
        if isinstance(projected_wallet, AbcMapping):
            self._push_wallet_projection = self._wallet_copy_snapshot(projected_wallet)
        return facts

    def _trade_facts(
        self,
        *,
        series: StrategySeries,
        cache: Dict[str, Any],
        bar_time: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, bool]:
        trades_revision = self._trade_revision(series)
        if cache.get("trades_revision") == trades_revision:
            trades_count = int(cache.get("trades_count") or 0)
            cache["series_stats_changed"] = False
            return [], cache.get("series_stats"), trades_count, False

        identity = self._series_identity(series)
        cursor_revision = cache.get("trade_cursor_revision")
        trade_changes_since = getattr(series.risk_engine, "serialise_trade_changes_since", None)
        if callable(trade_changes_since):
            trade_batch = trade_changes_since(cursor_revision)
            if not isinstance(trade_batch, AbcMapping):
                raise RuntimeError(
                    "bot_runtime_trade_fact_cursor_invalid: risk engine returned invalid trade change batch "
                    f"strategy_id={getattr(series, 'strategy_id', None)} "
                    f"symbol={getattr(series, 'symbol', None)} timeframe={getattr(series, 'timeframe', None)}"
                )
            trades = [
                dict(entry)
                for entry in trade_batch.get("trades", [])
                if isinstance(entry, AbcMapping)
            ]
            trades_count = int(trade_batch.get("total_trades") or len(trades))
            next_cursor_revision = int(trade_batch.get("to_revision") or 0)
            if trade_batch.get("cursor_expired"):
                logger.warning(
                    with_log_context(
                        "bot_runtime_trade_fact_cursor_expired",
                        self._runtime_log_context(
                            strategy_id=getattr(series, "strategy_id", None),
                            symbol=getattr(series, "symbol", None),
                            timeframe=getattr(series, "timeframe", None),
                            cursor_revision=cursor_revision,
                            trade_revision=next_cursor_revision,
                            changed_trades=len(trades),
                            total_trades=trades_count,
                        ),
                    )
                )
        else:
            logger.warning(
                with_log_context(
                    "bot_runtime_trade_fact_cursor_unavailable",
                    self._runtime_log_context(
                        strategy_id=getattr(series, "strategy_id", None),
                        symbol=getattr(series, "symbol", None),
                        timeframe=getattr(series, "timeframe", None),
                        cursor_revision=cursor_revision,
                        trade_revision=trades_revision[0] if len(trades_revision) == 1 else trades_revision,
                    ),
                )
            )
            trades = series.risk_engine.serialise_trades()
            trades_count = len(trades)
            next_cursor_revision = int(trades_revision[0]) if len(trades_revision) == 1 else trades_count
        raw_series_stats = dict(series.risk_engine.stats() or {})
        raw_series_stats["total_fees"] = raw_series_stats.get("fees_paid", 0.0)
        series_stats = self._compact_series_stats(raw_series_stats)
        cached_trade_map = (
            dict(cache.get("trade_fingerprints"))
            if isinstance(cache.get("trade_fingerprints"), AbcMapping)
            else {}
        )
        next_trade_map: Dict[str, str] = dict(cached_trade_map)
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
        next_open_trade_ids = {
            str(entry)
            for entry in cache.get("open_trade_ids", ())
            if str(entry).strip()
        }
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
                next_open_trade_ids.discard(trade_id)
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
        cache["trades_count"] = trades_count
        cache["trade_cursor_revision"] = next_cursor_revision
        cache["trades_revision"] = trades_revision
        cache["series_stats"] = series_stats
        cache["trade_fingerprints"] = next_trade_map
        cache["open_trade_ids"] = tuple(sorted(next_open_trade_ids))
        cache["emitted_trade_ids"] = tuple(sorted(next_emitted_trade_ids))
        cache["emitted_open_trade_ids"] = tuple(sorted(next_emitted_open_trade_ids))
        cache["emitted_closed_trade_ids"] = tuple(sorted(next_emitted_closed_trade_ids))
        cache["series_stats_changed"] = True
        return trade_facts, series_stats, trades_count, trade_entry_refresh_required

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

    def _should_refresh_visual_overlays(
        self,
        cache: Dict[str, Any],
        *,
        event: str,
        trade_entry_refresh_required: bool,
    ) -> bool:
        normalized_event = str(event or "").strip().lower()
        if normalized_event == "intrabar":
            return False
        if trade_entry_refresh_required:
            return True
        if normalized_event != "bar":
            return True

        last_emit_monotonic = float(cache.get("visual_overlay_emit_monotonic") or 0.0)
        if last_emit_monotonic <= 0.0:
            cache["visual_overlay_emit_monotonic"] = time.monotonic()
            return False
        elapsed_ms = max((time.monotonic() - last_emit_monotonic) * 1000.0, 0.0)
        return elapsed_ms >= float(BOTLENS_SELECTED_SYMBOL_VISUAL_REFRESH_INTERVAL_MS)

    def _botlens_bootstrap_closed_trade_limit_value(self) -> int:
        configured = getattr(self, "_botlens_bootstrap_closed_trade_limit", None)
        try:
            resolved = int(configured)
        except (TypeError, ValueError):
            resolved = BOTLENS_BOOTSTRAP_CLOSED_TRADE_LIMIT
        return max(resolved, 1)

    def _botlens_bootstrap_series_snapshot(
        self,
        series: StrategySeries,
        *,
        runtime_snapshot: Mapping[str, Any],
    ) -> Dict[str, Any]:
        state = self._series_state_for(series)
        candles = list(getattr(series, "candles", []) or [])
        raw_bar_index = getattr(state, "bar_index", None) if state is not None else None
        try:
            bar_index = int(raw_bar_index)
        except (TypeError, ValueError):
            bar_index = max(len(candles) - 1, 0)
        if candles:
            bar_index = min(max(bar_index, 0), len(candles) - 1)
        else:
            bar_index = max(bar_index, 0)

        builder = getattr(self, "_chart_state_builder", None)
        visible_candles = getattr(builder, "visible_candles", None)
        if not callable(visible_candles):
            raise RuntimeError("botlens bootstrap requires chart state builder visible_candles")

        runtime_state = getattr(self, "state", {})
        if not isinstance(runtime_state, AbcMapping):
            runtime_state = {}
        status = str(runtime_state.get("status") or runtime_snapshot.get("status") or "").lower()
        engine = getattr(series, "risk_engine", None)
        stats_fn = getattr(engine, "stats", None)
        if not callable(stats_fn):
            raise RuntimeError(
                "botlens bootstrap requires series risk engine stats "
                f"strategy_id={getattr(series, 'strategy_id', None)} "
                f"symbol={getattr(series, 'symbol', None)} timeframe={getattr(series, 'timeframe', None)}"
            )
        series_stats = dict(stats_fn() or {})
        series_stats["total_fees"] = series_stats.get("fees_paid", 0.0)

        return {
            "bar_index": bar_index,
            "candles": [
                dict(candle)
                for candle in visible_candles(
                    series,
                    status,
                    bar_index,
                    getattr(self, "_intrabar_manager", None),
                )
                if isinstance(candle, AbcMapping)
            ],
            "overlays": self._series_visible_overlays(series, status=status),
            "stats": series_stats,
        }

    def _botlens_bootstrap_trade_payloads(self, series: StrategySeries) -> List[Dict[str, Any]]:
        engine = getattr(series, "risk_engine", None)
        closed_trade_limit = self._botlens_bootstrap_closed_trade_limit_value()
        serialise_window = getattr(engine, "serialise_trade_window", None)
        if callable(serialise_window):
            return [dict(entry) for entry in serialise_window(max_closed=closed_trade_limit) if isinstance(entry, AbcMapping)]

        serialise_trades = getattr(engine, "serialise_trades", None)
        if not callable(serialise_trades):
            raise RuntimeError(
                "botlens bootstrap requires series risk engine serialise_trades "
                f"strategy_id={getattr(series, 'strategy_id', None)} "
                f"symbol={getattr(series, 'symbol', None)} timeframe={getattr(series, 'timeframe', None)}"
            )
        logger.warning(
            with_log_context(
                "botlens_bootstrap_trade_window_fallback",
                self._runtime_log_context(
                    strategy_id=getattr(series, "strategy_id", None),
                    symbol=getattr(series, "symbol", None),
                    timeframe=getattr(series, "timeframe", None),
                    closed_trade_limit=closed_trade_limit,
                ),
            )
        )
        trades = [dict(entry) for entry in serialise_trades() if isinstance(entry, AbcMapping)]
        include_indexes: set[int] = set()
        closed_indexes: List[int] = []
        for index, trade_payload in enumerate(trades):
            trade_id = str(trade_payload.get("trade_id") or "").strip()
            if not trade_id:
                continue
            if self._trade_payload_is_open(trade_payload):
                include_indexes.add(index)
            else:
                closed_indexes.append(index)
        include_indexes.update(closed_indexes[-closed_trade_limit:])
        dropped_closed = max(len(closed_indexes) - closed_trade_limit, 0)
        if dropped_closed > 0:
            logger.warning(
                with_log_context(
                    "botlens_bootstrap_closed_trades_truncated",
                    self._runtime_log_context(
                        strategy_id=getattr(series, "strategy_id", None),
                        symbol=getattr(series, "symbol", None),
                        timeframe=getattr(series, "timeframe", None),
                        closed_trade_limit=closed_trade_limit,
                        dropped_closed_trades=dropped_closed,
                    ),
                )
            )
        return [trades[index] for index in sorted(include_indexes)]

    def botlens_bootstrap_payload(self) -> Dict[str, Any]:
        if not self._series:
            raise RuntimeError("botlens bootstrap requires at least one prepared runtime series")
        series = self._series[0]
        identity = self._series_identity(series)
        runtime_snapshot = self.snapshot()
        selected_series = self._botlens_bootstrap_series_snapshot(series, runtime_snapshot=runtime_snapshot)
        bar_index = int(selected_series["bar_index"])
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
            key = overlay_cache_key(overlay, index)
            overlay_ops.append(
                {
                    "op": "upsert",
                    "key": key,
                    "overlay": compact_overlay_for_transport(
                        overlay,
                        key=key,
                        max_payload_items=int(
                            getattr(
                                self,
                                "_botlens_fact_stream_overlay_point_limit",
                                BOTLENS_FACT_STREAM_OVERLAY_POINT_LIMIT,
                            )
                            or BOTLENS_FACT_STREAM_OVERLAY_POINT_LIMIT
                        ),
                    ),
                }
            )
        if overlay_ops:
            facts.append(
                {
                    "fact_type": BOTLENS_FACT_OVERLAY_OPS,
                    "series_key": identity["series_key"],
                    "overlay_delta": {
                        "base_overlay_commit_seq": 0,
                        "overlay_commit_seq": 1,
                        "overlay_commit_seq_status": "overlay_scoped",
                        "ops": overlay_ops,
                    },
                }
            )
        series_stats = selected_series.get("stats") if isinstance(selected_series.get("stats"), AbcMapping) else {}
        facts.append(
            {
                "fact_type": BOTLENS_FACT_SERIES_STATS,
                "series_key": identity["series_key"],
                "stats": self._compact_series_stats(series_stats),
            }
        )
        for trade_payload in self._botlens_bootstrap_trade_payloads(series):
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
        for entry in self.logs(
            limit=int(getattr(self, "_botlens_fact_stream_log_fact_limit", BOTLENS_FACT_STREAM_LOG_FACT_LIMIT))
        ):
            if isinstance(entry, AbcMapping):
                facts.append({"fact_type": BOTLENS_FACT_LOG_EMITTED, "log": dict(entry)})
        for entry in self.decision_events(
            limit=int(
                getattr(
                    self,
                    "_botlens_fact_stream_decision_fact_limit",
                    BOTLENS_FACT_STREAM_DECISION_FACT_LIMIT,
                )
            )
        ):
            if isinstance(entry, AbcMapping):
                facts.append({"fact_type": BOTLENS_FACT_DECISION_EMITTED, "decision": dict(entry)})
        facts.extend(self._wallet_facts())
        observed_at = _isoformat(datetime.now(timezone.utc))
        last_candle_time = None
        candles = selected_series.get("candles") if isinstance(selected_series.get("candles"), list) else []
        if candles:
            last_candle = candles[-1]
            if isinstance(last_candle, AbcMapping):
                last_candle_time = last_candle.get("time")
        payload = {
            "type": "facts",
            "event": "bootstrap",
            "known_at": last_candle_time or runtime_snapshot.get("known_at") or runtime_snapshot.get("last_snapshot_at") or observed_at,
            "observed_at": observed_at,
            "series_key": identity["series_key"],
            "facts": facts,
        }
        gap_classification = (getattr(series, "meta", {}) or {}).get("candle_gap_classification")
        if gap_classification:
            payload["gap_classification"] = gap_classification
            payload["source_reason"] = "provider_closure"
        return payload

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
            "dispatch_ms": None,
            "queue_wait_ms": None,
            "subscriber_count": None,
            "subscribers_count": None,
            "dropped_messages": None,
            "coalesced_count": 0,
            "dropped_stale_count": 0,
            "botlens_fact_stream_coalesced_count": 0,
            "botlens_fact_stream_dropped_stale_count": 0,
            "botlens_fact_stream_fact_count": 0,
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
        dispatch_ms: Optional[float] = None
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
                    "dispatch_ms": 0.0,
                    "queue_wait_ms": 0.0,
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
                "dispatch_ms": 0.0,
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
            runtime_snapshot: Mapping[str, Any] = {}
            emit_runtime_state = False
            if self._should_probe_runtime_state_fact(event=event):
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
                stats_update_ms = 0.0
                payload_context["stats_reused"] = True
            elif emit_runtime_state:
                stats_update_ms = 0.0
                payload_context["stats_reused"] = False
            else:
                stats_update_ms = 0.0
                payload_context["stats_reused"] = False
            log_facts, dropped_log_facts = self._log_facts()
            decision_facts, dropped_decision_facts = self._decision_facts()
            payload["facts"].extend(log_facts)
            payload["facts"].extend(decision_facts)
            payload["facts"].extend(self._wallet_facts())
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
                gap_classification = (getattr(series, "meta", {}) or {}).get("candle_gap_classification")
                if gap_classification:
                    payload["gap_classification"] = gap_classification
                    payload["source_reason"] = "provider_closure"
                payload["facts"].append(
                    self._series_state_fact(
                        series=series,
                        bar_index=bar_index,
                        replace_last=bool(replace_last),
                    )
                )
                trade_facts, series_stats, trades_count, trade_entry_refresh_required = self._trade_facts(
                    series=series,
                    cache=cache,
                    bar_time=candle_time,
                )
                visible_overlays = None
                overlay_revision = None
                if self._should_refresh_visual_overlays(
                    cache,
                    event=event,
                    trade_entry_refresh_required=trade_entry_refresh_required,
                ):
                    refresh_overlays = getattr(self, "_refresh_indicator_overlays_for_state", None)
                    if series_state is not None and callable(refresh_overlays):
                        refresh_overlays(
                            series_state,
                            candle=candle,
                            reason="push_update_visual_due",
                        )
                    overlay_revision = self._series_overlay_revision(series, status=status)
                    if cache.get("overlay_revision") != overlay_revision or "visible_overlays" not in cache:
                        cache["visible_overlays"] = self._series_visible_overlays(
                            series,
                            status=status,
                            refresh=False,
                        )
                        cache["overlay_revision"] = overlay_revision
                    visible_overlays = cache.get("visible_overlays")
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
                        overlay_delta = self._build_overlay_delta(cache, visible_overlays)
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
                                        overlay_commit_seq=overlay_delta.get("overlay_commit_seq"),
                                        base_overlay_commit_seq=overlay_delta.get("base_overlay_commit_seq"),
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
                series_stats_changed = bool(cache.get("series_stats_changed"))
                if isinstance(series_stats, AbcMapping) and (
                    series_stats_changed or str(event or "").strip().lower() not in {"bar", "intrabar"}
                ):
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
            should_probe_payload_bytes = bool(self._obs_enabled and self._should_probe_payload_size())
            payload_context.update(
                self._botlens_fact_stream_surface_metrics(
                    payload,
                    include_bytes=should_probe_payload_bytes,
                )
            )
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
                    "log_facts_emitted": len(log_facts),
                    "decision_facts_emitted": len(decision_facts),
                    "log_facts_dropped": dropped_log_facts,
                    "decision_facts_dropped": dropped_decision_facts,
                }
            )
            if self._obs_enabled:
                payload_context["payload_bytes_sampled"] = should_probe_payload_bytes
                if should_probe_payload_bytes:
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
            live_fact_count = len(payload.get("facts") or [])
            canonical_facts = canonical_fact_payload(payload).get("facts")
            payload_context["live_fact_count"] = live_fact_count
            payload_context["canonical_fact_count"] = (
                len(canonical_facts) if isinstance(canonical_facts, list) else 0
            )
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
                append_result = dict(getattr(append_outcome.batch, "append_result", {}) or {})
                payload_context["canonical_event_count"] = int(append_result.get("event_count") or 0)
                dispatch_started = time.perf_counter()
                consumer_results = self._canonical_fact_appender.dispatch(append_outcome.batch)
                dispatch_ms = max((time.perf_counter() - dispatch_started) * 1000.0, 0.0)
                enqueue_ms = dispatch_ms
                payload = append_outcome.batch.live_payload
                subscriber_count, dropped_messages = self._broadcast_metrics_from_consumer_results(consumer_results)
            else:
                payload_context["noncanonical_facts_skipped"] = len(payload.get("facts") or [])
                subscriber_count, dropped_messages = 0, 0
                enqueue_ms = 0.0
                dispatch_ms = 0.0
            payload_context["enqueue_ms"] = enqueue_ms
            payload_context["stream_emit_ms"] = enqueue_ms
            payload_context["dispatch_ms"] = dispatch_ms
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
            "dispatch_ms": dispatch_ms,
            "stats_update_ms": stats_update_ms,
            "subscriber_count": float(subscriber_count) if subscriber_count is not None else None,
            "subscribers_count": float(subscriber_count) if subscriber_count is not None else None,
            "dropped_messages": float(dropped_messages) if dropped_messages is not None else None,
            "overlay_count": float(overlay_count) if overlay_count is not None else None,
            "overlay_points": float(overlay_points) if overlay_points is not None else None,
            "trace_persist_ms": trace_persist_ms,
        }
