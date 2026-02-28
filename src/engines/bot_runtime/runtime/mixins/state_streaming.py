"""Runtime persistence, state snapshots, and streaming payloads."""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from queue import Empty, Full, Queue
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import Candle
from utils.log_context import with_log_context

from ..core import _isoformat, _timeframe_to_seconds

logger = logging.getLogger(__name__)


class RuntimeStateStreamingMixin:
    def logs(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Return up to *limit* recent log entries."""

        with self._lock:
            entries = list(self._logs)
        if limit and limit > 0:
            entries = entries[-limit:]
        return entries

    def decision_events(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Return up to *limit* recent canonical runtime events."""

        with self._lock:
            entries = list(self._decision_events)
        if limit and limit > 0:
            entries = entries[-limit:]
        return entries

    def _persist_trade_entry(self, series: StrategySeries, trade: LadderPosition) -> None:
        if not series or not trade:
            return
        run_id = self._run_context.run_id if self._run_context else None
        contracts = sum(max(leg.contracts, 0) for leg in trade.legs)
        timeframe_label = series.timeframe
        timeframe_seconds = _timeframe_to_seconds(timeframe_label)
        instrument_id = (series.instrument or {}).get("id") if isinstance(series.instrument, dict) else None
        metrics = dict(trade._metrics_snapshot())
        try:
            from portal.backend.service.market.entry_context import build_entry_metrics, derive_entry_context
            from portal.backend.service.market.stats_contract import REGIME_VERSION, STATS_VERSION

            entry_context = derive_entry_context(
                instrument_id=instrument_id,
                timeframe_seconds=timeframe_seconds,
                entry_time=trade.entry_time,
                stats_version=STATS_VERSION,
                regime_version=REGIME_VERSION,
            )
            metrics.update(build_entry_metrics(entry_context))
        except Exception as exc:
            context = self._runtime_log_context(
                strategy_id=series.strategy_id,
                symbol=series.symbol,
                timeframe=timeframe_label,
                instrument_id=instrument_id,
                error=str(exc),
            )
            logger.warning(with_log_context("bot_runtime_entry_context_unavailable", context))
        self._persistence_buffer.record_trade_entry(
            {
                "trade_id": trade.trade_id,
                "run_id": run_id,
                "bot_id": self.bot_id,
                "strategy_id": series.strategy_id,
                "symbol": series.symbol,
                "direction": trade.direction,
                "entry_time": trade.entry_time,
                "entry_price": trade.entry_price,
                "stop_price": trade.stop_price,
                "contracts": contracts,
                "status": "open",
                "quote_currency": trade.quote_currency,
                "metrics": metrics,
                "instrument_id": instrument_id,
                "timeframe": timeframe_label,
                "timeframe_seconds": timeframe_seconds,
            }
        )

    def _persist_trade_close(self, series: StrategySeries, event: Mapping[str, Any]) -> None:
        trade_id = event.get("trade_id")
        if not trade_id:
            return
        run_id = self._run_context.run_id if self._run_context else None
        self._persistence_buffer.record_trade_entry(
            {
                "trade_id": trade_id,
                "run_id": run_id,
                "bot_id": self.bot_id,
                "strategy_id": getattr(series, "strategy_id", None),
                "symbol": getattr(series, "symbol", None),
                "direction": event.get("direction"),
                "status": "closed",
                "exit_time": event.get("time"),
                "gross_pnl": event.get("gross_pnl"),
                "fees_paid": event.get("fees_paid"),
                "net_pnl": event.get("net_pnl"),
                "quote_currency": event.get("currency"),
                "metrics": event.get("metrics"),
                "instrument_id": (series.instrument or {}).get("id") if isinstance(series.instrument, dict) else None,
                "timeframe": series.timeframe,
                "timeframe_seconds": _timeframe_to_seconds(series.timeframe),
            }
        )

    def _persist_runtime_state(self, status: str) -> None:
        """Send completion metadata back to the service layer for persistence."""

        if not self._state_callback:
            return
        payload = {
            "status": status,
            "last_stats": dict(self._last_stats or {}),
            "last_run_at": _isoformat(datetime.now(timezone.utc)),
        }
        try:
            self._state_callback(payload)
        except Exception as exc:  # pragma: no cover - defensive logging
            context = self._runtime_log_context(status=status, error=str(exc))
            logger.warning(with_log_context("bot_runtime_state_callback_failed", context))

    def _flush_persistence_buffer(self, reason: str) -> None:
        flush_started = datetime.now(timezone.utc)
        try:
            self._persistence_buffer.flush(reason=reason)
            self._record_step_trace(
                "persistence_flush",
                started_at=flush_started,
                ended_at=datetime.now(timezone.utc),
                ok=True,
                context={"reason": reason},
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            context = self._runtime_log_context(reason=reason, error=str(exc))
            logger.warning(with_log_context("bot_runtime_persistence_flush_failed", context))
            self._record_step_trace(
                "persistence_flush",
                started_at=flush_started,
                ended_at=datetime.now(timezone.utc),
                ok=False,
                error=str(exc),
                context={"reason": reason},
            )

    def _record_step_trace(
        self,
        step_name: str,
        *,
        started_at: datetime,
        ended_at: datetime,
        ok: bool,
        strategy_id: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        error: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[float]:
        run_id = self._run_context.run_id if self._run_context else None
        if not run_id:
            return None
        duration_ms = max((ended_at - started_at).total_seconds() * 1000.0, 0.0)
        try:
            from portal.backend.service.storage import storage

            persist_started = time.perf_counter()
            storage.record_bot_run_step(
                {
                    "run_id": run_id,
                    "bot_id": self.bot_id,
                    "step_name": step_name,
                    "started_at": _isoformat(started_at),
                    "ended_at": _isoformat(ended_at),
                    "duration_ms": duration_ms,
                    "ok": ok,
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "error": error,
                    "context": dict(context or {}),
                }
            )
            return max((time.perf_counter() - persist_started) * 1000.0, 0.0)
        except Exception as exc:  # pragma: no cover - defensive logging
            step_context = self._runtime_log_context(step=step_name, run_id=run_id, error=str(exc))
            logger.warning(with_log_context("bot_runtime_step_trace_persist_failed", step_context))
            return None

    def _update_state(self, candle: Candle, status: str = "running") -> Dict[str, Any]:
        update_started = time.perf_counter()
        stats_started = time.perf_counter()
        stats = self._aggregate_stats()
        stats_update_ms = max((time.perf_counter() - stats_started) * 1000.0, 0.0)
        with self._lock:
            self._last_stats = stats
        self._refresh_next_bar_at()
        progress = self._compute_progress()
        snapshot = {
            "status": status,
            "progress": progress,
            "last_bar": candle.to_dict(),
            "stats": stats,
            "paused": self._paused,
            "next_bar_at": _isoformat(self._next_bar_at),
            "next_bar_in_seconds": self._seconds_until_next_bar(),
        }
        with self._lock:
            self.state.update(snapshot)
        if self._state_callback:
            try:
                self._state_callback({"runtime": self.snapshot()})
            except Exception as exc:  # pragma: no cover - defensive logging
                context = self._runtime_log_context(error=str(exc))
                logger.warning(with_log_context("bot_runtime_stream_callback_failed", context), exc_info=exc)
        return {
            "stats_update_ms": stats_update_ms,
            "update_state_total_ms": max((time.perf_counter() - update_started) * 1000.0, 0.0),
            "stats": dict(stats),
        }

    def _seconds_until_next_bar(self) -> Optional[float]:
        if not self._next_bar_at:
            return None
        delta = (self._next_bar_at - datetime.now(timezone.utc)).total_seconds()
        return round(delta, 2) if delta > 0 else 0.0

    def _state_payload(self) -> Dict[str, object]:
        self._refresh_next_bar_at()
        with self._lock:
            payload = dict(self.state)
        payload.setdefault("bot_id", self.bot_id)
        if self._run_context is not None:
            payload.setdefault("run_id", self._run_context.run_id)
        payload.setdefault("stats", self._last_stats)
        if "next_bar_at" not in payload:
            payload["next_bar_at"] = _isoformat(self._next_bar_at)
        if "next_bar_in_seconds" not in payload:
            payload["next_bar_in_seconds"] = self._seconds_until_next_bar()
        if "started_at" not in payload and self._run_started_at is not None:
            payload["started_at"] = _isoformat(self._run_started_at)
        payload.setdefault("warnings", self.warnings())
        payload["bootstrap"] = self._bootstrap_status_payload()
        return payload

    def _bootstrap_status_payload(self) -> Dict[str, Any]:
        with self._series_update_lock:
            per_series: List[Dict[str, Any]] = []
            for state in self._series_states:
                series = state.series
                indicator_links = list((series.meta or {}).get("indicator_links") or [])
                per_series.append(
                    {
                        "strategy_id": series.strategy_id,
                        "symbol": series.symbol,
                        "timeframe": series.timeframe,
                        "replay_start_index": int(getattr(series, "replay_start_index", 0) or 0),
                        "bootstrap_completed": bool(getattr(series, "bootstrap_completed", False)),
                        "bootstrap_total_overlays": int(getattr(series, "bootstrap_total_overlays", 0) or 0),
                        "bootstrap_indicator_overlays": int(getattr(series, "bootstrap_indicator_overlays", 0) or 0),
                        "expected_indicators": len(indicator_links),
                    }
                )
        failed = [entry for entry in per_series if not entry.get("bootstrap_completed")]
        status = "failed" if failed else ("ready" if per_series else "idle")
        details = self._prepare_error if isinstance(self._prepare_error, Mapping) else {}
        failure_details = details.get("failures") if isinstance(details, Mapping) else None
        return {
            "status": status,
            "series": per_series,
            "failed_count": len(failed),
            "failure_details": failure_details if isinstance(failure_details, list) else [],
        }

    def snapshot(self) -> Dict[str, object]:
        """Return a thread-safe snapshot of runtime state."""

        return self._state_payload()

    def chart_payload(self) -> Dict[str, object]:
        """Return the latest candle, trade, overlay, and stat data for the lens."""

        payload = self._chart_state()
        payload["warnings"] = self.warnings()
        payload["bot_id"] = self.bot_id
        payload["run_id"] = self._run_context.run_id if self._run_context is not None else None
        payload["runtime"] = self.snapshot()
        overlays = payload.get("overlays")
        overlay_summary = self._overlay_summary(overlays if isinstance(overlays, list) else [])
        series_entries = payload.get("series")
        series_overlay_counts: List[Dict[str, Any]] = []
        if isinstance(series_entries, list):
            for entry in series_entries:
                if not isinstance(entry, Mapping):
                    continue
                series_overlays = entry.get("overlays")
                series_overlay_counts.append(
                    {
                        "strategy_id": entry.get("strategy_id"),
                        "symbol": entry.get("symbol"),
                        "timeframe": entry.get("timeframe"),
                        "overlays": len(series_overlays) if isinstance(series_overlays, list) else 0,
                    }
                )
        logger.info(
            with_log_context(
                "bot_overlay_snapshot_sent",
                self._runtime_log_context(
                    overlays=overlay_summary.get("total_overlays"),
                    overlay_types=overlay_summary.get("type_counts"),
                    overlay_payloads=overlay_summary.get("payload_counts"),
                    overlay_profile_params=overlay_summary.get("profile_params_samples"),
                    series_overlay_counts=series_overlay_counts,
                ),
            )
        )
        return payload

    def regime_overlay_dump(self) -> Dict[str, Any]:
        """Return raw and visible regime overlays for debugging (no trimming on raw)."""

        if not self._prepared:
            raise RuntimeError("Runtime is not prepared. Call warm_up() or start() before overlay dump.")
        # Ensure overlay cache is current.
        self._aggregate_overlays_to_cache()
        raw_overlays = [
            ov
            for ov in self._chart_overlays or []
            if isinstance(ov, Mapping) and str(ov.get("type") or "").lower() in {"regime_overlay", "regime_markers"}
        ]

        current_candle = self._primary_state_candle()
        current_epoch = int(current_candle.time.timestamp()) if current_candle else None
        status = str(self.state.get("status") or "").lower()
        visible = self._chart_state_builder.visible_overlays(raw_overlays, status, current_epoch)

        def _start_end(overlay: Mapping[str, Any]) -> Tuple[Optional[int], Optional[int]]:
            payload = overlay.get("payload") if isinstance(overlay, Mapping) else {}
            boxes = payload.get("boxes") if isinstance(payload, Mapping) else None
            if isinstance(boxes, list) and boxes:
                starts = [b.get("x1") or b.get("start") for b in boxes if isinstance(b, Mapping)]
                ends = [b.get("x2") or b.get("end") for b in boxes if isinstance(b, Mapping)]
                starts = [s for s in starts if isinstance(s, (int, float))]
                ends = [e for e in ends if isinstance(e, (int, float))]
                return (int(min(starts)) if starts else None, int(max(ends)) if ends else None)
            segments = payload.get("segments") if isinstance(payload, Mapping) else None
            if isinstance(segments, list) and segments:
                starts = [s.get("x1") for s in segments if isinstance(s, Mapping)]
                ends = [s.get("x2") for s in segments if isinstance(s, Mapping)]
                starts = [s for s in starts if isinstance(s, (int, float))]
                ends = [e for e in ends if isinstance(e, (int, float))]
                return (int(min(starts)) if starts else None, int(max(ends)) if ends else None)
            return (None, None)

        def _with_meta(overlay: Mapping[str, Any]) -> Dict[str, Any]:
            start_epoch, end_epoch = _start_end(overlay)
            return {
                "type": overlay.get("type"),
                "instrument_id": overlay.get("instrument_id"),
                "symbol": overlay.get("symbol"),
                "timeframe": overlay.get("timeframe"),
                "strategy_id": overlay.get("strategy_id"),
                "start_time": _isoformat(datetime.fromtimestamp(start_epoch, tz=timezone.utc)) if start_epoch else None,
                "end_time": _isoformat(datetime.fromtimestamp(end_epoch, tz=timezone.utc)) if end_epoch else None,
                "payload": overlay.get("payload"),
            }

        return {
            "current_epoch": current_epoch,
            "raw": [_with_meta(ov) for ov in raw_overlays],
            "visible": [_with_meta(ov) for ov in visible],
        }

    def subscribe(self) -> Tuple[str, Queue]:
        """Register a streaming subscriber and return its token/queue."""

        channel: Queue = Queue(maxsize=256)
        token = str(uuid.uuid4())
        with self._lock:
            self._subscribers[token] = channel
        return token, channel

    def unsubscribe(self, token: str) -> None:
        """Remove a streaming subscriber and drain its queue."""

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
            channels = list(self._subscribers.values())
        dropped_messages = 0
        for channel in channels:
            try:
                channel.put_nowait(message)
            except Full:
                try:
                    channel.get_nowait()
                except Empty:
                    pass
                try:
                    channel.put_nowait(message)
                except Full:
                    dropped_messages += 1
                    continue
        return len(channels), dropped_messages

    @staticmethod
    def _overlay_points_for_payload(payload: Mapping[str, Any]) -> int:
        points = 0
        for key in (
            "price_lines",
            "markers",
            "touchPoints",
            "touch_points",
            "boxes",
            "segments",
            "polylines",
            "bubbles",
            "regime_blocks",
        ):
            entries = payload.get(key)
            if isinstance(entries, list):
                points += len(entries)
        return points

    @staticmethod
    def _entry_fingerprint(entries: Sequence[Mapping[str, Any]]) -> Tuple[int, Optional[str], Optional[str]]:
        if not entries:
            return (0, None, None)
        last = entries[-1]
        marker: Optional[str] = None
        kind: Optional[str] = None
        if isinstance(last, Mapping):
            kind_value = last.get("type")
            kind = str(kind_value) if kind_value is not None else None
            for key in ("id", "event_id", "trade_id", "time", "created_at", "timestamp", "message"):
                value = last.get(key)
                if value is not None:
                    marker = str(value)
                    break
        return (len(entries), kind, marker)

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
        # Count UTF-8 bytes directly so payload size reflects transport cost.
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

    @staticmethod
    def _overlay_cache_key(overlay: Mapping[str, Any], ordinal: int) -> str:
        if not isinstance(overlay, Mapping):
            return f"overlay:{ordinal}"
        explicit = overlay.get("id")
        if explicit:
            return str(explicit)
        parts = [
            str(overlay.get("type") or "overlay"),
            str(overlay.get("strategy_id") or ""),
            str(overlay.get("symbol") or ""),
            str(overlay.get("timeframe") or ""),
            str(overlay.get("instrument_id") or ""),
            str(overlay.get("source") or ""),
            str(ordinal),
        ]
        return "|".join(parts)

    @staticmethod
    def _overlay_payload_fingerprint(overlay: Mapping[str, Any]) -> str:
        try:
            return json.dumps(overlay, sort_keys=True, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return str(overlay)

    def _build_overlay_delta(
        self,
        cache: Dict[str, Any],
        overlays: Sequence[Mapping[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        previous_entries = cache.get("overlay_entries")
        previous_fingerprints = cache.get("overlay_fingerprints")
        previous_order = cache.get("overlay_order")
        previous_seq = int(cache.get("overlay_seq") or 0)
        if not isinstance(previous_entries, dict) or not isinstance(previous_fingerprints, dict) or not isinstance(previous_order, list):
            previous_entries = {}
            previous_fingerprints = {}
            previous_order = []

        next_entries: Dict[str, Dict[str, Any]] = {}
        next_fingerprints: Dict[str, str] = {}
        next_order: List[str] = []
        for idx, overlay in enumerate(overlays):
            if not isinstance(overlay, Mapping):
                continue
            key = self._overlay_cache_key(overlay, idx)
            next_entries[key] = dict(overlay)
            next_fingerprints[key] = self._overlay_payload_fingerprint(overlay)
            next_order.append(key)

        if (
            previous_order == next_order
            and all(previous_fingerprints.get(key) == next_fingerprints.get(key) for key in next_order)
            and len(previous_entries) == len(next_entries)
        ):
            return None

        next_seq = previous_seq + 1
        ops: List[Dict[str, Any]] = []
        removed_keys = [key for key in previous_order if key not in next_entries]
        for key in removed_keys:
            ops.append({"op": "remove", "key": key})
        for key in next_order:
            if previous_fingerprints.get(key) != next_fingerprints.get(key):
                ops.append({"op": "upsert", "key": key, "overlay": next_entries[key]})

        cache["overlay_entries"] = next_entries
        cache["overlay_fingerprints"] = next_fingerprints
        cache["overlay_order"] = next_order
        cache["overlay_seq"] = next_seq
        return {
            "seq": next_seq,
            "base_seq": previous_seq,
            "ops": ops,
        }

    @staticmethod
    def _overlay_delta_op_counts(delta: Mapping[str, Any]) -> Dict[str, int]:
        ops = delta.get("ops")
        if not isinstance(ops, list):
            return {}
        counts: Dict[str, int] = {}
        for op in ops:
            if not isinstance(op, Mapping):
                continue
            key = str(op.get("op") or "unknown").lower()
            counts[key] = counts.get(key, 0) + 1
        return counts

    @classmethod
    def _count_overlay_points(cls, overlays: Sequence[Mapping[str, Any]]) -> int:
        points = 0
        for overlay in overlays or []:
            if not isinstance(overlay, Mapping):
                continue
            payload = overlay.get("payload")
            if isinstance(payload, Mapping):
                points += cls._overlay_points_for_payload(payload)
        return points

    @classmethod
    def _overlay_change_metrics(
        cls,
        before: Sequence[Mapping[str, Any]],
        after: Sequence[Mapping[str, Any]],
    ) -> Tuple[float, float]:
        changed = 0
        before_len = len(before or [])
        after_len = len(after or [])
        min_len = min(before_len, after_len)
        for idx in range(min_len):
            prev = before[idx] if isinstance(before[idx], Mapping) else {}
            curr = after[idx] if isinstance(after[idx], Mapping) else {}
            prev_type = str(prev.get("type") or "")
            curr_type = str(curr.get("type") or "")
            prev_points = cls._overlay_points_for_payload(prev.get("payload")) if isinstance(prev.get("payload"), Mapping) else 0
            curr_points = cls._overlay_points_for_payload(curr.get("payload")) if isinstance(curr.get("payload"), Mapping) else 0
            if prev_type != curr_type or prev_points != curr_points:
                changed += 1
        changed += abs(before_len - after_len)
        points_changed = abs(cls._count_overlay_points(after or []) - cls._count_overlay_points(before or []))
        return float(changed), float(points_changed)

    def _overlay_payload_metrics(self, payload: Mapping[str, Any]) -> Tuple[int, int]:
        overlay_count = 0
        overlay_points = 0

        def consume(overlays: Any) -> None:
            nonlocal overlay_count, overlay_points
            if not isinstance(overlays, list):
                return
            for overlay in overlays:
                if not isinstance(overlay, Mapping):
                    continue
                overlay_count += 1
                overlay_payload = overlay.get("payload")
                if isinstance(overlay_payload, Mapping):
                    overlay_points += self._overlay_points_for_payload(overlay_payload)

        consume(payload.get("overlays"))
        series_list = payload.get("series")
        if isinstance(series_list, list):
            for series_entry in series_list:
                if not isinstance(series_entry, Mapping):
                    continue
                consume(series_entry.get("overlays"))
        return overlay_count, overlay_points

    def _visible_candles(self) -> List[Dict[str, Any]]:
        # Use first series for chart state (backward compatibility)
        primary_state = self._series_states[0] if self._series_states else None
        primary = primary_state.series if primary_state else None
        return self._chart_state_builder.visible_candles(
            primary,
            self.state.get("status"),
            primary_state.bar_index if primary_state else 0,
            self._intrabar_manager,
        )

    def _log_candle_sequence(
        self,
        stage: str,
        strategy_id: Optional[str],
        candles: Sequence[Any],
    ) -> None:
        if not candles or len(candles) < 2:
            return

        key = (stage, strategy_id or "unknown")

        def epoch_from_entry(entry: Any) -> Optional[int]:
            if isinstance(entry, Candle):
                return int(entry.time.timestamp())
            if isinstance(entry, Mapping):
                return self._normalise_epoch(entry.get("time"))
            if isinstance(entry, (int, float)):
                return int(entry)
            return None

        previous: Optional[int] = None
        first_epoch: Optional[int] = None
        second_epoch: Optional[int] = None
        last_epoch: Optional[int] = None
        for idx, entry in enumerate(candles):
            epoch = epoch_from_entry(entry)
            if epoch is None:
                if key not in self._candle_diag_null:
                    self._candle_diag_null.add(key)
                    context = self._runtime_log_context(
                        strategy_id=strategy_id,
                        stage=stage,
                        index=idx,
                    )
                    logger.error(with_log_context("bot_runtime_candle_missing_time", context))
                continue
            if first_epoch is None:
                first_epoch = epoch
            elif second_epoch is None:
                second_epoch = epoch
            last_epoch = epoch
            if previous is not None and epoch < previous:
                context = self._runtime_log_context(
                    strategy_id=strategy_id,
                    stage=stage,
                    index=idx,
                    prev=previous,
                    current=epoch,
                )
                logger.error(with_log_context("bot_runtime_candle_order_violation", context))
                return
            previous = epoch

        if first_epoch is None or last_epoch is None:
            return
        start_iso = _isoformat(datetime.fromtimestamp(first_epoch, tz=timezone.utc))
        second_iso = (
            _isoformat(datetime.fromtimestamp(second_epoch, tz=timezone.utc))
            if second_epoch is not None
            else None
        )
        end_iso = _isoformat(datetime.fromtimestamp(last_epoch, tz=timezone.utc))
        if key in self._candle_diag_seen:
            return
        self._candle_diag_seen.add(key)
        context = self._runtime_log_context(
            strategy_id=strategy_id,
            stage=stage,
            count=len(candles),
            start=start_iso,
            second=second_iso,
            end=end_iso,
        )
        logger.debug(with_log_context("bot_runtime_candle_sequence_ok", context))

    def _current_epoch(self) -> Optional[int]:
        # Use first series for current epoch (backward compatibility)
        primary_state = self._series_states[0] if self._series_states else None
        primary = primary_state.series if primary_state else None
        if not primary_state or not primary or not primary.candles:
            return None
        if primary_state.bar_index <= 0:
            status = str(self.state.get("status") or "").lower()
            if status in {"idle", "initialising"}:
                return None
        idx = min(max(primary_state.bar_index - 1, 0), len(primary.candles) - 1)
        candle = primary.candles[idx]
        return int(candle.time.timestamp())

    def _current_epoch_for(self, series: Optional[StrategySeries]) -> Optional[int]:
        state = self._series_state_for(series)
        if not series or not series.candles or state is None:
            return None
        if state.bar_index <= 0:
            status = str(self.state.get("status") or "").lower()
            if status in {"idle", "initialising"}:
                return None
        idx = min(max(state.bar_index - 1, 0), len(series.candles) - 1)
        candle = series.candles[idx]
        return int(candle.time.timestamp())

    def _visible_overlays(self) -> List[Dict[str, Any]]:
        status = str(self.state.get("status") or "").lower()
        return self._chart_state_builder.visible_overlays(
            self._chart_overlays,
            status,
            self._current_epoch(),
        )

    def _series_payloads(self) -> List[Dict[str, Any]]:
        status = str(self.state.get("status") or "").lower()
        payloads: List[Dict[str, Any]] = []
        for series in self._series:
            state = self._series_state_for(series)
            bar_index = state.bar_index if state else 0
            overlays = list(series.overlays or [])
            if series.trade_overlay:
                overlays.append(series.trade_overlay)
            # Build per-series stats including avg/largest calculations
            series_stats = series.risk_engine.stats()
            series_stats["total_fees"] = series_stats.get("fees_paid", 0.0)
            # Calculate avg and largest win/loss for this series
            tolerance = 1e-8
            win_pnls = []
            loss_pnls = []
            for trade in series.risk_engine.trades:
                if trade.is_active():
                    continue
                pnl = trade.net_pnl
                if pnl > tolerance:
                    win_pnls.append(pnl)
                elif pnl < -tolerance:
                    loss_pnls.append(pnl)
            series_stats["avg_win"] = round(sum(win_pnls) / len(win_pnls), 4) if win_pnls else 0.0
            series_stats["avg_loss"] = round(sum(loss_pnls) / len(loss_pnls), 4) if loss_pnls else 0.0
            series_stats["largest_win"] = round(max(win_pnls), 4) if win_pnls else 0.0
            series_stats["largest_loss"] = round(min(loss_pnls), 4) if loss_pnls else 0.0

            payloads.append(
                {
                    "strategy_id": series.strategy_id,
                    "symbol": series.symbol,
                    "timeframe": series.timeframe,
                    "datasource": series.datasource,
                    "exchange": series.exchange,
                    "instrument": series.instrument,
                    "candles": self._chart_state_builder.visible_candles(
                        series,
                        status,
                        bar_index,
                        self._intrabar_manager,
                    ),
                    "overlays": self._chart_state_builder.visible_overlays(
                        overlays,
                        status,
                        self._current_epoch_for(series),
                    ),
                    "trades": series.risk_engine.serialise_trades(),
                    "stats": series_stats,
                }
            )
        return payloads

    def _chart_state(self) -> Dict[str, Any]:
        candles = self._visible_candles()
        overlays = self._visible_overlays()
        payload = self._chart_state_builder.chart_state(
            candles,
            self._aggregate_trades(),
            self._last_stats or self._aggregate_stats(),
            overlays,
            self.logs(),
            self.decision_events(),
        )
        payload["series"] = self._series_payloads()
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
                    overlays = list(series.overlays or [])
                    if series.trade_overlay:
                        overlays.append(series.trade_overlay)
                    overlay_revision = (
                        status,
                        self._current_epoch_for(series),
                        len(overlays),
                    )
                    if cache.get("overlay_revision") != overlay_revision:
                        cache["visible_overlays"] = self._chart_state_builder.visible_overlays(
                            overlays,
                            status,
                            self._current_epoch_for(series),
                        )
                        cache["overlay_revision"] = overlay_revision
                    visible_overlays = cache.get("visible_overlays")
                    if isinstance(visible_overlays, list):
                        overlay_summary = self._overlay_summary(visible_overlays)
                        overlay_delta = self._build_overlay_delta(cache, visible_overlays)
                        logger.info(
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
                            logger.info(
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
