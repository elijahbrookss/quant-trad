"""Runtime snapshots and chart-state projection helpers."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.series_identity import canonical_series_key
from utils.log_context import with_log_context

from ..core import _isoformat

logger = logging.getLogger(__name__)


class RuntimeProjectionMixin:
    @staticmethod
    def _progress_state_for_status(status: Any) -> str:
        normalized = str(status or "").strip().lower()
        if normalized in {"running", "paused"}:
            return "progressing"
        if normalized == "degraded":
            return "degraded"
        if normalized in {"error", "stopped", "completed"}:
            return "stalled"
        if normalized in {"initialising", "starting"}:
            return "starting"
        return normalized or "idle"

    def logs(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            entries = list(self._logs)
        if limit and limit > 0:
            entries = entries[-limit:]
        return entries

    def decision_events(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            entries = list(self._decision_events)
        if limit and limit > 0:
            entries = entries[-limit:]
        return [entry.serialize() for entry in entries]

    def _update_state(self, candle: Candle, status: str = "running") -> Dict[str, Any]:
        update_started = time.perf_counter()
        stats_started = time.perf_counter()
        stats = self._aggregate_stats()
        stats_update_ms = max((time.perf_counter() - stats_started) * 1000.0, 0.0)
        observed_at = _isoformat(datetime.now(timezone.utc))
        with self._lock:
            self._last_stats = stats
        self._refresh_next_bar_at()
        progress = self._compute_progress()
        snapshot = {
            "status": status,
            "progress_state": self._progress_state_for_status(status),
            "progress": progress,
            "last_bar": candle.to_dict(),
            "stats": stats,
            "paused": self._paused,
            "next_bar_at": _isoformat(self._next_bar_at),
            "next_bar_in_seconds": self._seconds_until_next_bar(),
            "last_snapshot_at": observed_at,
            "last_useful_progress_at": observed_at,
        }
        with self._lock:
            self.state.update(snapshot)
        if self._state_callback:
            try:
                self._state_callback({"runtime": self.snapshot()})
            except Exception as exc:  # pragma: no cover
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
        payload.setdefault("progress_state", self._progress_state_for_status(payload.get("status")))
        if "next_bar_at" not in payload:
            payload["next_bar_at"] = _isoformat(self._next_bar_at)
        if "next_bar_in_seconds" not in payload:
            payload["next_bar_in_seconds"] = self._seconds_until_next_bar()
        if "started_at" not in payload and self._run_started_at is not None:
            payload["started_at"] = _isoformat(self._run_started_at)
        payload.setdefault("last_snapshot_at", payload.get("last_useful_progress_at"))
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
        return self._state_payload()

    def chart_payload(self) -> Dict[str, object]:
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
        logger.debug(
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
        if not self._prepared:
            raise RuntimeError("Runtime is not prepared. Call warm_up() or start() before overlay dump.")
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

    def _visible_candles(self) -> List[Dict[str, Any]]:
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
            series_stats = series.risk_engine.stats()
            series_stats["total_fees"] = series_stats.get("fees_paid", 0.0)
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
            instrument = series.instrument if isinstance(series.instrument, Mapping) else {}
            instrument_id = str(instrument.get("id") or "").strip()
            series_key = canonical_series_key(instrument_id, series.timeframe)
            if not series_key:
                raise RuntimeError(
                    f"bot_runtime_series_projection_invalid: missing instrument_id/timeframe for strategy={series.strategy_id} symbol={series.symbol}"
                )

            payloads.append(
                {
                    "series_key": series_key,
                    "strategy_id": series.strategy_id,
                    "symbol": series.symbol,
                    "timeframe": series.timeframe,
                    "datasource": series.datasource,
                    "exchange": series.exchange,
                    "instrument_id": instrument_id,
                    "instrument": series.instrument,
                    "candles": self._chart_state_builder.visible_candles(
                        series,
                        status,
                        bar_index,
                        self._intrabar_manager,
                    ),
                    "overlays": self._series_visible_overlays(series, status=status),
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
