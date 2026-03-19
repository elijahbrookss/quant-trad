"""Bot runtime lifecycle and per-bar execution loop."""

from __future__ import annotations

import logging
import time
from contextlib import nullcontext
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import Candle, StrategySignal, isoformat
from strategies.evaluator import evaluate_typed_rules
from utils.log_context import with_log_context
from utils.perf_log import perf_log, should_sample

from ..components import SignalConsumption, consume_signals
from ..core import (
    INTRABAR_BASE_SECONDS,
    OVERLAY_SUMMARY_INTERVAL,
    WALK_FORWARD_SAMPLE_INTERVAL,
    SeriesExecutionState,
    _isoformat,
)

logger = logging.getLogger(__name__)


class RuntimeExecutionLoopMixin:
    def _require_prepared(self, action: str) -> None:
        if self._prepared:
            return
        raise RuntimeError(
            f"Runtime is not prepared for {action}. Call warm_up() or start() first."
        )

    def reset(self) -> None:
        """Clear cached series so the runtime can restart fresh."""

        if self._thread and self._thread.is_alive():
            raise RuntimeError("Cannot reset a running bot runtime")
        with self._lock:
            self._prepared = False
            self._series = []
            self._series_states = []
            self._series_state_map = {}
            self._primary_series_key = None
            self._total_bars = 0
            self._chart_overlays = []
            self._overlay_summary_cache = {}
            self._last_stats = {}
            self._next_bar_at = None
            self._logs.clear()
            self._warnings.clear()
            self._decision_events.clear()
            self._intrabar_manager.clear_cache()
            self._run_started_at = None
            self._run_context = None
            self._runner = None
            self._push_series_cache = {}
            self._push_logs_fingerprint = None
            self._push_decisions_fingerprint = None
            self._log_revision = 0
            self._decision_revision = 0
            self._push_logs_revision = -1
            self._push_decisions_revision = -1
            self._push_payload_size_probe_count = 0
            self.state = {"status": "idle", "progress": 0.0, "paused": False}
        self._stop.clear()
        self._pause_event.set()
        self._paused = False
        if self._series_builder is not None:
            self._series_builder.reset()
        self._overlay_cache.clear()

    def needs_reset(self) -> bool:
        """Return True when the runtime finished and can be rerun."""

        status = str(self.state.get("status") or "").lower()
        finished = status in {"completed", "stopped", "error"}
        exhausted = bool(self._series_states) and all(
            state.done or state.bar_index >= state.total_bars for state in self._series_states
        )
        thread_active = self._thread and self._thread.is_alive()
        return not thread_active and (finished or exhausted)

    def reset_if_finished(self) -> None:
        """Reset cached series if the previous run completed."""

        if self.needs_reset():
            self.reset()

    def warm_up(self) -> None:
        """Prepare strategy sessions so the lens can query data."""

        if self._prepared:
            return
        self._ensure_prepared()

    def start(self) -> None:
        """Start the execution loop in blocking mode."""

        logger.info(
            with_log_context(
                "bot_runtime_start_invoked",
                self._runtime_log_context(
                    mode=self.mode,
                    run_type=self.run_type,
                    series_runner=self._series_runner_type,
                    thread_alive=bool(self._thread and self._thread.is_alive()),
                ),
            )
        )
        self._set_phase("prepare", "bot_runtime_prepare_start")
        self._ensure_prepared()
        if self._thread and self._thread.is_alive():
            logger.info(with_log_context("bot_runtime_start_ignored", self._runtime_log_context()))
            return
        logger.info(with_log_context("bot_runtime_run_context_start", self._runtime_log_context()))
        self._stop.clear()
        self._pause_event.set()
        self._paused = False
        self._run_started_at = datetime.now(timezone.utc)
        self._run_context = self._build_run_context()
        wallet_config = self.config.get("wallet_config") if isinstance(self.config.get("wallet_config"), Mapping) else {}
        balances = wallet_config.get("balances") if isinstance(wallet_config, Mapping) else {}
        if isinstance(balances, Mapping):
            self._emit_wallet_initialized_event(balances)
        logger.info(with_log_context("bot_runtime_run_context_ready", self._runtime_log_context()))
        with self._lock:
            self.state.update(
                {"status": "starting", "paused": False, "started_at": _isoformat(self._run_started_at)}
            )
        self._set_phase("start_run", "bot_runtime_run_starting")
        logger.info(
            with_log_context(
                "bot_runtime_run_starting",
                self._runtime_log_context(
                    mode=self.mode,
                    run_type=self.run_type,
                    series_runner=self._series_runner_type,
                    series=len(self._series_states),
                ),
            )
        )
        self._log_event("start", message="Bot runtime started", mode=self.mode, run_type=self.run_type)
        self._push_update("start")
        self._run()

    def _run(self) -> None:
        try:
            logger.debug(with_log_context("bot_runtime_thread_started", self._runtime_log_context()))
            self._execute_loop()
        except Exception as exc:  # pragma: no cover - defensive logging
            context = self._runtime_log_context(error=str(exc))
            logger.exception(with_log_context("bot_runtime_loop_failed", context))
            try:
                if self._run_context is not None:
                    self._emit_runtime_error_event(location="bot_runtime._run", error=exc)
            except Exception:
                logger.exception(with_log_context("bot_runtime_runtime_error_emit_failed", context))
            self._set_error_state(str(exc))
            self._push_update("error")
            self._persist_runtime_state("error")
            self._flush_persistence_buffer("runtime_loop_failed")
            self._flush_step_trace_buffer("runtime_loop_failed", shutdown=True)

    def _execute_loop(self) -> None:
        self._require_prepared("execute_loop")
        status = "running"
        loop_started = datetime.now(timezone.utc)
        self._set_phase("running", "bot_runtime_running")
        self._log_event(
            "running",
            message="Bot execution loop started",
            series_runner=self._series_runner_type,
            series_count=len(self._series_states),
        )
        self._start_overlay_aggregator()
        try:
            self._runner = self._build_series_runner()
            self._runner.run()
        finally:
            self._stop_overlay_aggregator()
        runtime_status = str(self.state.get("status") or "").lower()
        if runtime_status == "error":
            status = "error"
        elif self._stop.is_set():
            status = "stopped"
        elif not self._live_mode:
            status = "completed"
        self._next_bar_at = None
        self._record_step_trace(
            "run_loop",
            started_at=loop_started,
            ended_at=datetime.now(timezone.utc),
            ok=(status != "error"),
            context={
                "status": status,
                "series_count": len(self._series_states),
            },
        )
        self._log_event(status, message=f"Bot runtime {status}")
        if status in {"completed", "stopped"}:
            duration_seconds = None
            if self._run_started_at is not None:
                duration_seconds = (datetime.now(timezone.utc) - self._run_started_at).total_seconds()
            summary = self._aggregate_stats()
            drawdown = self._max_drawdown_from_trades()
            context = self._runtime_log_context(
                status=status,
                trades=summary.get("completed_trades"),
                gross_pnl=summary.get("gross_pnl"),
                net_pnl=summary.get("net_pnl"),
                fees=summary.get("fees_paid"),
                drawdown=drawdown,
                duration_seconds=duration_seconds,
            )
            logger.info(with_log_context("bot_run_end_summary", context))
        # Update state with last candle from first series (backward compatibility)
        if self._series and self._series[0].candles:
            self._update_state(self._series[0].candles[-1], status=status)
        else:
            with self._lock:
                self.state.update({"status": status})
        self._push_update(status)
        self._persist_runtime_state(status)
        if status in {"completed", "stopped"}:
            self._persist_run_artifact(status)
        self._flush_persistence_buffer("runtime_loop_complete")
        self._flush_step_trace_buffer("runtime_loop_complete", shutdown=True)

    def _step_series_state(self, state: SeriesExecutionState) -> None:
        if state.done:
            return
        if state.intrabar_active():
            self._step_intrabar(state)
            return
        if state.bar_index >= state.total_bars:
            state.done = True
            return
        series = state.series
        strategy_id = getattr(series, "strategy_id", None)
        symbol = getattr(series, "symbol", None)
        timeframe = getattr(series, "timeframe", None)
        step_started = datetime.now(timezone.utc)
        step_context: Dict[str, Any] = {
            "bar_index": state.bar_index,
            "total_bars": state.total_bars,
            "series_key": self._strategy_key(series),
        }
        candle_update_ms: Optional[float] = None
        overlays_update_ms: Optional[float] = None
        pending_signals_ops_ms: Optional[float] = None
        execution_ms: Optional[float] = None
        stats_update_ms: Optional[float] = None
        persistence_ms: Optional[float] = None
        db_commit_ms: Optional[float] = None
        delta_build_ms: Optional[float] = None
        delta_serialize_ms: Optional[float] = None
        stream_emit_ms: Optional[float] = None
        indicators_count: Optional[float] = None
        overlays_changed_count: Optional[float] = None
        overlay_points_changed: Optional[float] = None
        signals_emitted_count: Optional[float] = None
        subscribers_count: Optional[float] = None
        series_overlay_entries_ms: Optional[float] = None
        series_overlay_indicator_entries_ms: Optional[float] = None
        series_overlay_regime_build_ms: Optional[float] = None
        series_overlay_indicator_entries_count: Optional[float] = None
        series_overlay_regime_entries_count: Optional[float] = None
        series_overlay_total_entries_count: Optional[float] = None
        series_overlay_regime_mode_rebuild: Optional[float] = None
        trades_touched_count: float = 0.0
        decision_events_logged = 0
        execution_events_logged = 0
        trade_events_processed = 0
        entry_created = False
        trade_lock_wait_ms: float = 0.0
        trade_lock_hold_ms: float = 0.0
        sample_enabled = self._obs_enabled and should_sample(self._obs_step_sample_rate)
        base_context = self._series_log_context(
            series,
            bar_index=state.bar_index,
            total_bars=state.total_bars,
        )
        perf_context = (
            perf_log(
                "bot_runtime_step_series_state",
                logger=logger,
                base_context=base_context,
                enabled=sample_enabled,
                slow_ms=self._obs_slow_ms,
            )
            if sample_enabled
            else nullcontext()
        )
        try:
            with perf_context:
                candle_update_started = time.perf_counter()
                candle = series.candles[state.bar_index]
                state.active_candle = candle
                epoch = int(candle.time.timestamp())
                bar_time = _isoformat(candle.time)
                candle_update_ms = max((time.perf_counter() - candle_update_started) * 1000.0, 0.0)
                step_context["epoch"] = epoch
                step_context["bar_time"] = bar_time

                context = self._series_log_context(
                    series,
                    bar_index=state.bar_index,
                    epoch=epoch,
                )
                logger.debug(with_log_context("apply_bar", context))
                if state.bar_index % WALK_FORWARD_SAMPLE_INTERVAL == 0:
                    info_context = self._series_log_context(
                        series,
                        bar_index=state.bar_index,
                        bar_time=isoformat(candle.time),
                        status=self.state.get("status"),
                    )
                    logger.info(with_log_context("walk_forward_step", info_context))

                signal_eval_started = datetime.now(timezone.utc)
                signal_event_logged = False
                next_last_evaluated_epoch = state.last_evaluated_epoch
                next_last_consumed_epoch = state.last_consumed_epoch
                try:
                    (
                        consumed_signals,
                        direction,
                        signals_pending,
                        signal_eval_metrics,
                        next_last_evaluated_epoch,
                        next_last_consumed_epoch,
                    ) = self._next_signal_for(
                        state,
                        series,
                        candle,
                        epoch,
                    )
                    overlays_update_ms = signal_eval_metrics.get("overlays_update_ms")
                    pending_signals_ops_ms = signal_eval_metrics.get("pending_signals_ops_ms")
                    indicators_count = signal_eval_metrics.get("indicators_count")
                    overlays_changed_count = signal_eval_metrics.get("overlays_changed_count")
                    overlay_points_changed = signal_eval_metrics.get("overlay_points_changed")
                    signals_emitted_count = signal_eval_metrics.get("signals_emitted_count")
                    series_overlay_entries_ms = signal_eval_metrics.get("series_overlay_entries_ms")
                    series_overlay_indicator_entries_ms = signal_eval_metrics.get("series_overlay_indicator_entries_ms")
                    series_overlay_regime_build_ms = signal_eval_metrics.get("series_overlay_regime_build_ms")
                    series_overlay_indicator_entries_count = signal_eval_metrics.get(
                        "series_overlay_indicator_entries_count"
                    )
                    series_overlay_regime_entries_count = signal_eval_metrics.get("series_overlay_regime_entries_count")
                    series_overlay_total_entries_count = signal_eval_metrics.get("series_overlay_total_entries_count")
                    series_overlay_regime_mode_rebuild = signal_eval_metrics.get("series_overlay_regime_mode_rebuild")
                    step_context["signals_pending"] = signals_pending
                    self._record_signal_consumption(state, epoch, consumed_signals, direction)

                    # Debug: Log signal consumption result
                    if direction is not None:
                        context = self._series_log_context(
                            series,
                            bar_index=state.bar_index,
                            epoch=epoch,
                            direction=direction,
                        )
                        logger.debug(with_log_context("signal_consumed", context))
                        self._emit_signal_event(series=series, candle=candle, direction=direction)
                        signal_event_logged = True
                    self._record_step_trace(
                        "step_signal_eval",
                        started_at=signal_eval_started,
                        ended_at=datetime.now(timezone.utc),
                        ok=True,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        context={
                            "bar_index": state.bar_index,
                            "bar_time": bar_time,
                            "consumed_signals_count": len(consumed_signals),
                            "direction_present": bool(direction),
                            "signal_event_logged": signal_event_logged,
                            "epochs_evaluated_this_tick": signal_eval_metrics.get("epochs_evaluated_this_tick"),
                            "strategy_eval_ms": signal_eval_metrics.get("strategy_eval_ms"),
                            "indicator_eval_ms": signal_eval_metrics.get("indicator_eval_ms"),
                            "rule_eval_ms": signal_eval_metrics.get("rule_eval_ms"),
                            "indicator_state_update_ms": signal_eval_metrics.get("indicator_state_update_ms"),
                            "signal_eval_ms": signal_eval_metrics.get("signal_eval_ms"),
                            "overlay_projection_ms": signal_eval_metrics.get("overlay_projection_ms"),
                            "overlay_projection_skipped_count": signal_eval_metrics.get("overlay_projection_skipped_count"),
                            "overlay_projection_projector_ms": signal_eval_metrics.get("overlay_projection_projector_ms"),
                            "overlay_projection_delta_ms": signal_eval_metrics.get("overlay_projection_delta_ms"),
                            "overlay_projection_normalize_ms": signal_eval_metrics.get("overlay_projection_normalize_ms"),
                            "overlay_projection_fingerprint_ms": signal_eval_metrics.get("overlay_projection_fingerprint_ms"),
                            "overlay_projection_entries_total": signal_eval_metrics.get("overlay_projection_entries_total"),
                            "overlay_projection_entries_changed": signal_eval_metrics.get("overlay_projection_entries_changed"),
                            "overlay_projection_ops_count": signal_eval_metrics.get("overlay_projection_ops_count"),
                            "overlay_projection_normalize_cache_hits": signal_eval_metrics.get("overlay_projection_normalize_cache_hits"),
                            "overlay_projection_normalize_cache_misses": signal_eval_metrics.get("overlay_projection_normalize_cache_misses"),
                            "state_revisions_changed_count": signal_eval_metrics.get("state_revisions_changed_count"),
                            "signals_emitted_count": signal_eval_metrics.get("signals_emitted_count"),
                            "overlays_update_ms": signal_eval_metrics.get("overlays_update_ms"),
                            "pending_signals_ops_ms": signal_eval_metrics.get("pending_signals_ops_ms"),
                            "indicators_count": signal_eval_metrics.get("indicators_count"),
                            "overlays_changed_count": signal_eval_metrics.get("overlays_changed_count"),
                            "overlay_points_changed": signal_eval_metrics.get("overlay_points_changed"),
                            "series_overlay_entries_ms": signal_eval_metrics.get("series_overlay_entries_ms"),
                            "series_overlay_indicator_entries_ms": signal_eval_metrics.get(
                                "series_overlay_indicator_entries_ms"
                            ),
                            "series_overlay_regime_build_ms": signal_eval_metrics.get("series_overlay_regime_build_ms"),
                            "series_overlay_indicator_entries_count": signal_eval_metrics.get(
                                "series_overlay_indicator_entries_count"
                            ),
                            "series_overlay_regime_entries_count": signal_eval_metrics.get(
                                "series_overlay_regime_entries_count"
                            ),
                            "series_overlay_total_entries_count": signal_eval_metrics.get(
                                "series_overlay_total_entries_count"
                            ),
                            "series_overlay_regime_mode_rebuild": signal_eval_metrics.get(
                                "series_overlay_regime_mode_rebuild"
                            ),
                        },
                    )
                except Exception as exc:
                    self._record_step_trace(
                        "step_signal_eval",
                        started_at=signal_eval_started,
                        ended_at=datetime.now(timezone.utc),
                        ok=False,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        error=str(exc),
                        context={
                            "bar_index": state.bar_index,
                            "bar_time": bar_time,
                            "signal_event_logged": signal_event_logged,
                            "epochs_evaluated_this_tick": None,
                            "strategy_eval_ms": None,
                            "indicator_eval_ms": None,
                            "rule_eval_ms": None,
                            "signals_emitted_count": None,
                            "indicator_state_update_ms": None,
                            "signal_eval_ms": None,
                            "overlay_projection_ms": None,
                            "overlay_projection_skipped_count": None,
                            "overlay_projection_projector_ms": None,
                            "overlay_projection_delta_ms": None,
                            "overlay_projection_normalize_ms": None,
                            "overlay_projection_fingerprint_ms": None,
                            "overlay_projection_entries_total": None,
                            "overlay_projection_entries_changed": None,
                            "overlay_projection_ops_count": None,
                            "overlay_projection_normalize_cache_hits": None,
                            "overlay_projection_normalize_cache_misses": None,
                            "state_revisions_changed_count": None,
                            "overlays_update_ms": None,
                            "pending_signals_ops_ms": None,
                            "indicators_count": None,
                            "overlays_changed_count": None,
                            "overlay_points_changed": None,
                            "series_overlay_entries_ms": None,
                            "series_overlay_indicator_entries_ms": None,
                            "series_overlay_regime_build_ms": None,
                            "series_overlay_indicator_entries_count": None,
                            "series_overlay_regime_entries_count": None,
                            "series_overlay_total_entries_count": None,
                            "series_overlay_regime_mode_rebuild": None,
                        },
                    )
                    raise

            execution_started_perf = time.perf_counter()
            decision_flow_started = datetime.now(timezone.utc)
            decision_flow_started_perf = time.perf_counter()
            blocking_trade = None
            new_trade = None
            try:
                # Attempt to create trade from signal
                if direction is not None:
                    instrument_id = None
                    if isinstance(series.instrument, Mapping):
                        instrument_id = series.instrument.get("id")
                    if not instrument_id:
                        decision_events_logged += 1
                        self._emit_decision_event(
                            series=series,
                            candle=candle,
                            decision="rejected",
                            direction=direction,
                            signal_price=float(candle.close),
                            reason_code="DECISION_REJECTED_INSTRUMENT_MISSING",
                            message="Instrument id missing.",
                            trade_id=None,
                            context={
                                "signal_type": "strategy_signal",
                                "signal_direction": direction,
                                "signal_price": candle.close,
                                "blocked_instrument_id": None,
                                "instrument_id": None,
                            },
                        )
                        direction = None
                    else:
                        trade_lock_wait_started = time.perf_counter()
                        with self._trade_lock:
                            trade_lock_wait_ms = max((time.perf_counter() - trade_lock_wait_started) * 1000.0, 0.0)
                            trade_lock_hold_started = time.perf_counter()
                            blocking_trade = self._active_trade_for_instrument(
                                instrument_id,
                                skip_series=series,
                            )
                            if blocking_trade is None:
                                new_trade = series.risk_engine.maybe_enter(candle, direction)
                            trade_lock_hold_ms = max((time.perf_counter() - trade_lock_hold_started) * 1000.0, 0.0)

                # Log decision event
                if direction is not None:
                    if new_trade is not None:
                        # Signal was accepted and trade was opened
                        decision_events_logged += 1
                        self._emit_decision_event(
                            series=series,
                            candle=candle,
                            decision="accepted",
                            direction=direction,
                            signal_price=float(candle.close),
                            reason_code="DECISION_ACCEPTED",
                            message=None,
                            trade_id=new_trade.trade_id,
                            context={"trade_time": _isoformat(new_trade.entry_time)},
                        )
                    else:
                        # Signal was rejected (no trade opened)
                        # Determine rejection reason
                        rejection_reason = "Active trade already open"
                        rejection_meta: Optional[Dict[str, Any]] = None
                        blocking_trade_id: Optional[str] = None
                        rejection_code: Optional[str] = None
                        rejection_context: Dict[str, Any] = {
                            "signal_type": "strategy_signal",
                            "signal_direction": direction,
                            "signal_price": candle.close,
                        }
                        if blocking_trade is not None:
                            rejection_reason = "Active trade already open for instrument"
                            rejection_code = "DECISION_REJECTED_ACTIVE_TRADE"
                            blocked_instrument_id = None
                            if isinstance(series.instrument, Mapping):
                                blocked_instrument_id = series.instrument.get("id")
                            blocking_trade_id = getattr(blocking_trade, "trade_id", None)
                            rejection_meta = {
                                "active_trade_id": blocking_trade_id,
                                "blocked_instrument_id": blocked_instrument_id,
                            }
                            rejection_context.update(rejection_meta)
                        elif series.risk_engine.active_trade is None:
                            rejection_reason = series.risk_engine.last_rejection_reason or "Risk engine declined entry"
                            rejection_code = "DECISION_REJECTED_RISK_ENGINE"
                            rejection_meta = series.risk_engine.last_rejection_detail
                            if isinstance(rejection_meta, Mapping):
                                rejection_context.update(rejection_meta)
                            rejection_context["risk_engine_reason"] = rejection_reason
                        resolved_trade_id, metadata_payload = self._normalise_rejection_metadata(
                            rejection_meta,
                            blocking_trade_id,
                        )

                        decision_events_logged += 1
                        rejection_context.update(metadata_payload)
                        self._emit_decision_event(
                            series=series,
                            candle=candle,
                            decision="rejected",
                            direction=direction,
                            signal_price=float(candle.close),
                            reason_code=rejection_code or "DECISION_REJECTED",
                            message=rejection_reason,
                            trade_id=resolved_trade_id,
                            context=rejection_context,
                        )

                if new_trade is not None:
                    entry_created = True
                    targets = [
                        {"name": leg.name, "price": round(leg.target_price, 4)}
                        for leg in new_trade.legs
                    ]
                    self._log_event(
                        "entry",
                        series,
                        candle,
                        trade_id=new_trade.trade_id,
                        direction=direction,
                        entry_price=round(new_trade.entry_price, 4),
                        stop_price=round(new_trade.stop_price, 4),
                        targets=targets,
                        bar_index=state.bar_index,
                        contracts=sum(max(leg.contracts, 0) for leg in new_trade.legs),
                        trade_time=_isoformat(new_trade.entry_time),
                    )
                    execution_events_logged += 1
                    self._emit_entry_filled_event(
                        series=series,
                        candle=candle,
                        trade=new_trade,
                        direction=str(direction or ""),
                    )
                    self._persist_trade_entry(
                        series,
                        new_trade,
                    )
                    self._update_trade_overlay(series)
                self._record_step_trace(
                    "step_decision_flow",
                    started_at=decision_flow_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    context={
                        "bar_index": state.bar_index,
                        "bar_time": bar_time,
                        "decision_events_logged": decision_events_logged,
                        "entry_created": entry_created,
                        "trade_lock_wait_ms": trade_lock_wait_ms,
                        "trade_lock_hold_ms": trade_lock_hold_ms,
                    },
                )
            except Exception as exc:
                self._record_step_trace(
                    "step_decision_flow",
                    started_at=decision_flow_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=False,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    error=str(exc),
                    context={
                        "bar_index": state.bar_index,
                        "bar_time": bar_time,
                        "decision_events_logged": decision_events_logged,
                        "entry_created": entry_created,
                        "trade_lock_wait_ms": trade_lock_wait_ms,
                        "trade_lock_hold_ms": trade_lock_hold_ms,
                    },
                )
                raise
            decision_flow_ms = max((time.perf_counter() - decision_flow_started_perf) * 1000.0, 0.0)
            prime_started_perf = time.perf_counter()
            prime_started = datetime.now(timezone.utc)
            try:
                trade_events = self._prime_intrabar_or_step_bar(state, candle)
                execution_prime_ms = max((time.perf_counter() - prime_started_perf) * 1000.0, 0.0)
                self._record_step_trace(
                    "step_execution_prime",
                    started_at=prime_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    context={
                        "bar_index": state.bar_index,
                        "bar_time": bar_time,
                        "trade_events_count": len(trade_events),
                        "intrabar_active_after_prime": bool(state.intrabar_active()),
                    },
                )
            except Exception as exc:
                self._record_step_trace(
                    "step_execution_prime",
                    started_at=prime_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=False,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    error=str(exc),
                    context={
                        "bar_index": state.bar_index,
                        "bar_time": bar_time,
                        "trade_events_count": None,
                        "intrabar_active_after_prime": bool(state.intrabar_active()),
                    },
                )
                raise
            exit_settlement = getattr(series.risk_engine, "exit_settlement", None) if series.risk_engine else None
            settlement_started = datetime.now(timezone.utc)
            settlement_started_perf = time.perf_counter()
            try:
                self._settlement_applier.apply(trade_events, exit_settlement)
                self._record_step_trace(
                    "settlement_apply",
                    started_at=settlement_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    strategy_id=getattr(series, "strategy_id", None),
                    symbol=getattr(series, "symbol", None),
                    timeframe=getattr(series, "timeframe", None),
                    context={"bar_index": state.bar_index, "bar_time": bar_time, "events": len(trade_events)},
                )
            except Exception as exc:
                self._record_step_trace(
                    "settlement_apply",
                    started_at=settlement_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=False,
                    strategy_id=getattr(series, "strategy_id", None),
                    symbol=getattr(series, "symbol", None),
                    timeframe=getattr(series, "timeframe", None),
                    error=str(exc),
                    context={"bar_index": state.bar_index, "bar_time": bar_time, "events": len(trade_events)},
                )
                raise
            settlement_ms = max((time.perf_counter() - settlement_started_perf) * 1000.0, 0.0)
            event_processing_started = datetime.now(timezone.utc)
            event_processing_started_perf = time.perf_counter()
            try:
                for event in trade_events:
                    trade_events_processed += 1
                    trade_time = self._trade_entry_time(series, event.get("trade_id"))
                    self._log_event(
                        event.get("type", "event"),
                        series,
                        candle,
                        trade_id=event.get("trade_id"),
                        leg=event.get("leg"),
                        price=event.get("price"),
                        event_time=event.get("time"),
                        bar_index=state.bar_index,
                        contracts=event.get("contracts"),
                        pnl=event.get("pnl"),
                        net_pnl=event.get("net_pnl"),
                        gross_pnl=event.get("gross_pnl"),
                        fees_paid=event.get("fees_paid"),
                        currency=event.get("currency"),
                        trade_time=trade_time,
                    )
                    raw_subtype = event.get("type")
                    event_ts = event.get("time")
                    if raw_subtype and event_ts:
                        event_subtype = str(raw_subtype)
                        if event_subtype in {"target", "stop", "close"}:
                            execution_events_logged += 1
                            self._emit_exit_filled_event(
                                series=series,
                                candle=candle,
                                event=event,
                            )
                            if event_subtype == "close":
                                self._persist_trade_close(series, event)
                self._update_trade_overlay(series)
                state.last_evaluated_epoch = max(state.last_evaluated_epoch, next_last_evaluated_epoch)
                state.last_consumed_epoch = max(state.last_consumed_epoch, next_last_consumed_epoch)
                series.last_consumed_epoch = state.last_consumed_epoch
                self._record_step_trace(
                    "step_trade_event_processing",
                    started_at=event_processing_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    context={
                        "bar_index": state.bar_index,
                        "bar_time": bar_time,
                        "trade_events_count": len(trade_events),
                        "trade_events_processed": trade_events_processed,
                        "execution_events_logged": execution_events_logged,
                    },
                )
            except Exception as exc:
                self._record_step_trace(
                    "step_trade_event_processing",
                    started_at=event_processing_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=False,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    error=str(exc),
                    context={
                        "bar_index": state.bar_index,
                        "bar_time": bar_time,
                        "trade_events_count": len(trade_events),
                        "trade_events_processed": trade_events_processed,
                        "execution_events_logged": execution_events_logged,
                    },
                )
                raise
            event_processing_ms = max((time.perf_counter() - event_processing_started_perf) * 1000.0, 0.0)
            execution_ms = max((time.perf_counter() - execution_started_perf) * 1000.0, 0.0)
            if not state.intrabar_active():
                finalize_started = datetime.now(timezone.utc)
                try:
                    finalize_metrics = self._finalize_bar_step(state, candle)
                    stats_update_ms = finalize_metrics.get("stats_update_ms")
                    persistence_ms = finalize_metrics.get("persist_ms")
                    db_commit_ms = finalize_metrics.get("db_commit_ms")
                    delta_build_ms = finalize_metrics.get("delta_build_ms")
                    delta_serialize_ms = finalize_metrics.get("delta_serialize_ms")
                    stream_emit_ms = finalize_metrics.get("stream_emit_ms")
                    subscribers_count = finalize_metrics.get("subscribers_count")
                    self._record_step_trace(
                        "step_finalize_bar",
                        started_at=finalize_started,
                        ended_at=datetime.now(timezone.utc),
                        ok=True,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        context={
                            "bar_index": state.bar_index,
                            "bar_time": bar_time,
                            "done": bool(state.done),
                            "finalize_residual_ms": finalize_metrics.get("finalize_residual_ms"),
                            "persist_ms": finalize_metrics.get("persist_ms"),
                            "stats_update_ms": finalize_metrics.get("stats_update_ms"),
                            "db_commit_ms": finalize_metrics.get("db_commit_ms"),
                            "delta_build_ms": finalize_metrics.get("delta_build_ms"),
                            "delta_serialize_ms": finalize_metrics.get("delta_serialize_ms"),
                            "stream_emit_ms": finalize_metrics.get("stream_emit_ms"),
                            "subscribers_count": finalize_metrics.get("subscribers_count"),
                            "step_trace_queue_depth": finalize_metrics.get("step_trace_queue_depth"),
                            "step_trace_dropped_count": finalize_metrics.get("step_trace_dropped_count"),
                            "step_trace_persist_lag_ms": finalize_metrics.get("step_trace_persist_lag_ms"),
                            "step_trace_persist_batch_ms": finalize_metrics.get("step_trace_persist_batch_ms"),
                            "step_trace_persist_error_count": finalize_metrics.get("step_trace_persist_error_count"),
                        },
                    )
                except Exception as exc:
                    self._record_step_trace(
                        "step_finalize_bar",
                        started_at=finalize_started,
                        ended_at=datetime.now(timezone.utc),
                        ok=False,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        error=str(exc),
                        context={
                            "bar_index": state.bar_index,
                            "bar_time": bar_time,
                            "done": bool(state.done),
                            "finalize_residual_ms": None,
                            "persist_ms": None,
                            "stats_update_ms": None,
                            "db_commit_ms": None,
                            "delta_build_ms": None,
                            "delta_serialize_ms": None,
                            "stream_emit_ms": None,
                            "subscribers_count": None,
                            "step_trace_queue_depth": None,
                            "step_trace_dropped_count": None,
                            "step_trace_persist_lag_ms": None,
                            "step_trace_persist_batch_ms": None,
                            "step_trace_persist_error_count": None,
                        },
                    )
                    raise
            trades_touched_count = float(trade_events_processed + (1 if entry_created else 0))
            step_context["trade_events_count"] = len(trade_events)
            step_context["trade_events_processed"] = trade_events_processed
            step_context["execution_events_logged"] = execution_events_logged
            step_context["decision_events_logged"] = decision_events_logged
            step_context["entry_created"] = entry_created
            step_context["trade_lock_wait_ms"] = trade_lock_wait_ms
            step_context["trade_lock_hold_ms"] = trade_lock_hold_ms
            step_context["candle_update_ms"] = candle_update_ms
            step_context["overlays_update_ms"] = overlays_update_ms
            step_context["pending_signals_ops_ms"] = pending_signals_ops_ms
            step_context["execution_ms"] = execution_ms
            step_context["stats_update_ms"] = stats_update_ms
            step_context["persistence_ms"] = persistence_ms
            step_context["step_trace_enqueue_ms"] = persistence_ms
            step_context["db_commit_ms"] = db_commit_ms
            step_context["delta_build_ms"] = delta_build_ms
            step_context["delta_serialize_ms"] = delta_serialize_ms
            step_context["stream_emit_ms"] = stream_emit_ms
            step_context["indicators_count"] = indicators_count
            step_context["overlays_changed_count"] = overlays_changed_count
            step_context["overlay_points_changed"] = overlay_points_changed
            step_context["signals_emitted_count"] = signals_emitted_count
            step_context["series_overlay_entries_ms"] = series_overlay_entries_ms
            step_context["series_overlay_indicator_entries_ms"] = series_overlay_indicator_entries_ms
            step_context["series_overlay_regime_build_ms"] = series_overlay_regime_build_ms
            step_context["series_overlay_indicator_entries_count"] = series_overlay_indicator_entries_count
            step_context["series_overlay_regime_entries_count"] = series_overlay_regime_entries_count
            step_context["series_overlay_total_entries_count"] = series_overlay_total_entries_count
            step_context["series_overlay_regime_mode_rebuild"] = series_overlay_regime_mode_rebuild
            step_context["trades_touched_count"] = trades_touched_count
            step_context["subscribers_count"] = subscribers_count
            if not state.intrabar_active():
                step_context["step_trace_queue_depth"] = finalize_metrics.get("step_trace_queue_depth")
                step_context["step_trace_dropped_count"] = finalize_metrics.get("step_trace_dropped_count")
                step_context["step_trace_persist_lag_ms"] = finalize_metrics.get("step_trace_persist_lag_ms")
                step_context["step_trace_persist_batch_ms"] = finalize_metrics.get("step_trace_persist_batch_ms")
                step_context["step_trace_persist_error_count"] = finalize_metrics.get("step_trace_persist_error_count")
            step_context["execution_decision_flow_ms"] = decision_flow_ms
            step_context["execution_prime_ms"] = execution_prime_ms
            step_context["execution_settlement_ms"] = settlement_ms
            step_context["execution_trade_event_processing_ms"] = event_processing_ms
            self._record_step_trace(
                "step_series_state",
                started_at=step_started,
                ended_at=datetime.now(timezone.utc),
                ok=True,
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                context=step_context,
            )
        except Exception as exc:
            step_context["decision_events_logged"] = decision_events_logged
            step_context["execution_events_logged"] = execution_events_logged
            step_context["trade_events_processed"] = trade_events_processed
            step_context["entry_created"] = entry_created
            step_context["trade_lock_wait_ms"] = trade_lock_wait_ms
            step_context["trade_lock_hold_ms"] = trade_lock_hold_ms
            step_context["candle_update_ms"] = candle_update_ms
            step_context["overlays_update_ms"] = overlays_update_ms
            step_context["pending_signals_ops_ms"] = pending_signals_ops_ms
            step_context["execution_ms"] = execution_ms
            step_context["stats_update_ms"] = stats_update_ms
            step_context["persistence_ms"] = persistence_ms
            step_context["step_trace_enqueue_ms"] = persistence_ms
            step_context["db_commit_ms"] = db_commit_ms
            step_context["delta_build_ms"] = delta_build_ms
            step_context["delta_serialize_ms"] = delta_serialize_ms
            step_context["stream_emit_ms"] = stream_emit_ms
            step_context["indicators_count"] = indicators_count
            step_context["overlays_changed_count"] = overlays_changed_count
            step_context["overlay_points_changed"] = overlay_points_changed
            step_context["signals_emitted_count"] = signals_emitted_count
            step_context["series_overlay_entries_ms"] = series_overlay_entries_ms
            step_context["series_overlay_indicator_entries_ms"] = series_overlay_indicator_entries_ms
            step_context["series_overlay_regime_build_ms"] = series_overlay_regime_build_ms
            step_context["series_overlay_indicator_entries_count"] = series_overlay_indicator_entries_count
            step_context["series_overlay_regime_entries_count"] = series_overlay_regime_entries_count
            step_context["series_overlay_total_entries_count"] = series_overlay_total_entries_count
            step_context["series_overlay_regime_mode_rebuild"] = series_overlay_regime_mode_rebuild
            step_context["trades_touched_count"] = trades_touched_count
            step_context["subscribers_count"] = subscribers_count
            step_context.update(self._step_trace_metrics())
            self._record_step_trace(
                "step_series_state",
                started_at=step_started,
                ended_at=datetime.now(timezone.utc),
                ok=False,
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                error=str(exc),
                context=step_context,
            )
            raise
    
    def _next_signal_for(
        self,
        state: SeriesExecutionState,
        series: StrategySeries,
        candle: Candle,
        epoch: int,
    ) -> Tuple[List[Dict[str, object]], Optional[str], int, Dict[str, Optional[float]], int, int]:
        if epoch <= state.last_evaluated_epoch:
            consume_started = time.perf_counter()
            consumed, chosen, updated_last = consume_signals(
                state.pending_signals,
                epoch=epoch,
                last_consumed_epoch=state.last_consumed_epoch,
            )
            pending_consume_ms = max((time.perf_counter() - consume_started) * 1000.0, 0.0)
            return (
                consumed,
                chosen,
                len(state.pending_signals),
                {
                    "epochs_evaluated_this_tick": 0.0,
                    "pending_signals_append_ms": 0.0,
                    "pending_signals_consume_ms": pending_consume_ms,
                    "pending_signals_ops_ms": pending_consume_ms,
                    "signals_emitted_count": 0.0,
                    "overlays_update_ms": 0.0,
                    "indicator_state_update_ms": 0.0,
                    "signal_eval_ms": 0.0,
                    "overlay_projection_ms": 0.0,
                    "overlay_projection_skipped_count": 0.0,
                    "overlay_projection_projector_ms": 0.0,
                    "overlay_projection_delta_ms": 0.0,
                    "overlay_projection_normalize_ms": 0.0,
                    "overlay_projection_fingerprint_ms": 0.0,
                    "overlay_projection_entries_total": 0.0,
                    "overlay_projection_entries_changed": 0.0,
                    "overlay_projection_ops_count": 0.0,
                    "overlay_projection_normalize_cache_hits": 0.0,
                    "overlay_projection_normalize_cache_misses": 0.0,
                    "series_overlay_entries_ms": 0.0,
                    "series_overlay_indicator_entries_ms": 0.0,
                    "series_overlay_regime_build_ms": 0.0,
                    "series_overlay_indicator_entries_count": 0.0,
                    "series_overlay_regime_entries_count": 0.0,
                    "series_overlay_total_entries_count": 0.0,
                    "series_overlay_regime_mode_rebuild": 0.0,
                    "state_revisions_changed_count": 0.0,
                    "indicators_count": 0.0,
                    "overlays_changed_count": 0.0,
                    "overlay_points_changed": 0.0,
                },
                state.last_evaluated_epoch,
                updated_last,
            )

        indicator_started = time.perf_counter()
        if state.indicator_engine is None:
            raise RuntimeError("indicator_runtime_missing: series indicator engine is not initialized")
        frame = state.indicator_engine.step(bar=candle, bar_time=candle.time)
        outputs = frame.outputs
        state.indicator_outputs = outputs
        state.indicator_overlays = frame.overlays
        indicator_state_update_ms = max((time.perf_counter() - indicator_started) * 1000.0, 0.0)

        signal_started = time.perf_counter()
        rules = (series.meta or {}).get("rules") or {}
        matched_rules = evaluate_typed_rules(
            rules=rules,
            outputs=outputs,
            output_types=state.indicator_output_types,
            current_epoch=epoch,
        )
        signal_eval_ms = max((time.perf_counter() - signal_started) * 1000.0, 0.0)

        previous_overlay_count = float(len(series.overlays or []))
        previous_overlay_points = float(self._count_overlay_points(series.overlays or []))
        overlay_projection_ms = 0.0
        overlay_projection_skipped_count = 0
        overlay_projection_projector_ms = 0.0
        overlay_projection_delta_ms = 0.0
        overlay_projection_normalize_ms = 0.0
        overlay_projection_fingerprint_ms = 0.0
        overlay_projection_entries_total = 0.0
        overlay_projection_entries_changed = 0.0
        overlay_projection_ops_count = 0.0
        overlay_projection_normalize_cache_hits = 0.0
        overlay_projection_normalize_cache_misses = 0.0
        overlays = self._series_overlay_entries(state)
        overlay_runtime_metrics = state.overlay_runtime_metrics if isinstance(state.overlay_runtime_metrics, Mapping) else {}
        series_overlay_entries_ms = float(overlay_runtime_metrics.get("series_overlay_entries_ms") or 0.0)
        series_overlay_indicator_entries_ms = float(
            overlay_runtime_metrics.get("series_overlay_indicator_entries_ms") or 0.0
        )
        series_overlay_regime_build_ms = float(overlay_runtime_metrics.get("series_overlay_regime_build_ms") or 0.0)
        series_overlay_indicator_entries_count = float(
            overlay_runtime_metrics.get("series_overlay_indicator_entries_count") or 0.0
        )
        series_overlay_regime_entries_count = float(
            overlay_runtime_metrics.get("series_overlay_regime_entries_count") or 0.0
        )
        series_overlay_total_entries_count = float(
            overlay_runtime_metrics.get("series_overlay_total_entries_count") or 0.0
        )
        series_overlay_regime_mode_rebuild = float(
            overlay_runtime_metrics.get("series_overlay_regime_mode_rebuild") or 0.0
        )
        overlays_update_ms = overlay_projection_ms + series_overlay_entries_ms
        overlays_changed_count, overlay_points_changed = self._overlay_change_metrics(series.overlays or [], overlays)
        series.overlays = overlays

        append_started = time.perf_counter()
        for match in matched_rules:
            action = str(match.get("action") or "")
            direction = "long" if action == "buy" else "short"
            state.pending_signals.append(StrategySignal(epoch=int(epoch), direction=direction))
        pending_append_ms = max((time.perf_counter() - append_started) * 1000.0, 0.0)

        consume_started = time.perf_counter()
        consumed, chosen, updated_last = consume_signals(
            state.pending_signals,
            epoch=epoch,
            last_consumed_epoch=state.last_consumed_epoch,
        )
        pending_consume_ms = max((time.perf_counter() - consume_started) * 1000.0, 0.0)
        pending_ops_ms = pending_append_ms + pending_consume_ms

        eval_metrics = {
            "epochs_evaluated_this_tick": 1.0,
            "pending_signals_append_ms": pending_append_ms,
            "pending_signals_consume_ms": pending_consume_ms,
            "pending_signals_ops_ms": pending_ops_ms,
            "signals_emitted_count": float(len(matched_rules)),
            "overlays_update_ms": overlays_update_ms,
            "indicator_state_update_ms": indicator_state_update_ms,
            "signal_eval_ms": signal_eval_ms,
            "overlay_projection_ms": overlay_projection_ms,
            "overlay_projection_skipped_count": float(overlay_projection_skipped_count),
            "overlay_projection_projector_ms": overlay_projection_projector_ms,
            "overlay_projection_delta_ms": overlay_projection_delta_ms,
            "overlay_projection_normalize_ms": overlay_projection_normalize_ms,
            "overlay_projection_fingerprint_ms": overlay_projection_fingerprint_ms,
            "overlay_projection_entries_total": overlay_projection_entries_total,
            "overlay_projection_entries_changed": overlay_projection_entries_changed,
            "overlay_projection_ops_count": overlay_projection_ops_count,
            "overlay_projection_normalize_cache_hits": overlay_projection_normalize_cache_hits,
            "overlay_projection_normalize_cache_misses": overlay_projection_normalize_cache_misses,
            "series_overlay_entries_ms": series_overlay_entries_ms,
            "series_overlay_indicator_entries_ms": series_overlay_indicator_entries_ms,
            "series_overlay_regime_build_ms": series_overlay_regime_build_ms,
            "series_overlay_indicator_entries_count": series_overlay_indicator_entries_count,
            "series_overlay_regime_entries_count": series_overlay_regime_entries_count,
            "series_overlay_total_entries_count": series_overlay_total_entries_count,
            "series_overlay_regime_mode_rebuild": series_overlay_regime_mode_rebuild,
            "state_revisions_changed_count": float(len(outputs)),
            "indicators_count": float(len(state.indicator_engine.order)),
            "overlays_changed_count": overlays_changed_count,
            "overlay_points_changed": overlay_points_changed,
            "overlay_count_before": previous_overlay_count,
            "overlay_count_after": float(len(overlays)),
            "overlay_points_before": previous_overlay_points,
            "overlay_points_after": float(self._count_overlay_points(overlays)),
        }

        next_last_evaluated = epoch
        return consumed, chosen, len(state.pending_signals), eval_metrics, next_last_evaluated, updated_last

    def _record_signal_consumption(
        self,
        state: SeriesExecutionState,
        epoch: int,
        consumed_signals: List[Dict[str, object]],
        chosen_direction: Optional[str],
    ) -> None:
        state.signal_consumptions.append(
            SignalConsumption(
                epoch=epoch,
                consumed_signals=list(consumed_signals),
                chosen_direction=chosen_direction,
            )
        )

    def _compute_playback_interval(self, base_seconds: float = 1.0) -> float:
        return 0.0

    def _has_open_trades(self) -> bool:
        for series in self._series or []:
            if getattr(series, "risk_engine", None) is None:
                continue
            if getattr(series.risk_engine, "active_trade", None) is not None:
                return True
        return False

    def _pace(self, interval: float, update_next_bar: bool = False) -> None:
        if interval <= 0:
            if update_next_bar:
                self._next_bar_at = None
                with self._lock:
                    self.state.update({"next_bar_at": None, "next_bar_in_seconds": None})
            return
        if update_next_bar:
            self._next_bar_at = datetime.now(timezone.utc) + timedelta(seconds=interval)
            with self._lock:
                self.state.update(
                    {
                        "next_bar_at": _isoformat(self._next_bar_at),
                        "next_bar_in_seconds": self._seconds_until_next_bar(),
                    }
                )
        target = time.time() + interval
        while not self._stop.is_set():
            if not self._pause_event.wait(timeout=0.2):
                continue
            remaining = target - time.time()
            if remaining <= 0:
                break
            time.sleep(min(0.25, remaining))

    def _append_live_candles_if_needed(self) -> bool:
        updated = False
        end_iso = _isoformat(datetime.now(timezone.utc))
        with self._series_update_lock:
            for series in self._series:
                last_time = series.candles[-1].time if series.candles else None
                if last_time is None:
                    continue
                start_iso = _isoformat(last_time + timedelta(seconds=1))
                if self._append_series_updates(series, start_iso, end_iso):
                    updated = True
        if updated:
            with self._series_update_lock:
                # Recalculate total bars as max across all series
                self._total_bars = max(len(s.candles) for s in self._series) if self._series else 0
                for state in self._series_states:
                    state.total_bars = len(state.series.candles)
                    if state.done and state.bar_index < state.total_bars:
                        state.done = False
            self._rebuild_overlay_cache()
            self._log_event("live_refresh", message="Appended live candles")
            self._push_update("live_refresh")
        return updated

    def _append_live_candles_for_state(self, state: SeriesExecutionState) -> bool:
        end_iso = _isoformat(datetime.now(timezone.utc))
        series = state.series
        last_time = series.candles[-1].time if series.candles else None
        if last_time is None:
            return False
        start_iso = _isoformat(last_time + timedelta(seconds=1))
        with self._series_update_lock:
            updated = self._append_series_updates(series, start_iso, end_iso)
            if not updated:
                return False
            state.total_bars = len(series.candles)
            if state.done and state.bar_index < state.total_bars:
                state.done = False
            self._total_bars = max(len(s.candles) for s in self._series) if self._series else 0
        self._rebuild_overlay_cache()
        self._log_event("live_refresh", message="Appended live candles")
        self._push_update("live_refresh")
        return True

    def _append_series_updates(self, series: StrategySeries, start_iso: str, end_iso: str) -> bool:
        return self._ensure_series_builder().append_series_updates(series, start_iso, end_iso)

    def pause(self) -> None:
        self._require_prepared("pause")
        if not (self._thread and self._thread.is_alive()):
            raise RuntimeError("Cannot pause runtime before start.")
        self._paused = True
        self._pause_event.clear()
        self._next_bar_at = None
        with self._lock:
            self.state.update({"status": "paused", "paused": True, "next_bar_at": None, "next_bar_in_seconds": None})
        self._log_event("pause", message="Bot paused")
        self._push_update("pause")

    def resume(self) -> None:
        self._require_prepared("resume")
        if not (self._thread and self._thread.is_alive()):
            raise RuntimeError("Cannot resume runtime before start.")
        if str(self.state.get("status") or "").lower() != "paused":
            raise RuntimeError("Cannot resume runtime unless it is paused.")
        self._paused = False
        self._pause_event.set()
        with self._lock:
            if self.state.get("status") == "paused":
                self.state.update({"status": "running", "paused": False})
        self._log_event("resume", message="Bot resumed")
        self._push_update("resume")

    def stop(self) -> None:
        self._stop.set()
        self._pause_event.set()
        if self._runner is not None:
            self._runner.stop()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=0.2)
        with self._lock:
            self.state.update({"status": "stopped", "paused": False})
        self._next_bar_at = None
        self._log_event("stop", message="Bot stopped")
        self._push_update("stop")

    def _aggregate_trades(self) -> List[Dict[str, Any]]:
        trades: List[Dict[str, Any]] = []
        for series in self._series:
            for entry in series.risk_engine.serialise_trades():
                payload = dict(entry)
                payload.setdefault("strategy_id", series.strategy_id)
                payload.setdefault("symbol", series.symbol)
                trades.append(payload)
        return trades

    def _aggregate_stats(self) -> Dict[str, float]:
        summary = {
            "total_trades": 0,
            "completed_trades": 0,
            "legs_closed": 0,
            "wins": 0,
            "losses": 0,
            "breakeven_trades": 0,
            "long_trades": 0,
            "short_trades": 0,
        }
        gross = 0.0
        fees = 0.0
        net = 0.0
        currency: Optional[str] = None
        multi_currency = False
        win_pnls: List[float] = []
        loss_pnls: List[float] = []
        tolerance = 1e-8

        for series in self._series:
            stats = series.risk_engine.stats()
            for key in summary:
                try:
                    summary[key] += int(stats.get(key, 0) or 0)
                except (TypeError, ValueError):
                    continue
            gross += float(stats.get("gross_pnl", 0.0) or 0.0)
            fees += float(stats.get("fees_paid", 0.0) or 0.0)
            net += float(stats.get("net_pnl", 0.0) or 0.0)
            series_currency = stats.get("quote_currency")
            if isinstance(series_currency, str) and series_currency:
                if currency is None:
                    currency = series_currency
                elif currency != series_currency:
                    multi_currency = True

            # Collect individual trade PnLs for avg/largest calculations
            for trade in series.risk_engine.trades:
                if trade.is_active():
                    continue
                pnl = trade.net_pnl
                if pnl > tolerance:
                    win_pnls.append(pnl)
                elif pnl < -tolerance:
                    loss_pnls.append(pnl)

        total = summary.get("completed_trades") or (summary["wins"] + summary["losses"])
        summary["win_rate"] = round(summary["wins"] / total, 4) if total else 0.0
        summary["gross_pnl"] = round(gross, 4)
        summary["fees_paid"] = round(fees, 4)
        summary["net_pnl"] = round(net, 4)
        summary["total_fees"] = round(fees, 4)  # Alias for frontend compatibility

        # Avg win/loss
        summary["avg_win"] = round(sum(win_pnls) / len(win_pnls), 4) if win_pnls else 0.0
        summary["avg_loss"] = round(sum(loss_pnls) / len(loss_pnls), 4) if loss_pnls else 0.0

        # Largest win/loss
        summary["largest_win"] = round(max(win_pnls), 4) if win_pnls else 0.0
        summary["largest_loss"] = round(min(loss_pnls), 4) if loss_pnls else 0.0

        # Max drawdown
        summary["max_drawdown"] = self._max_drawdown_from_trades()

        if multi_currency:
            summary["quote_currency"] = "MULTI"
        elif currency:
            summary["quote_currency"] = currency
        return summary

    def _max_drawdown_from_trades(self) -> float:
        trades = self._aggregate_trades()
        closed = []
        for trade in trades:
            closed_at = trade.get("closed_at")
            net_pnl = trade.get("net_pnl")
            if not closed_at or net_pnl is None:
                continue
            try:
                timestamp = datetime.fromisoformat(str(closed_at))
            except ValueError:
                continue
            closed.append((timestamp, float(net_pnl)))
        if not closed:
            return 0.0
        closed.sort(key=lambda item: item[0])
        equity = 0.0
        peak = 0.0
        max_drawdown = 0.0
        for _, pnl in closed:
            equity += pnl
            if equity > peak:
                peak = equity
            drawdown = peak - equity
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        return round(max_drawdown, 4)
