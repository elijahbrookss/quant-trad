"""Runtime trade persistence, telemetry events, and step tracing."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from engines.bot_runtime.core.domain import Candle
from utils.log_context import with_log_context

from ..core import _isoformat, _timeframe_to_seconds

logger = logging.getLogger(__name__)


class RuntimePersistenceMixin:
    @staticmethod
    def _event_timestamp(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        target = value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return _isoformat(target)

    def _persist_trade_entry(
        self,
        series: StrategySeries,
        trade: LadderPosition,
    ) -> None:
        if not series or not trade:
            return
        run_id = self._run_context.run_id if self._run_context else None
        contracts = sum(max(leg.contracts, 0) for leg in trade.legs)
        timeframe_label = series.timeframe
        timeframe_seconds = _timeframe_to_seconds(timeframe_label)
        instrument_id = (series.instrument or {}).get("id") if isinstance(series.instrument, dict) else None
        metrics = dict(trade._metrics_snapshot())
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
        if not self._state_callback:
            return
        payload = {
            "status": status,
            "last_stats": dict(self._last_stats or {}),
            "last_run_at": _isoformat(datetime.now(timezone.utc)),
        }
        try:
            self._state_callback(payload)
        except Exception as exc:  # pragma: no cover
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
        except Exception as exc:  # pragma: no cover
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

    def _flush_step_trace_buffer(self, reason: str, *, shutdown: bool = False) -> None:
        try:
            self._step_trace_buffer.flush(reason=reason, shutdown=shutdown, timeout_s=5.0)
        except Exception as exc:  # pragma: no cover
            context = self._runtime_log_context(reason=reason, shutdown=shutdown, error=str(exc))
            logger.warning(with_log_context("bot_runtime_step_trace_flush_failed", context))

    def _step_trace_metrics(self) -> Dict[str, float]:
        try:
            metrics = self._step_trace_buffer.metrics_snapshot()
            return {
                "step_trace_queue_depth": float(metrics.get("queue_depth") or 0.0),
                "step_trace_dropped_count": float(metrics.get("dropped_count") or 0.0),
                "step_trace_persist_lag_ms": float(metrics.get("persist_lag_ms") or 0.0),
                "step_trace_persist_batch_ms": float(metrics.get("persist_batch_ms") or 0.0),
                "step_trace_persist_error_count": float(metrics.get("persist_error_count") or 0.0),
            }
        except Exception:
            return {
                "step_trace_queue_depth": 0.0,
                "step_trace_dropped_count": 0.0,
                "step_trace_persist_lag_ms": 0.0,
                "step_trace_persist_batch_ms": 0.0,
                "step_trace_persist_error_count": 0.0,
            }

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
            payload_context = dict(context or {})
            payload_context.update(self._step_trace_metrics())
            enqueue_ms = self._step_trace_buffer.record(
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
                    "context": payload_context,
                }
            )
            return enqueue_ms
        except Exception as exc:  # pragma: no cover
            step_context = self._runtime_log_context(step=step_name, error=str(exc))
            logger.warning(with_log_context("bot_runtime_step_trace_persist_failed", step_context))
            return None
