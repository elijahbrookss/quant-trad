"""Runtime event emission and run artifact lifecycle."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from engines.bot_runtime.core.domain import StrategySignal
from engines.bot_runtime.core.runtime_events import (
    ExitKind,
    ReasonCode,
    RuntimeEvent,
    RuntimeEventCategory,
    RuntimeEventName,
    build_correlation_id,
    coerce_reason_code,
    new_runtime_event,
)
from engines.bot_runtime.core.wallet import project_wallet_from_events
from engines.bot_runtime.core.wallet import wallet_required_reservation
from engines.bot_runtime.core.wallet_gateway import SharedWalletGateway
from engines.bot_runtime.runtime.event_types import runtime_event_type
from utils.log_context import with_log_context

from ..components import RunContext
from ..core import _coerce_float, _isoformat

logger = logging.getLogger(__name__)


class RuntimeEventsMixin:
    def _allocate_runtime_event_seq(self) -> int:
        if self._run_context is None:
            raise ValueError("run context is required for runtime event sequencing")
        shared_wallet_proxy = self.config.get("shared_wallet_proxy")
        seq_override: Optional[int] = None
        if isinstance(shared_wallet_proxy, Mapping):
            seq_counter = shared_wallet_proxy.get("runtime_event_seq")
            proxy_lock = shared_wallet_proxy.get("lock")
            if seq_counter is not None:
                if proxy_lock is not None:
                    proxy_lock.acquire()
                try:
                    if hasattr(seq_counter, "get"):
                        current_value = int(seq_counter.get())
                    elif hasattr(seq_counter, "value"):
                        current_value = int(getattr(seq_counter, "value"))
                    else:
                        raise RuntimeError(
                            "shared runtime_event_seq counter does not expose get() or value; "
                            f"type={type(seq_counter)!r}"
                        )
                    next_value = current_value + 1
                    if hasattr(seq_counter, "set"):
                        seq_counter.set(next_value)
                    elif hasattr(seq_counter, "value"):
                        setattr(seq_counter, "value", next_value)
                    else:
                        raise RuntimeError(
                            "shared runtime_event_seq counter does not expose set() or value; "
                            f"type={type(seq_counter)!r}"
                        )
                    seq_override = int(next_value)
                finally:
                    if proxy_lock is not None:
                        proxy_lock.release()
            elif not bool(getattr(self, "_runtime_event_seq_missing_warned", False)):
                logger.warning(
                    "bot_runtime_event_seq_counter_missing | bot_id=%s | run_id=%s",
                    self.bot_id,
                    self._run_context.run_id,
                )
                setattr(self, "_runtime_event_seq_missing_warned", True)
        with self._lock:
            if seq_override is None:
                self._run_context.runtime_event_seq += 1
                seq = int(self._run_context.runtime_event_seq)
            else:
                seq = int(seq_override)
                self._run_context.runtime_event_seq = max(int(self._run_context.runtime_event_seq), seq)
        return int(seq)

    def _log_event(
        self,
        event: str,
        series: Optional[StrategySeries] = None,
        candle: Optional[Candle] = None,
        **fields: object,
    ) -> None:
        created_at = _isoformat(datetime.now(timezone.utc))
        entry: Dict[str, object] = {
            "id": str(uuid.uuid4()),
            "event": event,
            "timestamp": created_at,
            "created_at": created_at,
        }
        if series is not None:
            entry["strategy_id"] = series.strategy_id
            entry["symbol"] = series.symbol
        if candle is not None:
            entry["bar_time"] = _isoformat(candle.time)
            entry["chart_time"] = entry["bar_time"]
            entry.setdefault("price", round(candle.close, 4))
        for key, value in fields.items():
            if value is not None:
                entry[key] = value
        with self._lock:
            sinks = list(self._event_sinks)
        for sink in sinks:
            sink.record_log(entry)

    def _record_runtime_warning(self, warning: Optional[Mapping[str, object]]) -> None:
        """Capture runtime warnings for UI consumption."""

        if not warning:
            return
        entry: Dict[str, object] = dict(warning)
        entry.setdefault("level", "warning")
        entry.setdefault("type", entry.get("type") or "runtime_warning")
        entry.setdefault("message", entry.get("message") or "Runtime warning")
        entry.setdefault("source", entry.get("source") or "runtime")
        entry.setdefault("bot_id", self.bot_id)
        entry.setdefault("bot_mode", self.run_type)
        entry.setdefault("timestamp", _isoformat(datetime.now(timezone.utc)))
        entry.setdefault("id", str(uuid.uuid4()))
        context = dict(entry.get("context") or {})
        entry["context"] = context
        with self._lock:
            self._warnings.append(entry)
            self.state["warnings"] = list(self._warnings)

    def warnings(self) -> List[Dict[str, object]]:
        """Return the current runtime warnings."""

        with self._lock:
            return list(self._warnings)

    @staticmethod
    def _runtime_event_time(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _bar_correlation_id(self, series: StrategySeries, bar_ts: Optional[datetime]) -> str:
        if self._run_context is None:
            raise ValueError("run context is required before building correlation ids")
        return build_correlation_id(
            run_id=self._run_context.run_id,
            symbol=series.symbol,
            timeframe=series.timeframe,
            bar_ts=bar_ts,
        )

    def _runtime_events_reverse(self) -> List[RuntimeEvent]:
        if self._run_context is None:
            return []
        return list(reversed(self._run_context.runtime_events))

    def _find_signal_event(self, *, series: StrategySeries, correlation_id: str) -> Optional[RuntimeEvent]:
        for event in self._runtime_events_reverse():
            if event.event_name != RuntimeEventName.SIGNAL_EMITTED:
                continue
            if event.correlation_id != correlation_id:
                continue
            if event.strategy_id != series.strategy_id:
                continue
            if event.symbol != series.symbol:
                continue
            if event.timeframe != series.timeframe:
                continue
            return event
        return None

    def _find_decision_event(
        self,
        *,
        series: StrategySeries,
        correlation_id: str,
        trade_id: Optional[str],
    ) -> Optional[RuntimeEvent]:
        for event in self._runtime_events_reverse():
            if event.event_name != RuntimeEventName.DECISION_ACCEPTED:
                continue
            if event.correlation_id != correlation_id:
                continue
            if event.strategy_id != series.strategy_id:
                continue
            if trade_id and str(event.payload.get("trade_id") or "") != str(trade_id):
                continue
            return event
        return None

    def _find_entry_event(self, *, trade_id: Optional[str]) -> Optional[RuntimeEvent]:
        if not trade_id:
            return None
        for event in self._runtime_events_reverse():
            if event.event_name != RuntimeEventName.ENTRY_FILLED:
                continue
            if str(event.payload.get("trade_id") or "") != str(trade_id):
                continue
            return event
        return None

    def _find_trade_decision_event(self, *, trade_id: Optional[str]) -> Optional[RuntimeEvent]:
        if not trade_id:
            return None
        for event in self._runtime_events_reverse():
            if event.event_name != RuntimeEventName.DECISION_ACCEPTED:
                continue
            if str(event.payload.get("trade_id") or "") != str(trade_id):
                continue
            return event
        return None

    def _decision_trace_entry(self, event: RuntimeEvent) -> Dict[str, Any]:
        payload = dict(event.payload or {})
        event_subtype = payload.get("event_subtype")
        if event.event_name == RuntimeEventName.SIGNAL_EMITTED:
            event_subtype = "strategy_signal"
        elif event.event_name == RuntimeEventName.DECISION_ACCEPTED:
            event_subtype = "signal_accepted"
        elif event.event_name == RuntimeEventName.DECISION_REJECTED:
            event_subtype = "signal_rejected"
        elif event.event_name == RuntimeEventName.ENTRY_FILLED:
            event_subtype = "entry"
        elif event.event_name == RuntimeEventName.EXIT_FILLED:
            event_subtype = str(payload.get("exit_kind") or "close").lower()
        elif event.event_name == RuntimeEventName.RUNTIME_ERROR:
            event_subtype = "runtime_error"
        return {
            "event_id": event.event_id,
            "event_ts": _isoformat(event.event_ts),
            "event_type": event.category.value.lower(),
            "event_subtype": event_subtype,
            "reason_code": event.reason_code.value if event.reason_code is not None else None,
            "parent_event_id": event.parent_id,
            "trade_id": payload.get("trade_id"),
            "strategy_id": event.strategy_id,
            "strategy_hash": payload.get("strategy_hash"),
            "symbol": event.symbol,
            "timeframe": event.timeframe,
            "side": payload.get("direction") or payload.get("side"),
            "decision_id": payload.get("decision_id"),
            "rule_id": payload.get("rule_id"),
            "intent": payload.get("intent"),
            "event_key": payload.get("event_key"),
            "qty": payload.get("qty"),
            "price": payload.get("price"),
            "event_impact_pnl": payload.get("event_impact_pnl"),
            "trade_net_pnl": payload.get("trade_net_pnl"),
            "reason_detail": payload.get("message"),
            "rejection_stage": (
                payload.get("rejection_artifact", {}).get("rejection_stage")
                if isinstance(payload.get("rejection_artifact"), Mapping)
                else None
            ),
            "context": (
                payload.get("rejection_artifact", {}).get("context")
                if isinstance(payload.get("rejection_artifact"), Mapping)
                else None
            ),
        }

    def _persist_runtime_event(self, event: RuntimeEvent) -> None:
        if self._run_context is None:
            raise ValueError("run context is required for runtime event persistence")
        serialized = event.serialize()
        shared_wallet_proxy = self.config.get("shared_wallet_proxy")
        seq = self._allocate_runtime_event_seq()
        with self._lock:
            serialized["seq"] = int(seq)
            self._run_context.runtime_events.append(event)
            self._run_context.runtime_event_stream.append(serialized)
            trace_entry = self._decision_trace_entry(event)
            self._run_context.decision_trace.append(trace_entry)
            sinks = list(self._event_sinks)
        if self._report_artifact_bundle is not None:
            self._report_artifact_bundle.record_runtime_event(
                serialized=serialized,
                decision_entry=trace_entry,
            )
        for sink in sinks:
            sink.record_decision(serialized)
        if (
            isinstance(shared_wallet_proxy, Mapping)
            and event.event_name != RuntimeEventName.WALLET_INITIALIZED
            and shared_wallet_proxy.get("runtime_events") is not None
        ):
            runtime_events_proxy = shared_wallet_proxy.get("runtime_events")
            proxy_lock = shared_wallet_proxy.get("lock")
            if proxy_lock is not None:
                proxy_lock.acquire()
                try:
                    runtime_events_proxy.append(serialized)
                finally:
                    proxy_lock.release()
            else:
                runtime_events_proxy.append(serialized)
        self._deps.record_bot_runtime_event(
            {
                "event_id": event.event_id,
                "bot_id": self.bot_id,
                "run_id": self._run_context.run_id,
                "seq": seq,
                "event_type": runtime_event_type(event.event_name),
                "critical": event.event_name
                in {
                    RuntimeEventName.DECISION_REJECTED,
                    RuntimeEventName.ENTRY_FILLED,
                    RuntimeEventName.EXIT_FILLED,
                    RuntimeEventName.RUNTIME_ERROR,
                    RuntimeEventName.SYMBOL_DEGRADED,
                },
                "schema_version": event.schema_version,
                "event_time": serialized.get("event_ts"),
                "payload": serialized,
            }
        )

    def _emit_runtime_event(
        self,
        *,
        event_name: RuntimeEventName,
        series: Optional[StrategySeries],
        bar_ts: Optional[datetime],
        payload: Mapping[str, Any],
        reason_code: Optional[ReasonCode | str] = None,
        root_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        category: Optional[RuntimeEventCategory] = None,
        event_ts: Optional[datetime] = None,
        missing_parent_hint: Optional[str] = None,
    ) -> RuntimeEvent:
        if self._run_context is None:
            raise ValueError("run context is required before emitting runtime events")
        strategy_id = series.strategy_id if series is not None else "__runtime__"
        symbol = series.symbol if series is not None else None
        timeframe = series.timeframe if series is not None else None
        if series is not None:
            correlation_id = self._bar_correlation_id(series, bar_ts)
        else:
            correlation_id = build_correlation_id(
                run_id=self._run_context.run_id,
                symbol=None,
                timeframe=None,
                bar_ts=bar_ts,
            )
        payload_data = dict(payload or {})
        try:
            event = new_runtime_event(
                run_id=self._run_context.run_id,
                bot_id=self.bot_id,
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                bar_ts=bar_ts,
                event_name=event_name,
                category=category,
                correlation_id=correlation_id,
                reason_code=reason_code,
                root_id=root_id,
                parent_id=parent_id,
                event_ts=event_ts,
                payload=payload_data,
            )
        except ValueError as exc:
            message = str(exc)
            parent_validation_error = "parent_id is required" in message or "root_id is required" in message
            if not parent_validation_error:
                raise
            fallback_payload = dict(payload_data)
            fallback_payload["parent_missing"] = True
            fallback_payload["missing_parent_hint"] = missing_parent_hint or message
            fallback_reason = coerce_reason_code(reason_code)
            if event_name not in {
                RuntimeEventName.RUNTIME_ERROR,
                RuntimeEventName.SYMBOL_DEGRADED,
                RuntimeEventName.SYMBOL_RECOVERED,
            }:
                fallback_reason = ReasonCode.RUNTIME_PARENT_MISSING
            event = new_runtime_event(
                run_id=self._run_context.run_id,
                bot_id=self.bot_id,
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                bar_ts=bar_ts,
                event_name=event_name,
                category=category,
                correlation_id=correlation_id,
                reason_code=fallback_reason,
                root_id=root_id,
                parent_id=parent_id,
                event_ts=event_ts,
                payload=fallback_payload,
                allow_missing_parent=True,
            )
        self._persist_runtime_event(event)
        return event

    def _emit_wallet_initialized_event(self, balances: Mapping[str, Any]) -> RuntimeEvent:
        normalized = {
            str(currency).upper(): float(amount)
            for currency, amount in dict(balances or {}).items()
        }
        return self._emit_runtime_event(
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            series=None,
            bar_ts=None,
            payload={"balances": normalized, "source": "run_start"},
            category=RuntimeEventCategory.WALLET,
        )

    def _emit_runtime_error_event(
        self,
        *,
        location: str,
        error: Exception,
        series: Optional[StrategySeries] = None,
        bar_ts: Optional[datetime] = None,
    ) -> RuntimeEvent:
        return self._emit_runtime_event(
            event_name=RuntimeEventName.RUNTIME_ERROR,
            series=series,
            bar_ts=bar_ts,
            reason_code=ReasonCode.RUNTIME_EXCEPTION,
            payload={
                "exception_type": error.__class__.__name__,
                "message": str(error),
                "location": location,
            },
            category=RuntimeEventCategory.RUNTIME,
        )

    def _emit_signal_event(
        self,
        *,
        series: StrategySeries,
        candle: Candle,
        signal: StrategySignal,
        decision_artifact: Optional[Mapping[str, Any]] = None,
    ) -> RuntimeEvent:
        return self._emit_runtime_event(
            event_name=RuntimeEventName.SIGNAL_EMITTED,
            series=series,
            bar_ts=candle.time,
            reason_code=ReasonCode.SIGNAL_STRATEGY_SIGNAL,
            payload={
                "signal_id": signal.signal_id,
                "source_type": signal.source_type,
                "source_id": signal.source_id,
                "signal_type": "strategy_signal",
                "direction": signal.direction,
                "signal_price": float(candle.close),
                "strategy_hash": signal.strategy_hash,
                "decision_id": signal.decision_id,
                "rule_id": signal.rule_id,
                "intent": signal.intent,
                "event_key": signal.event_key,
                "decision_artifact": dict(decision_artifact or {}),
                "bar": {
                    "time": _isoformat(candle.time),
                    "open": float(candle.open),
                    "high": float(candle.high),
                    "low": float(candle.low),
                    "close": float(candle.close),
                },
            },
            event_ts=candle.time,
        )

    def _emit_decision_event(
        self,
        *,
        series: StrategySeries,
        candle: Candle,
        signal: StrategySignal,
        decision: str,
        decision_artifact: Optional[Mapping[str, Any]],
        rejection_artifact: Optional[Mapping[str, Any]],
        signal_price: float,
        reason_code: str,
        message: Optional[str],
        trade_id: Optional[str],
    ) -> RuntimeEvent:
        correlation_id = self._bar_correlation_id(series, candle.time)
        signal_event = self._find_signal_event(series=series, correlation_id=correlation_id)
        missing_parent_hint: Optional[str] = None
        resolved_root_id = signal_event.root_id if signal_event is not None else None
        resolved_parent_id = signal_event.event_id if signal_event is not None else None
        if signal_event is None:
            missing_parent_hint = (
                "signal event missing for decision emission | "
                f"strategy={series.strategy_id} symbol={series.symbol} correlation={correlation_id}"
            )
        event_name = RuntimeEventName.DECISION_ACCEPTED if decision == "accepted" else RuntimeEventName.DECISION_REJECTED
        payload: Dict[str, Any] = {
            "signal_id": signal.signal_id,
            "source_type": signal.source_type,
            "source_id": signal.source_id,
            "decision": decision,
            "direction": signal.direction,
            "signal_price": float(signal_price),
            "trade_id": trade_id,
            "strategy_hash": signal.strategy_hash,
            "decision_id": signal.decision_id,
            "rule_id": signal.rule_id,
            "intent": signal.intent,
            "event_key": signal.event_key,
            "event_subtype": "signal_accepted" if decision == "accepted" else "signal_rejected",
        }
        if isinstance(rejection_artifact, Mapping):
            payload["rejection_artifact"] = dict(rejection_artifact)
        if decision != "accepted":
            payload["message"] = message or "Decision rejected"
        return self._emit_runtime_event(
            event_name=event_name,
            series=series,
            bar_ts=candle.time,
            reason_code=coerce_reason_code(reason_code),
            root_id=resolved_root_id,
            parent_id=resolved_parent_id,
            payload=payload,
            event_ts=candle.time,
            missing_parent_hint=missing_parent_hint,
        )

    def _emit_entry_filled_event(
        self,
        *,
        series: StrategySeries,
        candle: Candle,
        trade: Any,
        direction: str,
    ) -> RuntimeEvent:
        trade_id = str(getattr(trade, "trade_id", "") or "")
        correlation_id = self._bar_correlation_id(series, candle.time)
        signal_event = self._find_signal_event(series=series, correlation_id=correlation_id)
        decision_event = self._find_decision_event(series=series, correlation_id=correlation_id, trade_id=trade_id)
        missing_bits: List[str] = []
        if signal_event is None:
            missing_bits.append(f"signal event missing | trade_id={trade_id} correlation={correlation_id}")
        if decision_event is None:
            missing_bits.append(f"decision event missing | trade_id={trade_id} correlation={correlation_id}")
        missing_parent_hint = "; ".join(missing_bits) if missing_bits else None
        qty = sum(max(getattr(leg, "contracts", 0.0), 0.0) for leg in getattr(trade, "legs", []))
        price = float(getattr(trade, "entry_price", candle.close) or candle.close)
        fee_paid = float(getattr(trade, "fees_paid", 0.0) or 0.0)
        notional = abs(price * float(getattr(series.risk_engine, "contract_size", 1.0) or 1.0) * float(qty))
        accounting_mode = None
        if getattr(series, "execution_profile", None) is not None:
            accounting_mode = series.execution_profile.accounting_mode
        collateral_reserved = 0.0
        if accounting_mode == "margin":
            _currency, collateral_reserved = wallet_required_reservation(
                side="buy" if direction == "long" else "sell",
                base_currency=str(getattr(trade, "base_currency", "") or ""),
                quote_currency=str(getattr(trade, "quote_currency", "") or ""),
                qty=float(qty),
                notional=float(notional),
                fee=float(fee_paid),
                short_requires_borrow=bool(getattr(series.risk_engine, "short_requires_borrow", False)),
                instrument=series.instrument if isinstance(series.instrument, Mapping) else None,
                execution_profile=getattr(series, "execution_profile", None),
            )
        wallet_fill_metadata = (
            dict(getattr(trade, "wallet_fill_metadata", {}) or {})
            if isinstance(getattr(trade, "wallet_fill_metadata", None), Mapping)
            else {}
        )
        wallet_delta_payload: Dict[str, Any] = {
            "collateral_reserved": float(collateral_reserved),
            "collateral_released": 0.0,
            "fee_paid": float(fee_paid),
            "balance_delta": float(-fee_paid) if accounting_mode == "margin" else None,
        }
        gateway_wallet_delta = wallet_fill_metadata.get("wallet_delta")
        if isinstance(gateway_wallet_delta, Mapping):
            wallet_delta_payload["collateral_reserved"] = max(
                float(_coerce_float(gateway_wallet_delta.get("collateral_reserved"), float(collateral_reserved)) or 0.0),
                0.0,
            )
            wallet_delta_payload["collateral_released"] = max(
                float(_coerce_float(gateway_wallet_delta.get("collateral_released"), 0.0) or 0.0),
                0.0,
            )
            wallet_delta_payload["fee_paid"] = max(
                float(_coerce_float(gateway_wallet_delta.get("fee_paid"), float(fee_paid)) or 0.0),
                0.0,
            )
            if gateway_wallet_delta.get("balance_delta") is not None:
                wallet_delta_payload["balance_delta"] = float(
                    _coerce_float(gateway_wallet_delta.get("balance_delta"), 0.0) or 0.0
                )
        payload = {
            "trade_id": trade_id,
            "side": "buy" if direction == "long" else "sell",
            "direction": direction,
            "qty": float(qty),
            "price": price,
            "notional": float(notional),
            "fee_paid": float(fee_paid),
            "base_currency": str(getattr(trade, "base_currency", "") or ""),
            "quote_currency": str(getattr(trade, "quote_currency", "") or ""),
            "accounting_mode": accounting_mode,
            "wallet_delta": wallet_delta_payload,
            "event_subtype": "entry",
        }
        reservation_id = wallet_fill_metadata.get("reservation_id")
        reservation_correlation_id = wallet_fill_metadata.get("correlation_id")
        required_delta = wallet_fill_metadata.get("required_delta")
        payload["reservation_id"] = str(reservation_id) if reservation_id else None
        payload["correlation_id"] = str(reservation_correlation_id or f"trade:{trade_id}")
        if isinstance(required_delta, Mapping):
            payload["required_delta"] = dict(required_delta)
        return self._emit_runtime_event(
            event_name=RuntimeEventName.ENTRY_FILLED,
            series=series,
            bar_ts=candle.time,
            reason_code=ReasonCode.EXEC_ENTRY_FILLED,
            root_id=(
                signal_event.root_id
                if signal_event is not None
                else (decision_event.root_id if decision_event is not None else None)
            ),
            parent_id=decision_event.event_id if decision_event is not None else None,
            payload=payload,
            event_ts=trade.entry_time if hasattr(trade, "entry_time") else candle.time,
            missing_parent_hint=missing_parent_hint,
        )

    def _emit_exit_filled_event(
        self,
        *,
        series: StrategySeries,
        candle: Candle,
        event: Mapping[str, Any],
    ) -> RuntimeEvent:
        trade_id = str(event.get("trade_id") or "")
        entry_event = self._find_entry_event(trade_id=trade_id)
        decision_event = self._find_trade_decision_event(trade_id=trade_id)
        missing_parent_hint = None
        if entry_event is None:
            missing_parent_hint = f"entry event missing for exit fill | trade_id={trade_id}"
        subtype = str(event.get("type") or "").lower()
        exit_kind = ExitKind.CLOSE
        reason = ReasonCode.EXEC_EXIT_CLOSE
        if subtype == "target":
            exit_kind = ExitKind.TARGET
            reason = ReasonCode.EXEC_EXIT_TARGET
        elif subtype == "stop":
            exit_kind = ExitKind.STOP
            reason = ReasonCode.EXEC_EXIT_STOP
        qty = float(event.get("contracts") or 0.0)
        price = float(event.get("price") or candle.close)
        notional = abs(price * float(getattr(series.risk_engine, "contract_size", 1.0) or 1.0) * qty)
        fee_paid = 0.0
        if subtype == "close":
            fee_paid = float(event.get("fees_paid") or 0.0)
        accounting_mode = None
        if getattr(series, "execution_profile", None) is not None:
            accounting_mode = series.execution_profile.accounting_mode
        realized_pnl = float(event.get("pnl") or 0.0)
        if subtype == "close":
            realized_pnl = float(event.get("net_pnl") or 0.0) + fee_paid
        prior_state = project_wallet_from_events(self._run_context.runtime_events if self._run_context else [])
        collateral_released = 0.0
        if accounting_mode == "margin":
            pos = prior_state.margin_positions.get(trade_id) if prior_state.margin_positions else None
            if isinstance(pos, Mapping):
                open_qty = float(pos.get("open_qty") or 0.0)
                locked = float(pos.get("locked_margin") or 0.0)
                if open_qty > 0 and locked > 0 and qty > 0:
                    collateral_released = min(locked * min(qty / open_qty, 1.0), locked)
        wallet_fill_metadata = dict(event.get("wallet_fill_metadata") or {}) if isinstance(event, Mapping) else {}
        wallet_delta_payload: Dict[str, Any] = {
            "collateral_reserved": 0.0,
            "collateral_released": float(collateral_released),
            "fee_paid": float(fee_paid),
            "balance_delta": float(realized_pnl - fee_paid) if accounting_mode == "margin" else None,
        }
        gateway_wallet_delta = wallet_fill_metadata.get("wallet_delta")
        if isinstance(gateway_wallet_delta, Mapping):
            wallet_delta_payload["collateral_reserved"] = max(
                float(_coerce_float(gateway_wallet_delta.get("collateral_reserved"), 0.0) or 0.0),
                0.0,
            )
            wallet_delta_payload["collateral_released"] = max(
                float(_coerce_float(gateway_wallet_delta.get("collateral_released"), float(collateral_released)) or 0.0),
                0.0,
            )
            wallet_delta_payload["fee_paid"] = max(
                float(_coerce_float(gateway_wallet_delta.get("fee_paid"), float(fee_paid)) or 0.0),
                0.0,
            )
            if gateway_wallet_delta.get("balance_delta") is not None:
                wallet_delta_payload["balance_delta"] = float(
                    _coerce_float(gateway_wallet_delta.get("balance_delta"), 0.0) or 0.0
                )
        payload = {
            "trade_id": trade_id,
            "side": "sell" if str(event.get("direction") or "").lower() == "long" else "buy",
            "direction": str(event.get("direction") or ""),
            "qty": qty,
            "price": price,
            "notional": float(notional),
            "fee_paid": float(fee_paid),
            "realized_pnl": float(realized_pnl),
            "base_currency": str(getattr(series.risk_engine, "base_currency", "") or ""),
            "quote_currency": str(event.get("currency") or getattr(series.risk_engine, "quote_currency", "") or ""),
            "accounting_mode": accounting_mode,
            "exit_kind": exit_kind.value,
            "event_impact_pnl": float(event.get("pnl") or 0.0) if subtype in {"target", "stop"} else None,
            "trade_net_pnl": float(event.get("net_pnl") or 0.0) if subtype == "close" else None,
            "wallet_delta": wallet_delta_payload,
            "event_subtype": subtype,
        }
        reservation_id = wallet_fill_metadata.get("reservation_id")
        reservation_correlation_id = wallet_fill_metadata.get("correlation_id")
        required_delta = wallet_fill_metadata.get("required_delta")
        payload["reservation_id"] = str(reservation_id) if reservation_id else None
        payload["correlation_id"] = str(reservation_correlation_id or f"trade:{trade_id}")
        if isinstance(required_delta, Mapping):
            payload["required_delta"] = dict(required_delta)
        event_ts = self._runtime_event_time(event.get("time")) or candle.time
        return self._emit_runtime_event(
            event_name=RuntimeEventName.EXIT_FILLED,
            series=series,
            bar_ts=candle.time,
            reason_code=reason,
            root_id=(
                entry_event.root_id
                if entry_event is not None
                else (decision_event.root_id if decision_event is not None else None)
            ),
            parent_id=(
                entry_event.event_id
                if entry_event is not None
                else (decision_event.event_id if decision_event is not None else None)
            ),
            payload=payload,
            event_ts=event_ts,
            missing_parent_hint=missing_parent_hint,
        )

    @staticmethod
    def _normalise_rejection_metadata(
        rejection_meta: Optional[Mapping[str, Any]],
        blocking_trade_id: Optional[str],
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        resolved_trade_id = blocking_trade_id
        metadata_payload: Dict[str, Any] = {}
        if not isinstance(rejection_meta, Mapping):
            return resolved_trade_id, metadata_payload
        metadata_payload = {
            k: v
            for k, v in rejection_meta.items()
            if k not in {"reason", "trade_id"}
        }
        if resolved_trade_id is None:
            meta_trade_id = rejection_meta.get("trade_id")
            if meta_trade_id is not None:
                resolved_trade_id = str(meta_trade_id)
        return resolved_trade_id, metadata_payload

    def _build_run_context(self) -> RunContext:
        wallet_config = self.config.get("wallet_config")
        if not isinstance(wallet_config, dict):
            raise ValueError("wallet_config is required to start a bot run")
        balances = wallet_config.get("balances")
        if not isinstance(balances, dict) or not balances:
            raise ValueError("wallet_config.balances is required to start a bot run")
        shared_wallet_proxy = self.config.get("shared_wallet_proxy")
        logger.info(with_log_context("bot_runtime_run_context_wallet_init", self._runtime_log_context()))
        configured_run_id = str(self.config.get("run_id") or "").strip()
        run_context = RunContext(
            bot_id=self.bot_id,
            run_id=configured_run_id if configured_run_id else str(uuid.uuid4()),
        )
        if not isinstance(shared_wallet_proxy, Mapping):
            raise ValueError("shared_wallet_proxy is required for bot runtime")
        run_context.wallet_gateway = SharedWalletGateway(shared_wallet_proxy)
        logger.info(with_log_context("bot_runtime_run_context_wallet_shared", self._runtime_log_context()))
        logger.info(
            with_log_context(
                "bot_runtime_run_context_attach_wallet_start",
                self._runtime_log_context(series=len(self._series)),
            )
        )
        for series in self._series:
            series.risk_engine.set_runtime_context(
                bot_id=self.bot_id,
                bot_mode=self.run_type,
                run_id=run_context.run_id,
            )
            if run_context.wallet_gateway is not None:
                series.risk_engine.attach_wallet_gateway(run_context.wallet_gateway)
        logger.info(with_log_context("bot_runtime_run_context_attach_wallet_done", self._runtime_log_context()))
        return run_context

    def _persist_run_artifact(self, status: str) -> None:
        if self._run_context is None:
            return

        artifact = self._run_artifact_payload(status)
        self._deps.update_bot_run_artifact(self.bot_id, artifact)
        if self._report_artifact_bundle is not None:
            self._report_artifact_bundle.finalize(runtime_status=status, artifact=artifact)

    def _run_artifact_payload(self, status: str) -> Dict[str, Any]:
        if self._run_context is None:
            raise ValueError("Run context is required to build artifact payload")
        self._run_context.status = status
        self._run_context.ended_at = _isoformat(datetime.now(timezone.utc))
        runtime_events = list(self._run_context.runtime_events)
        runtime_event_stream = list(self._run_context.runtime_event_stream)
        wallet_state = project_wallet_from_events(runtime_events)
        wallet_ledger_view: List[Dict[str, Any]] = []
        for event in runtime_event_stream:
            name = str(event.get("event_name") or "")
            if name not in {
                RuntimeEventName.WALLET_INITIALIZED.value,
                RuntimeEventName.WALLET_DEPOSITED.value,
                RuntimeEventName.ENTRY_FILLED.value,
                RuntimeEventName.EXIT_FILLED.value,
            }:
                continue
            wallet_ledger_view.append(
                {
                    "event_id": event.get("event_id"),
                    "event_name": name,
                    "event_ts": event.get("event_ts"),
                    "payload": dict(event.get("payload") or {}),
                }
            )
        return {
            "run_id": self._run_context.run_id,
            "bot_id": self.bot_id,
            "started_at": self._run_context.started_at,
            "ended_at": self._run_context.ended_at,
            "status": status,
            "wallet_start": dict(self.config.get("wallet_config") or {}),
            "runtime_event_stream": runtime_event_stream,
            "wallet_end": {
                "balances": wallet_state.balances,
                "locked_margin": getattr(wallet_state, "locked_margin", {}) or {},
                "free_collateral": getattr(wallet_state, "free_collateral", {}) or {},
            },
            "wallet_state": {
                "balances": wallet_state.balances,
                "locked_margin": getattr(wallet_state, "locked_margin", {}) or {},
                "free_collateral": getattr(wallet_state, "free_collateral", {}) or {},
                "margin_positions": getattr(wallet_state, "margin_positions", {}) or {},
            },
            "wallet_ledger": wallet_ledger_view,
            "decision_trace": list(self._run_context.decision_trace),
            "decision_artifacts": list(self._run_context.decision_artifacts),
            "rejection_artifacts": list(self._run_context.rejection_artifacts),
        }
