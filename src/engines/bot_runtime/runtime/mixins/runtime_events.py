"""Runtime event emission and run artifact lifecycle."""

from __future__ import annotations

from collections import deque
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from engines.bot_runtime.core.domain import StrategySignal
from engines.bot_runtime.core.runtime_events import (
    DecisionAcceptedContext,
    DecisionRejectedContext,
    EntryFilledContext,
    ExitFilledContext,
    ExitKind,
    ReasonCode,
    RuntimeBar,
    RuntimeErrorContext,
    RuntimeEvent,
    RuntimeEventName,
    RuntimeStatusContext,
    SignalEmittedContext,
    WalletDelta,
    WalletInitializedContext,
    build_correlation_id,
    coerce_reason_code,
    decision_trace_entry_from_runtime_event,
    new_runtime_event,
)
from engines.bot_runtime.core.wallet import project_wallet_from_events
from engines.bot_runtime.core.wallet import wallet_required_reservation
from engines.bot_runtime.core.wallet_gateway import SharedWalletGateway
from utils.log_context import with_log_context

from ..components import RunContext
from ..core import _coerce_float, _isoformat

logger = logging.getLogger(__name__)


def _parse_runtime_iso(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class RuntimeEventsMixin:
    def _allocate_shared_runtime_event_seq(
        self,
        *,
        require_counter: bool,
        run_id: Optional[str] = None,
        operation: str = "runtime event sequencing",
    ) -> Optional[int]:
        shared_wallet_proxy = self.config.get("shared_wallet_proxy")
        if not isinstance(shared_wallet_proxy, Mapping):
            if require_counter:
                raise ValueError(f"shared_wallet_proxy is required for {operation}")
            return None
        seq_counter = shared_wallet_proxy.get("runtime_event_seq")
        proxy_lock = shared_wallet_proxy.get("lock")
        if seq_counter is None:
            if require_counter:
                raise RuntimeError(f"shared runtime_event_seq counter is required for {operation}")
            if run_id is not None and not bool(getattr(self, "_runtime_event_seq_missing_warned", False)):
                logger.warning(
                    "bot_runtime_event_seq_counter_missing | bot_id=%s | run_id=%s",
                    self.bot_id,
                    run_id,
                )
                setattr(self, "_runtime_event_seq_missing_warned", True)
            return None
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
            return int(next_value)
        finally:
            if proxy_lock is not None:
                proxy_lock.release()

    def _allocate_canonical_fact_seq(self) -> int:
        start_context = getattr(self, "_start_context", None)
        if start_context is None:
            ensure_start_context = getattr(self, "_ensure_start_context", None)
            if callable(ensure_start_context):
                start_context = ensure_start_context()
        if start_context is None:
            raise ValueError("start context is required before canonical BotLens fact append")
        seq = self._allocate_shared_runtime_event_seq(
            require_counter=True,
            run_id=start_context.run_id,
            operation="canonical BotLens fact append",
        )
        if seq is None:
            raise RuntimeError("shared runtime_event_seq counter is required before canonical BotLens fact append")
        return int(seq)

    def _allocate_runtime_event_seq(self) -> int:
        if self._run_context is None:
            raise ValueError("run context is required before runtime event sequencing")
        seq_override = self._allocate_shared_runtime_event_seq(
            require_counter=False,
            run_id=self._run_context.run_id,
        )
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
            self._logs.append(entry)
        self._mark_logs_mutated()

    def _record_runtime_warning(self, warning: Optional[Mapping[str, object]]) -> None:
        """Capture runtime warnings for UI consumption."""

        if not warning:
            return
        entry: Dict[str, object] = dict(warning)
        warning_type = str(entry.get("warning_type") or entry.get("type") or "runtime_warning").strip() or "runtime_warning"
        entry["warning_type"] = warning_type
        entry["type"] = warning_type
        severity = str(entry.get("severity") or entry.get("level") or "warning").strip().lower() or "warning"
        entry["severity"] = severity
        entry["level"] = severity
        entry.setdefault("message", entry.get("message") or "Runtime warning")
        entry.setdefault("source", entry.get("source") or "runtime")
        entry.setdefault("bot_id", self.bot_id)
        entry.setdefault("bot_mode", self.run_type)
        now_iso = _isoformat(datetime.now(timezone.utc))
        entry.setdefault("timestamp", now_iso)
        context = dict(entry.get("context") or {})
        entry["context"] = context
        warning_id = str(entry.get("warning_id") or "").strip() or self._warning_identity(entry, context)
        entry["warning_id"] = warning_id
        entry["id"] = warning_id
        with self._lock:
            warnings = list(self._warnings)
            match_index = next(
                (index for index, existing in enumerate(warnings) if str(existing.get("warning_id") or "") == warning_id),
                None,
            )
            if match_index is None:
                entry.setdefault("count", 1)
                entry.setdefault("first_seen_at", entry.get("timestamp") or now_iso)
                entry["last_seen_at"] = entry.get("timestamp") or now_iso
                entry["updated_at"] = entry["last_seen_at"]
                if len(warnings) >= self._warning_limit:
                    warnings = warnings[-(self._warning_limit - 1) :] if self._warning_limit > 1 else []
                warnings.append(entry)
            else:
                existing = dict(warnings.pop(match_index))
                existing.update(entry)
                existing["count"] = int(existing.get("count") or 1) + 1
                existing["first_seen_at"] = existing.get("first_seen_at") or existing.get("timestamp") or now_iso
                existing["last_seen_at"] = entry.get("timestamp") or now_iso
                existing["updated_at"] = existing["last_seen_at"]
                warnings.append(existing)
            self._warnings = deque(warnings, maxlen=self._warning_limit)
            self.state["warnings"] = list(self._warnings)

    def warnings(self) -> List[Dict[str, object]]:
        """Return the current runtime warnings."""

        with self._lock:
            return list(self._warnings)

    @staticmethod
    def _warning_identity(entry: Mapping[str, object], context: Mapping[str, object]) -> str:
        fields = (
            str(entry.get("warning_type") or entry.get("type") or "runtime_warning").strip().lower(),
            str(entry.get("indicator_id") or context.get("indicator_id") or "").strip().lower(),
            str(entry.get("symbol_key") or context.get("symbol_key") or "").strip().lower(),
            str(entry.get("symbol") or context.get("symbol") or "").strip().lower(),
            str(entry.get("timeframe") or context.get("timeframe") or "").strip().lower(),
            str(entry.get("source") or "runtime").strip().lower(),
        )
        compact = [field for field in fields if field]
        return "::".join(compact) or str(uuid.uuid4())

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

    def _runtime_context_base(
        self,
        *,
        series: Optional[StrategySeries],
        bar_ts: Optional[datetime],
        parent_missing: bool = False,
        missing_parent_hint: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self._run_context is None:
            raise ValueError("run context is required before building runtime event context")
        return {
            "run_id": self._run_context.run_id,
            "bot_id": self.bot_id,
            "strategy_id": series.strategy_id if series is not None else "__runtime__",
            "symbol": series.symbol if series is not None else None,
            "timeframe": series.timeframe if series is not None else None,
            "bar_ts": bar_ts,
            "parent_missing": bool(parent_missing),
            "missing_parent_hint": missing_parent_hint,
        }

    def _find_signal_event(self, *, series: StrategySeries, correlation_id: str) -> Optional[RuntimeEvent]:
        for event in self._runtime_events_reverse():
            if event.event_name != RuntimeEventName.SIGNAL_EMITTED:
                continue
            if event.correlation_id != correlation_id:
                continue
            if event.context.strategy_id != series.strategy_id:
                continue
            if event.context.symbol != series.symbol:
                continue
            if event.context.timeframe != series.timeframe:
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
            if event.context.strategy_id != series.strategy_id:
                continue
            if trade_id and str(getattr(event.context, "trade_id", "") or "") != str(trade_id):
                continue
            return event
        return None

    def _find_entry_event(self, *, trade_id: Optional[str]) -> Optional[RuntimeEvent]:
        if not trade_id:
            return None
        for event in self._runtime_events_reverse():
            if event.event_name != RuntimeEventName.ENTRY_FILLED:
                continue
            if str(getattr(event.context, "trade_id", "") or "") != str(trade_id):
                continue
            return event
        return None

    def _find_trade_decision_event(self, *, trade_id: Optional[str]) -> Optional[RuntimeEvent]:
        if not trade_id:
            return None
        for event in self._runtime_events_reverse():
            if event.event_name != RuntimeEventName.DECISION_ACCEPTED:
                continue
            if str(getattr(event.context, "trade_id", "") or "") != str(trade_id):
                continue
            return event
        return None

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
            sinks = list(self._event_sinks)
        if self._report_artifact_bundle is not None:
            self._report_artifact_bundle.record_runtime_event(serialized=serialized)
        for sink in sinks:
            sink.emit(event)
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

    def _emit_runtime_event(
        self,
        *,
        event_name: RuntimeEventName,
        correlation_id: str,
        context: Any,
        root_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        event_ts: Optional[datetime] = None,
    ) -> RuntimeEvent:
        if self._run_context is None:
            raise ValueError("run context is required before emitting runtime events")
        event = new_runtime_event(
            event_name=event_name,
            correlation_id=correlation_id,
            context=context,
            root_id=root_id,
            parent_id=parent_id,
            event_ts=event_ts,
            allow_missing_parent=bool(getattr(context, "parent_missing", False)),
        )
        self._persist_runtime_event(event)
        return event

    def _emit_wallet_initialized_event(self, balances: Mapping[str, Any]) -> RuntimeEvent:
        normalized = {
            str(currency).upper(): float(amount)
            for currency, amount in dict(balances or {}).items()
        }
        correlation_id = build_correlation_id(
            run_id=self._run_context.run_id if self._run_context is not None else "",
            symbol=None,
            timeframe=None,
            bar_ts=None,
        )
        return self._emit_runtime_event(
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=correlation_id,
            context=WalletInitializedContext(
                **self._runtime_context_base(series=None, bar_ts=None),
                balances=normalized,
                source="run_start",
            ),
        )

    def _emit_runtime_error_event(
        self,
        *,
        location: str,
        error: Exception,
        series: Optional[StrategySeries] = None,
        bar_ts: Optional[datetime] = None,
    ) -> RuntimeEvent:
        correlation_id = (
            self._bar_correlation_id(series, bar_ts)
            if series is not None
            else build_correlation_id(
                run_id=self._run_context.run_id if self._run_context is not None else "",
                symbol=None,
                timeframe=None,
                bar_ts=bar_ts,
            )
        )
        return self._emit_runtime_event(
            event_name=RuntimeEventName.RUNTIME_ERROR,
            correlation_id=correlation_id,
            context=RuntimeErrorContext(
                **self._runtime_context_base(series=series, bar_ts=bar_ts),
                exception_type=error.__class__.__name__,
                message=str(error),
                location=location,
                reason_code=ReasonCode.RUNTIME_EXCEPTION,
            ),
        )

    def _emit_signal_event(
        self,
        *,
        series: StrategySeries,
        candle: Candle,
        signal: StrategySignal,
        decision_artifact: Optional[Mapping[str, Any]] = None,
    ) -> RuntimeEvent:
        correlation_id = self._bar_correlation_id(series, candle.time)
        return self._emit_runtime_event(
            event_name=RuntimeEventName.SIGNAL_EMITTED,
            correlation_id=correlation_id,
            context=SignalEmittedContext(
                **self._runtime_context_base(series=series, bar_ts=candle.time),
                signal_id=signal.signal_id,
                source_type=signal.source_type,
                source_id=signal.source_id,
                signal_type="strategy_signal",
                direction=signal.direction,
                signal_price=float(candle.close),
                strategy_hash=signal.strategy_hash,
                decision_id=signal.decision_id,
                rule_id=signal.rule_id,
                intent=signal.intent,
                event_key=signal.event_key,
                decision_artifact=dict(decision_artifact or {}),
                bar=RuntimeBar(
                    time=candle.time,
                    open=float(candle.open),
                    high=float(candle.high),
                    low=float(candle.low),
                    close=float(candle.close),
                ),
                reason_code=ReasonCode.SIGNAL_STRATEGY_SIGNAL,
            ),
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
        base_kwargs = self._runtime_context_base(
            series=series,
            bar_ts=candle.time,
            parent_missing=signal_event is None,
            missing_parent_hint=missing_parent_hint,
        )
        if decision == "accepted":
            context = DecisionAcceptedContext(
                **base_kwargs,
                signal_id=signal.signal_id,
                source_type=signal.source_type,
                source_id=signal.source_id,
                decision=decision,
                direction=signal.direction,
                signal_price=float(signal_price),
                trade_id=trade_id,
                strategy_hash=signal.strategy_hash,
                decision_id=signal.decision_id,
                rule_id=signal.rule_id,
                intent=signal.intent,
                event_key=signal.event_key,
                reason_code=coerce_reason_code(reason_code) or ReasonCode.DECISION_ACCEPTED,
            )
        else:
            rejection_context = {}
            if isinstance(rejection_artifact, Mapping):
                raw_context = rejection_artifact.get("context")
                if isinstance(raw_context, Mapping):
                    rejection_context = dict(raw_context)
            instrument = series.instrument if isinstance(series.instrument, Mapping) else {}
            rejection_context = self._ensure_rejected_attempt_identity(
                {
                    **rejection_context,
                    "run_id": base_kwargs.get("run_id"),
                    "strategy_id": series.strategy_id,
                    "instrument_id": instrument.get("id"),
                    "symbol": series.symbol,
                    "timeframe": series.timeframe,
                    "bar_time": _isoformat(candle.time),
                    "decision_id": signal.decision_id,
                    "signal_id": signal.signal_id,
                    "direction": signal.direction,
                    "event_key": signal.event_key,
                    "attempt_kind": "entry_request",
                }
            )
            rejection_artifact_payload = dict(rejection_artifact or {})
            artifact_context = rejection_artifact_payload.get("context")
            if isinstance(artifact_context, Mapping):
                artifact_context = dict(artifact_context)
            else:
                artifact_context = {}
            for key in ("attempt_id", "entry_request_id"):
                value = rejection_context.get(key)
                if value is not None:
                    artifact_context.setdefault(key, value)
            if artifact_context:
                rejection_artifact_payload["context"] = artifact_context
            context = DecisionRejectedContext(
                **base_kwargs,
                signal_id=signal.signal_id,
                source_type=signal.source_type,
                source_id=signal.source_id,
                decision=decision,
                direction=signal.direction,
                signal_price=float(signal_price),
                trade_id=None,
                attempt_id=(
                    rejection_context.get("attempt_id")
                    or rejection_context.get("entry_request_id")
                    or rejection_context.get("settlement_attempt_id")
                    or rejection_context.get("order_request_id")
                ),
                order_request_id=rejection_context.get("order_request_id"),
                entry_request_id=rejection_context.get("entry_request_id"),
                settlement_attempt_id=rejection_context.get("settlement_attempt_id"),
                blocking_trade_id=rejection_context.get("blocking_trade_id") or rejection_context.get("active_trade_id"),
                strategy_hash=signal.strategy_hash,
                decision_id=signal.decision_id,
                rule_id=signal.rule_id,
                intent=signal.intent,
                event_key=signal.event_key,
                rejection_artifact=rejection_artifact_payload,
                message=message or "Decision rejected",
                reason_code=coerce_reason_code(reason_code) or ReasonCode.RUNTIME_PARENT_MISSING,
            )
        return self._emit_runtime_event(
            event_name=event_name,
            correlation_id=correlation_id,
            context=context,
            root_id=resolved_root_id,
            parent_id=resolved_parent_id,
            event_ts=candle.time,
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
        reservation_id = wallet_fill_metadata.get("reservation_id")
        reservation_correlation_id = wallet_fill_metadata.get("correlation_id")
        required_delta = wallet_fill_metadata.get("required_delta")
        context = EntryFilledContext(
            **self._runtime_context_base(
                series=series,
                bar_ts=candle.time,
                parent_missing=bool(missing_parent_hint),
                missing_parent_hint=missing_parent_hint,
            ),
            trade_id=trade_id,
            wallet_correlation_id=str(reservation_correlation_id or f"trade:{trade_id}"),
            side="buy" if direction == "long" else "sell",
            direction=direction,
            qty=float(qty),
            price=price,
            notional=float(notional),
            fee_paid=float(fee_paid),
            base_currency=str(getattr(trade, "base_currency", "") or ""),
            quote_currency=str(getattr(trade, "quote_currency", "") or ""),
            accounting_mode=accounting_mode,
            wallet_delta=WalletDelta(**wallet_delta_payload),
            reservation_id=str(reservation_id) if reservation_id else None,
            required_delta=dict(required_delta) if isinstance(required_delta, Mapping) else {},
            reason_code=ReasonCode.EXEC_ENTRY_FILLED if not missing_parent_hint else ReasonCode.RUNTIME_PARENT_MISSING,
        )
        return self._emit_runtime_event(
            event_name=RuntimeEventName.ENTRY_FILLED,
            correlation_id=correlation_id,
            context=context,
            root_id=(
                signal_event.root_id
                if signal_event is not None
                else (decision_event.root_id if decision_event is not None else None)
            ),
            parent_id=decision_event.event_id if decision_event is not None else None,
            event_ts=trade.entry_time if hasattr(trade, "entry_time") else candle.time,
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
        reservation_id = wallet_fill_metadata.get("reservation_id")
        reservation_correlation_id = wallet_fill_metadata.get("correlation_id")
        required_delta = wallet_fill_metadata.get("required_delta")
        event_ts = self._runtime_event_time(event.get("time")) or candle.time
        exit_reason = reason if not missing_parent_hint else ReasonCode.RUNTIME_PARENT_MISSING
        context = ExitFilledContext(
            **self._runtime_context_base(
                series=series,
                bar_ts=candle.time,
                parent_missing=bool(missing_parent_hint),
                missing_parent_hint=missing_parent_hint,
            ),
            trade_id=trade_id,
            wallet_correlation_id=str(reservation_correlation_id or f"trade:{trade_id}"),
            side="sell" if str(event.get("direction") or "").lower() == "long" else "buy",
            direction=str(event.get("direction") or ""),
            qty=qty,
            price=price,
            notional=float(notional),
            fee_paid=float(fee_paid),
            realized_pnl=float(realized_pnl),
            base_currency=str(getattr(series.risk_engine, "base_currency", "") or ""),
            quote_currency=str(event.get("currency") or getattr(series.risk_engine, "quote_currency", "") or ""),
            accounting_mode=accounting_mode,
            exit_kind=exit_kind,
            event_impact_pnl=float(event.get("pnl") or 0.0) if subtype in {"target", "stop"} else None,
            trade_net_pnl=float(event.get("net_pnl") or 0.0) if subtype == "close" else None,
            wallet_delta=WalletDelta(**wallet_delta_payload),
            reservation_id=str(reservation_id) if reservation_id else None,
            required_delta=dict(required_delta) if isinstance(required_delta, Mapping) else {},
            event_subtype=subtype,
            reason_code=exit_reason,
        )
        return self._emit_runtime_event(
            event_name=RuntimeEventName.EXIT_FILLED,
            correlation_id=build_correlation_id(
                run_id=self._run_context.run_id if self._run_context is not None else "",
                symbol=series.symbol,
                timeframe=series.timeframe,
                bar_ts=candle.time,
            ),
            context=context,
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
            event_ts=event_ts,
        )

    @staticmethod
    def _ensure_rejected_attempt_identity(context: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(context or {})

        def _text(key: str) -> Optional[str]:
            value = payload.get(key)
            text = str(value or "").strip()
            return text or None

        blocking_trade_id = _text("blocking_trade_id") or _text("active_trade_id")
        if blocking_trade_id:
            payload["blocking_trade_id"] = blocking_trade_id

        existing_identity = (
            _text("attempt_id")
            or _text("entry_request_id")
            or _text("order_request_id")
            or _text("settlement_attempt_id")
            or _text("blocking_trade_id")
        )
        if existing_identity:
            if not _text("attempt_id"):
                payload["attempt_id"] = existing_identity
            return payload

        stable_key = "|".join(
            str(part or "")
            for part in (
                "rejected_decision_attempt",
                payload.get("attempt_kind") or "entry_request",
                payload.get("run_id"),
                payload.get("strategy_id"),
                payload.get("instrument_id"),
                payload.get("symbol"),
                payload.get("timeframe"),
                payload.get("bar_time"),
                payload.get("decision_id"),
                payload.get("signal_id"),
                payload.get("direction"),
                payload.get("event_key"),
            )
        )
        entry_request_id = f"entry_request:{uuid.uuid5(uuid.NAMESPACE_URL, stable_key)}"
        payload["entry_request_id"] = entry_request_id
        payload["attempt_id"] = entry_request_id
        return payload

    @staticmethod
    def _normalise_rejection_metadata(
        rejection_meta: Optional[Mapping[str, Any]],
        blocking_trade_id: Optional[str],
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        metadata_payload: Dict[str, Any] = {}
        if blocking_trade_id:
            metadata_payload["blocking_trade_id"] = str(blocking_trade_id)
        if not isinstance(rejection_meta, Mapping):
            return None, metadata_payload
        metadata_payload = {
            k: v
            for k, v in rejection_meta.items()
            if k not in {"reason", "trade_id"}
        }
        if blocking_trade_id:
            metadata_payload.setdefault("blocking_trade_id", str(blocking_trade_id))
        meta_trade_id = rejection_meta.get("trade_id")
        if meta_trade_id is not None:
            metadata_payload.setdefault("settlement_attempt_id", str(meta_trade_id))
            metadata_payload.setdefault("attempt_id", str(meta_trade_id))
        entry_request_id = metadata_payload.get("entry_request_id")
        if entry_request_id is not None:
            metadata_payload.setdefault("attempt_id", str(entry_request_id))
        return None, metadata_payload

    def _build_run_context(self) -> RunContext:
        wallet_config = self.config.get("wallet_config")
        if not isinstance(wallet_config, dict):
            raise ValueError("wallet_config is required to start a bot run")
        balances = wallet_config.get("balances")
        if not isinstance(balances, dict) or not balances:
            raise ValueError("wallet_config.balances is required to start a bot run")
        shared_wallet_proxy = self.config.get("shared_wallet_proxy")
        logger.info(with_log_context("bot_runtime_run_context_wallet_init", self._runtime_log_context()))
        start_context = getattr(self, "_start_context", None)
        if start_context is None:
            ensure_start_context = getattr(self, "_ensure_start_context", None)
            if callable(ensure_start_context):
                start_context = ensure_start_context()
        configured_run_id = str(getattr(start_context, "run_id", "") or "").strip()
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
        decision_trace = [
            entry
            for entry in (decision_trace_entry_from_runtime_event(event) for event in runtime_events)
            if entry is not None
        ]
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
                    "context": dict(event.get("context") or {}),
                }
            )
        performance_summary = self._runtime_performance_summary()
        return {
            "run_id": self._run_context.run_id,
            "bot_id": self.bot_id,
            "started_at": self._run_context.started_at,
            "ended_at": self._run_context.ended_at,
            "status": status,
            "performance_summary": performance_summary,
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
            "decision_trace": decision_trace,
            "decision_artifacts": list(self._run_context.decision_artifacts),
            "rejection_artifacts": list(self._run_context.rejection_artifacts),
        }

    def _runtime_performance_summary(self) -> Dict[str, Any]:
        if self._run_context is None:
            return {}
        started_at = self._run_context.started_at
        ended_at = self._run_context.ended_at
        started_dt = _parse_runtime_iso(started_at)
        ended_dt = _parse_runtime_iso(ended_at)
        runtime_wall_clock_seconds = (
            round(max((ended_dt - started_dt).total_seconds(), 0.0), 6)
            if started_dt is not None and ended_dt is not None
            else None
        )
        loop_started = getattr(self, "_runtime_loop_started_at", None)
        loop_ended = getattr(self, "_runtime_loop_ended_at", None)
        return {
            "runtime_started_at": started_at,
            "runtime_ended_at": ended_at,
            "runtime_loop_started_at": _isoformat(loop_started) if loop_started is not None else None,
            "runtime_loop_ended_at": _isoformat(loop_ended) if loop_ended is not None else None,
            "user_wall_clock_seconds": runtime_wall_clock_seconds,
            "db_run_started_ended_seconds": runtime_wall_clock_seconds,
            "runtime_loop_duration_seconds": (
                round(float(getattr(self, "_runtime_loop_duration_seconds", 0.0)), 6)
                if getattr(self, "_runtime_loop_duration_seconds", None) is not None
                else None
            ),
            "async_projection_flush_drain_seconds": (
                round(float(getattr(self, "_runtime_flush_drain_duration_seconds", 0.0)), 6)
                if getattr(self, "_runtime_flush_drain_duration_seconds", None) is not None
                else None
            ),
            "duration_basis": {
                "user_wall_clock_seconds": "run_context.started_at to run_context.ended_at after runtime artifact build",
                "runtime_loop_duration_seconds": "execution loop start to runner completion before flush/drain",
                "db_run_started_ended_seconds": "persisted portal_bot_runs started_at to ended_at",
                "async_projection_flush_drain_seconds": "runtime persistence and step-trace flush after loop completion",
            },
        }
