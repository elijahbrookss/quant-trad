"""Canonical runtime event contract for bot execution."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Mapping, Optional

from core.events import EventEnvelope, normalize_utc_datetime, parse_optional_datetime, serialize_value


SCHEMA_VERSION = 2


class RuntimeEventCategory(str, Enum):
    """Informational runtime event category."""

    SIGNAL = "SIGNAL"
    DECISION = "DECISION"
    EXECUTION = "EXECUTION"
    WALLET = "WALLET"
    RUNTIME = "RUNTIME"
    OUTCOME = "OUTCOME"


class ExitKind(str, Enum):
    TARGET = "TARGET"
    STOP = "STOP"
    CLOSE = "CLOSE"


class RuntimeEventName(str, Enum):
    SIGNAL_EMITTED = "SIGNAL_EMITTED"
    DECISION_ACCEPTED = "DECISION_ACCEPTED"
    DECISION_REJECTED = "DECISION_REJECTED"
    ENTRY_FILLED = "ENTRY_FILLED"
    EXIT_FILLED = "EXIT_FILLED"
    WALLET_INITIALIZED = "WALLET_INITIALIZED"
    WALLET_DEPOSITED = "WALLET_DEPOSITED"
    RUNTIME_ERROR = "RUNTIME_ERROR"
    SYMBOL_DEGRADED = "SYMBOL_DEGRADED"
    SYMBOL_RECOVERED = "SYMBOL_RECOVERED"


class ReasonCode(str, Enum):
    SIGNAL_STRATEGY_SIGNAL = "SIGNAL_STRATEGY_SIGNAL"
    DECISION_ACCEPTED = "DECISION_ACCEPTED"
    DECISION_REJECTED = "DECISION_REJECTED"
    DECISION_REJECTED_ACTIVE_TRADE = "DECISION_REJECTED_ACTIVE_TRADE"
    DECISION_REJECTED_INSTRUMENT_MISSING = "DECISION_REJECTED_INSTRUMENT_MISSING"
    DECISION_REJECTED_RISK_ENGINE = "DECISION_REJECTED_RISK_ENGINE"
    WALLET_INSUFFICIENT_CASH = "WALLET_INSUFFICIENT_CASH"
    WALLET_INSUFFICIENT_MARGIN = "WALLET_INSUFFICIENT_MARGIN"
    WALLET_INSUFFICIENT_QTY = "WALLET_INSUFFICIENT_QTY"
    WALLET_INSTRUMENT_MISCONFIGURED = "WALLET_INSTRUMENT_MISCONFIGURED"
    EXEC_ENTRY_FILLED = "EXEC_ENTRY_FILLED"
    EXEC_EXIT_TARGET = "EXEC_EXIT_TARGET"
    EXEC_EXIT_STOP = "EXEC_EXIT_STOP"
    EXEC_EXIT_CLOSE = "EXEC_EXIT_CLOSE"
    RUNTIME_EXCEPTION = "RUNTIME_EXCEPTION"
    RUNTIME_PARENT_MISSING = "RUNTIME_PARENT_MISSING"
    SYMBOL_DEGRADED = "SYMBOL_DEGRADED"
    SYMBOL_RECOVERED = "SYMBOL_RECOVERED"
    UNKNOWN = "UNKNOWN"


_EVENT_DEFAULT_CATEGORY: Dict[RuntimeEventName, RuntimeEventCategory] = {
    RuntimeEventName.SIGNAL_EMITTED: RuntimeEventCategory.SIGNAL,
    RuntimeEventName.DECISION_ACCEPTED: RuntimeEventCategory.DECISION,
    RuntimeEventName.DECISION_REJECTED: RuntimeEventCategory.DECISION,
    RuntimeEventName.ENTRY_FILLED: RuntimeEventCategory.EXECUTION,
    RuntimeEventName.EXIT_FILLED: RuntimeEventCategory.OUTCOME,
    RuntimeEventName.WALLET_INITIALIZED: RuntimeEventCategory.WALLET,
    RuntimeEventName.WALLET_DEPOSITED: RuntimeEventCategory.WALLET,
    RuntimeEventName.RUNTIME_ERROR: RuntimeEventCategory.RUNTIME,
    RuntimeEventName.SYMBOL_DEGRADED: RuntimeEventCategory.RUNTIME,
    RuntimeEventName.SYMBOL_RECOVERED: RuntimeEventCategory.RUNTIME,
}


def format_correlation_bar_ts(bar_ts: Optional[datetime]) -> str:
    """Return deterministic UTC timestamp text used in correlation IDs."""

    if bar_ts is None:
        return "na"
    utc = normalize_utc_datetime(bar_ts)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_correlation_id(
    *,
    run_id: str,
    symbol: Optional[str],
    timeframe: Optional[str],
    bar_ts: Optional[datetime],
) -> str:
    symbol_part = str(symbol) if symbol is not None else "runtime"
    timeframe_part = str(timeframe) if timeframe is not None else "runtime"
    return f"{str(run_id)}:{symbol_part}:{timeframe_part}:{format_correlation_bar_ts(bar_ts)}"


def coerce_reason_code(value: Optional[str | ReasonCode]) -> Optional[ReasonCode]:
    if value is None:
        return None
    if isinstance(value, ReasonCode):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return ReasonCode(text)
    except ValueError:
        return ReasonCode.UNKNOWN


def _require_str(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if value is None:
        raise ValueError(f"context.{key} is required")
    text = str(value).strip()
    if not text:
        raise ValueError(f"context.{key} is required")
    return text


def _optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _require_numeric(data: Mapping[str, Any], key: str) -> float:
    value = data.get(key)
    if value is None:
        raise ValueError(f"context.{key} is required")
    try:
        return float(value)
    except Exception as exc:
        raise ValueError(f"context.{key} must be numeric") from exc


def _copy_mapping(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): serialize_value(item) for key, item in value.items()}


def _require_distinct_signal_id(
    *,
    signal_id: Optional[str],
    decision_id: Optional[str],
    object_name: str,
) -> None:
    normalized_signal_id = _optional_text(signal_id)
    normalized_decision_id = _optional_text(decision_id)
    if (
        normalized_signal_id is not None
        and normalized_decision_id is not None
        and normalized_signal_id == normalized_decision_id
    ):
        raise ValueError(
            f"{object_name}.signal_id must not equal {object_name}.decision_id "
            f"value={normalized_signal_id}"
        )


@dataclass(frozen=True)
class RuntimeBar:
    time: datetime
    open: float
    high: float
    low: float
    close: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "time", normalize_utc_datetime(self.time))
        object.__setattr__(self, "open", float(self.open))
        object.__setattr__(self, "high", float(self.high))
        object.__setattr__(self, "low", float(self.low))
        object.__setattr__(self, "close", float(self.close))

    def to_dict(self) -> dict[str, Any]:
        return dict(serialize_value(self))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RuntimeBar":
        if not isinstance(value, Mapping):
            raise ValueError("context.bar is required")
        parsed_time = parse_optional_datetime(value.get("time"))
        if parsed_time is None:
            raise ValueError("context.bar.time is required")
        return cls(
            time=parsed_time,
            open=_require_numeric(value, "open"),
            high=_require_numeric(value, "high"),
            low=_require_numeric(value, "low"),
            close=_require_numeric(value, "close"),
        )


@dataclass(frozen=True)
class WalletDelta:
    collateral_reserved: float
    collateral_released: float
    fee_paid: float
    balance_delta: Optional[float] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "collateral_reserved", float(self.collateral_reserved))
        object.__setattr__(self, "collateral_released", float(self.collateral_released))
        object.__setattr__(self, "fee_paid", float(self.fee_paid))
        if self.collateral_reserved < 0.0:
            raise ValueError("context.wallet_delta.collateral_reserved must be >= 0")
        if self.collateral_released < 0.0:
            raise ValueError("context.wallet_delta.collateral_released must be >= 0")
        if self.fee_paid < 0.0:
            raise ValueError("context.wallet_delta.fee_paid must be >= 0")
        if self.balance_delta is not None:
            object.__setattr__(self, "balance_delta", float(self.balance_delta))

    def to_dict(self) -> dict[str, Any]:
        return dict(serialize_value(self))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WalletDelta":
        if not isinstance(value, Mapping):
            raise ValueError("context.wallet_delta is required")
        return cls(
            collateral_reserved=_require_numeric(value, "collateral_reserved"),
            collateral_released=_require_numeric(value, "collateral_released"),
            fee_paid=_require_numeric(value, "fee_paid"),
            balance_delta=(
                float(value.get("balance_delta"))
                if value.get("balance_delta") is not None
                else None
            ),
        )


@dataclass(frozen=True, kw_only=True)
class RuntimeEventContextBase:
    run_id: str
    bot_id: str
    strategy_id: str
    symbol: Optional[str]
    timeframe: Optional[str]
    bar_ts: Optional[datetime]
    parent_missing: bool = False
    missing_parent_hint: Optional[str] = None

    def __post_init__(self) -> None:
        if not str(self.run_id).strip():
            raise ValueError("context.run_id is required")
        if not str(self.bot_id).strip():
            raise ValueError("context.bot_id is required")
        if not str(self.strategy_id).strip():
            raise ValueError("context.strategy_id is required")
        object.__setattr__(self, "symbol", _optional_text(self.symbol))
        object.__setattr__(self, "timeframe", _optional_text(self.timeframe))
        if self.bar_ts is not None:
            object.__setattr__(self, "bar_ts", normalize_utc_datetime(self.bar_ts))

    def to_dict(self) -> dict[str, Any]:
        return dict(serialize_value(self))


@dataclass(frozen=True, kw_only=True)
class SignalEmittedContext(RuntimeEventContextBase):
    signal_type: str
    direction: str
    signal_price: float
    bar: RuntimeBar
    signal_id: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    strategy_hash: Optional[str] = None
    decision_id: Optional[str] = None
    rule_id: Optional[str] = None
    intent: Optional[str] = None
    event_key: Optional[str] = None
    decision_artifact: Mapping[str, Any] | None = None
    category: RuntimeEventCategory = RuntimeEventCategory.SIGNAL
    reason_code: Optional[ReasonCode] = ReasonCode.SIGNAL_STRATEGY_SIGNAL

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.bar, RuntimeBar):
            raise ValueError("context.bar is required")
        if not str(self.signal_type).strip():
            raise ValueError("context.signal_type is required")
        if not str(self.direction).strip():
            raise ValueError("context.direction is required")
        signal_id = _optional_text(self.signal_id)
        decision_id = _optional_text(self.decision_id)
        if signal_id is None:
            raise ValueError("context.signal_id is required")
        _require_distinct_signal_id(
            signal_id=signal_id,
            decision_id=decision_id,
            object_name="context",
        )
        object.__setattr__(self, "signal_id", signal_id)
        object.__setattr__(self, "source_type", _optional_text(self.source_type))
        object.__setattr__(self, "source_id", _optional_text(self.source_id))
        object.__setattr__(self, "strategy_hash", _optional_text(self.strategy_hash))
        object.__setattr__(self, "decision_id", decision_id)
        object.__setattr__(self, "rule_id", _optional_text(self.rule_id))
        object.__setattr__(self, "intent", _optional_text(self.intent))
        object.__setattr__(self, "event_key", _optional_text(self.event_key))
        object.__setattr__(self, "signal_price", float(self.signal_price))
        object.__setattr__(self, "decision_artifact", _copy_mapping(self.decision_artifact))


@dataclass(frozen=True, kw_only=True)
class DecisionAcceptedContext(RuntimeEventContextBase):
    decision: str
    signal_id: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    direction: Optional[str] = None
    signal_price: Optional[float] = None
    trade_id: Optional[str] = None
    strategy_hash: Optional[str] = None
    decision_id: Optional[str] = None
    rule_id: Optional[str] = None
    intent: Optional[str] = None
    event_key: Optional[str] = None
    event_subtype: str = "signal_accepted"
    category: RuntimeEventCategory = RuntimeEventCategory.DECISION
    reason_code: Optional[ReasonCode] = ReasonCode.DECISION_ACCEPTED

    def __post_init__(self) -> None:
        super().__post_init__()
        if not str(self.decision).strip():
            raise ValueError("context.decision is required")
        signal_id = _optional_text(self.signal_id)
        decision_id = _optional_text(self.decision_id)
        _require_distinct_signal_id(
            signal_id=signal_id,
            decision_id=decision_id,
            object_name="context",
        )
        object.__setattr__(self, "signal_id", signal_id)
        object.__setattr__(self, "source_type", _optional_text(self.source_type))
        object.__setattr__(self, "source_id", _optional_text(self.source_id))
        object.__setattr__(self, "direction", _optional_text(self.direction))
        object.__setattr__(self, "trade_id", _optional_text(self.trade_id))
        object.__setattr__(self, "strategy_hash", _optional_text(self.strategy_hash))
        object.__setattr__(self, "decision_id", decision_id)
        object.__setattr__(self, "rule_id", _optional_text(self.rule_id))
        object.__setattr__(self, "intent", _optional_text(self.intent))
        object.__setattr__(self, "event_key", _optional_text(self.event_key))
        if self.signal_price is not None:
            object.__setattr__(self, "signal_price", float(self.signal_price))


@dataclass(frozen=True, kw_only=True)
class DecisionRejectedContext(RuntimeEventContextBase):
    decision: str
    message: str
    signal_id: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    direction: Optional[str] = None
    signal_price: Optional[float] = None
    trade_id: Optional[str] = None
    attempt_id: Optional[str] = None
    order_request_id: Optional[str] = None
    entry_request_id: Optional[str] = None
    settlement_attempt_id: Optional[str] = None
    blocking_trade_id: Optional[str] = None
    strategy_hash: Optional[str] = None
    decision_id: Optional[str] = None
    rule_id: Optional[str] = None
    intent: Optional[str] = None
    event_key: Optional[str] = None
    rejection_artifact: Mapping[str, Any] | None = None
    event_subtype: str = "signal_rejected"
    category: RuntimeEventCategory = RuntimeEventCategory.DECISION
    reason_code: Optional[ReasonCode] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not str(self.decision).strip():
            raise ValueError("context.decision is required")
        if not str(self.message).strip():
            raise ValueError("context.message is required")
        signal_id = _optional_text(self.signal_id)
        decision_id = _optional_text(self.decision_id)
        _require_distinct_signal_id(
            signal_id=signal_id,
            decision_id=decision_id,
            object_name="context",
        )
        object.__setattr__(self, "signal_id", signal_id)
        object.__setattr__(self, "source_type", _optional_text(self.source_type))
        object.__setattr__(self, "source_id", _optional_text(self.source_id))
        object.__setattr__(self, "direction", _optional_text(self.direction))
        object.__setattr__(self, "trade_id", _optional_text(self.trade_id))
        object.__setattr__(self, "attempt_id", _optional_text(self.attempt_id))
        object.__setattr__(self, "order_request_id", _optional_text(self.order_request_id))
        object.__setattr__(self, "entry_request_id", _optional_text(self.entry_request_id))
        object.__setattr__(self, "settlement_attempt_id", _optional_text(self.settlement_attempt_id))
        object.__setattr__(self, "blocking_trade_id", _optional_text(self.blocking_trade_id))
        object.__setattr__(self, "strategy_hash", _optional_text(self.strategy_hash))
        object.__setattr__(self, "decision_id", decision_id)
        object.__setattr__(self, "rule_id", _optional_text(self.rule_id))
        object.__setattr__(self, "intent", _optional_text(self.intent))
        object.__setattr__(self, "event_key", _optional_text(self.event_key))
        if self.signal_price is not None:
            object.__setattr__(self, "signal_price", float(self.signal_price))
        object.__setattr__(self, "rejection_artifact", _copy_mapping(self.rejection_artifact))
        if self.reason_code is None:
            raise ValueError("context.reason_code is required for DECISION_REJECTED")


@dataclass(frozen=True, kw_only=True)
class EntryFilledContext(RuntimeEventContextBase):
    trade_id: str
    wallet_correlation_id: str
    side: str
    qty: float
    price: float
    notional: float
    wallet_delta: WalletDelta
    direction: Optional[str] = None
    fee_paid: Optional[float] = None
    base_currency: Optional[str] = None
    quote_currency: Optional[str] = None
    accounting_mode: Optional[str] = None
    reservation_id: Optional[str] = None
    required_delta: Mapping[str, Any] | None = None
    event_subtype: str = "entry"
    category: RuntimeEventCategory = RuntimeEventCategory.EXECUTION
    reason_code: Optional[ReasonCode] = ReasonCode.EXEC_ENTRY_FILLED

    def __post_init__(self) -> None:
        super().__post_init__()
        if not str(self.trade_id).strip():
            raise ValueError("context.trade_id is required")
        if not str(self.wallet_correlation_id).strip():
            raise ValueError("context.wallet_correlation_id is required")
        if not str(self.side).strip():
            raise ValueError("context.side is required")
        if not isinstance(self.wallet_delta, WalletDelta):
            raise ValueError("context.wallet_delta is required")
        object.__setattr__(self, "qty", float(self.qty))
        object.__setattr__(self, "price", float(self.price))
        object.__setattr__(self, "notional", float(self.notional))
        if self.fee_paid is not None:
            object.__setattr__(self, "fee_paid", float(self.fee_paid))
        reservation_id = self.reservation_id
        if reservation_id is not None and not str(reservation_id).strip():
            raise ValueError("context.reservation_id must be non-empty when provided")
        object.__setattr__(self, "required_delta", _copy_mapping(self.required_delta))


@dataclass(frozen=True, kw_only=True)
class ExitFilledContext(RuntimeEventContextBase):
    trade_id: str
    wallet_correlation_id: str
    side: str
    qty: float
    price: float
    notional: float
    exit_kind: ExitKind
    wallet_delta: WalletDelta
    direction: Optional[str] = None
    fee_paid: Optional[float] = None
    realized_pnl: Optional[float] = None
    base_currency: Optional[str] = None
    quote_currency: Optional[str] = None
    accounting_mode: Optional[str] = None
    event_impact_pnl: Optional[float] = None
    trade_net_pnl: Optional[float] = None
    reservation_id: Optional[str] = None
    required_delta: Mapping[str, Any] | None = None
    event_subtype: str = "close"
    category: RuntimeEventCategory = RuntimeEventCategory.OUTCOME
    reason_code: Optional[ReasonCode] = ReasonCode.EXEC_EXIT_CLOSE

    def __post_init__(self) -> None:
        super().__post_init__()
        if not str(self.trade_id).strip():
            raise ValueError("context.trade_id is required")
        if not str(self.wallet_correlation_id).strip():
            raise ValueError("context.wallet_correlation_id is required")
        if not str(self.side).strip():
            raise ValueError("context.side is required")
        if not isinstance(self.exit_kind, ExitKind):
            raise ValueError("context.exit_kind is required")
        if not isinstance(self.wallet_delta, WalletDelta):
            raise ValueError("context.wallet_delta is required")
        object.__setattr__(self, "qty", float(self.qty))
        object.__setattr__(self, "price", float(self.price))
        object.__setattr__(self, "notional", float(self.notional))
        if self.fee_paid is not None:
            object.__setattr__(self, "fee_paid", float(self.fee_paid))
        if self.realized_pnl is not None:
            object.__setattr__(self, "realized_pnl", float(self.realized_pnl))
        if self.event_impact_pnl is not None:
            object.__setattr__(self, "event_impact_pnl", float(self.event_impact_pnl))
        if self.trade_net_pnl is not None:
            object.__setattr__(self, "trade_net_pnl", float(self.trade_net_pnl))
        reservation_id = self.reservation_id
        if reservation_id is not None and not str(reservation_id).strip():
            raise ValueError("context.reservation_id must be non-empty when provided")
        object.__setattr__(self, "required_delta", _copy_mapping(self.required_delta))


@dataclass(frozen=True, kw_only=True)
class WalletInitializedContext(RuntimeEventContextBase):
    balances: Mapping[str, float]
    source: str
    category: RuntimeEventCategory = RuntimeEventCategory.WALLET
    reason_code: Optional[ReasonCode] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not str(self.source).strip():
            raise ValueError("context.source is required")
        normalized: Dict[str, float] = {}
        if not isinstance(self.balances, Mapping) or not self.balances:
            raise ValueError("context.balances is required")
        for currency, amount in self.balances.items():
            code = str(currency or "").strip().upper()
            if not code:
                raise ValueError("context.balances keys must be non-empty")
            float_amount = float(amount)
            if float_amount < 0.0:
                raise ValueError("context.balances values must be >= 0")
            normalized[code] = float_amount
        object.__setattr__(self, "balances", normalized)


@dataclass(frozen=True, kw_only=True)
class WalletDepositedContext(RuntimeEventContextBase):
    asset: str
    amount: float
    category: RuntimeEventCategory = RuntimeEventCategory.WALLET
    reason_code: Optional[ReasonCode] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not str(self.asset).strip():
            raise ValueError("context.asset is required")
        object.__setattr__(self, "amount", float(self.amount))
        if self.amount < 0.0:
            raise ValueError("context.amount must be >= 0")


@dataclass(frozen=True, kw_only=True)
class RuntimeErrorContext(RuntimeEventContextBase):
    exception_type: str
    message: str
    location: str
    category: RuntimeEventCategory = RuntimeEventCategory.RUNTIME
    reason_code: Optional[ReasonCode] = ReasonCode.RUNTIME_EXCEPTION

    def __post_init__(self) -> None:
        super().__post_init__()
        if not str(self.exception_type).strip():
            raise ValueError("context.exception_type is required")
        if not str(self.message).strip():
            raise ValueError("context.message is required")
        if not str(self.location).strip():
            raise ValueError("context.location is required")
        if self.reason_code is None:
            raise ValueError("context.reason_code is required for RUNTIME_ERROR")


@dataclass(frozen=True, kw_only=True)
class RuntimeStatusContext(RuntimeEventContextBase):
    message: str
    category: RuntimeEventCategory = RuntimeEventCategory.RUNTIME
    reason_code: Optional[ReasonCode] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not str(self.message).strip():
            raise ValueError("context.message is required")


RuntimeEventContext = (
    SignalEmittedContext
    | DecisionAcceptedContext
    | DecisionRejectedContext
    | EntryFilledContext
    | ExitFilledContext
    | WalletInitializedContext
    | WalletDepositedContext
    | RuntimeErrorContext
    | RuntimeStatusContext
)


_CONTEXT_TYPE_BY_EVENT: Dict[RuntimeEventName, type[RuntimeEventContextBase]] = {
    RuntimeEventName.SIGNAL_EMITTED: SignalEmittedContext,
    RuntimeEventName.DECISION_ACCEPTED: DecisionAcceptedContext,
    RuntimeEventName.DECISION_REJECTED: DecisionRejectedContext,
    RuntimeEventName.ENTRY_FILLED: EntryFilledContext,
    RuntimeEventName.EXIT_FILLED: ExitFilledContext,
    RuntimeEventName.WALLET_INITIALIZED: WalletInitializedContext,
    RuntimeEventName.WALLET_DEPOSITED: WalletDepositedContext,
    RuntimeEventName.RUNTIME_ERROR: RuntimeErrorContext,
    RuntimeEventName.SYMBOL_DEGRADED: RuntimeStatusContext,
    RuntimeEventName.SYMBOL_RECOVERED: RuntimeStatusContext,
}


@dataclass(frozen=True)
class RuntimeEvent(EventEnvelope):
    event_name: RuntimeEventName
    context: RuntimeEventContext

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.event_name, RuntimeEventName):
            raise ValueError("event_name must be a RuntimeEventName")
        expected_type = _CONTEXT_TYPE_BY_EVENT[self.event_name]
        if not isinstance(self.context, expected_type):
            raise ValueError(
                f"context type mismatch for {self.event_name.value}: "
                f"expected {expected_type.__name__}, got {type(self.context).__name__}"
            )


def _runtime_common_context(data: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "run_id": _require_str(data, "run_id"),
        "bot_id": _require_str(data, "bot_id"),
        "strategy_id": _require_str(data, "strategy_id"),
        "symbol": _optional_text(data.get("symbol")),
        "timeframe": _optional_text(data.get("timeframe")),
        "bar_ts": parse_optional_datetime(data.get("bar_ts")),
        "parent_missing": bool(data.get("parent_missing", False)),
        "missing_parent_hint": _optional_text(data.get("missing_parent_hint")),
    }


def _runtime_context_from_dict(
    event_name: RuntimeEventName,
    data: Mapping[str, Any],
) -> RuntimeEventContext:
    common = _runtime_common_context(data)
    category = RuntimeEventCategory(str(data.get("category") or _EVENT_DEFAULT_CATEGORY[event_name].value))
    reason_code = coerce_reason_code(data.get("reason_code"))

    if event_name == RuntimeEventName.SIGNAL_EMITTED:
        return SignalEmittedContext(
            **common,
            signal_type=_require_str(data, "signal_type"),
            direction=_require_str(data, "direction"),
            signal_price=_require_numeric(data, "signal_price"),
            bar=RuntimeBar.from_dict(data.get("bar") if isinstance(data.get("bar"), Mapping) else {}),
            signal_id=_optional_text(data.get("signal_id")),
            source_type=_optional_text(data.get("source_type")),
            source_id=_optional_text(data.get("source_id")),
            strategy_hash=_optional_text(data.get("strategy_hash")),
            decision_id=_optional_text(data.get("decision_id")),
            rule_id=_optional_text(data.get("rule_id")),
            intent=_optional_text(data.get("intent")),
            event_key=_optional_text(data.get("event_key")),
            decision_artifact=_copy_mapping(data.get("decision_artifact")),
            category=category,
            reason_code=reason_code or ReasonCode.SIGNAL_STRATEGY_SIGNAL,
        )

    if event_name == RuntimeEventName.DECISION_ACCEPTED:
        return DecisionAcceptedContext(
            **common,
            decision=_require_str(data, "decision"),
            signal_id=_optional_text(data.get("signal_id")),
            source_type=_optional_text(data.get("source_type")),
            source_id=_optional_text(data.get("source_id")),
            direction=_optional_text(data.get("direction")),
            signal_price=(
                float(data.get("signal_price"))
                if data.get("signal_price") is not None
                else None
            ),
            trade_id=_optional_text(data.get("trade_id")),
            strategy_hash=_optional_text(data.get("strategy_hash")),
            decision_id=_optional_text(data.get("decision_id")),
            rule_id=_optional_text(data.get("rule_id")),
            intent=_optional_text(data.get("intent")),
            event_key=_optional_text(data.get("event_key")),
            event_subtype=_optional_text(data.get("event_subtype")) or "signal_accepted",
            category=category,
            reason_code=reason_code or ReasonCode.DECISION_ACCEPTED,
        )

    if event_name == RuntimeEventName.DECISION_REJECTED:
        return DecisionRejectedContext(
            **common,
            decision=_require_str(data, "decision"),
            message=_require_str(data, "message"),
            signal_id=_optional_text(data.get("signal_id")),
            source_type=_optional_text(data.get("source_type")),
            source_id=_optional_text(data.get("source_id")),
            direction=_optional_text(data.get("direction")),
            signal_price=(
                float(data.get("signal_price"))
                if data.get("signal_price") is not None
                else None
            ),
            trade_id=_optional_text(data.get("trade_id")),
            strategy_hash=_optional_text(data.get("strategy_hash")),
            decision_id=_optional_text(data.get("decision_id")),
            rule_id=_optional_text(data.get("rule_id")),
            intent=_optional_text(data.get("intent")),
            event_key=_optional_text(data.get("event_key")),
            rejection_artifact=_copy_mapping(data.get("rejection_artifact")),
            event_subtype=_optional_text(data.get("event_subtype")) or "signal_rejected",
            category=category,
            reason_code=reason_code,
        )

    if event_name == RuntimeEventName.ENTRY_FILLED:
        return EntryFilledContext(
            **common,
            trade_id=_require_str(data, "trade_id"),
            wallet_correlation_id=_require_str(data, "wallet_correlation_id"),
            side=_require_str(data, "side"),
            qty=_require_numeric(data, "qty"),
            price=_require_numeric(data, "price"),
            notional=_require_numeric(data, "notional"),
            wallet_delta=WalletDelta.from_dict(data.get("wallet_delta") if isinstance(data.get("wallet_delta"), Mapping) else {}),
            direction=_optional_text(data.get("direction")),
            fee_paid=(float(data.get("fee_paid")) if data.get("fee_paid") is not None else None),
            base_currency=_optional_text(data.get("base_currency")),
            quote_currency=_optional_text(data.get("quote_currency")),
            accounting_mode=_optional_text(data.get("accounting_mode")),
            reservation_id=_optional_text(data.get("reservation_id")),
            required_delta=_copy_mapping(data.get("required_delta")),
            event_subtype=_optional_text(data.get("event_subtype")) or "entry",
            category=category,
            reason_code=reason_code or ReasonCode.EXEC_ENTRY_FILLED,
        )

    if event_name == RuntimeEventName.EXIT_FILLED:
        raw_exit_kind = _require_str(data, "exit_kind")
        return ExitFilledContext(
            **common,
            trade_id=_require_str(data, "trade_id"),
            wallet_correlation_id=_require_str(data, "wallet_correlation_id"),
            side=_require_str(data, "side"),
            qty=_require_numeric(data, "qty"),
            price=_require_numeric(data, "price"),
            notional=_require_numeric(data, "notional"),
            exit_kind=ExitKind(raw_exit_kind),
            wallet_delta=WalletDelta.from_dict(data.get("wallet_delta") if isinstance(data.get("wallet_delta"), Mapping) else {}),
            direction=_optional_text(data.get("direction")),
            fee_paid=(float(data.get("fee_paid")) if data.get("fee_paid") is not None else None),
            realized_pnl=(float(data.get("realized_pnl")) if data.get("realized_pnl") is not None else None),
            base_currency=_optional_text(data.get("base_currency")),
            quote_currency=_optional_text(data.get("quote_currency")),
            accounting_mode=_optional_text(data.get("accounting_mode")),
            event_impact_pnl=(
                float(data.get("event_impact_pnl"))
                if data.get("event_impact_pnl") is not None
                else None
            ),
            trade_net_pnl=(float(data.get("trade_net_pnl")) if data.get("trade_net_pnl") is not None else None),
            reservation_id=_optional_text(data.get("reservation_id")),
            required_delta=_copy_mapping(data.get("required_delta")),
            event_subtype=_optional_text(data.get("event_subtype")) or str(data.get("exit_kind") or "close").lower(),
            category=category,
            reason_code=reason_code or ReasonCode.EXEC_EXIT_CLOSE,
        )

    if event_name == RuntimeEventName.WALLET_INITIALIZED:
        return WalletInitializedContext(
            **common,
            balances=dict(data.get("balances") or {}),
            source=_require_str(data, "source"),
            category=category,
            reason_code=reason_code,
        )

    if event_name == RuntimeEventName.WALLET_DEPOSITED:
        return WalletDepositedContext(
            **common,
            asset=_require_str(data, "asset"),
            amount=_require_numeric(data, "amount"),
            category=category,
            reason_code=reason_code,
        )

    if event_name == RuntimeEventName.RUNTIME_ERROR:
        return RuntimeErrorContext(
            **common,
            exception_type=_require_str(data, "exception_type"),
            message=_require_str(data, "message"),
            location=_require_str(data, "location"),
            category=category,
            reason_code=reason_code or ReasonCode.RUNTIME_EXCEPTION,
        )

    if event_name in {RuntimeEventName.SYMBOL_DEGRADED, RuntimeEventName.SYMBOL_RECOVERED}:
        default_reason = ReasonCode.SYMBOL_DEGRADED if event_name == RuntimeEventName.SYMBOL_DEGRADED else ReasonCode.SYMBOL_RECOVERED
        return RuntimeStatusContext(
            **common,
            message=_require_str(data, "message"),
            category=category,
            reason_code=reason_code or default_reason,
        )

    raise ValueError(f"Unsupported runtime event name {event_name.value}")


def runtime_event_from_dict(payload: Mapping[str, Any]) -> RuntimeEvent:
    event_name = RuntimeEventName(str(payload.get("event_name") or ""))
    context_payload = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
    return RuntimeEvent(
        schema_version=int(payload.get("schema_version") or SCHEMA_VERSION),
        event_id=str(payload.get("event_id") or ""),
        event_ts=parse_optional_datetime(payload.get("event_ts")) or datetime.now().astimezone(),
        event_name=event_name,
        root_id=str(payload.get("root_id") or ""),
        parent_id=(_optional_text(payload.get("parent_id"))),
        correlation_id=str(payload.get("correlation_id") or ""),
        context=_runtime_context_from_dict(event_name, context_payload),
    )


def new_runtime_event(
    *,
    event_name: RuntimeEventName,
    correlation_id: str,
    context: RuntimeEventContext,
    root_id: Optional[str] = None,
    parent_id: Optional[str] = None,
    event_id: Optional[str] = None,
    event_ts: Optional[datetime] = None,
    allow_missing_parent: bool = False,
) -> RuntimeEvent:
    resolved_id = str(event_id or uuid.uuid4())
    resolved_event_ts = normalize_utc_datetime(event_ts or datetime.now().astimezone())

    if event_name == RuntimeEventName.SIGNAL_EMITTED:
        resolved_root_id = resolved_id
        resolved_parent_id = None
    else:
        resolved_root_id = str(root_id or "").strip()
        resolved_parent_id = str(parent_id).strip() if parent_id is not None else None
        if not resolved_root_id and event_name in {
            RuntimeEventName.WALLET_INITIALIZED,
            RuntimeEventName.WALLET_DEPOSITED,
            RuntimeEventName.RUNTIME_ERROR,
            RuntimeEventName.SYMBOL_DEGRADED,
            RuntimeEventName.SYMBOL_RECOVERED,
        }:
            resolved_root_id = resolved_id
            resolved_parent_id = None
        if allow_missing_parent and not resolved_root_id:
            resolved_root_id = resolved_id
            resolved_parent_id = None
        if not resolved_root_id:
            raise ValueError(f"root_id is required for {event_name.value}")

    if event_name in {
        RuntimeEventName.DECISION_ACCEPTED,
        RuntimeEventName.DECISION_REJECTED,
        RuntimeEventName.ENTRY_FILLED,
        RuntimeEventName.EXIT_FILLED,
    } and not resolved_parent_id and not allow_missing_parent:
        raise ValueError(f"parent_id is required for {event_name.value}")

    return RuntimeEvent(
        schema_version=SCHEMA_VERSION,
        event_id=resolved_id,
        event_ts=resolved_event_ts,
        event_name=event_name,
        root_id=resolved_root_id,
        parent_id=resolved_parent_id,
        correlation_id=str(correlation_id),
        context=context,
    )


def decision_trace_entry_from_runtime_event(event: RuntimeEvent) -> Optional[Dict[str, Any]]:
    if event.event_name in {
        RuntimeEventName.WALLET_INITIALIZED,
        RuntimeEventName.WALLET_DEPOSITED,
    }:
        return None
    serialized_context = dict(event.context.to_dict())
    event_subtype = serialized_context.get("event_subtype")
    if event.event_name == RuntimeEventName.SIGNAL_EMITTED:
        event_subtype = "strategy_signal"
    elif event.event_name == RuntimeEventName.DECISION_ACCEPTED:
        event_subtype = "signal_accepted"
    elif event.event_name == RuntimeEventName.DECISION_REJECTED:
        event_subtype = "signal_rejected"
    elif event.event_name == RuntimeEventName.ENTRY_FILLED:
        event_subtype = "entry"
    elif event.event_name == RuntimeEventName.EXIT_FILLED:
        event_subtype = str(serialized_context.get("exit_kind") or "close").lower()
    elif event.event_name == RuntimeEventName.RUNTIME_ERROR:
        event_subtype = "runtime_error"

    rejection_artifact = serialized_context.get("rejection_artifact")
    if not isinstance(rejection_artifact, Mapping):
        rejection_artifact = {}

    return {
        "event_id": event.event_id,
        "event_ts": event.serialize().get("event_ts"),
        "event_type": str(serialized_context.get("category") or _EVENT_DEFAULT_CATEGORY[event.event_name].value).lower(),
        "event_subtype": event_subtype,
        "reason_code": serialized_context.get("reason_code"),
        "parent_event_id": event.parent_id,
        "signal_id": serialized_context.get("signal_id"),
        "source_type": serialized_context.get("source_type"),
        "source_id": serialized_context.get("source_id"),
        "trade_id": serialized_context.get("trade_id"),
        "strategy_id": serialized_context.get("strategy_id"),
        "strategy_hash": serialized_context.get("strategy_hash"),
        "symbol": serialized_context.get("symbol"),
        "timeframe": serialized_context.get("timeframe"),
        "side": serialized_context.get("direction") or serialized_context.get("side"),
        "decision_id": serialized_context.get("decision_id"),
        "rule_id": serialized_context.get("rule_id"),
        "intent": serialized_context.get("intent"),
        "event_key": serialized_context.get("event_key"),
        "qty": serialized_context.get("qty"),
        "price": serialized_context.get("price"),
        "event_impact_pnl": serialized_context.get("event_impact_pnl"),
        "trade_net_pnl": serialized_context.get("trade_net_pnl"),
        "reason_detail": serialized_context.get("message"),
        "rejection_stage": rejection_artifact.get("rejection_stage"),
        "context": rejection_artifact.get("context"),
    }


__all__ = [
    "DecisionAcceptedContext",
    "DecisionRejectedContext",
    "EntryFilledContext",
    "ExitFilledContext",
    "ExitKind",
    "ReasonCode",
    "RuntimeBar",
    "RuntimeErrorContext",
    "RuntimeEvent",
    "RuntimeEventCategory",
    "RuntimeEventContext",
    "RuntimeEventContextBase",
    "RuntimeEventName",
    "RuntimeStatusContext",
    "SCHEMA_VERSION",
    "SignalEmittedContext",
    "WalletDelta",
    "WalletDepositedContext",
    "WalletInitializedContext",
    "build_correlation_id",
    "coerce_reason_code",
    "decision_trace_entry_from_runtime_event",
    "format_correlation_bar_ts",
    "new_runtime_event",
    "normalize_utc_datetime",
    "runtime_event_from_dict",
]
