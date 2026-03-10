"""Canonical runtime event contract for bot execution."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Mapping, Optional


SCHEMA_VERSION = 1


class RuntimeEventCategory(str, Enum):
    """Informational event category; event_name remains canonical taxonomy."""

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


def normalize_utc_datetime(value: datetime) -> datetime:
    """Return a timezone-aware UTC datetime with deterministic precision."""

    target = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return target.astimezone(timezone.utc)


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


@dataclass(frozen=True)
class RuntimeEvent:
    """Single canonical append-only event persisted by bot runtime."""

    schema_version: int
    event_id: str
    event_ts: datetime
    run_id: str
    bot_id: str
    strategy_id: str
    symbol: Optional[str]
    timeframe: Optional[str]
    bar_ts: Optional[datetime]
    event_name: RuntimeEventName
    category: RuntimeEventCategory
    root_id: str
    parent_id: Optional[str]
    correlation_id: str
    reason_code: Optional[ReasonCode]
    payload: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if int(self.schema_version) < 1:
            raise ValueError("schema_version must be >= 1")
        if not str(self.event_id).strip():
            raise ValueError("event_id is required")
        if not str(self.run_id).strip():
            raise ValueError("run_id is required")
        if not str(self.bot_id).strip():
            raise ValueError("bot_id is required")
        if not str(self.strategy_id).strip():
            raise ValueError("strategy_id is required")
        if not str(self.correlation_id).strip():
            raise ValueError("correlation_id is required")
        if not str(self.root_id).strip():
            raise ValueError("root_id is required")

        event_ts = self.event_ts
        if event_ts.tzinfo is None:
            object.__setattr__(self, "event_ts", event_ts.replace(tzinfo=timezone.utc))

        if self.bar_ts is not None and self.bar_ts.tzinfo is None:
            object.__setattr__(self, "bar_ts", self.bar_ts.replace(tzinfo=timezone.utc))

        _validate_payload(self)

    def serialize(self) -> Dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "event_id": self.event_id,
            "event_ts": _to_iso(self.event_ts),
            "run_id": self.run_id,
            "bot_id": self.bot_id,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "bar_ts": _to_iso(self.bar_ts),
            "event_name": self.event_name.value,
            "category": self.category.value,
            "root_id": self.root_id,
            "parent_id": self.parent_id,
            "correlation_id": self.correlation_id,
            "reason_code": self.reason_code.value if self.reason_code is not None else None,
            "payload": dict(self.payload or {}),
        }


def _to_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    target = normalize_utc_datetime(value)
    return target.isoformat().replace("+00:00", "Z")


def _parse_optional_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return normalize_utc_datetime(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return normalize_utc_datetime(parsed)


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


def runtime_event_from_dict(payload: Mapping[str, Any]) -> RuntimeEvent:
    event_name = RuntimeEventName(str(payload.get("event_name") or ""))
    category_raw = payload.get("category")
    category = RuntimeEventCategory(str(category_raw or _EVENT_DEFAULT_CATEGORY[event_name].value))
    reason = coerce_reason_code(payload.get("reason_code"))
    return RuntimeEvent(
        schema_version=int(payload.get("schema_version") or SCHEMA_VERSION),
        event_id=str(payload.get("event_id") or ""),
        event_ts=_parse_optional_ts(payload.get("event_ts")) or datetime.now(timezone.utc),
        run_id=str(payload.get("run_id") or ""),
        bot_id=str(payload.get("bot_id") or ""),
        strategy_id=str(payload.get("strategy_id") or ""),
        symbol=(str(payload.get("symbol")) if payload.get("symbol") is not None else None),
        timeframe=(str(payload.get("timeframe")) if payload.get("timeframe") is not None else None),
        bar_ts=_parse_optional_ts(payload.get("bar_ts")),
        event_name=event_name,
        category=category,
        root_id=str(payload.get("root_id") or ""),
        parent_id=(str(payload.get("parent_id")) if payload.get("parent_id") is not None else None),
        correlation_id=str(payload.get("correlation_id") or ""),
        reason_code=reason,
        payload=dict(payload.get("payload") or {}),
    )


def new_runtime_event(
    *,
    run_id: str,
    bot_id: str,
    strategy_id: str,
    symbol: Optional[str],
    timeframe: Optional[str],
    bar_ts: Optional[datetime],
    event_name: RuntimeEventName,
    correlation_id: str,
    payload: Mapping[str, Any],
    category: Optional[RuntimeEventCategory] = None,
    reason_code: Optional[ReasonCode | str] = None,
    root_id: Optional[str] = None,
    parent_id: Optional[str] = None,
    event_id: Optional[str] = None,
    event_ts: Optional[datetime] = None,
    allow_missing_parent: bool = False,
) -> RuntimeEvent:
    resolved_id = str(event_id or uuid.uuid4())
    resolved_event_ts = event_ts or datetime.now(timezone.utc)
    resolved_category = category or _EVENT_DEFAULT_CATEGORY[event_name]

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

    resolved_reason = coerce_reason_code(reason_code)
    if event_name in {RuntimeEventName.DECISION_REJECTED, RuntimeEventName.RUNTIME_ERROR} and resolved_reason is None:
        raise ValueError(f"reason_code is required for {event_name.value}")

    return RuntimeEvent(
        schema_version=SCHEMA_VERSION,
        event_id=resolved_id,
        event_ts=normalize_utc_datetime(resolved_event_ts),
        run_id=str(run_id),
        bot_id=str(bot_id),
        strategy_id=str(strategy_id),
        symbol=str(symbol) if symbol is not None else None,
        timeframe=str(timeframe) if timeframe is not None else None,
        bar_ts=normalize_utc_datetime(bar_ts) if bar_ts is not None else None,
        event_name=event_name,
        category=resolved_category,
        root_id=resolved_root_id,
        parent_id=resolved_parent_id,
        correlation_id=str(correlation_id),
        reason_code=resolved_reason,
        payload=dict(payload or {}),
    )


def _require_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"payload.{key} is required")
    text = str(value).strip()
    if not text:
        raise ValueError(f"payload.{key} is required")
    return text


def _require_numeric(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"payload.{key} is required")
    try:
        return float(value)
    except Exception as exc:
        raise ValueError(f"payload.{key} must be numeric") from exc


def _validate_wallet_delta(payload: Mapping[str, Any]) -> None:
    wallet_delta = payload.get("wallet_delta")
    if not isinstance(wallet_delta, Mapping):
        raise ValueError("payload.wallet_delta is required")
    for key in ("collateral_reserved", "collateral_released", "fee_paid"):
        value = _require_numeric(wallet_delta, key)
        if value < 0.0:
            raise ValueError(f"payload.wallet_delta.{key} must be >= 0")
    if wallet_delta.get("balance_delta") is not None:
        _require_numeric(wallet_delta, "balance_delta")

def _validate_optional_reservation_id(payload: Mapping[str, Any]) -> None:
    if "reservation_id" not in payload:
        raise ValueError("payload.reservation_id is required")
    value = payload.get("reservation_id")
    if value is None:
        return
    text = str(value).strip()
    if not text:
        raise ValueError("payload.reservation_id must be non-empty when provided")


def _validate_payload(event: RuntimeEvent) -> None:
    payload = event.payload or {}
    event_name = event.event_name
    if event_name == RuntimeEventName.SIGNAL_EMITTED:
        _require_str(payload, "signal_type")
        _require_str(payload, "direction")
        _require_numeric(payload, "signal_price")
        return

    if event_name == RuntimeEventName.DECISION_ACCEPTED:
        _require_str(payload, "decision")
        return

    if event_name == RuntimeEventName.DECISION_REJECTED:
        _require_str(payload, "decision")
        _require_str(payload, "message")
        if event.reason_code is None:
            raise ValueError("reason_code is required for DECISION_REJECTED")
        return

    if event_name == RuntimeEventName.ENTRY_FILLED:
        _require_str(payload, "trade_id")
        _require_str(payload, "correlation_id")
        _validate_optional_reservation_id(payload)
        _require_str(payload, "side")
        _require_numeric(payload, "qty")
        _require_numeric(payload, "price")
        _require_numeric(payload, "notional")
        _validate_wallet_delta(payload)
        return

    if event_name == RuntimeEventName.EXIT_FILLED:
        _require_str(payload, "trade_id")
        _require_str(payload, "correlation_id")
        _validate_optional_reservation_id(payload)
        _require_str(payload, "side")
        _require_numeric(payload, "qty")
        _require_numeric(payload, "price")
        _require_numeric(payload, "notional")
        raw_exit_kind = _require_str(payload, "exit_kind")
        ExitKind(raw_exit_kind)
        _validate_wallet_delta(payload)
        return

    if event_name == RuntimeEventName.WALLET_INITIALIZED:
        balances = payload.get("balances")
        if not isinstance(balances, Mapping) or not balances:
            raise ValueError("payload.balances is required")
        _require_str(payload, "source")
        for currency, amount in balances.items():
            if not str(currency).strip():
                raise ValueError("payload.balances keys must be non-empty")
            try:
                float_amount = float(amount)
            except Exception as exc:
                raise ValueError("payload.balances values must be numeric") from exc
            if float_amount < 0.0:
                raise ValueError("payload.balances values must be >= 0")
        return

    if event_name == RuntimeEventName.WALLET_DEPOSITED:
        _require_str(payload, "asset")
        amount = _require_numeric(payload, "amount")
        if amount < 0.0:
            raise ValueError("payload.amount must be >= 0")
        return

    if event_name == RuntimeEventName.RUNTIME_ERROR:
        _require_str(payload, "exception_type")
        _require_str(payload, "message")
        _require_str(payload, "location")
        if event.reason_code is None:
            raise ValueError("reason_code is required for RUNTIME_ERROR")
        return

    if event_name in {RuntimeEventName.SYMBOL_DEGRADED, RuntimeEventName.SYMBOL_RECOVERED}:
        _require_str(payload, "message")
        return


__all__ = [
    "SCHEMA_VERSION",
    "ExitKind",
    "ReasonCode",
    "RuntimeEvent",
    "RuntimeEventCategory",
    "RuntimeEventName",
    "build_correlation_id",
    "coerce_reason_code",
    "format_correlation_bar_ts",
    "new_runtime_event",
    "normalize_utc_datetime",
    "runtime_event_from_dict",
]
