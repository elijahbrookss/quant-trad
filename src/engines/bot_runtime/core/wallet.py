"""In-memory wallet ledger and projection utilities for backtests.

Supports both spot (full notional) and derivatives (margin-based) validation.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .execution_profile import SeriesExecutionProfile
from .margin import (
    MarginRequirement,
    MarginSessionType,
    calculate_margin_requirement,
    create_margin_calculator,
)
from .runtime_events import RuntimeEvent, RuntimeEventName

WALLET_PROJECTION_EPSILON = 1e-9
WALLET_LEDGER_EVENT_NAMES = frozenset(
    {
        "WALLET_INITIALIZED",
        "MARGIN_RESERVED",
        "MARGIN_REJECTED",
        "MARGIN_RELEASED",
        "FEE_APPLIED",
        "REALIZED_PNL_APPLIED",
        "POSITION_OPENED",
        "POSITION_CLOSED",
        "EQUITY_UPDATED",
    }
)
WALLET_LEDGER_EVENT_ORDER = {
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


@dataclass(frozen=True)
class WalletEvent:
    """Append-only wallet event for ledger auditing."""

    event_id: str
    event_type: str
    timestamp: str
    payload: Dict[str, Any]


@dataclass
class WalletState:
    """Projected wallet state derived from ledger events."""

    balances: Dict[str, float] = field(default_factory=dict)
    locked_margin: Dict[str, float] = field(default_factory=dict)
    free_collateral: Dict[str, float] = field(default_factory=dict)
    margin_positions: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class WalletLedger:
    """Append-only wallet ledger with deterministic projections."""

    def __init__(self) -> None:
        self._events: List[WalletEvent] = []

    def events(self) -> List[WalletEvent]:
        return list(self._events)

    def append(self, event_type: str, payload: Mapping[str, Any]) -> WalletEvent:
        event = WalletEvent(
            event_id=str(uuid.uuid4()),
            event_type=str(event_type),
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            payload=dict(payload or {}),
        )
        self._events.append(event)
        return event

    def deposit(self, balances: Mapping[str, float]) -> WalletEvent:
        normalized = {str(k).upper(): float(v) for k, v in (balances or {}).items()}
        return self.append("DEPOSIT", {"balances": normalized})

    def trade_fill(
        self,
        *,
        event_type: str = "TRADE_FILL",
        side: str,
        base_currency: str,
        quote_currency: str,
        qty: float,
        price: float,
        fee: float,
        notional: float,
        symbol: Optional[str] = None,
        trade_id: Optional[str] = None,
        leg_id: Optional[str] = None,
        position_direction: Optional[str] = None,
        accounting_mode: Optional[str] = None,
        realized_pnl: Optional[float] = None,
        margin_locked: Optional[float] = None,
    ) -> WalletEvent:
        return self.append(
            event_type,
            {
                "side": str(side),
                "base_currency": str(base_currency).upper(),
                "quote_currency": str(quote_currency).upper(),
                "qty": float(qty),
                "price": float(price),
                "fee": float(fee),
                "notional": float(notional),
                "symbol": symbol,
                "trade_id": trade_id,
                "leg_id": leg_id,
                "position_direction": position_direction,
                "accounting_mode": accounting_mode,
                "realized_pnl": realized_pnl,
                "margin_locked": margin_locked,
            },
        )

    def rejected(
        self, reason: str, payload: Mapping[str, Any], trade_id: Optional[str] = None, leg_id: Optional[str] = None
    ) -> WalletEvent:
        return self.append(
            "REJECTED",
            {
                "reason": reason,
                **dict(payload or {}),
                "trade_id": trade_id,
                "leg_id": leg_id,
            },
        )

    def project(self) -> WalletState:
        return project_wallet(self._events)


class LockedWalletLedger(WalletLedger):
    """Wallet ledger guarded by a lock for concurrent access."""

    def __init__(self, lock: Optional[threading.RLock] = None) -> None:
        super().__init__()
        self._lock = lock or threading.RLock()

    def events(self) -> List[WalletEvent]:
        with self._lock:
            return list(self._events)

    def append(self, event_type: str, payload: Mapping[str, Any]) -> WalletEvent:
        with self._lock:
            return super().append(event_type, payload)

    def deposit(self, balances: Mapping[str, float]) -> WalletEvent:
        with self._lock:
            return super().deposit(balances)

    def trade_fill(
        self,
        *,
        event_type: str = "TRADE_FILL",
        side: str,
        base_currency: str,
        quote_currency: str,
        qty: float,
        price: float,
        fee: float,
        notional: float,
        symbol: Optional[str] = None,
        trade_id: Optional[str] = None,
        leg_id: Optional[str] = None,
        position_direction: Optional[str] = None,
        accounting_mode: Optional[str] = None,
        realized_pnl: Optional[float] = None,
        margin_locked: Optional[float] = None,
    ) -> WalletEvent:
        with self._lock:
            return super().trade_fill(
                event_type=event_type,
                side=side,
                base_currency=base_currency,
                quote_currency=quote_currency,
                qty=qty,
                price=price,
                fee=fee,
                notional=notional,
                symbol=symbol,
                trade_id=trade_id,
                leg_id=leg_id,
                position_direction=position_direction,
                accounting_mode=accounting_mode,
                realized_pnl=realized_pnl,
                margin_locked=margin_locked,
            )

    def rejected(
        self, reason: str, payload: Mapping[str, Any], trade_id: Optional[str] = None, leg_id: Optional[str] = None
    ) -> WalletEvent:
        with self._lock:
            return super().rejected(reason, payload, trade_id=trade_id, leg_id=leg_id)

    def project(self) -> WalletState:
        with self._lock:
            return super().project()


def trace_wallet_balance(
    events: Iterable[WalletEvent],
    currency: str,
    *,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    code = str(currency).upper()
    balance = 0.0
    trace: List[Dict[str, Any]] = []
    for event in events:
        payload = event.payload or {}
        event_type = event.event_type
        delta = 0.0
        if event_type == "DEPOSIT":
            balances = payload.get("balances") or {}
            amount = balances.get(code)
            if amount is not None:
                delta = float(amount)
        elif event_type in {"TRADE_FILL", "ENTRY_FILL", "EXIT_FILL"}:
            side = str(payload.get("side") or "").lower()
            base = str(payload.get("base_currency") or "").upper()
            quote = str(payload.get("quote_currency") or "").upper()
            qty = float(payload.get("qty") or 0.0)
            notional = float(payload.get("notional") or 0.0)
            fee = float(payload.get("fee") or 0.0)
            accounting_mode = payload.get("accounting_mode")
            realized_pnl = float(payload.get("realized_pnl") or 0.0)
            if code == base:
                if accounting_mode == "margin":
                    delta = 0.0
                elif side in {"buy", "long"}:
                    delta = qty
                elif side in {"sell", "short"}:
                    delta = -qty
            elif code == quote:
                if accounting_mode == "margin":
                    delta = realized_pnl - fee
                elif side in {"buy", "long"}:
                    delta = -notional - fee
                elif side in {"sell", "short"}:
                    delta = notional - fee
        if delta != 0.0:
            balance += delta
            trace.append(
                {
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "timestamp": event.timestamp,
                    "delta": round(delta, 8),
                    "balance": round(balance, 8),
                    "symbol": payload.get("symbol"),
                    "trade_id": payload.get("trade_id"),
                    "leg_id": payload.get("leg_id"),
                    "side": payload.get("side"),
                    "base_currency": payload.get("base_currency"),
                    "quote_currency": payload.get("quote_currency"),
                    "notional": payload.get("notional"),
                    "fee": payload.get("fee"),
                    "position_direction": payload.get("position_direction"),
                    "accounting_mode": payload.get("accounting_mode"),
                    "realized_pnl": payload.get("realized_pnl"),
                }
            )
    if limit <= 0:
        return trace
    return trace[-limit:]


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _resolve_margin_model(
    *,
    instrument: Optional[Mapping[str, Any]],
    execution_profile: Optional[SeriesExecutionProfile],
):
    if execution_profile is not None:
        return (
            execution_profile.margin_calculator,
            execution_profile.margin_calc_type,
            execution_profile.instrument.instrument_type,
        )
    if instrument is None:
        return None, None, None
    calculator, calc_type = create_margin_calculator(instrument)
    instrument_type = instrument.get("instrument_type") if isinstance(instrument, Mapping) else None
    return calculator, calc_type, instrument_type


def _execution_profile_instrument_type(execution_profile: Optional[SeriesExecutionProfile]) -> Optional[str]:
    if execution_profile is None:
        return None
    return execution_profile.instrument.instrument_type


def _wallet_requirement_payload(
    requirement: MarginRequirement,
    *,
    available_quote: Optional[float] = None,
    quote: Optional[str] = None,
    qty_raw: Optional[float] = None,
    qty_final: Optional[float] = None,
    margin_calc_type: Optional[str] = None,
    margin_leg: Optional[str] = None,
    margin_rate_source_path: Optional[str] = None,
    instrument_type: Optional[str] = None,
    shortfall: Optional[float] = None,
    required_full_notional: Optional[float] = None,
) -> Dict[str, Any]:
    payload = {
        "available": available_quote,
        "available_collateral": available_quote,
        "required": requirement.total_required,
        "required_used": requirement.total_required,
        "required_full_notional": required_full_notional,
        "margin_total_required": requirement.total_required,
        "currency": quote,
        "notional": requirement.notional,
        "fee": requirement.estimated_entry_fee,
        "qty": requirement.quantity,
        "qty_raw": qty_raw if qty_raw is not None else requirement.quantity,
        "qty_final": qty_final if qty_final is not None else requirement.quantity,
        "margin_rate": requirement.margin_rate,
        "margin_method": requirement.calculation_method,
        "margin_type": requirement.calculation_method,
        "margin_session": requirement.session_type,
        "session": requirement.session_type,
        "margin_calc_type": margin_calc_type,
        "margin_leg": margin_leg,
        "margin_rate_source_path": margin_rate_source_path,
        "instrument_type": instrument_type,
        "required_margin": requirement.required_margin,
        "initial_margin": requirement.initial_margin,
        "collateral_to_lock": requirement.collateral_to_lock,
        "estimated_entry_fee": requirement.estimated_entry_fee,
        "estimated_exit_fee": requirement.estimated_exit_fee,
        "fee_buffer": requirement.fee_buffer,
        "safety_buffer": requirement.safety_buffer,
        "total_required_collateral": requirement.total_required_collateral,
        "margin_requirement": requirement.to_dict(),
    }
    if shortfall is not None:
        payload["shortfall"] = shortfall
    return payload


def _requirement_for_wallet_order(
    *,
    side: str,
    qty: float,
    notional: float,
    fee: float,
    instrument: Optional[Mapping[str, Any]],
    execution_profile: Optional[SeriesExecutionProfile],
    margin_session: Optional[MarginSessionType],
) -> Tuple[MarginRequirement, str, Optional[str]]:
    calculator, calc_type, instrument_type = _resolve_margin_model(
        instrument=instrument,
        execution_profile=execution_profile,
    )
    del calculator
    resolved_side = str(side or "").strip().lower()
    direction = "long" if resolved_side in {"buy", "long"} else "short"
    requirement = calculate_margin_requirement(
        notional=notional,
        entry_fee=fee,
        side=resolved_side,
        direction=direction,
        quantity=qty,
        price=0.0,
        contract_size=1.0,
        instrument=instrument,
        execution_profile=execution_profile,
        include_exit_fee_buffer=True,
        safety_multiplier=1.05,
        margin_session=margin_session,
        apply_safety_to_full_notional=False,
    )
    return requirement, calc_type, instrument_type or _execution_profile_instrument_type(execution_profile)


def _apply_margin_entry_lock(
    *,
    trade_id: Optional[str],
    quote_currency: str,
    qty: float,
    margin_locked: float,
    locked_margin: Dict[str, float],
    margin_positions: Dict[str, Dict[str, float]],
) -> None:
    locked = max(_coerce_float(margin_locked, 0.0), 0.0)
    if locked <= 0.0:
        return
    quote = str(quote_currency or "").upper()
    if not quote:
        return
    locked_margin[quote] = locked_margin.get(quote, 0.0) + locked
    trade_key = str(trade_id or "").strip()
    if not trade_key:
        return
    current = dict(margin_positions.get(trade_key) or {})
    current_qty = max(_coerce_float(current.get("open_qty"), 0.0), 0.0)
    current_locked = max(_coerce_float(current.get("locked_margin"), 0.0), 0.0)
    current_currency = str(current.get("currency") or quote).upper()
    margin_positions[trade_key] = {
        "currency": current_currency,
        "open_qty": current_qty + max(_coerce_float(qty, 0.0), 0.0),
        "locked_margin": current_locked + locked,
    }


def _apply_margin_exit_release(
    *,
    trade_id: Optional[str],
    qty: float,
    explicit_release: Optional[float],
    locked_margin: Dict[str, float],
    margin_positions: Dict[str, Dict[str, float]],
) -> float:
    trade_key = str(trade_id or "").strip()
    if not trade_key:
        return 0.0
    current = dict(margin_positions.get(trade_key) or {})
    if not current:
        return 0.0
    currency = str(current.get("currency") or "").upper()
    open_qty = max(_coerce_float(current.get("open_qty"), 0.0), 0.0)
    locked_total = max(_coerce_float(current.get("locked_margin"), 0.0), 0.0)
    close_qty = max(_coerce_float(qty, 0.0), 0.0)
    if open_qty <= 0.0 or locked_total <= 0.0 or close_qty <= 0.0:
        return 0.0
    release_ratio = min(close_qty / open_qty, 1.0)
    margin_release = locked_total * release_ratio
    if explicit_release is not None:
        explicit = max(float(explicit_release), 0.0)
        if explicit - locked_total > WALLET_PROJECTION_EPSILON:
            raise ValueError(
                f"wallet_projection_invariant: release exceeds reserve | trade_id={trade_key} release={explicit} reserve={locked_total}"
            )
        margin_release = explicit
    remaining_qty = max(open_qty - close_qty, 0.0)
    remaining_locked = max(locked_total - margin_release, 0.0)
    if remaining_qty <= 1e-12 or remaining_locked <= 1e-12:
        margin_release = locked_total
        margin_positions.pop(trade_key, None)
    else:
        margin_positions[trade_key] = {
            "currency": currency,
            "open_qty": remaining_qty,
            "locked_margin": remaining_locked,
        }
    if currency:
        next_locked = locked_margin.get(currency, 0.0) - margin_release
        if next_locked < -WALLET_PROJECTION_EPSILON:
            raise ValueError(
                f"wallet_projection_invariant: locked margin negative | currency={currency} value={next_locked}"
            )
        if abs(next_locked) <= WALLET_PROJECTION_EPSILON:
            locked_margin.pop(currency, None)
        else:
            locked_margin[currency] = next_locked
    return margin_release


def _wallet_projection_event_id(event: Any) -> Optional[str]:
    if isinstance(event, WalletEvent):
        return str(event.event_id or "")
    if isinstance(event, RuntimeEvent):
        return str(event.event_id or "")
    if isinstance(event, Mapping):
        raw = event.get("event_id")
        if raw is None and isinstance(event.get("payload"), Mapping):
            raw = event.get("payload", {}).get("event_id")
        if raw is None:
            return None
        text = str(raw).strip()
        return text or None
    return None


def _normalize_wallet_projection_event(event: Any) -> Tuple[Optional[str], Dict[str, Any]]:
    if isinstance(event, WalletEvent):
        raw_name = str(event.event_type or "").strip()
        payload = dict(event.payload or {})
        if raw_name in {RuntimeEventName.WALLET_INITIALIZED.value, "INITIALIZE"}:
            return "INITIALIZE", payload
        if raw_name == RuntimeEventName.WALLET_DEPOSITED.value:
            return "DEPOSIT_DELTA", payload
        return raw_name, payload
    if isinstance(event, RuntimeEvent):
        payload = dict(event.context.to_dict())
        name = event.event_name
        if name == RuntimeEventName.WALLET_INITIALIZED:
            return "INITIALIZE", payload
        if name == RuntimeEventName.WALLET_DEPOSITED:
            return "DEPOSIT_DELTA", payload
        if name == RuntimeEventName.ENTRY_FILLED:
            return "ENTRY_FILL", payload
        if name == RuntimeEventName.EXIT_FILLED:
            return "EXIT_FILL", payload
        return None, payload
    if isinstance(event, Mapping):
        payload = event.get("context")
        if isinstance(payload, Mapping):
            payload = dict(payload)
        else:
            payload = dict(event.get("payload") or {})
            nested = payload.get("context")
            if isinstance(nested, Mapping):
                payload = dict(nested)
        raw_name = str(event.get("event_name") or event.get("event_type") or "").strip()
        if raw_name in {RuntimeEventName.WALLET_INITIALIZED.value, "INITIALIZE"}:
            return "INITIALIZE", payload
        if raw_name == "DEPOSIT":
            return "DEPOSIT", payload
        if raw_name == RuntimeEventName.WALLET_DEPOSITED.value:
            return "DEPOSIT_DELTA", payload
        if raw_name in {RuntimeEventName.ENTRY_FILLED.value, "ENTRY_FILL"}:
            return "ENTRY_FILL", payload
        if raw_name in {RuntimeEventName.EXIT_FILLED.value, "EXIT_FILL"}:
            return "EXIT_FILL", payload
        if raw_name in WALLET_LEDGER_EVENT_NAMES:
            return raw_name, payload
        if raw_name in {"TRADE_FILL", "REJECTED"}:
            return raw_name, payload
        return None, payload
    return None, {}


def _wallet_ledger_currency(payload: Mapping[str, Any]) -> str:
    for key in ("currency", "quote_currency", "asset"):
        value = str(payload.get(key) or "").strip().upper()
        if value:
            return value
    margin_requirement = payload.get("margin_requirement")
    if isinstance(margin_requirement, Mapping):
        value = str(margin_requirement.get("currency") or "").strip().upper()
        if value:
            return value
    wallet_delta = payload.get("wallet_delta")
    if isinstance(wallet_delta, Mapping):
        value = str(wallet_delta.get("currency") or "").strip().upper()
        if value:
            return value
    return "USD"


def _wallet_ledger_number(payload: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    value = payload.get(key)
    if value in (None, ""):
        return float(default)
    return _coerce_float(value, default)


def _wallet_ledger_balance_delta(payload: Mapping[str, Any]) -> float:
    before = payload.get("balance_before")
    after = payload.get("balance_after")
    if before not in (None, "") and after not in (None, ""):
        return _coerce_float(after, 0.0) - _coerce_float(before, 0.0)
    if payload.get("balance_delta") not in (None, ""):
        return _coerce_float(payload.get("balance_delta"), 0.0)
    return 0.0


def _wallet_ledger_balances(value: Any) -> Dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    balances = value.get("balances") if "balances" in value else value
    if not isinstance(balances, Mapping):
        return {}
    normalized: Dict[str, float] = {}
    for currency, amount in balances.items():
        code = str(currency or "").strip().upper()
        if not code or amount in (None, ""):
            continue
        normalized[code] = _coerce_float(amount, 0.0)
    return normalized


def _wallet_initial_balances(payload: Mapping[str, Any]) -> Dict[str, float]:
    for candidate in (
        payload.get("balances"),
        payload.get("wallet_after"),
        payload.get("wallet_snapshot"),
        payload.get("wallet_before"),
    ):
        balances = _wallet_ledger_balances(candidate)
        if balances:
            return balances
    balance_after = payload.get("balance_after")
    if balance_after not in (None, ""):
        return {_wallet_ledger_currency(payload): _coerce_float(balance_after, 0.0)}
    raise ValueError(
        "wallet_projection_initialization_invalid: WALLET_INITIALIZED missing balances or balance_after"
    )


def _validate_wallet_state_invariants(
    *,
    balances: Mapping[str, float],
    locked_margin: Mapping[str, float],
    free_collateral: Mapping[str, float],
    margin_positions: Mapping[str, Mapping[str, Any]],
) -> None:
    for currency, value in locked_margin.items():
        if float(value) < -WALLET_PROJECTION_EPSILON:
            raise ValueError(
                f"wallet_projection_invariant: locked margin negative | currency={currency} value={value}"
            )
    for trade_id, payload in margin_positions.items():
        open_qty = float(payload.get("open_qty") or 0.0)
        locked_value = float(payload.get("locked_margin") or 0.0)
        if open_qty < -WALLET_PROJECTION_EPSILON:
            raise ValueError(
                f"wallet_projection_invariant: open qty negative | trade_id={trade_id} open_qty={open_qty}"
            )
        if locked_value < -WALLET_PROJECTION_EPSILON:
            raise ValueError(
                f"wallet_projection_invariant: trade lock negative | trade_id={trade_id} locked_margin={locked_value}"
            )
    currencies = set(balances.keys()) | set(locked_margin.keys()) | set(free_collateral.keys())
    for currency in currencies:
        expected = float(balances.get(currency, 0.0)) - float(locked_margin.get(currency, 0.0))
        observed = float(free_collateral.get(currency, 0.0))
        if abs(expected - observed) > 1e-9:
            raise ValueError(
                "wallet_projection_invariant: free collateral mismatch | "
                f"currency={currency} expected={expected} observed={observed}"
            )


def project_wallet(events: Iterable[Any]) -> WalletState:
    balances: Dict[str, float] = {}
    locked_margin: Dict[str, float] = {}
    margin_positions: Dict[str, Dict[str, float]] = {}
    seen_event_ids: set[str] = set()
    initialized = False
    mutated_after_initialization = False
    for event in events:
        event_id = _wallet_projection_event_id(event)
        if event_id:
            if event_id in seen_event_ids:
                continue
            seen_event_ids.add(event_id)
        event_type, payload = _normalize_wallet_projection_event(event)
        if not event_type:
            continue
        if event_type == "INITIALIZE":
            initial_balances = _wallet_initial_balances(payload)
            if initialized:
                if initial_balances != balances:
                    raise ValueError(
                        "wallet_projection_initialization_invalid: duplicate WALLET_INITIALIZED changed balances"
                    )
                if mutated_after_initialization or locked_margin or margin_positions:
                    raise ValueError(
                        "wallet_projection_initialization_invalid: duplicate WALLET_INITIALIZED after wallet activity"
                    )
                continue
            balances = initial_balances
            locked_margin = {}
            margin_positions = {}
            initialized = True
        elif event_type == "DEPOSIT":
            for currency, amount in (payload.get("balances") or {}).items():
                code = str(currency).upper()
                balances[code] = balances.get(code, 0.0) + float(amount)
            mutated_after_initialization = True
        elif event_type == "DEPOSIT_DELTA":
            code = str(payload.get("asset") or "").upper()
            if not code:
                continue
            amount = float(payload.get("amount") or 0.0)
            balances[code] = balances.get(code, 0.0) + amount
            mutated_after_initialization = True
        elif event_type == "MARGIN_RESERVED":
            currency = _wallet_ledger_currency(payload)
            margin_required = _wallet_ledger_number(
                payload,
                "margin_required",
                _wallet_ledger_number(payload, "collateral_reserved", 0.0),
            )
            _apply_margin_entry_lock(
                trade_id=str(payload.get("trade_id")) if payload.get("trade_id") else None,
                quote_currency=currency,
                qty=_wallet_ledger_number(payload, "qty", 0.0),
                margin_locked=margin_required,
                locked_margin=locked_margin,
                margin_positions=margin_positions,
            )
            mutated_after_initialization = True
        elif event_type == "MARGIN_RELEASED":
            _apply_margin_exit_release(
                trade_id=str(payload.get("trade_id")) if payload.get("trade_id") else None,
                qty=_wallet_ledger_number(payload, "qty", 0.0),
                explicit_release=_wallet_ledger_number(payload, "margin_required", 0.0),
                locked_margin=locked_margin,
                margin_positions=margin_positions,
            )
            mutated_after_initialization = True
        elif event_type in {"FEE_APPLIED", "REALIZED_PNL_APPLIED"}:
            currency = _wallet_ledger_currency(payload)
            balances[currency] = balances.get(currency, 0.0) + _wallet_ledger_balance_delta(payload)
            mutated_after_initialization = True
        elif event_type in {
            "MARGIN_REJECTED",
            "POSITION_OPENED",
            "POSITION_CLOSED",
            "EQUITY_UPDATED",
        }:
            continue
        elif event_type in {"TRADE_FILL", "ENTRY_FILL", "EXIT_FILL"}:
            side = str(payload.get("side") or "").lower()
            base = str(payload.get("base_currency") or "").upper()
            quote = str(payload.get("quote_currency") or "").upper()
            qty = float(payload.get("qty") or 0.0)
            notional = float(payload.get("notional") or 0.0)
            wallet_delta = payload.get("wallet_delta") if isinstance(payload.get("wallet_delta"), Mapping) else {}
            fee = float(
                wallet_delta.get("fee_paid")
                if isinstance(wallet_delta, Mapping) and wallet_delta.get("fee_paid") is not None
                else payload.get("fee", 0.0)
            )
            accounting_mode = payload.get("accounting_mode")
            if accounting_mode == "margin":
                if isinstance(wallet_delta, Mapping) and wallet_delta.get("balance_delta") is not None:
                    balance_delta = float(wallet_delta.get("balance_delta") or 0.0)
                else:
                    realized_pnl = float(payload.get("realized_pnl") or 0.0)
                    balance_delta = realized_pnl - fee
                balances[quote] = balances.get(quote, 0.0) + balance_delta
                trade_id = payload.get("trade_id")
                if event_type == "ENTRY_FILL":
                    margin_locked: Optional[float]
                    margin_reserved_raw = wallet_delta.get("collateral_reserved") if isinstance(wallet_delta, Mapping) else None
                    try:
                        margin_locked = float(margin_reserved_raw) if margin_reserved_raw is not None else None
                    except Exception:
                        margin_locked = None
                    if margin_locked is None:
                        margin_locked = _coerce_float(
                            payload.get("margin_locked")
                            or payload.get("reserved_amount")
                            or payload.get("required_used")
                            or payload.get("margin_total_required"),
                            0.0,
                        )
                    _apply_margin_entry_lock(
                        trade_id=str(trade_id) if trade_id else None,
                        quote_currency=quote,
                        qty=qty,
                        margin_locked=float(margin_locked or 0.0),
                        locked_margin=locked_margin,
                        margin_positions=margin_positions,
                    )
                elif event_type == "EXIT_FILL":
                    release_raw = wallet_delta.get("collateral_released") if isinstance(wallet_delta, Mapping) else None
                    try:
                        release_value = float(release_raw) if release_raw is not None else None
                    except Exception:
                        release_value = None
                    _apply_margin_exit_release(
                        trade_id=str(trade_id) if trade_id else None,
                        qty=qty,
                        explicit_release=release_value,
                        locked_margin=locked_margin,
                        margin_positions=margin_positions,
                    )
            elif side in {"buy", "long"}:
                balances[base] = balances.get(base, 0.0) + qty
                balances[quote] = balances.get(quote, 0.0) - notional - fee
            elif side in {"sell", "short"}:
                balances[base] = balances.get(base, 0.0) - qty
                balances[quote] = balances.get(quote, 0.0) + notional - fee
            mutated_after_initialization = True
        elif event_type == "REJECTED":
            continue
    free_collateral: Dict[str, float] = {}
    currencies = set(balances.keys()) | set(locked_margin.keys())
    for currency in currencies:
        free_value = balances.get(currency, 0.0) - locked_margin.get(currency, 0.0)
        free_collateral[currency] = 0.0 if abs(free_value) <= WALLET_PROJECTION_EPSILON else free_value
    _validate_wallet_state_invariants(
        balances=balances,
        locked_margin=locked_margin,
        free_collateral=free_collateral,
        margin_positions=margin_positions,
    )
    return WalletState(
        balances=balances,
        locked_margin=locked_margin,
        free_collateral=free_collateral,
        margin_positions=margin_positions,
    )


def project_wallet_from_events(events: Iterable[RuntimeEvent | Mapping[str, Any]]) -> WalletState:
    """Project wallet state from canonical runtime events."""

    return project_wallet(events)


def _wallet_state_as_payload(state: WalletState) -> Dict[str, Any]:
    return {
        "balances": dict(getattr(state, "balances", {}) or {}),
        "locked_margin": dict(getattr(state, "locked_margin", {}) or {}),
        "free_collateral": dict(getattr(state, "free_collateral", {}) or {}),
        "margin_positions": dict(getattr(state, "margin_positions", {}) or {}),
    }


def _wallet_event_context_for_validation(event: Any) -> Tuple[Optional[str], Dict[str, Any]]:
    event_type, payload = _normalize_wallet_projection_event(event)
    if event_type == "INITIALIZE":
        event_type = "WALLET_INITIALIZED"
    return event_type, dict(payload or {})


def _wallet_validation_event_identity(event: Any, event_type: Optional[str], payload: Mapping[str, Any]) -> Dict[str, Any]:
    run_seq: Any = payload.get("run_seq")
    if run_seq in (None, "") and isinstance(event, Mapping):
        run_seq = event.get("run_seq") or event.get("seq")
        raw_payload = event.get("payload")
        if run_seq in (None, "") and isinstance(raw_payload, Mapping):
            run_seq = raw_payload.get("run_seq") or raw_payload.get("seq")
    return {
        "event_id": _wallet_projection_event_id(event),
        "event_name": event_type,
        "run_seq": run_seq,
        "bar_time": payload.get("bar_time") or payload.get("known_at"),
        "trade_id": payload.get("trade_id"),
        "decision_id": payload.get("decision_id"),
    }


def _wallet_optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _wallet_event_order_from_id(value: Any) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) >= 2:
        return _wallet_optional_int(parts[-2])
    return None


def _wallet_ledger_sort_key(index: int, event: RuntimeEvent | Mapping[str, Any]) -> Tuple[int, int, int, int]:
    event_type, payload = _wallet_event_context_for_validation(event)
    if event_type not in WALLET_LEDGER_EVENT_NAMES:
        return (2, index, 0, index)
    wallet_commit_seq = _wallet_optional_int(payload.get("wallet_commit_seq"))
    if wallet_commit_seq is None:
        raise ValueError(
            "wallet_ledger_order_invalid: wallet_commit_seq is required "
            f"| event_name={event_type} | event_id={_wallet_projection_event_id(event)}"
        )
    event_id = _wallet_projection_event_id(event)
    wallet_event_order = int(WALLET_LEDGER_EVENT_ORDER.get(str(event_type or ""), 99))
    for candidate in (
        _wallet_optional_int(payload.get("wallet_event_order")),
        _wallet_event_order_from_id(payload.get("wallet_event_id")),
        _wallet_event_order_from_id(event_id),
    ):
        if candidate is not None:
            wallet_event_order = candidate
            break
    return (0, wallet_commit_seq, wallet_event_order, index)


def canonical_wallet_ledger_events(
    events: Iterable[RuntimeEvent | Mapping[str, Any]],
) -> List[RuntimeEvent | Mapping[str, Any]]:
    """Return wallet ledger events in wallet commit order plus event-local wallet order."""

    return [
        event
        for _key, event in sorted(
            ((_wallet_ledger_sort_key(index, event), event) for index, event in enumerate(list(events))),
            key=lambda item: item[0],
        )
    ]


def _wallet_state_amount(state: WalletState, section: str, currency: str) -> Optional[float]:
    values = getattr(state, section, None)
    if not isinstance(values, Mapping):
        return None
    code = str(currency or "").strip().upper()
    if not code or code not in values:
        return 0.0 if section in {"locked_margin", "free_collateral"} else None
    return _coerce_float(values.get(code), 0.0)


def _wallet_snapshot_amount(value: Any, section: str, currency: str) -> Optional[float]:
    if not isinstance(value, Mapping):
        return None
    values = value.get(section)
    if not isinstance(values, Mapping):
        return None
    code = str(currency or "").strip().upper()
    if not code:
        return None
    if code not in values:
        return 0.0 if section in {"locked_margin", "free_collateral"} else None
    return _coerce_float(values.get(code), 0.0)


def _wallet_issue_event_summary(event: RuntimeEvent | Mapping[str, Any]) -> Dict[str, Any]:
    event_type, payload = _wallet_event_context_for_validation(event)
    return _wallet_validation_event_identity(event, event_type, payload) | {
        "wallet_commit_seq": payload.get("wallet_commit_seq"),
        "wallet_eval_seq": payload.get("wallet_eval_seq"),
        "source_run_seq": payload.get("source_run_seq"),
        "wallet_event_order": payload.get("wallet_event_order"),
    }


def _wallet_issue_prior_events(
    events: Sequence[RuntimeEvent | Mapping[str, Any]],
    index: int,
    *,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    start = max(index - limit, 0)
    return [_wallet_issue_event_summary(event) for event in events[start:index]]


def _wallet_issue_same_bar_group(
    events: Sequence[RuntimeEvent | Mapping[str, Any]],
    index: int,
    payload: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    bar_time = str(payload.get("bar_time") or payload.get("known_at") or "").strip()
    if not bar_time:
        return []
    group: List[Dict[str, Any]] = []
    for event in events:
        event_type, event_payload = _wallet_event_context_for_validation(event)
        if event_type not in WALLET_LEDGER_EVENT_NAMES:
            continue
        event_bar = str(event_payload.get("bar_time") or event_payload.get("known_at") or "").strip()
        if event_bar == bar_time:
            group.append(_wallet_issue_event_summary(event))
    return group[:25]


def _wallet_first_state_issue(events: Iterable[RuntimeEvent | Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    prefix: List[RuntimeEvent | Mapping[str, Any]] = []
    raw_events = list(events)
    for event in raw_events:
        event_type, payload = _wallet_event_context_for_validation(event)
        if event_type not in WALLET_LEDGER_EVENT_NAMES:
            continue
        if _wallet_optional_int(payload.get("wallet_commit_seq")) is None:
            return {
                **_wallet_validation_event_identity(event, event_type, payload),
                "code": "wallet_ledger_order_missing",
                "reason": "missing_wallet_commit_seq",
                "wallet_commit_seq": payload.get("wallet_commit_seq"),
                "wallet_event_order": payload.get("wallet_event_order"),
            }
    ordered_events = canonical_wallet_ledger_events(raw_events)
    for event_index, event in enumerate(ordered_events):
        event_type, payload = _wallet_event_context_for_validation(event)
        if event_type not in WALLET_LEDGER_EVENT_NAMES:
            prefix.append(event)
            continue
        currency = _wallet_ledger_currency(payload)
        identity = _wallet_validation_event_identity(event, event_type, payload)
        wallet_after = payload.get("wallet_after") if isinstance(payload.get("wallet_after"), Mapping) else None
        wallet_before = payload.get("wallet_before") if isinstance(payload.get("wallet_before"), Mapping) else None
        if event_type == "WALLET_INITIALIZED":
            if wallet_after is None:
                return {
                    **identity,
                    "code": "wallet_ledger_state_malformed",
                    "reason": "missing_wallet_after",
                    "prior_wallet_events": _wallet_issue_prior_events(ordered_events, event_index),
                    "same_bar_operation_group": _wallet_issue_same_bar_group(ordered_events, event_index, payload),
                }
        else:
            missing = []
            for key in ("balance_before", "balance_after"):
                if payload.get(key) in (None, ""):
                    missing.append(key)
            if wallet_before is None:
                missing.append("wallet_before")
            if wallet_after is None:
                missing.append("wallet_after")
            if missing:
                return {
                    **identity,
                    "code": "wallet_ledger_state_malformed",
                    "reason": "missing_absolute_wallet_state",
                    "missing_fields": missing,
                    "prior_wallet_events": _wallet_issue_prior_events(ordered_events, event_index),
                    "same_bar_operation_group": _wallet_issue_same_bar_group(ordered_events, event_index, payload),
                }

        before_state = project_wallet_from_events(prefix)
        prefix.append(event)
        after_state = project_wallet_from_events(prefix)
        if wallet_before is not None and event_type != "WALLET_INITIALIZED":
            expected_before = _wallet_state_amount(before_state, "balances", currency)
            observed_before = _wallet_snapshot_amount(wallet_before, "balances", currency)
            if expected_before is not None and observed_before is not None and abs(expected_before - observed_before) > 0.01:
                return {
                    **identity,
                    "code": "wallet_ledger_state_mismatch",
                    "reason": "wallet_before_balance_mismatch",
                    "currency": currency,
                    "persisted": observed_before,
                    "replayed": expected_before,
                    "prior_wallet_events": _wallet_issue_prior_events(ordered_events, event_index),
                    "same_bar_operation_group": _wallet_issue_same_bar_group(ordered_events, event_index, payload),
                }
        if wallet_after is not None:
            expected_after = _wallet_state_amount(after_state, "balances", currency)
            observed_after = _wallet_snapshot_amount(wallet_after, "balances", currency)
            scalar_after = payload.get("balance_after")
            scalar_after_float = None if scalar_after in (None, "") else _coerce_float(scalar_after, 0.0)
            if expected_after is not None and observed_after is not None and abs(expected_after - observed_after) > 0.01:
                return {
                    **identity,
                    "code": "wallet_ledger_state_mismatch",
                    "reason": "wallet_after_balance_mismatch",
                    "currency": currency,
                    "persisted": observed_after,
                    "replayed": expected_after,
                    "prior_wallet_events": _wallet_issue_prior_events(ordered_events, event_index),
                    "same_bar_operation_group": _wallet_issue_same_bar_group(ordered_events, event_index, payload),
                }
            if expected_after is not None and scalar_after_float is not None and abs(expected_after - scalar_after_float) > 0.01:
                return {
                    **identity,
                    "code": "wallet_ledger_state_mismatch",
                    "reason": "balance_after_mismatch",
                    "currency": currency,
                    "persisted": scalar_after_float,
                    "replayed": expected_after,
                    "prior_wallet_events": _wallet_issue_prior_events(ordered_events, event_index),
                    "same_bar_operation_group": _wallet_issue_same_bar_group(ordered_events, event_index, payload),
                }
    return None


def first_wallet_ledger_state_issue(events: Iterable[RuntimeEvent | Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return the first malformed or inconsistent absolute wallet ledger state."""

    return _wallet_first_state_issue(list(events))


def validate_wallet_ledger_state(events: Iterable[RuntimeEvent | Mapping[str, Any]]) -> None:
    """Fail loudly if wallet ledger events are not replayable absolute state events."""

    issue = first_wallet_ledger_state_issue(events)
    if issue:
        raise ValueError(
            "wallet_ledger_state_invalid: "
            f"{issue.get('code')} | reason={issue.get('reason')} | "
            f"event_name={issue.get('event_name')} | run_seq={issue.get('run_seq')} | "
            f"event_id={issue.get('event_id')}"
        )


def wallet_can_apply(
    *,
    state: WalletState,
    side: str,
    base_currency: str,
    quote_currency: str,
    qty: float,
    qty_raw: Optional[float] = None,
    qty_final: Optional[float] = None,
    notional: float,
    fee: float,
    short_requires_borrow: bool,
    instrument: Optional[Mapping[str, Any]] = None,
    execution_profile: Optional[SeriesExecutionProfile] = None,
    margin_session: Optional[MarginSessionType] = None,
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """Validate if wallet can support the proposed trade.

    Args:
        state: Current wallet state
        side: Trade side ("buy", "long", "sell", "short")
        base_currency: Base currency code
        quote_currency: Quote currency code
        qty: Trade quantity
        notional: Trade notional value
        fee: Expected fee
        short_requires_borrow: If True, shorts require borrowing base asset (spot-style)
        instrument: Optional instrument configuration for margin-based validation.
                   If provided and instrument_type is "future"/"swap", uses margin rates.
        margin_session: Session type for margin rate selection. Defaults to OVERNIGHT (conservative).

    Returns:
        Tuple of (allowed: bool, reason: Optional[str], details: Dict)

    Error Reasons:
        - WALLET_INSUFFICIENT_CASH: Not enough quote currency for buy/long
        - WALLET_INSUFFICIENT_QTY: Not enough base currency for borrow-based short
        - WALLET_INSUFFICIENT_CASH_FOR_SHORT_COVER: Not enough for spot-style short cover
        - WALLET_INSUFFICIENT_MARGIN: Not enough for margin-based futures positions
        - WALLET_INSTRUMENT_MISCONFIGURED: Instrument missing required margin configuration
    """
    base = str(base_currency).upper()
    quote = str(quote_currency).upper()
    balances = state.balances
    available_base = balances.get(base, 0.0)
    available_quote = state.free_collateral.get(quote, balances.get(quote, 0.0))
    resolved_qty_raw, resolved_qty_final = _resolve_qty_fields(qty, qty_raw, qty_final)

    # BUY/LONG: Use margin-based requirement when instrument supports it
    if side in {"buy", "long"}:
        return _validate_long_cash_requirement(
            available_quote=available_quote,
            quote=quote,
            notional=notional,
            fee=fee,
            qty=qty,
            qty_raw=resolved_qty_raw,
            qty_final=resolved_qty_final,
            instrument=instrument,
            execution_profile=execution_profile,
            margin_session=margin_session,
        )

    # SHORT with borrow requirement (spot-style): need base currency
    if short_requires_borrow:
        required_qty = float(qty)
        if available_base + 1e-12 < required_qty:
            return (
                False,
                "WALLET_INSUFFICIENT_QTY",
                {
                    "available": available_base,
                    "required": required_qty,
                    "currency": base,
                    "qty": qty,
                    "qty_raw": resolved_qty_raw,
                    "qty_final": resolved_qty_final,
                },
            )
        return True, None, {}

    # SHORT without borrow (cash-settled derivatives or spot)
    if side in {"sell", "short"}:
        return _validate_short_cash_requirement(
            available_quote=available_quote,
            quote=quote,
            notional=notional,
            fee=fee,
            qty=qty,
            qty_raw=resolved_qty_raw,
            qty_final=resolved_qty_final,
            instrument=instrument,
            execution_profile=execution_profile,
            margin_session=margin_session,
        )

    return True, None, {}


def wallet_required_reservation(
    *,
    side: str,
    base_currency: str,
    quote_currency: str,
    qty: float,
    notional: float,
    fee: float,
    short_requires_borrow: bool,
    instrument: Optional[Mapping[str, Any]] = None,
    execution_profile: Optional[SeriesExecutionProfile] = None,
    margin_session: Optional[MarginSessionType] = None,
) -> Tuple[str, float]:
    """Resolve the balance hold needed to keep can_apply/apply_fill atomic.

    Returns:
        Tuple of (currency_code, amount_to_reserve).
    """
    currency, amount, _details = wallet_required_reservation_details(
        side=side,
        base_currency=base_currency,
        quote_currency=quote_currency,
        qty=qty,
        notional=notional,
        fee=fee,
        short_requires_borrow=short_requires_borrow,
        instrument=instrument,
        execution_profile=execution_profile,
        margin_session=margin_session,
    )
    return currency, amount


def wallet_required_reservation_details(
    *,
    side: str,
    base_currency: str,
    quote_currency: str,
    qty: float,
    notional: float,
    fee: float,
    short_requires_borrow: bool,
    instrument: Optional[Mapping[str, Any]] = None,
    execution_profile: Optional[SeriesExecutionProfile] = None,
    margin_session: Optional[MarginSessionType] = None,
) -> Tuple[str, float, Dict[str, Any]]:
    """Resolve reservation hold plus its canonical requirement breakdown."""

    normalized_side = str(side or "").lower()
    base = str(base_currency or "").upper()
    quote = str(quote_currency or "").upper()
    required_full_long = float(notional) + float(fee)
    required_full_short = float(notional) + float(fee) * 2.0

    if normalized_side in {"buy", "long"}:
        required = required_full_long
        details: Dict[str, Any] = {
            "currency": quote,
            "collateral_reserved": max(float(notional), 0.0),
            "estimated_entry_fee": max(float(fee), 0.0),
            "estimated_exit_fee": 0.0,
            "fee_buffer": max(float(fee), 0.0),
            "total_required_collateral": max(required, 0.0),
        }
        if instrument is not None or execution_profile is not None:
            try:
                requirement, calc_type, _instrument_type = _requirement_for_wallet_order(
                    side=normalized_side,
                    qty=qty,
                    notional=notional,
                    fee=fee,
                    instrument=instrument,
                    execution_profile=execution_profile,
                    margin_session=margin_session,
                )
                if calc_type == "margin":
                    required = float(requirement.total_required)
                    details = {
                        **requirement.to_dict(),
                        "currency": quote,
                        "collateral_reserved": float(requirement.collateral_to_lock),
                    }
            except ValueError:
                # Let can_apply report instrument misconfiguration. Use conservative hold.
                required = required_full_long
        return quote, max(required, 0.0), details

    if short_requires_borrow:
        return base, max(float(qty), 0.0), {
            "currency": base,
            "qty_reserved": max(float(qty), 0.0),
            "total_required_collateral": max(float(qty), 0.0),
        }

    if normalized_side in {"sell", "short"}:
        required = required_full_short
        details = {
            "currency": quote,
            "collateral_reserved": max(float(notional), 0.0),
            "estimated_entry_fee": max(float(fee), 0.0),
            "estimated_exit_fee": max(float(fee), 0.0),
            "fee_buffer": max(float(fee) * 2.0, 0.0),
            "total_required_collateral": max(required, 0.0),
        }
        if instrument is not None or execution_profile is not None:
            try:
                requirement, calc_type, _instrument_type = _requirement_for_wallet_order(
                    side=normalized_side,
                    qty=qty,
                    notional=notional,
                    fee=fee,
                    instrument=instrument,
                    execution_profile=execution_profile,
                    margin_session=margin_session,
                )
                if calc_type == "margin":
                    required = float(requirement.total_required)
                    details = {
                        **requirement.to_dict(),
                        "currency": quote,
                        "collateral_reserved": float(requirement.collateral_to_lock),
                    }
            except ValueError:
                required = required_full_short
        return quote, max(required, 0.0), details

    return quote, 0.0, {"currency": quote, "total_required_collateral": 0.0}


def wallet_can_apply_exit(
    *,
    state: WalletState,
    trade_id: Optional[str],
    qty: float,
    quote_currency: str,
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """Validate a margin exit against the open margin position, not free collateral."""

    close_qty = max(float(qty or 0.0), 0.0)
    if close_qty <= 0.0:
        return False, "WALLET_INVALID_EXIT_QTY", {"qty": qty}
    trade_key = str(trade_id or "").strip()
    if not trade_key:
        return False, "WALLET_MARGIN_POSITION_MISSING", {"trade_id": trade_id, "qty": close_qty}
    position = state.margin_positions.get(trade_key) if state.margin_positions else None
    if not isinstance(position, Mapping):
        return (
            True,
            None,
            {
                "trade_id": trade_key,
                "qty": close_qty,
                "margin_position_found": False,
                "collateral_released": 0.0,
            },
        )
    open_qty = max(_coerce_float(position.get("open_qty"), 0.0), 0.0)
    locked = max(_coerce_float(position.get("locked_margin"), 0.0), 0.0)
    currency = str(position.get("currency") or quote_currency or "").upper()
    if close_qty - open_qty > WALLET_PROJECTION_EPSILON:
        return (
            False,
            "WALLET_EXIT_QTY_EXCEEDS_OPEN_POSITION",
            {
                "trade_id": trade_key,
                "qty": close_qty,
                "open_qty": open_qty,
                "locked_margin": locked,
                "currency": currency,
            },
        )
    release_ratio = min(close_qty / open_qty, 1.0) if open_qty > 0 else 0.0
    collateral_released = min(locked * release_ratio, locked)
    return (
        True,
        None,
        {
            "trade_id": trade_key,
            "qty": close_qty,
            "open_qty": open_qty,
            "locked_margin": locked,
            "currency": currency,
            "margin_position_found": True,
            "collateral_released": collateral_released,
        },
    )


def _validate_short_cash_requirement(
    *,
    available_quote: float,
    quote: str,
    notional: float,
    fee: float,
    qty: float,
    qty_raw: float,
    qty_final: float,
    instrument: Optional[Mapping[str, Any]],
    execution_profile: Optional[SeriesExecutionProfile],
    margin_session: Optional[MarginSessionType],
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """Validate cash requirement for short positions.

    For futures/swaps: Uses margin-based requirement
    For spot: Uses full notional (cash-secured)
    """
    session = margin_session or MarginSessionType.OVERNIGHT  # Conservative default
    required_full_notional = float(notional) + float(fee) * 2

    # If instrument provided, try margin-based calculation
    if instrument is not None or execution_profile is not None:
        try:
            margin_req, calc_type, instrument_type = _requirement_for_wallet_order(
                side="short",
                qty=qty,
                notional=notional,
                fee=fee,
                instrument=instrument,
                execution_profile=execution_profile,
                margin_session=session,
            )
            margin_rate_source = _margin_rate_source_path(
                instrument,
                session,
                "short",
                execution_profile=execution_profile,
            )
            margin_total_required = margin_req.total_required
            required_used = margin_total_required if calc_type == "margin" else required_full_notional
            if available_quote + 1e-12 < required_used:
                reason = (
                    "WALLET_INSUFFICIENT_MARGIN"
                    if calc_type == "margin"
                    else "WALLET_INSUFFICIENT_CASH_FOR_SHORT_COVER"
                )
                shortfall = max(required_used - available_quote, 0.0)
                return (
                    False,
                    reason,
                    _wallet_requirement_payload(
                        margin_req,
                        available_quote=available_quote,
                        quote=quote,
                        qty_raw=qty_raw,
                        qty_final=qty_final,
                        margin_calc_type=calc_type,
                        margin_leg="short",
                        margin_rate_source_path=margin_rate_source,
                        instrument_type=instrument_type,
                        shortfall=shortfall,
                        required_full_notional=required_full_notional,
                    ),
                )
            return True, None, {}

        except ValueError as exc:
            # Instrument is misconfigured - fail loud
            return (
                False,
                "WALLET_INSTRUMENT_MISCONFIGURED",
                {
                    "error": str(exc),
                    "instrument_type": (
                        instrument.get("instrument_type")
                        if isinstance(instrument, Mapping)
                        else (execution_profile.instrument.instrument_type if execution_profile is not None else None)
                    ),
                    "symbol": (
                        instrument.get("symbol")
                        if isinstance(instrument, Mapping)
                        else (execution_profile.instrument.symbol if execution_profile is not None else None)
                    ),
                    "notional": notional,
                    "fee": fee,
                    "qty": qty,
                    "qty_raw": qty_raw,
                    "qty_final": qty_final,
                },
            )

    # No instrument metadata means cash-settlement validation only.
    if available_quote + 1e-12 < required_full_notional:
        return (
            False,
            "WALLET_INSUFFICIENT_CASH_FOR_SHORT_COVER",
            {
                "available": available_quote,
                "available_collateral": available_quote,
                "required": required_full_notional,
                "required_used": required_full_notional,
                "required_full_notional": required_full_notional,
                "margin_total_required": None,
                "currency": quote,
                "notional": notional,
                "fee": fee,
                "qty": qty,
                "qty_raw": qty_raw,
                "qty_final": qty_final,
                "margin_calc_type": None,
                "margin_leg": "short",
                "shortfall": max(required_full_notional - available_quote, 0.0),
            },
        )
    return True, None, {}


def _validate_long_cash_requirement(
    *,
    instrument: Optional[Mapping[str, Any]],
    execution_profile: Optional[SeriesExecutionProfile],
    available_quote: float,
    quote: str,
    notional: float,
    fee: float,
    qty: float,
    qty_raw: float,
    qty_final: float,
    margin_session: Optional[MarginSessionType],
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    required_full_notional = float(notional) + float(fee)
    session = margin_session or MarginSessionType.OVERNIGHT

    if instrument is not None or execution_profile is not None:
        try:
            _calculator, calc_type, instrument_type = _resolve_margin_model(
                instrument=instrument,
                execution_profile=execution_profile,
            )
            if calc_type == "margin":
                margin_req, _calc_type, instrument_type = _requirement_for_wallet_order(
                    side="long",
                    qty=qty,
                    notional=notional,
                    fee=fee,
                    instrument=instrument,
                    execution_profile=execution_profile,
                    margin_session=session,
                )
                margin_rate_source = _margin_rate_source_path(
                    instrument,
                    session,
                    "long",
                    execution_profile=execution_profile,
                )
                margin_total_required = margin_req.total_required
                required_used = margin_total_required
                if available_quote + 1e-12 < required_used:
                    shortfall = max(required_used - available_quote, 0.0)
                    return (
                        False,
                        "WALLET_INSUFFICIENT_MARGIN",
                        _wallet_requirement_payload(
                            margin_req,
                            available_quote=available_quote,
                            quote=quote,
                            qty_raw=qty_raw,
                            qty_final=qty_final,
                            margin_calc_type=calc_type,
                            margin_leg="long",
                            margin_rate_source_path=margin_rate_source,
                            instrument_type=instrument_type,
                            shortfall=shortfall,
                            required_full_notional=required_full_notional,
                        ),
                    )
                return True, None, {}

            if available_quote + 1e-12 < required_full_notional:
                return (
                    False,
                    "WALLET_INSUFFICIENT_CASH",
                    {
                        "available": available_quote,
                        "available_collateral": available_quote,
                        "required": required_full_notional,
                        "required_used": required_full_notional,
                        "required_full_notional": required_full_notional,
                        "margin_total_required": None,
                        "currency": quote,
                        "notional": notional,
                        "fee": fee,
                        "qty": qty,
                        "qty_raw": qty_raw,
                        "qty_final": qty_final,
                        "instrument_type": instrument_type,
                        "margin_calc_type": calc_type,
                        "margin_leg": "long",
                        "shortfall": max(required_full_notional - available_quote, 0.0),
                    },
                )
            return True, None, {}

        except ValueError as exc:
            return (
                False,
                "WALLET_INSTRUMENT_MISCONFIGURED",
                {
                    "error": str(exc),
                    "instrument_type": (
                        instrument.get("instrument_type")
                        if isinstance(instrument, Mapping)
                        else (execution_profile.instrument.instrument_type if execution_profile is not None else None)
                    ),
                    "symbol": (
                        instrument.get("symbol")
                        if isinstance(instrument, Mapping)
                        else (execution_profile.instrument.symbol if execution_profile is not None else None)
                    ),
                    "notional": notional,
                    "fee": fee,
                    "qty": qty,
                    "qty_raw": qty_raw,
                    "qty_final": qty_final,
                },
            )

    if available_quote + 1e-12 < required_full_notional:
        return (
            False,
            "WALLET_INSUFFICIENT_CASH",
            {
                "available": available_quote,
                "available_collateral": available_quote,
                "required": required_full_notional,
                "required_used": required_full_notional,
                "required_full_notional": required_full_notional,
                "margin_total_required": None,
                "currency": quote,
                "notional": notional,
                "fee": fee,
                "qty": qty,
                "qty_raw": qty_raw,
                "qty_final": qty_final,
                "margin_calc_type": None,
                "margin_leg": "long",
                "shortfall": max(required_full_notional - available_quote, 0.0),
            },
        )
    return True, None, {}


def _resolve_qty_fields(qty: float, qty_raw: Optional[float], qty_final: Optional[float]) -> Tuple[float, float]:
    resolved_final = float(qty_final) if qty_final is not None else float(qty)
    resolved_raw = float(qty_raw) if qty_raw is not None else float(qty)
    return resolved_raw, resolved_final


def _margin_rate_source_path(
    instrument: Optional[Mapping[str, Any]],
    session: MarginSessionType,
    direction: str,
    execution_profile: Optional[SeriesExecutionProfile] = None,
) -> Optional[str]:
    session_key = "intraday" if session == MarginSessionType.INTRADAY else "overnight"
    direction_key = "long_margin_rate" if direction == "long" else "short_margin_rate"
    if execution_profile is not None and execution_profile.margin_rates is not None:
        container = (
            {
                "intraday": {
                    "long_margin_rate": execution_profile.margin_rates.intraday_long,
                    "short_margin_rate": execution_profile.margin_rates.intraday_short,
                },
                "overnight": {
                    "long_margin_rate": execution_profile.margin_rates.overnight_long,
                    "short_margin_rate": execution_profile.margin_rates.overnight_short,
                },
            }.get(session_key)
            or {}
        )
        if container.get(direction_key) not in (None, 0):
            return f"execution_profile.margin_rates.{session_key}.{direction_key}"

    margin_rates = instrument.get("margin_rates") if isinstance(instrument, Mapping) else None
    if isinstance(margin_rates, Mapping):
        container = margin_rates.get(session_key)
        if isinstance(container, Mapping) and direction_key in container:
            return f"margin_rates.{session_key}.{direction_key}"
    metadata = instrument.get("metadata") if isinstance(instrument, Mapping) and isinstance(instrument.get("metadata"), Mapping) else {}
    instrument_fields = (
        metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
    )
    field_rates = instrument_fields.get("margin_rates")
    if isinstance(field_rates, Mapping):
        container = field_rates.get(session_key)
        if isinstance(container, Mapping) and direction_key in container:
            return f"metadata.instrument_fields.margin_rates.{session_key}.{direction_key}"
    return None


__all__ = [
    "MarginSessionType",
    "WalletEvent",
    "WalletLedger",
    "LockedWalletLedger",
    "WalletState",
    "canonical_wallet_ledger_events",
    "project_wallet",
    "project_wallet_from_events",
    "first_wallet_ledger_state_issue",
    "trace_wallet_balance",
    "validate_wallet_ledger_state",
    "wallet_can_apply",
    "wallet_can_apply_exit",
    "wallet_required_reservation",
    "wallet_required_reservation_details",
]
