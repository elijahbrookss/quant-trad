"""In-memory wallet ledger and projection utilities for backtests.

Supports both spot (full notional) and derivatives (margin-based) validation.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .margin import (
    MarginRequirement,
    MarginSessionType,
    create_margin_calculator,
)


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
    ) -> WalletEvent:
        return self.append(
            "TRADE_FILL",
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


def project_wallet(events: Iterable[WalletEvent]) -> WalletState:
    balances: Dict[str, float] = {}
    for event in events:
        event_type = event.event_type
        payload = event.payload or {}
        if event_type == "DEPOSIT":
            for currency, amount in (payload.get("balances") or {}).items():
                code = str(currency).upper()
                balances[code] = balances.get(code, 0.0) + float(amount)
        elif event_type == "TRADE_FILL":
            side = str(payload.get("side") or "").lower()
            base = str(payload.get("base_currency") or "").upper()
            quote = str(payload.get("quote_currency") or "").upper()
            qty = float(payload.get("qty") or 0.0)
            notional = float(payload.get("notional") or 0.0)
            fee = float(payload.get("fee") or 0.0)
            if side in {"buy", "long"}:
                balances[base] = balances.get(base, 0.0) + qty
                balances[quote] = balances.get(quote, 0.0) - notional - fee
            elif side in {"sell", "short"}:
                balances[base] = balances.get(base, 0.0) - qty
                balances[quote] = balances.get(quote, 0.0) + notional - fee
        elif event_type == "REJECTED":
            continue
    return WalletState(balances=balances)


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
    available_quote = balances.get(quote, 0.0)
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
            margin_session=margin_session,
        )

    return True, None, {}


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
    margin_session: Optional[MarginSessionType],
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """Validate cash requirement for short positions.

    For futures/swaps: Uses margin-based requirement
    For spot: Uses full notional (cash-secured)
    """
    session = margin_session or MarginSessionType.OVERNIGHT  # Conservative default
    required_full_notional = float(notional) + float(fee) * 2

    # If instrument provided, try margin-based calculation
    if instrument is not None:
        try:
            calculator, calc_type = create_margin_calculator(instrument)
            margin_req = calculator.calculate(
                notional=notional,
                fee=fee,
                direction="short",
                session=session,
            )
            instrument_type = instrument.get("instrument_type") if isinstance(instrument, Mapping) else None
            margin_rate_source = _margin_rate_source_path(instrument, session, "short")
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
                    {
                        "available": available_quote,
                        "available_collateral": available_quote,
                        "required": required_used,
                        "required_used": required_used,
                        "required_full_notional": required_full_notional,
                        "margin_total_required": margin_total_required,
                        "currency": quote,
                        "notional": notional,
                        "fee": fee,
                        "qty": qty,
                        "qty_raw": qty_raw,
                        "qty_final": qty_final,
                        "margin_rate": margin_req.margin_rate,
                        "margin_method": margin_req.calculation_method,
                        "margin_session": margin_req.session_type,
                        "margin_calc_type": calc_type,
                        "margin_leg": "short",
                        "margin_rate_source_path": margin_rate_source,
                        "instrument_type": instrument_type,
                        "required_margin": margin_req.required_margin,
                        "fee_buffer": margin_req.fee_buffer,
                        "safety_buffer": margin_req.safety_buffer,
                        "shortfall": shortfall,
                    },
                )
            return True, None, {}

        except ValueError as exc:
            # Instrument is misconfigured - fail loud
            return (
                False,
                "WALLET_INSTRUMENT_MISCONFIGURED",
                {
                    "error": str(exc),
                    "instrument_type": instrument.get("instrument_type"),
                    "symbol": instrument.get("symbol"),
                    "notional": notional,
                    "fee": fee,
                    "qty": qty,
                    "qty_raw": qty_raw,
                    "qty_final": qty_final,
                },
            )

    # Fallback: No instrument provided, use legacy spot-style calculation
    # This maintains backward compatibility
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

    if instrument is not None:
        try:
            calculator, calc_type = create_margin_calculator(instrument)
            instrument_type = instrument.get("instrument_type") if isinstance(instrument, Mapping) else None
            if calc_type == "margin":
                margin_req = calculator.calculate(
                    notional=notional,
                    fee=fee,
                    direction="long",
                    session=session,
                )
                margin_rate_source = _margin_rate_source_path(instrument, session, "long")
                margin_total_required = margin_req.total_required
                required_used = margin_total_required
                if available_quote + 1e-12 < required_used:
                    shortfall = max(required_used - available_quote, 0.0)
                    return (
                        False,
                        "WALLET_INSUFFICIENT_MARGIN",
                        {
                            "available": available_quote,
                            "available_collateral": available_quote,
                            "required": required_used,
                            "required_used": required_used,
                            "required_full_notional": required_full_notional,
                            "margin_total_required": margin_total_required,
                            "currency": quote,
                            "notional": notional,
                            "fee": fee,
                            "qty": qty,
                            "qty_raw": qty_raw,
                            "qty_final": qty_final,
                            "margin_rate": margin_req.margin_rate,
                            "margin_method": margin_req.calculation_method,
                            "margin_session": margin_req.session_type,
                            "margin_calc_type": calc_type,
                            "margin_leg": "long",
                            "margin_rate_source_path": margin_rate_source,
                            "instrument_type": instrument_type,
                            "required_margin": margin_req.required_margin,
                            "fee_buffer": margin_req.fee_buffer,
                            "safety_buffer": margin_req.safety_buffer,
                            "shortfall": shortfall,
                        },
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
                    "instrument_type": instrument.get("instrument_type") if isinstance(instrument, Mapping) else None,
                    "symbol": instrument.get("symbol") if isinstance(instrument, Mapping) else None,
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
    instrument: Mapping[str, Any],
    session: MarginSessionType,
    direction: str,
) -> Optional[str]:
    metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
    info = metadata.get("info") if isinstance(metadata.get("info"), Mapping) else {}
    details = info.get("future_product_details") if isinstance(info.get("future_product_details"), Mapping) else {}
    if not details:
        return None
    session_key = "intraday_margin_rate" if session == MarginSessionType.INTRADAY else "overnight_margin_rate"
    direction_key = "long_margin_rate" if direction == "long" else "short_margin_rate"
    container = details.get(session_key)
    if not isinstance(container, Mapping) or direction_key not in container:
        return None
    return f"metadata.info.future_product_details.{session_key}.{direction_key}"


__all__ = [
    "MarginSessionType",
    "WalletEvent",
    "WalletLedger",
    "WalletState",
    "project_wallet",
    "wallet_can_apply",
]
