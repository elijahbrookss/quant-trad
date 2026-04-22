"""Wallet gateway interfaces for bot runtime."""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol, Tuple

from .execution_profile import SeriesExecutionProfile
from .margin import MarginSessionType
from .runtime_events import RuntimeEvent, RuntimeEventName
from .wallet import (
    WalletEvent,
    WalletState,
    project_wallet_from_events,
    wallet_can_apply,
    wallet_required_reservation,
)

logger = logging.getLogger(__name__)


_RESERVATION_STATUS_ACTIVE = "ACTIVE"
_RESERVATION_STATUS_CONSUMED = "CONSUMED"
_RESERVATION_STATUS_RELEASED = "RELEASED"
_RESERVATION_STATUS_EXPIRED = "EXPIRED"
_RESERVATION_STATUS_STUCK = "STUCK"
_HOLD_STATUSES = {
    _RESERVATION_STATUS_ACTIVE,
    _RESERVATION_STATUS_CONSUMED,
    _RESERVATION_STATUS_STUCK,
}
_DEFAULT_RESERVATION_TTL_SECONDS = 30.0
_DEFAULT_CONSUMED_TIMEOUT_SECONDS = 120.0


class WalletGateway(Protocol):
    """Abstract wallet layer used by the risk engine."""

    def can_apply(
        self,
        *,
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
        reserve: bool = False,
        correlation_id: Optional[str] = None,
        trade_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        ...

    def apply_fill(
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
        reservation_id: Optional[str] = None,
        margin_locked: Optional[float] = None,
        correlation_id: Optional[str] = None,
        exit_kind: Optional[str] = None,
    ) -> Dict[str, Any]:
        ...

    def reject(
        self,
        reason: str,
        payload: Mapping[str, Any],
        trade_id: Optional[str] = None,
        leg_id: Optional[str] = None,
    ) -> None:
        ...

    def project(self) -> WalletState:
        ...

    def events(self) -> Iterable[WalletEvent]:
        ...


class BaseWalletGateway:
    """Event-sourced wallet gateway base for concrete runtime transports."""

    def __init__(
        self,
        *,
        reservation_ttl_seconds: float = _DEFAULT_RESERVATION_TTL_SECONDS,
        consumed_timeout_seconds: float = _DEFAULT_CONSUMED_TIMEOUT_SECONDS,
    ) -> None:
        self._reservation_ttl_seconds = max(float(reservation_ttl_seconds or 0.0), 0.001)
        self._consumed_timeout_seconds = max(float(consumed_timeout_seconds or 0.0), 0.001)
        self._last_seen_seq = 0

    def __enter__(self) -> "BaseWalletGateway":
        raise NotImplementedError("BaseWalletGateway requires a concrete lock strategy")

    def __exit__(self, exc_type, exc, tb) -> None:
        raise NotImplementedError("BaseWalletGateway requires a concrete lock strategy")

    def _iter_runtime_events(self) -> Iterable[RuntimeEvent | Mapping[str, Any]]:
        raise NotImplementedError("BaseWalletGateway requires a runtime event source")

    def _reservation_items(self) -> Iterable[Tuple[str, Mapping[str, Any]]]:
        raise NotImplementedError("BaseWalletGateway requires reservation storage")

    def _reservation_get(self, reservation_id: str) -> Optional[Mapping[str, Any]]:
        raise NotImplementedError("BaseWalletGateway requires reservation storage")

    def _reservation_set(self, reservation_id: str, payload: Mapping[str, Any]) -> None:
        raise NotImplementedError("BaseWalletGateway requires reservation storage")

    def _reservation_pop(self, reservation_id: str) -> Optional[Mapping[str, Any]]:
        raise NotImplementedError("BaseWalletGateway requires reservation storage")

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _utc_now_iso() -> str:
        return BaseWalletGateway._utc_now().isoformat().replace("+00:00", "Z")

    @staticmethod
    def _parse_iso_utc(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _normalise_event_name(event: RuntimeEvent | Mapping[str, Any]) -> Optional[str]:
        if isinstance(event, RuntimeEvent):
            return event.event_name.value
        if not isinstance(event, Mapping):
            return None
        raw_name = event.get("event_name") or event.get("event_type")
        if raw_name is None:
            payload = event.get("payload")
            if isinstance(payload, Mapping):
                raw_name = payload.get("event_name")
        text = str(raw_name or "").strip()
        return text or None

    @staticmethod
    def _event_trade_id(event: RuntimeEvent | Mapping[str, Any]) -> Optional[str]:
        if isinstance(event, RuntimeEvent):
            trade_id = getattr(event.context, "trade_id", None)
            return str(trade_id) if trade_id else None
        if not isinstance(event, Mapping):
            return None
        payload = BaseWalletGateway._event_payload(event)
        trade_id = payload.get("trade_id")
        return str(trade_id) if trade_id else None

    @staticmethod
    def _event_payload(event: RuntimeEvent | Mapping[str, Any]) -> Dict[str, Any]:
        if isinstance(event, RuntimeEvent):
            return dict(event.context.to_dict())
        if not isinstance(event, Mapping):
            return {}
        payload = event.get("context")
        if isinstance(payload, Mapping):
            return dict(payload)
        payload = event.get("payload")
        if isinstance(payload, Mapping):
            nested = payload.get("context") if isinstance(payload.get("context"), Mapping) else payload
            return dict(nested)
        return {}

    @staticmethod
    def _event_correlation_id(event: RuntimeEvent | Mapping[str, Any]) -> Optional[str]:
        payload = BaseWalletGateway._event_payload(event)
        payload_correlation = str(payload.get("wallet_correlation_id") or "").strip()
        if payload_correlation:
            return payload_correlation
        if isinstance(event, RuntimeEvent):
            value = str(event.correlation_id or "").strip()
            return value or None
        if not isinstance(event, Mapping):
            return None
        value = str(event.get("correlation_id") or "").strip()
        return value or None

    @staticmethod
    def _event_reservation_id(event: RuntimeEvent | Mapping[str, Any]) -> Optional[str]:
        payload = BaseWalletGateway._event_payload(event)
        value = payload.get("reservation_id")
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _event_exit_kind(event: RuntimeEvent | Mapping[str, Any]) -> Optional[str]:
        payload = BaseWalletGateway._event_payload(event)
        text = str(payload.get("exit_kind") or "").strip().upper()
        return text or None

    @staticmethod
    def _event_seq(event: RuntimeEvent | Mapping[str, Any], *, fallback: int = 0) -> int:
        if isinstance(event, Mapping):
            try:
                value = int(event.get("seq") or 0)
                if value > 0:
                    return value
            except (TypeError, ValueError):
                pass
        return int(max(fallback, 0))

    @staticmethod
    def _expected_runtime_event_name(fill_event_type: Optional[str]) -> str:
        text = str(fill_event_type or "ENTRY_FILL").strip().upper()
        if text == "EXIT_FILL":
            return RuntimeEventName.EXIT_FILLED.value
        return RuntimeEventName.ENTRY_FILLED.value

    @staticmethod
    def _reservation_status(payload: Mapping[str, Any]) -> str:
        return str(payload.get("status") or "").strip().upper() or _RESERVATION_STATUS_ACTIVE

    @staticmethod
    def _reservation_currency(payload: Mapping[str, Any]) -> str:
        required_delta = payload.get("required_delta")
        if isinstance(required_delta, Mapping):
            code = str(required_delta.get("currency") or "").strip().upper()
            if code:
                return code
        return str(payload.get("currency") or "").strip().upper()

    @staticmethod
    def _reservation_hold_amount(payload: Mapping[str, Any]) -> float:
        required_delta = payload.get("required_delta")
        if isinstance(required_delta, Mapping):
            hold_total = 0.0
            try:
                hold_total += max(float(required_delta.get("collateral_reserved") or 0.0), 0.0)
            except (TypeError, ValueError):
                pass
            try:
                hold_total += max(float(required_delta.get("fee_estimate") or 0.0), 0.0)
            except (TypeError, ValueError):
                pass
            if hold_total > 0.0:
                return hold_total
        try:
            amount = float(payload.get("amount") or 0.0)
        except (TypeError, ValueError):
            amount = 0.0
        return max(amount, 0.0)

    @staticmethod
    def _reservation_required_delta(payload: Mapping[str, Any]) -> Dict[str, Any]:
        value = payload.get("required_delta")
        if isinstance(value, Mapping):
            return dict(value)
        return {}

    def _runtime_events_with_seq(self) -> list[tuple[int, RuntimeEvent | Mapping[str, Any]]]:
        raw = list(self._iter_runtime_events())
        events: list[tuple[int, RuntimeEvent | Mapping[str, Any]]] = []
        max_seq = 0
        for idx, item in enumerate(raw, start=1):
            seq = self._event_seq(item, fallback=idx)
            if seq > max_seq:
                max_seq = seq
            events.append((seq, item))
        self._last_seen_seq = max(int(self._last_seen_seq), int(max_seq))
        return events

    def _reservation_observed(
        self,
        payload: Mapping[str, Any],
        events: Iterable[tuple[int, RuntimeEvent | Mapping[str, Any]]],
    ) -> bool:
        expected_event_name = self._expected_runtime_event_name(str(payload.get("event_type") or "ENTRY_FILL"))
        seq_created_at = int(payload.get("seq_created_at") or 0)
        expected_reservation_id = str(payload.get("reservation_id") or "").strip()
        expected_trade_id = str(payload.get("trade_id") or "").strip()
        expected_correlation_id = str(payload.get("correlation_id") or "").strip()
        expected_exit_kind = str(payload.get("exit_kind") or "").strip().upper()
        for seq, event in events:
            if int(seq) <= seq_created_at:
                continue
            if self._normalise_event_name(event) != expected_event_name:
                continue
            observed_reservation_id = self._event_reservation_id(event)
            if expected_reservation_id and observed_reservation_id:
                if observed_reservation_id == expected_reservation_id:
                    return True
                continue
            if expected_event_name == RuntimeEventName.ENTRY_FILLED.value:
                event_trade_id = self._event_trade_id(event) or ""
                event_correlation_id = self._event_correlation_id(event) or ""
                if (
                    expected_trade_id
                    and expected_correlation_id
                    and event_trade_id == expected_trade_id
                    and event_correlation_id == expected_correlation_id
                ):
                    return True
                continue
            if expected_event_name == RuntimeEventName.EXIT_FILLED.value:
                event_trade_id = self._event_trade_id(event) or ""
                event_correlation_id = self._event_correlation_id(event) or ""
                event_exit_kind = self._event_exit_kind(event) or ""
                if (
                    expected_trade_id
                    and expected_correlation_id
                    and expected_exit_kind
                    and event_trade_id == expected_trade_id
                    and event_exit_kind == expected_exit_kind
                    and event_correlation_id == expected_correlation_id
                ):
                    return True
                continue
        return False

    def _reconcile_reservations(
        self,
        events: Iterable[tuple[int, RuntimeEvent | Mapping[str, Any]]],
    ) -> None:
        observed_events = list(events)
        now = self._utc_now()
        now_iso = now.isoformat().replace("+00:00", "Z")
        for reservation_id, payload in list(self._reservation_items()):
            if not isinstance(payload, Mapping):
                self._reservation_pop(str(reservation_id))
                continue
            current = dict(payload)
            status = self._reservation_status(current)
            expires_at = self._parse_iso_utc(current.get("expires_at"))
            if status == _RESERVATION_STATUS_ACTIVE and expires_at is not None and now >= expires_at:
                current["status"] = _RESERVATION_STATUS_EXPIRED
                current["expired_at"] = now_iso
                self._reservation_set(str(reservation_id), current)
                continue
            if status in {_RESERVATION_STATUS_CONSUMED, _RESERVATION_STATUS_STUCK}:
                if self._reservation_observed(current, observed_events):
                    self._reservation_pop(str(reservation_id))
                    continue
                consumed_at = self._parse_iso_utc(current.get("consumed_at"))
                if consumed_at is None:
                    continue
                if now - consumed_at >= timedelta(seconds=self._consumed_timeout_seconds):
                    if status != _RESERVATION_STATUS_STUCK:
                        current["status"] = _RESERVATION_STATUS_STUCK
                        current["stuck_at"] = now_iso
                        current["stuck_reason"] = "canonical_event_missing_after_consumed_timeout"
                        self._reservation_set(str(reservation_id), current)
                        logger.warning(
                            "wallet_reservation_stuck | reservation_id=%s | trade_id=%s | correlation_id=%s",
                            reservation_id,
                            current.get("trade_id"),
                            current.get("correlation_id"),
                        )

    def _release_consumed_reservations(
        self,
        events: Iterable[RuntimeEvent | Mapping[str, Any]],
    ) -> None:
        # Deprecated hook retained for compatibility. Reconciliation uses seq-based checks.
        _ = list(events)

    def _project_from_events(
        self,
        *,
        events: Iterable[RuntimeEvent | Mapping[str, Any]],
        include_reservations: bool,
    ) -> WalletState:
        state = project_wallet_from_events(events)
        balances = dict(state.balances or {})
        locked_margin = dict(state.locked_margin or {})
        margin_positions = dict(state.margin_positions or {})
        free_collateral = dict(state.free_collateral or {})
        if not free_collateral:
            currencies = set(balances.keys()) | set(locked_margin.keys())
            for currency in currencies:
                free_collateral[currency] = balances.get(currency, 0.0) - locked_margin.get(currency, 0.0)
        if include_reservations:
            for _reservation_id, payload in list(self._reservation_items()):
                if not isinstance(payload, Mapping):
                    continue
                status = self._reservation_status(payload)
                if status not in _HOLD_STATUSES:
                    continue
                currency = self._reservation_currency(payload)
                if not currency:
                    continue
                amount = self._reservation_hold_amount(payload)
                if amount <= 0.0:
                    continue
                free_collateral[currency] = free_collateral.get(currency, 0.0) - amount
        return WalletState(
            balances=balances,
            locked_margin=locked_margin,
            free_collateral=free_collateral,
            margin_positions=margin_positions,
        )

    def can_apply(
        self,
        *,
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
        reserve: bool = False,
        correlation_id: Optional[str] = None,
        trade_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        with self:
            events_with_seq = self._runtime_events_with_seq()
            self._reconcile_reservations(events_with_seq)
            runtime_events = [event for _seq, event in events_with_seq]
            state = self._project_from_events(events=runtime_events, include_reservations=True)
            allowed, reason, payload = wallet_can_apply(
                state=state,
                side=side,
                base_currency=base_currency,
                quote_currency=quote_currency,
                qty=qty,
                qty_raw=qty_raw,
                qty_final=qty_final,
                notional=notional,
                fee=fee,
                short_requires_borrow=short_requires_borrow,
                instrument=instrument,
                execution_profile=execution_profile,
                margin_session=margin_session,
            )
            details = dict(payload or {})
            if not allowed:
                return allowed, reason, details

            if reserve:
                now = self._utc_now()
                reserve_currency, reserve_amount = wallet_required_reservation(
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
                if reserve_amount > 0.0:
                    reservation_id = str(uuid.uuid4())
                    resolved_trade_id = str(trade_id or "").strip() or None
                    if correlation_id:
                        resolved_correlation_id = str(correlation_id).strip()
                    elif resolved_trade_id:
                        resolved_correlation_id = f"trade:{resolved_trade_id}"
                    else:
                        resolved_correlation_id = f"reservation:{reservation_id}"
                    max_seq = max((seq for seq, _event in events_with_seq), default=int(self._last_seen_seq))
                    expires_at = now + timedelta(seconds=self._reservation_ttl_seconds)
                    fee_estimate = max(float(fee), 0.0)
                    collateral_reserved = max(float(reserve_amount) - fee_estimate, 0.0)
                    required_delta = {
                        "currency": reserve_currency,
                        "collateral_reserved": float(collateral_reserved),
                        "fee_estimate": float(fee_estimate),
                    }
                    total_hold = float(collateral_reserved + fee_estimate)
                    self._reservation_set(
                        reservation_id,
                        {
                            "reservation_id": reservation_id,
                            "currency": reserve_currency,
                            "amount": total_hold,
                            "created_at": now.isoformat().replace("+00:00", "Z"),
                            "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
                            "status": _RESERVATION_STATUS_ACTIVE,
                            "correlation_id": resolved_correlation_id,
                            "trade_id": resolved_trade_id,
                            "required_delta": required_delta,
                            "seq_created_at": int(max_seq),
                            "event_type": "ENTRY_FILL",
                        },
                    )
                    details["reservation_id"] = reservation_id
                    details["reservation_status"] = _RESERVATION_STATUS_ACTIVE
                    details["correlation_id"] = resolved_correlation_id
                    details["trade_id"] = resolved_trade_id
                    details["reserved_currency"] = reserve_currency
                    details["reserved_amount"] = total_hold
                    details["required_delta"] = required_delta
            return True, None, details

    def apply_fill(
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
        reservation_id: Optional[str] = None,
        margin_locked: Optional[float] = None,
        correlation_id: Optional[str] = None,
        exit_kind: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self:
            events_with_seq = self._runtime_events_with_seq()
            self._reconcile_reservations(events_with_seq)
            expected_event_name = self._expected_runtime_event_name(event_type)
            resolved_trade_id = str(trade_id or "").strip() or None
            resolved_correlation_id = str(correlation_id or "").strip() or None
            if reservation_id:
                key = str(reservation_id)
                reservation = self._reservation_get(key)
                if not isinstance(reservation, Mapping):
                    raise ValueError(f"reservation not found: {key}")
                status = self._reservation_status(reservation)
                if status != _RESERVATION_STATUS_ACTIVE:
                    raise ValueError(f"reservation not active: {key} status={status}")
                next_payload = dict(reservation)
                next_payload["status"] = _RESERVATION_STATUS_CONSUMED
                next_payload["consumed_at"] = self._utc_now_iso()
                next_payload["seq_consumed_at"] = int(self._last_seen_seq)
                next_payload["event_type"] = str(event_type or next_payload.get("event_type") or "ENTRY_FILL")
                next_payload["expected_event_name"] = expected_event_name
                if resolved_trade_id:
                    next_payload["trade_id"] = resolved_trade_id
                if resolved_correlation_id:
                    next_payload["correlation_id"] = resolved_correlation_id
                if expected_event_name == RuntimeEventName.EXIT_FILLED.value:
                    if exit_kind:
                        next_payload["exit_kind"] = str(exit_kind).strip().upper()
                else:
                    next_payload.pop("exit_kind", None)
                self._reservation_set(key, next_payload)
                required_delta = self._reservation_required_delta(next_payload)
                collateral_reserved = 0.0
                if isinstance(required_delta, Mapping):
                    try:
                        collateral_reserved = max(float(required_delta.get("collateral_reserved") or 0.0), 0.0)
                    except Exception:
                        collateral_reserved = 0.0
                wallet_delta: Dict[str, Any] = {
                    "collateral_reserved": float(max(collateral_reserved, 0.0)),
                    "collateral_released": 0.0,
                    "fee_paid": float(max(float(fee or 0.0), 0.0)),
                }
                return {
                    "reservation_id": key,
                    "reservation_status": _RESERVATION_STATUS_CONSUMED,
                    "correlation_id": str(next_payload.get("correlation_id") or ""),
                    "trade_id": str(next_payload.get("trade_id") or ""),
                    "required_delta": required_delta,
                    "wallet_delta": wallet_delta,
                    "event_name": expected_event_name,
                }
            wallet_delta = {
                "collateral_reserved": float(max(float(margin_locked or 0.0), 0.0))
                if expected_event_name == RuntimeEventName.ENTRY_FILLED.value
                else 0.0,
                "collateral_released": 0.0,
                "fee_paid": float(max(float(fee or 0.0), 0.0)),
            }
            return {
                "reservation_id": None,
                "reservation_status": None,
                "correlation_id": str(resolved_correlation_id or ""),
                "trade_id": str(resolved_trade_id or ""),
                "required_delta": {},
                "wallet_delta": wallet_delta,
                "event_name": expected_event_name,
            }

    def reject(
        self,
        reason: str,
        payload: Mapping[str, Any],
        trade_id: Optional[str] = None,
        leg_id: Optional[str] = None,
    ) -> None:
        with self:
            reservation_id = payload.get("reservation_id") if isinstance(payload, Mapping) else None
            if reservation_id:
                key = str(reservation_id)
                reservation = self._reservation_get(key)
                if isinstance(reservation, Mapping):
                    next_payload = dict(reservation)
                    next_payload["status"] = _RESERVATION_STATUS_RELEASED
                    next_payload["released_at"] = self._utc_now_iso()
                    next_payload["release_reason"] = str(reason or "")
                    self._reservation_set(key, next_payload)

    def project(self) -> WalletState:
        with self:
            events_with_seq = self._runtime_events_with_seq()
            self._reconcile_reservations(events_with_seq)
            runtime_events = [event for _seq, event in events_with_seq]
            return self._project_from_events(events=runtime_events, include_reservations=False)

    def events(self) -> Iterable[WalletEvent]:
        with self:
            events_with_seq = self._runtime_events_with_seq()
            self._reconcile_reservations(events_with_seq)
            raw = [event for _seq, event in events_with_seq]
        normalised: list[WalletEvent] = []
        for item in raw:
            if isinstance(item, RuntimeEvent):
                normalised.append(
                    WalletEvent(
                        event_id=str(item.event_id),
                        event_type=str(item.event_name.value),
                        timestamp=item.event_ts.isoformat().replace("+00:00", "Z"),
                        payload=dict(item.context.to_dict()),
                    )
                )
                continue
            if not isinstance(item, Mapping):
                continue
            event_name = self._normalise_event_name(item)
            if event_name is None:
                continue
            if event_name not in {
                RuntimeEventName.WALLET_INITIALIZED.value,
                RuntimeEventName.WALLET_DEPOSITED.value,
                RuntimeEventName.ENTRY_FILLED.value,
                RuntimeEventName.EXIT_FILLED.value,
            }:
                continue
            timestamp = item.get("event_ts") or item.get("timestamp")
            normalised.append(
                WalletEvent(
                    event_id=str(item.get("event_id") or ""),
                    event_type=str(event_name),
                    timestamp=str(timestamp or ""),
                    payload=BaseWalletGateway._event_payload(item),
                )
            )
        return normalised


class SharedWalletGateway(BaseWalletGateway):
    """Process-safe wallet gateway backed by canonical runtime events."""

    def __init__(
        self,
        proxy: Mapping[str, Any],
        *,
        reservation_ttl_seconds: float = _DEFAULT_RESERVATION_TTL_SECONDS,
        consumed_timeout_seconds: float = _DEFAULT_CONSUMED_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(
            reservation_ttl_seconds=reservation_ttl_seconds,
            consumed_timeout_seconds=consumed_timeout_seconds,
        )
        runtime_events = proxy.get("runtime_events")
        lock = proxy.get("lock")
        reservations = proxy.get("reservations")
        if runtime_events is None or lock is None or reservations is None:
            raise ValueError("shared wallet proxy requires runtime_events/reservations/lock")
        self._runtime_events = runtime_events
        self._lock = lock
        self._reservations = reservations
        self._local_lock = threading.RLock()
        seq_counter = proxy.get("runtime_event_seq")
        if seq_counter is not None:
            try:
                self._last_seen_seq = int(seq_counter.get())
            except Exception:
                self._last_seen_seq = int(getattr(seq_counter, "value", 0))

    def _with_lock(self):
        # Manager lock proxies are process-safe; local lock prevents re-entrant deadlocks in one process.
        self._local_lock.acquire()
        self._lock.acquire()
        return self

    def __enter__(self) -> "SharedWalletGateway":
        return self._with_lock()

    def __exit__(self, exc_type, exc, tb) -> None:
        self._lock.release()
        self._local_lock.release()

    def _iter_runtime_events(self) -> Iterable[RuntimeEvent | Mapping[str, Any]]:
        return list(self._runtime_events)

    def _reservation_items(self) -> Iterable[Tuple[str, Mapping[str, Any]]]:
        return list(dict(self._reservations).items())

    def _reservation_get(self, reservation_id: str) -> Optional[Mapping[str, Any]]:
        payload = self._reservations.get(str(reservation_id))
        if isinstance(payload, Mapping):
            return dict(payload)
        return None

    def _reservation_set(self, reservation_id: str, payload: Mapping[str, Any]) -> None:
        self._reservations[str(reservation_id)] = dict(payload or {})

    def _reservation_pop(self, reservation_id: str) -> Optional[Mapping[str, Any]]:
        payload = self._reservations.pop(str(reservation_id), None)
        if isinstance(payload, Mapping):
            return dict(payload)
        return None


__all__ = ["WalletGateway", "BaseWalletGateway", "SharedWalletGateway"]
