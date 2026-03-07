from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import threading
import time
from typing import Optional

import pytest

from engines.bot_runtime.core.runtime_events import (
    ExitKind,
    ReasonCode,
    RuntimeEventName,
    build_correlation_id,
    new_runtime_event,
)
from engines.bot_runtime.core.wallet import project_wallet_from_events
from engines.bot_runtime.core.wallet_gateway import SharedWalletGateway


def _bar_time() -> datetime:
    return datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)


def _correlation_id() -> str:
    return build_correlation_id(
        run_id="run-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=_bar_time(),
    )


def test_correlation_id_is_deterministic_and_utc_normalized() -> None:
    aware = datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=timezone.utc)
    naive = datetime(2026, 1, 1, 0, 0, 0, 123000)
    cid_aware = build_correlation_id(run_id="run-1", symbol="BTCUSDT", timeframe="1m", bar_ts=aware)
    cid_naive = build_correlation_id(run_id="run-1", symbol="BTCUSDT", timeframe="1m", bar_ts=naive)
    assert cid_aware == cid_naive
    assert cid_aware == "run-1:BTCUSDT:1m:2026-01-01T00:00:00.123Z"


def test_runtime_event_payload_validation_rejects_missing_required_fields() -> None:
    bar_ts = _bar_time()
    with pytest.raises(ValueError, match="payload.trade_id is required"):
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
            event_name=RuntimeEventName.ENTRY_FILLED,
            correlation_id=_correlation_id(),
            root_id="root-1",
            parent_id="parent-1",
            reason_code=ReasonCode.EXEC_ENTRY_FILLED,
            payload={
                "side": "buy",
                "qty": 1.0,
                "price": 100.0,
                "notional": 100.0,
                "wallet_delta": {
                    "collateral_reserved": 10.0,
                    "collateral_released": 0.0,
                    "fee_paid": 0.1,
                },
            },
        )


def test_runtime_event_payload_validation_rejects_negative_wallet_delta() -> None:
    with pytest.raises(ValueError, match="payload.wallet_delta.collateral_reserved must be >= 0"):
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=_bar_time(),
            event_name=RuntimeEventName.ENTRY_FILLED,
            correlation_id=_correlation_id(),
            root_id="root-1",
            parent_id="parent-1",
            reason_code=ReasonCode.EXEC_ENTRY_FILLED,
            payload={
                "trade_id": "trade-1",
                "correlation_id": "trade:trade-1",
                "reservation_id": "res-trade-1",
                "side": "buy",
                "qty": 1.0,
                "price": 100.0,
                "notional": 100.0,
                "wallet_delta": {
                    "collateral_reserved": -1.0,
                    "collateral_released": 0.0,
                    "fee_paid": 0.1,
                },
            },
        )


def test_runtime_error_allows_missing_parent_and_self_roots() -> None:
    event = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="__runtime__",
        symbol=None,
        timeframe=None,
        bar_ts=None,
        event_name=RuntimeEventName.RUNTIME_ERROR,
        correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
        reason_code=ReasonCode.RUNTIME_EXCEPTION,
        payload={"exception_type": "RuntimeError", "message": "boom", "location": "runtime.loop"},
    )
    assert event.parent_id is None
    assert event.root_id == event.event_id


def test_missing_parent_can_still_emit_with_runtime_parent_missing_reason() -> None:
    event = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=_bar_time(),
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=_correlation_id(),
        reason_code=ReasonCode.RUNTIME_PARENT_MISSING,
        payload={
            "trade_id": "trade-1",
            "correlation_id": "trade:trade-1",
            "reservation_id": None,
            "side": "buy",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "parent_missing": True,
            "missing_parent_hint": "decision event missing",
            "wallet_delta": {
                "collateral_reserved": 50.0,
                "collateral_released": 0.0,
                "fee_paid": 1.0,
                "balance_delta": -1.0,
            },
        },
        allow_missing_parent=True,
    )
    assert event.parent_id is None
    assert event.root_id == event.event_id
    assert event.reason_code == ReasonCode.RUNTIME_PARENT_MISSING


def test_wallet_projection_from_runtime_events_replays_margin_lifecycle() -> None:
    bar_ts = _bar_time()
    events = [
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
            payload={"balances": {"USD": 1000.0}, "source": "run_start"},
        ),
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
            event_name=RuntimeEventName.SIGNAL_EMITTED,
            correlation_id=_correlation_id(),
            reason_code=ReasonCode.SIGNAL_STRATEGY_SIGNAL,
            payload={"signal_type": "strategy_signal", "direction": "long", "signal_price": 100.0},
        ),
    ]
    signal = events[1]
    decision = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=bar_ts,
        event_name=RuntimeEventName.DECISION_ACCEPTED,
        correlation_id=_correlation_id(),
        root_id=signal.event_id,
        parent_id=signal.event_id,
        reason_code=ReasonCode.DECISION_ACCEPTED,
        payload={"decision": "accepted", "trade_id": "trade-1"},
    )
    events.append(decision)
    entry = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=bar_ts,
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=_correlation_id(),
        root_id=signal.event_id,
        parent_id=decision.event_id,
        reason_code=ReasonCode.EXEC_ENTRY_FILLED,
        payload={
            "trade_id": "trade-1",
            "correlation_id": "trade:trade-1",
            "reservation_id": "res-trade-1",
            "side": "buy",
            "qty": 2.0,
            "price": 100.0,
            "notional": 200.0,
            "accounting_mode": "margin",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "wallet_delta": {
                "collateral_reserved": 100.0,
                "collateral_released": 0.0,
                "fee_paid": 1.0,
                "balance_delta": -1.0,
            },
        },
    )
    events.append(entry)
    events.append(
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
            event_name=RuntimeEventName.EXIT_FILLED,
            correlation_id=_correlation_id(),
            root_id=signal.event_id,
            parent_id=entry.event_id,
            reason_code=ReasonCode.EXEC_EXIT_TARGET,
            payload={
                "trade_id": "trade-1",
                "correlation_id": "trade:trade-1",
                "reservation_id": "res-trade-1-exit-1",
                "side": "sell",
                "qty": 1.0,
                "price": 110.0,
                "notional": 110.0,
                "accounting_mode": "margin",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "exit_kind": ExitKind.TARGET.value,
                "wallet_delta": {
                    "collateral_reserved": 0.0,
                    "collateral_released": 50.0,
                    "fee_paid": 1.0,
                    "balance_delta": 9.0,
                },
            },
        )
    )
    events.append(
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=bar_ts,
            event_name=RuntimeEventName.EXIT_FILLED,
            correlation_id=_correlation_id(),
            root_id=signal.event_id,
            parent_id=entry.event_id,
            reason_code=ReasonCode.EXEC_EXIT_CLOSE,
            payload={
                "trade_id": "trade-1",
                "correlation_id": "trade:trade-1",
                "reservation_id": "res-trade-1-exit-2",
                "side": "sell",
                "qty": 1.0,
                "price": 96.0,
                "notional": 96.0,
                "accounting_mode": "margin",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "exit_kind": ExitKind.CLOSE.value,
                "wallet_delta": {
                    "collateral_reserved": 0.0,
                    "collateral_released": 50.0,
                    "fee_paid": 1.0,
                    "balance_delta": -5.0,
                },
            },
        )
    )

    state = project_wallet_from_events(events)
    assert abs(state.balances["USD"] - 1003.0) <= 1e-9
    assert abs(state.locked_margin.get("USD", 0.0) - 0.0) <= 1e-9
    assert abs(state.free_collateral["USD"] - 1003.0) <= 1e-9
    assert "trade-1" not in state.margin_positions


def test_wallet_projection_rejects_over_release() -> None:
    bar_ts = _bar_time()
    signal = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=bar_ts,
        event_name=RuntimeEventName.SIGNAL_EMITTED,
        correlation_id=_correlation_id(),
        reason_code=ReasonCode.SIGNAL_STRATEGY_SIGNAL,
        payload={"signal_type": "strategy_signal", "direction": "long", "signal_price": 100.0},
    )
    decision = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=bar_ts,
        event_name=RuntimeEventName.DECISION_ACCEPTED,
        correlation_id=_correlation_id(),
        root_id=signal.event_id,
        parent_id=signal.event_id,
        reason_code=ReasonCode.DECISION_ACCEPTED,
        payload={"decision": "accepted", "trade_id": "trade-1"},
    )
    entry = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=bar_ts,
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=_correlation_id(),
        root_id=signal.event_id,
        parent_id=decision.event_id,
        reason_code=ReasonCode.EXEC_ENTRY_FILLED,
        payload={
            "trade_id": "trade-1",
            "correlation_id": "trade:trade-1",
            "reservation_id": "res-trade-1-entry",
            "side": "buy",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "accounting_mode": "margin",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "wallet_delta": {
                "collateral_reserved": 50.0,
                "collateral_released": 0.0,
                "fee_paid": 1.0,
                "balance_delta": -1.0,
            },
        },
    )
    over_release = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=bar_ts,
        event_name=RuntimeEventName.EXIT_FILLED,
        correlation_id=_correlation_id(),
        root_id=signal.event_id,
        parent_id=entry.event_id,
        reason_code=ReasonCode.EXEC_EXIT_CLOSE,
        payload={
            "trade_id": "trade-1",
            "correlation_id": "trade:trade-1",
            "reservation_id": "res-trade-1-exit",
            "side": "sell",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "accounting_mode": "margin",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "exit_kind": ExitKind.CLOSE.value,
            "wallet_delta": {
                "collateral_reserved": 0.0,
                "collateral_released": 60.0,
                "fee_paid": 1.0,
                "balance_delta": -1.0,
            },
        },
    )
    with pytest.raises(ValueError, match="release exceeds reserve"):
        project_wallet_from_events([entry, over_release])


def test_wallet_projection_is_idempotent_for_duplicate_event_ids() -> None:
    init = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="__runtime__",
        symbol=None,
        timeframe=None,
        bar_ts=None,
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
        payload={"balances": {"USDC": 100.0}, "source": "run_start"},
    )
    deposit = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="__runtime__",
        symbol=None,
        timeframe=None,
        bar_ts=None,
        event_name=RuntimeEventName.WALLET_DEPOSITED,
        correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
        payload={"asset": "USDC", "amount": 10.0},
    )
    state = project_wallet_from_events([init, deposit, deposit])
    assert abs(state.balances["USDC"] - 110.0) <= 1e-9


def test_runtime_event_storage_has_unique_event_id_constraint() -> None:
    models_path = Path(__file__).resolve().parents[2] / "portal" / "backend" / "db" / "models.py"
    source = models_path.read_text(encoding="utf-8")
    assert "uq_portal_bot_run_events_event_id" in source


def test_runtime_source_enforces_pool_series_runner_only() -> None:
    runtime_path = (
        Path(__file__).resolve().parents[2]
        / "portal"
        / "backend"
        / "service"
        / "bots"
        / "bot_runtime"
        / "runtime"
        / "runtime.py"
    )
    source = runtime_path.read_text(encoding="utf-8")
    assert "return \"pool\"" in source
    assert "Expected 'pool'" in source


def test_runtime_source_requires_shared_wallet_proxy() -> None:
    runtime_path = (
        Path(__file__).resolve().parents[2]
        / "portal"
        / "backend"
        / "service"
        / "bots"
        / "bot_runtime"
        / "runtime"
        / "runtime.py"
    )
    source = runtime_path.read_text(encoding="utf-8")
    assert "shared_wallet_proxy is required for bot runtime" in source


def test_wallet_gateway_source_exposes_shared_only_implementation() -> None:
    gateway_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "engines"
        / "bot_runtime"
        / "core"
        / "wallet_gateway.py"
    )
    source = gateway_path.read_text(encoding="utf-8")
    assert "class SharedWalletGateway" in source
    assert "class BaseWalletGateway" in source
    assert "class LedgerWalletGateway" not in source


def test_shared_wallet_gateway_uses_runtime_event_projection() -> None:
    runtime_events = [
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
            payload={"balances": {"USDC": 100.0}, "source": "run_start"},
        ).serialize(),
    ]
    proxy = {
        "runtime_events": runtime_events,
        "reservations": {},
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)
    allowed, reason, _details = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=0.5,
        notional=50.0,
        fee=0.1,
        short_requires_borrow=False,
        reserve=False,
    )
    assert allowed is True
    assert reason is None

    entry_event = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=_bar_time(),
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=_correlation_id(),
        reason_code=ReasonCode.RUNTIME_PARENT_MISSING,
        payload={
            "trade_id": "trade-1",
            "correlation_id": "trade:trade-1",
            "reservation_id": None,
            "side": "buy",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "accounting_mode": "margin",
            "base_currency": "BTC",
            "quote_currency": "USDC",
            "wallet_delta": {
                "collateral_reserved": 80.0,
                "collateral_released": 0.0,
                "fee_paid": 0.0,
                "balance_delta": 0.0,
            },
        },
        allow_missing_parent=True,
    )
    runtime_events.append(entry_event.serialize())
    allowed_after_entry, _, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=0.5,
        notional=30.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert allowed_after_entry is False


def test_shared_wallet_gateway_reservation_held_until_entry_event_arrives() -> None:
    runtime_events = [
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
            payload={"balances": {"USDC": 100.0}, "source": "run_start"},
        ).serialize(),
    ]
    reservations: dict[str, dict[str, object]] = {}
    proxy = {
        "runtime_events": runtime_events,
        "reservations": reservations,
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)
    allowed, reason, details = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=60.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=True,
    )
    assert allowed is True
    assert reason is None
    reservation_id = str(details.get("reservation_id") or "")
    assert reservation_id

    gateway.apply_fill(
        event_type="ENTRY_FILL",
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        price=100.0,
        fee=0.0,
        notional=60.0,
        trade_id="trade-reserved",
        accounting_mode="margin",
        reservation_id=reservation_id,
    )

    blocked, blocked_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert blocked is False
    assert blocked_reason == "WALLET_INSUFFICIENT_CASH"

    runtime_events.append(
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="strategy-1",
            symbol="BTCUSDT",
            timeframe="1m",
            bar_ts=_bar_time(),
            event_name=RuntimeEventName.ENTRY_FILLED,
            correlation_id=_correlation_id(),
            reason_code=ReasonCode.RUNTIME_PARENT_MISSING,
            payload={
                "trade_id": "trade-reserved",
                "correlation_id": "trade:trade-reserved",
                "reservation_id": reservation_id,
                "side": "buy",
                "qty": 1.0,
                "price": 100.0,
                "notional": 100.0,
                "accounting_mode": "margin",
                "base_currency": "BTC",
                "quote_currency": "USDC",
                "wallet_delta": {
                    "collateral_reserved": 0.0,
                    "collateral_released": 0.0,
                    "fee_paid": 0.0,
                    "balance_delta": 0.0,
                },
            },
            allow_missing_parent=True,
        ).serialize()
    )

    allowed_after_event, allowed_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert allowed_after_event is True
    assert allowed_reason is None
    assert reservation_id not in reservations


def test_shared_wallet_gateway_active_reservation_reduces_available_collateral() -> None:
    runtime_events = [
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
            payload={"balances": {"USDC": 100.0}, "source": "run_start"},
        ).serialize()
    ]
    proxy = {
        "runtime_events": runtime_events,
        "reservations": {},
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)
    allowed, reason, _details = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=60.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=True,
        trade_id="trade-active",
        correlation_id="trade:trade-active",
    )
    assert allowed is True
    assert reason is None

    blocked, blocked_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert blocked is False
    assert blocked_reason == "WALLET_INSUFFICIENT_CASH"


def test_shared_wallet_gateway_active_reservation_expires_after_ttl() -> None:
    runtime_events = [
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
            payload={"balances": {"USDC": 100.0}, "source": "run_start"},
        ).serialize()
    ]
    reservations: dict[str, dict[str, object]] = {}
    proxy = {
        "runtime_events": runtime_events,
        "reservations": reservations,
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy, reservation_ttl_seconds=0.05, consumed_timeout_seconds=0.2)
    allowed, reason, details = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=70.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=True,
        trade_id="trade-expire",
        correlation_id="trade:trade-expire",
    )
    assert allowed is True
    assert reason is None
    reservation_id = str(details.get("reservation_id") or "")
    assert reservation_id
    time.sleep(0.08)

    allowed_after_expiry, reason_after_expiry, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert allowed_after_expiry is True
    assert reason_after_expiry is None
    assert reservations[reservation_id]["status"] == "EXPIRED"


def test_shared_wallet_gateway_consumed_reservation_clears_only_when_observed_after_watermark() -> None:
    init_event = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="__runtime__",
        symbol=None,
        timeframe=None,
        bar_ts=None,
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
        payload={"balances": {"USDC": 100.0}, "source": "run_start"},
    ).serialize()
    init_event["seq"] = 0
    stale_entry = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=_bar_time(),
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=_correlation_id(),
        reason_code=ReasonCode.RUNTIME_PARENT_MISSING,
        payload={
            "trade_id": "trade-seq",
            "correlation_id": "trade:trade-seq",
            "reservation_id": None,
            "side": "buy",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "accounting_mode": "margin",
            "base_currency": "BTC",
            "quote_currency": "USDC",
            "wallet_delta": {
                "collateral_reserved": 0.0,
                "collateral_released": 0.0,
                "fee_paid": 0.0,
                "balance_delta": 0.0,
            },
        },
        allow_missing_parent=True,
    ).serialize()
    stale_entry["seq"] = 1
    runtime_events = [init_event, stale_entry]
    reservations: dict[str, dict[str, object]] = {}
    proxy = {
        "runtime_events": runtime_events,
        "reservations": reservations,
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)
    allowed, reason, details = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=60.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=True,
        trade_id="trade-seq",
        correlation_id="trade:trade-seq",
    )
    assert allowed is True
    assert reason is None
    reservation_id = str(details.get("reservation_id") or "")
    assert reservation_id

    gateway.apply_fill(
        event_type="ENTRY_FILL",
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        price=100.0,
        fee=0.0,
        notional=60.0,
        trade_id="trade-seq",
        accounting_mode="margin",
        reservation_id=reservation_id,
        correlation_id="trade:trade-seq",
    )

    still_blocked, still_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert still_blocked is False
    assert still_reason == "WALLET_INSUFFICIENT_CASH"
    assert reservations[reservation_id]["status"] == "CONSUMED"
    assert gateway._last_seen_seq >= 1

    fresh_entry = dict(stale_entry)
    fresh_entry["event_id"] = "fresh-entry-id"
    fresh_entry["seq"] = 2
    runtime_events.append(fresh_entry)

    allowed_after_observed, reason_after_observed, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert allowed_after_observed is True
    assert reason_after_observed is None
    assert reservation_id not in reservations


def test_shared_wallet_gateway_can_apply_reserve_atomicity() -> None:
    runtime_events = [
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
            payload={"balances": {"USDC": 100.0}, "source": "run_start"},
        ).serialize()
    ]
    reservations: dict[str, dict[str, object]] = {}
    proxy = {
        "runtime_events": runtime_events,
        "reservations": reservations,
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)
    barrier = threading.Barrier(3)
    outcomes: list[tuple[bool, Optional[str]]] = []

    def _attempt(name: str) -> None:
        barrier.wait()
        allowed, reason, _ = gateway.can_apply(
            side="buy",
            base_currency="BTC",
            quote_currency="USDC",
            qty=1.0,
            notional=60.0,
            fee=0.0,
            short_requires_borrow=False,
            reserve=True,
            trade_id=name,
            correlation_id=f"trade:{name}",
        )
        outcomes.append((allowed, reason))

    t1 = threading.Thread(target=_attempt, args=("trade-a",))
    t2 = threading.Thread(target=_attempt, args=("trade-b",))
    t1.start()
    t2.start()
    barrier.wait()
    t1.join()
    t2.join()

    assert len(outcomes) == 2
    assert sum(1 for allowed, _reason in outcomes if allowed) == 1
    active = [payload for payload in reservations.values() if payload.get("status") == "ACTIVE"]
    assert len(active) == 1


def test_shared_wallet_gateway_apply_fill_marks_consumed_and_returns_metadata_without_wallet_mutation() -> None:
    runtime_events = [
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
            payload={"balances": {"USDC": 100.0}, "source": "run_start"},
        ).serialize()
    ]
    reservations: dict[str, dict[str, object]] = {}
    proxy = {
        "runtime_events": runtime_events,
        "reservations": reservations,
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)
    allowed, reason, details = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=40.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=True,
        trade_id="trade-meta",
        correlation_id="trade:trade-meta",
    )
    assert allowed is True
    assert reason is None
    reservation_id = str(details.get("reservation_id") or "")
    assert reservation_id

    before = gateway.project()
    metadata = gateway.apply_fill(
        event_type="ENTRY_FILL",
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        price=100.0,
        fee=0.0,
        notional=40.0,
        trade_id="trade-meta",
        reservation_id=reservation_id,
        correlation_id="trade:trade-meta",
    )
    after = gateway.project()

    assert before.balances == after.balances
    assert before.locked_margin == after.locked_margin
    assert metadata.get("reservation_id") == reservation_id
    assert metadata.get("reservation_status") == "CONSUMED"
    assert isinstance(metadata.get("wallet_delta"), dict)
    assert reservations[reservation_id]["status"] == "CONSUMED"


def test_shared_wallet_gateway_observation_prefers_reservation_id_then_entry_fallback() -> None:
    init_event = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="__runtime__",
        symbol=None,
        timeframe=None,
        bar_ts=None,
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
        payload={"balances": {"USDC": 100.0}, "source": "run_start"},
    ).serialize()
    init_event["seq"] = 0
    runtime_events = [init_event]
    reservations: dict[str, dict[str, object]] = {}
    proxy = {"runtime_events": runtime_events, "reservations": reservations, "lock": threading.RLock()}
    gateway = SharedWalletGateway(proxy)

    allowed, _, details = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=60.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=True,
        trade_id="trade-observe",
        correlation_id="trade:trade-observe",
    )
    assert allowed is True
    reservation_id = str(details["reservation_id"])
    gateway.apply_fill(
        event_type="ENTRY_FILL",
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        price=100.0,
        fee=0.0,
        notional=60.0,
        trade_id="trade-observe",
        reservation_id=reservation_id,
        correlation_id="trade:trade-observe",
    )

    mismatched = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=_bar_time(),
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=_correlation_id(),
        reason_code=ReasonCode.RUNTIME_PARENT_MISSING,
        payload={
            "trade_id": "trade-observe",
            "correlation_id": "trade:trade-observe",
            "reservation_id": "different-reservation-id",
            "side": "buy",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "accounting_mode": "margin",
            "base_currency": "BTC",
            "quote_currency": "USDC",
            "wallet_delta": {
                "collateral_reserved": 0.0,
                "collateral_released": 0.0,
                "fee_paid": 0.0,
                "balance_delta": 0.0,
            },
        },
        allow_missing_parent=True,
    ).serialize()
    mismatched["seq"] = 1
    runtime_events.append(mismatched)

    blocked, blocked_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert blocked is False
    assert blocked_reason == "WALLET_INSUFFICIENT_CASH"
    assert reservation_id in reservations

    legacy = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=_bar_time(),
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=_correlation_id(),
        reason_code=ReasonCode.RUNTIME_PARENT_MISSING,
        payload={
            "trade_id": "trade-observe",
            "correlation_id": "trade:trade-observe",
            "reservation_id": None,
            "side": "buy",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "accounting_mode": "margin",
            "base_currency": "BTC",
            "quote_currency": "USDC",
            "wallet_delta": {
                "collateral_reserved": 0.0,
                "collateral_released": 0.0,
                "fee_paid": 0.0,
                "balance_delta": 0.0,
            },
        },
        allow_missing_parent=True,
    ).serialize()
    legacy["payload"].pop("reservation_id", None)
    legacy["event_id"] = "legacy-entry-event"
    legacy["seq"] = 2
    runtime_events.append(legacy)

    allowed_after_legacy, allowed_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert allowed_after_legacy is True
    assert allowed_reason is None
    assert reservation_id not in reservations


def test_shared_wallet_gateway_exit_fallback_requires_exit_kind() -> None:
    init_event = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="__runtime__",
        symbol=None,
        timeframe=None,
        bar_ts=None,
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
        payload={"balances": {"USDC": 100.0}, "source": "run_start"},
    ).serialize()
    init_event["seq"] = 0
    runtime_events = [init_event]
    reservations: dict[str, dict[str, object]] = {}
    proxy = {"runtime_events": runtime_events, "reservations": reservations, "lock": threading.RLock()}
    gateway = SharedWalletGateway(proxy)

    allowed, _, details = gateway.can_apply(
        side="sell",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=60.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=True,
        trade_id="trade-exit",
        correlation_id="trade:trade-exit",
    )
    assert allowed is True
    reservation_id = str(details["reservation_id"])
    gateway.apply_fill(
        event_type="EXIT_FILL",
        side="sell",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        price=100.0,
        fee=0.0,
        notional=60.0,
        trade_id="trade-exit",
        reservation_id=reservation_id,
        correlation_id="trade:trade-exit",
        exit_kind="TARGET",
    )

    wrong_kind = new_runtime_event(
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        symbol="BTCUSDT",
        timeframe="1m",
        bar_ts=_bar_time(),
        event_name=RuntimeEventName.EXIT_FILLED,
        correlation_id=_correlation_id(),
        reason_code=ReasonCode.RUNTIME_PARENT_MISSING,
        payload={
            "trade_id": "trade-exit",
            "correlation_id": "trade:trade-exit",
            "reservation_id": None,
            "side": "sell",
            "qty": 1.0,
            "price": 100.0,
            "notional": 100.0,
            "accounting_mode": "margin",
            "base_currency": "BTC",
            "quote_currency": "USDC",
            "exit_kind": ExitKind.STOP.value,
            "wallet_delta": {
                "collateral_reserved": 0.0,
                "collateral_released": 0.0,
                "fee_paid": 0.0,
                "balance_delta": 0.0,
            },
        },
        allow_missing_parent=True,
    ).serialize()
    wrong_kind["payload"].pop("reservation_id", None)
    wrong_kind["seq"] = 1
    runtime_events.append(wrong_kind)

    still_blocked, still_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert still_blocked is False
    assert still_reason == "WALLET_INSUFFICIENT_CASH"
    assert reservation_id in reservations

    matching_kind = dict(wrong_kind)
    matching_kind["event_id"] = "legacy-exit-target"
    matching_kind["seq"] = 2
    matching_kind["payload"] = dict(wrong_kind["payload"])
    matching_kind["payload"]["exit_kind"] = ExitKind.TARGET.value
    runtime_events.append(matching_kind)

    allowed_after_match, allowed_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=50.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert allowed_after_match is True
    assert allowed_reason is None
    assert reservation_id not in reservations


def test_shared_wallet_gateway_hold_uses_collateral_plus_fee_estimate() -> None:
    runtime_events = [
        new_runtime_event(
            run_id="run-1",
            bot_id="bot-1",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            event_name=RuntimeEventName.WALLET_INITIALIZED,
            correlation_id=build_correlation_id(run_id="run-1", symbol=None, timeframe=None, bar_ts=None),
            payload={"balances": {"USDC": 100.0}, "source": "run_start"},
        ).serialize()
    ]
    proxy = {"runtime_events": runtime_events, "reservations": {}, "lock": threading.RLock()}
    gateway = SharedWalletGateway(proxy)
    allowed, reason, details = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=60.0,
        fee=2.0,
        short_requires_borrow=False,
        reserve=True,
        trade_id="trade-fee-hold",
        correlation_id="trade:trade-fee-hold",
    )
    assert allowed is True
    assert reason is None
    required_delta = dict(details.get("required_delta") or {})
    assert required_delta.get("collateral_reserved") == pytest.approx(60.0)
    assert required_delta.get("fee_estimate") == pytest.approx(2.0)
    assert float(details.get("reserved_amount") or 0.0) == pytest.approx(62.0)

    blocked, blocked_reason, _ = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=1.0,
        notional=39.0,
        fee=0.0,
        short_requires_borrow=False,
        reserve=False,
    )
    assert blocked is False
    assert blocked_reason == "WALLET_INSUFFICIENT_CASH"
