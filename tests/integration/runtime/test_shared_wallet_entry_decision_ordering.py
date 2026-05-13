from __future__ import annotations

import threading
import time
from typing import Callable, Dict, List, Mapping, Optional

from engines.bot_runtime.runtime.components.entry_decision_ordering import (
    SharedWalletEntryDecisionOrderCoordinator,
    stable_entry_decision_sort_key,
)


BAR_TIME = "2026-01-14T04:00:00Z"
STRATEGY_ID = "strategy-1"


def _proxy(expected_count: int) -> Dict[str, object]:
    return {
        "lock": threading.RLock(),
        "decision_order_state": {},
        "decision_order_participants": {},
        "decision_order_expected_count": expected_count,
    }


def _participant(symbol: str, *, next_bar_time: Optional[str] = None) -> Dict[str, object]:
    return {
        "participant_key": f"{STRATEGY_ID}|instrument-{symbol}|{symbol}|1h",
        "strategy_id": STRATEGY_ID,
        "instrument_id": f"instrument-{symbol}",
        "symbol": symbol,
        "timeframe": "1h",
        "next_bar_time": next_bar_time,
    }


def _candidate(
    symbol: str,
    *,
    bar_time: str = BAR_TIME,
    entry_request_id: Optional[str] = None,
    priority: int = 0,
) -> Dict[str, object]:
    return {
        "bar_time": bar_time,
        "strategy_id": STRATEGY_ID,
        "instrument_id": f"instrument-{symbol}",
        "symbol": symbol,
        "timeframe": "1h",
        "signal_priority": priority,
        "direction": "long",
        "side": "buy",
        "decision_id": f"{STRATEGY_ID}:instrument-{symbol}:1768363200:rule-1",
        "entry_request_id": entry_request_id or f"entry_request:{symbol}",
    }


def _run_candidate(
    coordinator: SharedWalletEntryDecisionOrderCoordinator,
    *,
    participant: Mapping[str, object],
    candidate: Mapping[str, object],
    bar_time: str = BAR_TIME,
    delay_seconds: float = 0.0,
    on_turn: Optional[Callable[[], str]] = None,
    mark_complete: bool = False,
    errors: List[BaseException],
) -> None:
    try:
        if delay_seconds:
            time.sleep(delay_seconds)
        ticket = coordinator.arrive_and_wait_turn(
            participant=participant,
            bar={"bar_time": bar_time, "timeframe": "1h"},
            candidate=candidate,
        )
        outcome = on_turn() if on_turn is not None else "accepted"
        coordinator.complete_candidate(ticket, outcome=outcome)
        coordinator.wait_until_complete(ticket)
        if mark_complete:
            coordinator.mark_participant_complete(participant)
    except BaseException as exc:  # noqa: BLE001 - tests need to re-raise thread failures.
        errors.append(exc)


def _run_no_candidate(
    coordinator: SharedWalletEntryDecisionOrderCoordinator,
    *,
    participant: Mapping[str, object],
    bar_time: str,
    next_bar_time: Optional[str] = None,
    delay_seconds: float = 0.0,
    mark_complete: bool = False,
    on_complete: Optional[Callable[[], None]] = None,
    errors: List[BaseException],
) -> None:
    try:
        if delay_seconds:
            time.sleep(delay_seconds)
        ticket = coordinator.arrive_and_wait_turn(
            participant=participant,
            bar={"bar_time": bar_time, "timeframe": "1h"},
            candidate=None,
        )
        assert ticket is None
        coordinator.wait_until_complete(ticket)
        if next_bar_time is not None:
            coordinator.update_participant_bar_state(
                {
                    **dict(participant),
                    "next_bar_time": next_bar_time,
                }
            )
        if on_complete is not None:
            on_complete()
        if mark_complete:
            coordinator.mark_participant_complete(participant)
    except BaseException as exc:  # noqa: BLE001 - tests need to re-raise thread failures.
        errors.append(exc)


def _join(threads: List[threading.Thread], errors: List[BaseException]) -> None:
    for thread in threads:
        thread.join(timeout=2.0)
    alive = [thread.name for thread in threads if thread.is_alive()]
    assert alive == []
    if errors:
        raise errors[0]


def test_delayed_same_bar_candidate_releases_in_deterministic_order() -> None:
    proxy = _proxy(expected_count=2)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP")
    etp = _participant("ETP")
    coordinator.register_participant(bip)
    coordinator.register_participant(etp)
    order: List[str] = []
    errors: List[BaseException] = []

    threads = [
        threading.Thread(
            name="etp-fast",
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": etp,
                "candidate": _candidate("ETP"),
                "on_turn": lambda: order.append("ETP") or "accepted",
                "errors": errors,
            },
        ),
        threading.Thread(
            name="bip-slow",
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": bip,
                "candidate": _candidate("BIP"),
                "delay_seconds": 0.05,
                "on_turn": lambda: order.append("BIP") or "accepted",
                "errors": errors,
            },
        ),
    ]
    for thread in threads:
        thread.start()
    _join(threads, errors)

    assert order == ["BIP", "ETP"]


def test_overlay_cost_delay_keeps_same_bar_wallet_candidate_order_stable() -> None:
    def run(delay_bip: float, delay_etp: float) -> List[str]:
        proxy = _proxy(expected_count=2)
        coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
        bip = _participant("BIP")
        etp = _participant("ETP")
        coordinator.register_participant(bip)
        coordinator.register_participant(etp)
        order: List[str] = []
        errors: List[BaseException] = []
        threads = [
            threading.Thread(
                target=_run_candidate,
                kwargs={
                    "coordinator": coordinator,
                    "participant": bip,
                    "candidate": _candidate("BIP"),
                    "delay_seconds": delay_bip,
                    "on_turn": lambda: order.append("BIP") or "accepted",
                    "errors": errors,
                },
            ),
            threading.Thread(
                target=_run_candidate,
                kwargs={
                    "coordinator": coordinator,
                    "participant": etp,
                    "candidate": _candidate("ETP"),
                    "delay_seconds": delay_etp,
                    "on_turn": lambda: order.append("ETP") or "accepted",
                    "errors": errors,
                },
            ),
        ]
        for thread in threads:
            thread.start()
        _join(threads, errors)
        return order

    assert run(delay_bip=0.0, delay_etp=0.0) == ["BIP", "ETP"]
    assert run(delay_bip=0.05, delay_etp=0.0) == ["BIP", "ETP"]
    assert run(delay_bip=0.0, delay_etp=0.05) == ["BIP", "ETP"]


def test_same_bar_wallet_contention_uses_deterministic_candidate_order() -> None:
    proxy = _proxy(expected_count=2)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP")
    etp = _participant("ETP")
    coordinator.register_participant(bip)
    coordinator.register_participant(etp)
    remaining_slots = {"count": 1}
    accepted: List[str] = []
    rejected: List[str] = []
    errors: List[BaseException] = []

    def decide(symbol: str) -> str:
        if remaining_slots["count"] > 0:
            remaining_slots["count"] -= 1
            accepted.append(symbol)
            return "accepted"
        rejected.append(symbol)
        return "rejected"

    threads = [
        threading.Thread(
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": etp,
                "candidate": _candidate("ETP"),
                "on_turn": lambda: decide("ETP"),
                "errors": errors,
            },
        ),
        threading.Thread(
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": bip,
                "candidate": _candidate("BIP"),
                "delay_seconds": 0.05,
                "on_turn": lambda: decide("BIP"),
                "errors": errors,
            },
        ),
    ]
    for thread in threads:
        thread.start()
    _join(threads, errors)

    assert accepted == ["BIP"]
    assert rejected == ["ETP"]


def test_entry_request_final_tiebreaker_is_stable_across_repeated_sorts() -> None:
    candidates = [
        _candidate("BIP", entry_request_id="entry_request:b"),
        _candidate("BIP", entry_request_id="entry_request:a"),
        _candidate("ETP", entry_request_id="entry_request:c"),
    ]

    expected = [
        "entry_request:a",
        "entry_request:b",
        "entry_request:c",
    ]
    for _ in range(5):
        ordered = sorted(reversed(candidates), key=stable_entry_decision_sort_key)
        assert [str(candidate["entry_request_id"]) for candidate in ordered] == expected


def test_no_candidate_bars_do_not_create_arbitration_state_or_wait() -> None:
    proxy = _proxy(expected_count=2)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP")
    etp = _participant("ETP")
    coordinator.register_participant(bip)
    coordinator.register_participant(etp)

    started = time.monotonic()
    for idx in range(25):
        bar_time = f"2026-01-14T{idx % 24:02d}:00:00Z"
        ticket = coordinator.arrive_and_wait_turn(
            participant={**bip, "next_bar_time": bar_time},
            bar={"bar_time": bar_time, "timeframe": "1h"},
            candidate=None,
        )
        assert ticket is None
    elapsed = time.monotonic() - started

    assert elapsed < 0.5
    assert proxy["decision_order_state"] == {}


def test_no_candidate_bars_do_not_block_workers() -> None:
    proxy = _proxy(expected_count=2)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP")
    etp = _participant("ETP")
    coordinator.register_participant(bip)
    coordinator.register_participant(etp)
    advanced: List[str] = []
    errors: List[BaseException] = []

    def fast_no_candidate_worker() -> None:
        try:
            for hour in range(12):
                bar_time = f"2026-01-14T{hour:02d}:00:00Z"
                next_time = f"2026-01-14T{hour + 1:02d}:00:00Z"
                ticket = coordinator.arrive_and_wait_turn(
                    participant={**bip, "next_bar_time": bar_time},
                    bar={"bar_time": bar_time, "timeframe": "1h"},
                    candidate=None,
                )
                assert ticket is None
                coordinator.update_participant_bar_state({**bip, "next_bar_time": next_time})
                advanced.append(bar_time)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=fast_no_candidate_worker)
    thread.start()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert errors == []
    assert len(advanced) == 12
    assert proxy["decision_order_state"] == {}


def test_sparse_calendar_gap_placeholder_prevents_deadlock() -> None:
    ten = "2026-01-11T10:00:00Z"
    eleven = "2026-01-11T11:00:00Z"
    proxy = _proxy(expected_count=3)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP", next_bar_time=ten)
    etp = _participant("ETP", next_bar_time=ten)
    xpp = _participant("XPP", next_bar_time=eleven)
    for participant in (bip, etp, xpp):
        coordinator.register_participant(participant)
    completed: List[str] = []
    errors: List[BaseException] = []

    threads = [
        threading.Thread(
            target=_run_no_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": xpp,
                "bar_time": eleven,
                "on_complete": lambda: completed.append("XPP-11"),
                "errors": errors,
            },
        ),
        threading.Thread(
            target=_run_no_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": bip,
                "bar_time": ten,
                "next_bar_time": eleven,
                "delay_seconds": 0.05,
                "on_complete": lambda: completed.append("BIP-10"),
                "errors": errors,
            },
        ),
        threading.Thread(
            target=_run_no_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": etp,
                "bar_time": ten,
                "next_bar_time": eleven,
                "delay_seconds": 0.05,
                "on_complete": lambda: completed.append("ETP-10"),
                "errors": errors,
            },
        ),
    ]
    for thread in threads:
        thread.start()
    _join(threads, errors)

    assert sorted(completed) == ["BIP-10", "ETP-10", "XPP-11"]
    assert proxy["decision_order_state"] == {}


def test_sparse_calendar_wallet_contention_waits_for_same_bar_candidate() -> None:
    ten = "2026-01-11T10:00:00Z"
    eleven = "2026-01-11T11:00:00Z"
    proxy = _proxy(expected_count=3)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP", next_bar_time=ten)
    etp = _participant("ETP", next_bar_time=ten)
    xpp = _participant("XPP", next_bar_time=eleven)
    for participant in (bip, etp, xpp):
        coordinator.register_participant(participant)
    order: List[str] = []
    errors: List[BaseException] = []

    threads = [
        threading.Thread(
            target=_run_no_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": xpp,
                "bar_time": eleven,
                "errors": errors,
            },
        ),
        threading.Thread(
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": etp,
                "candidate": _candidate("ETP", bar_time=ten),
                "bar_time": ten,
                "on_turn": lambda: order.append("ETP") or "accepted",
                "errors": errors,
            },
        ),
        threading.Thread(
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": bip,
                "candidate": _candidate("BIP", bar_time=ten),
                "bar_time": ten,
                "delay_seconds": 0.05,
                "on_turn": lambda: order.append("BIP") or "accepted",
                "errors": errors,
            },
        ),
    ]
    for thread in threads:
        thread.start()
    _join(threads, errors)

    assert order == ["BIP", "ETP"]


def test_future_bar_worker_can_advance_without_prior_bar_wait() -> None:
    ten = "2026-01-11T10:00:00Z"
    eleven = "2026-01-11T11:00:00Z"
    proxy = _proxy(expected_count=3)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP", next_bar_time=ten)
    etp = _participant("ETP", next_bar_time=ten)
    xpp = _participant("XPP", next_bar_time=eleven)
    for participant in (bip, etp, xpp):
        coordinator.register_participant(participant)
    events: List[str] = []
    errors: List[BaseException] = []

    threads = [
        threading.Thread(
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": xpp,
                "candidate": _candidate("XPP", bar_time=eleven),
                "bar_time": eleven,
                "on_turn": lambda: events.append("XPP-11-turn") or "accepted",
                "errors": errors,
            },
        ),
        threading.Thread(
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": bip,
                "candidate": _candidate("BIP", bar_time=ten),
                "bar_time": ten,
                "delay_seconds": 0.05,
                "on_turn": lambda: time.sleep(0.05) or events.append("BIP-10-turn") or "accepted",
                "mark_complete": True,
                "errors": errors,
            },
        ),
        threading.Thread(
            target=_run_no_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": etp,
                "bar_time": ten,
                "next_bar_time": eleven,
                "delay_seconds": 0.05,
                "mark_complete": True,
                "errors": errors,
            },
        ),
    ]
    for thread in threads:
        thread.start()
    _join(threads, errors)

    assert events == ["XPP-11-turn", "BIP-10-turn"]


def test_sparse_gaps_use_compact_progress_metadata_not_bar_placeholders() -> None:
    nine = "2026-01-11T09:00:00Z"
    eleven = "2026-01-11T11:00:00Z"
    proxy = _proxy(expected_count=2)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP", next_bar_time=nine)
    xpp = _participant("XPP", next_bar_time=nine)
    coordinator.register_participant(bip)
    coordinator.register_participant(xpp)

    coordinator.update_participant_bar_state({**xpp, "next_bar_time": eleven})

    xpp_state = proxy["decision_order_participants"][xpp["participant_key"]]
    assert len(xpp_state["gap_ranges"]) == 1
    assert xpp_state["gap_ranges"][0]["start"] == "2026-01-11T10:00:00Z"
    assert xpp_state["gap_ranges"][0]["end"] == eleven
    assert proxy["decision_order_state"] == {}


def test_coordinator_state_stays_bounded_across_many_no_candidate_bars() -> None:
    proxy = _proxy(expected_count=2)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP")
    etp = _participant("ETP")
    coordinator.register_participant(bip)
    coordinator.register_participant(etp)

    for idx in range(500):
        bar_time = f"2026-02-{1 + idx // 24:02d}T{idx % 24:02d}:00:00Z"
        next_time = f"2026-02-{1 + (idx + 1) // 24:02d}T{(idx + 1) % 24:02d}:00:00Z"
        ticket = coordinator.arrive_and_wait_turn(
            participant={**bip, "next_bar_time": bar_time},
            bar={"bar_time": bar_time, "timeframe": "1h"},
            candidate=None,
        )
        assert ticket is None
        coordinator.update_participant_bar_state({**bip, "next_bar_time": next_time})

    assert proxy["decision_order_state"] == {}


def test_far_ahead_backtest_candidate_does_not_wait_for_lagging_symbols() -> None:
    candidate_bar = "2026-02-15T00:00:00Z"
    proxy = _proxy(expected_count=3)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=0.2, poll_interval_seconds=0.001)
    bip = _participant("BIP", next_bar_time="2025-12-17T16:00:00Z")
    etp = _participant("ETP", next_bar_time="2025-12-17T19:00:00Z")
    xpp = _participant("XPP", next_bar_time=candidate_bar)
    for participant in (bip, etp, xpp):
        coordinator.register_participant(participant)

    started = time.monotonic()
    ticket = coordinator.arrive_and_wait_turn(
        participant=xpp,
        bar={"bar_time": candidate_bar, "timeframe": "1h"},
        candidate=_candidate("XPP", bar_time=candidate_bar),
    )
    elapsed = time.monotonic() - started

    assert ticket is not None
    assert elapsed < 0.1
    coordinator.complete_candidate(ticket, outcome="accepted")
    coordinator.wait_until_complete(ticket)

    state = proxy["decision_order_state"][f"{candidate_bar}|1h"]
    assert state["complete"] is True
    assert state.get("missing_participants") in (None, [])
    assert state.get("error") in (None, "")


def test_unresolved_prior_sparse_bar_does_not_block_future_bar_candidate() -> None:
    ten = "2026-01-11T10:00:00Z"
    eleven = "2026-01-11T11:00:00Z"
    proxy = _proxy(expected_count=3)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=1.0, poll_interval_seconds=0.001)
    bip = _participant("BIP", next_bar_time=ten)
    etp = _participant("ETP", next_bar_time=ten)
    xpp = _participant("XPP", next_bar_time=eleven)
    for participant in (bip, etp, xpp):
        coordinator.register_participant(participant)

    events: List[str] = []
    errors: List[BaseException] = []
    bip_entered = threading.Event()

    def decide_bip() -> str:
        events.append("BIP-10-turn")
        return "accepted"

    bip_thread = threading.Thread(
        target=_run_candidate,
        kwargs={
            "coordinator": coordinator,
            "participant": bip,
            "candidate": _candidate("BIP", bar_time=ten),
            "bar_time": ten,
            "on_turn": lambda: bip_entered.set() or decide_bip(),
            "errors": errors,
        },
    )
    bip_thread.start()

    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        state = proxy["decision_order_state"].get(f"{ten}|1h") or {}
        if "ETP" in "".join(state.get("missing_participants") or []):
            break
        time.sleep(0.001)

    started = time.monotonic()
    xpp_ticket = coordinator.arrive_and_wait_turn(
        participant=xpp,
        bar={"bar_time": eleven, "timeframe": "1h"},
        candidate=_candidate("XPP", bar_time=eleven),
    )
    elapsed = time.monotonic() - started

    assert xpp_ticket is not None
    assert elapsed < 0.1
    events.append("XPP-11-turn")
    coordinator.complete_candidate(xpp_ticket, outcome="accepted")
    coordinator.wait_until_complete(xpp_ticket)

    assert not bip_entered.is_set()
    coordinator.update_participant_bar_state({**etp, "next_bar_time": eleven})
    bip_thread.join(timeout=1.0)

    assert not bip_thread.is_alive()
    assert errors == []
    assert events == ["XPP-11-turn", "BIP-10-turn"]


def test_unrelated_participant_failure_does_not_abort_arrived_candidate() -> None:
    proxy = _proxy(expected_count=2)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP")
    etp = _participant("ETP")
    coordinator.register_participant(bip)
    coordinator.register_participant(etp)
    gate = threading.Event()
    accepted: List[str] = []
    errors: List[BaseException] = []

    def decide() -> str:
        assert gate.wait(timeout=1.0)
        accepted.append("BIP")
        return "accepted"

    thread = threading.Thread(
        target=_run_candidate,
        kwargs={
            "coordinator": coordinator,
            "participant": bip,
            "candidate": _candidate("BIP"),
            "on_turn": decide,
            "errors": errors,
        },
    )
    thread.start()
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        state = proxy["decision_order_state"].get(f"{BAR_TIME}|1h") or {}
        if state.get("active_candidate"):
            break
        time.sleep(0.001)
    coordinator.mark_participant_failed(etp, error="worker crashed before bar")
    gate.set()
    thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert errors == []
    assert accepted == ["BIP"]
    state = proxy["decision_order_state"][f"{BAR_TIME}|1h"]
    assert state["complete"] is True
    assert state.get("error") in (None, "")
