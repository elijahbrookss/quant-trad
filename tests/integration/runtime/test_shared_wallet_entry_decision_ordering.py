from __future__ import annotations

import threading
import time
import multiprocessing as mp
from typing import Callable, Dict, List, Mapping, Optional

import pytest

from engines.bot_runtime.runtime.components.entry_decision_ordering import (
    SharedWalletEntryDecisionOrderCoordinator,
    stable_entry_decision_sort_key,
)
from engines.bot_runtime.runtime.components.runtime_policy import BacktestSharedWalletArbitrationPolicy


BAR_TIME = "2026-01-14T04:00:00Z"
STRATEGY_ID = "strategy-1"


def _proxy(expected_count: int) -> Dict[str, object]:
    return {
        "lock": threading.RLock(),
        "decision_order_state": {},
        "decision_order_participants": {},
        "decision_order_expected_count": expected_count,
        "decision_order_wait_top": [],
        "decision_order_wait_control": {},
        "decision_order_wait_total_ms": 0.0,
        "decision_order_wait_record_count": 0.0,
    }


def _backtest_coordinator(
    proxy: Mapping[str, object],
    *,
    timeout_seconds: float = 0.01,
) -> SharedWalletEntryDecisionOrderCoordinator:
    return SharedWalletEntryDecisionOrderCoordinator(
        proxy,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=0.001,
        arbitration_policy=BacktestSharedWalletArbitrationPolicy(),
    )


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


def _wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.001)
    assert predicate()


def _missing_participants(proxy: Mapping[str, object], bar_key: str) -> str:
    state_store = proxy["decision_order_state"]
    assert isinstance(state_store, Mapping)
    state = state_store.get(bar_key) or {}
    assert isinstance(state, Mapping)
    return "".join(str(participant) for participant in state.get("missing_participants") or [])


def test_candidate_ticket_records_wait_pressure_without_changing_release_semantics() -> None:
    proxy = _proxy(expected_count=2)
    coordinator = _backtest_coordinator(proxy, timeout_seconds=2.0)
    bip = _participant("BIP", next_bar_time=BAR_TIME)
    etp = _participant("ETP", next_bar_time=BAR_TIME)
    coordinator.register_participant(bip)
    coordinator.register_participant(etp)
    tickets = []
    errors: List[BaseException] = []

    def _run_waiting_candidate() -> None:
        try:
            ticket = coordinator.arrive_and_wait_turn(
                participant=etp,
                bar={"bar_time": BAR_TIME, "timeframe": "1h"},
                candidate=_candidate("ETP"),
            )
            tickets.append(ticket)
            coordinator.complete_candidate(ticket, outcome="accepted")
            coordinator.wait_until_complete(ticket)
        except BaseException as exc:  # noqa: BLE001 - tests need to re-raise thread failures.
            errors.append(exc)

    thread = threading.Thread(target=_run_waiting_candidate)
    thread.start()
    time.sleep(0.02)
    coordinator.update_participant_bar_state(
        {
            **bip,
            "next_bar_time": "2026-01-14T05:00:00Z",
        }
    )

    _join([thread], errors)

    assert tickets
    summary = dict(tickets[0].wait_summary)
    assert summary["outcome"] == "release"
    assert summary["release_count"] == 1
    assert summary["fail_count"] == 0
    assert summary["wait_count"] == 1
    assert summary["wait_poll_count"] >= 1
    assert summary["max_blocking_participant_count"] >= 1
    assert summary["candidate_symbol"] == "ETP"
    assert summary["candidate_timeframe"] == "1h"
    assert summary["candidate_bar_time"] == BAR_TIME
    assert summary["policy_name"] == "backtest_shared_wallet_arbitration"
    assert summary["wait_reason"] == "backtest_waiting_for_market_progress"
    assert summary["release_reason"] == "candidate_has_active_turn"
    diagnostic = dict(summary["wait_diagnostic"])
    assert diagnostic["materiality"] == "diagnostic"
    assert diagnostic["diagnostic_scope"] == "coordinator_wait_attribution"
    assert diagnostic["final_action"] == "released"
    assert diagnostic["candidate_symbol"] == "ETP"
    assert diagnostic["candidate_timeframe"] == "1h"
    assert diagnostic["candidate_bar_time"] == BAR_TIME
    assert diagnostic["wait_reason"] == "backtest_waiting_for_market_progress"
    assert diagnostic["release_reason"] == "candidate_has_active_turn"
    assert diagnostic["wait_elapsed_ms"] >= 1
    assert diagnostic["wait_poll_count"] >= 1
    assert diagnostic["blocking_participant_count"] == 1
    blockers = list(diagnostic["blocking_participants"])
    assert len(blockers) == 1
    assert blockers[0]["participant_symbol"] == "BIP"
    assert blockers[0]["participant_timeframe"] == "1h"
    assert blockers[0]["next_bar_time"] == BAR_TIME
    assert blockers[0]["next_bar_epoch"] is not None
    assert blockers[0]["first_next_bar_time"] == BAR_TIME
    assert blockers[0]["first_status"] == "active"
    assert blockers[0]["release_next_bar_time"] == "2026-01-14T05:00:00Z"
    assert blockers[0]["release_context"]["next_bar_time"] == "2026-01-14T05:00:00Z"
    assert diagnostic["wait_started_at"]
    assert diagnostic["wait_ended_at"]
    top_waits = list(coordinator.top_wait_diagnostics())
    assert len(top_waits) == 1
    assert top_waits[0]["candidate_symbol"] == "ETP"
    assert top_waits[0]["blocking_participants"][0]["participant_symbol"] == "BIP"


def test_merged_top_waits_include_true_longest_wait_across_workers() -> None:
    proxy = _proxy(expected_count=3)
    proxy["decision_order_wait_top_n"] = 2
    coordinator_a = _backtest_coordinator(proxy, timeout_seconds=2.0)
    coordinator_b = _backtest_coordinator(proxy, timeout_seconds=2.0)
    coordinator_c = _backtest_coordinator(proxy, timeout_seconds=2.0)
    for participant in (
        {**_participant("BIP"), "worker_id": "worker-a"},
        {**_participant("ETP"), "worker_id": "worker-b"},
        {**_participant("XPP"), "worker_id": "worker-c"},
    ):
        coordinator_a.register_participant(participant)

    with proxy["lock"]:
        coordinator_a._record_wait_diagnostic_locked(  # noqa: SLF001 - regression targets diagnostic retention.
            {
                "materiality": "diagnostic",
                "diagnostic_scope": "coordinator_wait_attribution",
                "candidate_symbol": "BIP",
                "candidate_timeframe": "1h",
                "candidate_bar_time": "2026-01-01T00:00:00Z",
                "wait_elapsed_ms": 50,
                "wait_count": 1,
                "wait_poll_count": 5,
                "final_action": "released",
                "release_reason": "candidate_has_active_turn",
                "worker_id": "worker-a",
            }
        )
        coordinator_b._record_wait_diagnostic_locked(  # noqa: SLF001
            {
                "materiality": "diagnostic",
                "diagnostic_scope": "coordinator_wait_attribution",
                "candidate_symbol": "XPP",
                "candidate_timeframe": "1h",
                "candidate_bar_time": "2026-02-15T00:00:00Z",
                "wait_elapsed_ms": 219000,
                "wait_count": 1,
                "wait_poll_count": 4800,
                "final_action": "released",
                "release_reason": "candidate_has_active_turn",
                "worker_id": "worker-b",
            }
        )
        coordinator_c._record_wait_diagnostic_locked(  # noqa: SLF001
            {
                "materiality": "diagnostic",
                "diagnostic_scope": "coordinator_wait_attribution",
                "candidate_symbol": "ETP",
                "candidate_timeframe": "1h",
                "candidate_bar_time": "2026-01-13T16:00:00Z",
                "wait_elapsed_ms": 26000,
                "wait_count": 1,
                "wait_poll_count": 540,
                "final_action": "released",
                "release_reason": "candidate_has_active_turn",
                "worker_id": "worker-c",
            }
        )

    for participant in (
        {**_participant("BIP"), "worker_id": "worker-a"},
        {**_participant("ETP"), "worker_id": "worker-b"},
        {**_participant("XPP"), "worker_id": "worker-c"},
    ):
        coordinator_a.mark_participant_complete(participant)

    latest_worker_snapshot = [
        item for item in coordinator_a.top_wait_diagnostics() if item.get("worker_id") == "worker-a"
    ]
    assert latest_worker_snapshot == []

    merged = coordinator_a.claim_merged_wait_diagnostics()
    assert merged is not None
    assert merged["aggregation_level"] == "run"
    assert merged["materiality"] == "diagnostic"
    assert merged["source_reason"] == "run_final"
    assert merged["top_n"] == 2
    assert merged["total_wait_count"] == 3.0
    assert merged["total_wait_ms"] == 245050.0
    assert merged["workers_included"] == ["worker-a", "worker-b", "worker-c"]
    assert merged["top_wait_workers_included"] == ["worker-b", "worker-c"]
    waits = list(merged["top_waits"])
    assert [item["candidate_symbol"] for item in waits] == ["XPP", "ETP"]
    assert [item["wait_elapsed_ms"] for item in waits] == [219000, 26000]
    assert [item["diagnostic_rank"] for item in waits] == [1, 2]
    assert coordinator_b.claim_merged_wait_diagnostics() is None


def test_top_waits_are_shared_when_worker_config_copies_proxy_mapping() -> None:
    with mp.Manager() as manager:
        shared_proxy = {
            "lock": manager.RLock(),
            "decision_order_state": manager.dict(),
            "decision_order_participants": manager.dict(),
            "decision_order_expected_count": 2,
            "decision_order_wait_top": manager.list(),
            "decision_order_wait_control": manager.dict(),
            "decision_order_wait_total_ms": manager.Value("d", 0.0),
            "decision_order_wait_record_count": manager.Value("i", 0),
        }
        coordinator_a = _backtest_coordinator(dict(shared_proxy), timeout_seconds=2.0)
        coordinator_b = _backtest_coordinator(dict(shared_proxy), timeout_seconds=2.0)
        coordinator_a.register_participant({**_participant("BIP"), "worker_id": "worker-a"})
        coordinator_b.register_participant({**_participant("XPP"), "worker_id": "worker-b"})

        with shared_proxy["lock"]:
            coordinator_a._record_wait_diagnostic_locked(  # noqa: SLF001 - simulates worker-local terminal snapshot.
                {
                    "materiality": "diagnostic",
                    "diagnostic_scope": "coordinator_wait_attribution",
                    "candidate_symbol": "BIP",
                    "wait_elapsed_ms": 100,
                    "wait_count": 1,
                    "wait_poll_count": 10,
                    "final_action": "released",
                    "worker_id": "worker-a",
                }
            )
            coordinator_b._record_wait_diagnostic_locked(  # noqa: SLF001
                {
                    "materiality": "diagnostic",
                    "diagnostic_scope": "coordinator_wait_attribution",
                    "candidate_symbol": "XPP",
                    "wait_elapsed_ms": 500,
                    "wait_count": 1,
                    "wait_poll_count": 50,
                    "final_action": "released",
                    "worker_id": "worker-b",
                }
            )

        top_waits = list(coordinator_a.top_wait_diagnostics())
        assert [item["candidate_symbol"] for item in top_waits[:2]] == ["XPP", "BIP"]
        assert shared_proxy["decision_order_wait_record_count"].value == 2


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


def test_future_bar_candidate_waits_for_prior_wallet_candidate() -> None:
    ten = "2026-01-11T10:00:00Z"
    eleven = "2026-01-11T11:00:00Z"
    twelve = "2026-01-11T12:00:00Z"
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
            name="xpp-future",
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
            name="bip-prior",
            target=_run_candidate,
            kwargs={
                "coordinator": coordinator,
                "participant": bip,
                "candidate": _candidate("BIP", bar_time=ten),
                "bar_time": ten,
                "delay_seconds": 0.05,
                "on_turn": lambda: events.append("BIP-10-turn") or "accepted",
                "mark_complete": True,
                "errors": errors,
            },
        ),
    ]

    threads[0].start()
    _wait_until(lambda: "BIP" in _missing_participants(proxy, f"{eleven}|1h"))
    assert events == []
    assert threads[0].is_alive()

    threads[1].start()
    coordinator.update_participant_bar_state({**etp, "next_bar_time": twelve})
    _join(threads, errors)

    assert events == ["BIP-10-turn", "XPP-11-turn"]


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


def test_backtest_future_candidate_waits_without_wall_clock_timeout() -> None:
    candidate_bar = "2026-02-15T00:00:00Z"
    after_candidate = "2026-02-15T01:00:00Z"
    proxy = _proxy(expected_count=3)
    coordinator = _backtest_coordinator(proxy, timeout_seconds=0.01)
    bip = _participant("BIP", next_bar_time="2026-01-30T05:00:00Z")
    etp = _participant("ETP", next_bar_time="2026-01-30T02:00:00Z")
    xpp = _participant("XPP", next_bar_time=candidate_bar)
    for participant in (bip, etp, xpp):
        coordinator.register_participant(participant)
    events: List[str] = []
    errors: List[BaseException] = []

    thread = threading.Thread(
        name="xpp-backtest-wait",
        target=_run_candidate,
        kwargs={
            "coordinator": coordinator,
            "participant": xpp,
            "candidate": _candidate("XPP", bar_time=candidate_bar),
            "bar_time": candidate_bar,
            "on_turn": lambda: events.append("XPP-turn") or "accepted",
            "errors": errors,
        },
    )
    thread.start()

    try:
        _wait_until(
            lambda: "BIP" in _missing_participants(proxy, f"{candidate_bar}|1h")
            and "ETP" in _missing_participants(proxy, f"{candidate_bar}|1h")
        )
        time.sleep(0.05)
        state = proxy["decision_order_state"][f"{candidate_bar}|1h"]

        assert errors == []
        assert events == []
        assert thread.is_alive()
        assert state.get("error") in (None, "")
        assert state.get("missing_participants")
        assert state["diagnostics"]["arbitration_policy"] == "backtest_shared_wallet_arbitration"
        assert state["diagnostics"]["policy_action"] == "wait"
        assert state["diagnostics"]["wait_decision_reason"] == "backtest_waiting_for_market_progress"
        assert state["diagnostics"]["waiting_candidate_symbol"] == "XPP"
    finally:
        coordinator.update_participant_bar_state({**bip, "next_bar_time": after_candidate})
        coordinator.update_participant_bar_state({**etp, "next_bar_time": after_candidate})
        _join([thread], errors)

    assert events == ["XPP-turn"]


def test_default_arbitration_policy_preserves_turn_timeout() -> None:
    candidate_bar = "2026-02-15T00:00:00Z"
    proxy = _proxy(expected_count=2)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=0.01, poll_interval_seconds=0.001)
    bip = _participant("BIP", next_bar_time="2026-01-30T05:00:00Z")
    xpp = _participant("XPP", next_bar_time=candidate_bar)
    for participant in (bip, xpp):
        coordinator.register_participant(participant)

    with pytest.raises(RuntimeError, match="entry_decision_order_turn_timeout"):
        coordinator.arrive_and_wait_turn(
            participant=xpp,
            bar={"bar_time": candidate_bar, "timeframe": "1h"},
            candidate=_candidate("XPP", bar_time=candidate_bar),
        )

    state = proxy["decision_order_state"][f"{candidate_bar}|1h"]
    diagnostics = state["diagnostics"]
    assert state["error"] == "entry_decision_order_turn_timeout"
    assert diagnostics["arbitration_policy"] == "wall_clock_shared_wallet_arbitration"
    assert diagnostics["policy_action"] == "fail"
    assert diagnostics["wait_decision_reason"] == "wall_clock_turn_timeout"
    wait_diagnostic = diagnostics["wait_diagnostic"]
    assert wait_diagnostic["materiality"] == "diagnostic"
    assert wait_diagnostic["final_action"] == "failed"
    assert wait_diagnostic["candidate_symbol"] == "XPP"
    assert wait_diagnostic["wait_reason"] == "wall_clock_waiting_for_candidate_turn"
    assert wait_diagnostic["failure_reason"] == "wall_clock_turn_timeout"
    assert wait_diagnostic["blocking_participants"][0]["participant_symbol"] == "BIP"
    top_waits = list(coordinator.top_wait_diagnostics())
    assert len(top_waits) == 1
    assert top_waits[0]["final_action"] == "failed"
    assert top_waits[0]["candidate_symbol"] == "XPP"


def test_far_ahead_backtest_candidate_waits_for_lagging_symbols() -> None:
    candidate_bar = "2026-02-15T00:00:00Z"
    after_candidate = "2026-02-15T01:00:00Z"
    proxy = _proxy(expected_count=3)
    coordinator = _backtest_coordinator(proxy, timeout_seconds=0.01)
    bip = _participant("BIP", next_bar_time="2025-12-15T15:00:00Z")
    etp = _participant("ETP", next_bar_time="2025-12-17T19:00:00Z")
    xpp = _participant("XPP", next_bar_time=candidate_bar)
    for participant in (bip, etp, xpp):
        coordinator.register_participant(participant)
    events: List[str] = []
    errors: List[BaseException] = []

    thread = threading.Thread(
        name="xpp-far-ahead",
        target=_run_candidate,
        kwargs={
            "coordinator": coordinator,
            "participant": xpp,
            "candidate": _candidate("XPP", bar_time=candidate_bar),
            "bar_time": candidate_bar,
            "on_turn": lambda: events.append("XPP-turn") or "accepted",
            "errors": errors,
        },
    )
    thread.start()

    _wait_until(
        lambda: "BIP" in _missing_participants(proxy, f"{candidate_bar}|1h")
        and "ETP" in _missing_participants(proxy, f"{candidate_bar}|1h")
    )
    assert events == []
    assert thread.is_alive()

    coordinator.update_participant_bar_state({**bip, "next_bar_time": after_candidate})
    time.sleep(0.01)
    assert events == []
    assert thread.is_alive()

    coordinator.update_participant_bar_state({**etp, "next_bar_time": after_candidate})
    _join([thread], errors)

    assert events == ["XPP-turn"]

    state = proxy["decision_order_state"][f"{candidate_bar}|1h"]
    assert state["complete"] is True
    assert state.get("missing_participants") in (None, [])
    assert state.get("error") in (None, "")


def test_unresolved_prior_sparse_bar_blocks_future_bar_candidate() -> None:
    ten = "2026-01-11T10:00:00Z"
    eleven = "2026-01-11T11:00:00Z"
    twelve = "2026-01-11T12:00:00Z"
    proxy = _proxy(expected_count=3)
    coordinator = SharedWalletEntryDecisionOrderCoordinator(proxy, timeout_seconds=2.0, poll_interval_seconds=0.001)
    bip = _participant("BIP", next_bar_time=ten)
    etp = _participant("ETP", next_bar_time=ten)
    xpp = _participant("XPP", next_bar_time=eleven)
    for participant in (bip, etp, xpp):
        coordinator.register_participant(participant)

    events: List[str] = []
    errors: List[BaseException] = []

    bip_thread = threading.Thread(
        name="bip-prior",
        target=_run_candidate,
        kwargs={
            "coordinator": coordinator,
            "participant": bip,
            "candidate": _candidate("BIP", bar_time=ten),
            "bar_time": ten,
            "on_turn": lambda: events.append("BIP-10-turn") or "accepted",
            "mark_complete": True,
            "errors": errors,
        },
    )
    bip_thread.start()

    _wait_until(lambda: "ETP" in _missing_participants(proxy, f"{ten}|1h"))

    xpp_thread = threading.Thread(
        name="xpp-future",
        target=_run_candidate,
        kwargs={
            "coordinator": coordinator,
            "participant": xpp,
            "candidate": _candidate("XPP", bar_time=eleven),
            "bar_time": eleven,
            "on_turn": lambda: events.append("XPP-11-turn") or "accepted",
            "errors": errors,
        },
    )
    xpp_thread.start()

    _wait_until(
        lambda: "BIP" in _missing_participants(proxy, f"{eleven}|1h")
        and "ETP" in _missing_participants(proxy, f"{eleven}|1h")
    )
    assert events == []
    assert bip_thread.is_alive()
    assert xpp_thread.is_alive()

    coordinator.update_participant_bar_state({**etp, "next_bar_time": twelve})
    _join([bip_thread, xpp_thread], errors)

    assert events == ["BIP-10-turn", "XPP-11-turn"]


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
