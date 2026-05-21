"""Deterministic shared-wallet entry decision ordering."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from .runtime_policy import SharedWalletArbitrationDecision, SharedWalletArbitrationPolicy


DecisionSortKey = Tuple[str, str, str, str, int, str, str, str]
_COMPLETED_STATE_RETENTION = 32
_DEFAULT_WAIT_DIAGNOSTIC_TOP_N = 10


@dataclass(frozen=True)
class EntryDecisionOrderTicket:
    """A worker's claim on one shared-wallet decision arbitration bar."""

    bar_key: str
    participant_key: str
    candidate_id: Optional[str]
    wait_summary: Mapping[str, Any] = field(default_factory=dict)

    @property
    def has_candidate(self) -> bool:
        return bool(self.candidate_id)


def stable_entry_decision_sort_key(candidate: Mapping[str, Any]) -> DecisionSortKey:
    """Return the explicit deterministic ordering key for entry candidates."""

    priority = _coerce_int(candidate.get("signal_priority"), 0)
    return (
        _text(candidate.get("bar_time")),
        _text(candidate.get("strategy_id")),
        _text(candidate.get("symbol")),
        _text(candidate.get("timeframe")),
        -priority,
        _text(candidate.get("direction") or candidate.get("side")),
        _text(candidate.get("decision_id")),
        _text(candidate.get("entry_request_id")),
    )


class SharedWalletEntryDecisionOrderCoordinator:
    """Coordinate shared-wallet entry candidates by deterministic bar order.

    Workers publish lightweight progress watermarks every bar. Only real entry
    candidates enter arbitration. For a candidate bar, the coordinator waits
    until expected participants have either arrived with a candidate or advanced
    past that bar. This keeps wallet mutation order independent of worker
    arrival timing while still allowing sparse-calendar workers to progress.
    """

    def __init__(
        self,
        shared_proxy: Optional[Mapping[str, Any]],
        *,
        timeout_seconds: float = 120.0,
        poll_interval_seconds: float = 0.005,
        arbitration_policy: Optional[SharedWalletArbitrationPolicy] = None,
        wait_diagnostic_top_n: int = _DEFAULT_WAIT_DIAGNOSTIC_TOP_N,
    ) -> None:
        self._proxy = shared_proxy if isinstance(shared_proxy, Mapping) else None
        self._timeout_seconds = max(float(timeout_seconds or 0.0), 0.001)
        self._poll_interval_seconds = max(float(poll_interval_seconds or 0.0), 0.001)
        self._arbitration_policy = arbitration_policy or SharedWalletArbitrationPolicy.for_run_type(
            "default",
            timeout_seconds=self._timeout_seconds,
        )
        self._wait_diagnostic_top_n_value = max(int(wait_diagnostic_top_n or _DEFAULT_WAIT_DIAGNOSTIC_TOP_N), 1)

    @property
    def enabled(self) -> bool:
        if not isinstance(self._proxy, Mapping):
            return False
        return (
            self._proxy.get("decision_order_state") is not None
            and self._proxy.get("decision_order_participants") is not None
            and self._lock() is not None
            and self._expected_count() > 1
        )

    def register_participant(self, payload: Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        participant_key = _participant_key(payload)
        if not participant_key:
            raise RuntimeError("entry_decision_order_participant_invalid: participant_key is required")
        with _ProxyLock(self._lock()):
            self._update_participant_locked(payload, active=True, registered=True)

    def update_participant_bar_state(self, payload: Mapping[str, Any]) -> None:
        """Publish a worker progress watermark.

        This is the normal no-candidate hot path. It does not create bar
        arbitration state and does not wait for other workers.
        """

        if not self.enabled:
            return
        participant_key = _participant_key(payload)
        if not participant_key:
            return
        with _ProxyLock(self._lock()):
            self._update_participant_locked(payload)
            self._refresh_releases_locked()
            self._prune_completed_locked()

    def mark_participant_complete(self, payload: Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        participant_key = _participant_key(payload)
        if not participant_key:
            return
        with _ProxyLock(self._lock()):
            self._update_participant_locked(payload, active=False)
            participants = self._participants()
            current = dict(participants.get(participant_key) or {})
            if current:
                current["active"] = False
                current["completed_at"] = _utc_now_iso()
                participants[participant_key] = current
            self._refresh_releases_locked()
            self._prune_completed_locked()

    def mark_participant_failed(self, payload: Mapping[str, Any], *, error: Any) -> None:
        if not self.enabled:
            return
        participant_key = _participant_key(payload)
        if not participant_key:
            return
        with _ProxyLock(self._lock()):
            participants = self._participants()
            current = dict(participants.get(participant_key) or {})
            if current:
                current.update(_json_mapping(payload))
                current["participant_key"] = participant_key
                current["active"] = False
                current["failed_at"] = _utc_now_iso()
                current["failure_error"] = _text(error)
                participants[participant_key] = current
            for bar_key, raw_state in list(dict(self._state_store()).items()):
                state = dict(raw_state or {})
                if bool(state.get("complete")) or _text(state.get("error")):
                    continue
                if not self._participant_has_candidate_in_state(participant_key, state):
                    continue
                state["error"] = "entry_decision_order_participant_failed"
                state["error_at"] = _utc_now_iso()
                state["error_participant_key"] = participant_key
                state["error_detail"] = _text(error)
                state["diagnostics"] = self._diagnostics_for_state(state)
                self._set_bar_state(str(bar_key), state)

    def arrive_and_wait_turn(
        self,
        *,
        participant: Mapping[str, Any],
        bar: Mapping[str, Any],
        candidate: Optional[Mapping[str, Any]],
    ) -> Optional[EntryDecisionOrderTicket]:
        if not self.enabled:
            return None
        participant_key = _participant_key(participant)
        if not participant_key:
            raise RuntimeError("entry_decision_order_participant_invalid: participant_key is required")
        if candidate is None:
            # Bundle 1.5.2: no-candidate bars are progress-only and must not
            # create arbitration state or wait on other workers.
            self.update_participant_bar_state(participant)
            return None
        bar_key = _bar_key(bar)
        if not bar_key:
            raise RuntimeError(
                "entry_decision_order_bar_invalid: bar_time/timeframe are required "
                f"participant_key={participant_key}"
            )
        candidate_id = _candidate_id(candidate, participant_key=participant_key, bar_key=bar_key)
        ticket = EntryDecisionOrderTicket(
            bar_key=bar_key,
            participant_key=participant_key,
            candidate_id=candidate_id,
        )
        self._record_candidate(ticket=ticket, participant=participant, bar=bar, candidate=candidate)
        wait_summary = self._wait_until_turn(ticket)
        if wait_summary:
            ticket = replace(ticket, wait_summary=wait_summary)
        return ticket

    def complete_candidate(
        self,
        ticket: Optional[EntryDecisionOrderTicket],
        *,
        outcome: str,
    ) -> None:
        if ticket is None or not ticket.has_candidate or not self.enabled:
            return
        with _ProxyLock(self._lock()):
            state = self._bar_state(ticket.bar_key)
            if not state:
                return
            active_candidate = _text(state.get("active_candidate"))
            if active_candidate and active_candidate != ticket.candidate_id:
                raise RuntimeError(
                    "entry_decision_order_complete_invalid: active candidate mismatch "
                    f"bar_key={ticket.bar_key} participant_key={ticket.participant_key} "
                    f"candidate_id={ticket.candidate_id} active_candidate={active_candidate}"
                )
            processed = list(state.get("processed") or [])
            if ticket.candidate_id not in processed:
                processed.append(ticket.candidate_id)
            outcomes = dict(state.get("outcomes") or {})
            outcomes[str(ticket.candidate_id)] = _text(outcome) or "completed"
            state["processed"] = processed
            state["outcomes"] = outcomes
            state["active_candidate"] = None
            self._set_bar_state(ticket.bar_key, state)
            self._refresh_releases_locked()
            self._prune_completed_locked()

    def wait_until_complete(self, ticket: Optional[EntryDecisionOrderTicket]) -> None:
        if ticket is None or not self.enabled:
            return
        deadline = time.monotonic() + self._timeout_seconds
        while True:
            with _ProxyLock(self._lock()):
                state = self._bar_state(ticket.bar_key)
                if not state or bool(state.get("complete")):
                    return
                self._raise_if_error(state, ticket)
                self._refresh_releases_locked()
                state = self._bar_state(ticket.bar_key)
                if not state or bool(state.get("complete")):
                    return
                self._raise_if_error(state, ticket)
            if time.monotonic() >= deadline:
                self._raise_timeout("entry_decision_order_complete_timeout", ticket)
            time.sleep(self._poll_interval_seconds)

    def _record_candidate(
        self,
        *,
        ticket: EntryDecisionOrderTicket,
        participant: Mapping[str, Any],
        bar: Mapping[str, Any],
        candidate: Mapping[str, Any],
    ) -> None:
        with _ProxyLock(self._lock()):
            self._update_participant_locked(participant)
            state = self._bar_state(ticket.bar_key)
            state.setdefault("bar_key", ticket.bar_key)
            state.setdefault("created_at", _utc_now_iso())
            if bool(state.get("complete")):
                state["complete"] = False
                state.pop("completed_at", None)
            state.setdefault("processed", [])
            state.setdefault("outcomes", {})
            state.setdefault("candidates", {})
            state.setdefault("candidate_participants", {})
            state.update(_bar_state_fields(ticket.bar_key, bar))
            candidates = dict(state.get("candidates") or {})
            candidate_payload = {
                **_json_mapping(candidate),
                "participant_key": ticket.participant_key,
                "candidate_id": ticket.candidate_id,
                "sort_key": list(stable_entry_decision_sort_key(candidate)),
                "arrived_at": _utc_now_iso(),
            }
            candidates[str(ticket.candidate_id)] = candidate_payload
            state["candidates"] = candidates
            candidate_participants = dict(state.get("candidate_participants") or {})
            candidate_participants[ticket.participant_key] = ticket.candidate_id
            state["candidate_participants"] = candidate_participants
            self._set_bar_state(ticket.bar_key, state)
            self._refresh_releases_locked()

    def _wait_until_turn(self, ticket: EntryDecisionOrderTicket) -> Dict[str, Any]:
        started_at = time.monotonic()
        wait_started_at_wall: Optional[str] = None
        last_wait_signature: Optional[Tuple[Any, ...]] = None
        wait_poll_count = 0
        max_blocking_participant_count = 0
        first_wait_reason: Optional[str] = None
        first_portfolio_watermark: Optional[str] = None
        first_blocking_participants: Tuple[str, ...] = ()
        first_blocking_participant_snapshots: Tuple[Mapping[str, Any], ...] = ()
        last_wait_reason: Optional[str] = None
        last_portfolio_watermark: Optional[str] = None
        last_blocking_participants: Tuple[str, ...] = ()
        last_blocking_participant_snapshots: Tuple[Mapping[str, Any], ...] = ()
        while True:
            with _ProxyLock(self._lock()):
                state = self._bar_state(ticket.bar_key)
                if not state:
                    raise RuntimeError(
                        "entry_decision_order_candidate_missing: "
                        f"bar_key={ticket.bar_key} participant_key={ticket.participant_key} "
                        f"candidate_id={ticket.candidate_id}"
                    )
                self._raise_if_error(state, ticket)
                self._refresh_releases_locked()
                state = self._bar_state(ticket.bar_key)
                self._raise_if_error(state, ticket)
                elapsed_seconds = max(time.monotonic() - started_at, 0.0)
                diagnostics = self._diagnostics_for_state(state, elapsed_wait_seconds=elapsed_seconds)
                decision = self._arbitration_policy.decide_candidate_turn(
                    candidate_has_turn=_text(state.get("active_candidate")) == ticket.candidate_id,
                    blocking_participants=tuple(str(item) for item in diagnostics.get("missing_participants") or ()),
                    elapsed_seconds=elapsed_seconds,
                    timeout_seconds=self._timeout_seconds,
                    state=state,
                    diagnostics=diagnostics,
                )
                if decision.action == "release":
                    return self._pressure_summary_for_turn(
                        ticket=ticket,
                        outcome="release",
                        decision=decision,
                        elapsed_seconds=elapsed_seconds,
                        state=state,
                        diagnostics=diagnostics,
                        wait_poll_count=wait_poll_count,
                        max_blocking_participant_count=max_blocking_participant_count,
                        wait_started_at=wait_started_at_wall,
                        wait_ended_at=_utc_now_iso() if wait_poll_count > 0 else None,
                        first_wait_reason=first_wait_reason,
                        first_portfolio_watermark=first_portfolio_watermark,
                        first_blocking_participants=first_blocking_participants,
                        first_blocking_participant_snapshots=first_blocking_participant_snapshots,
                        last_wait_reason=last_wait_reason,
                        last_portfolio_watermark=last_portfolio_watermark,
                        last_blocking_participants=last_blocking_participants,
                        last_blocking_participant_snapshots=last_blocking_participant_snapshots,
                    )
                if decision.action == "fail":
                    self._raise_turn_policy_failure_locked(
                        reason="entry_decision_order_turn_timeout",
                        ticket=ticket,
                        decision=decision,
                        elapsed_seconds=elapsed_seconds,
                        state=state,
                        diagnostics=diagnostics,
                        wait_poll_count=wait_poll_count,
                        max_blocking_participant_count=max(
                            max_blocking_participant_count,
                            len(tuple(decision.blocking_participants or ())),
                        ),
                        wait_started_at=wait_started_at_wall,
                        wait_ended_at=_utc_now_iso() if wait_poll_count > 0 else None,
                        first_wait_reason=first_wait_reason or decision.reason,
                        first_portfolio_watermark=first_portfolio_watermark or decision.portfolio_watermark,
                        first_blocking_participants=first_blocking_participants
                        or tuple(decision.blocking_participants or ()),
                        first_blocking_participant_snapshots=first_blocking_participant_snapshots,
                        last_wait_reason=last_wait_reason,
                        last_portfolio_watermark=last_portfolio_watermark,
                        last_blocking_participants=tuple(decision.blocking_participants or last_blocking_participants),
                        last_blocking_participant_snapshots=last_blocking_participant_snapshots,
                    )
                current_blocking_participants = tuple(decision.blocking_participants or ())
                current_blocking_participant_snapshots = self._blocking_participant_snapshots_locked(
                    current_blocking_participants,
                    state=state,
                )
                if wait_poll_count == 0:
                    wait_started_at_wall = _utc_now_iso()
                    first_wait_reason = decision.reason
                    first_portfolio_watermark = decision.portfolio_watermark
                    first_blocking_participants = current_blocking_participants
                    first_blocking_participant_snapshots = current_blocking_participant_snapshots
                wait_poll_count += 1
                max_blocking_participant_count = max(
                    max_blocking_participant_count,
                    len(current_blocking_participants),
                )
                last_wait_reason = decision.reason
                last_portfolio_watermark = decision.portfolio_watermark
                last_blocking_participants = current_blocking_participants
                last_blocking_participant_snapshots = current_blocking_participant_snapshots
                wait_signature = (
                    decision.reason,
                    tuple(decision.blocking_participants),
                    decision.portfolio_watermark,
                    elapsed_seconds >= self._timeout_seconds,
                )
                if wait_signature != last_wait_signature:
                    self._record_wait_decision_locked(
                        ticket=ticket,
                        decision=decision,
                        elapsed_seconds=elapsed_seconds,
                        state=state,
                    )
                    last_wait_signature = wait_signature
            time.sleep(self._poll_interval_seconds)

    def _pressure_summary_for_turn(
        self,
        *,
        ticket: EntryDecisionOrderTicket,
        outcome: str,
        decision: SharedWalletArbitrationDecision,
        elapsed_seconds: float,
        state: Mapping[str, Any],
        diagnostics: Mapping[str, Any],
        wait_poll_count: int,
        max_blocking_participant_count: int,
        wait_started_at: Optional[str],
        wait_ended_at: Optional[str],
        first_wait_reason: Optional[str],
        first_portfolio_watermark: Optional[str],
        first_blocking_participants: Sequence[str],
        first_blocking_participant_snapshots: Sequence[Mapping[str, Any]],
        last_wait_reason: Optional[str],
        last_portfolio_watermark: Optional[str],
        last_blocking_participants: Sequence[str],
        last_blocking_participant_snapshots: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        candidate_payload = dict(dict(state.get("candidates") or {}).get(str(ticket.candidate_id)) or {})
        participant_progress = diagnostics.get("participant_progress") if isinstance(diagnostics, Mapping) else {}
        participant_next_bar_watermark = None
        if isinstance(participant_progress, Mapping):
            next_times = [
                str(payload.get("next_bar_time") or "").strip()
                for payload in participant_progress.values()
                if isinstance(payload, Mapping) and str(payload.get("next_bar_time") or "").strip()
            ]
            participant_next_bar_watermark = min(next_times) if next_times else None
        blocking_count = len(tuple(decision.blocking_participants or ()))
        summary = {
            "outcome": str(outcome or "").strip() or "unknown",
            "elapsed_wait_ms": int(round(max(float(elapsed_seconds or 0.0), 0.0) * 1000.0)),
            "wait_count": 1 if wait_poll_count > 0 else 0,
            "wait_poll_count": max(int(wait_poll_count), 0),
            "release_count": 1 if str(outcome or "").strip() == "release" else 0,
            "fail_count": 1 if str(outcome or "").strip() == "fail" else 0,
            "blocking_participant_count": blocking_count,
            "max_blocking_participant_count": max(int(max_blocking_participant_count), blocking_count),
            "participant_next_bar_watermark": participant_next_bar_watermark,
            "portfolio_watermark": decision.portfolio_watermark or last_portfolio_watermark,
            "policy_name": self._arbitration_policy.name,
            "wait_started_at": wait_started_at,
            "wait_ended_at": wait_ended_at,
            "first_wait_reason": first_wait_reason,
            "first_portfolio_watermark": first_portfolio_watermark,
            "wait_reason": last_wait_reason,
            "release_reason": decision.reason if str(outcome or "").strip() == "release" else None,
            "failure_reason": decision.reason if str(outcome or "").strip() == "fail" else None,
            "candidate_symbol": candidate_payload.get("symbol") or state.get("waiting_candidate_symbol"),
            "candidate_timeframe": candidate_payload.get("timeframe") or state.get("timeframe"),
            "candidate_bar_time": candidate_payload.get("bar_time") or state.get("bar_time"),
            "candidate_decision_id": candidate_payload.get("decision_id") or state.get("waiting_candidate_decision_id"),
            "candidate_id": ticket.candidate_id,
            "participant_key": ticket.participant_key,
            "bar_key": ticket.bar_key,
        }
        wait_diagnostic = self._wait_diagnostic_for_turn(
            ticket=ticket,
            summary=summary,
            state=state,
            candidate_payload=candidate_payload,
            decision=decision,
            first_blocking_participants=first_blocking_participants,
            first_blocking_participant_snapshots=first_blocking_participant_snapshots,
            last_blocking_participants=last_blocking_participants,
            last_blocking_participant_snapshots=last_blocking_participant_snapshots,
        )
        summary["wait_diagnostic"] = wait_diagnostic
        self._record_wait_diagnostic_locked(wait_diagnostic)
        return summary

    def _refresh_releases_locked(self) -> None:
        for bar_key in self._pending_bar_keys_locked():
            state = self._bar_state(bar_key)
            if not state or bool(state.get("complete")):
                continue
            if _text(state.get("error")):
                continue
            if _text(state.get("active_candidate")):
                continue
            safe, missing = self._candidate_group_safe(state)
            if not safe:
                state["expected_participants"] = list(self._expected_participants_for_state(state))
                state["missing_participants"] = list(missing)
                self._set_bar_state(bar_key, state)
                continue
            order = list(self._ordered_candidate_ids(state))
            state["expected_participants"] = list(self._expected_participants_for_state(state))
            state["missing_participants"] = []
            state["candidate_order"] = list(order)
            processed = set(state.get("processed") or [])
            remaining = [candidate_id for candidate_id in order if candidate_id not in processed]
            if not remaining:
                state["complete"] = True
                state["completed_at"] = _utc_now_iso()
                self._set_bar_state(bar_key, state)
                continue
            state["active_candidate"] = remaining[0]
            state["active_started_at"] = _utc_now_iso()
            self._set_bar_state(bar_key, state)

    def _candidate_group_safe(self, state: Mapping[str, Any]) -> Tuple[bool, Sequence[str]]:
        candidates = dict(state.get("candidates") or {})
        if not candidates:
            return False, ()
        state_info = _state_epoch_timeframe(state)
        if state_info is None:
            return True, ()
        bar_epoch, _timeframe = state_info
        participants = dict(self._participants())
        candidate_participants = set(dict(state.get("candidate_participants") or {}).keys())
        missing = []
        for participant_key in self._expected_participants_for_state(state):
            if participant_key in candidate_participants:
                continue
            payload = dict(participants.get(participant_key) or {})
            if self._participant_bar_resolved(payload, bar_epoch=bar_epoch):
                continue
            missing.append(str(participant_key))
        return not missing, tuple(sorted(missing))

    @staticmethod
    def _participant_bar_resolved(payload: Mapping[str, Any], *, bar_epoch: int) -> bool:
        if not payload:
            return False
        if payload.get("active") is False or payload.get("failed_at") or payload.get("completed_at"):
            return True
        if _participant_gap_covers(payload, bar_epoch):
            return True
        next_epoch = _coerce_epoch(payload.get("next_bar_time"))
        if next_epoch is None:
            return False
        return int(next_epoch) > int(bar_epoch)

    def _expected_participants_for_state(self, state: Mapping[str, Any]) -> Sequence[str]:
        state_info = _state_epoch_timeframe(state)
        timeframe = state_info[1] if state_info is not None else _text(state.get("timeframe"))
        participants = dict(self._participants())
        expected = [
            str(participant_key)
            for participant_key, raw_payload in participants.items()
            if not timeframe or _text(dict(raw_payload or {}).get("timeframe")) == timeframe
        ]
        return tuple(sorted(expected))

    def _participant_has_candidate_in_state(
        self,
        participant_key: str,
        state: Mapping[str, Any],
    ) -> bool:
        return participant_key in set(dict(state.get("candidate_participants") or {}).keys())

    def _ordered_candidate_ids(self, state: Mapping[str, Any]) -> Sequence[str]:
        candidates = dict(state.get("candidates") or {})
        return [
            candidate_id
            for candidate_id, _payload in sorted(
                candidates.items(),
                key=lambda item: tuple(item[1].get("sort_key") or stable_entry_decision_sort_key(item[1])),
            )
        ]

    def _pending_bar_keys_locked(self) -> Sequence[str]:
        keys = []
        for bar_key, raw_state in dict(self._state_store()).items():
            state = dict(raw_state or {})
            if bool(state.get("complete")):
                continue
            if not dict(state.get("candidates") or {}):
                continue
            keys.append(str(bar_key))
        return tuple(sorted(keys, key=lambda key: _parse_bar_key(key) or (0, "")))

    def _prune_completed_locked(self) -> None:
        completed = []
        for bar_key, raw_state in dict(self._state_store()).items():
            state = dict(raw_state or {})
            if bool(state.get("complete")):
                completed.append((str(bar_key), _parse_bar_key(str(bar_key)) or (0, "")))
        if len(completed) <= _COMPLETED_STATE_RETENTION:
            return
        completed.sort(key=lambda item: item[1])
        for bar_key, _info in completed[: len(completed) - _COMPLETED_STATE_RETENTION]:
            try:
                del self._state_store()[bar_key]
            except KeyError:
                pass

    def _update_participant_locked(
        self,
        payload: Mapping[str, Any],
        *,
        active: Optional[bool] = None,
        registered: bool = False,
    ) -> None:
        participant_key = _participant_key(payload)
        if not participant_key:
            return
        participants = self._participants()
        current = dict(participants.get(participant_key) or {})
        previous_next_epoch = _coerce_epoch(current.get("next_bar_time"))
        previous_timeframe = _text(current.get("timeframe") or payload.get("timeframe"))
        current.update(_json_mapping(payload))
        current["participant_key"] = participant_key
        if active is not None:
            current["active"] = bool(active)
        else:
            current["active"] = bool(current.get("active", True))
        if registered:
            current.setdefault("registered_at", _utc_now_iso())
        current["progress_updated_at"] = _utc_now_iso()
        next_epoch = _coerce_epoch(current.get("next_bar_time"))
        timeframe = _text(current.get("timeframe") or previous_timeframe)
        step_seconds = _timeframe_seconds(timeframe)
        if (
            previous_next_epoch is not None
            and next_epoch is not None
            and step_seconds > 0
            and int(next_epoch) - int(previous_next_epoch) > int(step_seconds)
        ):
            gaps = list(current.get("gap_ranges") or [])
            gaps.append(
                {
                    "start": _epoch_to_iso(int(previous_next_epoch) + int(step_seconds)),
                    "end": _epoch_to_iso(int(next_epoch)),
                    "classification": _text(current.get("gap_classification")) or "unknown_gap",
                    "reason_code": "DECISION_ORDER_CANDLE_GAP",
                    "observed_at": _utc_now_iso(),
                }
            )
            current["gap_ranges"] = gaps[-32:]
        participants[participant_key] = current

    def _bar_state(self, bar_key: str) -> Dict[str, Any]:
        state = self._state_store().get(bar_key)
        if isinstance(state, Mapping):
            return dict(state)
        return {}

    def _set_bar_state(self, bar_key: str, state: Mapping[str, Any]) -> None:
        self._state_store()[bar_key] = dict(state or {})

    def _state_store(self) -> Any:
        return self._proxy["decision_order_state"]  # type: ignore[index]

    def _participants(self) -> Any:
        return self._proxy["decision_order_participants"]  # type: ignore[index]

    def _lock(self) -> Any:
        if not isinstance(self._proxy, Mapping):
            return None
        return self._proxy.get("decision_order_lock") or self._proxy.get("lock")

    def _expected_count(self) -> int:
        if not isinstance(self._proxy, Mapping):
            return 0
        return max(_coerce_int(self._proxy.get("decision_order_expected_count"), 0), 0)

    @staticmethod
    def _raise_if_error(state: Mapping[str, Any], ticket: EntryDecisionOrderTicket) -> None:
        error = _text(state.get("error"))
        if not error:
            return
        raise RuntimeError(
            "entry_decision_order_bar_error: "
            f"bar_key={ticket.bar_key} participant_key={ticket.participant_key} "
            f"error={error} diagnostics={_diagnostic_json(state.get('diagnostics') or state)}"
        )

    def _raise_timeout(self, reason: str, ticket: EntryDecisionOrderTicket) -> None:
        with _ProxyLock(self._lock()):
            state = self._bar_state(ticket.bar_key)
            state.setdefault("bar_key", ticket.bar_key)
            state["error"] = reason
            state["error_at"] = _utc_now_iso()
            state["error_participant_key"] = ticket.participant_key
            state["error_candidate_id"] = ticket.candidate_id
            state["timeout_ms"] = int(round(self._timeout_seconds * 1000.0))
            state["diagnostics"] = self._diagnostics_for_state(state)
            self._set_bar_state(ticket.bar_key, state)
        raise RuntimeError(
            f"{reason}: bar_key={ticket.bar_key} participant_key={ticket.participant_key} "
            f"candidate_id={ticket.candidate_id or '<none>'} timeout_ms={int(round(self._timeout_seconds * 1000.0))} "
            f"diagnostics={_diagnostic_json(state.get('diagnostics') or state)}"
        )

    def _raise_turn_policy_failure_locked(
        self,
        *,
        reason: str,
        ticket: EntryDecisionOrderTicket,
        decision: SharedWalletArbitrationDecision,
        elapsed_seconds: float,
        state: Mapping[str, Any],
        diagnostics: Mapping[str, Any],
        wait_poll_count: int,
        max_blocking_participant_count: int,
        wait_started_at: Optional[str],
        wait_ended_at: Optional[str],
        first_wait_reason: Optional[str],
        first_portfolio_watermark: Optional[str],
        first_blocking_participants: Sequence[str],
        first_blocking_participant_snapshots: Sequence[Mapping[str, Any]],
        last_wait_reason: Optional[str],
        last_portfolio_watermark: Optional[str],
        last_blocking_participants: Sequence[str],
        last_blocking_participant_snapshots: Sequence[Mapping[str, Any]],
    ) -> None:
        next_state = dict(state or {})
        next_state.setdefault("bar_key", ticket.bar_key)
        next_state["error"] = reason
        next_state["error_at"] = _utc_now_iso()
        next_state["error_participant_key"] = ticket.participant_key
        next_state["error_candidate_id"] = ticket.candidate_id
        next_state["timeout_ms"] = int(round(self._timeout_seconds * 1000.0))
        next_state["elapsed_wait_ms"] = int(round(max(float(elapsed_seconds or 0.0), 0.0) * 1000.0))
        next_state["arbitration_policy"] = self._arbitration_policy.name
        next_state["policy_action"] = decision.action
        next_state["wait_decision_reason"] = decision.reason
        if decision.portfolio_watermark:
            next_state["portfolio_watermark"] = decision.portfolio_watermark
        wait_summary = self._pressure_summary_for_turn(
            ticket=ticket,
            outcome="fail",
            decision=decision,
            elapsed_seconds=elapsed_seconds,
            state=next_state,
            diagnostics=diagnostics,
            wait_poll_count=wait_poll_count,
            max_blocking_participant_count=max_blocking_participant_count,
            wait_started_at=wait_started_at,
            wait_ended_at=wait_ended_at,
            first_wait_reason=first_wait_reason,
            first_portfolio_watermark=first_portfolio_watermark,
            first_blocking_participants=first_blocking_participants,
            first_blocking_participant_snapshots=first_blocking_participant_snapshots,
            last_wait_reason=last_wait_reason or decision.reason,
            last_portfolio_watermark=last_portfolio_watermark or decision.portfolio_watermark,
            last_blocking_participants=last_blocking_participants,
            last_blocking_participant_snapshots=last_blocking_participant_snapshots,
        )
        next_state["diagnostics"] = self._diagnostics_for_state(
            next_state,
            elapsed_wait_seconds=elapsed_seconds,
            policy_decision=decision,
        )
        next_state["diagnostics"]["wait_diagnostic"] = wait_summary.get("wait_diagnostic")
        self._set_bar_state(ticket.bar_key, next_state)
        raise RuntimeError(
            f"{reason}: bar_key={ticket.bar_key} participant_key={ticket.participant_key} "
            f"candidate_id={ticket.candidate_id or '<none>'} "
            f"policy={self._arbitration_policy.name} policy_reason={decision.reason} "
            f"elapsed_wait_ms={int(round(max(float(elapsed_seconds or 0.0), 0.0) * 1000.0))} "
            f"timeout_ms={int(round(self._timeout_seconds * 1000.0))} "
            f"diagnostics={_diagnostic_json(next_state.get('diagnostics') or next_state)}"
        )

    def _wait_diagnostic_for_turn(
        self,
        *,
        ticket: EntryDecisionOrderTicket,
        summary: Mapping[str, Any],
        state: Mapping[str, Any],
        candidate_payload: Mapping[str, Any],
        decision: SharedWalletArbitrationDecision,
        first_blocking_participants: Sequence[str],
        first_blocking_participant_snapshots: Sequence[Mapping[str, Any]],
        last_blocking_participants: Sequence[str],
        last_blocking_participant_snapshots: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        parsed = _state_epoch_timeframe(state)
        candidate_bar_epoch = int(parsed[0]) if parsed is not None else _coerce_epoch(summary.get("candidate_bar_time"))
        final_action = "released" if str(summary.get("outcome")) == "release" else "failed"
        first_keys = tuple(str(item) for item in (first_blocking_participants or ()))
        last_keys = tuple(str(item) for item in (last_blocking_participants or decision.blocking_participants or ()))
        blocking_keys = _unique_text_sequence((*first_keys, *last_keys, *tuple(decision.blocking_participants or ())))
        first_snapshots = {
            str(item.get("participant_key") or ""): dict(item)
            for item in first_blocking_participant_snapshots
            if isinstance(item, Mapping) and str(item.get("participant_key") or "").strip()
        }
        last_snapshots = {
            str(item.get("participant_key") or ""): dict(item)
            for item in last_blocking_participant_snapshots
            if isinstance(item, Mapping) and str(item.get("participant_key") or "").strip()
        }
        blocking_participants = []
        for participant_key in blocking_keys:
            first_snapshot = dict(first_snapshots.get(participant_key) or {})
            wait_snapshot = dict(last_snapshots.get(participant_key) or first_snapshot or {})
            release_payload = dict(self._participants().get(participant_key) or {})
            release_snapshot = self._participant_diagnostic_snapshot(
                participant_key,
                release_payload,
                candidate_bar_epoch=candidate_bar_epoch,
            )
            if first_snapshot:
                item = dict(first_snapshot)
            elif wait_snapshot:
                item = dict(wait_snapshot)
            else:
                item = dict(release_snapshot)
            if first_snapshot:
                item["first_context"] = dict(first_snapshot)
                item["first_next_bar_time"] = first_snapshot.get("next_bar_time")
                item["first_next_bar_epoch"] = first_snapshot.get("next_bar_epoch")
                item["first_current_bar_time"] = first_snapshot.get("current_bar_time")
                item["first_current_bar_epoch"] = first_snapshot.get("current_bar_epoch")
                item["first_active"] = first_snapshot.get("active")
                item["first_status"] = first_snapshot.get("status")
                item["first_gap_evidence_applied"] = first_snapshot.get("gap_evidence_applied")
                item["first_gap_range_count"] = first_snapshot.get("gap_range_count")
                item["first_gap_classification"] = first_snapshot.get("gap_classification")
                item["first_bar_index"] = first_snapshot.get("bar_index")
                item["first_total_bars"] = first_snapshot.get("total_bars")
                item["first_progress_updated_at"] = first_snapshot.get("progress_updated_at")
            if wait_snapshot:
                item["last_wait_context"] = dict(wait_snapshot)
            if release_snapshot:
                item["release_context"] = dict(release_snapshot)
            release_next_bar_time = release_snapshot.get("next_bar_time")
            release_current_bar_time = release_snapshot.get("current_bar_time")
            item["release_next_bar_time"] = release_next_bar_time
            item["release_next_bar_epoch"] = release_snapshot.get("next_bar_epoch")
            item["release_current_bar_time"] = release_current_bar_time
            item["release_current_bar_epoch"] = release_snapshot.get("current_bar_epoch")
            item["release_active"] = release_snapshot.get("active")
            item["release_status"] = release_snapshot.get("status")
            item["release_gap_evidence_applied"] = release_snapshot.get("gap_evidence_applied")
            item["release_gap_range_count"] = release_snapshot.get("gap_range_count")
            item["release_gap_classification"] = release_snapshot.get("gap_classification")
            item["release_bar_index"] = release_snapshot.get("bar_index")
            item["release_total_bars"] = release_snapshot.get("total_bars")
            item["release_progress_updated_at"] = release_snapshot.get("progress_updated_at")
            if not item:
                item = self._participant_diagnostic_snapshot(
                    participant_key,
                    release_payload,
                    candidate_bar_epoch=candidate_bar_epoch,
                )
            blocking_participants.append(item)
        source_run_id = (
            candidate_payload.get("run_id")
            or state.get("run_id")
            or dict(self._participants().get(ticket.participant_key) or {}).get("run_id")
        )
        source_bot_id = (
            candidate_payload.get("bot_id")
            or state.get("bot_id")
            or dict(self._participants().get(ticket.participant_key) or {}).get("bot_id")
        )
        return {
            "materiality": "diagnostic",
            "diagnostic_scope": "coordinator_wait_attribution",
            "candidate_id": ticket.candidate_id,
            "decision_id": summary.get("candidate_decision_id"),
            "candidate_symbol": summary.get("candidate_symbol"),
            "candidate_timeframe": summary.get("candidate_timeframe"),
            "candidate_bar_time": summary.get("candidate_bar_time"),
            "candidate_bar_epoch": candidate_bar_epoch,
            "candidate_participant_key": ticket.participant_key,
            "wait_elapsed_ms": summary.get("elapsed_wait_ms"),
            "wait_started_at": summary.get("wait_started_at"),
            "wait_ended_at": summary.get("wait_ended_at"),
            "wait_count": summary.get("wait_count"),
            "wait_poll_count": summary.get("wait_poll_count"),
            "policy_name": summary.get("policy_name"),
            "first_wait_reason": summary.get("first_wait_reason"),
            "first_portfolio_watermark": summary.get("first_portfolio_watermark"),
            "wait_reason": summary.get("wait_reason") or decision.reason,
            "release_reason": summary.get("release_reason"),
            "failure_reason": summary.get("failure_reason"),
            "final_action": final_action,
            "blocking_participant_count": len(blocking_participants),
            "max_blocking_participant_count": summary.get("max_blocking_participant_count"),
            "blocking_participants": blocking_participants,
            "portfolio_watermark": summary.get("portfolio_watermark"),
            "participant_next_bar_watermark": summary.get("participant_next_bar_watermark"),
            "worker_id": candidate_payload.get("worker_id")
            or dict(self._participants().get(ticket.participant_key) or {}).get("worker_id"),
            "run_id": source_run_id,
            "bot_id": source_bot_id,
            "bar_key": ticket.bar_key,
        }

    def _blocking_participant_snapshots_locked(
        self,
        participant_keys: Sequence[str],
        *,
        state: Mapping[str, Any],
    ) -> Tuple[Mapping[str, Any], ...]:
        parsed = _state_epoch_timeframe(state)
        candidate_bar_epoch = int(parsed[0]) if parsed is not None else None
        snapshots = []
        participants = self._participants()
        for participant_key in participant_keys:
            payload = dict(participants.get(str(participant_key)) or {})
            snapshots.append(
                self._participant_diagnostic_snapshot(
                    str(participant_key),
                    payload,
                    candidate_bar_epoch=candidate_bar_epoch,
                )
            )
        return tuple(snapshots)

    @staticmethod
    def _participant_diagnostic_snapshot(
        participant_key: str,
        payload: Mapping[str, Any],
        *,
        candidate_bar_epoch: Optional[int],
    ) -> Dict[str, Any]:
        next_bar_time = payload.get("next_bar_time")
        current_bar_time = payload.get("current_bar_time")
        return {
            "participant_key": str(participant_key),
            "participant_symbol": payload.get("symbol"),
            "participant_timeframe": payload.get("timeframe"),
            "next_bar_time": next_bar_time,
            "next_bar_epoch": _coerce_epoch(next_bar_time),
            "current_bar_time": current_bar_time,
            "current_bar_epoch": _coerce_epoch(current_bar_time),
            "active": bool(payload.get("active", True)),
            "status": _participant_status(payload),
            "failed_at": payload.get("failed_at"),
            "completed_at": payload.get("completed_at"),
            "failure_error": payload.get("failure_error"),
            "gap_evidence_applied": (
                _participant_gap_covers(payload, int(candidate_bar_epoch))
                if candidate_bar_epoch is not None
                else False
            ),
            "gap_range_count": len(list(payload.get("gap_ranges") or [])),
            "gap_classification": payload.get("gap_classification"),
            "bar_index": payload.get("bar_index"),
            "total_bars": payload.get("total_bars"),
            "progress_updated_at": payload.get("progress_updated_at"),
            "worker_id": payload.get("worker_id"),
        }

    def _record_wait_diagnostic_locked(self, record: Mapping[str, Any]) -> None:
        if not isinstance(record, Mapping):
            return
        if str(record.get("final_action") or "") != "failed" and _coerce_int(record.get("wait_count"), 0) <= 0:
            return
        self._increment_wait_totals_locked(record)
        records = list(self._retained_wait_records_locked())
        records.append(dict(record))
        failed = [item for item in records if str(item.get("final_action") or "") == "failed"]
        released = [item for item in records if str(item.get("final_action") or "") != "failed"]
        released.sort(key=lambda item: float(item.get("wait_elapsed_ms") or 0.0), reverse=True)
        retained = failed + released[: self._wait_diagnostic_top_n()]
        retained.sort(
            key=lambda item: (
                1 if str(item.get("final_action") or "") == "failed" else 0,
                float(item.get("wait_elapsed_ms") or 0.0),
            ),
            reverse=True,
        )
        for index, item in enumerate(retained, start=1):
            item["diagnostic_rank"] = index
        self._replace_wait_records_locked(retained)

    def _wait_diagnostic_top_n(self) -> int:
        if not isinstance(self._proxy, Mapping):
            return self._wait_diagnostic_top_n_value
        return max(_coerce_int(self._proxy.get("decision_order_wait_top_n"), self._wait_diagnostic_top_n_value), 1)

    def top_wait_diagnostics(self) -> Sequence[Mapping[str, Any]]:
        if not self.enabled:
            return ()
        with _ProxyLock(self._lock()):
            return tuple(self._retained_wait_records_locked())

    def claim_merged_wait_diagnostics(self) -> Optional[Mapping[str, Any]]:
        """Return one run-level top-wait diagnostic payload if this worker should emit it."""

        if not self.enabled:
            return None
        with _ProxyLock(self._lock()):
            top_waits = list(self._retained_wait_records_locked())
            if not top_waits:
                return None
            control = self._wait_control_store()
            if _control_get(control, "merged_emitted_at") or self._proxy.get("decision_order_wait_merged_emitted_at"):
                return None
            participants = {
                str(participant_key): dict(payload or {})
                for participant_key, payload in dict(self._participants()).items()
            }
            expected_count = max(self._expected_count(), len(participants))
            terminal_count = sum(1 for payload in participants.values() if _participant_terminal(payload))
            if expected_count > 0 and terminal_count < expected_count:
                return None
            generated_at = _utc_now_iso()
            if control is not None:
                control["merged_emitted_at"] = generated_at
            else:
                self._proxy["decision_order_wait_merged_emitted_at"] = generated_at
            for index, item in enumerate(top_waits, start=1):
                item["diagnostic_rank"] = index
            participant_worker_ids = sorted(
                {_text(payload.get("worker_id")) for payload in participants.values() if _text(payload.get("worker_id"))}
            )
            top_wait_worker_ids = sorted({_text(item.get("worker_id")) for item in top_waits if _text(item.get("worker_id"))})
            worker_ids = participant_worker_ids or top_wait_worker_ids
            total_wait_ms = self._proxy_numeric_value("decision_order_wait_total_ms")
            total_wait_count = self._proxy_numeric_value("decision_order_wait_record_count")
            return {
                "materiality": "diagnostic",
                "diagnostic_scope": "coordinator_wait_attribution",
                "source_reason": "run_final",
                "aggregation_level": "run",
                "generated_at": generated_at,
                "top_n": len(top_waits),
                "configured_top_n": self._wait_diagnostic_top_n(),
                "worker_count": len(worker_ids),
                "workers_included": worker_ids,
                "top_wait_worker_count": len(top_wait_worker_ids),
                "top_wait_workers_included": top_wait_worker_ids,
                "expected_participant_count": expected_count,
                "terminal_participant_count": terminal_count,
                "total_wait_ms": total_wait_ms,
                "total_wait_count": total_wait_count,
                "max_wait_ms": max((float(item.get("wait_elapsed_ms") or 0.0) for item in top_waits), default=0.0),
                "top_waits": top_waits,
            }

    def _retained_wait_records_locked(self) -> Sequence[Dict[str, Any]]:
        records = [dict(item) for item in list(self._proxy.get("decision_order_wait_top") or []) if isinstance(item, Mapping)]
        records.sort(
            key=lambda item: (
                1 if str(item.get("final_action") or "") == "failed" else 0,
                float(item.get("wait_elapsed_ms") or 0.0),
            ),
            reverse=True,
        )
        return tuple(records)

    def _replace_wait_records_locked(self, records: Sequence[Mapping[str, Any]]) -> None:
        retained = [dict(item) for item in records if isinstance(item, Mapping)]
        wait_store = self._proxy.get("decision_order_wait_top")
        if wait_store is not None and hasattr(wait_store, "append"):
            try:
                wait_store[:] = retained
                return
            except Exception:
                try:
                    del wait_store[:]
                    for item in retained:
                        wait_store.append(dict(item))
                    return
                except Exception:
                    pass
        self._proxy["decision_order_wait_top"] = retained

    def _increment_wait_totals_locked(self, record: Mapping[str, Any]) -> None:
        wait_count = max(_coerce_int(record.get("wait_count"), 0), 0)
        if str(record.get("final_action") or "") == "failed" and wait_count <= 0:
            wait_count = 1
        if wait_count <= 0:
            return
        self._increment_proxy_numeric_value("decision_order_wait_record_count", float(wait_count))
        self._increment_proxy_numeric_value("decision_order_wait_total_ms", float(record.get("wait_elapsed_ms") or 0.0))

    def _increment_proxy_numeric_value(self, key: str, delta: float) -> None:
        target = self._proxy.get(key)
        if hasattr(target, "value"):
            target.value = float(target.value or 0.0) + float(delta or 0.0)
            return
        self._proxy[key] = float(self._proxy.get(key) or 0.0) + float(delta or 0.0)

    def _proxy_numeric_value(self, key: str) -> Optional[float]:
        target = self._proxy.get(key)
        if hasattr(target, "value"):
            return float(target.value or 0.0)
        if key not in self._proxy:
            return None
        return float(self._proxy.get(key) or 0.0)

    def _wait_control_store(self) -> Optional[Any]:
        control = self._proxy.get("decision_order_wait_control")
        if control is not None and hasattr(control, "__setitem__") and hasattr(control, "get"):
            return control
        return None

    def _record_wait_decision_locked(
        self,
        *,
        ticket: EntryDecisionOrderTicket,
        decision: SharedWalletArbitrationDecision,
        elapsed_seconds: float,
        state: Mapping[str, Any],
    ) -> None:
        next_state = dict(state or {})
        next_state["waiting_participant_key"] = ticket.participant_key
        next_state["waiting_candidate_id"] = ticket.candidate_id
        candidate_payload = dict(dict(next_state.get("candidates") or {}).get(str(ticket.candidate_id)) or {})
        if candidate_payload:
            next_state["waiting_candidate_symbol"] = candidate_payload.get("symbol")
            next_state["waiting_candidate_timeframe"] = candidate_payload.get("timeframe")
            next_state["waiting_candidate_bar_time"] = candidate_payload.get("bar_time")
            next_state["waiting_candidate_decision_id"] = candidate_payload.get("decision_id")
        next_state["arbitration_policy"] = self._arbitration_policy.name
        next_state["policy_action"] = decision.action
        next_state["wait_decision_reason"] = decision.reason
        next_state["elapsed_wait_ms"] = int(round(max(float(elapsed_seconds or 0.0), 0.0) * 1000.0))
        if decision.portfolio_watermark:
            next_state["portfolio_watermark"] = decision.portfolio_watermark
        next_state["diagnostics"] = self._diagnostics_for_state(
            next_state,
            elapsed_wait_seconds=elapsed_seconds,
            policy_decision=decision,
        )
        self._set_bar_state(ticket.bar_key, next_state)

    def _diagnostics_for_state(
        self,
        state: Mapping[str, Any],
        *,
        elapsed_wait_seconds: Optional[float] = None,
        policy_decision: Optional[SharedWalletArbitrationDecision] = None,
    ) -> Dict[str, Any]:
        expected = list(self._expected_participants_for_state(state))
        candidate_participants = sorted(dict(state.get("candidate_participants") or {}).keys())
        safe, missing = self._candidate_group_safe(state)
        participants = dict(self._participants())
        participant_progress = {}
        participant_failures = {}
        known_sparse_gaps = {}
        for participant_key in sorted(set(expected) | set(candidate_participants) | set(missing)):
            payload = dict(participants.get(participant_key) or {})
            if payload.get("next_bar_time") or payload.get("current_bar_time"):
                participant_progress[participant_key] = {
                    "next_bar_time": payload.get("next_bar_time"),
                    "current_bar_time": payload.get("current_bar_time"),
                    "active": bool(payload.get("active", True)),
                }
            if payload.get("failure_error"):
                participant_failures[participant_key] = payload.get("failure_error")
            if payload.get("gap_ranges"):
                known_sparse_gaps[participant_key] = list(payload.get("gap_ranges") or [])
        diagnostics = {
            "bar_key": state.get("bar_key"),
            "expected_participants": expected,
            "arrived_participants": candidate_participants,
            "missing_participants": list(missing),
            "candidate_count": len(dict(state.get("candidates") or {})),
            "participant_progress": participant_progress,
            "known_sparse_gaps": known_sparse_gaps,
            "safe_to_release": bool(safe),
            "participant_failures": participant_failures,
            "timeout_ms": int(round(self._timeout_seconds * 1000.0)),
            "arbitration_policy": self._arbitration_policy.name,
            "waiting_candidate_id": state.get("waiting_candidate_id"),
            "waiting_participant_key": state.get("waiting_participant_key"),
            "waiting_candidate_symbol": state.get("waiting_candidate_symbol"),
            "waiting_candidate_timeframe": state.get("waiting_candidate_timeframe"),
            "waiting_candidate_bar_time": state.get("waiting_candidate_bar_time"),
            "waiting_candidate_decision_id": state.get("waiting_candidate_decision_id"),
        }
        if elapsed_wait_seconds is not None:
            diagnostics["elapsed_wait_ms"] = int(round(max(float(elapsed_wait_seconds or 0.0), 0.0) * 1000.0))
        if policy_decision is not None:
            diagnostics["policy_action"] = policy_decision.action
            diagnostics["wait_decision_reason"] = policy_decision.reason
            diagnostics["blocking_participants"] = list(policy_decision.blocking_participants)
            diagnostics["portfolio_watermark"] = policy_decision.portfolio_watermark
        return diagnostics


class _ProxyLock:
    def __init__(self, lock: Any) -> None:
        self._lock = lock

    def __enter__(self) -> None:
        if self._lock is not None:
            self._lock.acquire()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._lock is not None:
            self._lock.release()


def _participant_key(payload: Mapping[str, Any]) -> str:
    explicit = _text(payload.get("participant_key"))
    if explicit:
        return explicit
    return "|".join(
        _text(part)
        for part in (
            payload.get("strategy_id"),
            payload.get("instrument_id"),
            payload.get("symbol"),
            payload.get("timeframe"),
        )
    )


def _bar_key(payload: Mapping[str, Any]) -> str:
    return "|".join(
        _text(part)
        for part in (
            payload.get("bar_time"),
            payload.get("timeframe"),
        )
    )


def _bar_state_fields(bar_key: str, bar: Mapping[str, Any]) -> Dict[str, Any]:
    parsed = _parse_bar_key(bar_key)
    fields = {
        "bar_key": bar_key,
        "bar_time": _text(bar.get("bar_time")),
        "timeframe": _text(bar.get("timeframe")),
    }
    if parsed is not None:
        fields["bar_epoch"] = int(parsed[0])
        fields["timeframe"] = str(parsed[1])
    return fields


def _state_epoch_timeframe(state: Mapping[str, Any]) -> Optional[Tuple[int, str]]:
    epoch = _coerce_epoch(state.get("bar_epoch"))
    timeframe = _text(state.get("timeframe"))
    if epoch is not None and timeframe:
        return int(epoch), timeframe
    return _parse_bar_key(state.get("bar_key"))


def _parse_bar_key(value: Any) -> Optional[Tuple[int, str]]:
    text = _text(value)
    if "|" not in text:
        return None
    bar_time, timeframe = text.rsplit("|", 1)
    epoch = _coerce_epoch(bar_time)
    if epoch is None:
        return None
    timeframe_text = _text(timeframe)
    if not timeframe_text:
        return None
    return int(epoch), timeframe_text


def _participant_gap_covers(payload: Mapping[str, Any], epoch: int) -> bool:
    for raw_gap in list(payload.get("gap_ranges") or []):
        if not isinstance(raw_gap, Mapping):
            continue
        start_epoch = _coerce_epoch(raw_gap.get("start"))
        end_epoch = _coerce_epoch(raw_gap.get("end"))
        if start_epoch is None or end_epoch is None:
            continue
        if int(start_epoch) <= int(epoch) < int(end_epoch):
            return True
    return False


def _participant_status(payload: Mapping[str, Any]) -> str:
    if payload.get("failed_at") or payload.get("failure_error"):
        return "failed"
    if payload.get("completed_at") or payload.get("active") is False:
        return "done"
    return "active"


def _coerce_epoch(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None
    text = _text(value)
    if not text:
        return None
    if text.isdigit():
        return int(text)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp())


def _epoch_to_iso(epoch: int) -> str:
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _timeframe_seconds(value: Any) -> int:
    text = _text(value).lower()
    if not text:
        return 0
    units = (
        ("ms", 0.001),
        ("s", 1),
        ("m", 60),
        ("h", 3600),
        ("d", 86400),
        ("w", 604800),
    )
    for suffix, multiplier in units:
        if not text.endswith(suffix):
            continue
        raw = text[: -len(suffix)]
        try:
            value_num = int(raw)
        except (TypeError, ValueError):
            return 0
        return max(int(value_num * multiplier), 0)
    return 0


def _candidate_id(
    candidate: Optional[Mapping[str, Any]],
    *,
    participant_key: str,
    bar_key: str,
) -> Optional[str]:
    if not isinstance(candidate, Mapping):
        return None
    explicit = _text(candidate.get("candidate_id"))
    if explicit:
        return explicit
    return "|".join(
        _text(part)
        for part in (
            participant_key,
            bar_key,
            candidate.get("decision_id"),
            candidate.get("entry_request_id"),
            candidate.get("direction"),
        )
    )


def _json_mapping(payload: Mapping[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in dict(payload or {}).items():
        if isinstance(value, datetime):
            normalized[str(key)] = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        elif isinstance(value, (str, int, float, bool)) or value is None:
            normalized[str(key)] = value
        else:
            normalized[str(key)] = str(value)
    return normalized


def _text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value or "").strip()


def _unique_text_sequence(values: Sequence[Any]) -> Tuple[str, ...]:
    seen = set()
    result = []
    for value in values:
        text = _text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return tuple(result)


def _control_get(control: Any, key: str) -> Any:
    if control is not None and hasattr(control, "get"):
        return control.get(key)
    return None


def _participant_terminal(payload: Mapping[str, Any]) -> bool:
    if not isinstance(payload, Mapping):
        return False
    return bool(payload.get("failed_at") or payload.get("completed_at") or payload.get("active") is False)


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _diagnostic_json(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return str(dict(value or {}))


__all__ = [
    "DecisionSortKey",
    "EntryDecisionOrderTicket",
    "SharedWalletEntryDecisionOrderCoordinator",
    "stable_entry_decision_sort_key",
]
