"""Deterministic shared-wallet entry decision ordering."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


DecisionSortKey = Tuple[str, str, str, str, int, str, str, str]
_COMPLETED_STATE_RETENTION = 32


@dataclass(frozen=True)
class EntryDecisionOrderTicket:
    """A worker's claim on one shared-wallet decision arbitration bar."""

    bar_key: str
    participant_key: str
    candidate_id: Optional[str]

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
    ) -> None:
        self._proxy = shared_proxy if isinstance(shared_proxy, Mapping) else None
        self._timeout_seconds = max(float(timeout_seconds or 0.0), 0.001)
        self._poll_interval_seconds = max(float(poll_interval_seconds or 0.0), 0.001)

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
        self._wait_until_turn(ticket)
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

    def _wait_until_turn(self, ticket: EntryDecisionOrderTicket) -> None:
        deadline = time.monotonic() + self._timeout_seconds
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
                if _text(state.get("active_candidate")) == ticket.candidate_id:
                    return
            if time.monotonic() >= deadline:
                self._raise_timeout("entry_decision_order_turn_timeout", ticket)
            time.sleep(self._poll_interval_seconds)

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
        if int(next_epoch) > int(bar_epoch):
            return True
        if int(next_epoch) < int(bar_epoch):
            return True
        return False

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

    def _diagnostics_for_state(self, state: Mapping[str, Any]) -> Dict[str, Any]:
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
        return {
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
        }


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
