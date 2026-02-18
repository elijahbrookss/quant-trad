"""Signal <-> execution contract helpers.

Contract summary:
- A signal "fires" when closed-bar evaluation becomes true.
- ``signal_time`` is that bar's close epoch (knowledge time), not an execution time.
- Signal payloads must remain execution-agnostic. Execution-policy/ATM computes any
  entry timing internally and must not mutate signal timestamps.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, NotRequired, TypedDict


_EXECUTION_FIELDS = (
    "action_time",
    "entry_time",
    "fill_time",
    "planned_entry_time",
    "next_open_time",
    "next_bar_open_time",
)


class AtmSignalContract(TypedDict):
    signal_type: str
    signal_time: int | float | str | datetime
    symbol: str
    timeframe_seconds: int
    indicator_id: str
    rule_id: str
    pattern_id: str
    runtime_scope: str
    metadata: Mapping[str, Any]
    known_at: NotRequired[int | float | str | datetime]
    bar_index: NotRequired[int]
    dedupe_key: NotRequired[str]
    event_id: NotRequired[str]


def _to_epoch(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _nested_meta(signal: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = signal.get("metadata")
    return nested if isinstance(nested, Mapping) else {}


def _get(signal: Mapping[str, Any], key: str) -> Any:
    if key in signal:
        return signal.get(key)
    return _nested_meta(signal).get(key)


def assert_signal_contract(signal: Mapping[str, Any]) -> None:
    """Raise if signal is missing ATM-required contract fields."""

    if not isinstance(signal, Mapping):
        raise RuntimeError("signal_contract_invalid: signal must be a mapping")

    signal_type = _get(signal, "signal_type") or _get(signal, "type")
    if not str(signal_type or "").strip():
        raise RuntimeError("signal_contract_invalid: missing signal_type/type")

    signal_time = _get(signal, "signal_time") or signal.get("time")
    signal_epoch = _to_epoch(signal_time)
    if signal_epoch is None:
        raise RuntimeError("signal_contract_invalid: missing/invalid signal_time")

    symbol = _get(signal, "symbol") or signal.get("symbol")
    if not str(symbol or "").strip():
        raise RuntimeError("signal_contract_invalid: missing symbol")

    timeframe_seconds = _get(signal, "timeframe_seconds")
    try:
        timeframe_int = int(timeframe_seconds)
    except (TypeError, ValueError):
        timeframe_int = 0
    if timeframe_int <= 0:
        raise RuntimeError("signal_contract_invalid: missing/invalid timeframe_seconds")

    indicator_id = _get(signal, "indicator_id")
    if not str(indicator_id or "").strip():
        raise RuntimeError("signal_contract_invalid: missing indicator_id")

    runtime_scope = _get(signal, "runtime_scope")
    if not str(runtime_scope or "").strip():
        raise RuntimeError("signal_contract_invalid: missing runtime_scope")

    rule_id = _get(signal, "rule_id")
    if not str(rule_id or "").strip():
        raise RuntimeError("signal_contract_invalid: missing rule_id")

    pattern_id = _get(signal, "pattern_id")
    if not str(pattern_id or "").strip():
        raise RuntimeError("signal_contract_invalid: missing pattern_id")

    metadata = signal.get("metadata")
    diagnostics = signal.get("diagnostics")
    if not isinstance(metadata, Mapping) and not isinstance(diagnostics, Mapping):
        raise RuntimeError("signal_contract_invalid: missing metadata/diagnostics mapping")

    known_at = _get(signal, "known_at")
    if known_at is not None:
        known_epoch = _to_epoch(known_at)
        if known_epoch is None:
            raise RuntimeError("signal_contract_invalid: invalid known_at")
        if known_epoch > signal_epoch:
            raise RuntimeError(
                "signal_contract_invalid: known_at must be <= signal_time"
            )


def assert_signal_time_is_closed_bar(signal: Mapping[str, Any], candle: Any) -> None:
    """Raise if signal_time does not exactly equal the emitting candle close epoch."""

    signal_epoch = _to_epoch(_get(signal, "signal_time") or signal.get("time"))
    candle_time = getattr(candle, "time", None)
    candle_epoch = _to_epoch(candle_time)
    if signal_epoch is None or candle_epoch is None:
        raise RuntimeError("signal_time_validation_failed: missing signal/candle epoch")
    if signal_epoch != candle_epoch:
        raise RuntimeError(
            f"signal_time_validation_failed: signal_time={signal_epoch} candle_epoch={candle_epoch}"
        )


def assert_no_execution_fields(signal: Mapping[str, Any], *, mode: str = "raise") -> list[str]:
    """Guard the signal boundary: emitters must not provide execution timing fields."""

    leaks: list[str] = []
    metadata = _nested_meta(signal)
    for field in _EXECUTION_FIELDS:
        if signal.get(field) not in (None, "") or metadata.get(field) not in (None, ""):
            leaks.append(field)
    if leaks and mode == "raise":
        joined = ",".join(sorted(set(leaks)))
        raise RuntimeError(f"signal_contract_invalid: execution fields not allowed ({joined})")
    return leaks


__all__ = [
    "AtmSignalContract",
    "assert_no_execution_fields",
    "assert_signal_contract",
    "assert_signal_time_is_closed_bar",
]
