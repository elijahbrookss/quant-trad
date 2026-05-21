"""Canonical signal-output item contract helpers."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping, TypedDict

try:  # Python <3.11 compatibility
    from typing import NotRequired
except ImportError:  # pragma: no cover - executed on Python <3.11
    from typing_extensions import NotRequired


_EXECUTION_FIELDS = (
    "action_time",
    "entry_time",
    "fill_time",
    "planned_entry_time",
    "next_open_time",
    "next_bar_open_time",
)

_ALLOWED_EVENT_KEYS = {
    "key",
    "direction",
    "pattern_id",
    "known_at",
    "confidence",
    "metadata",
}

_ALLOWED_REFERENCE_KEYS = {
    "kind",
    "key",
    "family",
    "name",
    "label",
    "price",
    "precision",
    "source",
    "formed_at",
    "known_at",
    "context",
}


class SignalReference(TypedDict, total=False):
    kind: str
    key: NotRequired[str]
    family: NotRequired[str]
    name: NotRequired[str]
    label: NotRequired[str]
    price: NotRequired[float]
    precision: NotRequired[int]
    source: NotRequired[str]
    formed_at: NotRequired[int | float | str | datetime]
    known_at: NotRequired[int | float | str | datetime]
    context: NotRequired[Mapping[str, Any]]


class SignalOutputEvent(TypedDict):
    key: str
    direction: NotRequired[str]
    pattern_id: NotRequired[str]
    known_at: NotRequired[int | float | str | datetime]
    confidence: NotRequired[float]
    metadata: NotRequired[Mapping[str, Any]]


def _to_epoch(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        numeric = float(value)
        return int(numeric) if math.isfinite(numeric) else None
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


def _metadata(event: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = event.get("metadata")
    return metadata if isinstance(metadata, Mapping) else {}


def _require_non_empty_text(value: Any, *, field: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"indicator_signal_output_invalid: {field} must be non-empty string")


def _require_finite_numeric(value: Any, *, field: str) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        raise RuntimeError(f"indicator_signal_output_invalid: {field} must be numeric") from None
    if not math.isfinite(numeric):
        raise RuntimeError(f"indicator_signal_output_invalid: {field} must be finite")
    return numeric


def _validate_signal_reference(reference: Mapping[str, Any]) -> None:
    unexpected_keys = sorted(str(key) for key in reference.keys() if key not in _ALLOWED_REFERENCE_KEYS)
    if unexpected_keys:
        raise RuntimeError(
            "indicator_signal_output_invalid: reference has unexpected keys "
            f"({','.join(unexpected_keys)})"
        )

    kind = reference.get("kind")
    if not isinstance(kind, str) or not kind.strip():
        raise RuntimeError("indicator_signal_output_invalid: reference.kind required")

    for field in ("key", "family", "name", "label", "source"):
        _require_non_empty_text(reference.get(field), field=f"reference.{field}")

    price = reference.get("price")
    if price is not None:
        _require_finite_numeric(price, field="reference.price")

    precision = reference.get("precision")
    if precision is not None:
        if isinstance(precision, bool):
            raise RuntimeError("indicator_signal_output_invalid: reference.precision must be int")
        try:
            normalized_precision = int(precision)
        except (TypeError, ValueError):
            raise RuntimeError("indicator_signal_output_invalid: reference.precision must be int") from None
        if normalized_precision < 0:
            raise RuntimeError("indicator_signal_output_invalid: reference.precision must be >= 0")

    for field in ("formed_at", "known_at"):
        value = reference.get(field)
        if value is not None and _to_epoch(value) is None:
            raise RuntimeError(f"indicator_signal_output_invalid: invalid reference.{field}")

    context = reference.get("context")
    if context is not None and not isinstance(context, Mapping):
        raise RuntimeError("indicator_signal_output_invalid: reference.context must be mapping")


def assert_signal_output_event(event: Mapping[str, Any]) -> None:
    """Raise if a signal-output item violates the canonical item shape."""

    if not isinstance(event, Mapping):
        raise RuntimeError("indicator_signal_output_invalid: event must be a mapping")

    unexpected_keys = sorted(str(key) for key in event.keys() if key not in _ALLOWED_EVENT_KEYS)
    if unexpected_keys:
        raise RuntimeError(
            "indicator_signal_output_invalid: unexpected keys "
            f"({','.join(unexpected_keys)})"
        )

    key = event.get("key")
    if not isinstance(key, str) or not key.strip():
        raise RuntimeError("indicator_signal_output_invalid: missing key")

    direction = event.get("direction")
    if direction is not None and (not isinstance(direction, str) or not direction.strip()):
        raise RuntimeError("indicator_signal_output_invalid: direction must be non-empty string")

    pattern_id = event.get("pattern_id")
    if pattern_id is not None and (not isinstance(pattern_id, str) or not pattern_id.strip()):
        raise RuntimeError("indicator_signal_output_invalid: pattern_id must be non-empty string")

    known_at = event.get("known_at")
    if known_at is not None and _to_epoch(known_at) is None:
        raise RuntimeError("indicator_signal_output_invalid: invalid known_at")

    confidence = event.get("confidence")
    if confidence is not None:
        try:
            numeric = float(confidence)
        except (TypeError, ValueError):
            raise RuntimeError("indicator_signal_output_invalid: confidence must be numeric") from None
        if not math.isfinite(numeric):
            raise RuntimeError("indicator_signal_output_invalid: confidence must be finite")

    metadata = event.get("metadata")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise RuntimeError("indicator_signal_output_invalid: metadata must be mapping")
    if isinstance(metadata, Mapping):
        reference = metadata.get("reference")
        if reference is not None:
            if not isinstance(reference, Mapping):
                raise RuntimeError("indicator_signal_output_invalid: metadata.reference must be mapping")
            _validate_signal_reference(reference)
        trigger_price = metadata.get("trigger_price")
        if trigger_price is not None:
            _require_finite_numeric(trigger_price, field="metadata.trigger_price")


def assert_signal_output_has_no_execution_fields(event: Mapping[str, Any], *, mode: str = "raise") -> list[str]:
    """Guard the signal-output boundary from execution-timing leakage."""

    leaks: list[str] = []
    metadata = _metadata(event)
    for field in _EXECUTION_FIELDS:
        if event.get(field) not in (None, "") or metadata.get(field) not in (None, ""):
            leaks.append(field)
    if leaks and mode == "raise":
        joined = ",".join(sorted(set(leaks)))
        raise RuntimeError(
            f"indicator_signal_output_invalid: execution fields not allowed ({joined})"
        )
    return leaks


def signal_output_known_at_epoch(event: Mapping[str, Any]) -> int | None:
    """Return the normalized known-at timestamp when present."""

    return _to_epoch(event.get("known_at"))


def signal_output_reference(event: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return the canonical signal reference payload when present."""

    metadata = _metadata(event)
    reference = metadata.get("reference")
    return reference if isinstance(reference, Mapping) else {}


__all__ = [
    "SignalReference",
    "SignalOutputEvent",
    "assert_signal_output_event",
    "assert_signal_output_has_no_execution_fields",
    "signal_output_known_at_epoch",
    "signal_output_reference",
]
