"""Provider-neutral policy contracts for supervised continuous streams.

The runtime that applies these policies lives at the worker/service boundary.
Keeping the contracts here lets paper/live feeds, archival collectors, and
future provider adapters share reconnect and bounded-buffer semantics without
importing one another's domain services.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class StreamReconnectPolicy:
    """Bounded reconnect behavior for one provider stream."""

    enabled: bool = True
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    continuous_disconnect_budget_seconds: float = 900.0
    heartbeat_stale_seconds: float = 30.0

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any] | None,
        *,
        defaults: "StreamReconnectPolicy | None" = None,
    ) -> "StreamReconnectPolicy":
        base = defaults or cls()
        payload = dict(value or {})
        unknown = sorted(
            set(payload)
            - {
                "reconnect_enabled",
                "initial_backoff_seconds",
                "max_backoff_seconds",
                "continuous_disconnect_budget_seconds",
                "heartbeat_stale_seconds",
            }
        )
        if unknown:
            raise ValueError(
                "stream_reconnect_policy contains unsupported fields: "
                + ",".join(unknown)
            )
        policy = cls(
            enabled=_coerce_bool(payload.get("reconnect_enabled", base.enabled), "reconnect_enabled"),
            initial_backoff_seconds=_coerce_float(
                payload.get("initial_backoff_seconds", base.initial_backoff_seconds),
                "initial_backoff_seconds",
                minimum=0.0,
            ),
            max_backoff_seconds=_coerce_float(
                payload.get("max_backoff_seconds", base.max_backoff_seconds),
                "max_backoff_seconds",
                minimum=0.001,
            ),
            continuous_disconnect_budget_seconds=_coerce_float(
                payload.get(
                    "continuous_disconnect_budget_seconds",
                    base.continuous_disconnect_budget_seconds,
                ),
                "continuous_disconnect_budget_seconds",
                minimum=0.001,
            ),
            heartbeat_stale_seconds=_coerce_float(
                payload.get("heartbeat_stale_seconds", base.heartbeat_stale_seconds),
                "heartbeat_stale_seconds",
                minimum=0.001,
            ),
        )
        if policy.max_backoff_seconds < policy.initial_backoff_seconds:
            raise ValueError(
                "stream_reconnect_policy.max_backoff_seconds must be >= initial_backoff_seconds"
            )
        return policy

    def to_dict(self) -> dict[str, Any]:
        return {
            "reconnect_enabled": self.enabled,
            "initial_backoff_seconds": self.initial_backoff_seconds,
            "max_backoff_seconds": self.max_backoff_seconds,
            "continuous_disconnect_budget_seconds": self.continuous_disconnect_budget_seconds,
            "heartbeat_stale_seconds": self.heartbeat_stale_seconds,
        }


@dataclass(frozen=True)
class ContinuousStreamPolicy:
    """Bound memory/disk handoff and reconnect policy for a stream collector."""

    segment_max_seconds: float = 60.0
    max_inflight_segments: int = 4
    lease_seconds: float = 90.0
    heartbeat_seconds: float = 10.0
    spool_reconcile_seconds: float = 300.0
    reconnect: StreamReconnectPolicy = field(default_factory=StreamReconnectPolicy)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "ContinuousStreamPolicy":
        payload = dict(value or {})
        unknown = sorted(
            set(payload)
            - {
                "segment_max_seconds",
                "max_inflight_segments",
                "lease_seconds",
                "heartbeat_seconds",
                "spool_reconcile_seconds",
                "reconnect_policy",
            }
        )
        if unknown:
            raise ValueError(
                "continuous_stream_policy contains unsupported fields: "
                + ",".join(unknown)
            )
        reconnect_payload = payload.get("reconnect_policy")
        if reconnect_payload is not None and not isinstance(
            reconnect_payload, Mapping
        ):
            raise ValueError(
                "continuous_stream_policy.reconnect_policy must be an object"
            )
        policy = cls(
            segment_max_seconds=_coerce_float(
                payload.get("segment_max_seconds", cls.segment_max_seconds),
                "segment_max_seconds",
                minimum=1.0,
            ),
            max_inflight_segments=_coerce_int(
                payload.get("max_inflight_segments", cls.max_inflight_segments),
                "max_inflight_segments",
                minimum=1,
            ),
            lease_seconds=_coerce_float(
                payload.get("lease_seconds", cls.lease_seconds),
                "lease_seconds",
                minimum=30.0,
            ),
            heartbeat_seconds=_coerce_float(
                payload.get("heartbeat_seconds", cls.heartbeat_seconds),
                "heartbeat_seconds",
                minimum=1.0,
            ),
            spool_reconcile_seconds=_coerce_float(
                payload.get(
                    "spool_reconcile_seconds",
                    cls.spool_reconcile_seconds,
                ),
                "spool_reconcile_seconds",
                minimum=30.0,
            ),
            reconnect=StreamReconnectPolicy.from_mapping(
                reconnect_payload
            ),
        )
        if policy.heartbeat_seconds >= policy.lease_seconds:
            raise ValueError(
                "continuous_stream_policy.heartbeat_seconds must be less than lease_seconds"
            )
        return policy

    def to_dict(self) -> dict[str, Any]:
        return {
            "segment_max_seconds": self.segment_max_seconds,
            "max_inflight_segments": self.max_inflight_segments,
            "lease_seconds": self.lease_seconds,
            "heartbeat_seconds": self.heartbeat_seconds,
            "spool_reconcile_seconds": self.spool_reconcile_seconds,
            "reconnect_policy": self.reconnect.to_dict(),
        }


def _coerce_bool(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    raise ValueError(f"stream_reconnect_policy.{field} must be boolean")


def _coerce_float(value: Any, field: str, *, minimum: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"continuous_stream_policy.{field} must be numeric") from None
    if number < minimum:
        raise ValueError(f"continuous_stream_policy.{field} must be >= {minimum}")
    return number


def _coerce_int(value: Any, field: str, *, minimum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"continuous_stream_policy.{field} must be an integer") from None
    if number < minimum:
        raise ValueError(f"continuous_stream_policy.{field} must be >= {minimum}")
    return number


__all__ = ["ContinuousStreamPolicy", "StreamReconnectPolicy"]
