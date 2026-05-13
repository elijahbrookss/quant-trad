"""Run-type policy helpers for bot runtime."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ExecutionMode(str, Enum):
    """Runtime execution semantics, independent from playback pacing."""

    FAST = "fast"
    FULL = "full"

    @classmethod
    def normalize(cls, value: object) -> "ExecutionMode":
        normalized = str(value or "").strip().lower().replace("_", "-")
        if normalized in {"fast", "instant"}:
            return cls.FAST
        if normalized in {"full", "walk-forward", "walkforward"}:
            return cls.FULL
        raise ValueError(f"Unknown execution_mode '{value}'. Expected FAST or FULL.")

    @classmethod
    def from_config(cls, value: object, *, legacy_mode: object = None) -> "ExecutionMode":
        if value not in (None, ""):
            return cls.normalize(value)
        legacy = str(legacy_mode or "").strip().lower().replace("_", "-")
        if legacy in {"walk-forward", "walkforward"}:
            return cls.FULL
        return cls.FAST


@dataclass(frozen=True)
class RuntimeModePolicy:
    """Centralize run-type behavior switches."""

    run_type: str
    allow_live_refresh: bool
    use_intrabar: bool
    enforce_wallet: bool

    @staticmethod
    def for_run_type(run_type: str) -> "RuntimeModePolicy":
        normalized = (run_type or "").lower()
        allow_live_refresh = normalized in {"sim_trade", "paper", "live"}
        use_intrabar = True
        enforce_wallet = True
        return RuntimeModePolicy(
            run_type=normalized,
            allow_live_refresh=allow_live_refresh,
            use_intrabar=use_intrabar,
            enforce_wallet=enforce_wallet,
        )


__all__ = ["ExecutionMode", "RuntimeModePolicy"]
