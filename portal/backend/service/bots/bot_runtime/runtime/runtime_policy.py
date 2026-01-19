"""Run-type policy helpers for bot runtime."""

from __future__ import annotations

from dataclasses import dataclass


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
        allow_live_refresh = normalized == "sim_trade"
        use_intrabar = True
        enforce_wallet = True
        return RuntimeModePolicy(
            run_type=normalized,
            allow_live_refresh=allow_live_refresh,
            use_intrabar=use_intrabar,
            enforce_wallet=enforce_wallet,
        )


__all__ = ["RuntimeModePolicy"]
