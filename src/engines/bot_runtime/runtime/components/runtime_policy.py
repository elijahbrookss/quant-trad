"""Run-type policy helpers for bot runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Sequence, Tuple


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


@dataclass(frozen=True)
class SharedWalletArbitrationDecision:
    """Policy decision for a shared-wallet candidate waiting on its turn."""

    action: str
    reason: str
    blocking_participants: Tuple[str, ...] = ()
    portfolio_watermark: Optional[str] = None
    diagnostics: Mapping[str, Any] = field(default_factory=dict)


class SharedWalletArbitrationPolicy:
    """Mode-specific shared-wallet arbitration semantics.

    The entry decision coordinator owns ordering mechanics and participant
    state. This policy owns whether a waiting candidate should keep waiting or
    fail for the current runtime mode.
    """

    name = "shared_wallet_arbitration"

    def decide_candidate_turn(
        self,
        *,
        candidate_has_turn: bool,
        blocking_participants: Sequence[str],
        elapsed_seconds: float,
        timeout_seconds: float,
        state: Mapping[str, Any],
        diagnostics: Mapping[str, Any],
    ) -> SharedWalletArbitrationDecision:
        if candidate_has_turn:
            return SharedWalletArbitrationDecision(
                action="release",
                reason="candidate_has_active_turn",
                blocking_participants=(),
                portfolio_watermark=_portfolio_watermark(diagnostics),
            )
        return SharedWalletArbitrationDecision(
            action="wait",
            reason="waiting_for_candidate_turn",
            blocking_participants=_participant_tuple(blocking_participants),
            portfolio_watermark=_portfolio_watermark(diagnostics),
        )

    @staticmethod
    def for_run_type(
        run_type: str,
        *,
        timeout_seconds: float = 120.0,
    ) -> "SharedWalletArbitrationPolicy":
        normalized = str(run_type or "").strip().lower()
        if normalized == "backtest":
            return BacktestSharedWalletArbitrationPolicy()
        return WallClockSharedWalletArbitrationPolicy(timeout_seconds=timeout_seconds)


class BacktestSharedWalletArbitrationPolicy(SharedWalletArbitrationPolicy):
    """Backtest arbitration waits on market progress, not wall-clock expiry."""

    name = "backtest_shared_wallet_arbitration"

    def decide_candidate_turn(
        self,
        *,
        candidate_has_turn: bool,
        blocking_participants: Sequence[str],
        elapsed_seconds: float,
        timeout_seconds: float,
        state: Mapping[str, Any],
        diagnostics: Mapping[str, Any],
    ) -> SharedWalletArbitrationDecision:
        del elapsed_seconds, timeout_seconds, state
        if candidate_has_turn:
            return SharedWalletArbitrationDecision(
                action="release",
                reason="candidate_has_active_turn",
                blocking_participants=(),
                portfolio_watermark=_portfolio_watermark(diagnostics),
            )
        blocking = _participant_tuple(blocking_participants)
        return SharedWalletArbitrationDecision(
            action="wait",
            reason="backtest_waiting_for_market_progress" if blocking else "backtest_waiting_for_candidate_turn",
            blocking_participants=blocking,
            portfolio_watermark=_portfolio_watermark(diagnostics),
        )


@dataclass(frozen=True)
class WallClockSharedWalletArbitrationPolicy(SharedWalletArbitrationPolicy):
    """Compatibility policy for non-backtest/default runtime behavior."""

    timeout_seconds: float = 120.0
    name = "wall_clock_shared_wallet_arbitration"

    def decide_candidate_turn(
        self,
        *,
        candidate_has_turn: bool,
        blocking_participants: Sequence[str],
        elapsed_seconds: float,
        timeout_seconds: float,
        state: Mapping[str, Any],
        diagnostics: Mapping[str, Any],
    ) -> SharedWalletArbitrationDecision:
        del state
        timeout_value = max(float(timeout_seconds or self.timeout_seconds or 0.0), 0.001)
        blocking = _participant_tuple(blocking_participants)
        if candidate_has_turn:
            return SharedWalletArbitrationDecision(
                action="release",
                reason="candidate_has_active_turn",
                blocking_participants=(),
                portfolio_watermark=_portfolio_watermark(diagnostics),
            )
        if float(elapsed_seconds or 0.0) >= timeout_value:
            return SharedWalletArbitrationDecision(
                action="fail",
                reason="wall_clock_turn_timeout",
                blocking_participants=blocking,
                portfolio_watermark=_portfolio_watermark(diagnostics),
            )
        return SharedWalletArbitrationDecision(
            action="wait",
            reason="wall_clock_waiting_for_candidate_turn",
            blocking_participants=blocking,
            portfolio_watermark=_portfolio_watermark(diagnostics),
        )


def _participant_tuple(values: Sequence[str]) -> Tuple[str, ...]:
    return tuple(sorted(str(value) for value in values if str(value).strip()))


def _portfolio_watermark(diagnostics: Mapping[str, Any]) -> Optional[str]:
    progress = diagnostics.get("participant_progress")
    if not isinstance(progress, Mapping):
        return None
    next_times = []
    for payload in progress.values():
        if not isinstance(payload, Mapping):
            continue
        next_time = str(payload.get("next_bar_time") or "").strip()
        if next_time:
            next_times.append(next_time)
    if not next_times:
        return None
    return min(next_times)


__all__ = [
    "BacktestSharedWalletArbitrationPolicy",
    "ExecutionMode",
    "RuntimeModePolicy",
    "SharedWalletArbitrationDecision",
    "SharedWalletArbitrationPolicy",
    "WallClockSharedWalletArbitrationPolicy",
]
