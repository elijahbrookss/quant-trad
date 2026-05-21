"""Bot execution behavior helpers.

Execution behavior is separate from run type.  `paper` says which runtime mode
is selected; `observe-only` says the runtime must ingest market data without
entering order, fill, trade, fee, slippage, or wallet mutation semantics.
"""

from __future__ import annotations

from typing import Any, Mapping

OBSERVE_ONLY_BEHAVIOR = "observe-only"
SIMULATED_BEHAVIOR = "simulated"


def normalize_execution_behavior(value: Any) -> str:
    text = str(value or SIMULATED_BEHAVIOR).strip().lower().replace("_", "-")
    if text in {"observe-only", "observe", "observer", "no-fill", "nofill"}:
        return OBSERVE_ONLY_BEHAVIOR
    if text in {"simulated", "simulate", "normal", "default", ""}:
        return SIMULATED_BEHAVIOR
    raise ValueError("execution_behavior must be 'simulated' or 'observe-only'")


def execution_behavior_from_bot(bot: Mapping[str, Any]) -> str:
    risk = bot.get("risk") if isinstance(bot.get("risk"), Mapping) else {}
    return normalize_execution_behavior(bot.get("execution_behavior") or risk.get("execution_behavior"))


def is_observe_only_bot(bot: Mapping[str, Any]) -> bool:
    return (
        str(bot.get("run_type") or "").strip().lower() == "paper"
        and execution_behavior_from_bot(bot) == OBSERVE_ONLY_BEHAVIOR
    )


def is_observe_only_run(run: Mapping[str, Any]) -> bool:
    config = run.get("config_snapshot") if isinstance(run.get("config_snapshot"), Mapping) else {}
    bot = config.get("bot") if isinstance(config.get("bot"), Mapping) else {}
    risk = bot.get("risk") if isinstance(bot.get("risk"), Mapping) else {}
    value = config.get("execution_behavior") or bot.get("execution_behavior") or risk.get("execution_behavior")
    return (
        str(run.get("run_type") or bot.get("run_type") or "").strip().lower() == "paper"
        and normalize_execution_behavior(value) == OBSERVE_ONLY_BEHAVIOR
    )


__all__ = [
    "OBSERVE_ONLY_BEHAVIOR",
    "SIMULATED_BEHAVIOR",
    "execution_behavior_from_bot",
    "is_observe_only_bot",
    "is_observe_only_run",
    "normalize_execution_behavior",
]
