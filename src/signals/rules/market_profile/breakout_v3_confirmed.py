"""Decorator registration for runtime-emitted Market Profile breakout v3 signals."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from signals.engine.signal_generator import RulePhase, signal_rule


@signal_rule(
    "market_profile",
    rule_id="market_profile_breakout_v3_confirmed",
    label="Value Area Breakout v3",
    description="Confirmed VAH/VAL breakout and break-in events (body-only, candidate+confirm lifecycle).",
    phase=RulePhase.PER_PAYLOAD,
)
def market_profile_breakout_v3_confirmed_rule(
    context: Mapping[str, Any], payload: Any
) -> List[Dict[str, Any]]:
    """Catalog-visible no-op rule.

    Runtime signal emission for this rule_id is produced by the market_profile
    runtime plugin; this decorator exists so rule discovery/UI wiring is automatic.
    """
    _ = context
    _ = payload
    return []


__all__ = ["market_profile_breakout_v3_confirmed_rule"]
