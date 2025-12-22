"""Close-only Market Profile retest v2 rule."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Sequence

from signals.engine.signal_generator import RulePhase, signal_rule
from signals.rules.market_profile._evaluators.retest_v2_eval import detect_retests_v2
from signals.rules.market_profile.breakout_v2 import _BREAKOUT_V2_CACHE_KEY

log = logging.getLogger("MarketProfileRetestV2")


@signal_rule(
    "market_profile",
    rule_id="market_profile_retest_v2",
    label="Value Area Retest v2",
    description="Pullback detection: walks forward from breakout signals looking for price returning near broken level.",
    phase=RulePhase.AGGREGATION,  # Run after bootstrap phase
    depends_on=["market_profile_breakout_v2"],  # Explicit dependency
)
def market_profile_retest_v2_rule(
    context: Mapping[str, Any], payloads: Sequence[Any]
) -> List[Dict[str, Any]]:
    """Aggregation rule: runs once after breakout_v2, scans all cached breakouts for retests."""
    df = context.get("df")
    if df is None or getattr(df, "empty", True):
        return []

    # Read breakouts from cache populated by breakout_v2 in BOOTSTRAP phase
    all_breakouts = context.get(_BREAKOUT_V2_CACHE_KEY, [])

    if not all_breakouts:
        log.warning("Retest v2 | no breakouts found in cache - breakout_v2 must run first")
        return []

    log.debug("Retest v2 | using cached breakouts=%d", len(all_breakouts))

    # Use 50 candles max lookback for pullback detection
    max_lookback = int(context.get("market_profile_retest_v2_max_lookback", 50))
    results = detect_retests_v2(context, None, all_breakouts, max_lookback=max_lookback)

    log.debug("Retest v2 | emitted=%d | scanned_breakouts=%d", len(results), len(all_breakouts))
    return results


__all__ = ["market_profile_retest_v2_rule"]
