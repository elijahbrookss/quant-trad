"""Close-only Market Profile retest v2 rule."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping

from signals.engine.signal_generator import signal_rule
from signals.rules.common.cache import ensure_cache
from signals.rules.market_profile._evaluators.retest_v2_eval import detect_retests_v2
from signals.rules.market_profile.breakout_v2 import (
    _BREAKOUT_V2_CACHE_KEY,
    _BREAKOUT_V2_READY_FLAG,
)

log = logging.getLogger("MarketProfileRetestV2")


@signal_rule(
    "market_profile",
    rule_id="market_profile_retest_v2",
    label="Value Area Retest v2",
    description="Close-only retests of breakout v2 events with quick reclaim/reject.",
)
def market_profile_retest_v2_rule(
    context: Mapping[str, Any], payload: Any
) -> List[Dict[str, Any]]:
    df = context.get("df")
    if df is None or getattr(df, "empty", True):
        return []

    if not context.get(_BREAKOUT_V2_READY_FLAG):
        log.debug("Retest v2 | skip | breakouts not ready")
        return []

    ensure_cache(context, _BREAKOUT_V2_CACHE_KEY, list)
    breakouts = context.get(_BREAKOUT_V2_CACHE_KEY, [])

    results = detect_retests_v2(context, payload, breakouts, window=5, reclaim_bars=1)
    log.debug("Retest v2 | emitted=%d", len(results))
    return results


__all__ = ["market_profile_retest_v2_rule"]
