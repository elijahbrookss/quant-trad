"""Close-only Market Profile breakout v2 rule."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping

from signals.engine.signal_generator import signal_rule
from signals.rules.common.cache import append_to_cache, ensure_cache, mark_ready
from signals.rules.market_profile._evaluators.breakout_v2_eval import detect_breakouts_v2

log = logging.getLogger("MarketProfileBreakoutV2")

_BREAKOUT_V2_CACHE_KEY = "market_profile_breakouts_v2"
_BREAKOUT_V2_READY_FLAG = "_market_profile_breakouts_v2_ready"


@signal_rule(
    "market_profile",
    rule_id="market_profile_breakout_v2",
    label="Value Area Breakout v2",
    description="Close-only transitions across VAH/VAL with 3-bar confirmation and explicit origin zones.",
)
def market_profile_breakout_v2_rule(
    context: Mapping[str, Any], payload: Any
) -> List[Dict[str, Any]]:
    df = context.get("df")
    if df is None or getattr(df, "empty", True):
        return []

    ensure_cache(context, _BREAKOUT_V2_CACHE_KEY, list)

    confirm_bars = int(context.get("market_profile_breakout_v2_confirm_bars", 3) or 3)
    lockout_bars = int(context.get("market_profile_breakout_v2_lockout_bars", 3) or 3)

    matches = detect_breakouts_v2(
        context,
        payload,
        confirm_bars=confirm_bars,
        lockout_bars=lockout_bars,
    )
    results = []

    for meta in matches:
        # Guard against duplicates via cache
        if append_to_cache(context, _BREAKOUT_V2_CACHE_KEY, [meta]):
            results.append(meta)

    mark_ready(context, _BREAKOUT_V2_READY_FLAG)
    log.debug("Breakout v2 | emitted=%d | cache_size=%d", len(results), len(context.get(_BREAKOUT_V2_CACHE_KEY, [])))
    return results


__all__ = [
    "market_profile_breakout_v2_rule",
    "_BREAKOUT_V2_CACHE_KEY",
    "_BREAKOUT_V2_READY_FLAG",
]
