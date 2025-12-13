"""
Retest signal rule for Market Profile indicator.

Detects pullbacks to recently broken value area boundaries that hold.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping

from signals.engine.signal_generator import signal_rule
from signals.rules.common.cache import mark_ready
from signals.rules.patterns import evaluate_signal_patterns

from ._meta import ensure_market_profile_rule_metadata

from ._evaluators.retest_eval import RETEST_PATTERN

log = logging.getLogger("MarketProfileRetest")

# Re-use cache keys from breakout rule
_BREAKOUT_READY_FLAG = "_market_profile_breakouts_ready"


@signal_rule(
    "market_profile",
    rule_id="market_profile_retest",
    label="Value Area Retest",
    description=(
        "Highlights pullbacks to a recently broken value area boundary that hold, "
        "signalling continuation setups."
    ),
)
def market_profile_retest_rule(
    context: Mapping[str, Any], payload: Any
) -> List[Dict[str, Any]]:
    """
    Detect retests of broken value area boundaries.

    Depends on breakout cache being populated first.

    Args:
        context: Execution context with dataframe and configuration
        payload: Value area data to evaluate

    Returns:
        List of signal dictionaries for detected retests
    """
    df = context.get("df")
    if df is None or getattr(df, "empty", True):
        return []

    if not context.get(_BREAKOUT_READY_FLAG):
        log.debug("Retest rule | skip | reason=breakouts_not_ready")
        return []

    # Evaluate retest pattern
    results = [
        ensure_market_profile_rule_metadata(
            meta,
            rule_id=RETEST_PATTERN.rule_id or "market_profile_retest",
            pattern_id=RETEST_PATTERN.pattern_id,
            aliases=(RETEST_PATTERN.signal_type,),
        )
        for meta in evaluate_signal_patterns(context, payload, [RETEST_PATTERN])
    ]

    mark_ready(context, _BREAKOUT_READY_FLAG, ready=True)

    log.debug("Retest rule | emitted=%d", len(results))
    return results
