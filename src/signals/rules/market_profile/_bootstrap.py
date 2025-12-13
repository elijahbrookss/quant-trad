"""Bootstrap helpers for Market Profile rule execution."""

from __future__ import annotations

from typing import Any, MutableMapping, Sequence

from signals.rules.common.cache import mark_ready

from .breakout import _BREAKOUT_READY_FLAG, market_profile_breakout_rule


def ensure_breakouts_ready(context: MutableMapping[str, Any], payloads: Sequence[Any]) -> None:
    """Populate breakout cache once per context before running dependent rules."""

    if context.get(_BREAKOUT_READY_FLAG):
        return

    payload = payloads[0] if payloads else {}
    market_profile_breakout_rule(context, payload)
    mark_ready(context, _BREAKOUT_READY_FLAG, ready=True)


__all__ = ["ensure_breakouts_ready"]
