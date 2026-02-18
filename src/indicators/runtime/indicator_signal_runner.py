from __future__ import annotations

"""Signal execution utilities for indicators."""

from typing import Any, Dict, List, Mapping, Optional

import logging

from signals.engine.signal_generator import (
    build_signal_overlays,
    describe_indicator_rules,
    run_indicator_rules,
)

logger = logging.getLogger(__name__)

_RULE_HINTS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "market_profile": {
        "market_profile_breakout": {
            "signal_type": "breakout",
            "directions": [
                {
                    "id": "long",
                    "label": "Long breakout",
                    "description": "Breakout above the active value area high (VAH) that confirms continuation.",
                },
                {
                    "id": "short",
                    "label": "Short breakdown",
                    "description": "Breakdown below the active value area low (VAL) signalling downside momentum.",
                },
            ],
        },
        "market_profile_retest": {
            "signal_type": "retest",
            "directions": [
                {
                    "id": "long",
                    "label": "Long retest",
                    "description": (
                        "Breakout above VAH with a successful retest hold or a reclaim of VAL after a breakout,"
                        " favouring continuation to the upside."
                    ),
                },
                {
                    "id": "short",
                    "label": "Short retest",
                    "description": (
                        "Breakdown below VAH with a rejection retest or a breakdown of VAL that holds," " signalling continuation lower."
                    ),
                },
            ],
        },
    },
    "pivot_level": {
        "pivot_breakout": {
            "signal_type": "breakout",
        },
        "pivot_retest": {
            "signal_type": "retest",
        },
    },
}

_RULE_SUNSET: Dict[str, set[str]] = {
    "market_profile": {
        "market_profile_breakout_v2",
        "market_profile_retest_v2",
        "market_profile_breakout_v3_confirmed",
    }
}


class IndicatorSignalRunner:
    """Run indicator rules and overlays."""

    def describe_rules(self, indicator_type: str) -> List[Dict[str, Any]]:
        return describe_indicator_rules(indicator_type) or []

    def run_rules(self, indicator, df, **config):
        return run_indicator_rules(indicator, df, **config)

    def build_overlays(self, indicator, signals, df, **config):
        return build_signal_overlays(indicator, signals, df, **config)

    def build_signal_catalog(self, indicator_type: str) -> List[Dict[str, Any]]:
        rule_meta = self.describe_rules(indicator_type)

        logger.info(
            "build_signal_catalog | indicator_type='%s' | describe_rules_returned=%d rules",
            indicator_type,
            len(rule_meta) if rule_meta else 0
        )

        if not rule_meta:
            logger.warning(
                "⚠ No rules found for indicator_type='%s' | Check: "
                "1) Rules decorated with @signal_rule('%s', ...) "
                "2) Rules imported in src/signals/__init__.py "
                "3) Indicator NAME matches decorator arg",
                indicator_type,
                indicator_type
            )
            return []

        catalog: List[Dict[str, Any]] = []
        indicator_key = indicator_type.lower()
        hints_for_indicator = _RULE_HINTS.get(indicator_key, {})

        for entry in rule_meta:
            rule_id = str(entry.get("id", "")).strip()
            if not rule_id:
                continue
            if rule_id.lower() in _RULE_SUNSET.get(indicator_key, set()):
                continue
            hint = hints_for_indicator.get(rule_id.lower(), {})
            signal_type = hint.get("signal_type") or self._guess_signal_type(
                indicator_key, rule_id
            )
            directions = hint.get("directions") or self._default_direction_hints(signal_type)
            enriched = dict(entry)
            enriched["signal_type"] = signal_type
            if directions:
                enriched["directions"] = directions
            catalog.append(enriched)

        return catalog

    def _guess_signal_type(self, indicator_type: str, rule_id: str) -> str:
        hints = _RULE_HINTS.get(indicator_type.lower(), {}).get(rule_id.lower(), {})
        if hints.get("signal_type"):
            return str(hints["signal_type"])

        rule_key = rule_id.lower()
        if "retest" in rule_key:
            return "retest"
        if "breakout" in rule_key or "break" in rule_key:
            return "breakout"
        if "touch" in rule_key:
            return "touch"
        if "trend" in rule_key:
            return "trend"
        return rule_key or "signal"

    def _default_direction_hints(self, signal_type: str) -> List[Dict[str, str]]:
        normalized = (signal_type or "").lower()
        if normalized in {"breakout", "retest", "touch", "trend"}:
            return [
                {
                    "id": "long",
                    "label": "Long",
                    "description": "Setup that supports a long bias.",
                },
                {
                    "id": "short",
                    "label": "Short",
                    "description": "Setup that supports a short bias.",
                },
            ]
        return []


def default_signal_runner() -> IndicatorSignalRunner:
    return IndicatorSignalRunner()
