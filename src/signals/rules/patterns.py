"""Utility helpers for declarative signal pattern definitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Sequence

PatternResult = Mapping[str, Any]
PatternReturn = Optional[Iterable[PatternResult] | PatternResult]
PatternEvaluator = Callable[[Mapping[str, Any], Any], PatternReturn]


@dataclass(frozen=True)
class SignalPattern:
    """Declarative definition for a simple indicator signal pattern."""

    pattern_id: str
    label: str
    description: str
    signal_type: str
    evaluator: PatternEvaluator
    rule_id: Optional[str] = None

    def evaluate(self, context: Mapping[str, Any], payload: Any) -> List[PatternResult]:
        """Run the pattern evaluator and normalise its results."""

        return _normalise_pattern_results(self.evaluator(context, payload))


def _normalise_pattern_results(result: PatternReturn) -> List[PatternResult]:
    if result is None:
        return []

    if isinstance(result, Mapping):
        return [result]

    if isinstance(result, Sequence) and not isinstance(result, (str, bytes)):
        normalised: List[PatternResult] = []
        for item in result:
            if isinstance(item, Mapping):
                normalised.append(item)
        return normalised

    if isinstance(result, Iterable):
        normalised = [item for item in result if isinstance(item, Mapping)]
        if normalised:
            return normalised

    raise TypeError(
        "Signal pattern evaluators must return a mapping, an iterable of mappings, or None."
    )


def evaluate_signal_patterns(
    context: Mapping[str, Any],
    payload: Any,
    patterns: Sequence[SignalPattern],
    *,
    default_confidence: float = 1.0,
) -> List[dict]:
    """Evaluate a sequence of patterns and enrich their metadata payloads."""

    results: List[dict] = []
    for pattern in patterns:
        for meta in pattern.evaluate(context, payload):
            enriched = dict(meta)
            enriched.setdefault("type", pattern.signal_type)
            enriched.setdefault("confidence", default_confidence)
            enriched.setdefault("pattern_id", pattern.pattern_id)
            enriched.setdefault("rule_id", pattern.rule_id or pattern.pattern_id)
            enriched.setdefault("pattern_label", pattern.label)
            enriched.setdefault("pattern_description", pattern.description)
            # Ensure metadata mirrors both identifiers for downstream matching.
            metadata = enriched.get("metadata")
            if isinstance(metadata, MutableMapping):
                metadata.setdefault("pattern_id", enriched["pattern_id"])
                metadata.setdefault("rule_id", enriched["rule_id"])
            results.append(enriched)
    return results


def maybe_mutable_context(context: Mapping[str, Any]) -> Optional[MutableMapping[str, Any]]:
    """Return a mutable mapping when the provided context supports mutation."""

    if isinstance(context, MutableMapping):
        return context
    return None


def assign_rule_metadata(
    rule: Callable[..., Any],
    *,
    rule_id: str,
    label: str,
    description: str,
) -> None:
    """Attach descriptive metadata for discovery APIs on a rule callable."""

    setattr(rule, "signal_id", rule_id)
    setattr(rule, "signal_label", label)
    setattr(rule, "signal_description", description)


__all__ = [
    "SignalPattern",
    "evaluate_signal_patterns",
    "maybe_mutable_context",
    "assign_rule_metadata",
]
