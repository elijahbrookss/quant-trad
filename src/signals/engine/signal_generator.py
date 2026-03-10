"""Signal generation orchestrator with registry-based rule dispatch."""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union

from engines.indicator_engine.plugins import (
    ensure_builtin_indicator_plugins_registered,
    plugin_registry,
)

from signals.base import BaseSignal
from signals.overlays.registry import get_overlay_spec
from signals.overlays.schema import normalize_overlays


import time
from pprint import pformat

try:  # pragma: no cover - optional import for type checking only
    from pandas import DataFrame  # type: ignore
except Exception:  # pragma: no cover
    DataFrame = Any  # fallback for environments without pandas


logger = logging.getLogger(__name__)

RuleCallable = Callable[[Mapping[str, Any], Any], Optional[Sequence[Mapping[str, Any]]]]
OverlayAdapter = Callable[[Sequence[BaseSignal], "DataFrame"], Sequence[Mapping[str, Any]]]
_RESERVED_CONFIG_KEYS = {"rule_payloads", "enabled_rules"}
_TRACE_CONFIG_KEYS = {"trace", "log_context", "validate_only"}


def _registration_from_plugin(indicator_type: str) -> Optional[Tuple[Sequence[RuleCallable], Optional[OverlayAdapter]]]:
    key = str(indicator_type or "").strip().lower()
    ensure_builtin_indicator_plugins_registered()
    try:
        manifest = plugin_registry().resolve(key)
    except RuntimeError:
        return None
    rules: tuple[RuleCallable, ...] = ()
    return rules, manifest.signal_overlay_adapter


def _df_summary(df: "DataFrame") -> Mapping[str, Any]:
    try:
        rows = len(df)
        cols = list(getattr(df, "columns", []))
        start = getattr(getattr(df, "index", []), "__getitem__", lambda *_: None)(0)
        end = getattr(getattr(df, "index", []), "__getitem__", lambda *_: None)(-1)
        return {"rows": rows, "cols": cols, "start": start, "end": end}
    except Exception:
        shape = getattr(df, "shape", ("?", "?"))
        return {"rows": shape[0], "cols": shape[1]}

def enable_diagnostic_logging(level: int = logging.DEBUG) -> None:
    """One-call pretty logger for local debugging."""
    root = logging.getLogger()
    if not root.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        ))
        root.addHandler(h)
    root.setLevel(level)
    logger.debug("Diagnostic logging enabled at level=%s", logging.getLevelName(level))


def _normalise_indicator_type(indicator: Union[str, Any]) -> str:
    if isinstance(indicator, str):
        return str(indicator).strip().lower()
    if isinstance(indicator, type):
        return str(getattr(indicator, "NAME", indicator.__name__)).strip().lower()
    return str(getattr(indicator, "NAME", indicator.__class__.__name__)).strip().lower()


def _rule_identifiers(rule: RuleCallable) -> Tuple[str, ...]:
    """Return a tuple of identifiers that can reference a rule."""

    identifiers: List[str] = []

    explicit = getattr(rule, "signal_id", None)
    if explicit:
        identifiers.append(str(explicit))

    label = getattr(rule, "signal_label", None)
    if label:
        identifiers.append(str(label))

    name = getattr(rule, "__name__", None)
    if name:
        identifiers.append(str(name))

    # Final fallback to repr to ensure at least one identifier
    if not identifiers:
        identifiers.append(repr(rule))

    # Normalise identifiers for comparisons (case-insensitive)
    normalised = tuple({ident.lower(): ident for ident in identifiers}.values())
    return normalised if normalised else (repr(rule),)



def _resolve_dependencies(
    all_rules: Sequence[RuleCallable],
    explicitly_requested: Set[str],
) -> Tuple[List[RuleCallable], Set[str]]:
    """Resolve rule dependencies and return (rules_to_execute, explicitly_requested_ids).

    Args:
        all_rules: All available rules
        explicitly_requested: Set of rule_ids that were explicitly requested

    Returns:
        - rules_to_execute: List of rules to execute (includes dependencies)
        - explicitly_requested_ids: Set of rule_ids that were explicitly requested (unchanged)
    """
    # Build a map of rule_id -> rule
    rules_by_id: Dict[str, RuleCallable] = {}
    for rule in all_rules:
        rule_id = getattr(rule, "signal_id", None)
        if rule_id:
            rules_by_id[rule_id] = rule

    # Track which rules to execute (start with explicitly requested)
    rules_to_execute_ids: Set[str] = set(explicitly_requested)

    # Recursively add dependencies
    to_process = list(explicitly_requested)
    while to_process:
        current_rule_id = to_process.pop()
        current_rule = rules_by_id.get(current_rule_id)
        if not current_rule:
            continue

        dependencies = getattr(current_rule, "_rule_depends_on", [])
        for dep_id in dependencies:
            if dep_id not in rules_to_execute_ids:
                logger.debug("Adding dependency: %s (required by %s)", dep_id, current_rule_id)
                rules_to_execute_ids.add(dep_id)
                to_process.append(dep_id)

    # Build final list of rules to execute (preserve original order)
    final_rules = [rule for rule in all_rules if getattr(rule, "signal_id", None) in rules_to_execute_ids]

    return final_rules, explicitly_requested


def _filter_enabled_rules(
    rules: Sequence[RuleCallable],
    enabled_rules: Optional[Iterable[Any]],
    indicator_type: str,
) -> Tuple[Sequence[RuleCallable], Set[str]]:
    """Filter rules based on enabled_rules config and resolve dependencies.

    Returns:
        - filtered_rules: List of rules to execute (includes dependencies)
        - explicitly_requested: Set of rule_ids that were explicitly requested (for signal filtering)
    """
    if not enabled_rules:
        # No filter specified - all rules are explicitly requested
        explicitly_requested = {getattr(r, "signal_id", None) for r in rules if getattr(r, "signal_id", None)}
        return rules, explicitly_requested

    desired: Set[str] = {str(rule_id).lower() for rule_id in enabled_rules if rule_id is not None}
    if not desired:
        explicitly_requested = {getattr(r, "signal_id", None) for r in rules if getattr(r, "signal_id", None)}
        return rules, explicitly_requested

    # Find explicitly requested rules
    filtered: List[RuleCallable] = []
    explicitly_requested: Set[str] = set()

    for rule in rules:
        identifiers = {ident.lower() for ident in _rule_identifiers(rule)}
        if identifiers & desired:
            filtered.append(rule)
            rule_id = getattr(rule, "signal_id", None)
            if rule_id:
                explicitly_requested.add(rule_id)

    if not filtered:
        logger.warning(
            "No matching enabled rules for indicator '%s'. Requested=%s available=%s",
            indicator_type,
            sorted(desired),
            [tuple(_rule_identifiers(rule))[0] for rule in rules],
        )
        return rules, {getattr(r, "signal_id", None) for r in rules if getattr(r, "signal_id", None)}

    # Resolve dependencies
    final_rules, explicitly_requested = _resolve_dependencies(rules, explicitly_requested)

    return tuple(final_rules), explicitly_requested


def _resolve_payloads(config: Mapping[str, Any]) -> List[Any]:
    payloads = config.get("rule_payloads")
    if payloads is None:
        return [None]
    if isinstance(payloads, Iterable) and not isinstance(payloads, (str, bytes, Mapping)):
        return list(payloads)
    return [payloads]


def _build_context(
    indicator: Any,
    indicator_type: str,
    market_df: "DataFrame",
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    context = {"indicator": indicator, "indicator_type": indicator_type, "df": market_df}
    for key, value in config.items():
        if key in _RESERVED_CONFIG_KEYS:
            continue
        context[key] = value
    if "symbol" not in context and hasattr(indicator, "symbol"):
        context["symbol"] = getattr(indicator, "symbol")
    if indicator_type not in context and not isinstance(indicator, str):
        context[indicator_type] = indicator
    return context


def _metadata_to_signal(meta: Mapping[str, Any], default_confidence: float = 1.0) -> BaseSignal:
    try:
        signal_type = meta["type"]
        symbol = meta["symbol"]
        timestamp = meta["time"]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Signal metadata missing required field: {exc}") from exc

    confidence = meta.get("confidence", default_confidence)
    metadata = {
        key: value
        for key, value in meta.items()
        if key not in {"type", "symbol", "time", "confidence"}
    }
    return BaseSignal(
        type=signal_type,
        symbol=symbol,
        time=timestamp,
        confidence=confidence,
        metadata=metadata,
    )


def _validate_and_process_signal(
    meta: Mapping[str, Any],
    rule_name: str,
    rule_id: Optional[str],
    context: Mapping[str, Any],
    validate_only: bool,
    signals: List[BaseSignal],
    explicitly_requested: Set[str],
    payload_idx: Optional[int] = None,
) -> bool:
    """Validate signal metadata and add to signals list. Returns True if successful.

    Args:
        rule_id: The rule_id of the rule that generated this signal
        explicitly_requested: Set of rule_ids that were explicitly requested by the user

    Note:
        Signals are only emitted if the rule_id is in explicitly_requested.
        This allows dependency rules to run without polluting the signal output.
    """
    # Validate minimal fields before conversion
    missing = {"type", "time"} - set(meta.keys())
    if missing:
        logger.warning(
            "Result missing required keys %s | rule=%s%s | meta=%r",
            missing,
            rule_name,
            f" | payload_idx={payload_idx}" if payload_idx is not None else "",
            meta
        )
        return False

    # Ensure symbol is set
    meta_with_symbol = meta
    if "symbol" not in meta and "symbol" in context:
        meta_with_symbol = dict(meta)
        meta_with_symbol.setdefault("symbol", context["symbol"])

    if validate_only:
        logger.debug("VALIDATE-ONLY: would emit %r", meta_with_symbol)
        return True

    # Filter signals: only emit from explicitly requested rules
    if rule_id and rule_id not in explicitly_requested:
        logger.debug(
            "Skipping signal from dependency rule | rule=%s | rule_id=%s | signal=%r",
            rule_name, rule_id, meta_with_symbol.get("type")
        )
        return True  # Still return True (valid signal, just filtered)

    try:
        sig = _metadata_to_signal(meta_with_symbol)
        signals.append(sig)
        return True
    except Exception:
        logger.exception(
            "Failed to convert metadata to BaseSignal | rule=%s | meta=%r",
            rule_name, meta_with_symbol
        )
        return False

def run_indicator_rules(
    indicator: Union[str, Any],
    market_df: "DataFrame",
    **config: Any,
) -> List[BaseSignal]:
    indicator_type = _normalise_indicator_type(indicator)
    raise RuntimeError(
        "legacy_signal_batch_path_disabled: runtime snapshot per-bar signal emission is the only supported path "
        f"(indicator_type={indicator_type})"
    )


def build_signal_overlays(
    indicator: Union[str, Any],
    signals: Sequence[BaseSignal],
    plot_df: "DataFrame",
    **kwargs: Any,
) -> List[Mapping[str, Any]]:
    indicator_type = _normalise_indicator_type(indicator)
    if not get_overlay_spec(indicator_type):
        raise ValueError(
            f"overlay spec missing for type '{indicator_type}'. Register with overlay_type/register_overlay_type."
        )
    registration = _registration_from_plugin(indicator_type)
    if registration is None:
        logger.warning("signal_plugin_missing_for_overlay | indicator_type=%s", indicator_type)
        raise ValueError(f"No rules registered for indicator '{indicator_type}'")

    _, adapter = registration
    if adapter is None:
        logger.error("signal_overlay_adapter_missing | indicator_type=%s", indicator_type)
        raise ValueError(f"No overlay adapter registered for indicator '{indicator_type}'")

    logger.debug(
        "Building overlays | indicator=%s | signals=%d | plot_df=%s | kwargs=%s",
        indicator_type, len(signals), _df_summary(plot_df), list(kwargs.keys())
    )
    try:
        raw_overlays = list(adapter(signals, plot_df, **kwargs))
    except Exception:
        logger.exception("Overlay adapter error | indicator=%s", indicator_type)
        return []

    overlays = normalize_overlays(indicator_type, raw_overlays)
    if overlays:
        logger.debug(
            "Built %d overlay artefact(s) for indicator '%s'",
            len(overlays), indicator_type
        )
        return overlays

    logger.debug(
        "signal_overlay_adapter_empty | indicator_type=%s signals=%d",
        indicator_type,
        len(signals),
    )
    return []



def describe_indicator_rules(indicator_type: str) -> List[Mapping[str, Any]]:
    raise RuntimeError(
        "legacy_signal_rule_catalog_disabled: runtime snapshot per-bar signal emission is the only supported path "
        f"(indicator_type={indicator_type})"
    )


__all__ = [
    "run_indicator_rules",
    "build_signal_overlays",
    "describe_indicator_rules",
]
