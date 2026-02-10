"""Signal generation orchestrator with registry-based rule dispatch."""

from __future__ import annotations

import logging
from collections import Counter
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple, Union

from engines.bot_runtime.core.indicator_state.contracts import IndicatorStateSnapshot
from engines.bot_runtime.core.indicator_state.plugins import (
    IndicatorPluginManifest,
    ensure_builtin_indicator_plugins_registered,
    plugin_registry,
)

from signals.base import BaseSignal
from signals.overlays.registry import get_overlay_spec
from signals.overlays.schema import build_overlay, normalize_overlays
from signals.rules.common.utils import bias_label_from_direction, clean_numeric, to_epoch_seconds


import time
from pprint import pformat

try:  # pragma: no cover - optional import for type checking only
    from pandas import DataFrame  # type: ignore
except Exception:  # pragma: no cover
    DataFrame = Any  # fallback for environments without pandas


logger = logging.getLogger(__name__)

RuleCallable = Callable[[Mapping[str, Any], Any], Optional[Sequence[Mapping[str, Any]]]]
OverlayAdapter = Callable[[Sequence[BaseSignal], "DataFrame"], Sequence[Mapping[str, Any]]]


class RulePhase(Enum):
    """Execution phase for signal rules.

    Rules are executed in phase order:
    1. BOOTSTRAP: Run once with all payloads, typically to populate caches
    2. PER_PAYLOAD: Run once per payload (default behavior)
    3. AGGREGATION: Run once with all payloads, typically to consume caches
    """
    BOOTSTRAP = "bootstrap"
    PER_PAYLOAD = "per-payload"
    AGGREGATION = "aggregation"


class _DecoratedRegistration:
    """Mutable container for decorator-driven registrations."""

    def __init__(self, indicator_type: str, rules: Optional[List[RuleCallable]] = None, overlay_adapter: Optional[OverlayAdapter] = None, registered: bool = False) -> None:
        self.indicator_type = indicator_type
        self.rules = list(rules or [])
        self.overlay_adapter = overlay_adapter
        self.registered = registered


_DECORATED: MutableMapping[str, _DecoratedRegistration] = {}
_RESERVED_CONFIG_KEYS = {"rule_payloads", "enabled_rules"}
_TRACE_CONFIG_KEYS = {"trace", "log_context", "validate_only"}


def _registration_from_plugin(indicator_type: str) -> Optional[Tuple[Sequence[RuleCallable], Optional[OverlayAdapter]]]:
    key = str(indicator_type or "").strip().lower()
    ensure_builtin_indicator_plugins_registered()
    try:
        manifest = plugin_registry().resolve(key)
    except RuntimeError:
        return None
    rules = tuple(manifest.signal_rules or ())
    if not rules:
        return None
    return rules, manifest.signal_overlay_adapter



class _SignalOnlyStateEngine:
    """Signal-only manifest engine for non-runtime signal generation paths."""

    def initialize(self, window_context: Mapping[str, Any]) -> Dict[str, Any]:
        return {"revision": 0, "payload": {}}

    def apply_bar(self, state: Mapping[str, Any], bar: Any) -> tuple[Mapping[str, Any], int]:
        return {"changed": False}, int(state.get("revision", 0))

    def snapshot(self, state: Mapping[str, Any]) -> IndicatorStateSnapshot:
        return IndicatorStateSnapshot(
            revision=int(state.get("revision", 0)),
            known_at=0,
            formed_at=0,
            source_timeframe="signal-only",
            payload=dict(state.get("payload") or {}),
        )


def _ensure_signal_manifest(indicator_type: str) -> None:
    key = str(indicator_type or "").strip().lower()
    try:
        plugin_registry().resolve(key)
        return
    except RuntimeError:
        pass
    plugin_registry().register(
        IndicatorPluginManifest(
            indicator_type=key,
            engine_factory=lambda _meta: _SignalOnlyStateEngine(),
            evaluation_mode="rolling",
        )
    )
    logger.warning("signal_manifest_auto_registered | indicator_type=%s", key)


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

def register_indicator_rules(
    indicator_type: str,
    rules: Sequence[RuleCallable],
    overlay_adapter: Optional[OverlayAdapter] = None,
) -> None:
    """Register ordered rule callables for an indicator type via core plugin registry."""

    key = str(indicator_type or "").strip().lower()
    if not key:
        raise ValueError("indicator_type must be provided for registration")

    normalized_rules = tuple(rules or ())
    if not normalized_rules:
        raise ValueError("At least one rule callable must be provided")

    for idx, rule in enumerate(normalized_rules):
        if not callable(rule):
            raise TypeError(f"Rule at position {idx} for '{key}' is not callable")

    _ensure_signal_manifest(key)
    existing = _registration_from_plugin(key)
    if existing is not None:
        existing_rules, existing_overlay = existing
        existing_rules_set = set(existing_rules)
        new_rules_set = set(normalized_rules)
        if not existing_rules_set.issubset(new_rules_set):
            raise ValueError(f"Rules for indicator '{key}' are already registered with different rules")
        if existing_rules_set == new_rules_set and (existing_overlay is not None or overlay_adapter is None):
            return

    plugin_registry().register_signal_components(
        indicator_type=key,
        signal_rules=normalized_rules,
        signal_overlay_adapter=overlay_adapter,
    )
    logger.info(
        "indicator_signal_components_registered | indicator_type=%s | rules=%s",
        key,
        [getattr(r, "__name__", repr(r))[:50] for r in normalized_rules],
    )


def _normalise_indicator_type(indicator: Union[str, Any]) -> str:
    if isinstance(indicator, str):
        return str(indicator).strip().lower()
    if isinstance(indicator, type):
        return str(getattr(indicator, "NAME", indicator.__name__)).strip().lower()
    return str(getattr(indicator, "NAME", indicator.__class__.__name__)).strip().lower()


def _get_decorated_registration(indicator: Union[str, Any]) -> _DecoratedRegistration:
    indicator_type = _normalise_indicator_type(indicator)
    registration = _DECORATED.get(indicator_type)
    if registration is None:
        registration = _DecoratedRegistration(indicator_type=indicator_type, rules=[])
        _DECORATED[indicator_type] = registration
    return registration


def _attempt_autoregistration(registration: _DecoratedRegistration) -> None:
    if registration.registered or not registration.rules:
        return

    register_indicator_rules(
        registration.indicator_type,
        tuple(registration.rules),
        overlay_adapter=registration.overlay_adapter,
    )
    registration.registered = True


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


def indicator(indicator_type: Optional[Union[str, Any]] = None) -> Callable[[Any], Any]:
    """Decorator to mark an indicator type for declarative rule registration."""

    def decorator(obj: Any) -> Any:
        _get_decorated_registration(indicator_type or obj)
        return obj

    return decorator


def signal_rule(
    indicator: Union[str, Any],
    *,
    rule_id: Optional[str] = None,
    label: Optional[str] = None,
    description: Optional[str] = None,
    phase: RulePhase = RulePhase.PER_PAYLOAD,
    depends_on: Optional[List[str]] = None,
) -> Callable[[RuleCallable], RuleCallable]:
    """Decorator to attach metadata and register a signal rule for an indicator.

    Args:
        indicator: Indicator type this rule applies to
        rule_id: Unique identifier for the rule
        label: Human-readable label
        description: Description of what the rule detects
        phase: Execution phase (BOOTSTRAP, PER_PAYLOAD, or AGGREGATION)
        depends_on: List of rule_ids this rule depends on (must run after)
    """

    def decorator(func: RuleCallable) -> RuleCallable:
        if rule_id:
            setattr(func, "signal_id", rule_id)
        if label:
            setattr(func, "signal_label", label)
        if description:
            setattr(func, "signal_description", description)

        # Store phase and dependency metadata
        setattr(func, "_rule_phase", phase)
        setattr(func, "_rule_depends_on", depends_on or [])

        registration = _get_decorated_registration(indicator)
        registration.rules.append(func)
        # Reset registered flag to trigger re-registration with updated rules
        registration.registered = False
        _attempt_autoregistration(registration)
        return func

    return decorator


def overlay_adapter(indicator: Union[str, Any]) -> Callable[[OverlayAdapter], OverlayAdapter]:
    """Decorator to register an overlay adapter alongside an indicator's rules."""

    def decorator(func: OverlayAdapter) -> OverlayAdapter:
        registration = _get_decorated_registration(indicator)
        registration.overlay_adapter = func

        key = registration.indicator_type
        if registration.rules:
            register_indicator_rules(
                key,
                tuple(registration.rules),
                overlay_adapter=func,
            )
            registration.registered = True
        else:
            _ensure_signal_manifest(key)
            plugin_registry().register_signal_components(
                indicator_type=key,
                signal_overlay_adapter=func,
            )
        
        _attempt_autoregistration(registration)
        return func

    return decorator



def indicator_plugin(
    *,
    rules: Optional[Sequence[RuleCallable]] = None,
    overlay: Optional[OverlayAdapter] = None,
) -> Callable[[Any], Any]:
    """Single decorator for indicator signal/overlay registration.

    This is the canonical plugin entrypoint for new indicators: pass rule callables
    and optional overlay adapter once, instead of stacking multiple decorators.
    """

    def decorator(indicator_obj: Any) -> Any:
        registration = _get_decorated_registration(indicator_obj)
        if rules:
            for rule in rules:
                if not callable(rule):
                    raise TypeError("indicator_plugin rules must be callables")
                registration.rules.append(rule)
        if overlay is not None:
            if not callable(overlay):
                raise TypeError("indicator_plugin overlay must be callable")
            registration.overlay_adapter = overlay
        registration.registered = False
        _attempt_autoregistration(registration)
        return indicator_obj

    return decorator

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
    registration = _registration_from_plugin(indicator_type)
    if registration is None:
        logger.warning("signal_rules_missing | indicator_type=%s", indicator_type)
        raise ValueError(f"No rules registered for indicator '{indicator_type}'")
    active_rules_for_indicator, _ = registration

    context = _build_context(indicator, indicator_type, market_df, config)
    payloads = _resolve_payloads(config)

    logger.info(
        "Signal run triggered | indicator=%s | payloads=%d",
        indicator_type,
        len(payloads),
    )

    trace = bool(config.get("trace") or config.get("log_context"))
    validate_only = bool(config.get("validate_only"))

    if trace:
        logger.debug(
            "Signal run start | indicator=%s | df=%s | payload_count=%d",
            indicator_type, _df_summary(market_df), len(payloads)
        )
        # Safe context preview (without df)
        ctx_preview = {k: v for k, v in context.items() if k not in {"df"}}
        logger.debug("Context keys=%s", sorted(ctx_preview.keys()))
        logger.debug("Context preview=\n%s", pformat(ctx_preview))

    enabled_rules = config.get("enabled_rules")
    active_rules, explicitly_requested = _filter_enabled_rules(active_rules_for_indicator, enabled_rules, indicator_type)

    # Group rules by execution phase
    bootstrap_rules = [r for r in active_rules if getattr(r, "_rule_phase", RulePhase.PER_PAYLOAD) == RulePhase.BOOTSTRAP]
    per_payload_rules = [r for r in active_rules if getattr(r, "_rule_phase", RulePhase.PER_PAYLOAD) == RulePhase.PER_PAYLOAD]
    aggregation_rules = [r for r in active_rules if getattr(r, "_rule_phase", RulePhase.PER_PAYLOAD) == RulePhase.AGGREGATION]

    signals: List[BaseSignal] = []

    # Phase 1: BOOTSTRAP - Run once with all payloads (populate caches)
    if bootstrap_rules:
        logger.debug("Phase 1: BOOTSTRAP | rules=%d", len(bootstrap_rules))
        for rule in bootstrap_rules:
            rule_name = getattr(rule, "__name__", repr(rule))
            rule_id = getattr(rule, "signal_id", None)
            t_start = time.perf_counter()
            signals_before = len(signals)
            try:
                results = rule(context, payloads)  # Pass ALL payloads
                if results:
                    for meta in results:
                        if _validate_and_process_signal(meta, rule_name, rule_id, context, validate_only, signals, explicitly_requested):
                            pass  # Signal added to list
            except Exception:
                logger.exception("Bootstrap rule error | rule=%s", rule_name)
            signals_added = len(signals) - signals_before
            logger.debug(
                "Bootstrap rule complete | rule=%s | emitted=%d | time_ms=%d",
                rule_name, signals_added, int((time.perf_counter() - t_start) * 1000)
            )

    # Phase 2: PER_PAYLOAD - Run once per payload (standard behavior)
    if per_payload_rules:
        logger.debug("Phase 2: PER_PAYLOAD | rules=%d | payloads=%d", len(per_payload_rules), len(payloads))
        for rule in per_payload_rules:
            rule_name = getattr(rule, "__name__", repr(rule))
            rule_id = getattr(rule, "signal_id", None)
            t_rule_start = time.perf_counter()

            for p_idx, payload in enumerate(payloads):
                t_payload_start = time.perf_counter()
                try:
                    results = rule(context, payload)
                except Exception:
                    logger.exception(
                        "Rule error | rule=%s | payload_idx=%d | payload=%r",
                        rule_name, p_idx, payload
                    )
                    continue

                took_ms = int((time.perf_counter() - t_payload_start) * 1000)
                count = 0 if not results else len(results)
                logger.debug(
                    "Rule payload done | rule=%s | payload_idx=%d | results=%d | %dms",
                    rule_name, p_idx, count, took_ms
                )

                if results:
                    for meta in results:
                        _validate_and_process_signal(meta, rule_name, rule_id, context, validate_only, signals, explicitly_requested, p_idx)

            logger.debug(
                "Rule complete | rule=%s | emitted_so_far=%d | rule_time_ms=%d",
                rule_name, len(signals), int((time.perf_counter() - t_rule_start) * 1000)
            )

    # Phase 3: AGGREGATION - Run once with all payloads (consume caches)
    if aggregation_rules:
        logger.debug("Phase 3: AGGREGATION | rules=%d", len(aggregation_rules))
        for rule in aggregation_rules:
            rule_name = getattr(rule, "__name__", repr(rule))
            rule_id = getattr(rule, "signal_id", None)
            t_start = time.perf_counter()
            signals_before = len(signals)
            try:
                results = rule(context, payloads)  # Pass ALL payloads
                if results:
                    for meta in results:
                        if _validate_and_process_signal(meta, rule_name, rule_id, context, validate_only, signals, explicitly_requested):
                            pass  # Signal added to list
            except Exception:
                logger.exception("Aggregation rule error | rule=%s", rule_name)
            signals_added = len(signals) - signals_before
            logger.debug(
                "Aggregation rule complete | rule=%s | emitted=%d | time_ms=%d",
                rule_name, signals_added, int((time.perf_counter() - t_start) * 1000)
            )

    type_counts = Counter(sig.type for sig in signals if getattr(sig, "type", None))
    logger.info(
        "Signal type summary | indicator=%s | total=%d | counts=%s",
        indicator_type,
        len(signals),
        dict(type_counts),
    )

    logger.info(
        "Signal run complete | indicator=%s | total_signals=%d",
        indicator_type,
        len(signals),
    )
    return signals


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
        logger.debug(
            "No overlay adapter registered for '%s' -> returning fallback bubbles", indicator_type
        )
        return _signals_to_bubbles(indicator_type, signals)

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

    fallback_overlays = _signals_to_bubbles(indicator_type, signals)
    if fallback_overlays:
        logger.debug(
            "Overlay adapter returned none; emitted %d fallback bubble overlay(s) for indicator '%s'",
            len(fallback_overlays),
            indicator_type,
        )
    return fallback_overlays


def _signals_to_bubbles(
    indicator_type: str, signals: Sequence[BaseSignal]
) -> List[Mapping[str, Any]]:
    """Fallback bubble overlays when no adapter output is available."""

    bubbles: List[Mapping[str, Any]] = []
    for sig in signals:
        meta = sig.metadata or {}
        marker_time = to_epoch_seconds(getattr(sig, "time", None))
        price = _resolve_signal_price(meta)

        if marker_time is None or price is None:
            continue

        direction = meta.get("pointer_direction") or meta.get("direction")
        label = meta.get("pattern_label") or f"{(sig.type or 'Signal').title()}"
        detail = meta.get("pattern_description") or meta.get("rule_id") or sig.type
        accent = _accent_for_direction(direction)
        bubble_payload: Dict[str, Any] = {
            "time": marker_time,
            "price": float(price),
            "label": label,
            "detail": detail,
            "direction": direction,
            "accentColor": accent,
            "backgroundColor": "rgba(14,165,233,0.2)",
            "subtype": "bubble",
        }

        bias = bias_label_from_direction(direction)
        if bias:
            bubble_payload["bias"] = bias

        bubbles.append(bubble_payload)

    if not bubbles:
        return []

    payload = {"bubbles": bubbles, "markers": [], "price_lines": [], "polylines": []}
    return [build_overlay(indicator_type, payload)]


def _resolve_signal_price(metadata: Mapping[str, Any]) -> Optional[float]:
    """Best-effort extraction of a numeric price for bubble overlays."""

    for key in ("price", "level_price", "trigger_close", "close", "value"):
        price = clean_numeric(metadata.get(key))
        if price is not None:
            return price
    return None


def _accent_for_direction(direction: Optional[str]) -> str:
    hint = (direction or "").lower()
    if hint in {"up", "above", "long", "buy"}:
        return "#22c55e"
    if hint in {"down", "below", "short", "sell"}:
        return "#f43f5e"
    return "#38bdf8"



def describe_indicator_rules(indicator_type: str) -> List[Mapping[str, Any]]:
    """Return friendly metadata about registered rules for an indicator."""

    registration = _registration_from_plugin(indicator_type)
    if registration is None:
        return []

    rules, _ = registration
    descriptions: List[Mapping[str, Any]] = []
    for rule in rules:
        identifiers = _rule_identifiers(rule)
        rule_id = identifiers[0]
        label = getattr(rule, "signal_label", None) or rule_id.replace("_", " ").title()
        description = getattr(rule, "signal_description", None)
        descriptions.append({
            "id": rule_id,
            "label": label,
            "description": description,
        })

    return descriptions


__all__ = [
    "RulePhase",
    "indicator",
    "signal_rule",
    "overlay_adapter",
    "indicator_plugin",
    "register_indicator_rules",
    "run_indicator_rules",
    "build_signal_overlays",
    "describe_indicator_rules",
]
