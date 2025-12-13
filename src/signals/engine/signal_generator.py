"""Signal generation orchestrator with registry-based rule dispatch."""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple, Union

from signals.base import BaseSignal
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


@dataclass
class _DecoratedRegistration:
    """Mutable container for decorator-driven registrations."""

    indicator_type: str
    rules: List[RuleCallable]
    overlay_adapter: Optional[OverlayAdapter] = None
    registered: bool = False


@dataclass(frozen=True)
class IndicatorRegistration:
    """Container describing how to process rules for an indicator."""

    rules: Sequence[RuleCallable]
    overlay_adapter: Optional[OverlayAdapter] = None


_REGISTRY: MutableMapping[str, IndicatorRegistration] = {}
_DECORATED: MutableMapping[str, _DecoratedRegistration] = {}
_RESERVED_CONFIG_KEYS = {"rule_payloads", "enabled_rules"}
_TRACE_CONFIG_KEYS = {"trace", "log_context", "validate_only"}

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
    """Register ordered rule callables for an indicator type."""

    if not indicator_type:
        raise ValueError("indicator_type must be provided for registration")

    existing = _REGISTRY.get(indicator_type)
    if existing is not None:
        # Allow updating with a superset of rules (for decorator accumulation)
        existing_rules_set = set(existing.rules)
        new_rules_set = set(rules)

        # If new rules is a superset or equal, allow the update
        if not existing_rules_set.issubset(new_rules_set):
            # New rules is missing some existing rules - this is an error
            raise ValueError(f"Rules for indicator '{indicator_type}' are already registered with different rules")

        # Update if we have new rules or a new overlay adapter
        if existing_rules_set != new_rules_set or (existing.overlay_adapter is None and overlay_adapter is not None):
            _REGISTRY[indicator_type] = IndicatorRegistration(
                rules=tuple(rules),
                overlay_adapter=overlay_adapter or existing.overlay_adapter,
            )
            logger.info(
                "✓ Registered rules for '%s' | rules=%s | registry_keys=%s",
                indicator_type,
                [getattr(r, "__name__", repr(r))[:50] for r in rules],
                sorted(_REGISTRY.keys())
            )
        return

    normalized_rules = tuple(rules or ())
    if not normalized_rules:
        raise ValueError("At least one rule callable must be provided")

    for idx, rule in enumerate(normalized_rules):
        if not callable(rule):
            raise TypeError(f"Rule at position {idx} for '{indicator_type}' is not callable")

    _REGISTRY[indicator_type] = IndicatorRegistration(
        rules=normalized_rules,
        overlay_adapter=overlay_adapter,
    )
    logger.info(
        "✓ Registered rules for '%s' | rules=%s | registry_keys=%s",
        indicator_type,
        [getattr(r, "__name__", repr(r))[:50] for r in normalized_rules],
        sorted(_REGISTRY.keys())
    )


def _normalise_indicator_type(indicator: Union[str, Any]) -> str:
    if isinstance(indicator, str):
        return indicator
    if isinstance(indicator, type):
        return getattr(indicator, "NAME", indicator.__name__)
    return getattr(indicator, "NAME", indicator.__class__.__name__)


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

    try:
        register_indicator_rules(
            registration.indicator_type,
            tuple(registration.rules),
            overlay_adapter=registration.overlay_adapter,
        )
        registration.registered = True
    except ValueError:
        existing = _REGISTRY.get(registration.indicator_type)
        if existing is None:
            raise
        if tuple(existing.rules) != tuple(registration.rules):
            raise
        if existing.overlay_adapter is None and registration.overlay_adapter is not None:
            _REGISTRY[registration.indicator_type] = IndicatorRegistration(
                rules=existing.rules,
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
) -> Callable[[RuleCallable], RuleCallable]:
    """Decorator to attach metadata and register a signal rule for an indicator."""

    def decorator(func: RuleCallable) -> RuleCallable:
        if rule_id:
            setattr(func, "signal_id", rule_id)
        if label:
            setattr(func, "signal_label", label)
        if description:
            setattr(func, "signal_description", description)

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

        existing = _REGISTRY.get(registration.indicator_type)
        if existing is not None and existing.overlay_adapter is None:
            _REGISTRY[registration.indicator_type] = IndicatorRegistration(
                rules=existing.rules,
                overlay_adapter=func,
            )

        _attempt_autoregistration(registration)
        return func

    return decorator


def _filter_enabled_rules(
    rules: Sequence[RuleCallable],
    enabled_rules: Optional[Iterable[Any]],
    indicator_type: str,
) -> Sequence[RuleCallable]:
    if not enabled_rules:
        return rules

    desired: Set[str] = {str(rule_id).lower() for rule_id in enabled_rules if rule_id is not None}
    if not desired:
        return rules

    filtered: List[RuleCallable] = []
    for rule in rules:
        identifiers = {ident.lower() for ident in _rule_identifiers(rule)}
        if identifiers & desired:
            filtered.append(rule)

    if not filtered:
        logger.warning(
            "No matching enabled rules for indicator '%s'. Requested=%s available=%s",
            indicator_type,
            sorted(desired),
            [tuple(_rule_identifiers(rule))[0] for rule in rules],
        )
        return rules

    return tuple(filtered)


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
    if indicator_type == "market_profile" and "market_profile" not in context:
        from indicators.market_profile import MarketProfileIndicator

        if isinstance(indicator, MarketProfileIndicator):
            context["market_profile"] = indicator
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

def run_indicator_rules(
    indicator: Union[str, Any],
    market_df: "DataFrame",
    **config: Any,
) -> List[BaseSignal]:
    indicator_type = _normalise_indicator_type(indicator)
    registration = _REGISTRY.get(indicator_type)
    if registration is None:
        # Extra hint when the name doesn't match
        logger.warning(
            "No rules found for indicator '%s'. Registered types: %s",
            indicator_type, list(_REGISTRY.keys())
        )
        raise ValueError(f"No rules registered for indicator '{indicator_type}'")

    context = _build_context(indicator, indicator_type, market_df, config)
    payloads = _resolve_payloads(config)

    if indicator_type == "market_profile":
        from signals.rules.market_profile._bootstrap import ensure_breakouts_ready

        try:
            ensure_breakouts_ready(context, payloads)
        except Exception:
            logger.exception("Failed to initialise market profile breakouts")

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
    active_rules = _filter_enabled_rules(registration.rules, enabled_rules, indicator_type)

    signals: List[BaseSignal] = []
    total_rules = len(active_rules)
    for r_idx, rule in enumerate(active_rules):
        rule_name = getattr(rule, "__name__", repr(rule))
        t_rule_start = time.perf_counter()
        logger.debug("Rule[%d/%d] %s -> payloads=%d", r_idx+1, total_rules, rule_name, len(payloads))

        rule_emitted = 0
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

            if not results:
                continue

            for meta_idx, meta in enumerate(results):
                # Validate minimal fields before conversion
                missing = {"type", "time"} - set(meta.keys())
                if missing:
                    logger.warning(
                        "Result missing required keys %s | rule=%s | payload_idx=%d | meta=%r",
                        missing, rule_name, p_idx, meta
                    )
                    continue

                # Ensure symbol is set
                if "symbol" not in meta and "symbol" in context:
                    meta = dict(meta)
                    meta.setdefault("symbol", context["symbol"])

                if validate_only:
                    logger.debug("VALIDATE-ONLY: would emit %r", meta)
                    continue

                try:
                    sig = _metadata_to_signal(meta)
                except Exception:
                    logger.exception(
                        "Failed to convert metadata to BaseSignal | rule=%s | meta_idx=%d | meta=%r",
                        rule_name, meta_idx, meta
                    )
                    continue

                signals.append(sig)
                rule_emitted += 1

        logger.debug(
            "Rule complete | rule=%s | emitted_so_far=%d | rule_time_ms=%d",
            rule_name, len(signals), int((time.perf_counter() - t_rule_start) * 1000)
        )

    if indicator_type == "market_profile":
        type_counts = Counter(sig.type for sig in signals if getattr(sig, "type", None))
        logger.info(
            "Market profile signal summary | total=%d | breakouts=%d | retests=%d",
            len(signals),
            type_counts.get("breakout", 0),
            type_counts.get("retest", 0),
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
    registration = _REGISTRY.get(indicator_type)
    if registration is None:
        logger.warning(
            "No rules registered for indicator '%s' (overlay build requested). Registered: %s",
            indicator_type, list(_REGISTRY.keys())
        )
        raise ValueError(f"No rules registered for indicator '{indicator_type}'")

    adapter = registration.overlay_adapter
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

    registration = _REGISTRY.get(indicator_type)
    if registration is None:
        return []

    descriptions: List[Mapping[str, Any]] = []
    for rule in registration.rules:
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
    "indicator",
    "signal_rule",
    "overlay_adapter",
    "register_indicator_rules",
    "run_indicator_rules",
    "build_signal_overlays",
    "describe_indicator_rules",
]
