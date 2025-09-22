"""Signal generation orchestrator with registry-based rule dispatch."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Union

from signals.base import BaseSignal

try:  # pragma: no cover - optional import for type checking only
    from pandas import DataFrame  # type: ignore
except Exception:  # pragma: no cover
    DataFrame = Any  # fallback for environments without pandas


logger = logging.getLogger(__name__)

RuleCallable = Callable[[Mapping[str, Any], Any], Optional[Sequence[Mapping[str, Any]]]]
OverlayAdapter = Callable[[Sequence[BaseSignal], "DataFrame"], Sequence[Mapping[str, Any]]]


@dataclass(frozen=True)
class IndicatorRegistration:
    """Container describing how to process rules for an indicator."""

    rules: Sequence[RuleCallable]
    overlay_adapter: Optional[OverlayAdapter] = None


_REGISTRY: MutableMapping[str, IndicatorRegistration] = {}
_RESERVED_CONFIG_KEYS = {"rule_payloads"}


def register_indicator_rules(
    indicator_type: str,
    rules: Sequence[RuleCallable],
    overlay_adapter: Optional[OverlayAdapter] = None,
) -> None:
    """Register ordered rule callables for an indicator type."""

    if not indicator_type:
        raise ValueError("indicator_type must be provided for registration")

    if indicator_type in _REGISTRY:
        raise ValueError(f"Rules for indicator '{indicator_type}' are already registered")

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
    logger.debug("Registered %d rule(s) for indicator '%s'", len(normalized_rules), indicator_type)


def _normalise_indicator_type(indicator: Union[str, Any]) -> str:
    if isinstance(indicator, str):
        return indicator
    return getattr(indicator, "NAME", indicator.__class__.__name__)


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
    """Execute registered rules for an indicator and emit :class:`BaseSignal` objects."""

    indicator_type = _normalise_indicator_type(indicator)
    registration = _REGISTRY.get(indicator_type)
    if registration is None:
        raise ValueError(f"No rules registered for indicator '{indicator_type}'")

    context = _build_context(indicator, indicator_type, market_df, config)
    payloads = _resolve_payloads(config)

    signals: List[BaseSignal] = []
    for rule in registration.rules:
        for payload in payloads:
            results = rule(context, payload)
            if not results:
                continue
            for meta in results:
                if "symbol" not in meta and "symbol" in context:
                    meta = dict(meta)
                    meta.setdefault("symbol", context["symbol"])
                signals.append(_metadata_to_signal(meta))
    logger.debug(
        "Generated %d signal(s) for indicator '%s' using %d payload(s)",
        len(signals), indicator_type, len(payloads)
    )
    return signals


def build_signal_overlays(
    indicator: Union[str, Any],
    signals: Sequence[BaseSignal],
    plot_df: "DataFrame",
    **kwargs: Any,
) -> List[Mapping[str, Any]]:
    """Build plot overlays for an indicator if an adapter has been registered."""

    indicator_type = _normalise_indicator_type(indicator)
    registration = _REGISTRY.get(indicator_type)
    if registration is None:
        raise ValueError(f"No rules registered for indicator '{indicator_type}'")

    adapter = registration.overlay_adapter
    if adapter is None:
        return []

    overlays = list(adapter(signals, plot_df, **kwargs))
    logger.debug(
        "Built %d overlay artefact(s) for indicator '%s'", len(overlays), indicator_type
    )
    return overlays


__all__ = [
    "register_indicator_rules",
    "run_indicator_rules",
    "build_signal_overlays",
]
