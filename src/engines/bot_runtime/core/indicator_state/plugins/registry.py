"""Core indicator plugin registry primitives."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from engines.bot_runtime.core.domain import Candle

from ..contracts import IndicatorStateEngine
from ..overlay_projection import OverlayEntryProjector

EngineFactory = Callable[[Mapping[str, Any]], IndicatorStateEngine]
SignalEmitter = Callable[[Mapping[str, Any], Candle, Candle | None], Mapping[str, Any]]
SignalRule = Callable[[Mapping[str, Any], Any], Any]
SignalOverlayAdapter = Callable[[Sequence[Any], Any], Sequence[Mapping[str, Any]]]


@dataclass(frozen=True)
class IndicatorPluginManifest:
    indicator_type: str
    engine_factory: EngineFactory
    evaluation_mode: str  # session | rolling
    signal_emitter: Optional[SignalEmitter] = None
    overlay_projector: Optional[OverlayEntryProjector] = None
    signal_rules: Sequence[SignalRule] = ()
    signal_overlay_adapter: Optional[SignalOverlayAdapter] = None


class IndicatorPluginRegistry:
    def __init__(self) -> None:
        self._plugins: Dict[str, IndicatorPluginManifest] = {}
        self._pending_signal_rules: Dict[str, Sequence[SignalRule]] = {}
        self._pending_signal_overlay_adapters: Dict[str, SignalOverlayAdapter] = {}

    def register(self, manifest: IndicatorPluginManifest) -> None:
        key = str(manifest.indicator_type or "").strip().lower()
        if not key:
            raise RuntimeError("indicator_plugin_register_failed: indicator_type is required")
        if manifest.engine_factory is None:
            raise RuntimeError(f"indicator_plugin_register_failed: engine is required | indicator_type={key}")
        mode = str(manifest.evaluation_mode or "").strip().lower()
        if mode not in {"session", "rolling"}:
            raise RuntimeError(
                f"indicator_plugin_register_failed: evaluation_mode must be 'session' or 'rolling' | indicator_type={key}"
            )
        merged_rules = tuple(manifest.signal_rules or self._pending_signal_rules.pop(key, ()))
        merged_overlay = manifest.signal_overlay_adapter or self._pending_signal_overlay_adapters.pop(key, None)
        self._plugins[key] = replace(
            manifest,
            indicator_type=key,
            signal_rules=merged_rules,
            signal_overlay_adapter=merged_overlay,
        )

    def resolve(self, indicator_type: str) -> IndicatorPluginManifest:
        key = str(indicator_type or "").strip().lower()
        manifest = self._plugins.get(key)
        if manifest is None:
            raise RuntimeError(f"indicator_plugin_missing: indicator_type={key}")
        return manifest

    def list_types(self) -> list[str]:
        return sorted(self._plugins.keys())


    def get_signal_rules(self, indicator_type: str) -> tuple[SignalRule, ...]:
        key = str(indicator_type or "").strip().lower()
        manifest = self._plugins.get(key)
        if manifest is None:
            return ()
        return tuple(manifest.signal_rules or ())

    def get_signal_overlay_adapter(self, indicator_type: str) -> Optional[SignalOverlayAdapter]:
        key = str(indicator_type or "").strip().lower()
        manifest = self._plugins.get(key)
        if manifest is None:
            return None
        return manifest.signal_overlay_adapter

    def register_signal_components(
        self,
        *,
        indicator_type: str,
        signal_rules: Optional[Sequence[SignalRule]] = None,
        signal_overlay_adapter: Optional[SignalOverlayAdapter] = None,
    ) -> None:
        key = str(indicator_type or "").strip().lower()
        if not key:
            raise RuntimeError("indicator_signal_components_register_failed: indicator_type is required")
        if signal_rules is not None and any(not callable(rule) for rule in signal_rules):
            raise RuntimeError(f"indicator_signal_components_register_failed: non-callable signal rule | indicator_type={key}")
        if signal_overlay_adapter is not None and not callable(signal_overlay_adapter):
            raise RuntimeError(f"indicator_signal_components_register_failed: non-callable overlay adapter | indicator_type={key}")

        existing = self._plugins.get(key)
        if existing is None:
            if signal_rules is not None:
                self._pending_signal_rules[key] = tuple(signal_rules)
            if signal_overlay_adapter is not None:
                self._pending_signal_overlay_adapters[key] = signal_overlay_adapter
            return

        rules = tuple(signal_rules) if signal_rules is not None else tuple(existing.signal_rules)
        overlay = signal_overlay_adapter or existing.signal_overlay_adapter
        self._plugins[key] = replace(existing, signal_rules=rules, signal_overlay_adapter=overlay)


_registry = IndicatorPluginRegistry()


def indicator_plugin_manifest(
    *,
    indicator_type: str,
    engine_factory: EngineFactory,
    evaluation_mode: str,
    signal_emitter: Optional[SignalEmitter] = None,
    overlay_projector: Optional[OverlayEntryProjector] = None,
    signal_rules: Optional[Sequence[SignalRule]] = None,
    signal_overlay_adapter: Optional[SignalOverlayAdapter] = None,
) -> Callable[[object], object]:
    """Single decorator for runtime plugin + signal/overlay registration."""

    def decorator(obj: object) -> object:
        _registry.register(
            IndicatorPluginManifest(
                indicator_type=indicator_type,
                engine_factory=engine_factory,
                evaluation_mode=evaluation_mode,
                signal_emitter=signal_emitter,
                overlay_projector=overlay_projector,
                signal_rules=tuple(signal_rules or ()),
                signal_overlay_adapter=signal_overlay_adapter,
            )
        )
        return obj

    return decorator


def plugin_registry() -> IndicatorPluginRegistry:
    return _registry
