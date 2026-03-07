"""Core indicator plugin registry primitives."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass, replace
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from engines.bot_runtime.core.domain import Candle

from .contracts import IndicatorStateEngine
from .overlay_projection import OverlayEntryProjector

EngineFactory = Callable[[Mapping[str, Any]], IndicatorStateEngine]
SignalEmitter = Callable[[Mapping[str, Any], Candle, Candle | None], Mapping[str, Any]]
SignalOverlayAdapter = Callable[[Sequence[Any], Any], Sequence[Mapping[str, Any]]]


@dataclass(frozen=True)
class SignalDirectionSpec:
    id: str
    label: str
    description: str


@dataclass(frozen=True)
class SignalCatalogEntry:
    id: str
    label: str
    description: str
    signal_type: str
    aliases: Sequence[str] = field(default_factory=tuple)
    directions: Sequence[SignalDirectionSpec] = field(default_factory=tuple)


@dataclass(frozen=True)
class IndicatorPluginManifest:
    indicator_type: str
    engine_factory: EngineFactory
    evaluation_mode: str  # session | rolling
    signal_emitter: Optional[SignalEmitter] = None
    overlay_projector: Optional[OverlayEntryProjector] = None
    signal_overlay_adapter: Optional[SignalOverlayAdapter] = None
    signal_rules: Sequence[SignalCatalogEntry] = field(default_factory=tuple)


class IndicatorPluginRegistry:
    def __init__(self) -> None:
        self._plugins: Dict[str, IndicatorPluginManifest] = {}

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
        normalized_rules = _normalize_signal_rules(
            indicator_type=key,
            rules=manifest.signal_rules,
        )
        self._plugins[key] = replace(
            manifest,
            indicator_type=key,
            signal_rules=normalized_rules,
        )

    def resolve(self, indicator_type: str) -> IndicatorPluginManifest:
        key = str(indicator_type or "").strip().lower()
        manifest = self._plugins.get(key)
        if manifest is None:
            raise RuntimeError(f"indicator_plugin_missing: indicator_type={key}")
        return manifest

    def list_types(self) -> list[str]:
        return sorted(self._plugins.keys())


_registry = IndicatorPluginRegistry()


def _normalize_signal_rules(
    *,
    indicator_type: str,
    rules: Sequence[SignalCatalogEntry],
) -> tuple[SignalCatalogEntry, ...]:
    normalized: list[SignalCatalogEntry] = []
    seen_ids: set[str] = set()
    seen_aliases: set[str] = set()
    for index, entry in enumerate(rules):
        candidate: SignalCatalogEntry
        if isinstance(entry, SignalCatalogEntry):
            candidate = entry
        elif is_dataclass(entry):
            candidate = SignalCatalogEntry(**asdict(entry))
        elif isinstance(entry, Mapping):
            directions_payload = entry.get("directions") or []
            directions: list[SignalDirectionSpec] = []
            for direction_idx, direction in enumerate(directions_payload):
                if isinstance(direction, SignalDirectionSpec):
                    directions.append(direction)
                elif is_dataclass(direction):
                    directions.append(SignalDirectionSpec(**asdict(direction)))
                elif isinstance(direction, Mapping):
                    directions.append(
                        SignalDirectionSpec(
                            id=str(direction.get("id") or "").strip(),
                            label=str(direction.get("label") or "").strip(),
                            description=str(direction.get("description") or "").strip(),
                        )
                    )
                else:
                    raise RuntimeError(
                        "indicator_plugin_register_failed: signal_rules direction invalid "
                        f"| indicator_type={indicator_type} signal_index={index} direction_index={direction_idx}"
                    )
            candidate = SignalCatalogEntry(
                id=str(entry.get("id") or "").strip(),
                label=str(entry.get("label") or "").strip(),
                description=str(entry.get("description") or "").strip(),
                signal_type=str(entry.get("signal_type") or "").strip(),
                aliases=tuple(
                    str(alias).strip()
                    for alias in (entry.get("aliases") or ())
                    if str(alias).strip()
                ),
                directions=tuple(directions),
            )
        else:
            raise RuntimeError(
                "indicator_plugin_register_failed: signal_rules entry invalid "
                f"| indicator_type={indicator_type} signal_index={index}"
            )

        rule_id = str(candidate.id or "").strip().lower()
        if not rule_id:
            raise RuntimeError(
                "indicator_plugin_register_failed: signal_rules id is required "
                f"| indicator_type={indicator_type} signal_index={index}"
            )
        if rule_id in seen_ids:
            raise RuntimeError(
                "indicator_plugin_register_failed: signal_rules id must be unique "
                f"| indicator_type={indicator_type} signal_id={rule_id}"
            )
        seen_ids.add(rule_id)
        signal_type = str(candidate.signal_type or "").strip().lower()
        if not signal_type:
            raise RuntimeError(
                "indicator_plugin_register_failed: signal_rules signal_type is required "
                f"| indicator_type={indicator_type} signal_id={rule_id}"
            )
        aliases = tuple(
            str(alias or "").strip().lower()
            for alias in (candidate.aliases or ())
            if str(alias or "").strip()
        )
        deduped_aliases: list[str] = []
        for alias in aliases:
            if alias == rule_id:
                continue
            if alias in seen_ids or alias in seen_aliases:
                raise RuntimeError(
                    "indicator_plugin_register_failed: signal_rules alias must be unique "
                    f"| indicator_type={indicator_type} signal_id={rule_id} alias={alias}"
                )
            seen_aliases.add(alias)
            deduped_aliases.append(alias)
        directions = tuple(
            SignalDirectionSpec(
                id=str(direction.id or "").strip().lower(),
                label=str(direction.label or "").strip(),
                description=str(direction.description or "").strip(),
            )
            for direction in (candidate.directions or ())
            if str(direction.id or "").strip()
        )
        normalized.append(
            SignalCatalogEntry(
                id=rule_id,
                label=str(candidate.label or "").strip() or rule_id,
                description=str(candidate.description or "").strip(),
                signal_type=signal_type,
                aliases=tuple(deduped_aliases),
                directions=directions,
            )
        )
    return tuple(normalized)


def register_plugin(manifest: IndicatorPluginManifest) -> None:
    _registry.register(manifest)


def plugin_registry() -> IndicatorPluginRegistry:
    return _registry
