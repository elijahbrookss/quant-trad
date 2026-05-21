"""Canonical indicator definition registry."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from indicators.definition_contract import validate_indicator_definition
from indicators.manifest import IndicatorManifest, validate_indicator_manifest

from .candle_stats.definition import CandleStatsIndicator
from .market_profile.definition import MarketProfileIndicator
from .pivot_level.definition import PivotLevelIndicatorDefinition
from .regime.definition import RegimeIndicator
from .trendline.definition import TrendlineIndicatorDefinition
from .vwap.definition import VWAPIndicatorDefinition


INDICATOR_MAP: dict[str, Any] = {
    "candle_stats": CandleStatsIndicator,
    "regime": RegimeIndicator,
    "vwap": VWAPIndicatorDefinition,
    "pivot_level": PivotLevelIndicatorDefinition,
    "trendline": TrendlineIndicatorDefinition,
    "market_profile": MarketProfileIndicator,
}


def get_indicator_definition(type_id: str) -> Any:
    indicator_type = str(type_id or "").strip()
    definition = INDICATOR_MAP.get(indicator_type)
    if definition is None:
        raise KeyError(f"Unknown indicator type: {indicator_type}")
    return definition


def get_indicator_manifest(type_id: str) -> IndicatorManifest:
    definition = get_indicator_definition(type_id)
    manifest = getattr(definition, "MANIFEST", None)
    if not isinstance(manifest, IndicatorManifest):
        raise RuntimeError(f"indicator_definition_invalid: manifest missing type={type_id}")
    return manifest


def list_indicator_types() -> list[str]:
    return list(INDICATOR_MAP.keys())


def validate_indicator_registry() -> None:
    for indicator_type, definition in INDICATOR_MAP.items():
        validate_indicator_definition(indicator_type, definition)
        manifest = getattr(definition, "MANIFEST", None)
        validate_indicator_manifest(manifest)
        if manifest.type != indicator_type:
            raise RuntimeError(
                "indicator_definition_invalid: manifest type mismatch "
                f"registry={indicator_type} manifest={manifest.type}"
            )
        definition_module = str(getattr(definition, "__module__", ""))
        if not definition_module.endswith(".definition"):
            raise RuntimeError(
                "indicator_definition_invalid: definitions must live in package definition.py "
                f"type={indicator_type} module={definition_module}"
            )
        module_file = Path(__file__).resolve().parent / indicator_type / "definition.py"
        manifest_file = Path(__file__).resolve().parent / indicator_type / "manifest.py"
        if not module_file.exists() or not manifest_file.exists():
            raise RuntimeError(
                "indicator_definition_invalid: indicator packages must provide definition.py and manifest.py "
                f"type={indicator_type}"
            )


validate_indicator_registry()

__all__ = [
    "INDICATOR_MAP",
    "get_indicator_definition",
    "get_indicator_manifest",
    "list_indicator_types",
    "validate_indicator_registry",
]
