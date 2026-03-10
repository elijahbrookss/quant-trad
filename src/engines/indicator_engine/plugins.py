"""Indicator plugin bootstrap for manifest registration."""

from __future__ import annotations

import importlib
import sys

from .plugin_registry import (
    IndicatorPluginManifest,
    IndicatorPluginRegistry,
    SignalCatalogEntry,
    SignalDirectionSpec,
    plugin_registry,
    register_plugin,
)


_BUILTIN_PLUGIN_MODULES = (
    "indicators.market_profile.plugin",
    "indicators.pivot_level.plugin",
    "indicators.trendline.plugin",
    "indicators.vwap.plugin",
)
_BUILTIN_PLUGIN_TYPES = {"market_profile", "pivot_level", "trendline", "vwap"}


def ensure_builtin_indicator_plugins_registered() -> None:
    registry = plugin_registry()
    missing = _BUILTIN_PLUGIN_TYPES - set(registry.list_types())
    if not missing:
        return
    for module_name in _BUILTIN_PLUGIN_MODULES:
        module = sys.modules.get(module_name)
        if module is None:
            importlib.import_module(module_name)
        else:
            importlib.reload(module)


__all__ = [
    "IndicatorPluginManifest",
    "IndicatorPluginRegistry",
    "SignalCatalogEntry",
    "SignalDirectionSpec",
    "register_plugin",
    "plugin_registry",
    "ensure_builtin_indicator_plugins_registered",
]
