"""Core indicator plugin package."""

import importlib
import sys

from .registry import (
    IndicatorPluginManifest,
    IndicatorPluginRegistry,
    indicator_plugin_manifest,
    plugin_registry,
)


_BUILTIN_PLUGIN_MODULES = (
    "engines.bot_runtime.core.indicator_state.plugins.market_profile",
    "engines.bot_runtime.core.indicator_state.plugins.pivot_level",
    "engines.bot_runtime.core.indicator_state.plugins.trendline",
    "engines.bot_runtime.core.indicator_state.plugins.vwap",
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
    "indicator_plugin_manifest",
    "plugin_registry",
    "ensure_builtin_indicator_plugins_registered",
]
