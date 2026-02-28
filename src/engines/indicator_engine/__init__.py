"""Shared indicator runtime engine contracts and registry."""

from .basic_engines import (
    RollingWindowEngineConfig,
    RollingWindowStateEngine,
    VWAPStateEngine,
    build_pivot_engine,
    build_trendline_engine,
)
from .contracts import (
    INDICATOR_SNAPSHOT_SCHEMA_VERSION,
    IndicatorStateDelta,
    IndicatorStateEngine,
    IndicatorStateSnapshot,
    OverlayProjectionInput,
    ProjectionDelta,
    SignalEvaluationInput,
)
from .overlay_projection import OverlayEntryProjector, project_overlay_delta
from .plugins import (
    IndicatorPluginManifest,
    IndicatorPluginRegistry,
    SignalCatalogEntry,
    SignalDirectionSpec,
    ensure_builtin_indicator_plugins_registered,
    plugin_registry,
    register_plugin,
)
from .signal_evaluator import evaluate_rules_from_state_snapshots

__all__ = [
    "INDICATOR_SNAPSHOT_SCHEMA_VERSION",
    "IndicatorStateDelta",
    "IndicatorStateEngine",
    "IndicatorStateSnapshot",
    "OverlayProjectionInput",
    "ProjectionDelta",
    "SignalEvaluationInput",
    "RollingWindowEngineConfig",
    "RollingWindowStateEngine",
    "VWAPStateEngine",
    "build_pivot_engine",
    "build_trendline_engine",
    "OverlayEntryProjector",
    "IndicatorPluginManifest",
    "IndicatorPluginRegistry",
    "SignalCatalogEntry",
    "SignalDirectionSpec",
    "register_plugin",
    "plugin_registry",
    "ensure_builtin_indicator_plugins_registered",
    "evaluate_rules_from_state_snapshots",
    "project_overlay_delta",
]
