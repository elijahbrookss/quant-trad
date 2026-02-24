"""Shared indicator runtime engine contracts and registry."""

from .basic_engines import (
    RollingWindowEngineConfig,
    RollingWindowStateEngine,
    VWAPStateEngine,
    build_pivot_engine,
    build_trendline_engine,
)
from .contracts import (
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
    ensure_builtin_indicator_plugins_registered,
    indicator_plugin_manifest,
    plugin_registry,
)
from .signal_evaluator import evaluate_rules_from_state_snapshots

__all__ = [
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
    "indicator_plugin_manifest",
    "plugin_registry",
    "ensure_builtin_indicator_plugins_registered",
    "evaluate_rules_from_state_snapshots",
    "project_overlay_delta",
]
