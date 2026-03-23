"""Built-in overlay registrations to ensure registry coverage."""

from __future__ import annotations

import importlib
import logging
import pkgutil

from .registry import list_overlay_specs, register_overlay_type

_REGISTERED = False
logger = logging.getLogger(__name__)


def _discover_indicator_overlay_modules() -> None:
    """Import `indicators.*.overlays` so decorator-based specs self-register."""
    try:
        import indicators as indicators_pkg
    except Exception as exc:
        logger.warning("overlay_indicator_module_discovery_failed | error=%s", exc)
        return

    discovered = 0
    failed = 0
    for _importer, modname, ispkg in pkgutil.walk_packages(
        path=indicators_pkg.__path__,
        prefix=f"{indicators_pkg.__name__}.",
    ):
        if ispkg or not modname.endswith(".overlays"):
            continue
        try:
            importlib.import_module(modname)
            discovered += 1
        except Exception as exc:
            failed += 1
            logger.warning("overlay_module_import_failed | module=%s error=%s", modname, exc)
    logger.debug(
        "overlay_modules_discovered | discovered=%s failed=%s",
        discovered,
        failed,
    )


def ensure_builtin_overlays_registered() -> None:
    global _REGISTERED
    if _REGISTERED:
        return

    _discover_indicator_overlay_modules()

    register_overlay_type(
        "strategy_signal",
        label="Strategy Signals",
        pane_views=("marker",),
        description="Strategy evaluation signal markers.",
        renderers={"lightweight": "marker", "mpl": "scatter"},
        payload_keys=("markers",),
        ui_color="#10b981",
    )
    register_overlay_type(
        "indicator_signal",
        label="Indicator Signals",
        pane_views=("signal_bubble",),
        description="Indicator research signal bubbles.",
        renderers={"lightweight": "signal_bubble", "mpl": "scatter"},
        payload_keys=("bubbles",),
        ui_color="#38bdf8",
    )
    register_overlay_type(
        "regime_overlay",
        label="Market Structure",
        pane_views=("va_box", "segment"),
        description="Regime structure bands with optional change markers.",
        renderers={"lightweight": "va_box", "mpl": "box"},
        payload_keys=("boxes", "segments", "regime_blocks", "regime_points"),
        ui_color="#94a3b8",
    )
    register_overlay_type(
        "regime_markers",
        label="Regime Markers",
        pane_views=("marker",),
        description="Regime change markers for research context.",
        renderers={"lightweight": "marker", "mpl": "scatter"},
        payload_keys=("markers",),
        ui_color="#94a3b8",
        ui_default_visible=False,
    )

    lens_colors = {
        "structure": "#f59e0b",
        "expansion": "#a855f7",
        "liquidity": "#22d3ee",
        "volatility": "#0ea5e9",
    }
    for lens in ("structure", "expansion", "liquidity", "volatility"):
        register_overlay_type(
            f"regime_overlay_{lens}",
            label=f"{lens.title()} Band",
            pane_views=("va_box",),
            description=f"Regime {lens} lens bands.",
            renderers={"lightweight": "va_box", "mpl": "box"},
            payload_keys=("boxes",),
            ui_color=lens_colors.get(lens, "#94a3b8"),
            ui_default_visible=False,
        )

    _REGISTERED = True
    logger.debug("overlay_registry_initialized | overlay_types=%d", len(list_overlay_specs()))


__all__ = ["ensure_builtin_overlays_registered"]
