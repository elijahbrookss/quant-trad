"""
Signals package with automatic signal rule discovery.

This package automatically discovers and imports all signal rule modules,
triggering @signal_rule decorator registration without manual imports.

Simply import this package to register all signals:
    import signals  # Auto-discovers all signal rules
"""

import importlib
import pkgutil
import logging

logger = logging.getLogger(__name__)


def _discover_signal_rules():
    """
    Auto-discover and import all signal rule modules.

    Walks through signals/rules/ and imports all modules to trigger
    @signal_rule decorator execution and registration.
    """
    try:
        import signals.rules as rules_package

        discovered_count = 0
        for importer, modname, ispkg in pkgutil.walk_packages(
            path=rules_package.__path__,
            prefix=f"{rules_package.__name__}.",
        ):
            # Import leaf modules (where @signal_rule decorators are)
            if not ispkg:
                try:
                    importlib.import_module(modname)
                    discovered_count += 1
                    logger.debug(f"Discovered signal module: {modname}")
                except Exception as e:
                    logger.warning(f"Failed to import signal module {modname}: {e}")

        logger.info(f"Auto-discovered {discovered_count} signal rule modules")

    except ImportError as e:
        logger.warning(f"Could not import signals.rules package: {e}")


# Trigger auto-discovery when this package is imported
_discover_signal_rules()


# Export commonly used items for convenience
from .base import BaseSignal
from .engine.signal_generator import indicator_plugin, overlay_adapter, signal_rule

__all__ = [
    "BaseSignal",
    "signal_rule",
    "overlay_adapter",
    "indicator_plugin",
]
