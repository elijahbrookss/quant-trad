"""ATM template helpers for per-strategy risk configuration.

This module is a compatibility wrapper that re-exports functionality from src.atm.
All core ATM template logic now lives in the src/ library.
"""

from __future__ import annotations

# Re-export from src.atm
from atm import DEFAULT_ATM_TEMPLATE, merge_templates, normalise_template, template_metrics

__all__ = [
    "DEFAULT_ATM_TEMPLATE",
    "normalise_template",
    "merge_templates",
    "template_metrics",
]
