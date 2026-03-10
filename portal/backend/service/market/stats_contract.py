"""Shared stats/regime version contract constants.

Keep this module dependency-light so callers can safely import version constants
without pulling worker/runtime side effects from ``stats_queue``.
"""

from __future__ import annotations

STATS_VERSION = "v1"
REGIME_VERSION = "v1"

__all__ = ["STATS_VERSION", "REGIME_VERSION"]
