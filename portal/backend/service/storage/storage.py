"""Compatibility facade for storage repositories.

Historically this module contained all persistence behavior. Implementations are
now grouped by cohesive model domains in ``portal.backend.service.storage.repos``.
"""

from __future__ import annotations

from .repos import *  # noqa: F401,F403

