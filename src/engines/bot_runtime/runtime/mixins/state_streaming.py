"""Compatibility shim for the split runtime streaming mixins."""

from __future__ import annotations

from .runtime_persistence import RuntimePersistenceMixin
from .runtime_projection import RuntimeProjectionMixin
from .runtime_push_stream import RuntimePushStreamMixin


class RuntimeStateStreamingMixin(
    RuntimePersistenceMixin,
    RuntimeProjectionMixin,
    RuntimePushStreamMixin,
):
    """Aggregated runtime streaming behavior split by responsibility."""


__all__ = ["RuntimeStateStreamingMixin"]
