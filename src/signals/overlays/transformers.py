"""Overlay transformers for playback-time visibility adjustments."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

from engines.bot_runtime.core.domain import normalize_epoch

logger = logging.getLogger(__name__)

OverlayTransformer = Callable[[Mapping[str, Any], int], Optional[Mapping[str, Any]]]
_REGISTRY: Dict[str, OverlayTransformer] = {}


def overlay_transformer(types: str | Iterable[str]) -> Callable[[OverlayTransformer], OverlayTransformer]:
    """Register a transformer for one or more overlay types."""

    def decorator(func: OverlayTransformer) -> OverlayTransformer:
        if isinstance(types, str):
            names = [types]
        else:
            names = list(types)
        for name in names:
            if not name:
                continue
            _REGISTRY[str(name).lower()] = func
        return func

    return decorator


def apply_overlay_transform(overlay: Mapping[str, Any], current_epoch: int) -> Optional[Mapping[str, Any]]:
    """Apply overlay transformer for the given overlay type if registered."""

    if not isinstance(overlay, Mapping):
        return None
    overlay_type = str(overlay.get("type") or "").lower()
    transformer = _REGISTRY.get(overlay_type)
    if transformer is None:
        return overlay
    try:
        return transformer(overlay, current_epoch)
    except Exception as exc:
        logger.error("overlay_transform_failed | type=%s | error=%s", overlay_type, exc)
        return None


def normalize_overlay_epoch(value: Any) -> Optional[int]:
    """Normalize timestamps for overlay transformers."""

    return normalize_epoch(value)


__all__ = [
    "overlay_transformer",
    "apply_overlay_transform",
    "normalize_overlay_epoch",
]
