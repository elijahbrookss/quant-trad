"""Overlay type registry and contract metadata."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OverlaySpec:
    """Declarative contract for a chart overlay type."""

    type: str
    label: str
    pane_key: str = "price"
    pane_views: Tuple[str, ...] = field(default_factory=tuple)
    description: Optional[str] = None
    renderers: Mapping[str, Any] = field(default_factory=dict)
    payload_keys: Tuple[str, ...] = field(default_factory=tuple)
    ui_color: Optional[str] = None
    ui_default_visible: Optional[bool] = None


_REGISTRY: Dict[str, OverlaySpec] = {}


def register_overlay_type(
    types: str | Iterable[str],
    *,
    label: Optional[str] = None,
    pane_key: Optional[str] = None,
    pane_views: Optional[Sequence[str]] = None,
    description: Optional[str] = None,
    renderers: Optional[Mapping[str, Any]] = None,
    payload_keys: Optional[Sequence[str]] = None,
    ui_color: Optional[str] = None,
    ui_default_visible: Optional[bool] = None,
) -> None:
    """Register or update an overlay contract."""

    names = [types] if isinstance(types, str) else list(types)
    if not names:
        raise ValueError("overlay types are required")

    if not pane_views:
        raise ValueError("pane_views are required for overlay registration")

    resolved_label = label or str(names[0])
    resolved_pane_key = str(pane_key or "price").strip() or "price"
    view_tuple = tuple(str(view) for view in (pane_views or []))
    if not view_tuple:
        raise ValueError("pane_views are required for overlay registration")
    renderer_payload = dict(renderers or {})
    payload_tuple = tuple(str(key) for key in (payload_keys or []))

    for raw in names:
        overlay_type = str(raw or "").strip()
        if not overlay_type:
            continue
        existing = _REGISTRY.get(overlay_type)
        spec = OverlaySpec(
            type=overlay_type,
            label=resolved_label,
            pane_key=resolved_pane_key,
            pane_views=view_tuple,
            description=description,
            renderers=renderer_payload,
            payload_keys=payload_tuple,
            ui_color=ui_color,
            ui_default_visible=ui_default_visible,
        )
        if existing and existing != spec:
            raise ValueError(
                f"Overlay type '{overlay_type}' already registered with different metadata."
            )
        if existing == spec:
            continue
        _REGISTRY[overlay_type] = spec


def overlay_type(
    types: str | Iterable[str],
    *,
    label: Optional[str] = None,
    pane_key: Optional[str] = None,
    pane_views: Optional[Sequence[str]] = None,
    description: Optional[str] = None,
    renderers: Optional[Mapping[str, Any]] = None,
    payload_keys: Optional[Sequence[str]] = None,
    ui_color: Optional[str] = None,
    ui_default_visible: Optional[bool] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to register overlay contracts alongside adapters or transformers."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        register_overlay_type(
            types,
            label=label,
            pane_key=pane_key,
            pane_views=pane_views,
            description=description,
            renderers=renderers,
            payload_keys=payload_keys,
            ui_color=ui_color,
            ui_default_visible=ui_default_visible,
        )
        return func

    return decorator


def get_overlay_spec(overlay_type: Optional[str]) -> Optional[OverlaySpec]:
    if not overlay_type:
        return None
    return _REGISTRY.get(str(overlay_type))


def validate_overlay_payload(overlay_type: Optional[str], payload: Mapping[str, Any]) -> None:
    spec = get_overlay_spec(overlay_type)
    if not spec or not spec.payload_keys:
        return
    missing = [key for key in spec.payload_keys if key not in payload]
    if missing:
        logger.warning(
            "overlay_payload_missing_keys | type=%s | missing=%s",
            overlay_type,
            missing,
        )


def list_overlay_specs() -> Sequence[OverlaySpec]:
    return list(_REGISTRY.values())


__all__ = [
    "OverlaySpec",
    "overlay_type",
    "register_overlay_type",
    "get_overlay_spec",
    "validate_overlay_payload",
    "list_overlay_specs",
]
