from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional, Sequence

from ..indicator_factory import INDICATOR_MAP as _INDICATOR_MAP
from ..dependency_bindings import validate_dependency_bindings
from ..output_prefs import normalize_output_prefs
from .context import IndicatorServiceContext, _context
from .utils import (
    build_meta_from_record,
    ensure_color,
    load_indicator_record,
    normalize_color,
    pull_datasource_exchange,
    scrub_runtime_params,
)

logger = logging.getLogger(__name__)


class IndicatorInstanceCreator:
    """Validate indicator config and persist metadata."""

    def __init__(self, ctx: IndicatorServiceContext = _context) -> None:
        self._ctx = ctx

    def create(
        self,
        type_str: str,
        name: Optional[str],
        params: Dict[str, Any],
        dependencies: Optional[Sequence[Dict[str, Any]]] = None,
        color: Optional[str] = None,
        output_prefs: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        definition = self._resolve_type(type_str)
        params_copy = dict(params or {})
        datasource, exchange = pull_datasource_exchange(params_copy, ctx=self._ctx)
        resolved_params = definition.resolve_config(
            scrub_runtime_params(params_copy),
            strict_unknown=True,
        )
        resolved_dependencies = validate_dependency_bindings(
            manifest=definition.MANIFEST,
            bindings=dependencies,
            ctx=self._ctx,
        )
        resolved_output_prefs = normalize_output_prefs(
            manifest=definition.MANIFEST,
            output_prefs=output_prefs,
        )
        meta = {
            "id": str(uuid.uuid4()),
            "type": type_str,
            "params": resolved_params,
            "dependencies": resolved_dependencies,
            "output_prefs": resolved_output_prefs,
            "enabled": True,
            "name": name or type_str.replace("_", " ").title(),
            "color": normalize_color(color),
        }
        if datasource:
            meta["datasource"] = datasource
        if exchange:
            meta["exchange"] = exchange
        payload = ensure_color(meta, ctx=self._ctx)
        self._ctx.repository.upsert(payload)
        refreshed = self._ctx.repository.get(payload["id"])
        return build_meta_from_record(refreshed, ctx=self._ctx) if refreshed else payload

    @staticmethod
    def _resolve_type(type_str: str):
        definition = _INDICATOR_MAP.get(type_str)
        if not definition:
            raise ValueError(f"Unknown indicator type: {type_str}")
        return definition


class IndicatorInstanceUpdater:
    """Handle indicator updates without runtime instantiation side effects."""

    def __init__(self, ctx: IndicatorServiceContext = _context) -> None:
        self._ctx = ctx

    def update(
        self,
        inst_id: str,
        type_str: str,
        params: Dict[str, Any],
        name: Optional[str],
        dependencies: Optional[Sequence[Dict[str, Any]]] = None,
        output_prefs: Optional[Dict[str, Dict[str, Any]]] = None,
        *,
        color: Optional[str] = None,
        color_provided: bool = False,
    ) -> Dict[str, Any]:
        record = load_indicator_record(inst_id, ctx=self._ctx)
        meta = build_meta_from_record(record, ctx=self._ctx)
        if type_str != meta["type"]:
            raise ValueError("Cannot change indicator type; create a new instance instead")

        definition = self._resolve_type(type_str)
        params_copy = dict(params or {})
        datasource, exchange = pull_datasource_exchange(
            params_copy,
            fallback_meta=meta,
            ctx=self._ctx,
        )
        resolved_params = definition.resolve_config(
            scrub_runtime_params(params_copy),
            strict_unknown=True,
        )
        resolved_dependencies = validate_dependency_bindings(
            manifest=definition.MANIFEST,
            bindings=dependencies,
            ctx=self._ctx,
            indicator_id=inst_id,
        )
        resolved_output_prefs = normalize_output_prefs(
            manifest=definition.MANIFEST,
            output_prefs=output_prefs,
        )

        params_unchanged = dict(meta.get("params") or {}) == resolved_params
        dependencies_unchanged = list(meta.get("dependencies") or []) == resolved_dependencies
        output_prefs_unchanged = dict(meta.get("output_prefs") or {}) == resolved_output_prefs
        name_unchanged = (name or meta.get("name")) == meta.get("name")
        color_unchanged = (
            not color_provided
            or normalize_color(color) == normalize_color(meta.get("color"))
        )
        datasource_unchanged = (datasource or None) == (meta.get("datasource") or None)
        exchange_unchanged = (exchange or None) == (meta.get("exchange") or None)
        if (
            params_unchanged
            and dependencies_unchanged
            and output_prefs_unchanged
            and name_unchanged
            and color_unchanged
            and datasource_unchanged
            and exchange_unchanged
        ):
            return meta

        self._ctx.overlay_cache.purge_indicator(inst_id)

        meta_payload = dict(meta)
        meta_payload["params"] = resolved_params
        meta_payload["dependencies"] = resolved_dependencies
        meta_payload["output_prefs"] = resolved_output_prefs
        meta_payload["name"] = name or meta.get("name") or type_str.replace("_", " ").title()
        if color_provided:
            meta_payload["color"] = normalize_color(color)
        if datasource:
            meta_payload["datasource"] = datasource
        else:
            meta_payload.pop("datasource", None)
        if exchange:
            meta_payload["exchange"] = exchange
        else:
            meta_payload.pop("exchange", None)
        payload = ensure_color(meta_payload, ctx=self._ctx)
        self._ctx.repository.upsert(payload)
        refreshed = self._ctx.repository.get(inst_id)
        return build_meta_from_record(refreshed, ctx=self._ctx) if refreshed else payload

    @staticmethod
    def _resolve_type(type_str: str):
        definition = _INDICATOR_MAP.get(type_str)
        if not definition:
            raise ValueError(f"Unknown indicator type: {type_str}")
        return definition


__all__ = ["IndicatorInstanceCreator", "IndicatorInstanceUpdater"]
