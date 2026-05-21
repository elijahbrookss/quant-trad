"""Manifest-backed parameter helpers for indicator definitions."""

from __future__ import annotations

from typing import Any, Dict, Mapping

from .manifest import IndicatorManifest, manifest_param_defaults, manifest_param_types, resolve_manifest_params


def _manifest_for_definition(indicator_cls: Any) -> IndicatorManifest:
    manifest = getattr(indicator_cls, "MANIFEST", None)
    if not isinstance(manifest, IndicatorManifest):
        indicator_name = getattr(indicator_cls, "__name__", str(indicator_cls))
        raise RuntimeError(
            f"Indicator '{indicator_name}' must declare MANIFEST; signature-based param contracts are not supported"
        )
    return manifest


def indicator_required_params(indicator_cls: Any) -> list[str]:
    manifest = _manifest_for_definition(indicator_cls)
    return [param.key for param in manifest.params if param.required]


def indicator_default_params(indicator_cls: Any) -> Dict[str, Any]:
    return manifest_param_defaults(_manifest_for_definition(indicator_cls))


def indicator_field_types(indicator_cls: Any) -> Dict[str, str]:
    return manifest_param_types(_manifest_for_definition(indicator_cls))


def indicator_param_names(indicator_cls: Any) -> list[str]:
    manifest = _manifest_for_definition(indicator_cls)
    return [param.key for param in manifest.params]


def resolve_indicator_params(
    indicator_cls: Any,
    params: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    return resolve_manifest_params(_manifest_for_definition(indicator_cls), params)
