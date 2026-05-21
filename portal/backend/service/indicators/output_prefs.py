from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, Tuple

from indicators.manifest import IndicatorManifest, manifest_output_catalog, manifest_signal_output_names


def normalize_output_prefs(
    *,
    manifest: IndicatorManifest,
    output_prefs: Mapping[str, Any] | None,
) -> Dict[str, Dict[str, Any]]:
    allowed_signal_outputs = set(manifest_signal_output_names(manifest))
    normalized: Dict[str, Dict[str, Any]] = {}
    if not isinstance(output_prefs, Mapping):
        return normalized
    for output_name, prefs in output_prefs.items():
        normalized_output_name = str(output_name or "").strip()
        if not normalized_output_name or normalized_output_name not in allowed_signal_outputs:
            continue
        if isinstance(prefs, Mapping) and prefs.get("enabled") is False:
            normalized[normalized_output_name] = {"enabled": False}
    return normalized


def typed_outputs_with_prefs(
    *,
    manifest: IndicatorManifest,
    output_prefs: Mapping[str, Any] | None,
) -> Tuple[list[dict[str, Any]], Dict[str, Dict[str, Any]]]:
    normalized_output_prefs = normalize_output_prefs(
        manifest=manifest,
        output_prefs=output_prefs,
    )
    catalog = manifest_output_catalog(manifest)
    for output in catalog:
        if output.get("type") != "signal":
            continue
        output["enabled"] = normalized_output_prefs.get(str(output.get("name") or ""), {}).get("enabled", True)
    return catalog, normalized_output_prefs


def enabled_signal_output_names(
    *,
    manifest: IndicatorManifest,
    output_prefs: Mapping[str, Any] | None,
) -> list[str]:
    normalized_output_prefs = normalize_output_prefs(
        manifest=manifest,
        output_prefs=output_prefs,
    )
    return [
        output_name
        for output_name in manifest_signal_output_names(manifest)
        if normalized_output_prefs.get(output_name, {}).get("enabled", True)
    ]


__all__ = [
    "enabled_signal_output_names",
    "normalize_output_prefs",
    "typed_outputs_with_prefs",
]
