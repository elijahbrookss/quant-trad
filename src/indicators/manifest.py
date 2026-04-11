"""Canonical full indicator manifests and manifest-derived helpers."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping, Sequence

from engines.indicator_engine.contracts import (
    DetailDefinition,
    IndicatorRuntimeSpec,
    OutputDefinition,
    OutputRef,
    OverlayDefinition,
)

IndicatorParamType = Literal[
    "int",
    "float",
    "bool",
    "string",
    "int_list",
    "float_list",
    "string_list",
]

_NO_DEFAULT = object()


@dataclass(frozen=True)
class IndicatorOption:
    value: Any
    label: str
    description: str = ""
    badge: str | None = None
    disabled: bool = False


@dataclass(frozen=True)
class IndicatorParam:
    key: str
    type: IndicatorParamType
    label: str
    description: str = ""
    required: bool = False
    editable: bool = True
    advanced: bool = False
    group: str = "general"
    default: Any = _NO_DEFAULT
    options: tuple[Any, ...] = ()

    @property
    def has_default(self) -> bool:
        return self.default is not _NO_DEFAULT


@dataclass(frozen=True)
class IndicatorOutput:
    name: str
    type: Literal["signal", "context", "metric"]
    label: str
    event_keys: tuple[str, ...] = ()
    state_keys: tuple[str, ...] = ()
    fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class IndicatorOverlay:
    name: str
    overlay_type: str
    label: str
    description: str = ""


@dataclass(frozen=True)
class IndicatorDetail:
    name: str
    label: str
    description: str = ""


@dataclass(frozen=True)
class IndicatorColorPalette:
    key: str
    label: str
    description: str = ""
    signal_color: str | None = None
    overlay_colors: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class IndicatorDependency:
    indicator_type: str
    output_name: str
    label: str = ""
    description: str = ""


@dataclass(frozen=True)
class IndicatorRuntimeInput:
    source_timeframe: str | None = None
    source_timeframe_param: str | None = None
    lookback_bars: int | None = None
    lookback_bars_param: str | None = None
    lookback_days: int | None = None
    lookback_days_param: str | None = None


@dataclass(frozen=True)
class IndicatorManifest:
    type: str
    version: str
    label: str
    description: str
    color_mode: Literal["single", "palette"] = "single"
    color_palettes: tuple[IndicatorColorPalette, ...] = ()
    params: tuple[IndicatorParam, ...] = ()
    outputs: tuple[IndicatorOutput, ...] = ()
    overlays: tuple[IndicatorOverlay, ...] = ()
    details: tuple[IndicatorDetail, ...] = ()
    dependencies: tuple[IndicatorDependency, ...] = ()
    runtime_inputs: tuple[IndicatorRuntimeInput, ...] = ()


TIMEFRAME_OPTIONS: tuple[IndicatorOption, ...] = (
    IndicatorOption("1m", "1 Minute"),
    IndicatorOption("5m", "5 Minutes"),
    IndicatorOption("15m", "15 Minutes"),
    IndicatorOption("30m", "30 Minutes"),
    IndicatorOption("1h", "1 Hour", badge="Featured"),
    IndicatorOption("4h", "4 Hours", badge="Featured"),
    IndicatorOption("1d", "1 Day", badge="Featured"),
    IndicatorOption("1w", "1 Week"),
)


def validate_indicator_manifest(manifest: IndicatorManifest) -> None:
    manifest_type = str(manifest.type or "").strip()
    if not manifest_type:
        raise RuntimeError("indicator_manifest_invalid: type is required")
    if not str(manifest.version or "").strip():
        raise RuntimeError(
            f"indicator_manifest_invalid: version required type={manifest_type}"
        )
    color_mode = str(manifest.color_mode or "").strip().lower()
    if color_mode not in {"single", "palette"}:
        raise RuntimeError(
            f"indicator_manifest_invalid: color_mode invalid type={manifest_type} color_mode={manifest.color_mode}"
        )

    seen_params: set[str] = set()
    for param in manifest.params:
        key = str(param.key or "").strip()
        if not key:
            raise RuntimeError(
                f"indicator_manifest_invalid: param key required type={manifest_type}"
            )
        if key in seen_params:
            raise RuntimeError(
                f"indicator_manifest_invalid: duplicate param key type={manifest_type} key={key}"
            )
        if param.required and param.has_default:
            raise RuntimeError(
                f"indicator_manifest_invalid: param cannot be required and defaulted type={manifest_type} key={key}"
            )
        seen_params.add(key)

    seen_outputs: set[str] = set()
    for output in manifest.outputs:
        name = str(output.name or "").strip()
        if not name:
            raise RuntimeError(
                f"indicator_manifest_invalid: output name required type={manifest_type}"
            )
        if name in seen_outputs:
            raise RuntimeError(
                f"indicator_manifest_invalid: duplicate output name type={manifest_type} name={name}"
            )
        seen_outputs.add(name)

    seen_overlays: set[str] = set()
    for overlay in manifest.overlays:
        name = str(overlay.name or "").strip()
        overlay_type = str(overlay.overlay_type or "").strip()
        if not name:
            raise RuntimeError(
                f"indicator_manifest_invalid: overlay name required type={manifest_type}"
            )
        if not overlay_type:
            raise RuntimeError(
                f"indicator_manifest_invalid: overlay type required type={manifest_type} name={name}"
            )
        if name in seen_overlays:
            raise RuntimeError(
                f"indicator_manifest_invalid: duplicate overlay name type={manifest_type} name={name}"
            )
        seen_overlays.add(name)

    seen_details: set[str] = set()
    for detail in manifest.details:
        name = str(detail.name or "").strip()
        if not name:
            raise RuntimeError(
                f"indicator_manifest_invalid: detail name required type={manifest_type}"
            )
        if name in seen_details:
            raise RuntimeError(
                f"indicator_manifest_invalid: duplicate detail name type={manifest_type} name={name}"
            )
        seen_details.add(name)

    seen_palettes: set[str] = set()
    for palette in manifest.color_palettes:
        key = str(palette.key or "").strip()
        if not key:
            raise RuntimeError(
                f"indicator_manifest_invalid: color palette key required type={manifest_type}"
            )
        if key in seen_palettes:
            raise RuntimeError(
                f"indicator_manifest_invalid: duplicate color palette key type={manifest_type} key={key}"
            )
        seen_palettes.add(key)


def editable_manifest_params(manifest: IndicatorManifest) -> list[IndicatorParam]:
    return [param for param in manifest.params if bool(param.editable)]


def manifest_param_defaults(manifest: IndicatorManifest) -> dict[str, Any]:
    defaults: dict[str, Any] = {}
    for param in manifest.params:
        if param.has_default:
            defaults[param.key] = deepcopy(param.default)
    return defaults


def resolve_manifest_params(
    manifest: IndicatorManifest,
    params: Mapping[str, Any] | None,
    *,
    strict_unknown: bool = False,
) -> dict[str, Any]:
    param_keys = {param.key for param in manifest.params}
    incoming = dict(params or {})
    unknown = sorted(key for key in incoming.keys() if key not in param_keys)
    if unknown and strict_unknown:
        raise ValueError(
            f"{manifest.type} indicator received unknown params: {unknown}"
        )
    resolved = {key: deepcopy(value) for key, value in incoming.items() if key in param_keys}
    for key, value in manifest_param_defaults(manifest).items():
        resolved.setdefault(key, value)

    missing = [
        param.key
        for param in manifest.params
        if param.required and resolved.get(param.key) is None
    ]
    if missing:
        raise ValueError(
            f"{manifest.type} indicator missing required params: {sorted(missing)}"
        )
    return resolved


def manifest_param_types(manifest: IndicatorManifest) -> dict[str, str]:
    return {param.key: param.type for param in manifest.params}


def manifest_output_catalog(manifest: IndicatorManifest) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for output in manifest.outputs:
        item: dict[str, Any] = {
            "name": output.name,
            "type": output.type,
            "label": output.label,
        }
        if output.event_keys:
            item["event_keys"] = list(output.event_keys)
        if output.state_keys:
            item["state_keys"] = list(output.state_keys)
        if output.fields:
            item["fields"] = list(output.fields)
        catalog.append(item)
    return catalog


def manifest_signal_output_names(manifest: IndicatorManifest) -> list[str]:
    return [output.name for output in manifest.outputs if output.type == "signal"]


def manifest_overlay_catalog(manifest: IndicatorManifest) -> list[dict[str, Any]]:
    return [
        {
            "name": overlay.name,
            "overlay_type": overlay.overlay_type,
            "label": overlay.label,
            "description": overlay.description,
        }
        for overlay in manifest.overlays
    ]


def manifest_detail_catalog(manifest: IndicatorManifest) -> list[dict[str, Any]]:
    return [
        {
            "name": detail.name,
            "label": detail.label,
            "description": detail.description,
        }
        for detail in manifest.details
    ]


def manifest_color_palette_catalog(manifest: IndicatorManifest) -> list[dict[str, Any]]:
    return [
        {
            "key": palette.key,
            "label": palette.label,
            "description": palette.description,
            "signal_color": palette.signal_color,
            "overlay_colors": dict(palette.overlay_colors),
        }
        for palette in manifest.color_palettes
    ]


def resolve_manifest_color_palette(
    manifest: IndicatorManifest,
    value: str | None,
) -> str | None:
    palettes = [str(palette.key) for palette in manifest.color_palettes if str(palette.key or "").strip()]
    if str(manifest.color_mode or "").strip().lower() != "palette":
        return None
    if not palettes:
        return None
    candidate = str(value or "").strip()
    if not candidate:
        return palettes[0]
    if candidate in palettes:
        return candidate
    raise ValueError(
        f"{manifest.type} indicator received unknown color_palette: {candidate}"
    )


def _serialize_option(option: Any) -> dict[str, Any]:
    if isinstance(option, IndicatorOption):
        return {
            "value": deepcopy(option.value),
            "label": option.label,
            "description": option.description,
            "badge": option.badge,
            "disabled": bool(option.disabled),
        }
    return {
        "value": deepcopy(option),
        "label": str(option),
        "description": "",
        "badge": None,
        "disabled": False,
    }


def serialize_indicator_manifest(manifest: IndicatorManifest) -> dict[str, Any]:
    validate_indicator_manifest(manifest)
    return {
        "type": manifest.type,
        "version": manifest.version,
        "label": manifest.label,
        "description": manifest.description,
        "color_mode": manifest.color_mode,
        "color_palettes": manifest_color_palette_catalog(manifest),
        "params": [
            {
                "key": param.key,
                "type": param.type,
                "label": param.label,
                "description": param.description,
                "required": bool(param.required),
                "editable": bool(param.editable),
                "advanced": bool(param.advanced),
                "group": param.group,
                "has_default": bool(param.has_default),
                "default": deepcopy(param.default) if param.has_default else None,
                "options": [_serialize_option(option) for option in param.options],
            }
            for param in manifest.params
        ],
        "outputs": manifest_output_catalog(manifest),
        "overlays": manifest_overlay_catalog(manifest),
        "details": manifest_detail_catalog(manifest),
        "dependencies": [
            {
                "indicator_type": dependency.indicator_type,
                "output_name": dependency.output_name,
                "label": dependency.label,
                "description": dependency.description,
            }
            for dependency in manifest.dependencies
        ],
        "runtime_inputs": [
            {
                "source_timeframe": spec.source_timeframe,
                "source_timeframe_param": spec.source_timeframe_param,
                "lookback_bars": spec.lookback_bars,
                "lookback_bars_param": spec.lookback_bars_param,
                "lookback_days": spec.lookback_days,
                "lookback_days_param": spec.lookback_days_param,
            }
            for spec in manifest.runtime_inputs
        ],
    }


def build_runtime_spec(
    manifest: IndicatorManifest,
    *,
    instance_id: str,
    version: str | None = None,
    dependencies: Sequence[OutputRef] = (),
) -> IndicatorRuntimeSpec:
    validate_indicator_manifest(manifest)
    resolved_instance_id = str(instance_id or "").strip()
    if not resolved_instance_id:
        raise RuntimeError(
            f"indicator_runtime_spec_invalid: instance_id required type={manifest.type}"
        )
    return IndicatorRuntimeSpec(
        instance_id=resolved_instance_id,
        manifest_type=manifest.type,
        version=str(version or manifest.version),
        dependencies=tuple(dependencies),
        outputs=tuple(
            OutputDefinition(name=output.name, type=output.type)
            for output in manifest.outputs
        ),
        overlays=tuple(
            OverlayDefinition(name=overlay.name, overlay_type=overlay.overlay_type)
            for overlay in manifest.overlays
        ),
        details=tuple(
            DetailDefinition(name=detail.name)
            for detail in manifest.details
        ),
    )


def extract_manifest_instance_params(
    manifest: IndicatorManifest,
    instance: Any,
) -> dict[str, Any]:
    extracted: dict[str, Any] = {}
    for param in manifest.params:
        if (
            param.key == "bin_size"
            and hasattr(instance, "_bin_size_locked")
            and not bool(getattr(instance, "_bin_size_locked"))
        ):
            continue
        if not hasattr(instance, param.key):
            continue
        extracted[param.key] = deepcopy(getattr(instance, param.key))
    return extracted


def manifest_runtime_input_specs(
    manifest: IndicatorManifest,
) -> list[IndicatorRuntimeInput]:
    return list(manifest.runtime_inputs)


def validate_indicator_package_layout(paths: Iterable[str]) -> None:
    for path in paths:
        if not str(path or "").strip():
            raise RuntimeError("indicator_package_invalid: empty path")
