"""Explicit indicator definition contract helpers."""

from __future__ import annotations

from typing import Any

from .manifest import IndicatorManifest


def definition_supports_runtime(definition: Any) -> bool:
    return callable(getattr(definition, "build_runtime_indicator", None))


def definition_supports_compute(definition: Any) -> bool:
    request_builder = getattr(definition, "build_compute_data_request", None)
    compute_builder = getattr(definition, "build_compute_indicator", None)
    return callable(request_builder) and callable(compute_builder)


def validate_indicator_definition(indicator_type: str, definition: Any) -> None:
    manifest = getattr(definition, "MANIFEST", None)
    if not isinstance(manifest, IndicatorManifest):
        raise RuntimeError(
            f"indicator_definition_invalid: manifest missing type={indicator_type}"
        )
    if not callable(getattr(definition, "resolve_config", None)):
        raise RuntimeError(
            "indicator_definition_invalid: resolve_config required "
            f"type={indicator_type}"
        )

    has_runtime = definition_supports_runtime(definition)
    has_compute_request = callable(
        getattr(definition, "build_compute_data_request", None)
    )
    has_compute_builder = callable(
        getattr(definition, "build_compute_indicator", None)
    )
    has_compute = has_compute_request and has_compute_builder

    if has_compute_request != has_compute_builder:
        raise RuntimeError(
            "indicator_definition_invalid: compute definitions must declare both "
            "build_compute_data_request and build_compute_indicator "
            f"type={indicator_type}"
        )
    if not has_runtime and not has_compute:
        raise RuntimeError(
            "indicator_definition_invalid: definition must support runtime and/or compute "
            f"type={indicator_type}"
        )
    runtime_request_builder = getattr(definition, "build_runtime_data_request", None)
    if runtime_request_builder is not None and not callable(runtime_request_builder):
        raise RuntimeError(
            "indicator_definition_invalid: build_runtime_data_request must be callable "
            f"type={indicator_type}"
        )
    runtime_source_facts_builder = getattr(definition, "build_runtime_source_facts", None)
    if runtime_source_facts_builder is not None and not callable(runtime_source_facts_builder):
        raise RuntimeError(
            "indicator_definition_invalid: build_runtime_source_facts must be callable "
            f"type={indicator_type}"
        )


__all__ = [
    "definition_supports_compute",
    "definition_supports_runtime",
    "validate_indicator_definition",
]
