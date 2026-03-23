"""Canonical typed-output indicator contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping, Sequence, Tuple

from .signal_output import assert_signal_output_event

OutputType = Literal["signal", "context", "metric"]


@dataclass(frozen=True)
class OutputRef:
    indicator_id: str
    output_name: str

    @property
    def key(self) -> str:
        return f"{self.indicator_id}.{self.output_name}"


@dataclass(frozen=True)
class OutputDefinition:
    name: str
    type: OutputType


@dataclass(frozen=True)
class OverlayDefinition:
    name: str
    overlay_type: str


@dataclass(frozen=True)
class IndicatorRuntimeSpec:
    instance_id: str
    manifest_type: str
    version: str
    dependencies: Tuple[OutputRef, ...]
    outputs: Tuple[OutputDefinition, ...]
    overlays: Tuple[OverlayDefinition, ...] = ()


@dataclass(frozen=True)
class RuntimeOutput:
    bar_time: datetime
    ready: bool
    value: dict[str, Any]

    def copy(self) -> "RuntimeOutput":
        return RuntimeOutput(
            bar_time=self.bar_time,
            ready=bool(self.ready),
            value=deepcopy(dict(self.value or {})),
        )


@dataclass(frozen=True)
class RuntimeOverlay:
    bar_time: datetime
    ready: bool
    value: dict[str, Any]

    def copy(self) -> "RuntimeOverlay":
        return RuntimeOverlay(
            bar_time=self.bar_time,
            ready=bool(self.ready),
            value=deepcopy(dict(self.value or {})),
        )


@dataclass(frozen=True)
class EngineFrame:
    outputs: dict[str, RuntimeOutput]
    overlays: dict[str, RuntimeOverlay]


class Indicator(ABC):
    runtime_spec: IndicatorRuntimeSpec

    @abstractmethod
    def apply_bar(
        self,
        bar: Any,
        inputs: Mapping[OutputRef, RuntimeOutput],
    ) -> None:
        """Advance internal indicator state for one bar."""

    @abstractmethod
    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        """Return all declared outputs for the current bar."""

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        """Return all declared overlays for the current bar."""
        return {}

    def configure_replay_window(self, *, history_bars: int | None = None) -> None:
        """Allow walk-forward window consumers to provide execution hints."""
        _ = history_bars


def output_ref_key(indicator_id: str, output_name: str) -> str:
    return f"{indicator_id}.{output_name}"


def validate_output_definitions(definitions: Sequence[OutputDefinition]) -> None:
    seen: set[str] = set()
    for definition in definitions:
        name = str(definition.name or "").strip()
        if not name:
            raise RuntimeError("indicator_manifest_invalid: output name is required")
        if name in seen:
            raise RuntimeError(f"indicator_manifest_invalid: duplicate output name={name}")
        seen.add(name)
        if str(definition.type or "").strip().lower() not in {"signal", "context", "metric"}:
            raise RuntimeError(
                f"indicator_manifest_invalid: invalid output type name={name} type={definition.type}"
            )


def validate_overlay_definitions(definitions: Sequence[OverlayDefinition]) -> None:
    seen: set[str] = set()
    for definition in definitions:
        name = str(definition.name or "").strip()
        overlay_type = str(definition.overlay_type or "").strip()
        if not name:
            raise RuntimeError("indicator_manifest_invalid: overlay name is required")
        if name in seen:
            raise RuntimeError(f"indicator_manifest_invalid: duplicate overlay name={name}")
        if not overlay_type:
            raise RuntimeError(
                f"indicator_manifest_invalid: overlay type required name={name}"
            )
        seen.add(name)


def validate_runtime_output(
    *,
    definition: OutputDefinition,
    output: RuntimeOutput,
    bar_time: datetime,
    dependency_inputs: Mapping[OutputRef, RuntimeOutput],
) -> None:
    if not isinstance(output, RuntimeOutput):
        raise RuntimeError(
            f"indicator_output_invalid: output={definition.name} runtime output required"
        )
    if output.bar_time != bar_time:
        raise RuntimeError(
            "indicator_output_invalid: bar_time mismatch "
            f"output={definition.name} expected={bar_time} actual={output.bar_time}"
        )
    if not isinstance(output.ready, bool):
        raise RuntimeError(
            f"indicator_output_invalid: output={definition.name} ready must be bool"
        )
    if not isinstance(output.value, dict):
        raise RuntimeError(
            f"indicator_output_invalid: output={definition.name} value must be dict"
        )
    if any(not dep.ready for dep in dependency_inputs.values()) and output.ready:
        raise RuntimeError(
            f"indicator_output_invalid: dependency_not_ready output={definition.name}"
        )
    if not output.ready:
        return
    if definition.type == "signal":
        _validate_signal_output(definition.name, output.value)
        return
    if definition.type == "context":
        _validate_context_output(definition.name, output.value)
        return
    if definition.type == "metric":
        _validate_metric_output(definition.name, output.value)
        return
    raise RuntimeError(
            f"indicator_output_invalid: unsupported output type={definition.type} output={definition.name}"
        )


def validate_runtime_overlay(
    *,
    definition: OverlayDefinition,
    overlay: RuntimeOverlay,
    bar_time: datetime,
    dependency_inputs: Mapping[OutputRef, RuntimeOutput],
) -> None:
    if not isinstance(overlay, RuntimeOverlay):
        raise RuntimeError(
            f"indicator_overlay_invalid: overlay={definition.name} runtime overlay required"
        )
    if overlay.bar_time != bar_time:
        raise RuntimeError(
            "indicator_overlay_invalid: bar_time mismatch "
            f"overlay={definition.name} expected={bar_time} actual={overlay.bar_time}"
        )
    if not isinstance(overlay.ready, bool):
        raise RuntimeError(
            f"indicator_overlay_invalid: overlay={definition.name} ready must be bool"
        )
    if not isinstance(overlay.value, dict):
        raise RuntimeError(
            f"indicator_overlay_invalid: overlay={definition.name} value must be dict"
        )
    if any(not dep.ready for dep in dependency_inputs.values()) and overlay.ready:
        raise RuntimeError(
            f"indicator_overlay_invalid: dependency_not_ready overlay={definition.name}"
        )
    if not overlay.ready:
        return
    _validate_canonical_overlay(definition.name, definition.overlay_type, overlay.value)


def _validate_signal_output(output_name: str, value: Mapping[str, Any]) -> None:
    if set(value.keys()) != {"events"}:
        raise RuntimeError(
            f"indicator_output_invalid: signal shape output={output_name} keys={sorted(value.keys())}"
        )
    events = value.get("events")
    if not isinstance(events, list):
        raise RuntimeError(
            f"indicator_output_invalid: signal events must be list output={output_name}"
        )
    for index, event in enumerate(events):
        if not isinstance(event, Mapping):
            raise RuntimeError(
                f"indicator_output_invalid: signal event must be mapping output={output_name} index={index}"
            )
        try:
            assert_signal_output_event(event)
        except RuntimeError as exc:
            raise RuntimeError(
                f"indicator_output_invalid: signal event shape output={output_name} index={index} detail={exc}"
            ) from exc


def _validate_context_output(output_name: str, value: Mapping[str, Any]) -> None:
    allowed = {"state_key", "fields"}
    if "state_key" not in value:
        raise RuntimeError(
            f"indicator_output_invalid: context state_key required output={output_name}"
        )
    if not set(value.keys()).issubset(allowed):
        raise RuntimeError(
            f"indicator_output_invalid: context keys invalid output={output_name} keys={sorted(value.keys())}"
        )
    if not isinstance(value.get("state_key"), str):
        raise RuntimeError(
            f"indicator_output_invalid: context state_key must be str output={output_name}"
        )
    if "fields" in value and not isinstance(value.get("fields"), dict):
        raise RuntimeError(
            f"indicator_output_invalid: context fields must be dict output={output_name}"
        )


def _validate_metric_output(output_name: str, value: Mapping[str, Any]) -> None:
    if not value:
        raise RuntimeError(
            f"indicator_output_invalid: metric value empty output={output_name}"
        )
    for key, metric_value in value.items():
        if not isinstance(key, str):
            raise RuntimeError(
                f"indicator_output_invalid: metric key must be str output={output_name}"
            )
        if isinstance(metric_value, bool) or not isinstance(metric_value, (int, float)):
            raise RuntimeError(
                "indicator_output_invalid: metric value must be numeric "
                f"output={output_name} field={key}"
            )


def _validate_canonical_overlay(
    overlay_name: str,
    overlay_type: str,
    value: Mapping[str, Any],
) -> None:
    from overlays.builtins import ensure_builtin_overlays_registered
    from overlays.registry import get_overlay_spec, validate_overlay_payload

    actual_type = str(value.get("type") or "").strip()
    if actual_type != overlay_type:
        raise RuntimeError(
            "indicator_overlay_invalid: overlay type mismatch "
            f"overlay={overlay_name} expected={overlay_type} actual={actual_type}"
        )
    payload = value.get("payload")
    pane_views = value.get("pane_views")
    ui = value.get("ui")
    if not isinstance(payload, Mapping):
        raise RuntimeError(
            f"indicator_overlay_invalid: overlay payload required overlay={overlay_name}"
        )
    if not isinstance(pane_views, (list, tuple)):
        raise RuntimeError(
            f"indicator_overlay_invalid: pane_views required overlay={overlay_name}"
        )
    if not isinstance(ui, Mapping):
        raise RuntimeError(
            f"indicator_overlay_invalid: ui required overlay={overlay_name}"
        )
    ensure_builtin_overlays_registered()
    if get_overlay_spec(overlay_type) is None:
        raise RuntimeError(
            f"indicator_overlay_invalid: overlay spec missing overlay={overlay_name} type={overlay_type}"
        )
    validate_overlay_payload(overlay_type, payload)
