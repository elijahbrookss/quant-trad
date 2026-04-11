"""Deterministic typed-output indicator execution engine."""

from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
import time
from typing import Any, Deque, Dict, Iterable, Mapping

from .contracts import (
    DetailDefinition,
    EngineFrame,
    Indicator,
    IndicatorGuardMetric,
    IndicatorGuardWarning,
    IndicatorRuntimeSpec,
    OverlayDefinition,
    OutputRef,
    OutputType,
    RuntimeDetail,
    RuntimeOverlay,
    RuntimeOutput,
    output_ref_key,
    validate_detail_definitions,
    validate_overlay_definitions,
    validate_output_definitions,
    validate_runtime_detail,
    validate_runtime_overlay,
    validate_runtime_output,
)


_OVERLAY_POINT_KEYS = (
    "price_lines",
    "markers",
    "touchPoints",
    "touch_points",
    "boxes",
    "segments",
    "polylines",
    "bubbles",
    "regime_blocks",
    "regime_points",
)


@dataclass(frozen=True)
class IndicatorGuardConfig:
    enabled: bool = True
    time_soft_limit_ms: float = 35.0
    time_consecutive_bars: int = 3
    time_window_bars: int = 20
    time_window_breach_count: int = 5
    overlay_points_soft_limit: int = 400
    overlay_points_hard_limit: int = 1200
    overlay_payload_soft_limit_bytes: int = 131072
    overlay_payload_hard_limit_bytes: int = 262144

    @classmethod
    def disabled(cls) -> "IndicatorGuardConfig":
        return cls(enabled=False)


class IndicatorExecutionEngine:
    def __init__(
        self,
        indicators: Iterable[Indicator],
        *,
        guard_config: IndicatorGuardConfig | None = None,
    ) -> None:
        self._indicators_by_id: Dict[str, Indicator] = {}
        self._runtime_specs_by_id: Dict[str, IndicatorRuntimeSpec] = {}
        self._output_defs: Dict[OutputRef, OutputType] = {}
        self._overlay_defs: Dict[str, Dict[str, OverlayDefinition]] = {}
        self._detail_defs: Dict[str, Dict[str, DetailDefinition]] = {}
        self._guard_config = guard_config or IndicatorGuardConfig.disabled()
        self._guard_state_by_id: Dict[str, Dict[str, Any]] = {}
        for indicator in indicators:
            runtime_spec = getattr(indicator, "runtime_spec", None)
            if runtime_spec is None:
                raise RuntimeError("indicator_engine_invalid: indicator runtime spec required")
            indicator_id = str(runtime_spec.instance_id or "").strip()
            if not indicator_id:
                raise RuntimeError("indicator_engine_invalid: runtime spec instance_id required")
            if indicator_id in self._indicators_by_id:
                raise RuntimeError(f"indicator_engine_invalid: duplicate indicator id={indicator_id}")
            validate_output_definitions(runtime_spec.outputs)
            validate_overlay_definitions(runtime_spec.overlays)
            validate_detail_definitions(runtime_spec.details)
            self._indicators_by_id[indicator_id] = indicator
            self._runtime_specs_by_id[indicator_id] = runtime_spec
            self._overlay_defs[indicator_id] = {
                overlay.name: overlay for overlay in runtime_spec.overlays
            }
            self._detail_defs[indicator_id] = {
                detail.name: detail for detail in runtime_spec.details
            }
            for output in runtime_spec.outputs:
                ref = OutputRef(indicator_id=indicator_id, output_name=output.name)
                self._output_defs[ref] = output.type

        self._order = self._topological_order()
        self._flat_output_types = {
            output_ref_key(ref.indicator_id, ref.output_name): output_type
            for ref, output_type in self._output_defs.items()
        }

    @property
    def order(self) -> tuple[str, ...]:
        return self._order

    @property
    def output_types(self) -> Dict[str, OutputType]:
        return dict(self._flat_output_types)

    def step(
        self,
        *,
        bar: object,
        bar_time: datetime,
        include_overlays: bool = True,
    ) -> EngineFrame:
        by_ref: Dict[OutputRef, RuntimeOutput] = {}
        flat: Dict[str, RuntimeOutput] = {}
        flat_overlays: Dict[str, RuntimeOverlay] = {}
        flat_details: Dict[str, RuntimeDetail] = {}
        guard_metrics: list[IndicatorGuardMetric] = []
        guard_warnings: list[IndicatorGuardWarning] = []
        for indicator_id in self._order:
            indicator = self._indicators_by_id[indicator_id]
            runtime_spec = self._runtime_specs_by_id[indicator_id]
            inputs = {ref: by_ref[ref] for ref in runtime_spec.dependencies}
            apply_started = time.perf_counter()
            indicator.apply_bar(bar, inputs)
            execution_time_ms = max((time.perf_counter() - apply_started) * 1000.0, 0.0)
            outputs = dict(indicator.snapshot())
            declared = {definition.name: definition for definition in runtime_spec.outputs}
            if set(outputs.keys()) != set(declared.keys()):
                raise RuntimeError(
                    "indicator_output_invalid: output presence mismatch "
                    f"indicator_id={indicator_id} declared={sorted(declared.keys())} returned={sorted(outputs.keys())}"
                )
            for output_name, definition in declared.items():
                runtime_output = outputs[output_name]
                validate_runtime_output(
                    definition=definition,
                    output=runtime_output,
                    bar_time=bar_time,
                    dependency_inputs=inputs,
                )
                copied = runtime_output.copy()
                ref = OutputRef(indicator_id=indicator_id, output_name=output_name)
                by_ref[ref] = copied
                flat[output_ref_key(indicator_id, output_name)] = copied

            detail_started = time.perf_counter()
            details = dict(indicator.detail_snapshot())
            detail_time_ms = max((time.perf_counter() - detail_started) * 1000.0, 0.0)
            declared_details = self._detail_defs[indicator_id]
            if set(details.keys()) != set(declared_details.keys()):
                raise RuntimeError(
                    "indicator_detail_invalid: detail presence mismatch "
                    f"indicator_id={indicator_id} declared={sorted(declared_details.keys())} returned={sorted(details.keys())}"
                )
            for detail_name, definition in declared_details.items():
                runtime_detail = details[detail_name]
                validate_runtime_detail(
                    definition=definition,
                    detail=runtime_detail,
                    bar_time=bar_time,
                    dependency_inputs=inputs,
                )
                flat_details[output_ref_key(indicator_id, detail_name)] = runtime_detail.copy()

            overlay_time_ms = 0.0
            overlay_count = 0
            overlay_points = 0
            overlay_payload_bytes = 0
            overlay_suppressed = False
            suppressed_overlay_names: tuple[str, ...] = ()
            if include_overlays:
                overlay_started = time.perf_counter()
                overlays = dict(indicator.overlay_snapshot())
                overlay_time_ms = max((time.perf_counter() - overlay_started) * 1000.0, 0.0)
                declared_overlays = self._overlay_defs[indicator_id]
                if set(overlays.keys()) != set(declared_overlays.keys()):
                    raise RuntimeError(
                        "indicator_overlay_invalid: overlay presence mismatch "
                        f"indicator_id={indicator_id} declared={sorted(declared_overlays.keys())} returned={sorted(overlays.keys())}"
                    )
                ready_overlay_names: list[str] = []
                for overlay_name, definition in declared_overlays.items():
                    runtime_overlay = overlays[overlay_name]
                    validate_runtime_overlay(
                        definition=definition,
                        overlay=runtime_overlay,
                        bar_time=bar_time,
                        dependency_inputs=inputs,
                    )
                    copied_overlay = runtime_overlay.copy()
                    if copied_overlay.ready:
                        ready_overlay_names.append(overlay_name)
                        overlay_count += 1
                        overlay_points += self._overlay_points(copied_overlay.value)
                        overlay_payload_bytes += self._payload_size_bytes(copied_overlay.value)
                    overlays[overlay_name] = copied_overlay

                hard_reasons = self._hard_overlay_reasons(
                    overlay_points=overlay_points,
                    overlay_payload_bytes=overlay_payload_bytes,
                )
                if hard_reasons and ready_overlay_names:
                    overlay_suppressed = True
                    suppressed_overlay_names = tuple(sorted(ready_overlay_names))
                    for overlay_name in ready_overlay_names:
                        overlays[overlay_name] = RuntimeOverlay(
                            bar_time=bar_time,
                            ready=False,
                            value={},
                        )

                for overlay_name in declared_overlays.keys():
                    flat_overlays[output_ref_key(indicator_id, overlay_name)] = overlays[overlay_name]

                if hard_reasons:
                    guard_warnings.append(
                        self._overlay_suppressed_warning(
                            runtime_spec=runtime_spec,
                            overlay_points=overlay_points,
                            overlay_payload_bytes=overlay_payload_bytes,
                            reasons=hard_reasons,
                            overlay_names=suppressed_overlay_names,
                        )
                    )
                else:
                    if self._guard_config.enabled and overlay_points > int(self._guard_config.overlay_points_soft_limit):
                        guard_warnings.append(
                            self._overlay_points_warning(
                                runtime_spec=runtime_spec,
                                overlay_points=overlay_points,
                            )
                        )
                    if self._guard_config.enabled and overlay_payload_bytes > int(
                        self._guard_config.overlay_payload_soft_limit_bytes
                    ):
                        guard_warnings.append(
                            self._overlay_payload_warning(
                                runtime_spec=runtime_spec,
                                overlay_payload_bytes=overlay_payload_bytes,
                            )
                        )

            total_time_ms = execution_time_ms + detail_time_ms + overlay_time_ms
            guard_metrics.append(
                IndicatorGuardMetric(
                    indicator_id=indicator_id,
                    manifest_type=runtime_spec.manifest_type,
                    version=runtime_spec.version,
                    execution_time_ms=round(total_time_ms, 4),
                    overlay_time_ms=round(overlay_time_ms, 4),
                    overlay_count=overlay_count,
                    overlay_points=overlay_points,
                    overlay_payload_bytes=overlay_payload_bytes,
                    overlay_suppressed=overlay_suppressed,
                )
            )
            if self._should_warn_on_time_budget(indicator_id=indicator_id, execution_time_ms=total_time_ms):
                guard_warnings.append(
                    self._time_budget_warning(
                        runtime_spec=runtime_spec,
                        execution_time_ms=total_time_ms,
                    )
                )
        return EngineFrame(
            outputs=flat,
            overlays=flat_overlays,
            details=flat_details,
            guard_metrics=tuple(guard_metrics),
            guard_warnings=tuple(guard_warnings),
        )

    @staticmethod
    def _payload_size_bytes(payload: Mapping[str, Any]) -> int:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return len(encoded.encode("utf-8"))

    @staticmethod
    def _overlay_points(overlay: Mapping[str, Any]) -> int:
        payload = overlay.get("payload")
        if not isinstance(payload, Mapping):
            return 0
        points = 0
        for key in _OVERLAY_POINT_KEYS:
            entries = payload.get(key)
            if isinstance(entries, list):
                points += len(entries)
        return points

    def _guard_state(self, indicator_id: str) -> Dict[str, Any]:
        state = self._guard_state_by_id.get(indicator_id)
        if state is not None:
            return state
        state = {
            "time_breach_streak": 0,
            "time_breach_window": deque(
                maxlen=max(int(self._guard_config.time_window_bars), 1)
            ),
        }
        self._guard_state_by_id[indicator_id] = state
        return state

    def _should_warn_on_time_budget(self, *, indicator_id: str, execution_time_ms: float) -> bool:
        if not self._guard_config.enabled:
            return False
        breached = float(execution_time_ms) > float(self._guard_config.time_soft_limit_ms)
        state = self._guard_state(indicator_id)
        state["time_breach_streak"] = int(state.get("time_breach_streak") or 0) + 1 if breached else 0
        breach_window: Deque[int] = state["time_breach_window"]
        breach_window.append(1 if breached else 0)
        return breached and (
            int(state["time_breach_streak"]) >= int(self._guard_config.time_consecutive_bars)
            or sum(int(value) for value in breach_window) >= int(self._guard_config.time_window_breach_count)
        )

    def _hard_overlay_reasons(self, *, overlay_points: int, overlay_payload_bytes: int) -> list[str]:
        if not self._guard_config.enabled:
            return []
        reasons: list[str] = []
        hard_points = int(self._guard_config.overlay_points_hard_limit)
        hard_payload_bytes = int(self._guard_config.overlay_payload_hard_limit_bytes)
        if hard_points > 0 and int(overlay_points) > hard_points:
            reasons.append("overlay_points")
        if hard_payload_bytes > 0 and int(overlay_payload_bytes) > hard_payload_bytes:
            reasons.append("overlay_payload_bytes")
        return reasons

    def _time_budget_warning(
        self,
        *,
        runtime_spec: IndicatorRuntimeSpec,
        execution_time_ms: float,
    ) -> IndicatorGuardWarning:
        title = "Execution budget exceeded"
        return IndicatorGuardWarning(
            warning_type="indicator_time_budget_exceeded",
            severity="warning",
            indicator_id=runtime_spec.instance_id,
            manifest_type=runtime_spec.manifest_type,
            version=runtime_spec.version,
            title=title,
            message=(
                f"{runtime_spec.instance_id} exceeded the indicator execution budget repeatedly "
                f"({execution_time_ms:.2f}ms > {float(self._guard_config.time_soft_limit_ms):.2f}ms)."
            ),
            context={
                "execution_time_ms": round(float(execution_time_ms), 4),
                "time_soft_limit_ms": float(self._guard_config.time_soft_limit_ms),
                "time_consecutive_bars": int(self._guard_config.time_consecutive_bars),
                "time_window_bars": int(self._guard_config.time_window_bars),
                "time_window_breach_count": int(self._guard_config.time_window_breach_count),
            },
        )

    def _overlay_points_warning(
        self,
        *,
        runtime_spec: IndicatorRuntimeSpec,
        overlay_points: int,
    ) -> IndicatorGuardWarning:
        return IndicatorGuardWarning(
            warning_type="indicator_overlay_points_exceeded",
            severity="warning",
            indicator_id=runtime_spec.instance_id,
            manifest_type=runtime_spec.manifest_type,
            version=runtime_spec.version,
            title="Overlay point budget exceeded",
            message=(
                f"{runtime_spec.instance_id} exceeded the overlay point budget "
                f"({int(overlay_points)} > {int(self._guard_config.overlay_points_soft_limit)})."
            ),
            context={
                "overlay_points": int(overlay_points),
                "overlay_points_soft_limit": int(self._guard_config.overlay_points_soft_limit),
            },
        )

    def _overlay_payload_warning(
        self,
        *,
        runtime_spec: IndicatorRuntimeSpec,
        overlay_payload_bytes: int,
    ) -> IndicatorGuardWarning:
        return IndicatorGuardWarning(
            warning_type="indicator_overlay_payload_exceeded",
            severity="warning",
            indicator_id=runtime_spec.instance_id,
            manifest_type=runtime_spec.manifest_type,
            version=runtime_spec.version,
            title="Overlay payload budget exceeded",
            message=(
                f"{runtime_spec.instance_id} exceeded the overlay payload budget "
                f"({int(overlay_payload_bytes)}B > {int(self._guard_config.overlay_payload_soft_limit_bytes)}B)."
            ),
            context={
                "overlay_payload_bytes": int(overlay_payload_bytes),
                "overlay_payload_soft_limit_bytes": int(self._guard_config.overlay_payload_soft_limit_bytes),
            },
        )

    def _overlay_suppressed_warning(
        self,
        *,
        runtime_spec: IndicatorRuntimeSpec,
        overlay_points: int,
        overlay_payload_bytes: int,
        reasons: list[str],
        overlay_names: tuple[str, ...],
    ) -> IndicatorGuardWarning:
        reason_labels = {
            "overlay_points": "point",
            "overlay_payload_bytes": "payload",
        }
        reason_summary = " and ".join(reason_labels.get(reason, reason) for reason in reasons)
        return IndicatorGuardWarning(
            warning_type="indicator_overlay_suppressed",
            severity="warning",
            indicator_id=runtime_spec.instance_id,
            manifest_type=runtime_spec.manifest_type,
            version=runtime_spec.version,
            title="Overlay emission suppressed",
            message=(
                f"{runtime_spec.instance_id} overlay emission was suppressed after a hard {reason_summary} budget breach."
            ),
            context={
                "overlay_points": int(overlay_points),
                "overlay_payload_bytes": int(overlay_payload_bytes),
                "overlay_points_hard_limit": int(self._guard_config.overlay_points_hard_limit),
                "overlay_payload_hard_limit_bytes": int(self._guard_config.overlay_payload_hard_limit_bytes),
                "suppressed_overlay_names": list(overlay_names),
                "hard_breach_reasons": list(reasons),
            },
        )

    def _topological_order(self) -> tuple[str, ...]:
        edges: Dict[str, set[str]] = defaultdict(set)
        indegree: Dict[str, int] = {indicator_id: 0 for indicator_id in self._runtime_specs_by_id}
        for indicator_id, runtime_spec in self._runtime_specs_by_id.items():
            for dependency in runtime_spec.dependencies:
                if dependency.indicator_id == indicator_id:
                    raise RuntimeError(f"indicator_engine_invalid: self dependency indicator_id={indicator_id}")
                if dependency.indicator_id not in self._runtime_specs_by_id:
                    raise RuntimeError(
                        "indicator_engine_invalid: dependency indicator missing "
                        f"indicator_id={indicator_id} dependency={dependency.indicator_id}"
                    )
                if dependency not in self._output_defs:
                    raise RuntimeError(
                        "indicator_engine_invalid: dependency output missing "
                        f"indicator_id={indicator_id} dependency={dependency.indicator_id}.{dependency.output_name}"
                    )
                if indicator_id not in edges[dependency.indicator_id]:
                    edges[dependency.indicator_id].add(indicator_id)
                    indegree[indicator_id] += 1

        queue = deque(sorted(indicator_id for indicator_id, degree in indegree.items() if degree == 0))
        order: list[str] = []
        while queue:
            current = queue.popleft()
            order.append(current)
            for dependent in sorted(edges.get(current) or ()):
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    queue.append(dependent)
        if len(order) != len(self._runtime_specs_by_id):
            raise RuntimeError("indicator_engine_invalid: dependency cycle detected")
        return tuple(order)


__all__ = ["IndicatorExecutionEngine", "IndicatorGuardConfig"]
