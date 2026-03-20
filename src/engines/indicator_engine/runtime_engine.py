"""Deterministic typed-output indicator execution engine."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, Iterable, Mapping

from .contracts import (
    EngineFrame,
    Indicator,
    IndicatorRuntimeSpec,
    OverlayDefinition,
    OutputRef,
    OutputType,
    RuntimeOverlay,
    RuntimeOutput,
    output_ref_key,
    validate_overlay_definitions,
    validate_runtime_overlay,
    validate_output_definitions,
    validate_runtime_output,
)


class IndicatorExecutionEngine:
    def __init__(self, indicators: Iterable[Indicator]) -> None:
        self._indicators_by_id: Dict[str, Indicator] = {}
        self._runtime_specs_by_id: Dict[str, IndicatorRuntimeSpec] = {}
        self._output_defs: Dict[OutputRef, OutputType] = {}
        self._overlay_defs: Dict[str, Dict[str, OverlayDefinition]] = {}
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
            self._indicators_by_id[indicator_id] = indicator
            self._runtime_specs_by_id[indicator_id] = runtime_spec
            self._overlay_defs[indicator_id] = {
                overlay.name: overlay for overlay in runtime_spec.overlays
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
        for indicator_id in self._order:
            indicator = self._indicators_by_id[indicator_id]
            runtime_spec = self._runtime_specs_by_id[indicator_id]
            inputs = {ref: by_ref[ref] for ref in runtime_spec.dependencies}
            indicator.apply_bar(bar, inputs)
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
            if include_overlays:
                overlays = dict(indicator.overlay_snapshot())
                declared_overlays = self._overlay_defs[indicator_id]
                if set(overlays.keys()) != set(declared_overlays.keys()):
                    raise RuntimeError(
                        "indicator_overlay_invalid: overlay presence mismatch "
                        f"indicator_id={indicator_id} declared={sorted(declared_overlays.keys())} returned={sorted(overlays.keys())}"
                    )
                for overlay_name, definition in declared_overlays.items():
                    runtime_overlay = overlays[overlay_name]
                    validate_runtime_overlay(
                        definition=definition,
                        overlay=runtime_overlay,
                        bar_time=bar_time,
                        dependency_inputs=inputs,
                    )
                    flat_overlays[output_ref_key(indicator_id, overlay_name)] = runtime_overlay.copy()
        return EngineFrame(outputs=flat, overlays=flat_overlays)

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


__all__ = ["IndicatorExecutionEngine"]
