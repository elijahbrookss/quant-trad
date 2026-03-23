"""Typed-output indicator engine exports."""

from .contracts import (
    EngineFrame,
    Indicator,
    IndicatorRuntimeSpec,
    OverlayDefinition,
    OutputDefinition,
    OutputRef,
    OutputType,
    RuntimeOverlay,
    RuntimeOutput,
    validate_overlay_definitions,
    validate_runtime_overlay,
    validate_output_definitions,
    validate_runtime_output,
)
from .runtime_engine import IndicatorExecutionEngine
from .signal_output import (
    SignalOutputEvent,
    assert_signal_output_event,
    assert_signal_output_has_no_execution_fields,
    signal_output_known_at_epoch,
)

__all__ = [
    "EngineFrame",
    "Indicator",
    "IndicatorRuntimeSpec",
    "OverlayDefinition",
    "OutputDefinition",
    "OutputRef",
    "OutputType",
    "RuntimeOverlay",
    "RuntimeOutput",
    "IndicatorExecutionEngine",
    "validate_overlay_definitions",
    "validate_runtime_overlay",
    "validate_output_definitions",
    "validate_runtime_output",
    "SignalOutputEvent",
    "assert_signal_output_event",
    "assert_signal_output_has_no_execution_fields",
    "signal_output_known_at_epoch",
]
