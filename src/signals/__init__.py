"""Shared signal contracts and execution helpers."""

from .base import BaseSignal
from .contract import (
    assert_no_execution_fields,
    assert_signal_contract,
    assert_signal_time_is_closed_bar,
)
from .engine.signal_generator import (
    build_signal_overlays,
    describe_indicator_rules,
    run_indicator_rules,
)

__all__ = [
    "BaseSignal",
    "assert_signal_contract",
    "assert_no_execution_fields",
    "assert_signal_time_is_closed_bar",
    "run_indicator_rules",
    "build_signal_overlays",
    "describe_indicator_rules",
]
