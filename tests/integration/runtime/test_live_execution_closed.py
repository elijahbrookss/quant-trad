from __future__ import annotations

from typing import cast

import pytest

from engines.bot_runtime.adapters.live import LiveAdapter
from engines.bot_runtime.core.execution_adapter import ExecutionAdapter
from engines.bot_runtime.core.execution_order import FillOrder
from engines.bot_runtime.strategy.series_builder_parts.series_construction import (
    SeriesBuilderConstructionMixin,
)


class _ExternalExecutor(ExecutionAdapter):
    def __init__(self) -> None:
        self.called = False

    def execute_order(self, order):  # noqa: ANN001 - test double
        del order
        self.called = True
        raise AssertionError("external executor must never be reached")


def test_live_adapter_rejects_injected_external_executors() -> None:
    executor = _ExternalExecutor()

    with pytest.raises(ValueError, match="external order submission is closed"):
        LiveAdapter(short_requires_borrow=False, derivatives_adapter=executor)

    assert executor.called is False


def test_live_series_composition_stays_fail_closed_even_with_injected_config() -> None:
    executor = _ExternalExecutor()
    builder = SeriesBuilderConstructionMixin()
    builder.run_type = "live"
    builder.config = {
        "spot_execution_adapter": executor,
        "derivatives_execution_adapter": executor,
    }

    adapter = builder._adapter_for_run_type(
        short_requires_borrow=False,
        tick_size=1.0,
        qty_step=None,
        min_qty=None,
        min_notional=None,
        contract_size=1.0,
        execution_assumptions=cast(object, None),
        execution_context=cast(object, None),
    )

    with pytest.raises(RuntimeError, match="external order submission is closed"):
        adapter.execute_order(cast(FillOrder, object()))

    assert executor.called is False
