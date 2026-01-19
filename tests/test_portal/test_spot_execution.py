import pytest

from engines.bot_runtime.core.execution import (
    FillRejection,
    SpotExecutionConstraints,
    SpotExecutionModel,
)


def test_spot_execution_rejects_qty_rounding_to_zero():
    constraints = SpotExecutionConstraints(
        tick_size=0.01,
        qty_step=0.0001,
        min_qty=0.0001,
        min_notional=5.0,
    )
    model = SpotExecutionModel(constraints)
    result, rejection = model.fill_market(
        side="buy",
        requested_qty=0.00005,
        price=30000.0,
        fee_rate=0.001,
    )
    assert result is None
    assert isinstance(rejection, FillRejection)
    assert rejection.reason == "QTY_ROUNDS_TO_ZERO"


def test_spot_execution_fills_valid_qty_with_fees():
    constraints = SpotExecutionConstraints(
        tick_size=0.01,
        qty_step=0.001,
        min_qty=0.001,
        min_notional=5.0,
    )
    model = SpotExecutionModel(constraints)
    result, rejection = model.fill_market(
        side="buy",
        requested_qty=0.02,
        price=2000.0,
        fee_rate=0.001,
    )
    assert rejection is None
    assert result is not None
    assert result.filled_qty == pytest.approx(0.02)
    assert result.notional == pytest.approx(40.0)
    assert result.fee == pytest.approx(0.04)


def test_spot_execution_rejects_min_notional():
    constraints = SpotExecutionConstraints(
        tick_size=0.01,
        qty_step=0.1,
        min_qty=0.1,
        min_notional=10.0,
    )
    model = SpotExecutionModel(constraints)
    result, rejection = model.fill_market(
        side="buy",
        requested_qty=0.1,
        price=50.0,
        fee_rate=0.001,
    )
    assert result is None
    assert isinstance(rejection, FillRejection)
    assert rejection.reason == "MIN_NOTIONAL_NOT_MET"
