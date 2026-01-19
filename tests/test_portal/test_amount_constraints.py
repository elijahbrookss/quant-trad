"""Tests for amount/quantity constraint resolution and normalization."""

import pytest

from engines.bot_runtime.core.amount_constraints import (
    normalize_qty_with_constraints,
    resolve_amount_constraints,
)


CONFLICTING_INSTRUMENT = {
    "symbol": "CONF-STEP",
    "min_order_size": 1,
    "metadata": {
        "precision": {"amount": 1.0},
        "info": {"base_increment": "1", "qty_step": 0.1},
        "limits": {"amount": {"min": 1.0, "max": 5000.0}},
    },
}


CONSISTENT_INSTRUMENT = {
    "symbol": "OK-STEP",
    "min_order_size": 1,
    "metadata": {
        "precision": {"amount": 1},
        "info": {"base_increment": "0.1"},
        "limits": {"amount": {"min": 1.0, "max": 5000.0}},
    },
}


def test_resolve_amount_constraints_conflict_raises() -> None:
    with pytest.raises(ValueError) as exc_info:
        resolve_amount_constraints(CONFLICTING_INSTRUMENT)
    assert "Conflicting qty_step sources" in str(exc_info.value)


def test_resolve_amount_constraints_uses_base_increment() -> None:
    constraints = resolve_amount_constraints(CONSISTENT_INSTRUMENT)
    assert constraints.qty_step == pytest.approx(0.1)
    assert constraints.min_qty == pytest.approx(1.0)
    assert constraints.max_qty == pytest.approx(5000.0)
    assert constraints.precision == 1
    assert constraints.step_source == "base_increment"


def test_normalize_qty_clamps_and_rounds() -> None:
    constraints = resolve_amount_constraints(CONSISTENT_INSTRUMENT)

    normalized = normalize_qty_with_constraints(constraints, 8.06451613)
    assert normalized.qty_final == pytest.approx(8.0)
    assert normalized.rejected_reason is None

    clamped = normalize_qty_with_constraints(constraints, 6000.0)
    assert clamped.qty_final == pytest.approx(5000.0)
    assert clamped.max_clamped is True

    rejected = normalize_qty_with_constraints(constraints, 0.5)
    assert rejected.qty_final is None
    assert rejected.rejected_reason == "MIN_QTY_NOT_MET"
