import pytest
pytest.importorskip("sqlalchemy")
import math
from datetime import date

from portal.backend.service.reports.metrics import (
    compute_expectancy,
    compute_max_drawdown,
    compute_monthly_returns,
    compute_profit_factor,
    compute_sharpe,
)


def test_compute_sharpe():
    returns = [0.01, 0.02, -0.01, 0.03]
    sharpe = compute_sharpe(returns, periods_per_year=252)
    assert sharpe is not None
    assert math.isclose(sharpe, 13.416407864998739, rel_tol=1e-6)


def test_compute_max_drawdown():
    equity = [100, 120, 110, 130, 90]
    pct, abs_dd = compute_max_drawdown(equity)
    assert pct is not None
    assert abs_dd is not None
    assert math.isclose(abs_dd, 40.0, rel_tol=1e-6)
    assert math.isclose(pct, 40.0 / 130.0, rel_tol=1e-6)


def test_compute_profit_factor():
    pnls = [10, -5, 7, -2]
    factor = compute_profit_factor(pnls)
    assert factor is not None
    assert math.isclose(factor, 17.0 / 7.0, rel_tol=1e-6)


def test_compute_expectancy():
    pnls = [10, -5, 7, -2]
    expectancy = compute_expectancy(pnls)
    assert expectancy is not None
    assert math.isclose(expectancy, 2.5, rel_tol=1e-6)


def test_compute_monthly_returns():
    daily_equity = [
        (date(2024, 1, 1), 100.0),
        (date(2024, 1, 31), 110.0),
        (date(2024, 2, 1), 110.0),
        (date(2024, 2, 28), 121.0),
    ]
    monthly = compute_monthly_returns(daily_equity)
    assert monthly == [
        {"month": "2024-01", "return": 0.1},
        {"month": "2024-02", "return": 0.1},
    ]
