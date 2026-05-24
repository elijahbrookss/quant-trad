from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from indicators.config import IndicatorExecutionContext
from indicators.vwap import VWAPIndicator, VWAPIndicatorDefinition
from matplotlib.patches import Patch


@pytest.fixture
def constant_price_df():
    idx = pd.date_range("2025-01-01", periods=10, freq="D")
    return pd.DataFrame(
        {
            "high": [100] * 10,
            "low": [100] * 10,
            "close": [100] * 10,
            "volume": [1000] * 10,
        },
        index=idx,
    )


def test_definition_builds_compute_request_from_execution_context() -> None:
    resolved = VWAPIndicatorDefinition.resolve_config(
        {
            "stddev_window": 20,
            "stddev_multipliers": [1.0, 2.0],
            "reset_by": "cumulative",
        },
        strict_unknown=True,
    )
    execution_context = IndicatorExecutionContext(
        symbol="CL",
        start="2025-05-15T00:00:00+00:00",
        end="2025-05-30T00:00:00+00:00",
        interval="15m",
    )

    request = VWAPIndicatorDefinition.build_compute_data_request(
        resolved_params=resolved,
        execution_context=execution_context,
    )

    assert request.symbol == "CL"
    assert request.interval == "15m"
    assert resolved["reset_by"] == "cumulative"


def test_compute_constant_price(constant_price_df) -> None:
    ind = VWAPIndicator(
        df=constant_price_df,
        stddev_window=5,
        stddev_multipliers=[1.0, 2.0],
        reset_by="D",
    )
    df = ind.df

    assert (df["vwap"] == 100).all()
    for multiplier in ind.stddev_multipliers:
        assert (df[f"upper_{int(multiplier)}std"].dropna() == 100).all()
        assert (df[f"lower_{int(multiplier)}std"].dropna() == 100).all()


def test_to_overlays_and_legend_handles(constant_price_df) -> None:
    ind = VWAPIndicator(
        df=constant_price_df,
        stddev_window=5,
        stddev_multipliers=[1.0, 2.0],
        reset_by="D",
    )
    overlays, legend_entries = ind.to_overlays(plot_df=constant_price_df)
    expected = 1 + len(ind.stddev_multipliers) * 2

    assert len(overlays) == expected
    labels = {label for label, _ in legend_entries}
    expected_labels = {"VWAP"} | {
        f"VWAP + {multiplier}\u03c3" for multiplier in ind.stddev_multipliers
    } | {
        f"VWAP - {multiplier}\u03c3" for multiplier in ind.stddev_multipliers
    }
    assert labels == expected_labels

    handles = VWAPIndicator.build_legend_handles(legend_entries)
    assert all(isinstance(handle, Patch) for handle in handles)
    assert {handle.get_label() for handle in handles} == expected_labels
