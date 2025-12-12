from engines.backtest.orders import OrderTemplateBuilder
from engines.backtest.risk_engine import DEFAULT_RISK


def test_order_builder_respects_size_fraction_and_contracts():
    builder = OrderTemplateBuilder(
        {
            "contracts": 4,
            "take_profit_orders": [
                {"ticks": 10, "size_fraction": 0.5},
                {"ticks": 20, "contracts": 1},
            ],
        },
        DEFAULT_RISK,
    )

    orders = builder.build_orders()
    assert orders[0]["contracts"] == 2
    assert orders[1]["contracts"] == 1


def test_order_builder_scales_to_total_contracts():
    builder = OrderTemplateBuilder({"targets": [10, 20]}, DEFAULT_RISK)

    scaled_orders = builder.with_total_contracts(5)

    assert len(scaled_orders) == 2
    assert sum(order["contracts"] for order in scaled_orders) == 5
