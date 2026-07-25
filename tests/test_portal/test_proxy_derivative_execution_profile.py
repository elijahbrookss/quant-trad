from __future__ import annotations

from engines.bot_runtime.core.execution_profile import compile_series_execution_profile


def test_spot_source_can_compile_as_proxy_derivative() -> None:
    profile = compile_series_execution_profile(
        {
            "id": "btc-spot",
            "symbol": "BTC/USD",
            "instrument_type": "spot",
            "tick_size": 0.01,
            "contract_size": 1.0,
            "tick_value": 0.01,
            "base_currency": "BTC",
            "quote_currency": "USD",
            "can_short": False,
            "short_requires_borrow": False,
            "proxy_derivative_margin_rates": {
                "intraday": {"long_margin_rate": 0.1, "short_margin_rate": 0.1},
                "overnight": {"long_margin_rate": 0.25, "short_margin_rate": 0.3},
            },
            "proxy_derivative_instrument_fields": {
                "tick_size": 5.0,
                "contract_size": 0.01,
                "tick_value": 0.05,
                "min_order_size": 1.0,
                "qty_step": 1.0,
                "min_notional": None,
                "can_short": True,
                "short_requires_borrow": False,
            },
        },
        require_margin_accounting=True,
        execution_semantics="proxy_derivative",
    )

    assert profile.instrument.instrument_type == "spot"
    assert profile.instrument.source_instrument_type == "spot"
    assert profile.instrument.execution_semantics == "proxy_derivative"
    assert profile.instrument.research_market_role == "proxy_underlier"
    assert profile.is_derivatives() is True
    assert profile.capabilities.supports_short is True
    assert profile.capabilities.short_requires_borrow is False
    assert profile.margin_calc_type == "margin"
    assert profile.margin_rates is not None
    assert profile.margin_rates.overnight_short == 0.3
    assert profile.constraints.tick_size == 5.0
    assert profile.constraints.contract_size == 0.01
    assert profile.constraints.tick_value == 0.05
    assert profile.constraints.min_order_size == 1.0
    assert profile.constraints.qty_step == 1.0
    assert profile.constraints.min_notional is None
    assert profile.validated_for_runtime is True


def test_proxy_derivative_requires_explicit_margin_rates() -> None:
    try:
        compile_series_execution_profile(
            {
                "id": "btc-spot",
                "symbol": "BTC/USD",
                "instrument_type": "spot",
                "tick_size": 0.01,
                "contract_size": 1.0,
                "tick_value": 0.01,
                "base_currency": "BTC",
                "quote_currency": "USD",
                "can_short": False,
                "short_requires_borrow": False,
                "proxy_derivative_instrument_fields": {
                    "tick_size": 5.0,
                    "contract_size": 0.01,
                    "tick_value": 0.05,
                    "min_order_size": 1.0,
                    "qty_step": 1.0,
                    "can_short": True,
                    "short_requires_borrow": False,
                },
            },
            require_margin_accounting=True,
            execution_semantics="proxy_derivative",
        )
    except ValueError as exc:
        assert "proxy_derivative_margin_rates required" in str(exc)
    else:
        raise AssertionError("proxy_derivative execution should require explicit margin rates")


def test_proxy_derivative_requires_explicit_execution_fields() -> None:
    try:
        compile_series_execution_profile(
            {
                "id": "btc-spot",
                "symbol": "BTC/USD",
                "instrument_type": "spot",
                "tick_size": 0.01,
                "contract_size": 1.0,
                "tick_value": 0.01,
                "base_currency": "BTC",
                "quote_currency": "USD",
                "can_short": False,
                "short_requires_borrow": False,
                "proxy_derivative_margin_rates": {
                    "intraday": {"long_margin_rate": 0.1, "short_margin_rate": 0.1},
                    "overnight": {"long_margin_rate": 0.25, "short_margin_rate": 0.3},
                },
            },
            require_margin_accounting=True,
            execution_semantics="proxy_derivative",
        )
    except ValueError as exc:
        assert "proxy_derivative_instrument_fields required" in str(exc)
    else:
        raise AssertionError("proxy_derivative execution should require explicit execution fields")
