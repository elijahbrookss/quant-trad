import pytest

pytest.importorskip("sqlalchemy", reason="SQLAlchemy required for instrument service tests")

from portal.backend.service.market import instrument_service
from portal.backend.service.market.instrument_service import _tick_from_market, instrument_runtime_profile, instrument_runtime_status


def test_tick_from_market_handles_integer_precision():
    market = {"precision": {"price": 5}}
    tick = _tick_from_market(market)
    assert tick == pytest.approx(0.00001)


def test_tick_from_market_handles_decimal_precision():
    market = {"precision": {"price": 0.25}}
    tick = _tick_from_market(market)
    assert tick == pytest.approx(0.25)


def test_tick_from_market_falls_back_to_limits():
    market = {"limits": {"price": {"min": 0.5}}}
    tick = _tick_from_market(market)
    assert tick == pytest.approx(0.5)


def test_spot_proxy_runtime_profile_preserves_source_identity():
    record = {
        "id": "btc-spot",
        "symbol": "BTC/USD",
        "datasource": "CCXT",
        "exchange": "coinbase",
        "instrument_type": "spot",
        "metadata": {
            "instrument_fields": {
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
                    "can_short": True,
                    "short_requires_borrow": False,
                },
            }
        },
    }

    payload = instrument_runtime_profile(record)
    profile = payload["profile"]

    assert payload["runtime_policy"] == "proxy_derivative_v1"
    assert profile["instrument"]["source_instrument_type"] == "spot"
    assert profile["instrument"]["execution_semantics"] == "proxy_derivative"
    assert profile["constraints"]["contract_size"] == 0.01
    assert profile["accounting_mode"] == "margin"


def test_plain_spot_runtime_status_uses_spot_policy(monkeypatch):
    monkeypatch.setattr(instrument_service, "load_instruments", lambda: [])

    record = {
        "id": "eth-spot",
        "symbol": "ETH/USD",
        "instrument_type": "spot",
        "metadata": {
            "instrument_fields": {
                "tick_size": 0.01,
                "contract_size": 1.0,
                "tick_value": 0.01,
                "base_currency": "ETH",
                "quote_currency": "USD",
                "can_short": False,
                "short_requires_borrow": False,
            }
        },
    }

    status = instrument_runtime_status(record)

    assert status["runtime_ready"] is True
    assert status["runtime_policy"] == "spot_v1"
    assert status["execution_semantics"] == "spot"
