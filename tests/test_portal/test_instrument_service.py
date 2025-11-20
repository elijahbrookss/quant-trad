import pytest

pytest.importorskip("sqlalchemy", reason="SQLAlchemy required for instrument service tests")

from portal.backend.service.instrument_service import _tick_from_market


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
