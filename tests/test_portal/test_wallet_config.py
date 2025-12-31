import pytest

from portal.backend.service.bot_service import _validate_wallet_config


def test_wallet_config_requires_balances():
    with pytest.raises(ValueError, match="wallet_config is required"):
        _validate_wallet_config(None)

    with pytest.raises(ValueError, match="wallet_config.balances"):
        _validate_wallet_config({})


def test_wallet_config_requires_minimum():
    with pytest.raises(ValueError, match="sum to at least"):
        _validate_wallet_config({"balances": {"USDC": 5}})


def test_wallet_config_normalizes_balances():
    config = _validate_wallet_config({"balances": {"usdc": 25, "btc": 0}})
    assert config["balances"]["USDC"] == 25.0
    assert config["balances"]["BTC"] == 0.0
