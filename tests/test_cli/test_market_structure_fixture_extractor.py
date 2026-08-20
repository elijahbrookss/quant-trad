from __future__ import annotations

import json

import pytest

from scripts.reporting.extract_coinbase_market_structure_fixtures import (
    _assert_no_sensitive_keys,
    _select_frames,
)


def _row(product_id: str, payload: dict) -> dict:
    return {
        "requested_product_id": product_id,
        "raw_frame": json.dumps(payload, separators=(",", ":")).encode("utf-8"),
    }


def test_fixture_selector_classifies_trade_update_and_zero_quantity_delete() -> None:
    rows = [
        _row(
            "BIP-20DEC30-CDE",
            {
                "channel": "market_trades",
                "events": [
                    {
                        "type": "update",
                        "trades": [
                            {
                                "trade_id": "1",
                                "product_id": "BIP-20DEC30-CDE",
                                "price": "100000",
                                "size": "2",
                                "side": "BUY",
                            }
                        ],
                    }
                ],
            },
        ),
        _row(
            "BIP-20DEC30-CDE",
            {
                "channel": "l2_data",
                "events": [
                    {
                        "type": "update",
                        "product_id": "BIP-20DEC30-CDE",
                        "updates": [
                            {
                                "side": "bid",
                                "price_level": "100000",
                                "new_quantity": "0",
                            }
                        ],
                    }
                ],
            },
        ),
    ]

    selected = _select_frames(rows)

    assert set(selected) == {
        "bip_market_trades_update",
        "bip_level2_zero_delete",
    }


def test_fixture_sensitive_key_scan_fails_before_publication() -> None:
    with pytest.raises(ValueError, match="Sensitive key .*jwt"):
        _assert_no_sensitive_keys({"events": [{"jwt": "must-not-publish"}]})
    with pytest.raises(ValueError, match="Sensitive key .*access_token"):
        _assert_no_sensitive_keys({"events": [{"access_token": "must-not-publish"}]})
    with pytest.raises(ValueError, match="Credential-shaped value"):
        _assert_no_sensitive_keys({"events": [{"message": "Bearer must-not-publish"}]})
