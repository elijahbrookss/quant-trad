from __future__ import annotations

import pytest

from engines.indicator_engine.signal_output import assert_signal_output_event


def test_signal_output_event_accepts_generic_reference_contract() -> None:
    assert_signal_output_event(
        {
            "key": "pivot_breakout_long",
            "direction": "long",
            "metadata": {
                "trigger_price": 5314.0,
                "reference": {
                    "kind": "price_level",
                    "family": "pivot",
                    "name": "R1",
                    "label": "R1",
                    "price": 5312.25,
                    "precision": 2,
                    "source": "daily_floor_pivots",
                    "key": "pivot:2026-03-23:R1",
                    "context": {
                        "session_key": "2026-03-23",
                    },
                },
            },
        }
    )


def test_signal_output_event_rejects_invalid_reference_contract() -> None:
    with pytest.raises(RuntimeError, match="reference.kind required"):
        assert_signal_output_event(
            {
                "key": "pivot_breakout_long",
                "metadata": {
                    "reference": {
                        "price": 5312.25,
                    },
                },
            }
        )
