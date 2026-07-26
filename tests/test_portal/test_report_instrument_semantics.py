from __future__ import annotations

import copy

import pytest

from portal.backend.service.reports.instrument_semantics import (
    merge_fill_instrument_semantics,
)


def _configured_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "instrument_id": "instrument-btc",
        "symbol": "BTC",
        "instrument_type": "spot",
        "source_instrument_type": "spot",
        "execution_semantics": None,
        "research_market_role": None,
        "accounting_mode": None,
        "margin_calc_type": None,
    }
    row.update(overrides)
    return row


def test_spot_fill_completes_missing_report_semantics_without_mutating_input() -> None:
    configured = [_configured_row()]
    original = copy.deepcopy(configured)

    observed = merge_fill_instrument_semantics(
        configured,
        [
            {
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "accounting_mode": "spot",
            }
        ],
    )

    assert configured == original
    assert observed[0]["accounting_mode"] == "spot"
    assert observed[0]["execution_semantics"] == "spot"


def test_margin_fill_does_not_invent_derivative_execution_semantics() -> None:
    observed = merge_fill_instrument_semantics(
        [_configured_row(instrument_type="future", source_instrument_type="future")],
        [
            {
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "accounting_mode": "margin",
            }
        ],
    )

    assert observed[0]["accounting_mode"] == "margin"
    assert observed[0]["execution_semantics"] is None


def test_conflicting_configured_and_fill_semantics_fail_loudly() -> None:
    with pytest.raises(ValueError, match="conflicting report execution_semantics"):
        merge_fill_instrument_semantics(
            [
                _configured_row(
                    instrument_type="future",
                    source_instrument_type="future",
                    execution_semantics="derivative",
                    accounting_mode="margin",
                )
            ],
            [
                {
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "accounting_mode": "spot",
                }
            ],
        )


def test_ambiguous_symbol_only_fill_identity_fails_loudly() -> None:
    with pytest.raises(ValueError, match="ambiguous report instrument metadata"):
        merge_fill_instrument_semantics(
            [
                _configured_row(instrument_id="instrument-btc-1"),
                _configured_row(instrument_id="instrument-btc-2"),
            ],
            [{"symbol": "BTC", "accounting_mode": "spot"}],
        )


def test_fill_event_order_does_not_change_instrument_semantics_order() -> None:
    fills = [
        {
            "instrument_id": "instrument-eth",
            "symbol": "ETH",
            "accounting_mode": "spot",
        },
        {
            "instrument_id": "instrument-btc",
            "symbol": "BTC",
            "accounting_mode": "spot",
        },
    ]

    assert merge_fill_instrument_semantics([], fills) == (
        merge_fill_instrument_semantics([], list(reversed(fills)))
    )


def test_distinct_instrument_ids_may_share_a_symbol() -> None:
    observed = merge_fill_instrument_semantics(
        [],
        [
            {
                "instrument_id": "instrument-btc-spot",
                "symbol": "BTC",
                "accounting_mode": "spot",
            },
            {
                "instrument_id": "instrument-btc-future",
                "symbol": "BTC",
                "accounting_mode": "margin",
                "execution_semantics": "derivative",
            },
        ],
    )

    assert [row["instrument_id"] for row in observed] == [
        "instrument-btc-future",
        "instrument-btc-spot",
    ]


def test_untyped_fill_execution_semantics_cannot_change_report_identity() -> None:
    observed = merge_fill_instrument_semantics(
        [
            _configured_row(
                instrument_type="future",
                source_instrument_type="future",
                execution_semantics="derivative",
                accounting_mode="margin",
            )
        ],
        [
            {
                "instrument_id": "instrument-btc",
                "symbol": "BTC",
                "accounting_mode": "margin",
                "execution_semantics": "spot",
            }
        ],
    )

    assert observed[0]["accounting_mode"] == "margin"
    assert observed[0]["execution_semantics"] == "derivative"


def test_zero_fill_run_rejects_invalid_configured_semantic_pair() -> None:
    with pytest.raises(ValueError, match="conflicting report instrument semantics"):
        merge_fill_instrument_semantics(
            [
                _configured_row(
                    execution_semantics="derivative",
                    accounting_mode="spot",
                )
            ],
            [],
        )


def test_duplicate_configured_identity_rejects_conflicting_semantics() -> None:
    with pytest.raises(
        ValueError,
        match="conflicting configured report execution_semantics",
    ):
        merge_fill_instrument_semantics(
            [
                _configured_row(execution_semantics="spot"),
                _configured_row(execution_semantics="derivative"),
            ],
            [],
        )
