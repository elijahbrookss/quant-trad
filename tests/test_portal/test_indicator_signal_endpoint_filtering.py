from __future__ import annotations

import pytest

from portal.backend.service.indicators.signal_payload_filtering import (
    filter_signal_payload,
    normalise_signal_event_keys,
    normalise_signal_output_names,
)


def test_filter_signal_payload_keeps_all_outputs_without_request_filter() -> None:
    payload = {
        "machine": {
            "signals": [
                {
                    "signal_id": "sig-1",
                    "output_name": "balance_breakout",
                    "event_key": "balance_breakout_long",
                },
                {
                    "signal_id": "sig-2",
                    "output_name": "confirmed_balance_breakout",
                    "event_key": "confirmed_balance_breakout_long",
                },
            ],
        },
        "ui": {
            "overlays": [
                {
                    "source": "signal",
                    "overlay_name": "balance_breakout",
                    "payload": {"bubbles": [{"signal_id": "sig-1"}]},
                },
                {
                    "source": "signal",
                    "overlay_name": "confirmed_balance_breakout",
                    "payload": {"bubbles": [{"signal_id": "sig-2"}]},
                },
            ],
        },
        "runtime_invariants": {
            "signals_count": 2,
            "signal_overlay_count": 2,
        },
    }

    filtered = filter_signal_payload(
        payload,
        output_names=None,
        event_keys=set(),
    )

    assert "signals" not in filtered
    assert "overlays" not in filtered
    assert len(filtered["machine"]["signals"]) == 2
    assert len(filtered["ui"]["overlays"]) == 2
    assert filtered["runtime_invariants"]["signals_count"] == 2
    assert filtered["runtime_invariants"]["signal_overlay_count"] == 2


def test_filter_signal_payload_applies_explicit_output_filter() -> None:
    payload = {
        "machine": {
            "signals": [
                {"signal_id": "sig-1", "output_name": "balance_breakout", "event_key": "balance_breakout_long"},
                {
                    "signal_id": "sig-2",
                    "output_name": "confirmed_balance_breakout",
                    "event_key": "confirmed_balance_breakout_long",
                },
            ],
        },
        "ui": {
            "overlays": [
                {"source": "signal", "overlay_name": "balance_breakout", "payload": {"bubbles": [{"signal_id": "sig-1"}]}},
                {
                    "source": "signal",
                    "overlay_name": "confirmed_balance_breakout",
                    "payload": {"bubbles": [{"signal_id": "sig-2"}]},
                },
            ],
        },
        "runtime_invariants": {"signals_count": 2, "signal_overlay_count": 2},
    }

    filtered = filter_signal_payload(
        payload,
        output_names={"confirmed_balance_breakout"},
        event_keys=set(),
    )

    assert filtered["machine"]["signals"] == [
        {"signal_id": "sig-2", "output_name": "confirmed_balance_breakout", "event_key": "confirmed_balance_breakout_long"}
    ]
    assert filtered["ui"]["overlays"] == [
        {"source": "signal", "overlay_name": "confirmed_balance_breakout", "payload": {"bubbles": [{"signal_id": "sig-2"}]}}
    ]
    assert filtered["runtime_invariants"]["signals_count"] == 1
    assert filtered["runtime_invariants"]["signal_overlay_count"] == 1


def test_normalise_signal_filters_keep_explicit_request_filters() -> None:
    assert normalise_signal_output_names({"output_names": ["balance_breakout", "balance_retest"]}) == {
        "balance_breakout",
        "balance_retest",
    }
    assert normalise_signal_event_keys(
        {"event_keys": ["balance_breakout_long", "balance_reclaim_long", "balance_retest_long"]}
    ) == {"balance_breakout_long", "balance_reclaim_long", "balance_retest_long"}


def test_filter_signal_payload_requires_canonical_machine_and_ui_sections() -> None:
    with pytest.raises(RuntimeError, match="machine.signals"):
        filter_signal_payload(
            {"runtime_path": "engine_snapshot.v1"},
            output_names={"balance_breakout"},
            event_keys=set(),
        )

    with pytest.raises(RuntimeError, match="ui.overlays"):
        filter_signal_payload(
            {
                "machine": {"signals": []},
                "runtime_path": "engine_snapshot.v1",
            },
            output_names={"balance_breakout"},
            event_keys=set(),
        )
