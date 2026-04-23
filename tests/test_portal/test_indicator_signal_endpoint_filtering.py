from __future__ import annotations

import pytest

from portal.backend.service.indicators.signal_payload_filtering import (
    enabled_signal_output_names_from_meta,
    filter_signal_payload,
    normalise_enabled_event_keys,
)


def test_enabled_signal_output_names_from_meta_uses_saved_prefs() -> None:
    meta = {
        "typed_outputs": [
            {"name": "balance_breakout", "type": "signal", "enabled": False},
            {"name": "confirmed_balance_breakout", "type": "signal", "enabled": True},
            {"name": "balance_reclaim", "type": "signal"},
            {"name": "value_location", "type": "context"},
            {"name": "balance_retest", "type": "signal"},
        ]
    }

    assert enabled_signal_output_names_from_meta(meta) == {
        "confirmed_balance_breakout",
        "balance_reclaim",
        "balance_retest",
    }


def test_filter_signal_payload_removes_disabled_output_overlays() -> None:
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
        enabled_output_names={"confirmed_balance_breakout"},
        enabled_event_keys=set(),
    )

    assert "signals" not in filtered
    assert "overlays" not in filtered
    assert filtered["machine"]["signals"] == [
        {
            "signal_id": "sig-2",
            "output_name": "confirmed_balance_breakout",
            "event_key": "confirmed_balance_breakout_long",
        }
    ]
    assert filtered["ui"]["overlays"] == [
        {
            "source": "signal",
            "overlay_name": "confirmed_balance_breakout",
            "payload": {"bubbles": [{"signal_id": "sig-2"}]},
        }
    ]
    assert filtered["runtime_invariants"]["signals_count"] == 1
    assert filtered["runtime_invariants"]["signal_overlay_count"] == 1


def test_normalise_enabled_event_keys_keeps_explicit_event_key_filter() -> None:
    assert normalise_enabled_event_keys(
        {"enabled_event_keys": ["balance_breakout_long", "balance_reclaim_long", "balance_retest_long"]}
    ) == {"balance_breakout_long", "balance_reclaim_long", "balance_retest_long"}


def test_filter_signal_payload_requires_canonical_machine_and_ui_sections() -> None:
    with pytest.raises(RuntimeError, match="machine.signals"):
        filter_signal_payload(
            {"runtime_path": "engine_snapshot_v1"},
            enabled_output_names={"balance_breakout"},
            enabled_event_keys=set(),
        )

    with pytest.raises(RuntimeError, match="ui.overlays"):
        filter_signal_payload(
            {
                "machine": {"signals": []},
                "runtime_path": "engine_snapshot_v1",
            },
            enabled_output_names={"balance_breakout"},
            enabled_event_keys=set(),
        )
