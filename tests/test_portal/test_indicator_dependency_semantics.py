from __future__ import annotations

from types import SimpleNamespace

import pytest

from indicators.registry import get_indicator_manifest
from portal.backend.service.indicators.dependency_bindings import (
    assert_indicator_delete_allowed,
    find_indicator_dependents,
    validate_dependency_bindings,
)
from portal.backend.service.indicators.persistence_payload import (
    merge_indicator_payload,
    split_indicator_payload,
)
from portal.backend.service.indicators.output_prefs import typed_outputs_with_prefs


def _ctx_with_records(records):
    return SimpleNamespace(
        repository=SimpleNamespace(get=lambda indicator_id: records.get(indicator_id))
    )


def test_dependency_bindings_require_explicit_matching_instance() -> None:
    manifest = get_indicator_manifest("regime")
    records = {
        "cs-1": {"id": "cs-1", "type": "candle_stats"},
    }

    resolved = validate_dependency_bindings(
        manifest=manifest,
        bindings=[
            {
                "indicator_id": "cs-1",
                "indicator_type": "candle_stats",
                "output_name": "candle_stats",
            }
        ],
        ctx=_ctx_with_records(records),
        indicator_id="regime-1",
    )

    assert resolved == [
        {
            "indicator_id": "cs-1",
            "indicator_type": "candle_stats",
            "output_name": "candle_stats",
        }
    ]

    with pytest.raises(ValueError, match="missing explicit dependency binding"):
        validate_dependency_bindings(
            manifest=manifest,
            bindings=[],
            ctx=_ctx_with_records(records),
            indicator_id="regime-1",
        )

def test_indicator_storage_round_trips_dependencies_outside_public_params() -> None:
    stored = merge_indicator_payload(
        {"days_back": 180},
        [
            {
                "indicator_id": "cs-1",
                "indicator_type": "candle_stats",
                "output_name": "candle_stats",
            }
        ],
    )

    params, dependencies, output_prefs = split_indicator_payload(stored)

    assert params == {"days_back": 180}
    assert dependencies == [
        {
            "indicator_id": "cs-1",
            "indicator_type": "candle_stats",
            "output_name": "candle_stats",
        }
    ]
    assert output_prefs is None


def test_indicator_storage_round_trips_signal_output_prefs() -> None:
    stored = merge_indicator_payload(
        {"days_back": 180},
        [],
        output_prefs={
            "balance_breakout": {"enabled": False},
            "ignored": {"enabled": True},
        },
    )

    params, dependencies, output_prefs = split_indicator_payload(stored)

    assert params == {"days_back": 180}
    assert dependencies == []
    assert output_prefs == {
        "balance_breakout": {"enabled": False},
        "ignored": {"enabled": True},
    }


def test_typed_outputs_with_prefs_marks_only_signal_outputs() -> None:
    manifest = get_indicator_manifest("market_profile")

    typed_outputs, normalized = typed_outputs_with_prefs(
        manifest=manifest,
        output_prefs={
            "balance_breakout": {"enabled": False},
            "not_real": {"enabled": False},
        },
    )

    by_name = {entry["name"]: entry for entry in typed_outputs}

    assert normalized == {"balance_breakout": {"enabled": False}}
    assert by_name["balance_breakout"]["enabled"] is False
    assert by_name["value_area_metrics"]["type"] == "metric"
    assert "enabled" not in by_name["value_area_metrics"]


def test_delete_is_blocked_when_other_indicators_depend_on_target() -> None:
    ctx = SimpleNamespace(
        repository=SimpleNamespace(
            get=lambda indicator_id: None,
            load=lambda: [
                {"id": "cs-1", "name": "Stats A", "type": "candle_stats", "dependencies": []},
                {
                    "id": "regime-1",
                    "name": "Regime A",
                    "type": "regime",
                    "dependencies": [
                        {
                            "indicator_id": "cs-1",
                            "indicator_type": "candle_stats",
                            "output_name": "candle_stats",
                        }
                    ],
                },
            ],
        )
    )

    dependents = find_indicator_dependents(indicator_id="cs-1", ctx=ctx)

    assert dependents == [
        {
            "indicator_id": "regime-1",
            "name": "Regime A",
            "type": "regime",
            "output_name": "candle_stats",
        }
    ]

    with pytest.raises(RuntimeError, match="indicator_delete_blocked"):
        assert_indicator_delete_allowed(indicator_id="cs-1", ctx=ctx)


def test_delete_allows_closure_delete_when_dependent_is_in_same_batch() -> None:
    ctx = SimpleNamespace(
        repository=SimpleNamespace(
            get=lambda indicator_id: None,
            load=lambda: [
                {"id": "cs-1", "name": "Stats A", "type": "candle_stats", "dependencies": []},
                {
                    "id": "regime-1",
                    "name": "Regime A",
                    "type": "regime",
                    "dependencies": [
                        {
                            "indicator_id": "cs-1",
                            "indicator_type": "candle_stats",
                            "output_name": "candle_stats",
                        }
                    ],
                },
            ],
        )
    )

    assert_indicator_delete_allowed(
        indicator_id="cs-1",
        ctx=ctx,
        deleting_ids=["cs-1", "regime-1"],
    )
