from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from market_data.contracts import NumericFact, NumericFactRecord, SourceIdentity
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan

from portal.backend.service.research.event_fact_evaluator import (
    EventFactEvaluator,
    normalize_event_fact_configuration,
)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _record(index: int, *, value: str, known_at: datetime) -> NumericFactRecord:
    source = SourceIdentity(
        provider="REFERENCE_PROVIDER",
        venue="PUBLIC_NETWORK",
        source_kind="public_contract",
        adapter_version="reference_adapter.v1",
    )
    fact = NumericFact(
        fact_type="market.reference_price",
        contract_version="market.reference_price.v1",
        value=Decimal(value),
        raw_value=value,
        unit="USD",
        dimensions={"quote_currency": "USD"},
        effective_at=known_at - timedelta(seconds=5),
        effective_at_method="source_timestamp",
        accepted_at=known_at,
        known_at=known_at,
        known_at_method="source_confirmation",
        source_event_key=f"event-{index}",
    )
    return NumericFactRecord(
        series_id=2,
        revision=1,
        market_commit_seq=index + 1,
        ingestion_run_id="ingest-1",
        source_identity_key=source.identity_key,
        source=source,
        provenance={"round": index},
        fact=fact,
    )


def _configuration():
    return normalize_event_fact_configuration(
        detector={
            "type": "indicator_event",
            "output_name": "balance_breakout",
            "event_keys": [
                {"key": "balance_breakout_long", "direction": "long"},
                {"key": "balance_breakout_short", "direction": "short"},
            ],
        },
        outcomes={
            "horizons": [1, 2],
            "horizon_kind": "bars",
            "primary_horizon": 1,
        },
        statistics={
            "features": {
                "baseline": [
                    {"name": "direction", "operator": "event_direction"},
                    {
                        "name": "distance_bps",
                        "operator": "event_metadata_number",
                        "path": "metadata.distance_from_reference_pct",
                        "scale": 10000,
                    },
                ],
                "enriched": [
                    {
                        "name": "reference_value",
                        "operator": "latest_value",
                        "input_alias": "reference_price",
                    },
                    {
                        "name": "basis_bps",
                        "operator": "venue_basis_bps",
                        "input_alias": "reference_price",
                    },
                    {
                        "name": "updates_6h",
                        "operator": "update_count_window",
                        "input_alias": "reference_price",
                        "window_seconds": 21600,
                    },
                ],
            },
            "eligibility": {"min_samples": 1},
        },
    )


def _plan(*, gap_policy: str, gaps=()) -> ResolvedCheckPlan:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return ResolvedCheckPlan(
        schema_version=CHECK_PLAN_SCHEMA_VERSION,
        request_hash="request-hash",
        market_data_requirements=(),
        indicator_graph=(),
        evaluation_range={"start": _iso(start), "end_exclusive": _iso(start + timedelta(hours=4))},
        materialization_range={"start": _iso(start), "end_exclusive": _iso(start + timedelta(hours=6))},
        warmup={"bars": 14, "seconds": 50400, "timeframe_seconds": 3600},
        outcome_tail={"horizons": [1, 2], "horizon_kind": "bars", "bars": 2, "seconds": 7200},
        gap_policy=gap_policy,
        quality_evidence=tuple(gaps),
    )


def _inputs():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    detector, outcomes, statistics = _configuration()
    candles = []
    for index, close in enumerate([100.0, 102.0, 101.0, 103.0, 104.0, 105.0]):
        open_time = start + timedelta(hours=index)
        candles.append(
            {
                "time": _iso(open_time),
                "open_time": _iso(open_time),
                "close_time": _iso(open_time + timedelta(hours=1)),
                "known_at": _iso(open_time + timedelta(hours=1)),
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 10 + index,
            }
        )
    events = [
        {
            "time": _iso(start + timedelta(hours=1)),
            "bar_index": 1,
            "indicator_id": "profile-1",
            "output_name": "balance_breakout",
            "output_type": "signal",
            "event_key": "balance_breakout_long",
            "event": {
                "key": "balance_breakout_long",
                "direction": "long",
                "metadata": {"distance_from_reference_pct": 0.01},
            },
        },
        {
            "time": _iso(start + timedelta(hours=3)),
            "bar_index": 3,
            "indicator_id": "profile-1",
            "output_name": "balance_breakout",
            "output_type": "signal",
            "event_key": "balance_breakout_short",
            "event": {
                "key": "balance_breakout_short",
                "direction": "short",
                "metadata": {"distance_from_reference_pct": 0.02},
            },
        },
    ]
    records = [
        _record(1, value="99", known_at=start + timedelta(minutes=30)),
        _record(2, value="101", known_at=start + timedelta(hours=1, minutes=30)),
        _record(3, value="102", known_at=start + timedelta(hours=3, minutes=30)),
    ]
    return {
        "detector": detector,
        "outcomes": outcomes,
        "statistics": statistics,
        "indicator_evidence": {"candles": candles, "outputs": events},
        "fact_records_by_alias": {"reference_price": records},
        "fact_requirements_by_alias": {
            "reference_price": {"max_staleness_seconds": 21600}
        },
        "data_quality": {"status": "clean"},
    }


def test_event_fact_check_uses_indicator_events_causal_facts_and_exact_outcomes() -> None:
    result = EventFactEvaluator().evaluate(
        plan=_plan(gap_policy="continue_degraded"), inputs=_inputs()
    )

    assert result["candidate_count"] == 2
    assert result["sample_count"] == 2
    assert result["direction_counts"] == {"long": 1, "short": 1}
    assert result["events"][0]["decision_time"] == "2026-01-01T02:00:00.000000Z"
    assert result["events"][0]["features"]["reference_value"] == 101.0
    assert result["events"][0]["features"]["basis_bps"] == pytest.approx(
        (102.0 / 101.0 - 1.0) * 10000.0
    )
    assert result["outcome_resolution"]["1"]["resolved_count"] == 2
    assert result["outcome_resolution"]["2"]["resolved_count"] == 2
    assert len(result["hashes"]["selected_facts_hash"]) == 64
    assert result["event_ownership"] == "indicator"


def test_event_fact_check_preserves_unresolved_horizon_reason() -> None:
    inputs = _inputs()
    inputs["indicator_evidence"]["candles"] = [
        row
        for row in inputs["indicator_evidence"]["candles"]
        if row["open_time"] != "2026-01-01T02:00:00Z"
    ]

    result = EventFactEvaluator().evaluate(
        plan=_plan(gap_policy="continue_degraded"), inputs=inputs
    )

    horizon = result["outcome_resolution"]["1"]
    assert horizon["unresolved_count"] == 1
    assert horizon["unresolved_reasons"] == {"target_bar_missing": 1}


@pytest.mark.parametrize(
    ("policy", "status", "action"),
    [
        ("reject", "blocked", "rejected_before_event_emission"),
        ("reset_rewarm", "completed", "reset_and_rewarm"),
        (
            "continue_degraded",
            "completed",
            "continued_with_degraded_status",
        ),
    ],
)
def test_event_fact_check_gap_policies_are_explicit(
    policy: str, status: str, action: str
) -> None:
    inputs = _inputs()
    if policy == "reject":
        inputs["indicator_gap_rejection"] = {
            "policy": "reject",
            "gap_start": "2026-01-01T02:00:00Z",
            "gap_end": "2026-01-01T03:00:00Z",
        }
    else:
        inputs["indicator_evidence"]["gap_transitions"] = [
            {
                "gap_start": "2026-01-01T02:00:00Z",
                "gap_end": "2026-01-01T03:00:00Z",
                "actions": [
                    {
                        "indicator_id": "profile-1",
                        "action": (
                            "reset_and_rewarm"
                            if policy == "reset_rewarm"
                            else "continued_degraded"
                        ),
                    }
                ],
            }
        ]
    result = EventFactEvaluator().evaluate(
        plan=_plan(gap_policy=policy),
        inputs=inputs,
    )

    assert result["status"] == status
    assert result["gap_decision"]["indicator_action"] == action


def test_event_fact_check_rejects_non_indicator_signal_rows() -> None:
    inputs = _inputs()
    inputs["indicator_evidence"]["outputs"][0]["output_type"] = "metric"

    with pytest.raises(RuntimeError, match="event_ownership_invalid"):
        EventFactEvaluator().evaluate(
            plan=_plan(gap_policy="continue_degraded"), inputs=inputs
        )


def test_event_fact_check_rejects_direction_substitution() -> None:
    inputs = _inputs()
    inputs["indicator_evidence"]["outputs"][0]["event"]["direction"] = "short"

    with pytest.raises(RuntimeError, match="event_direction_mismatch"):
        EventFactEvaluator().evaluate(
            plan=_plan(gap_policy="continue_degraded"), inputs=inputs
        )
