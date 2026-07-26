from __future__ import annotations

from copy import deepcopy

import pytest

from portal.backend.service.reports.candle_continuity import (
    classify_unknown_gaps_from_provider_evidence,
)


def _provider_gap(**values):
    return {"classification": "provider_missing_data", **values}


def test_classification_preserves_gap_order_and_uses_first_covering_evidence() -> None:
    gaps = [
        {"gap_id": "known", "classification": "runtime_missing"},
        {
            "gap_id": "covered",
            "previous_ts": "2026-03-06T21:00:00Z",
            "current_ts": "2026-03-06T23:00:00Z",
            "classification": "unknown_gap",
            "expected_interval_seconds": 3600,
            "missing_candle_estimate": 1,
        },
        {
            "gap_id": "uncovered",
            "start": "2026-03-07T04:00:00Z",
            "end": "2026-03-07T05:00:00Z",
            "classification": "unknown_gap",
        },
    ]
    original = deepcopy(gaps)
    provider_evidence = [
        _provider_gap(
            start="2026-03-06T22:00:00Z",
            end="2026-03-06T23:00:00Z",
            metadata={
                "reason_code": "first_covering_reason",
                "evidence": "first_covering_evidence",
                "provider_evidence": {"provider_message": "first"},
            },
        ),
        _provider_gap(
            start="2026-03-06T20:00:00Z",
            end="2026-03-07T00:00:00Z",
            metadata={
                "reason_code": "second_covering_reason",
                "evidence": "second_covering_evidence",
            },
        ),
    ]

    result = classify_unknown_gaps_from_provider_evidence(gaps, provider_evidence)

    assert [gap["gap_id"] for gap in result] == ["known", "covered", "uncovered"]
    assert result[0] == gaps[0]
    assert result[1] == {
        **gaps[1],
        "classification": "provider_missing_data",
        "reason_code": "first_covering_reason",
        "evidence": "first_covering_evidence",
        "provider_gap_start": "2026-03-06T22:00:00Z",
        "provider_gap_end": "2026-03-06T23:00:00Z",
        "provider_evidence": {"provider_message": "first"},
    }
    assert result[2] == gaps[2]
    assert gaps == original


def test_classification_uses_explicit_missing_window_and_default_evidence() -> None:
    gaps = [
        {
            "missing_start": "2026-03-06T22:00:00Z",
            "missing_end": "2026-03-06T23:00:00Z",
        }
    ]
    provider_evidence = [
        _provider_gap(
            start="2026-03-06T22:00:00Z",
            end="2026-03-06T23:00:00Z",
            metadata={},
        )
    ]

    result = classify_unknown_gaps_from_provider_evidence(gaps, provider_evidence)

    assert result == [
        {
            **gaps[0],
            "classification": "provider_missing_data",
            "reason_code": "source_sparse",
            "evidence": "canonical_provider_gap_evidence",
            "provider_gap_start": "2026-03-06T22:00:00Z",
            "provider_gap_end": "2026-03-06T23:00:00Z",
        }
    ]


def test_classification_leaves_malformed_and_partially_covered_gaps_unknown() -> None:
    gaps = [
        None,
        {
            "gap_id": "malformed",
            "previous_ts": "not-a-timestamp",
            "current_ts": "2026-03-06T23:00:00Z",
            "classification": "unknown_gap",
            "expected_interval_seconds": 3600,
        },
        {
            "gap_id": "partial",
            "start": "2026-03-06T22:00:00Z",
            "end": "2026-03-06T23:00:00Z",
            "classification": "unknown_gap",
        },
    ]
    provider_evidence = [
        _provider_gap(
            start="2026-03-06T22:00:00Z",
            end="2026-03-06T22:59:59Z",
        )
    ]

    result = classify_unknown_gaps_from_provider_evidence(gaps, provider_evidence)

    assert result == [gaps[1], gaps[2]]


def test_classification_preserves_alias_provider_gap_payload_shape() -> None:
    gaps = [
        {"start_ts": "2026-03-06T22:00:00Z", "end_ts": "2026-03-06T23:00:00Z"}
    ]
    provider_evidence = [
        _provider_gap(
            start_ts="2026-03-06T22:00:00Z",
            end_ts="2026-03-06T23:00:00Z",
            metadata={"reason_code": "provider_response_empty"},
        )
    ]

    result = classify_unknown_gaps_from_provider_evidence(gaps, provider_evidence)

    assert result == [
        {
            **gaps[0],
            "classification": "provider_missing_data",
            "reason_code": "provider_response_empty",
            "evidence": "canonical_provider_gap_evidence",
            "provider_gap_start": None,
            "provider_gap_end": None,
        }
    ]


def test_classification_rejects_non_provider_gap_evidence() -> None:
    with pytest.raises(ValueError, match="classification must be provider_missing_data"):
        classify_unknown_gaps_from_provider_evidence(
            [{"start": "2026-03-06T22:00:00Z", "end": "2026-03-06T23:00:00Z"}],
            [
                {
                    "classification": "ingestion_failure",
                    "start": "2026-03-06T22:00:00Z",
                    "end": "2026-03-06T23:00:00Z",
                }
            ],
        )
