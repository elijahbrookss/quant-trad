from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from research_science import (
    FrozenFact,
    TemporalJoinSpec,
    apply_temporal_joins,
    primary_frames,
)


START = datetime(2026, 8, 6, tzinfo=UTC)


def _fact(
    key: str,
    *,
    minute: int,
    known_delay_seconds: int,
    value: float,
) -> FrozenFact:
    event_time = START + timedelta(minutes=minute)
    return FrozenFact(
        fact_key=key,
        event_time=event_time,
        sample_time=event_time,
        known_at=event_time + timedelta(seconds=known_delay_seconds),
        value=value,
        evidence_hash=f"{key}-{minute}-{known_delay_seconds}",
    )


def test_temporal_join_uses_latest_causal_context_not_latest_sample() -> None:
    price = tuple(
        _fact("price", minute=minute, known_delay_seconds=5, value=100 + minute)
        for minute in range(1, 4)
    )
    context = (
        _fact("sentiment", minute=0, known_delay_seconds=5, value=0.1),
        _fact("sentiment", minute=2, known_delay_seconds=30, value=0.9),
    )
    frames = primary_frames(price, primary_fact_key="price")
    result = apply_temporal_joins(
        frames,
        facts_by_key={"sentiment": context},
        joins=(
            TemporalJoinSpec(
                left_fact_key="price",
                right_fact_key="sentiment",
                output_key="sentiment_context",
                missing_policy="reject_frame",
            ),
        ),
    )

    assert [row.facts["sentiment_context"] for row in result] == [0.1, 0.1, 0.9]
    assert all(
        row.decision_time >= START + timedelta(minutes=index, seconds=5)
        for index, row in enumerate(result, start=1)
    )


def test_temporal_join_is_prefix_invariant() -> None:
    price = tuple(
        _fact("price", minute=minute, known_delay_seconds=5, value=100 + minute)
        for minute in range(1, 4)
    )
    context = tuple(
        _fact("volatility", minute=minute, known_delay_seconds=1, value=minute / 10)
        for minute in range(4)
    )
    join = TemporalJoinSpec(
        left_fact_key="price",
        right_fact_key="volatility",
        output_key="volatility_context",
        missing_policy="reject_frame",
    )
    prefix = apply_temporal_joins(
        primary_frames(price[:2], primary_fact_key="price"),
        facts_by_key={"volatility": context[:3]},
        joins=(join,),
    )
    full = apply_temporal_joins(
        primary_frames(price, primary_fact_key="price"),
        facts_by_key={"volatility": context},
        joins=(join,),
    )

    assert [row.frame_hash for row in prefix] == [
        row.frame_hash for row in full[:2]
    ]


def test_temporal_join_missing_policies_are_explicit() -> None:
    frames = primary_frames(
        (_fact("price", minute=1, known_delay_seconds=1, value=101),),
        primary_fact_key="price",
    )
    with pytest.raises(ValueError, match="no causal fact"):
        apply_temporal_joins(
            frames,
            facts_by_key={},
            joins=(
                TemporalJoinSpec(
                    left_fact_key="price",
                    right_fact_key="context",
                    output_key="context",
                    missing_policy="reject_frame",
                ),
            ),
        )

    excluded = apply_temporal_joins(
        frames,
        facts_by_key={},
        joins=(
            TemporalJoinSpec(
                left_fact_key="price",
                right_fact_key="context",
                output_key="context",
                missing_policy="exclude_frame",
            ),
        ),
    )
    assert excluded == ()

    nullable = apply_temporal_joins(
        frames,
        facts_by_key={},
        joins=(
            TemporalJoinSpec(
                left_fact_key="price",
                right_fact_key="context",
                output_key="context",
                missing_policy="null",
            ),
        ),
    )
    assert nullable[0].facts["context"] is None
