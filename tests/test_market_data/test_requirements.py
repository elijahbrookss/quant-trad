from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from market_data.contracts import (
    CANDLE_FACT_TYPE,
    OPEN_INTEREST_FACT_TYPE,
    InstrumentRole,
    MarketDataRequirement,
    OpenInterestFact,
    OpenInterestRecord,
    SourceIdentity,
)
from market_data.requirements import (
    BoundMarketDataContext,
    InstrumentResolutionContext,
    MarketDataPlanResolver,
    UnavailableMarketData,
)


UTC = timezone.utc


def _oi_record(*, known_at: datetime, value: float, commit_seq: int) -> OpenInterestRecord:
    return OpenInterestRecord(
        series_id=7,
        revision=1,
        market_commit_seq=commit_seq,
        ingestion_run_id=f"poll-{commit_seq}",
        source_identity_key="coinbase-oi",
        source=SourceIdentity(
            "COINBASE", "COINBASE_DIRECT", "poll_api", "coinbase.oi.v1"
        ),
        provenance={"provider_event_time_available": False},
        fact=OpenInterestFact(
            sample_time=known_at - timedelta(seconds=5),
            value=value,
            received_at=known_at,
            accepted_at=known_at,
            known_at=known_at,
            known_at_method="platform_acceptance",
        ),
    )


def test_resolver_supports_primary_underlying_benchmark_and_explicit_roles() -> None:
    requirements = (
        MarketDataRequirement(
            key="price",
            fact_type=CANDLE_FACT_TYPE,
            timeframe_seconds=3600,
        ),
        MarketDataRequirement(
            key="underlying_price",
            fact_type=CANDLE_FACT_TYPE,
            timeframe_seconds=3600,
            instrument_role=InstrumentRole.UNDERLYING,
        ),
        MarketDataRequirement(
            key="market_benchmark",
            fact_type=CANDLE_FACT_TYPE,
            timeframe_seconds=3600,
            instrument_role=InstrumentRole.BENCHMARK,
            instrument_ref="crypto_market",
        ),
        MarketDataRequirement(
            key="fixed_reference",
            fact_type=CANDLE_FACT_TYPE,
            timeframe_seconds=3600,
            instrument_role=InstrumentRole.EXPLICIT,
            instrument_ref="coinbase-btc-spot",
        ),
    )
    plan = MarketDataPlanResolver().resolve(
        [("indicator:example", requirements)],
        instruments=InstrumentResolutionContext(
            primary_instrument_ids=("coinbase-btc-future",),
            underlying_by_primary={"coinbase-btc-future": "coinbase-btc-spot"},
            benchmarks={"crypto_market": "coinbase-btc-index"},
            known_instrument_ids=frozenset(
                {
                    "coinbase-btc-future",
                    "coinbase-btc-spot",
                    "coinbase-btc-index",
                }
            ),
        ),
    )

    assert {series.instrument_id for series in plan.series} == {
        "coinbase-btc-future",
        "coinbase-btc-spot",
        "coinbase-btc-index",
    }
    spot_series = next(
        series for series in plan.series if series.instrument_id == "coinbase-btc-spot"
    )
    assert {binding.requirement.key for binding in spot_series.bindings} == {
        "underlying_price",
        "fixed_reference",
    }
    assert plan.plan_hash == MarketDataPlanResolver().resolve(
        [("indicator:example", requirements)],
        instruments=InstrumentResolutionContext(
            primary_instrument_ids=("coinbase-btc-future",),
            underlying_by_primary={"coinbase-btc-future": "coinbase-btc-spot"},
            benchmarks={"crypto_market": "coinbase-btc-index"},
            known_instrument_ids=frozenset(
                {
                    "coinbase-btc-future",
                    "coinbase-btc-spot",
                    "coinbase-btc-index",
                }
            ),
        ),
    ).plan_hash


def test_resolver_fails_loud_when_underlying_relationship_is_not_canonical() -> None:
    with pytest.raises(ValueError, match="role=underlying"):
        MarketDataPlanResolver().resolve(
            [
                (
                    "indicator:basis",
                    (
                        MarketDataRequirement(
                            key="underlying",
                            fact_type=CANDLE_FACT_TYPE,
                            timeframe_seconds=60,
                            instrument_role=InstrumentRole.UNDERLYING,
                        ),
                    ),
                )
            ],
            instruments=InstrumentResolutionContext(
                primary_instrument_ids=("future-1",)
            ),
        )


def test_bound_context_never_uses_future_or_stale_open_interest() -> None:
    requirement = MarketDataRequirement(
        key="open_interest",
        fact_type=OPEN_INTEREST_FACT_TYPE,
        max_staleness_seconds=60,
    )
    plan = MarketDataPlanResolver().resolve(
        [("indicator:oi", (requirement,))],
        instruments=InstrumentResolutionContext(primary_instrument_ids=("future-1",)),
    )
    series = plan.series[0]
    decision_time = datetime(2024, 1, 1, 12, 1, tzinfo=UTC)
    before = _oi_record(
        known_at=decision_time - timedelta(seconds=30), value=100, commit_seq=1
    )
    future = _oi_record(
        known_at=decision_time + timedelta(seconds=1), value=999, commit_seq=2
    )
    context = BoundMarketDataContext(
        plan=plan,
        records_by_series={
            (
                series.instrument_id,
                series.fact_type,
                series.contract_version,
                series.timeframe_seconds,
            ): (before, future)
        },
    )

    selected = context.latest(
        "open_interest",
        consumer_id="indicator:oi",
        primary_instrument_id="future-1",
        evaluation_time=decision_time,
    )

    assert isinstance(selected, OpenInterestRecord)
    assert selected.fact.value == 100


def test_optional_unavailable_is_structured_and_required_unavailable_fails() -> None:
    decision_time = datetime(2024, 1, 1, 12, 1, tzinfo=UTC)
    optional = MarketDataRequirement(
        key="open_interest",
        fact_type=OPEN_INTEREST_FACT_TYPE,
        max_staleness_seconds=60,
        required=False,
    )
    optional_plan = MarketDataPlanResolver().resolve(
        [("check:oi", (optional,))],
        instruments=InstrumentResolutionContext(primary_instrument_ids=("future-1",)),
    )
    optional_context = BoundMarketDataContext(
        plan=optional_plan,
        records_by_series={},
    )
    unavailable = optional_context.latest(
        "open_interest",
        consumer_id="check:oi",
        primary_instrument_id="future-1",
        evaluation_time=decision_time,
    )
    assert isinstance(unavailable, UnavailableMarketData)
    assert unavailable.reason == "no_known_fact"

    required_plan = MarketDataPlanResolver().resolve(
        [
            (
                "check:oi",
                (
                    MarketDataRequirement(
                        key="open_interest",
                        fact_type=OPEN_INTEREST_FACT_TYPE,
                        max_staleness_seconds=60,
                    ),
                ),
            )
        ],
        instruments=InstrumentResolutionContext(primary_instrument_ids=("future-1",)),
    )
    with pytest.raises(RuntimeError, match="market_data_required_unavailable"):
        BoundMarketDataContext(
            plan=required_plan,
            records_by_series={},
        ).latest(
            "open_interest",
            consumer_id="check:oi",
            primary_instrument_id="future-1",
            evaluation_time=decision_time,
        )
