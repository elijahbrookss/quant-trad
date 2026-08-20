from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from market_data.structure import (
    ArchiveStatus,
    COINBASE_AGGRESSOR_TRANSFORM_VERSION,
    MarketSide,
    MarketTradeFact,
    MarketTradeRecord,
    OrderingAssurance,
    TradeCoverageIntervalVersion,
    TradeDeliveryKind,
    TradeFlowAggregateRecord,
    aggregate_trade_bucket,
)
from research_science import (
    TradeFlowReplayPolicy,
    derive_trade_flow_replay,
)


def _coverage(start: datetime, *, last_ordinal: int = 4) -> TradeCoverageIntervalVersion:
    return TradeCoverageIntervalVersion(
        interval_id="coverage-1",
        revision=1,
        definition_id="definition-1",
        session_id="session-1",
        connection_epoch=0,
        provider_product_id="BTC-PERP",
        channel="market_trades",
        status="closed_valid",
        ordering_assurance=OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS,
        archive_status=ArchiveStatus.COMPLETE,
        opening_raw_record_id="raw-opening",
        opening_receive_ordinal=1,
        opening_effective_at=start - timedelta(seconds=1),
        last_raw_record_id=f"raw-{last_ordinal}",
        last_receive_ordinal=last_ordinal,
        last_effective_at=start + timedelta(minutes=4),
        closing_raw_record_id=f"raw-{last_ordinal}",
        closing_receive_ordinal=last_ordinal,
        closing_effective_at=start + timedelta(minutes=4),
        canonicalization_watermark_ordinal=last_ordinal,
        archive_complete_through_ordinal=last_ordinal,
        known_at=start + timedelta(hours=1),
    )


def _trade(
    start: datetime,
    *,
    minute: int,
    second: int,
    ordinal: int,
    price: str,
    accepted_delay: timedelta = timedelta(hours=1),
) -> MarketTradeRecord:
    event_time = start + timedelta(minutes=minute, seconds=second)
    received_at = event_time + timedelta(milliseconds=100)
    fact = MarketTradeFact(
        provider_product_id="BTC-PERP",
        provider_trade_id=f"trade-{ordinal}",
        delivery_kind=TradeDeliveryKind.UPDATE,
        price=Decimal(price),
        provider_size=Decimal("1"),
        provider_size_unit="contracts",
        maker_side=MarketSide.BUY,
        aggressor_side=MarketSide.SELL,
        aggressor_transform_version=COINBASE_AGGRESSOR_TRANSFORM_VERSION,
        contract_quantity=Decimal("1"),
        base_quantity=Decimal("1"),
        quote_notional=Decimal(price),
        base_currency="BTC",
        quote_currency="USD",
        product_definition_version_id="btc-perp.v1",
        provider_event_time=event_time,
        provider_message_time=event_time,
        received_at=received_at,
        accepted_at=received_at + accepted_delay,
        known_at=received_at + accepted_delay,
        provider_sequence_num=ordinal,
        connection_epoch=0,
        receive_ordinal=ordinal,
        event_ordinal=ordinal,
        trade_ordinal=0,
        raw_record_id=f"raw-{ordinal}",
        coverage_interval_id="coverage-1",
    )
    return MarketTradeRecord(
        version_id=f"trade-version-{ordinal}",
        series_id=1,
        source_id=1,
        revision=1,
        market_commit_seq=ordinal,
        provenance_hash="a" * 64,
        quality={},
        fact=fact,
    )


def _aggregate(
    start: datetime,
    *,
    minute: int,
    trades: tuple[MarketTradeRecord, ...],
    coverage: TradeCoverageIntervalVersion,
    version: int,
) -> TradeFlowAggregateRecord:
    bucket_start = start + timedelta(minutes=minute)
    fact = aggregate_trade_bucket(
        (row.fact for row in trades),
        interval_seconds=60,
        bucket_start=bucket_start,
        coverage=coverage,
        computed_at=start + timedelta(hours=1, minutes=minute),
    )
    return TradeFlowAggregateRecord(
        version_id=f"aggregate-{version}",
        series_id=2,
        revision=1,
        market_commit_seq=100 + version,
        aggregation_version="market.trade_flow.v1",
        provenance_hash="b" * 64,
        quality={},
        fact=fact,
    )


def _fixture():
    start = datetime(2026, 8, 5, tzinfo=UTC)
    coverage = _coverage(start)
    trades = (
        _trade(start, minute=0, second=10, ordinal=1, price="100"),
        _trade(start, minute=1, second=5, ordinal=2, price="101"),
        _trade(start, minute=2, second=5, ordinal=3, price="102"),
    )
    aggregates = (
        _aggregate(start, minute=0, trades=trades, coverage=coverage, version=1),
        _aggregate(start, minute=1, trades=trades, coverage=coverage, version=2),
        _aggregate(start, minute=2, trades=trades, coverage=coverage, version=3),
    )
    return start, coverage, trades, aggregates


def test_replay_availability_uses_receipt_watermark_and_preserves_canonical_clock() -> None:
    _, coverage, trades, aggregates = _fixture()
    policy = TradeFlowReplayPolicy(processing_latency_ms=50)

    buckets, artifact = derive_trade_flow_replay(
        policy=policy,
        aggregates=aggregates,
        source_trades=trades,
        coverage_versions={(coverage.interval_id, coverage.revision): coverage},
    )

    assert len(buckets) == 2
    assert artifact.exclusion_counts == {"source_watermark_unavailable": 1}
    assert buckets[0].replay_available_at == (
        trades[1].fact.received_at + timedelta(milliseconds=50)
    )
    assert buckets[0].canonical_known_at == aggregates[0].fact.known_at
    assert buckets[0].replay_available_at < buckets[0].canonical_known_at


def test_replay_semantics_ignore_later_batch_canonicalization_timestamps() -> None:
    _, coverage, trades, aggregates = _fixture()
    policy = TradeFlowReplayPolicy()
    left, left_artifact = derive_trade_flow_replay(
        policy=policy,
        aggregates=aggregates,
        source_trades=trades,
        coverage_versions={(coverage.interval_id, coverage.revision): coverage},
    )
    delayed_trades = tuple(
        replace(
            row,
            fact=replace(
                row.fact,
                accepted_at=row.fact.accepted_at + timedelta(hours=2),
                known_at=row.fact.known_at + timedelta(hours=2),
            ),
        )
        for row in trades
    )
    delayed_aggregates = tuple(
        replace(
            row,
            fact=replace(row.fact, known_at=row.fact.known_at + timedelta(hours=2)),
        )
        for row in aggregates
    )
    right, right_artifact = derive_trade_flow_replay(
        policy=policy,
        aggregates=delayed_aggregates,
        source_trades=delayed_trades,
        coverage_versions={(coverage.interval_id, coverage.revision): coverage},
    )

    assert [row.replay_bucket_hash for row in left] == [
        row.replay_bucket_hash for row in right
    ]
    assert left_artifact.replay_semantic_hash == right_artifact.replay_semantic_hash


def test_replay_prefix_is_invariant_when_later_buckets_are_added() -> None:
    _, coverage, trades, aggregates = _fixture()
    policy = TradeFlowReplayPolicy()
    prefix, _ = derive_trade_flow_replay(
        policy=policy,
        aggregates=aggregates[:1],
        source_trades=trades[:2],
        coverage_versions={(coverage.interval_id, coverage.revision): coverage},
    )
    full, _ = derive_trade_flow_replay(
        policy=policy,
        aggregates=aggregates,
        source_trades=trades,
        coverage_versions={(coverage.interval_id, coverage.revision): coverage},
    )
    assert prefix[0].replay_bucket_hash == full[0].replay_bucket_hash
    assert prefix[0].replay_available_at == full[0].replay_available_at


def test_replay_fails_on_raw_aggregate_tamper() -> None:
    _, coverage, trades, aggregates = _fixture()
    tampered = (
        replace(
            trades[0],
            fact=replace(
                trades[0].fact,
                price=Decimal("999"),
                quote_notional=Decimal("999"),
            ),
        ),
        *trades[1:],
    )
    with pytest.raises(ValueError, match="do not reconcile"):
        derive_trade_flow_replay(
            policy=TradeFlowReplayPolicy(),
            aggregates=aggregates,
            source_trades=tampered,
            coverage_versions={(coverage.interval_id, coverage.revision): coverage},
        )


def test_replay_fails_on_an_internal_aggregate_gap() -> None:
    _, coverage, trades, aggregates = _fixture()
    with pytest.raises(ValueError, match="aggregate bucket gap"):
        derive_trade_flow_replay(
            policy=TradeFlowReplayPolicy(),
            aggregates=(aggregates[0], aggregates[2]),
            source_trades=trades,
            coverage_versions={(coverage.interval_id, coverage.revision): coverage},
        )


def test_zero_trade_bucket_requires_a_later_source_watermark() -> None:
    start, coverage, trades, _ = _fixture()
    zero = _aggregate(
        start,
        minute=1,
        trades=(trades[0], trades[2]),
        coverage=coverage,
        version=7,
    )
    eligible, _ = derive_trade_flow_replay(
        policy=TradeFlowReplayPolicy(),
        aggregates=(zero,),
        source_trades=(trades[0], trades[2]),
        coverage_versions={(coverage.interval_id, coverage.revision): coverage},
    )
    assert len(eligible) == 1
    assert eligible[0].source_trade_count == 0

    unavailable, artifact = derive_trade_flow_replay(
        policy=TradeFlowReplayPolicy(),
        aggregates=(zero,),
        source_trades=(trades[0],),
        coverage_versions={(coverage.interval_id, coverage.revision): coverage},
    )
    assert unavailable == ()
    assert artifact.exclusion_counts == {"source_watermark_unavailable": 1}
