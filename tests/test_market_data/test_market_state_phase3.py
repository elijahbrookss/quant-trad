from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from market_data.contracts import (
    FundingRateFact,
    FundingRateRecord,
    OpenInterestFact,
    OpenInterestRecord,
    SourceIdentity,
)
from market_data.market_state import (
    MarketStateValuationContract,
    derive_basis_features,
    derive_book_features,
    derive_derivative_state_features,
    derive_response_features,
    derive_trade_flow_feature,
)
from market_data.order_book import BookSourcePosition, BookStateView
from market_data.structure import (
    COINBASE_AGGRESSOR_TRANSFORM_VERSION,
    MarketSide,
    MarketTradeFact,
    ProviderSizeUnit,
    TradeDeliveryKind,
    TradeFlowAggregateFact,
)


BASE = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
HASH_A = "a" * 64
SOURCE = SourceIdentity(
    provider="COINBASE",
    venue="COINBASE_DIRECT",
    source_kind="stream",
    adapter_version="test.v1",
)


def _position(ordinal: int) -> BookSourcePosition:
    return BookSourcePosition(
        definition_id="l2-bip",
        session_id="session-1",
        connection_epoch=1,
        provider_product_id="BIP-20DEC30-CDE",
        provider_sequence_num=ordinal,
        receive_ordinal=ordinal,
        event_ordinal=0,
    )


def _state(
    ordinal: int,
    offset: str,
    *,
    bids: tuple[tuple[str, str], ...] = (
        ("99.70", "20"),
        ("99.94", "10"),
        ("99.98", "5"),
    ),
    asks: tuple[tuple[str, str], ...] = (
        ("100.02", "6"),
        ("100.06", "11"),
        ("100.30", "21"),
    ),
    series_id: int = 10,
    validity_interval_id: str = "valid-1",
) -> BookStateView:
    effective = BASE + timedelta(seconds=float(offset))
    return BookStateView(
        series_id=series_id,
        validity_interval_id=validity_interval_id,
        source_position=_position(ordinal),
        product_definition_version_id="coinbase.BIP-20DEC30-CDE.provider-proof.v1",
        provider_size_unit=ProviderSizeUnit.CONTRACTS,
        effective_at=effective,
        known_at=effective + timedelta(milliseconds=5),
        state_hash=f"{ordinal:064x}",
        bids=tuple((Decimal(price), Decimal(size)) for price, size in bids),
        asks=tuple((Decimal(price), Decimal(size)) for price, size in asks),
    )


def _contract() -> MarketStateValuationContract:
    return MarketStateValuationContract(
        product_definition_version_id="coinbase.BIP-20DEC30-CDE.provider-proof.v1",
        provider_size_unit=ProviderSizeUnit.CONTRACTS,
        base_currency="BTC",
        quote_currency="USD",
        contract_size=Decimal("0.01"),
    )


def _trade(
    trade_id: str,
    *,
    offset: str,
    side: MarketSide,
    price: str,
    receive_ordinal: int,
    provider_size: str = "1",
) -> MarketTradeFact:
    event_time = BASE + timedelta(seconds=float(offset))
    size = Decimal(provider_size)
    base = size * Decimal("0.01")
    maker = MarketSide.SELL if side is MarketSide.BUY else MarketSide.BUY
    return MarketTradeFact(
        provider_product_id="BIP-20DEC30-CDE",
        provider_trade_id=trade_id,
        delivery_kind=TradeDeliveryKind.UPDATE,
        price=Decimal(price),
        provider_size=size,
        provider_size_unit=ProviderSizeUnit.CONTRACTS,
        maker_side=maker,
        aggressor_side=side,
        aggressor_transform_version=COINBASE_AGGRESSOR_TRANSFORM_VERSION,
        contract_quantity=size,
        base_quantity=base,
        quote_notional=Decimal(price) * base,
        base_currency="BTC",
        quote_currency="USD",
        product_definition_version_id="coinbase.BIP-20DEC30-CDE.provider-proof.v1",
        provider_event_time=event_time,
        provider_message_time=event_time,
        received_at=event_time + timedelta(milliseconds=1),
        accepted_at=event_time + timedelta(milliseconds=2),
        known_at=event_time + timedelta(milliseconds=2),
        provider_sequence_num=receive_ordinal,
        connection_epoch=1,
        receive_ordinal=receive_ordinal,
        event_ordinal=0,
        trade_ordinal=0,
        raw_record_id=f"raw-{trade_id}",
        coverage_interval_id="coverage-1",
    )


def _aggregate(trades: tuple[MarketTradeFact, ...]) -> TradeFlowAggregateFact:
    buys = tuple(row for row in trades if row.aggressor_side is MarketSide.BUY)
    sells = tuple(row for row in trades if row.aggressor_side is MarketSide.SELL)
    prices = [row.price for row in trades]
    return TradeFlowAggregateFact(
        interval_seconds=1,
        bucket_start=BASE,
        bucket_end=BASE + timedelta(seconds=1),
        trade_count=len(trades),
        maker_buy_count=len(sells),
        maker_sell_count=len(buys),
        aggressor_buy_count=len(buys),
        aggressor_sell_count=len(sells),
        contract_volume=sum((row.contract_quantity for row in trades), Decimal(0)),
        base_volume=sum((row.base_quantity for row in trades), Decimal(0)),
        quote_notional=sum((row.quote_notional for row in trades), Decimal(0)),
        maker_buy_base_volume=sum((row.base_quantity for row in sells), Decimal(0)),
        maker_sell_base_volume=sum((row.base_quantity for row in buys), Decimal(0)),
        aggressor_buy_base_volume=sum((row.base_quantity for row in buys), Decimal(0)),
        aggressor_sell_base_volume=sum((row.base_quantity for row in sells), Decimal(0)),
        cvd_delta=(
            sum((row.base_quantity for row in buys), Decimal(0))
            - sum((row.base_quantity for row in sells), Decimal(0))
        ),
        cvd_unit="base",
        open_price=prices[0],
        high_price=max(prices),
        low_price=min(prices),
        close_price=prices[-1],
        first_trade_id=trades[0].provider_trade_id,
        last_trade_id=trades[-1].provider_trade_id,
        first_receive_ordinal=trades[0].receive_ordinal,
        last_receive_ordinal=trades[-1].receive_ordinal,
        coverage_interval_id="coverage-1",
        coverage_revision=1,
        aggregate_complete=True,
        archive_complete=True,
        canonicalization_complete=True,
        late_trade_count=0,
        known_at=BASE + timedelta(seconds=1),
        input_fingerprint="f" * 64,
    )


def test_book_features_are_typed_causal_and_replay_stable() -> None:
    first = _state(1, "0.2")
    unseen = _state(2, "0.8")
    early_bbo, early_depth = derive_book_features(
        (first, unseen),
        contract=_contract(),
        bbo_series_id=20,
        depth_series_id=21,
        computed_at=BASE + timedelta(seconds=1),
    )
    replay_bbo, replay_depth = derive_book_features(
        (unseen, first),
        contract=_contract(),
        bbo_series_id=20,
        depth_series_id=21,
        computed_at=BASE + timedelta(seconds=1),
    )
    assert len(early_bbo) == 1
    assert len(early_depth) == 3
    assert early_bbo[0].series_id == 20
    assert early_bbo[0].source_l2_series_id == 10
    assert {row.band_bps for row in early_depth} == {5, 10, 25}
    assert all(row.series_id == 21 for row in early_depth)
    assert all(row.source_l2_series_id == 10 for row in early_depth)
    assert all(row.imbalance is None or -1 <= row.imbalance <= 1 for row in early_depth)
    assert [row.material_hash for row in early_bbo] == [
        row.material_hash for row in replay_bbo
    ]
    assert [row.material_hash for row in early_depth] == [
        row.material_hash for row in replay_depth
    ]


def test_later_book_evidence_does_not_change_earlier_feature() -> None:
    earlier = _state(1, "0.2")
    later = _state(2, "1.2")
    prefix, _ = derive_book_features(
        (earlier,),
        contract=_contract(),
        bbo_series_id=20,
        depth_series_id=21,
        computed_at=BASE + timedelta(seconds=1),
    )
    full, _ = derive_book_features(
        (earlier, later),
        contract=_contract(),
        bbo_series_id=20,
        depth_series_id=21,
        computed_at=BASE + timedelta(seconds=2),
    )
    assert prefix[0].material_hash == full[0].material_hash
    assert prefix[0].known_at == full[0].known_at


def test_trade_flow_reconciles_side_notional_and_normalized_cvd() -> None:
    trades = (
        _trade("buy-1", offset="0.2", side=MarketSide.BUY, price="100", receive_ordinal=1),
        _trade("sell-1", offset="0.7", side=MarketSide.SELL, price="101", receive_ordinal=2),
    )
    feature = derive_trade_flow_feature(
        series_id=30,
        source_trade_flow_series_id=31,
        aggregate=_aggregate(trades),
        trades=trades,
        computed_at=BASE + timedelta(seconds=1),
    )
    assert feature is not None
    assert feature.series_id == 30
    assert feature.source_trade_flow_series_id == 31
    assert feature.aggressor_buy_notional == Decimal("1.00")
    assert feature.aggressor_sell_notional == Decimal("1.01")
    assert feature.cvd_notional == Decimal("-0.01")
    assert feature.cvd_volume_share == 0


def test_trade_flow_suppresses_incomplete_or_zero_aggregate() -> None:
    trade = _trade(
        "buy-1",
        offset="0.2",
        side=MarketSide.BUY,
        price="100",
        receive_ordinal=1,
    )
    aggregate = _aggregate((trade,))
    incomplete = TradeFlowAggregateFact(
        **{
            **aggregate.__dict__,
            "aggregate_complete": False,
            "archive_complete": False,
        }
    )
    assert (
        derive_trade_flow_feature(
            series_id=30,
            source_trade_flow_series_id=31,
            aggregate=incomplete,
            trades=(trade,),
            computed_at=BASE + timedelta(seconds=1),
        )
        is None
    )


def test_basis_alignment_is_backward_looking_and_staleness_bounded() -> None:
    futures, _ = derive_book_features(
        (_state(1, "0.4"),),
        contract=_contract(),
        bbo_series_id=20,
        depth_series_id=21,
        computed_at=BASE + timedelta(seconds=1),
    )
    spot_state = _state(2, "0.2", series_id=11)
    spot, _ = derive_book_features(
        (spot_state,),
        contract=_contract(),
        bbo_series_id=22,
        depth_series_id=23,
        computed_at=BASE + timedelta(seconds=1),
    )
    rows = derive_basis_features(
        futures,
        spot,
        series_id=40,
        mapping_id="bip-btc",
        computed_at=BASE + timedelta(seconds=1),
    )
    assert len(rows) == 1
    assert rows[0].series_id == 40
    assert rows[0].futures_series_id == 20
    assert rows[0].spot_series_id == 22
    assert rows[0].spot_staleness_seconds == 0
    stale_spot = tuple(
        type(row)(**{**row.__dict__, "bucket_start": row.bucket_start - timedelta(seconds=3), "bucket_end": row.bucket_end - timedelta(seconds=3), "source_effective_at": row.source_effective_at - timedelta(seconds=3), "known_at": row.known_at - timedelta(seconds=3)})
        for row in spot
    )
    assert (
        derive_basis_features(
            futures,
            stale_spot,
            series_id=40,
            mapping_id="bip-btc",
            computed_at=BASE + timedelta(seconds=1),
        )
        == ()
    )


def _oi_record(series_id: int, offset: int, value: float, commit: int) -> OpenInterestRecord:
    sample = BASE + timedelta(seconds=offset)
    fact = OpenInterestFact(
        sample_time=sample,
        value=value,
        known_at=sample + timedelta(milliseconds=5),
        known_at_method="platform_receipt",
        accepted_at=sample + timedelta(milliseconds=5),
    )
    return OpenInterestRecord(
        series_id=series_id,
        revision=1,
        market_commit_seq=commit,
        ingestion_run_id=f"oi-{commit}",
        source_identity_key=SOURCE.identity_key,
        source=SOURCE,
        provenance={},
        fact=fact,
    )


def _funding_record(series_id: int, offset: int, commit: int) -> FundingRateRecord:
    sample = BASE + timedelta(seconds=offset)
    fact = FundingRateFact(
        sample_time=sample,
        rate=0.0001,
        funding_time=sample + timedelta(hours=1),
        interval_seconds=3600,
        known_at=sample + timedelta(milliseconds=5),
        known_at_method="platform_receipt",
        accepted_at=sample + timedelta(milliseconds=5),
    )
    return FundingRateRecord(
        series_id=series_id,
        revision=1,
        market_commit_seq=commit,
        ingestion_run_id=f"funding-{commit}",
        source_identity_key=SOURCE.identity_key,
        source=SOURCE,
        provenance={},
        fact=fact,
    )


def test_oi_funding_relationship_requires_consecutive_gap_free_oi() -> None:
    oi = (_oi_record(50, 0, 100, 1), _oi_record(50, 60, 110, 2))
    funding = (_funding_record(51, 60, 3),)
    rows = derive_derivative_state_features(
        series_id=52,
        instrument_id="bip",
        oi_records=oi,
        funding_records=funding,
        oi_gaps=(),
        expected_oi_interval_seconds=60,
        computed_at=BASE + timedelta(seconds=121),
    )
    assert len(rows) == 1
    assert rows[0].oi_log_change is not None
    assert rows[0].funding_rate == Decimal("0.0001")
    assert rows[0].funding_semantics == "provider_reported"
    gapped = derive_derivative_state_features(
        series_id=52,
        instrument_id="bip",
        oi_records=oi,
        funding_records=funding,
        oi_gaps=(
            {
                "start": BASE.isoformat(),
                "end": (BASE + timedelta(seconds=60)).isoformat(),
            },
        ),
        expected_oi_interval_seconds=60,
        computed_at=BASE + timedelta(seconds=121),
    )
    assert len(gapped) == 1
    assert gapped[0].oi_log_change is None
    assert gapped[0].funding_rate == Decimal("0.0001")


def test_directional_response_uses_consumed_ask_depth_for_buys() -> None:
    trade = _trade(
        "buy-1",
        offset="0.2",
        side=MarketSide.BUY,
        price="100.02",
        receive_ordinal=10,
        provider_size="10",
    )
    flow = derive_trade_flow_feature(
        series_id=30,
        source_trade_flow_series_id=31,
        aggregate=_aggregate((trade,)),
        trades=(trade,),
        computed_at=BASE + timedelta(seconds=1),
    )
    assert flow is not None
    pre = _state(
        1,
        "0.1",
        asks=(("100.02", "100"), ("100.06", "100")),
    )
    trough = _state(
        2,
        "0.6",
        asks=(("100.02", "40"), ("100.06", "40")),
    )
    post = _state(
        3,
        "1.3",
        asks=(("100.02", "70"), ("100.06", "70")),
    )
    rows = derive_response_features(
        (pre, trough, post),
        (trade,),
        (flow,),
        contract=_contract(),
        series_id=60,
        computed_at=BASE + timedelta(seconds=2),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.direction is MarketSide.BUY
    assert row.series_id == 60
    assert row.source_flow_feature_series_id == 30
    assert row.consumed_depth_notional > 0
    assert row.replenished_depth_notional > 0
    assert row.depth_replenishment > 0
    assert row.pre_book_source_position == pre.source_position
    assert row.trough_book_source_position == trough.source_position
    assert row.post_book_source_position == post.source_position
