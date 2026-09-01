from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import portal.backend.service.market.market_structure_service as market_structure_module
import pytest
from data_providers.streams.contracts import ProviderRawMessage
from market_data.fact_registry import get_fact_contract
from market_data.order_book import (
    BookLifecycle,
    BookSide,
    BookSourcePosition,
    L2EventFact,
    L2EventType,
    L2Mutation,
    L2ProductContract,
    Level2BookReconstructor,
)
from market_data.structure import MarketTradeRecord, ProductContract, ProviderSizeUnit
from market_data.stream_enrollment import load_stream_enrollment_manifest
from market_data.market_state import (
    BBO_FACT_TYPE,
    DEPTH_FACT_TYPE,
    DERIVATIVE_STATE_FACT_TYPE,
    RESPONSE_FACT_TYPE,
)
from portal.backend.service.market.market_structure_service import (
    MarketStructureService,
    _build_execution_book_tape_from_replay,
)
from portal.backend.service.storage.repos.market_structure import (
    AggregateIngestionOutcome,
    ArchiveCommitResult,
    BookIngestionOutcome,
    FeatureIngestionOutcome,
    StreamClaim,
    TradeIngestionOutcome,
)


class _SafetyRepository:
    def __init__(self) -> None:
        self.persisted = None

    def record_safety_event(self, **kwargs):
        self.persisted = kwargs
        return dict(kwargs)

    def list_safety_events(self, **_kwargs):
        return []


class _EnrollmentMarketDataRepository:
    def __init__(self) -> None:
        self._series_ids: dict[tuple[str, str, int | None, str], int] = {}

    def register_source(self, _identity, *, lineage) -> int:
        assert lineage["manifest_hash"]
        return 1

    def register_series(
        self,
        *,
        instrument_id: str,
        fact_type: str,
        timeframe_seconds: int | None,
        contract_version: str,
    ) -> int:
        key = (instrument_id, fact_type, timeframe_seconds, contract_version)
        return self._series_ids.setdefault(key, len(self._series_ids) + 10)


class _EnrollmentRepository:
    def __init__(self) -> None:
        self.definition_calls: list[dict] = []

    def register_product_definition(self, **kwargs):
        return kwargs

    def upsert_stream_definition(self, **kwargs):
        self.definition_calls.append(kwargs)
        return kwargs


def test_reapplying_enrollment_manifest_has_stable_stream_definition_material(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    products = {
        "b2deb0a0-f292-408a-876d-3dadd8e3819b": "BIP-20DEC30-CDE",
        "44226144-fb38-4566-92c4-580734d76d3c": "ETP-20DEC30-CDE",
        "bead556e-22e2-4ac0-8ee0-0d8c5310e9a0": "SLP-20DEC30-CDE",
    }
    market_data_repository = _EnrollmentMarketDataRepository()
    repository = _EnrollmentRepository()
    monkeypatch.setattr(
        market_structure_module,
        "market_data_repo",
        market_data_repository,
    )
    monkeypatch.setattr(
        market_structure_module,
        "get_instrument_record",
        lambda instrument_id: {
            "symbol": products[instrument_id],
            "metadata": {
                "instrument_fields": {
                    "tick_size": "0.01",
                    "qty_step": "0.01",
                }
            },
        },
    )
    service = MarketStructureService(repository=repository)

    first = service.apply_stream_enrollment_manifest()
    second = service.apply_stream_enrollment_manifest(
        manifest=load_stream_enrollment_manifest(
            "config/market_data/coinbase_perpetual_trade_fleet.v1.json"
        )
    )

    assert first["manifest_hash"] == second["manifest_hash"]
    assert len(repository.definition_calls) == 6
    assert repository.definition_calls[:3] == repository.definition_calls[3:]
    for call in repository.definition_calls:
        assert set(call["config"]["aggregate_series_ids"]) == {"1", "60"}
        assert set(call["config"]["flow_feature_series_ids"]) == {"1", "60"}
        assert len(call["config"]["output_series"]) == 5
        assert {
            row["fact_type"] for row in call["config"]["output_series"]
        } == {
            "market.trade",
            "market.trade_flow",
            "market.trade_flow_feature",
        }
        assert call["config"]["runtime_policy"]["lease_seconds"] == 90.0
        assert call["config"]["runtime_policy"]["heartbeat_seconds"] == 10.0
        assert "collector_runtime" not in call["config"]
    assert {
        key[1]
        for key in market_data_repository._series_ids
    } >= {"market.trade_flow", "market.trade_flow_feature"}


def test_level2_fleet_manifest_registers_continuous_book_and_feature_series(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    products = {
        "b2deb0a0-f292-408a-876d-3dadd8e3819b": "BIP-20DEC30-CDE",
        "44226144-fb38-4566-92c4-580734d76d3c": "ETP-20DEC30-CDE",
        "bead556e-22e2-4ac0-8ee0-0d8c5310e9a0": "SLP-20DEC30-CDE",
    }
    market_data_repository = _EnrollmentMarketDataRepository()
    repository = _EnrollmentRepository()
    monkeypatch.setattr(
        market_structure_module,
        "market_data_repo",
        market_data_repository,
    )
    monkeypatch.setattr(
        market_structure_module,
        "get_instrument_record",
        lambda instrument_id: {
            "symbol": products[instrument_id],
            "metadata": {
                "instrument_fields": {
                    "tick_size": "1",
                    "qty_step": "1",
                }
            },
        },
    )

    result = MarketStructureService(
        repository=repository
    ).apply_stream_enrollment_manifest(
        manifest_path="config/market_data/coinbase_perpetual_l2_fleet.v1.json"
    )

    assert result["fleet_id"] == "coinbase_perpetual_l2"
    assert len(repository.definition_calls) == 3
    definition = repository.definition_calls[0]
    assert definition["definition_id"] == "ms_coinbase_l2_bip_20dec30_cde"
    assert definition["channels"] == ("level2", "heartbeats")
    assert definition["enabled"] is True
    assert definition["max_spool_bytes"] == 4 * 1024**3
    assert definition["config"]["schema_version"] == (
        "market_structure_l2_stream_config.v1"
    )
    assert definition["config"]["safety_policy"]["policy_hash"]
    assert definition["config"]["runtime_policy"]["lease_seconds"] == 90.0
    assert {
        row["fact_type"] for row in definition["config"]["output_series"]
    } == {
        "market.l2_book",
        BBO_FACT_TYPE,
        DEPTH_FACT_TYPE,
        RESPONSE_FACT_TYPE,
    }
    assert {
        key[1] for key in market_data_repository._series_ids
    } >= {
        "market.l2_book",
        BBO_FACT_TYPE,
        DEPTH_FACT_TYPE,
        RESPONSE_FACT_TYPE,
        "market.trade",
        "market.trade_flow_feature",
    }
    assert {
        call["definition_id"] for call in repository.definition_calls
    } == {
        "ms_coinbase_l2_bip_20dec30_cde",
        "ms_coinbase_l2_etp_20dec30_cde",
        "ms_coinbase_l2_slp_20dec30_cde",
    }


class _FakeStream:
    unknown_update_sides: tuple[str, ...] = ()

    def __init__(self, *, stream_session_id: str, **_kwargs) -> None:
        self.session_id = stream_session_id
        self.closed = False

    async def connect(self) -> int:
        return 0

    async def subscribe(self, _subscriptions) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def raw_messages(self):
        base = datetime.now(UTC) - timedelta(seconds=10)
        frames = [
            {
                "channel": "subscriptions",
                "timestamp": (base + timedelta(milliseconds=1)).isoformat(),
                "sequence_num": 0,
                "events": [{"subscriptions": {"market_trades": ["BTC-USD"]}}],
            },
            {
                "channel": "heartbeats",
                "timestamp": (base + timedelta(milliseconds=2)).isoformat(),
                "sequence_num": 1,
                "events": [{"current_time": base.isoformat(), "heartbeat_counter": "1"}],
            },
            {
                "channel": "market_trades",
                "timestamp": (base + timedelta(milliseconds=3)).isoformat(),
                "sequence_num": 2,
                "events": [
                    {
                        "type": "snapshot",
                        "trades": [
                            {
                                "product_id": "BTC-USD",
                                "trade_id": "snapshot-1",
                                "price": "60000",
                                "size": "0.01",
                                "side": "BUY",
                                "time": (base - timedelta(seconds=1)).isoformat(),
                            }
                        ],
                    }
                ],
            },
            {
                "channel": "market_trades",
                "timestamp": (base + timedelta(seconds=1)).isoformat(),
                "sequence_num": 3,
                "events": [
                    {
                        "type": "update",
                        "trades": [
                            {
                                "product_id": "BTC-USD",
                                "trade_id": "update-1",
                                "price": "60001",
                                "size": "0.02",
                                "side": "SELL",
                                "time": (base + timedelta(seconds=1)).isoformat(),
                            },
                            *[
                                {
                                    "product_id": "BTC-USD",
                                    "trade_id": f"unknown-{index}",
                                    "price": "60001",
                                    "size": "0.01",
                                    "side": side,
                                    "time": (
                                        base + timedelta(seconds=1)
                                    ).isoformat(),
                                }
                                for index, side in enumerate(
                                    self.unknown_update_sides, start=1
                                )
                            ],
                        ],
                    }
                ],
            },
            {
                "channel": "heartbeats",
                "timestamp": (base + timedelta(seconds=2)).isoformat(),
                "sequence_num": 4,
                "events": [{"current_time": base.isoformat(), "heartbeat_counter": "2"}],
            },
            {
                "channel": "market_trades",
                "timestamp": (base + timedelta(seconds=3)).isoformat(),
                "sequence_num": 5,
                "events": [
                    {
                        "type": "update",
                        "trades": [
                            {
                                "product_id": "BTC-USD",
                                "trade_id": "update-2",
                                "price": "60002",
                                "size": "0.03",
                                "side": "BUY",
                                "time": (base + timedelta(seconds=3)).isoformat(),
                            }
                        ],
                    }
                ],
            },
        ]
        for ordinal, payload in enumerate(frames, start=1):
            yield ProviderRawMessage.build(
                provider="COINBASE",
                venue="COINBASE_DIRECT",
                stream_session_id=self.session_id,
                connection_epoch=0,
                receive_ordinal=ordinal,
                received_at=datetime.now(UTC).isoformat(),
                raw_frame=json.dumps(payload, separators=(",", ":")),
            )
        await asyncio.Event().wait()


class _FakeRepository:
    def __init__(self) -> None:
        self.trades: dict[str, MarketTradeRecord] = {}
        self.coverage = None
        self.released = False
        self.manifests: list[str] = []
        self.aggregate_count = 0
        self.quality_events: list[dict] = []

    def claim_stream(self, **_kwargs) -> StreamClaim:
        return StreamClaim(
            definition_id="btc-definition",
            definition_generation=1,
            source_id=1,
            series_id=10,
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            provider_product_id="BTC-USD",
            channels=("market_trades", "heartbeats"),
            auth_mode="public",
            contract_version="market.trade.v1",
            max_spool_bytes=10 * 1024 * 1024,
            max_segment_bytes=1024 * 1024,
            config={
                "product_definition_version_id": "coinbase.BTC-USD.product_contract.v1",
                "aggregate_series_ids": {"1": 11, "60": 12},
                "flow_feature_series_ids": {"1": 13, "60": 14},
            },
            owner_id="test-owner",
            lease_token="token",
            lease_generation=1,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            session_id="test-session",
        )

    def get_product_contract(self, _definition_version_id: str) -> ProductContract:
        return ProductContract(
            provider_product_id="BTC-USD",
            provider_size_unit="base",
            base_currency="BTC",
            quote_currency="USD",
            product_definition_version_id="coinbase.BTC-USD.product_contract.v1",
        )

    def append_session_event(self, _claim, *, event_ordinal, **_kwargs) -> str:
        return f"event-{event_ordinal}"

    def heartbeat(self, _claim, *, lease_seconds) -> datetime:
        return datetime.now(UTC) + timedelta(seconds=lease_seconds)

    def commit_archive(self, _claim, *, records, **_kwargs) -> ArchiveCommitResult:
        manifest = f"manifest-{len(self.manifests) + 1}"
        self.manifests.append(manifest)
        return ArchiveCommitResult(
            manifest_id=manifest,
            inserted_manifest=True,
            inserted_mapping_count=len(records),
            mapped_record_count=len(records),
        )

    def record_quality_event(self, _claim, **_kwargs) -> str:
        self.quality_events.append(dict(_kwargs))
        return f"quality-{len(self.quality_events)}"

    def ingest_trades(self, claim, *, facts, **_kwargs) -> TradeIngestionOutcome:
        inserted = []
        noop = 0
        for fact in facts:
            if fact.provider_trade_id in self.trades:
                noop += 1
                continue
            record = MarketTradeRecord(
                version_id=f"trade-{fact.provider_trade_id}",
                series_id=claim.series_id,
                source_id=claim.source_id,
                revision=1,
                market_commit_seq=len(self.trades) + 1,
                provenance_hash="0" * 64,
                quality={},
                fact=fact,
            )
            self.trades[fact.provider_trade_id] = record
            inserted.append(record)
        return TradeIngestionOutcome(
            requested_count=len(facts),
            inserted_count=len(inserted),
            noop_count=noop,
            max_commit_seq=max((row.market_commit_seq for row in inserted), default=0),
            records=tuple(inserted),
        )

    def append_coverage_version(self, _claim, *, coverage, **_kwargs) -> str:
        self.coverage = coverage
        return "coverage-version"

    def read_trades(self, **_kwargs):
        return list(self.trades.values())

    def ingest_aggregates(self, *, facts, **_kwargs) -> AggregateIngestionOutcome:
        count = len(facts)
        self.aggregate_count += count
        return AggregateIngestionOutcome(
            inserted_count=count,
            noop_count=0,
            max_commit_seq=100 + self.aggregate_count,
            records=(),
        )

    def ingest_market_state_features(self, **kwargs) -> FeatureIngestionOutcome:
        facts = tuple(
            fact
            for values in kwargs.values()
            for fact in values
        )
        return FeatureIngestionOutcome(
            inserted_count=len(facts),
            noop_count=0,
            max_commit_seq=200 + len(facts),
            material_hashes=tuple(fact.material_hash for fact in facts),
        )

    def read_trade_flow_features(self, **_kwargs):
        return ()

    def archive_status(self, *, definition_id) -> dict:
        return {
            "schema_version": "market.stream_archive_status.v1",
            "definition_id": definition_id,
            "manifest_count": len(self.manifests),
        }

    def release(self, _claim) -> None:
        self.released = True


class _FakeL2Stream:
    def __init__(self, *, stream_session_id: str, **_kwargs) -> None:
        self.session_id = stream_session_id
        self.closed = False

    async def connect(self) -> int:
        return 0

    async def subscribe(self, _subscriptions) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def raw_messages(self):
        base = datetime.now(UTC) - timedelta(seconds=10)
        frames = [
            {
                "channel": "subscriptions",
                "timestamp": (base + timedelta(milliseconds=1)).isoformat(),
                "sequence_num": 0,
                "events": [{"subscriptions": {"level2": ["BTC-USD"]}}],
            },
            {
                "channel": "heartbeats",
                "timestamp": (base + timedelta(milliseconds=2)).isoformat(),
                "sequence_num": 1,
                "events": [
                    {"current_time": base.isoformat(), "heartbeat_counter": "1"}
                ],
            },
            {
                "channel": "l2_data",
                "timestamp": (base + timedelta(milliseconds=3)).isoformat(),
                "sequence_num": 2,
                "events": [
                    {
                        "type": "snapshot",
                        "product_id": "BTC-USD",
                        "updates": [
                            {
                                "side": "bid",
                                "event_time": base.isoformat(),
                                "price_level": "60000",
                                "new_quantity": "1",
                            },
                            {
                                "side": "offer",
                                "event_time": base.isoformat(),
                                "price_level": "60001",
                                "new_quantity": "1",
                            },
                        ],
                    }
                ],
            },
            {
                "channel": "l2_data",
                "timestamp": (base + timedelta(milliseconds=4)).isoformat(),
                "sequence_num": 3,
                "events": [
                    {
                        "type": "update",
                        "product_id": "BTC-USD",
                        "updates": [
                            {
                                "side": "bid",
                                "event_time": (base + timedelta(seconds=1)).isoformat(),
                                "price_level": "60002",
                                "new_quantity": "1",
                            }
                        ],
                    }
                ],
            },
        ]
        for ordinal, payload in enumerate(frames, start=1):
            yield ProviderRawMessage.build(
                provider="COINBASE",
                venue="COINBASE_DIRECT",
                stream_session_id=self.session_id,
                connection_epoch=0,
                receive_ordinal=ordinal,
                received_at=datetime.now(UTC).isoformat(),
                raw_frame=json.dumps(payload, separators=(",", ":")),
            )
        await asyncio.Event().wait()


class _FakeL2Repository(_FakeRepository):
    def __init__(self) -> None:
        super().__init__()
        self.last_book_ingest = None

    def claim_stream(self, **_kwargs) -> StreamClaim:
        return StreamClaim(
            definition_id="btc-l2-definition",
            definition_generation=1,
            source_id=1,
            series_id=20,
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            provider_product_id="BTC-USD",
            channels=("level2", "heartbeats"),
            auth_mode="public",
            contract_version="market.l2_book.v1",
            max_spool_bytes=10 * 1024 * 1024,
            max_segment_bytes=1024 * 1024,
            config={
                "product_definition_version_id": "pdv-btc",
                "provider_size_unit": "base",
                "price_increment": "1",
                "quantity_increment": "0.00000001",
                "bbo_series_id": 21,
                "depth_series_id": 22,
                "response_feature_series_id": 23,
                "trade_series_id": 10,
                "flow_feature_series_ids": {"1": 13, "60": 14},
                "base_currency": "BTC",
                "quote_currency": "USD",
                "contract_size": None,
            },
            owner_id="test-owner",
            lease_token="token",
            lease_generation=1,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            session_id="test-l2-session",
        )

    def commit_book_checkpoint(self, _claim, **_kwargs) -> bool:
        return True

    def ingest_book_facts(self, _claim, **kwargs) -> BookIngestionOutcome:
        self.last_book_ingest = kwargs
        return BookIngestionOutcome(
            inserted_snapshot_count=len(kwargs["snapshots"]),
            noop_snapshot_count=0,
            inserted_batch_count=len(kwargs["batches"]),
            noop_batch_count=0,
            inserted_validity_count=len(kwargs["validity_versions"]),
            max_commit_seq=1,
        )

    def link_book_quality_event(self, _claim, **_kwargs) -> None:
        return None


def test_bounded_l2_reports_terminal_invalid_state_without_false_clean_close(
    tmp_path: Path,
) -> None:
    repository = _FakeL2Repository()
    service = MarketStructureService(
        repository=repository,
        stream_factory=_FakeL2Stream,
    )

    result = asyncio.run(
        service.capture_bounded(
            definition_id="btc-l2-definition",
            duration_seconds=1,
            storage_root=tmp_path,
            owner_id="test-owner",
        )
    )

    assert result["status"] == "completed"
    assert result["snapshot_count"] == 1
    assert result["mutation_batch_count"] == 0
    assert result["final_state_hash"] is None
    assert result["validity_closed_cleanly"] is False
    assert repository.last_book_ingest is not None
    assert repository.last_book_ingest["lifecycle"] is BookLifecycle.INVALID
    assert repository.last_book_ingest["final_connection_epoch"] == 0
    assert repository.last_book_ingest["final_receive_ordinal"] == 4
    assert repository.released is True


def test_replay_projection_builds_causal_hash_verified_tape_and_closes_gaps() -> None:
    base = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)
    contract = L2ProductContract(
        provider_product_id="BTC-USD",
        product_definition_version_id="btc-product.v1",
        provider_size_unit=ProviderSizeUnit.BASE,
        price_increment=Decimal("1"),
        quantity_increment=Decimal("0.1"),
    )

    def event(
        event_type: L2EventType,
        ordinal: int,
        updates: list[tuple[BookSide, str, str]],
    ) -> L2EventFact:
        observed = base + timedelta(seconds=ordinal)
        return L2EventFact(
            event_type=event_type,
            position=BookSourcePosition(
                definition_id="definition-1",
                session_id="session-1",
                connection_epoch=0,
                provider_product_id="BTC-USD",
                provider_sequence_num=ordinal,
                receive_ordinal=ordinal,
                event_ordinal=0,
            ),
            product_definition_version_id="btc-product.v1",
            mutations=tuple(
                L2Mutation(
                    mutation_ordinal=index,
                    side=side,
                    price=price,
                    new_quantity=quantity,
                    provider_event_time=observed,
                    provider_size_unit=ProviderSizeUnit.BASE,
                )
                for index, (side, price, quantity) in enumerate(updates)
            ),
            provider_message_time=observed,
            received_at=observed + timedelta(milliseconds=1),
            accepted_at=observed + timedelta(milliseconds=2),
            known_at=observed + timedelta(milliseconds=2),
            raw_record_id=f"raw-{ordinal}",
        )

    reducer = Level2BookReconstructor(series_id=20, contract=contract)
    opened = reducer.process(
        event(
            L2EventType.SNAPSHOT,
            1,
            [
                (BookSide.BID, "99", "2"),
                (BookSide.ASK, "101", "3"),
            ],
        )
    )
    assert opened.state is not None
    invalidated = reducer.process(
        event(
            L2EventType.UPDATE,
            2,
            [(BookSide.BID, "101", "1")],
        )
    )
    assert invalidated.validity_versions
    tape = _build_execution_book_tape_from_replay(
        states=[opened.state],
        closing_validity=invalidated.validity_versions,
        instrument_id="instrument-btc",
        replay_fingerprint="f" * 64,
    )

    assert tape.replay_certified is True
    assert tape.source_capability == "l2"
    assert tape.snapshots[0].reconstruction_state_hash == opened.state.state_hash
    assert tape.validity_closures[0].source_reference.receive_ordinal == 2
    assert tape.tape_hash == type(tape).from_dict(tape.to_dict()).tape_hash
    assert tape.select_at(opened.state.known_at).snapshot_hash == tape.snapshots[0].snapshot_hash
    with pytest.raises(LookupError, match="invalid_at_arrival"):
        tape.select_at(invalidated.validity_versions[0].known_at)


def test_bounded_collector_spools_archives_persists_and_aggregates(
    tmp_path: Path,
) -> None:
    repository = _FakeRepository()
    service = MarketStructureService(
        repository=repository,
        stream_factory=_FakeStream,
    )
    result = asyncio.run(
        service.capture_bounded(
            definition_id="btc-definition",
            duration_seconds=1,
            storage_root=tmp_path,
            owner_id="test-owner",
        )
    )
    assert result["status"] == "completed"
    assert result["raw_record_count"] == 6
    assert result["trade_events"] == 3
    assert result["manifest_ids"] == ["manifest-1"]
    assert result["trade_ingestion"]["inserted"] == 3
    assert result["coverage"]["status"] == "closed_valid"
    assert result["spool_backlog_bytes"] == 0
    assert repository.coverage is not None
    assert repository.aggregate_count > 0
    assert repository.released is True


class _UnknownSideFakeStream(_FakeStream):
    unknown_update_sides = ("UNKNOWN_ORDER_SIDE", "UNKNOWN_ORDER_SIDE")


def test_bounded_collector_quarantines_unknown_trade_sides_without_failing(
    tmp_path: Path,
) -> None:
    repository = _FakeRepository()
    result = asyncio.run(
        MarketStructureService(
            repository=repository,
            stream_factory=_UnknownSideFakeStream,
        ).capture_bounded(
            definition_id="btc-definition",
            duration_seconds=1,
            storage_root=tmp_path,
            owner_id="test-owner",
        )
    )

    assert result["status"] == "completed"
    assert result["trade_events"] == 5
    assert result["trade_ingestion"] == {
        "requested": 3,
        "inserted": 3,
        "noop": 0,
        "max_commit_seq": 3,
        "rejected": 2,
    }
    assert result["coverage"]["status"] == "invalid"
    assert set(repository.trades) == {"snapshot-1", "update-1", "update-2"}
    rejection = next(
        row
        for row in repository.quality_events
        if row["classification"] == "provider_trade_side_unknown"
    )
    assert rejection["coverage_interval_id"] == result["coverage"]["interval_id"]
    assert rejection["evidence"]["quarantined_trade_count"] == 2
    assert rejection["evidence"]["provider_side_values"] == [
        "UNKNOWN_ORDER_SIDE"
    ]


class _FakeConfigurationRepository:
    def register_product_definition(self, **_kwargs) -> None:
        return None

    def upsert_stream_definition(self, **kwargs) -> dict:
        return {
            "definition_id": kwargs["definition_id"],
            "provider_product_id": kwargs["provider_product_id"],
        }

    def register_instrument_mapping(self, **_kwargs) -> str:
        return "mapping-1"


class _ContractValidatingMarketDataRepository:
    def __init__(self) -> None:
        self.next_source_id = 1
        self.next_series_id = 1
        self.series_calls: list[dict] = []

    def register_source(self, *_args, **_kwargs) -> int:
        source_id = self.next_source_id
        self.next_source_id += 1
        return source_id

    def register_series(self, **kwargs) -> int:
        get_fact_contract(kwargs["fact_type"]).validate(
            contract_version=kwargs["contract_version"],
            timeframe_seconds=kwargs["timeframe_seconds"],
        )
        self.series_calls.append(dict(kwargs))
        series_id = self.next_series_id
        self.next_series_id += 1
        return series_id


def test_configure_pair_registers_derivative_state_without_timeframe(
    monkeypatch,
) -> None:
    pair = market_structure_module.MARKET_STRUCTURE_PAIRS["bip_btc"]
    futures = {
        "id": "future-instrument",
        "symbol": pair.futures_product_id,
        "tick_size": "0.01",
        "qty_step": "1",
    }
    spot = {
        "id": "spot-instrument",
        "symbol": pair.spot_product_id,
        "tick_size": "0.01",
        "qty_step": "0.00000001",
    }
    fake_market_data_repository = _ContractValidatingMarketDataRepository()

    monkeypatch.setattr(
        market_structure_module,
        "get_instrument_record",
        lambda _instrument_id: futures,
    )
    monkeypatch.setattr(
        market_structure_module,
        "resolve_or_create_instrument",
        lambda *_args, **_kwargs: (spot, None),
    )
    monkeypatch.setattr(
        market_structure_module,
        "market_data_repo",
        fake_market_data_repository,
    )

    result = MarketStructureService(
        repository=_FakeConfigurationRepository(),
    ).configure_pair(pair_id="bip_btc")

    derivative_calls = [
        call
        for call in fake_market_data_repository.series_calls
        if call["fact_type"] == DERIVATIVE_STATE_FACT_TYPE
    ]
    assert result["pair_id"] == "bip_btc"
    assert len(derivative_calls) == 1
    assert derivative_calls[0]["timeframe_seconds"] is None


def test_operator_safety_halt_is_explicit_and_audited() -> None:
    repository = _SafetyRepository()
    service = MarketStructureService(repository=repository)
    result = service.set_safety_halt(
        request_id="request-a",
        scope_type="stream",
        scope_id="definition-a",
        requested_by="operator-a",
        reason="operator test",
        policy_hash="a" * 64,
        evidence={"ticket": "test"},
    )

    assert result["event_type"] == "halted"
    assert repository.persisted["scope_id"] == "definition-a"


def test_operator_safety_acknowledgement_is_a_separate_event() -> None:
    repository = _SafetyRepository()
    service = MarketStructureService(repository=repository)

    result = service.acknowledge_safety_halt(
        request_id="request-b",
        scope_type="fleet",
        scope_id="coinbase_perpetual_trades",
        requested_by="operator-a",
        reason="condition cleared",
        policy_hash="a" * 64,
    )

    assert result["event_type"] == "acknowledged"
