from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from data_providers.streams.contracts import ProviderRawMessage
from market_data.order_book import BookLifecycle
from market_data.structure import MarketTradeRecord
from portal.backend.service.market.market_structure_service import MarketStructureService
from portal.backend.service.storage.repos.market_structure import (
    AggregateIngestionOutcome,
    ArchiveCommitResult,
    BookIngestionOutcome,
    StreamClaim,
    TradeIngestionOutcome,
)


class _FakeStream:
    def __init__(self, *, stream_session_id: str, **_kwargs) -> None:
        self.session_id = stream_session_id
        self.closed = False

    async def connect(self) -> None:
        return None

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
                            }
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
            config={"aggregate_series_ids": {"1": 11, "60": 12}},
            owner_id="test-owner",
            lease_token="token",
            lease_generation=1,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            session_id="test-session",
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
        return "quality"

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

    async def connect(self) -> None:
        return None

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
