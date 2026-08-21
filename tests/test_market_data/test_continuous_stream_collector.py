from __future__ import annotations

import asyncio
import gzip
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from data_providers.streams.coinbase import CoinbaseMessageParser
from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
)
from market_data.structure import RawStreamRecord
from portal.backend.service.market.continuous_stream_collector import (
    CoinbaseContinuousTransportAdapter,
    CoinbaseMarketTradeProjectionAdapter,
    ContinuousMarketStructureCollector,
    ContinuousStreamRuntime,
    _EpochProjectionState,
    _SegmentCheckpoint,
    _SessionEventWriter,
)
from portal.backend.service.market.market_structure_service import _CaptureAnalyzer
from portal.backend.service.storage.repos.market_structure import StreamClaim


def _claim(*, session_id: str) -> StreamClaim:
    return StreamClaim(
        definition_id="definition-a",
        definition_generation=1,
        source_id=1,
        series_id=2,
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        provider_product_id="BTC-USD",
        channels=("market_trades", "heartbeats"),
        auth_mode="public",
        contract_version="market.market_trade.v1",
        max_spool_bytes=1024**3,
        max_segment_bytes=1024**2,
        config={},
        owner_id="worker",
        lease_token="token",
        lease_generation=1,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
        session_id=session_id,
    )


def test_coinbase_transport_normalizes_provider_level2_channel_alias() -> None:
    transport = CoinbaseContinuousTransportAdapter()

    assert transport.observed_channel(b'{"channel":"l2_data"}') == "level2"
    assert transport.observed_channel(b'{"channel":"market_trades"}') == (
        "market_trades"
    )


def _l2_claim(*, session_id: str) -> StreamClaim:
    return StreamClaim(
        definition_id="definition-l2",
        definition_generation=1,
        source_id=1,
        series_id=2,
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        provider_product_id="BTC-USD",
        channels=("level2", "heartbeats"),
        auth_mode="public",
        contract_version="market.l2_book.v1",
        max_spool_bytes=1024**3,
        max_segment_bytes=1024**2,
        config={
            "product_definition_version_id": "coinbase.BTC-USD.product_contract.v1",
            "provider_size_unit": "base",
            "bbo_series_id": 3,
            "depth_series_id": 4,
            "base_currency": "BTC",
            "quote_currency": "USD",
        },
        owner_id="worker",
        lease_token="token",
        lease_generation=1,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
        session_id=session_id,
    )


class _RecoveryRepository:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.committed_records = 0
        self.closed_coverages = 0
        self.released = False

    def claim_stream(self, **kwargs):
        assert kwargs["resume_session_id"] == "session-before-crash"
        assert kwargs["bounded"] is True
        return _claim(session_id=kwargs["resume_session_id"])

    def next_session_event_ordinal(self, _claim):
        return 4

    def append_session_event(self, _claim, **kwargs):
        self.events.append(dict(kwargs))
        return f"event-{kwargs['event_ordinal']}"

    def heartbeat(self, _claim, **_kwargs):
        return datetime.now(UTC)

    def commit_archive(self, _claim, *, records, **_kwargs):
        self.committed_records += len(records)
        return SimpleNamespace(manifest_id="manifest-recovered")

    def ingest_trades(self, _claim, **_kwargs):
        return SimpleNamespace(inserted_count=0, noop_count=0)

    def close_open_session_coverages(self, _claim, **_kwargs):
        self.closed_coverages += 1
        return 1

    def release(self, _claim):
        self.released = True


def test_restart_recovery_repairs_archives_and_closes_prior_coverage(
    tmp_path: Path,
) -> None:
    spool_root = tmp_path / "spool"
    segment = DurableRawSpoolSegment(
        root=spool_root,
        definition_id="definition-a",
        session_id="session-before-crash",
        connection_epoch=2,
        segment_ordinal=7,
    )
    message = ProviderRawMessage.build(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        stream_session_id=segment.session_id,
        connection_epoch=2,
        receive_ordinal=1,
        received_at="2026-08-05T12:00:00Z",
        raw_frame=(
            '{"channel":"heartbeats","sequence_num":1,'
            '"events":[{"current_time":"2026-08-05T12:00:00Z"}]}'
        ),
    )
    segment.append(
        RawStreamRecord.from_provider_message(
            message,
            definition_id=segment.definition_id,
            spool_segment_id=segment.spool_segment_id,
            provider_product_id="BTC-USD",
            requested_channel="market_trades",
            observed_channel="heartbeats",
        )
    )
    segment.close()
    with segment.open_path.open("ab") as handle:
        handle.write(b'{"partial"')
        handle.flush()

    repository = _RecoveryRepository()
    collector = ContinuousMarketStructureCollector(repository=repository)
    collector._recover_orphaned_spools_sync(
        definition={"id": "definition-a"},
        owner_id="worker",
        lease_seconds=90,
        spool_root=spool_root,
        object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
        temporary_root=tmp_path / "tmp",
        projection=CoinbaseMarketTradeProjectionAdapter(),
    )

    assert repository.committed_records == 1
    assert repository.closed_coverages == 1
    assert repository.released is True
    event_types = [item["event_type"] for item in repository.events]
    assert event_types == [
        "collector_restart_recovery_started",
        "spool_segment_recovered",
        "collector_restart_recovery_completed",
    ]
    assert repository.events[1]["evidence"]["truncated_tail_bytes"] > 0
    assert not segment.open_path.exists()
    assert not segment.sealed_path.exists()


def test_terminal_checkpoint_retires_only_its_connection_epoch_state() -> None:
    collector = ContinuousMarketStructureCollector(repository=object())
    claim = _claim(session_id="session-a")
    first = object()
    second = object()
    states = {1: first, 2: second}

    collector._retire_epoch_state(
        states=states,
        connection_epoch=1,
        claim=claim,
    )

    assert states == {2: second}
    with pytest.raises(RuntimeError, match="epoch_retirement_invalid"):
        collector._retire_epoch_state(states=states, connection_epoch=1, claim=claim)


class _Level2Repository:
    def __init__(self) -> None:
        self.archive_count = 0
        self.checkpoint_ids: list[str] = []
        self.book_ingests: list[dict] = []
        self.feature_count = 0
        self.quality: list[dict] = []
        self.events: list[dict] = []
        self.manifests: list[dict] = []
        self.checkpoint_rows: list[dict] = []
        self.validity_openings: dict[str, dict] = {}
        self.reconstruction: dict | None = None

    def commit_archive(self, _claim, *, encoded, acknowledgement, records):
        self.archive_count += 1
        manifest_id = f"manifest-{self.archive_count}"
        self.manifests.append(
            {
                "id": manifest_id,
                "connection_epoch": records[0].connection_epoch,
                "spool_segment_id": encoded.spool_segment_id,
                "object_key": acknowledgement.object_key,
                "object_sha256": acknowledgement.sha256,
            }
        )
        return SimpleNamespace(manifest_id=manifest_id)

    def commit_book_checkpoint(
        self, _claim, *, checkpoint, encoded, acknowledgement, **_kwargs
    ):
        self.checkpoint_ids.append(checkpoint.checkpoint_id)
        self.checkpoint_rows.append(
            {
                "id": checkpoint.checkpoint_id,
                "series_id": checkpoint.series_id,
                "validity_interval_id": checkpoint.validity_interval_id,
                "reconstruction_version": "coinbase_advanced_trade_l2_absolute.v1",
                "product_definition_version_id": (
                    checkpoint.product_definition_version_id
                ),
                "provider_size_unit": checkpoint.provider_size_unit.value,
                "session_id": checkpoint.source_position.session_id,
                "connection_epoch": checkpoint.source_position.connection_epoch,
                "provider_sequence_num": (
                    checkpoint.source_position.provider_sequence_num
                ),
                "receive_ordinal": checkpoint.source_position.receive_ordinal,
                "event_ordinal": checkpoint.source_position.event_ordinal,
                "effective_at": checkpoint.effective_at,
                "known_at": checkpoint.known_at,
                "state_hash": checkpoint.state_hash,
                "object_key": acknowledgement.object_key,
                "object_sha256": acknowledgement.sha256,
                "content_fingerprint": checkpoint.content_fingerprint,
                "format": "parquet",
                "compression": "zstd",
                "schema_version": "market.book_checkpoint_levels.v1",
                "level_count": encoded.level_count,
                "bid_level_count": len(checkpoint.bids),
                "ask_level_count": len(checkpoint.asks),
                "mutation_count_since_prior": (
                    checkpoint.mutation_count_since_prior
                ),
            }
        )
        return True

    def ingest_book_facts(self, _claim, **kwargs):
        captured = {
            **kwargs,
            "snapshots": tuple(kwargs["snapshots"]),
            "batches": tuple(kwargs["batches"]),
            "validity_versions": tuple(kwargs["validity_versions"]),
        }
        self.book_ingests.append(captured)
        for validity in captured["validity_versions"]:
            if validity.revision != 1:
                continue
            self.validity_openings[validity.interval_id] = {
                "id": validity.version_id,
                "interval_id": validity.interval_id,
                "revision": validity.revision,
                "series_id": validity.series_id,
                "status": validity.status.value,
                "ordering_assurance": validity.ordering_assurance.value,
                "opening_snapshot_id": validity.opening_snapshot_id,
                "opening_session_id": validity.opening_position.session_id,
                "opening_connection_epoch": (
                    validity.opening_position.connection_epoch
                ),
                "opening_sequence_num": (
                    validity.opening_position.provider_sequence_num
                ),
                "opening_receive_ordinal": (
                    validity.opening_position.receive_ordinal
                ),
                "opening_event_ordinal": validity.opening_position.event_ordinal,
                "opening_effective_at": validity.opening_effective_at,
                "opening_known_at": validity.opening_known_at,
                "last_session_id": validity.last_valid_position.session_id,
                "last_connection_epoch": (
                    validity.last_valid_position.connection_epoch
                ),
                "last_sequence_num": (
                    validity.last_valid_position.provider_sequence_num
                ),
                "last_receive_ordinal": (
                    validity.last_valid_position.receive_ordinal
                ),
                "last_event_ordinal": validity.last_valid_position.event_ordinal,
                "last_valid_effective_at": validity.last_valid_effective_at,
                "last_state_hash": validity.last_state_hash,
                "known_at": validity.known_at,
            }
        self.reconstruction = {
            "definition_id": _claim.definition_id,
            "session_id": _claim.session_id,
            "connection_epoch": kwargs["final_connection_epoch"],
            "lifecycle": kwargs["lifecycle"].value,
            "validity_interval_id": kwargs["final_validity_interval_id"],
            "checkpoint_id": kwargs["checkpoint_id"],
            "provider_sequence_num": kwargs["final_sequence_num"],
            "receive_ordinal": kwargs["final_receive_ordinal"],
            "event_ordinal": kwargs["final_event_ordinal"],
            "state_hash": kwargs["final_state_hash"],
        }
        return SimpleNamespace(
            inserted_snapshot_count=len(captured["snapshots"]),
            noop_snapshot_count=0,
            inserted_batch_count=len(captured["batches"]),
            noop_batch_count=0,
            inserted_validity_count=len(captured["validity_versions"]),
            max_commit_seq=len(self.book_ingests),
        )

    def ingest_market_state_features(self, **kwargs):
        count = sum(len(tuple(values)) for values in kwargs.values())
        self.feature_count += count
        return SimpleNamespace(inserted_count=count, noop_count=0)

    def record_quality_event(self, _claim, **kwargs):
        self.quality.append(dict(kwargs))
        return f"quality-{len(self.quality)}"

    def link_book_quality_event(self, _claim, **_kwargs):
        return None

    def append_session_event(self, _claim, **kwargs):
        self.events.append(dict(kwargs))
        return f"event-{kwargs['event_ordinal']}"

    def get_book_reconstruction_state(self, **kwargs):
        if self.reconstruction is None:
            return None
        if (
            self.reconstruction["definition_id"] != kwargs["definition_id"]
            or self.reconstruction["session_id"] != kwargs["session_id"]
            or self.reconstruction["connection_epoch"]
            != kwargs["connection_epoch"]
        ):
            return None
        return dict(self.reconstruction)

    def list_book_checkpoints(self, **_kwargs):
        return [dict(row) for row in self.checkpoint_rows]

    def get_book_validity_opening(self, *, interval_id, **_kwargs):
        row = self.validity_openings.get(interval_id)
        return dict(row) if row is not None else None

    def list_session_manifests(self, **_kwargs):
        return [dict(row) for row in self.manifests]


def _captured_l2_frames() -> tuple[bytes, bytes]:
    fixture = (
        Path(__file__).parents[1]
        / "fixtures/providers/coinbase/market_structure_phase0/raw_frames.json.gz"
    )
    with gzip.open(fixture, "rt", encoding="utf-8") as handle:
        rows = json.load(handle)["frames"]
    selected: list[bytes] = []
    for row in rows:
        payload = json.loads(row["raw_frame"])
        if payload.get("channel") != "l2_data":
            continue
        events = [
            event
            for event in payload.get("events", [])
            if event.get("product_id") == "BTC-USD"
        ]
        if not events:
            continue
        selected.append(row["raw_frame"].encode("utf-8"))
        if len(selected) == 2:
            return selected[0], selected[1]
    raise AssertionError("fixture does not contain two BTC-USD Level 2 frames")


def test_continuous_level2_segments_preserve_state_and_close_restart_gap(
    tmp_path: Path,
) -> None:
    repository = _Level2Repository()
    collector = ContinuousMarketStructureCollector(repository=repository)
    claim = _l2_claim(session_id="l2-session")
    states: dict[int, _EpochProjectionState] = {}
    counters = {
        "manifests": 0,
        "quality_events": 0,
        "book_snapshots": 0,
        "book_batches": 0,
        "book_mutations": 0,
        "book_checkpoints": 0,
        "book_features": 0,
    }
    event_writer = _SessionEventWriter(repository, claim)
    parser = CoinbaseMessageParser()
    analyzer = _CaptureAnalyzer(claim, primary_channel="level2")
    frames = _captured_l2_frames()
    object_store = FilesystemRawArchiveObjectStore(tmp_path / "objects")

    for ordinal, frame in enumerate(frames, start=1):
        segment = DurableRawSpoolSegment(
            root=tmp_path / "spool",
            definition_id=claim.definition_id,
            session_id=claim.session_id,
            connection_epoch=0,
            segment_ordinal=ordinal - 1,
        )
        message = ProviderRawMessage.build(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            stream_session_id=claim.session_id,
            connection_epoch=0,
            receive_ordinal=ordinal,
            received_at=datetime.now(UTC) + timedelta(milliseconds=ordinal),
            raw_frame=frame,
        )
        record = RawStreamRecord.from_provider_message(
            message,
            definition_id=claim.definition_id,
            spool_segment_id=segment.spool_segment_id,
            provider_product_id=claim.provider_product_id,
            requested_channel="level2",
            observed_channel="l2_data",
        )
        segment.append(record)
        segment.seal()
        events = parser.parse_raw(
            frame,
            received_at=message.received_at,
            raw_ref={"raw_record_id": record.raw_record_id},
        )
        analyzer.observe(record, events)
        terminal = ordinal == len(frames)
        external_quality = (
            (
                {
                    "connection_epoch": 0,
                    "receive_ordinal": ordinal,
                    "channel": "level2",
                    "classification": "collector_restart_gap",
                    "reason": "test restart boundary",
                    "detected_at": datetime.now(UTC),
                    "raw_record_id": record.raw_record_id,
                    "sequence_before": analyzer.last_sequence,
                    "sequence_after": None,
                    "invalidating": True,
                    "evidence": {"restart_recovery": True},
                },
            )
            if terminal
            else ()
        )
        collector._finalize_level2_segment(
            claim=claim,
            checkpoint=_SegmentCheckpoint(
                segment=segment,
                analysis=analyzer.finalize(),
                terminal=terminal,
                terminal_reason="test_restart",
                closing_session_event_id=None,
                external_quality=external_quality,
            ),
            object_store=object_store,
            temporary_root=tmp_path / "tmp",
            event_writer=event_writer,
            states=states,
            counters=counters,
        )
        analyzer.quality.clear()

    assert repository.archive_count == 2
    assert counters["book_snapshots"] >= 1
    assert counters["book_batches"] >= 1
    assert counters["book_checkpoints"] >= 1
    assert repository.feature_count > 0
    assert repository.book_ingests[0]["lifecycle"].value == "valid"
    assert repository.book_ingests[-1]["lifecycle"].value == "invalid"
    assert any(
        row.status.value == "closed_invalidated"
        for row in repository.book_ingests[-1]["validity_versions"]
    )
    assert states == {}
    assert not tuple((tmp_path / "spool").rglob("*.open"))
    assert not tuple((tmp_path / "spool").rglob("*.sealed"))


def test_continuous_level2_restart_restores_checkpoint_plus_raw_delta(
    tmp_path: Path,
) -> None:
    repository = _Level2Repository()
    collector = ContinuousMarketStructureCollector(repository=repository)
    claim = _l2_claim(session_id="l2-restore-session")
    segment = DurableRawSpoolSegment(
        root=tmp_path / "spool",
        definition_id=claim.definition_id,
        session_id=claim.session_id,
        connection_epoch=0,
        segment_ordinal=0,
    )
    parser = CoinbaseMessageParser()
    analyzer = _CaptureAnalyzer(claim, primary_channel="level2")
    for ordinal, frame in enumerate(_captured_l2_frames(), start=1):
        message = ProviderRawMessage.build(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            stream_session_id=claim.session_id,
            connection_epoch=0,
            receive_ordinal=ordinal,
            received_at=datetime.now(UTC) + timedelta(milliseconds=ordinal),
            raw_frame=frame,
        )
        record = RawStreamRecord.from_provider_message(
            message,
            definition_id=claim.definition_id,
            spool_segment_id=segment.spool_segment_id,
            provider_product_id=claim.provider_product_id,
            requested_channel="level2",
            observed_channel="l2_data",
        )
        segment.append(record)
        events = parser.parse_raw(
            frame,
            received_at=message.received_at,
            raw_ref={"raw_record_id": record.raw_record_id},
        )
        analyzer.observe(record, events)
    segment.seal()
    states: dict[int, _EpochProjectionState] = {}
    counters = {
        "manifests": 0,
        "quality_events": 0,
        "book_snapshots": 0,
        "book_batches": 0,
        "book_mutations": 0,
        "book_checkpoints": 0,
        "book_features": 0,
    }
    object_store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    collector._finalize_level2_segment(
        claim=claim,
        checkpoint=_SegmentCheckpoint(
            segment=segment,
            analysis=analyzer.finalize(),
            terminal=False,
            terminal_reason=None,
            closing_session_event_id=None,
        ),
        object_store=object_store,
        temporary_root=tmp_path / "tmp",
        event_writer=_SessionEventWriter(repository, claim),
        states=states,
        counters=counters,
    )
    expected_hash = states[0].book_reducer.current_state_hash

    restored, _parser, _analyzer = collector._restore_level2_projection(
        claim=claim,
        connection_epoch=0,
        object_store=object_store,
        excluded_spool_segment_ids=set(),
    )

    assert restored.book_reducer is not None
    assert restored.book_reducer.current_state_hash == expected_hash
    assert restored.book_last_position is not None
    assert restored.book_last_position.receive_ordinal == 2
    assert restored.book_checkpoint_id == repository.checkpoint_ids[-1]


def test_continuous_runtime_accepts_registered_non_coinbase_transport_and_projection(
    tmp_path: Path,
) -> None:
    claim = StreamClaim(
        definition_id="future-provider-quotes",
        definition_generation=1,
        source_id=1,
        series_id=2,
        provider="FUTURE_PROVIDER",
        venue="DIRECT",
        provider_product_id="ABC-USD",
        channels=("quotes", "heartbeats"),
        auth_mode="public",
        contract_version="market.quote.v1",
        max_spool_bytes=1024**2,
        max_segment_bytes=1024,
        config={"runtime_policy": {}},
        owner_id="worker",
        lease_token="token",
        lease_generation=1,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
        session_id="future-session",
    )

    class _Repository:
        def __init__(self):
            self.events = []
            self.released = False

        def list_stream_definitions(self, **_kwargs):
            return [
                {
                    "id": claim.definition_id,
                    "provider": claim.provider,
                    "venue": claim.venue,
                    "channels": claim.channels,
                    "config": claim.config,
                }
            ]

        def claim_stream(self, **_kwargs):
            return claim

        def append_session_event(self, _claim, **kwargs):
            self.events.append(dict(kwargs))
            return f"future-event-{len(self.events)}"

        def heartbeat(self, _claim, **_kwargs):
            return datetime.now(UTC)

        def release(self, _claim):
            self.released = True

    stop = {"requested": False}

    class _Stream:
        async def connect(self):
            return None

        async def subscribe(self, _subscriptions):
            return None

        async def raw_messages(self):
            yield ProviderRawMessage.build(
                provider="FUTURE_PROVIDER",
                venue="DIRECT",
                stream_session_id=claim.session_id,
                connection_epoch=0,
                receive_ordinal=1,
                received_at="2026-08-20T12:00:00Z",
                raw_frame='{"channel":"quotes","bid":"10","ask":"11"}',
            )
            stop["requested"] = True

        async def close(self):
            return None

    class _Parser:
        def parse_raw(self, _raw_frame, **_kwargs):
            return ()

    class _Transport:
        transport_id = "future.websocket.v1"

        def supports(self, definition):
            return definition["provider"] == "FUTURE_PROVIDER"

        def create_stream(self, _claim):
            return _Stream()

        def create_parser(self, _claim):
            return _Parser()

        def subscription(self, _claim):
            return object()

        def observed_channel(self, _raw_frame):
            return "quotes"

    class _Projection:
        projection_id = "market.quote_projection.v1"
        primary_channel = "quotes"
        channels = ("quotes", "heartbeats")
        disconnect_invalidates = False

        class _Analyzer:
            def __init__(self):
                self.raw_count = 0
                self.last_heartbeat_at = datetime.now(UTC)
                self.last_record = None
                self.last_sequence = None
                self.quality = []
                self.quotes = 0

            def observe(self, record, _events):
                self.raw_count += 1
                self.quotes += 1
                self.last_record = record

            def observe_idle(self, _now):
                return None

            def finalize(self):
                return {"quotes": self.quotes}

        def create_analyzer(self, _claim):
            return self._Analyzer()

        def counters(self):
            return {"quotes": 0}

        def finalize_segment(self, _runtime, **kwargs):
            kwargs["counters"]["quotes"] += kwargs["checkpoint"].analysis[
                "quotes"
            ]

        def begin_recovery(self, _runtime, **_kwargs):
            return None

        def recover_segment(self, _runtime, **_kwargs):
            raise AssertionError("no segment should be recovered")

        def complete_recovery(self, _runtime, **_kwargs):
            return {}

    repository = _Repository()
    with pytest.raises(ValueError, match="both transport and projection adapters"):
        asyncio.run(
            ContinuousStreamRuntime(repository=repository).run(
                definition_id=claim.definition_id,
                owner_id="worker",
                stop_requested=lambda: True,
                bounded_validation=False,
                storage_root=tmp_path,
            )
        )

    result = asyncio.run(
        ContinuousStreamRuntime(repository=repository).run(
            definition_id=claim.definition_id,
            owner_id="worker",
            stop_requested=lambda: stop["requested"],
            bounded_validation=False,
            storage_root=tmp_path,
            transport=_Transport(),
            projection=_Projection(),
        )
    )

    assert result["status"] == "stopped"
    assert result["raw_records"] == 1
    assert result["quotes"] == 1
    assert repository.released is True
    assert [row["event_type"] for row in repository.events] == [
        "connected",
        "subscription_sent",
        "continuous_capture_stopped",
    ]
