from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
)
from market_data.structure import RawStreamRecord
from portal.backend.service.market.continuous_stream_collector import (
    ContinuousMarketStructureCollector,
)
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
