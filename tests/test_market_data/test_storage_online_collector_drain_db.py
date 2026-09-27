"""Real collector drain/recovery under online capture; transport is scripted."""
import asyncio
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from threading import Event
from types import FunctionType
from time import monotonic

import pytest
from sqlalchemy import text

from data_providers.streams.contracts import ProviderRawMessage
from portal.backend.service.market.continuous_stream_collector import (
    CoinbaseContinuousTransportAdapter, CoinbaseMarketTradeProjectionAdapter,
    ContinuousStreamRuntime,
)
from portal.backend.service.storage.repos import market_data
from scripts.automation.storage_online_drain import inspect_spool
from scripts.db import fact_header_v2_online as online
from scripts.db import archive_root_v2_online as archives
from tests.test_market_data.test_archive_online_copy_db import _prepare
from tests.test_market_data.test_fact_header_copy_db import _frozen_records
from tests.test_market_data.test_fact_storage_tiers_db import storage
from tests.test_market_data.tiered_v1_fixture import ensure_v1_payload_partition
from tests.test_market_data.tiered_v1_ingestion import SourceV1Ingestion

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                reason="requires owned SSD/HDD storage-demo topology")]


@pytest.mark.parametrize("fail_before_canonical_ack", [False, True])
def test_real_collector_stop_waits_for_ack_and_recovers_retained_wal(
        storage, tmp_path, monkeypatch, fail_before_canonical_ack):
    engine, options, source, book = _prepare(
        storage, tmp_path, monkeypatch, prepare_captures=False)
    prepare = dict(placement=storage.copy_plan, attempt_seconds=180,
                   **{k: v for k, v in options.items() if k not in ("page_rows", "max_page_bytes")})
    online.prepare_attempt(engine, **prepare)
    with engine.connect() as conn:
        original_capture = conn.scalar(text("SELECT to_jsonb(c) FROM qt_fact_header_cutover_v2.capture c"))
        frozen = _frozen_records(conn)
    # Execute the deployed v1 partition boundary, not implicit v2 provisioning.
    def source_day(session):
        ensure_v1_payload_partition(session.connection(), storage.today)
        return storage.today
    monkeypatch.setattr(market_data, "current_fact_storage_day", source_day)
    # The deployed range reader uses the same projection/filter with the v1
    # fact_versions FROM clause. No v2 range function exists before cutover.
    monkeypatch.setattr(market_data, "CANONICAL_RANGE_ROW_FROM", market_data.CANONICAL_ROW_FROM)
    v1_ingest = SourceV1Ingestion._ingest_canonical_rows_with_session
    source_ingest = FunctionType(v1_ingest.__code__, vars(market_data),
                                argdefs=v1_ingest.__defaults__)
    source_ingest.__kwdefaults__ = v1_ingest.__kwdefaults__
    monkeypatch.setattr(market_data.PostgresMarketDataRepository,
                        "_ingest_canonical_rows_with_session", source_ingest)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(source))
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(source))
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", storage.copy_plan.recent.filesystem_uuid)
    monkeypatch.setenv("QT_MARKET_DATA_WORKING_EXPECTED_UUID", storage.copy_plan.recent.filesystem_uuid)
    structures = book.structures
    definition_id = "online-collector-drain"
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade",
        contract_version="market.trade.v1", timeframe_seconds=None)
    structures.upsert_stream_definition(definition_id=definition_id,
        source_id=book.source_id, series_id=series, provider=book.source.provider,
        venue=book.source.venue, provider_product_id="BTC-USD",
        channels=("market_trades", "heartbeats"), auth_mode="public",
        contract_version="market.trade.v1", max_spool_bytes=1024**3,
        max_segment_bytes=128*1024**2,
        config={"product_definition_version_id": book.claim.config["product_definition_version_id"]})
    stop = Event()
    entered = Event()
    release = Event()
    frames = []

    class Stream:
        def __init__(self, **kwargs):
            self.session_id = kwargs["stream_session_id"]

        async def connect(self):
            return 0

        async def subscribe(self, subscriptions):
            assert subscriptions

        async def raw_messages(self):
            timestamp = datetime.now(UTC).isoformat()
            message = ProviderRawMessage.build(provider=book.source.provider, venue=book.source.venue,
                stream_session_id=self.session_id, connection_epoch=0, receive_ordinal=1,
                received_at=timestamp, raw_frame=json.dumps({
                    "channel": "market_trades", "timestamp": timestamp, "sequence_num": 1,
                    "events": [{"type": "update", "trades": [{
                        "product_id": "BTC-USD", "trade_id": "online-drain-trade",
                        "price": "100", "size": "0.01", "side": "BUY", "time": timestamp}]}]}))
            frames.append(message)
            yield message
            stop.set()

        async def close(self):
            pass

    transport = CoinbaseContinuousTransportAdapter(stream_factory=Stream)
    projection = CoinbaseMarketTradeProjectionAdapter()
    original_ingest = structures.ingest_trades

    def blocked_ingest(*args, **kwargs):
        entered.set()
        assert release.wait(20), "fixture canonical acknowledgement gate expired"
        if fail_before_canonical_ack:
            raise RuntimeError("fixture_before_canonical_ack")
        return original_ingest(*args, **kwargs)

    def observe():
        return inspect_spool(source, deadline=monotonic()+5, max_entries=1000, check=lambda: None)

    def counts():
        with engine.connect() as conn:
            return tuple(conn.scalar(text(sql), {"definition": definition_id, "series": series}) for sql in (
                "SELECT count(*) FROM market.fact_versions WHERE series_id=:series",
                "SELECT count(*) FROM market.raw_archive_manifests WHERE definition_id=:definition",
                "SELECT count(*) FROM market.raw_archive_record_mappings m JOIN market.raw_archive_manifests a ON a.id=m.manifest_id WHERE a.definition_id=:definition"))

    baseline_pending = observe()["pending_files"]
    async def exercise():
        task = asyncio.create_task(ContinuousStreamRuntime(repository=structures).run(
            definition_id=definition_id, owner_id="drain-fixture", stop_requested=stop.is_set,
            bounded_validation=True, storage_root=source, projection=projection, transport=transport))
        try:
            assert await asyncio.to_thread(entered.wait, 15), "collector never reached canonical finalizer"
            assert stop.is_set() and not task.done()
            assert observe()["pending_files"] == baseline_pending+1
            # Archive acknowledgement alone must not remove WAL or report stopped.
            assert counts() == (0, 1, 1)
        finally:
            release.set()
        if fail_before_canonical_ack:
            with pytest.raises(RuntimeError, match="fixture_before_canonical_ack"):
                await asyncio.wait_for(task, 20)
            return None
        return await asyncio.wait_for(task, 20)

    with monkeypatch.context() as blocked:
        blocked.setattr(structures, "ingest_trades", blocked_ingest)
        result = asyncio.run(exercise())
    assert len(frames) == 1
    if fail_before_canonical_ack:
        assert counts() == (0, 1, 1)
        assert observe()["pending_files"] == baseline_pending+1
        retained = {str(p): p.read_bytes() for p in (source/"spool"/definition_id).rglob("*.sealed")}
        assert retained
        # Actual runtime startup recovers before claiming a new transport session.
        # Stop is already requested, so this retry must not receive another frame.
        result = asyncio.run(ContinuousStreamRuntime(repository=structures).run(
            definition_id=definition_id, owner_id="drain-recovery", stop_requested=stop.is_set,
            bounded_validation=True, storage_root=source, projection=projection, transport=transport))
        assert all(not Path(p).exists() for p in retained)
    assert result["status"] == "stopped"
    assert len(frames) == 1 and counts() == (1, 1, 1)
    assert observe()["pending_files"] == baseline_pending
    assert not observe()["final_switch_authorized"]
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT to_jsonb(c) FROM qt_fact_header_cutover_v2.capture c")) == original_capture
        assert _frozen_records(conn) == frozen
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {archives.QUEUE})"))
    print("QT_COLLECTOR_DRAIN_RESULT="+json.dumps({
        "failure_before_canonical_ack": fail_before_canonical_ack,
        "real_runtime_waited_for_finalizer": True, "facts_manifests_mappings": counts(),
        "preexisting_pending_spools_preserved": baseline_pending,
        "scripted_transport": True, "host_signal_or_switch_authority": False}))
