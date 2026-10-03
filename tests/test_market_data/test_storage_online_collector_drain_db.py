"""Real collector drain/recovery under online capture; transport is scripted."""
import asyncio
from datetime import UTC, datetime
import json
import hashlib
import os
import stat
from pathlib import Path
from threading import Event
from types import FunctionType
from uuid import uuid4
from time import monotonic

import pytest
from sqlalchemy import text

from data_providers.streams.contracts import ProviderRawMessage
from portal.backend.service.market.continuous_stream_collector import (
    CoinbaseContinuousTransportAdapter, CoinbaseMarketTradeProjectionAdapter,
    ContinuousStreamRuntime,
)
from portal.backend.service.market.collector_supervisor import (
    ContinuousCollectorSupervisor, CollectorAdapterRegistry,
)
from portal.backend.service.storage.repos import market_data
from tests.test_market_data.test_continuous_collector_supervisor import (
    _Repository as SupervisorDiscoveryFixture, _OperationsRepository,
)
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


@pytest.mark.parametrize("fail_before_canonical_ack,supervised,after_switch,canonical_committed", [
    pytest.param(False, False, False, False, id="runtime-clean"),
    pytest.param(True, False, False, False, id="runtime-recovery"),
    pytest.param(False, True, False, False, id="supervisor-clean"),
    pytest.param(True, True, False, False, id="supervisor-recovery"),
    pytest.param(True, True, True, False, id="supervisor-recovery-after-switch"),
    pytest.param(True, True, True, True, id="supervisor-committed-recovery-after-switch"),
])
def test_real_collector_stop_waits_for_ack_and_recovers_retained_wal(
        storage, tmp_path, monkeypatch, fail_before_canonical_ack, supervised, after_switch,
        canonical_committed):
    engine, options, source, book = _prepare(
        storage, tmp_path, monkeypatch, prepare_captures=False,
        destination_directory=(Path("/qt-history")/("collector-switch-"+uuid4().hex)/"objects"
                               if after_switch else None))
    prepare = dict(placement=storage.copy_plan, attempt_seconds=180,
                   **{k: v for k, v in options.items() if k not in ("page_rows", "max_page_bytes")})
    online.prepare_attempt(engine, **prepare)
    with engine.connect() as conn:
        original_capture = conn.scalar(text("SELECT to_jsonb(c) FROM qt_fact_header_cutover_v2.capture c"))
        frozen = _frozen_records(conn)
    candidate_day = market_data.current_fact_storage_day
    candidate_range = market_data.CANONICAL_RANGE_ROW_FROM
    candidate_ingest = market_data.PostgresMarketDataRepository._ingest_canonical_rows_with_session
    runtime_root = source
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
        max_segment_bytes=128*1024**2, enabled=supervised,
        config={"product_definition_version_id": book.claim.config["product_definition_version_id"]})
    stop = Event()
    entered = Event()
    release = Event()
    frames = []
    supervisor_stop = []
    runtime_results = []

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
                        "product_id": "BTC-USD", "trade_id": "online-drain-trade-"+str(len(frames)),
                        "price": "100", "size": "0.01", "side": "BUY", "time": timestamp}]}]}))
            frames.append(message)
            yield message
            stop.set()  # The fixture has delivered its one frame.
            if supervised:
                while not supervisor_stop[0]():
                    await asyncio.sleep(0.01)

        async def close(self):
            pass

    transport = CoinbaseContinuousTransportAdapter(stream_factory=Stream)
    projection = CoinbaseMarketTradeProjectionAdapter()
    original_ingest = structures.ingest_trades

    class Discovery(SupervisorDiscoveryFixture):
        # Controlled discovery/safety metadata only. The collector's definition,
        # claim, publication and canonical acknowledgement use actual PostgreSQL.
        def list_stream_definitions(self):
            return [dict(super().list_stream_definitions()[0], id=definition_id)]

    class Adapter:
        adapter_id = "fixture.real_coinbase_runtime"
        def supports(self, definition):
            return definition["id"] == definition_id
        def registration_errors(self, definition):
            return []
        async def run(self, *, stop_requested, **kwargs):
            supervisor_stop.append(stop_requested)
            result = await ContinuousStreamRuntime(repository=structures).run(
                **kwargs, stop_requested=stop_requested, storage_root=source,
                projection=projection, transport=transport)
            runtime_results.append(result)
            return result

    supervisor = ContinuousCollectorSupervisor(owner_id="real-drain-supervisor",
        repository=Discovery(), operations_repository=_OperationsRepository(),
        registry=CollectorAdapterRegistry((Adapter(),)), poll_seconds=0.25)

    def blocked_ingest(*args, **kwargs):
        entered.set()
        assert release.wait(20), "fixture canonical acknowledgement gate expired"
        if fail_before_canonical_ack:
            if canonical_committed:
                original_ingest(*args, **kwargs)
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

    def file_snapshot(root):
        return {str(p): (p.read_bytes(), p.stat().st_uid, p.stat().st_gid,
                        stat.S_IMODE(p.stat().st_mode))
                for p in root.rglob("*") if p.is_file()}

    baseline_pending = observe()["pending_files"]
    baseline_spools = file_snapshot(source/"spool")
    async def exercise():
        if supervised:
            supervisor.start()
            assert await asyncio.to_thread(stop.wait, 15), "collector did not receive fixture frame"
            task = asyncio.create_task(asyncio.to_thread(supervisor.stop, timeout_seconds=15))
        else:
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
            expected = "supervisor_stop_failed" if supervised else "fixture_before_canonical_ack"
            with pytest.raises(RuntimeError, match=expected):
                await asyncio.wait_for(task, 20)
            if supervised:
                assert supervisor.snapshot()["state"] == "failed"
                assert "fixture_before_canonical_ack" in supervisor.snapshot()["errors"][definition_id]
            return None
        completed = await asyncio.wait_for(task, 20)
        if supervised:
            assert supervisor.snapshot()["state"] == "stopped"
            assert not supervisor._thread.is_alive()
            return runtime_results[0]
        return completed

    try:
        with monkeypatch.context() as blocked:
            blocked.setattr(structures, "ingest_trades", blocked_ingest)
            result = asyncio.run(exercise())
    finally:
        release.set()
        if supervisor._thread.is_alive():
            # Cleanup must not leave a fixture collector serving the next DB.
            supervisor.stop(timeout_seconds=20)
    assert len(frames) == 1
    if fail_before_canonical_ack:
        assert counts() == (int(canonical_committed), 1, 1)
        assert observe()["pending_files"] == baseline_pending+1
        retained = {str(p): p.read_bytes() for p in (source/"spool"/definition_id).rglob("*.sealed")}
        assert retained
        if after_switch:
            preserved_objects = file_snapshot(options["source_root"])
            preserved_spools = file_snapshot(source/"spool")
            # All fixture publishers have joined. This internal DB switch is not
            # a host publisher-exclusion certificate or a production entrypoint.
            from scripts.automation.storage_online_controller import OnlineController
            from scripts.automation.storage_online_operation import prepare_background
            from scripts.db import fact_header_v2_online_proof as protection
            with engine.begin() as conn:
                protection.prepare(conn)
                started = conn.scalar(text("SELECT prepared_at FROM qt_fact_header_cutover_v2.capture"))
            runtime_root = options["destination_root"].parent
            monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(runtime_root))
            monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", storage.copy_plan.history.filesystem_uuid)
            with OnlineController(engine, placement=storage.copy_plan,
                    expected_started_at=started.isoformat(), max_objects=128,
                    max_bytes=64*1024**2, **options) as worker:
                # Fresh online captures use the supported serial staging order;
                # the historical held-copy helper cannot prepare this schema.
                def exchange(operation, **fields):
                    return worker.command(dict(controller_id=worker.controller_id,
                        sequence=worker._sequence+1, operation=operation, **fields))
                prepare_background(exchange, preparation_seconds=60)
                worker.commit_database(deadline=monotonic()+30)
                assert worker.inspect_outcome(deadline=monotonic()+4)["database_handoff_committed"]
            # Preserve the original SSD working/spool root and bytes. Restore
            # candidate v2 ingestion/read/partition behavior for normal recovery.
            assert file_snapshot(source/"spool") == preserved_spools
            assert file_snapshot(options["source_root"]) == preserved_objects
            monkeypatch.setattr(market_data, "current_fact_storage_day", candidate_day)
            monkeypatch.setattr(market_data, "CANONICAL_RANGE_ROW_FROM", candidate_range)
            monkeypatch.setattr(market_data.PostgresMarketDataRepository,
                               "_ingest_canonical_rows_with_session", candidate_ingest)
        # Actual runtime startup recovers before claiming a new transport session.
        # Stop is already requested, so this retry must not receive another frame.
        result = asyncio.run(ContinuousStreamRuntime(repository=structures).run(
            definition_id=definition_id, owner_id="drain-recovery", stop_requested=stop.is_set,
            bounded_validation=True, storage_root=runtime_root, projection=projection, transport=transport))
        assert all(not Path(p).exists() for p in retained)
    assert result["status"] == "stopped"
    assert len(frames) == 1 and counts() == (1, 1, 1)
    assert observe()["pending_files"] == baseline_pending
    assert not observe()["final_switch_authorized"]
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT to_jsonb(c) FROM qt_fact_header_cutover_v2.capture c")) == original_capture
        assert _frozen_records(conn) == frozen
        if not after_switch:
            assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {archives.QUEUE})"))
    if after_switch:
        # Normal recovery is idempotent, and a new provider frame can publish
        # through candidate v2 ingestion into the HDD archive root afterwards.
        stop.clear()
        result = asyncio.run(ContinuousStreamRuntime(repository=structures).run(
            definition_id=definition_id, owner_id="after-switch-intake", stop_requested=stop.is_set,
            bounded_validation=True, storage_root=runtime_root, projection=projection, transport=transport))
        assert result["status"] == "stopped" and len(frames) == 2
        assert counts() == (2, 2, 2)
        with engine.connect() as conn:
            manifests = conn.execute(text("SELECT object_key, object_sha256 FROM market.raw_archive_manifests "
                                           "WHERE definition_id=:definition"), {"definition": definition_id}).all()
            assert len(manifests) == 2
            from market_data.archive import FilesystemRawArchiveObjectStore
            store = FilesystemRawArchiveObjectStore(options["destination_root"])
            for key, digest in manifests:
                path = store.local_path(key)
                assert path.is_relative_to(options["destination_root"])
                assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
            assert _frozen_records(conn) == frozen
        assert file_snapshot(options["source_root"]) == preserved_objects
    final_spools = file_snapshot(source/"spool")
    assert all(final_spools.get(p) == value for p, value in baseline_spools.items())
    assert observe()["pending_files"] == baseline_pending
    print("QT_COLLECTOR_DRAIN_RESULT="+json.dumps({
        "supervisor_owned_stop": supervised, "recovered_after_guarded_database_switch": after_switch,
        "failure_before_canonical_ack": fail_before_canonical_ack,
        "canonical_fact_committed_before_lost_ack": canonical_committed,
        "new_frame_after_switch": after_switch,
        "real_runtime_waited_for_finalizer": True, "facts_manifests_mappings": counts(),
        "preexisting_pending_spools_preserved": baseline_pending,
        "scripted_transport": True, "host_signal_or_switch_authority": False}))
