import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path

import pytest
from sqlalchemy import text

from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import FilesystemRawArchiveObjectStore, RawArchiveReadLimits
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.contracts import SourceIdentity
from market_data.fact_archive import archive_evidence_hash
from market_data.market_state import BBO_FACT_TYPE, BBO_FACT_VERSION, DEPTH_FACT_TYPE, DEPTH_FACT_VERSION
from portal.backend.service.market.continuous_stream_collector import (
    CoinbaseContinuousTransportAdapter, CoinbaseLevel2BookProjectionAdapter,
    CoinbaseMarketTradeProjectionAdapter, ContinuousStreamRuntime,
)
from portal.backend.service.storage.repos import market_structure
from scripts.db import canonical_cold_provider_boundary as boundary
from scripts.db import canonical_cold_collector_drain as drain
from tests.test_market_data.test_fact_storage_tiers_db import storage  # noqa: F401


pytestmark = pytest.mark.db


def _definition(storage, structures, channel, product):
    name = f"fixture-boundary-{channel}"
    fact_type = "market.trade" if channel == "market_trades" else "market.l2_book"
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream", adapter_version=name)
    source_id = storage.repo.register_source(source)
    series_id = storage.repo.register_series(instrument_id="storage-fixture", fact_type=fact_type,
                                            contract_version=f"{fact_type}.v1", timeframe_seconds=None)
    now = datetime.now(UTC)
    product_version = f"{name}.product.v1"
    structures.register_product_definition(definition_version_id=product_version, source_id=source_id,
        instrument_id="storage-fixture", provider_product_id=product, product_type="spot", venue=source.venue,
        status="fixture", base_currency="BTC", quote_currency="USD", provider_size_unit="base", contract_size=None,
        price_increment=None, base_increment=None, effective_at=now, received_at=now, provenance={"fixture": name})
    config = {"product_definition_version_id": product_version, "provider_size_unit": "base",
              "base_currency": "BTC", "quote_currency": "USD"}
    if channel == "level2":
        for api_key, fact, schema in (("bbo_series_id", BBO_FACT_TYPE, BBO_FACT_VERSION),
                                      ("depth_series_id", DEPTH_FACT_TYPE, DEPTH_FACT_VERSION)):
            config[api_key] = storage.repo.register_series(instrument_id="storage-fixture", fact_type=fact,
                                                           contract_version=schema, timeframe_seconds=1)
    else:
        for key, fact in (("aggregate_series_ids", "market.trade_flow"), ("flow_feature_series_ids", "market.trade_flow_feature")):
            config[key] = {str(interval): storage.repo.register_series(instrument_id="storage-fixture", fact_type=fact,
                contract_version=f"{fact}.v1", timeframe_seconds=interval) for interval in (1, 60)}
    structures.upsert_stream_definition(definition_id=name, source_id=source_id, series_id=series_id,
        provider=source.provider, venue=source.venue, provider_product_id=product, channels=(channel, "heartbeats"),
        auth_mode="public", contract_version=f"{fact_type}.v1", max_spool_bytes=1024**2,
        max_segment_bytes=256 * 1024, config=config)
    return name


def _messages(claim, channel, *, resumed, inject_gap):
    now = datetime.now(UTC)
    for index in range(4):
        when = (now + timedelta(microseconds=index)).isoformat()
        sequence = index if not (inject_gap and index == 3) else index + 1
        if index == 0:
            data = {"channel": "subscriptions", "events": [{"subscriptions": {channel: [claim.provider_product_id]}}]}
        elif index == 1:
            data = {"channel": "heartbeats", "events": [{"current_time": when, "heartbeat_counter": "1"}]}
        elif channel == "market_trades":
            data = {"channel": channel, "events": [{"type": "snapshot" if index == 2 else "update", "trades": [{
                "trade_id": str((1000 if resumed else 0) + index), "product_id": claim.provider_product_id,
                "price": "100", "size": "2", "side": "BUY", "time": when}]}]}
        else:
            levels = [("bid", "99", "10"), ("offer", "101", "11")] if index == 2 else [("bid", "99", "12")]
            data = {"channel": "l2_data", "events": [{"type": "snapshot" if index == 2 else "update",
                "product_id": claim.provider_product_id, "updates": [
                    {"side": side, "price_level": price, "new_quantity": quantity, "event_time": when}
                    for side, price, quantity in levels]}]}
        yield ProviderRawMessage.build(provider=claim.provider, venue=claim.venue, stream_session_id=claim.session_id,
            connection_epoch=0, receive_ordinal=index + 1, received_at=when,
            raw_frame=json.dumps(data | {"timestamp": when, "sequence_num": sequence}))


class ScriptedCoinbaseTransport(CoinbaseContinuousTransportAdapter):
    def __init__(self, channel, *, inject_gap, resumed, received, stopped):
        self.channel = channel
        self.inject_gap = inject_gap
        self.resumed = resumed
        self.received = received
        self.stopped = stopped
        self.claim = None

    def create_stream(self, claim):
        self.claim = claim
        return self

    async def connect(self):
        return 0

    async def subscribe(self, subscriptions):
        assert len(subscriptions) == 1

    async def raw_messages(self):
        for row in _messages(self.claim, self.channel, resumed=self.resumed, inject_gap=self.inject_gap):
            # Stop while the runtime consumes the invalidating frame. Its
            # finalizer still persists typed quality and the terminal receipt.
            if self.inject_gap and row.receive_ordinal == 4:
                self.stopped.set()
            yield row
        self.received.set()
        await self.stopped.wait()

    async def close(self):
        pass


@pytest.mark.parametrize("channel", ["market_trades", "level2"])
def test_native_provider_boundary_drain_and_restart(storage, tmp_path, monkeypatch, channel):
    monkeypatch.setattr(market_structure, "db", storage.database)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(tmp_path))
    structures = market_structure.market_structure_repository
    product = "BIP-FIXTURE" if channel == "level2" else "BTC-FIXTURE"
    definition_id = _definition(storage, structures, channel, product)
    with storage.database._engine.begin() as conn:
        conn.execute(text("""UPDATE market.stream_definitions SET enabled=true,desired_state='running'
            WHERE id=:id"""), {"id": definition_id})
        conn.execute(text("""
            INSERT INTO market.collection_definitions
                (id,source_id,series_id,enabled,desired_state,poll_interval_seconds,next_scheduled_at,available_at)
            SELECT 'fixture-scheduled-poll',source_id,series_id,true,'running',60,now(),now()
            FROM market.stream_definitions WHERE id=:id
        """), {"id": definition_id})
    if channel == "level2":
        prior = structures.claim_stream(definition_id=definition_id, owner_id="fixture-prior", lease_seconds=30, bounded=True)
        structures.record_quality_event(prior, connection_epoch=1, receive_ordinal=2, channel=channel,
            classification="book_invalid", reason="canonical_raw_reference_prefix_budget_exceeded (fixture)", detected_at=datetime.now(UTC))
        structures.release(prior)
    runtime = ContinuousStreamRuntime(repository=structures)
    projection = CoinbaseMarketTradeProjectionAdapter() if channel == "market_trades" else CoinbaseLevel2BookProjectionAdapter()
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    limits = drain.CollectorDrainLimits(ArchiveVerificationLimits(max_objects=16, max_bytes=16 * 1024**2),
                                       RawArchiveReadLimits(max_rows=64), max_events=64, max_raw_rows=64,
                                       max_spool_entries=64)

    def snapshot(phase):
        with storage.database._engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            return boundary.capture(conn, phase=phase, definition_id=definition_id,
                                    spool_root=tmp_path / "spool", object_store=store)

    async def run():
        received, stopped = asyncio.Event(), asyncio.Event()
        transport = ScriptedCoinbaseTransport(channel, inject_gap=False, resumed=False, received=received, stopped=stopped)
        task = asyncio.create_task(runtime.run(definition_id=definition_id, owner_id="fixture-old", stop_requested=stopped.is_set,
            bounded_validation=True, storage_root=tmp_path, projection=projection, transport=transport))
        await asyncio.wait_for(received.wait(), timeout=10)
        pre_stop = snapshot("pre_stop")
        with storage.database._engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            with pytest.raises(RuntimeError, match="approved_roster_changed"):
                drain.capture_collector_tail(conn, stream_ids=(definition_id,), poll_ids=(), check=lambda: None)
            collector_tail = drain.capture_collector_tail(
                conn, stream_ids=(definition_id,), poll_ids=("fixture-scheduled-poll",), check=lambda: None,
            )
        stopped.set()
        first = await asyncio.wait_for(task, timeout=15)
        drained = snapshot("drained")
        with storage.database._engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            parameters = dict(before=collector_tail, source_commit_seq=drained["watermark"], spool_root=tmp_path / "spool",
                              store=store, limits=limits, check=lambda: None)
            assert collector_tail["source_commit_seq"] == pre_stop["watermark"]
            with pytest.raises(RuntimeError, match="commit_boundary_changed"):
                drain.verify_collector_drain(conn, **{**parameters, "source_commit_seq": drained["watermark"] + 1})
            pending = tmp_path / "spool" / "pending.open"
            pending.write_bytes(b"fixture")
            with pytest.raises(RuntimeError, match="pending_spool"):
                drain.verify_collector_drain(conn, **parameters)
            pending.unlink()
            with pytest.raises(RuntimeError, match="raw_row_budget_exceeded"):
                drain.verify_collector_drain(conn, **{**parameters, "limits": replace(limits, max_raw_rows=3)})
            manifest_path = store.local_path(drained["manifests"][-1]["object_key"])
            original = manifest_path.read_bytes()
            manifest_path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
            try:
                with pytest.raises(RuntimeError, match="archive_verification_checksum_mismatch"):
                    drain.verify_collector_drain(conn, **parameters)
            finally:
                manifest_path.write_bytes(original)
            verified_drain = drain.verify_collector_drain(conn, **parameters)
            assert verified_drain["pre_stop_sha256"] == collector_tail["sha256"]
            assert verified_drain["streams"][0]["last_receive_ordinal"] == 4
            assert verified_drain["streams"][0]["segments"][0]["mappings"]["count"] == 4
            assert verified_drain["streams"][0]["interruption"]["missing_events"] is None
            assert verified_drain["polls"][0]["first_due_at_risk"]
        received, stopped = asyncio.Event(), asyncio.Event()
        transport = ScriptedCoinbaseTransport(channel, inject_gap=channel == "level2", resumed=True, received=received, stopped=stopped)
        task = asyncio.create_task(runtime.run(definition_id=definition_id, owner_id="fixture-new", stop_requested=stopped.is_set,
            bounded_validation=True, storage_root=tmp_path, projection=projection, transport=transport))
        if channel == "market_trades":
            await asyncio.wait_for(received.wait(), timeout=10)
            stopped.set()
        second = await asyncio.wait_for(task, timeout=15)
        return pre_stop, drained, snapshot("resumed"), first, second

    pre_stop, drained, resumed, first, second = asyncio.run(run())
    output = boundary.validate(pre_stop, drained, resumed)
    assert first["spool_backlog_bytes"] == second["spool_backlog_bytes"] == 0
    assert output["pre_stop_tail_records"] == first["raw_records"] == second["raw_records"] == 4
    assert output["interruption"]["missing_events"] is None
    if channel == "level2":
        assert len(output["pre_existing_bip_prefix_evidence"]) == 1
        assert any(row["classification"] == "sequence_gap" for row in output["post_start_events"])
    else:
        assert output["new_interval"]["status"] == "closed_valid"
        assert not output["pre_existing_events"] and not output["post_start_events"]
    bad = deepcopy(drained)
    bad["raw"].pop()
    bad["sha256"] = archive_evidence_hash({key: value for key, value in bad.items() if key != "sha256"})
    with pytest.raises(RuntimeError, match="lost or changed a durable pre-stop raw receipt"):
        boundary.validate(pre_stop, bad, resumed)
    if directory := os.getenv("QT_PROVIDER_BOUNDARY_FIXTURE_OUTPUT"):
        destination = Path(directory)
        for evidence in (pre_stop, drained, resumed):
            (destination / f"{channel}-{evidence['phase']}.json").write_text(json.dumps(evidence, indent=2) + "\n")
        (destination / f"{channel}-result.json").write_text(json.dumps(output, indent=2) + "\n")
