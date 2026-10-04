"""Owned Docker signal fixture: real runtime/publication, scripted transport.

Imported only by the explicit disposable host rehearsal. Discovery, lifecycle
and heartbeat remain test-controlled; no deployed-image readiness is inferred.
"""
import asyncio
from contextlib import contextmanager
from datetime import UTC, date, datetime
import json
import os
from pathlib import Path
from time import monotonic, sleep
from types import FunctionType

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


def prepare_definition(storage, book, root):
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade",
        contract_version="market.trade.v1", timeframe_seconds=None)
    definition = "online-signal-publication"
    book.structures.upsert_stream_definition(definition_id=definition,
        source_id=book.source_id, series_id=series, provider=book.source.provider,
        venue=book.source.venue, provider_product_id="BTC-USD",
        channels=("market_trades", "heartbeats"), auth_mode="public",
        contract_version="market.trade.v1", max_spool_bytes=1024**3,
        max_segment_bytes=128*1024**2, enabled=True,
        config={"product_definition_version_id": book.claim.config["product_definition_version_id"]})
    # Private fixture rendezvous, not a production admission receipt.
    spec=dict(definition=definition, series=series, provider=book.source.provider,
              venue=book.source.venue, day=storage.today.isoformat())
    temporary=root/"real-publication.json.tmp"
    temporary.write_text(json.dumps(spec));temporary.replace(root/"real-publication.json")


async def run_publication(root, *, stop_requested, owner_id, **kwargs):
    from data_providers.streams.contracts import ProviderRawMessage
    from portal.backend.service.market.continuous_stream_collector import (
        CoinbaseContinuousTransportAdapter, CoinbaseMarketTradeProjectionAdapter, ContinuousStreamRuntime)
    from portal.backend.service.storage.repos import market_data, market_structure
    from tests.test_market_data.tiered_v1_fixture import ensure_v1_payload_partition
    from tests.test_market_data.tiered_v1_ingestion import SourceV1Ingestion

    spec=json.loads((root/"real-publication.json").read_text())
    engine=create_engine(os.environ["PG_DSN"])
    # The parent fixture already created/admitted its v1 schema. Do not run the
    # candidate v2 startup bootstrap against that deliberately preserved schema.
    class FixtureDatabase:
        @contextmanager
        def session(self):
            with Session(engine) as session, session.begin():
                yield session
    market_data.db=market_structure.db=FixtureDatabase()
    def source_day(session):
        day=date.fromisoformat(spec["day"])
        ensure_v1_payload_partition(session.connection(), day)
        return day
    market_data.current_fact_storage_day=source_day
    market_data.CANONICAL_RANGE_ROW_FROM=market_data.CANONICAL_ROW_FROM
    frozen=SourceV1Ingestion._ingest_canonical_rows_with_session
    ingest=FunctionType(frozen.__code__,vars(market_data),argdefs=frozen.__defaults__)
    ingest.__kwdefaults__=frozen.__kwdefaults__
    market_data.PostgresMarketDataRepository._ingest_canonical_rows_with_session=ingest
    working=root.parent
    os.environ.update(MARKET_STRUCTURE_STORAGE_ROOT=str(working),
        MARKET_STRUCTURE_WORKING_ROOT=str(working),QT_STORAGE_UDEV_ROOT=str(root/"fixture-udev"),
        QT_MARKET_DATA_EXPECTED_UUID="uuid-copy-ssd",QT_MARKET_DATA_WORKING_EXPECTED_UUID="uuid-copy-ssd")
    udev=root/"fixture-udev";udev.mkdir(exist_ok=True)
    device=working.stat().st_dev
    (udev/f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=uuid-copy-ssd\n")
    structures=market_structure.market_structure_repository
    def counts():
        with engine.connect() as conn:
            return [conn.scalar(text(sql),spec) for sql in (
                "SELECT count(*) FROM market.fact_versions WHERE series_id=:series",
                "SELECT count(*) FROM market.raw_archive_manifests WHERE definition_id=:definition",
                "SELECT count(*) FROM market.raw_archive_record_mappings m JOIN market.raw_archive_manifests a ON a.id=m.manifest_id WHERE a.definition_id=:definition")]
    original=structures.ingest_trades
    def acknowledge(*args,**kwargs):
        assert stop_requested(), "fixture finalized before Docker shutdown"
        assert counts()==[0,1,1]
        assert list((working/"spool"/spec["definition"]).rglob("*.sealed"))
        (root/"canonical-ack-pending").write_text("pending")
        # A bounded diagnostic delay makes a false early stop observable.
        sleep(.5)
        result=original(*args,**kwargs)
        assert counts()==[1,1,1]
        (root/"canonical-ack-completed").write_text("completed")
        return result
    structures.ingest_trades=acknowledge
    class Stream:
        def __init__(self,**kw):self.session_id=kw["stream_session_id"]
        async def connect(self):return 0
        async def subscribe(self,subscriptions):assert subscriptions
        async def raw_messages(self):
            timestamp=datetime.now(UTC).isoformat()
            yield ProviderRawMessage.build(provider=spec["provider"],venue=spec["venue"],
                stream_session_id=self.session_id,connection_epoch=0,receive_ordinal=1,
                received_at=timestamp,raw_frame=json.dumps({"channel":"market_trades",
                "timestamp":timestamp,"sequence_num":1,"events":[{"type":"update","trades":[{
                "product_id":"BTC-USD","trade_id":"docker-final-drain","price":"100",
                "size":"0.01","side":"BUY","time":timestamp}]}]}))
            (root/"real-frame-received").write_text("received")
            while not stop_requested():await asyncio.sleep(.01)
        async def close(self):pass
    started=monotonic()
    try:
        result=await ContinuousStreamRuntime(repository=structures).run(
            definition_id=spec["definition"],owner_id=owner_id,stop_requested=stop_requested,
            bounded_validation=False,storage_root=working,transport=CoinbaseContinuousTransportAdapter(stream_factory=Stream),
            projection=CoinbaseMarketTradeProjectionAdapter())
        assert result["status"]=="stopped" and counts()==[1,1,1]
        assert not list((working/"spool"/spec["definition"]).rglob("*.sealed"))
        (root/"real-publication-result.json").write_text(json.dumps(dict(
            facts_manifests_mappings=counts(),wal_retired_after_canonical_ack=True,
            stop_waited_for_publication=True,elapsed_seconds=monotonic()-started,
            scripted_transport=True,production_readiness=False)))
        return result
    finally:
        engine.dispose()
