"""Owned connected-cutover fixture using ordinary QT recovery and readers.

No production entrypoint. Called only by the explicit disposable host rehearsal;
transport is scripted and definitions are not enrolled for external collection.
"""
import asyncio
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
from threading import Event

from sqlalchemy import text


def _digest(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def configure_source(storage, monkeypatch):
    from tests.test_market_data import test_archive_online_copy_db as fixture
    from tests.test_market_data.test_fact_storage_tiers_db import BASE
    from portal.backend.service.storage.repos import market_structure
    repo = market_structure.market_structure_repository
    claims, frozen = [], []
    actual_claim, actual_register = repo.claim_stream, repo.upsert_stream_definition
    actual_cold = fixture._cold_book_handoff

    def register(**args):
        channels = tuple(args["channels"])
        assert channels in (("level2",), ("market_trades",))
        args["channels"] = (*channels, "heartbeats")
        args["enabled"] = False  # No external provider enrollment in this fixture.
        config = dict(args.get("config") or {})
        if "product_definition_version_id" not in config:
            product = args["definition_id"] + ".runtime.product.v1"
            repo.register_product_definition(definition_version_id=product,
                source_id=args["source_id"], instrument_id="storage-fixture",
                provider_product_id=args["provider_product_id"], product_type="spot", venue=args["venue"],
                status="fixture", base_currency="BTC", quote_currency="USD", provider_size_unit="base",
                contract_size=None, price_increment=None, base_increment=None,
                effective_at=BASE, received_at=BASE, provenance={"fixture": "connected-recovery"})
            config["product_definition_version_id"] = product
        args["config"] = config
        return actual_register(**args)

    def claim(**args):
        value = actual_claim(**args)
        repo.append_session_event(value, event_ordinal=0, connection_epoch=0,
            event_type="connected", occurred_at=BASE,
            evidence={"fixture": "connected-recovery", "bounded_validation": True})
        claims.append(value)
        return value

    def cold(*args, **kwargs):
        value = actual_cold(*args, **kwargs)
        # Still the real v2 source here, before the fixture reconstructs v1.
        with storage.database._engine.connect() as conn:
            bindings = conn.execute(text("SELECT dataset_id,series_id FROM market.dataset_series ORDER BY dataset_id,series_id")).all()
        for dataset, series in bindings:
            rows = storage.repo.read_dataset_fact_revisions(dataset_id=dataset, series_id=series)
            frozen.append(dict(dataset=dataset, series=series, rows=len(rows), sha256=_digest(rows)))
        assert sorted(x["rows"] for x in frozen) == [2, 6]
        return value

    monkeypatch.setattr(repo, "upsert_stream_definition", register)
    monkeypatch.setattr(repo, "claim_stream", claim)
    monkeypatch.setattr(fixture, "_cold_book_handoff", cold)
    return dict(claims=claims, frozen=frozen)


def finish_source(storage, state, control, policy):
    from portal.backend.service.storage.repos import market_structure
    claims = state["claims"]
    assert len(claims) == 4 and len({c.definition_id for c in claims}) == 4
    for claim in claims:
        market_structure.market_structure_repository.release(claim)
    assert policy.movement_enabled and policy.backup_enabled
    spec = dict(frozen=state["frozen"], definitions=[c.definition_id for c in claims],
        policy_hash=policy.fingerprint, scripted_transport=True)
    (control/"runtime-recovery.json").write_text(json.dumps(spec))


def verify(spec):
    """Inside the ordinary candidate collector container; no extra privileges."""
    from data_providers.streams.contracts import ProviderRawMessage
    from portal.backend.db import db
    from portal.backend.service.market.continuous_stream_runtime import ContinuousStreamRuntime
    from portal.backend.service.market.continuous_stream_collector import (
        CoinbaseContinuousTransportAdapter, CoinbaseLevel2BookProjectionAdapter, CoinbaseMarketTradeProjectionAdapter)
    from portal.backend.service.storage.repos.market_data import market_data_repo as market_data_repository
    from portal.backend.service.storage.repos.market_structure import market_structure_repository
    assert os.getuid() == 1000 and 70 in os.getgroups()
    archive = Path(os.environ["MARKET_STRUCTURE_STORAGE_ROOT"])
    working = Path(os.environ["MARKET_STRUCTURE_WORKING_ROOT"])
    assert archive.stat().st_dev != working.stat().st_dev
    assert db.ensure_schema(), str(db.last_error)
    structures = market_structure_repository
    runtime = ContinuousStreamRuntime(repository=structures)
    stop = Event(); stop.set()
    received = []

    class Stream:
        def __init__(self, **kwargs): self.session_id = kwargs["stream_session_id"]
        async def connect(self): return 0
        async def subscribe(self, subscriptions): assert subscriptions
        async def close(self): pass
        async def raw_messages(self):
            timestamp = datetime.now(UTC).isoformat()
            frame = json.dumps({"channel": "market_trades", "timestamp": timestamp, "sequence_num": 1,
                "events": [{"type": "update", "trades": [{"product_id": "BTC-USD", "trade_id": "fresh-after-connected-switch",
                    "price": "101", "size": "0.01", "side": "BUY", "time": timestamp}]}]})
            received.append(frame)
            yield ProviderRawMessage.build(provider="COINBASE", venue="COINBASE_DIRECT",
                stream_session_id=self.session_id, connection_epoch=0, receive_ordinal=1,
                received_at=timestamp, raw_frame=frame)
            stop.set()

    def read_frozen():
        result = []
        for expected in spec["frozen"]:
            rows = market_data_repository.read_dataset_fact_revisions(dataset_id=expected["dataset"], series_id=expected["series"])
            assert len(rows) == expected["rows"] and _digest(rows) == expected["sha256"]
            result.append(len(rows))
        return result

    def collect(definition):
        row = structures.list_stream_definitions(definition_id=definition)[0]
        projection = CoinbaseLevel2BookProjectionAdapter() if row["series_fact_type"] == "market.l2_book" else CoinbaseMarketTradeProjectionAdapter()
        return asyncio.run(runtime.run(definition_id=definition, owner_id="connected-runtime-fixture", stop_requested=stop.is_set,
            bounded_validation=True, storage_root=archive, projection=projection,
            transport=CoinbaseContinuousTransportAdapter(stream_factory=Stream)))

    def counts():
        with db._engine.connect() as conn:
            return [conn.scalar(text(sql)) for sql in (
                "SELECT count(*) FROM market.fact_versions",
                "SELECT count(*) FROM market.raw_archive_manifests",
                "SELECT count(*) FROM market.raw_archive_record_mappings")]

    before = read_frozen()
    pending = list((working/"spool").rglob("*.sealed")) + list((working/"spool").rglob("*.open"))
    assert len(pending) == 12
    for definition in spec["definitions"]:
        assert collect(definition)["status"] == "stopped"
    assert not received
    assert not list((working/"spool").rglob("*.sealed")) and not list((working/"spool").rglob("*.open"))
    recovered = counts()
    for definition in spec["definitions"]:
        assert collect(definition)["status"] == "stopped"
    assert counts() == recovered and not received
    stop.clear()
    assert collect("exact-lineage")["status"] == "stopped" and len(received) == 1
    after = counts()
    assert after == [n+1 for n in recovered]
    trade = structures.list_stream_definitions(definition_id="exact-lineage")[0]
    now = datetime.now(UTC)
    current = market_data_repository.read_facts(series_id=trade["series_id"], start=now-timedelta(minutes=5), end=now+timedelta(minutes=1))
    assert len(current) == 1
    assert read_frozen() == before
    return dict(normal_wal_segments_recovered=len(pending), reentry_without_duplicates=True,
        fresh_scripted_records=1, current_read_count=len(current), frozen_rows=before,
        recovered_counts=recovered, after_fresh_counts=after, policy_hash=spec["policy_hash"],
        ordinary_uid1000=True, external_provider_enrollment=False)
