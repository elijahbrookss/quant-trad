from __future__ import annotations

import json
import uuid
from dataclasses import replace
from datetime import timedelta

import pytest

from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore, publish_spool_archive
from market_data.canonical_adapters import canonicalize_bbo_feature, decode_bbo_feature_record
from market_data.order_book import BookSourcePosition
from market_data.structure import RawStreamRecord
from portal.backend.service.storage.repos.market_structure import market_structure_repository
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, SourceIdentity
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.research import service
from portal.backend.service.storage.repos.market_data import market_data_repo
from tests.test_portal.test_event_fact_snapshot_check import _BASE, _bbo_record, _iso

pytestmark = pytest.mark.db


def test_availability_check_freezes_runs_and_replays_without_provider_calls(monkeypatch, tmp_path):
    import portal.backend.service.market.runtime_market_data as runtime_market_data

    token = uuid.uuid4().hex
    instrument_id = f"check-clock-{token[:20]}"
    with db.session() as session:
        session.add(InstrumentRecord(
            id=instrument_id, datasource="TEST", exchange="ISOLATED",
            symbol=f"CLOCK-{token[:8]}", instrument_type="spot",
            can_short=False, short_requires_borrow=False, has_funding=False,
            extra_metadata={"fixture": "check-availability-workflow"},
        ))
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream",
                            adapter_version=f"clock-test.{token}")
    source_id = market_data_repo.register_source(source, lineage={"fixture": token})
    candle_series = market_data_repo.register_series(
        instrument_id=instrument_id, fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=60, contract_version=CANDLE_FACT_VERSION,
    )
    candles = []
    for index in range(5):
        opened = _BASE + timedelta(minutes=index)
        closed = opened + timedelta(minutes=1)
        candles.append(CandleFact(
            open_time=opened, close_time=closed, open=100 + index,
            high=102 + index, low=99 + index, close=101 + index, volume=10,
            trade_count=None, source_published_at=None, received_at=None,
            accepted_at=_BASE + timedelta(days=240), known_at=closed,
            known_at_method="interval_close_inferred",
        ))
    market_data_repo.ingest_candles(
        series_id=candle_series, source_id=source_id, facts=candles,
        request={"fixture": token},
    )
    book_series = market_data_repo.register_series(
        instrument_id=instrument_id, fact_type="market.bbo",
        timeframe_seconds=1, contract_version="market.bbo.v1",
    )
    l2_series = market_data_repo.register_series(
        instrument_id=instrument_id, fact_type="market.l2_book",
        timeframe_seconds=None, contract_version="market.l2_book.v1",
    )
    definition_id = f"clock-book-{token}"
    market_structure_repository.upsert_stream_definition(
        definition_id=definition_id, source_id=source_id, series_id=l2_series,
        provider=source.provider, venue=source.venue, provider_product_id="BTC-USD",
        channels=("level2",), auth_mode="public", contract_version="market.l2_book.v1",
        max_spool_bytes=1024**3, max_segment_bytes=128 * 1024**2,
        config={"fixture": token},
    )
    claim = market_structure_repository.claim_stream(
        definition_id=definition_id, owner_id=token, lease_seconds=600, bounded=True,
    )
    # One unchanged book snapshot supports the following fixed-cadence samples.
    snapshot_time = _BASE - timedelta(seconds=61)
    spool = DurableRawSpoolSegment(
        root=tmp_path / "spool", definition_id=definition_id,
        session_id=claim.session_id, connection_epoch=0, segment_ordinal=0,
    )
    message = ProviderRawMessage.build(
        provider=source.provider, venue=source.venue, stream_session_id=claim.session_id,
        connection_epoch=0, receive_ordinal=1, received_at=_iso(snapshot_time),
        raw_frame=json.dumps({
            "channel": "l2_data", "timestamp": _iso(snapshot_time), "sequence_num": 1,
            "events": [{"type": "snapshot", "product_id": "BTC-USD", "updates": [
                {"side": "bid", "price_level": "99", "new_quantity": "2", "event_time": _iso(snapshot_time)},
                {"side": "offer", "price_level": "101", "new_quantity": "3", "event_time": _iso(snapshot_time)},
            ]}],
        }),
    )
    raw = RawStreamRecord.from_provider_message(
        message, definition_id=definition_id, spool_segment_id=spool.spool_segment_id,
        provider_product_id="BTC-USD", requested_channel="level2", observed_channel="level2",
    )
    spool.append(raw)
    spool.seal()
    encoded, acknowledgement, archived_records = publish_spool_archive(
        spool, object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
        temporary_directory=tmp_path / "staging",
    )
    manifest = market_structure_repository.commit_archive(
        claim, encoded=encoded, acknowledgement=acknowledgement, records=archived_records,
    )
    position = BookSourcePosition(
        definition_id=definition_id, session_id=claim.session_id, connection_epoch=0,
        provider_product_id="BTC-USD", provider_sequence_num=1, receive_ordinal=1,
        event_ordinal=0,
    )
    books = []
    # Continuous one-second coverage includes the declared sixty-second lookback.
    for offset in range(-60, 180):
        end = _BASE + timedelta(seconds=offset + 1)
        record = _bbo_record(bucket_end=end, known_at=end + timedelta(seconds=7), commit_seq=1)
        feature = replace(
            decode_bbo_feature_record(record).fact,
            series_id=book_series, source_l2_series_id=l2_series,
            source_position=position, source_effective_at=snapshot_time,
        )
        books.append(canonicalize_bbo_feature(feature, source=source))
    market_data_repo.ingest_facts(
        series_id=book_series, source_id=source_id, facts=books,
        request={"fixture": token},
    )

    class ProviderCallTrap:
        def __init__(self, **kwargs):
            pass

        def __getattr__(self, name):
            raise AssertionError(f"frozen Check attempted provider access: {name}")

    monkeypatch.setattr(runtime_market_data, "MarketDataCollectorService", ProviderCallTrap)
    payload = {
        "check_family": "event_fact_analysis",
        "scope": {"instrument_id": instrument_id, "timeframe": "1m",
                  "start": _iso(_BASE), "end": _iso(_BASE + timedelta(minutes=3))},
        "detector": {"type": "fact_snapshot", "input_alias": "bbo",
                     "evaluation_trigger": "required_facts_available"},
        "outcomes": {"horizons": [1], "primary_horizon": 1},
        "statistics": {"features": {"baseline": [], "enriched": []},
                       "eligibility": {"min_samples": 0}},
        "inputs": [{"alias": "bbo", "fact_type": "market.bbo",
                    "contract_version": "market.bbo.v1", "timeframe_seconds": 1,
                    "max_staleness_seconds": 60, "source_policy": {"mode": "exact",
                                      "source_identity_key": source.identity_key}}],
        "gap_policy": "continue_degraded",
        "preparation": {"freeze": True, "name": f"clock-{token}"},
    }
    prepared = service.prepare_research_check_evidence(payload)
    assert prepared["status"] == "frozen", prepared
    run = service.run_research_check(prepared["next_request"])
    assert run["replayable"] is True
    assert run["evidence"]["input_binding"]["provider_access"] == "disabled"
    evaluated = run["result"]["result"]
    assert evaluated["schema_version"] == "event_fact_analysis_result.v4"
    first = evaluated["events"][0]
    assert first["eligible"] is True
    assert first["timing"]["sample_end"] == "2026-01-01T00:01:00.000000Z"
    assert first["timing"]["primary_known_at"] == "2026-01-01T00:01:00.000000Z"
    assert first["timing"]["decision_time"] == "2026-01-01T00:01:07.000000Z"
    assert first["timing"]["outcome_price_time"] == "2026-01-01T00:02:00.000000Z"
    assert first["entry_price"] == 102.0

    # New mutable data must not alter the frozen evidence or replay results.
    original = books[120 - 1]  # sample ending 00:01:00
    market_data_repo.ingest_facts(
        series_id=book_series, source_id=source_id,
        facts=[replace(original, known_at=original.known_at + timedelta(seconds=20),
                       received_at=original.known_at + timedelta(seconds=20),
                       accepted_at=original.known_at + timedelta(seconds=20))],
        request={"fixture": token, "correction": True},
    )
    replay = service.replay_research_check(run["check"]["id"])
    assert replay["status"] == "matched", replay
    assert replay["matches"] is True
    assert replay["provider_call_performed"] is False
    assert replay["original_result_hash"] == replay["replayed_result_hash"]
    assert replay["original_evidence_hash"] == replay["replayed_evidence_hash"]
    assert replay["original_plan_hash"] == replay["replayed_plan_hash"]
