from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import timedelta

import pytest

from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, SourceIdentity
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.research import service
from portal.backend.service.storage.repos.market_data import market_data_repo
from tests.test_portal.test_event_fact_snapshot_check import _BASE, _bbo_record, _iso

pytestmark = pytest.mark.db


def test_availability_check_freezes_runs_and_replays_without_provider_calls(monkeypatch):
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
    source = SourceIdentity(provider="TEST", venue="ISOLATED", source_kind="fixture",
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
    books = []
    # Continuous one-second coverage includes the declared sixty-second lookback.
    for offset in range(-60, 180):
        end = _BASE + timedelta(seconds=offset + 1)
        record = _bbo_record(bucket_end=end, known_at=end + timedelta(seconds=7),
                             commit_seq=offset + 61)
        # Synthetic canonical observations carry no claim to collector raw archives.
        books.append(replace(record.fact, source=source,
                             transformation_id="fixture.bbo.v1",
                             provenance={"fixture": token}))
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
