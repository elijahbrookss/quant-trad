"""Disposable current-path history declaration, frozen evidence and correction isolation."""
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import uuid

import pytest

from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, SourceIdentity
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.research import service
from portal.backend.service.storage.repos.market_data import market_data_repo
from portal.backend.service.research.controlled_reversal_evaluator import DETECTOR, PATH_SCHEMA

pytestmark = pytest.mark.db


def test_controlled_reversal_frozen_roundtrip_with_21_actual_warmup_candles(monkeypatch):
    import portal.backend.service.market.runtime_market_data as runtime_market_data
    token = uuid.uuid4().hex
    instrument_id = "reversal-"+token[:20]
    start = datetime(2022,1,1,tzinfo=UTC)
    with db.session() as session:
        session.add(InstrumentRecord(id=instrument_id,datasource="TEST",exchange="ISOLATED",
            symbol="CR-"+token[:8],instrument_type="spot",can_short=False,
            short_requires_borrow=False,has_funding=False,extra_metadata={"fixture": "raw-v4"}))
    source = SourceIdentity(provider="TEST",venue="ISOLATED",source_kind="historical",adapter_version="raw-v4."+token)
    source_id = market_data_repo.register_source(source,lineage={"fixture":token})
    series = market_data_repo.register_series(instrument_id=instrument_id,fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=60,contract_version=CANDLE_FACT_VERSION)
    facts = []
    for i in range(90):
        close = 120-(i%30)*.1
        opened = close
        if i in (20,50):
            opened = close+.2
        elif i == 21:
            opened,close = 117.9,118.3
        elif i == 51:
            opened,close = 118.01,118.01*(118.3/117.9)
        clock = start+timedelta(minutes=i)
        facts.append(CandleFact(open_time=clock,close_time=clock+timedelta(minutes=1),open=opened,
            high=max(opened,close)+1,low=min(opened,close)-1,close=close,volume=1.,
            accepted_at=start+timedelta(days=1),known_at=clock+timedelta(minutes=1),
            known_at_method="interval_close_inferred"))
    market_data_repo.ingest_candles(series_id=series,source_id=source_id,facts=facts,request={"fixture":token})
    class ProviderCallTrap:
        def __init__(self,**kwargs):
            pass
        def __getattr__(self,name):
            raise AssertionError("frozen controlled reversal accessed provider: "+name)
    monkeypatch.setattr(runtime_market_data,"MarketDataCollectorService",ProviderCallTrap)
    payload = {"check_family":"raw_forward_outcome", "scope":{"instrument_id":instrument_id,
        "timeframe":"1m","start":(start+timedelta(minutes=21)).isoformat(),
        "end":(start+timedelta(minutes=90)).isoformat()},"detector":dict(DETECTOR),
        "outcomes":{"schema_version":PATH_SCHEMA,"interval":"1m",
            "trend_lookback_bars":5,"volatility_lookback_bars":20,"entry_lag_bars":1,
            "outcome_horizon_bars":5,"matching_policy":"same_date_hour","comparison_window_days":0},"gap_policy":"continue_degraded",
        "preparation":{"freeze":True,"name":"controlled-reversal-"+token}}
    prepared = service.prepare_research_check_evidence(payload)
    assert prepared["status"] == "frozen",prepared
    evidence = service.run_research_check(prepared["next_request"])
    assert evidence["replayable"] and evidence["evidence"]["input_binding"]["provider_access"] == "disabled"
    assert evidence["result"]["evaluator_version"] == "19"
    result = evidence["result"]["result"]
    assert result["complete_sample_count"] == 1 and result["controlled_reversal_comparison"]["decision"] == "unsupported"
    assert result["pairs"][0]["outcomes"]["treated"]["forward_return"] == pytest.approx(117.3/117.8-1)
    plan = evidence["check"]["payload"]["plan"]
    assert plan["warmup"]["bars"] == 21 and plan["outcome_tail"]["seconds"] == 0
    assert "volume" not in plan["market_data_requirements"][0]["required_fields"]
    assert datetime.fromisoformat(plan["materialization_range"]["start"].replace("Z","+00:00")) == start
    retained = service.get_research_item(evidence["check"]["id"])
    assert retained["observation_eligible"] is True
    assert retained["payload"]["result"]["result_hash"] == evidence["result"]["result_hash"]
    # Corrections to live source cannot alter the accepted frozen input or result.
    market_data_repo.ingest_candles(series_id=series,source_id=source_id,
        facts=[replace(facts[27],close=118.)],request={"fixture":token,"correction":True})
    replay = service.replay_research_check(evidence["check"]["id"])
    assert replay["matches"] is True and replay["status"] == "matched",replay
    assert replay["provider_call_performed"] is False
    for kind in ("result","evidence","plan"):
        assert replay[f"original_{kind}_hash"] == replay[f"replayed_{kind}_hash"]
