"""Disposable canonical raw-v3 freeze, correction isolation and durable replay."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import uuid

import pytest

from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, SourceIdentity
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.research import service
from portal.backend.service.storage.repos.market_data import market_data_repo
from portal.backend.service.research.raw_candle_evaluator import OUTCOME_SCHEMA

pytestmark=pytest.mark.db


def test_raw_candle_freeze_persistence_replay_and_real_gap_exclusion(monkeypatch):
    import portal.backend.service.market.runtime_market_data as runtime_market_data
    token=uuid.uuid4().hex;instrument_id=f"raw-v3-{token[:20]}"
    start=datetime(2022,1,1,tzinfo=UTC)
    with db.session() as session:
        session.add(InstrumentRecord(id=instrument_id,datasource="TEST",exchange="ISOLATED",
            symbol=f"RAW-{token[:8]}",instrument_type="spot",can_short=False,
            short_requires_borrow=False,has_funding=False,extra_metadata={"fixture":"raw-v3"}))
    source=SourceIdentity(provider="TEST",venue="ISOLATED",source_kind="historical",
                          adapter_version=f"raw-v3.{token}")
    source_id=market_data_repo.register_source(source,lineage={"fixture":token})
    series=market_data_repo.register_series(instrument_id=instrument_id,fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=60,contract_version=CANDLE_FACT_VERSION)
    facts=[]
    for index in range(18):
        if index==12:
            continue
        opened=start+timedelta(minutes=index);closed=opened+timedelta(minutes=1)
        price=100+index;open_price=price
        if index in (0,7):
            open_price=price+1
        elif index in (1,8):
            open_price=price-1;price+=1
        volume=20 if index==1 else 0 if index in (7,8) else 10
        facts.append(CandleFact(open_time=opened,close_time=closed,open=open_price,
            high=max(open_price,price)+1,low=min(open_price,price)-1,close=price,volume=volume,
            accepted_at=start+timedelta(days=1),known_at=closed,known_at_method="interval_close_inferred"))
    market_data_repo.ingest_candles(series_id=series,source_id=source_id,facts=facts,request={"fixture":token})
    market_data_repo.record_gap_evidence(series_id=series,source_id=source_id,
        start=start+timedelta(minutes=12),end=start+timedelta(minutes=13),
        classification="provider_missing_data",expected_count=1,observed_count=0,
        evidence={"schema_version":"market_gap_evidence.v1","reason_code":"synthetic-source-gap"})
    class ProviderCallTrap:
        def __init__(self,**kwargs):
            pass
        def __getattr__(self,name):
            raise AssertionError(f"frozen raw Check attempted provider access: {name}")
    monkeypatch.setattr(runtime_market_data,"MarketDataCollectorService",ProviderCallTrap)
    payload={"check_family":"raw_forward_outcome","scope":{"instrument_id":instrument_id,
        "timeframe":"1m","start":(start+timedelta(minutes=1)).isoformat(),
        "end":(start+timedelta(minutes=18)).isoformat()},"detector":{"all":[
            {"field":"previous_close","operator":"lt","value_field":"previous_open"},
            {"field":"close","operator":"gt","value_field":"open"},
            {"field":"open","operator":"lte","value_field":"previous_close"},
            {"field":"close","operator":"gte","value_field":"previous_open"},
            {"field":"volume","operator":"gt","value_field":"previous_volume"}]},
        "outcomes":{"schema_version":OUTCOME_SCHEMA,"forward_bars":[5],"entry_lag_bars":1,
                    "min_sample_count":1},"gap_policy":"continue_degraded",
        "preparation":{"freeze":True,"name":f"raw-v3-{token}"}}
    prepared=service.prepare_research_check_evidence(payload)
    assert prepared["status"]=="frozen",prepared
    high=service.run_research_check(prepared["next_request"])
    assert high["replayable"] and high["evidence"]["input_binding"]["provider_access"]=="disabled"
    assert high["result"]["evaluator_version"]=="3"
    result=high["result"]["result"]
    assert result["complete_sample_count"]==1
    assert result["outcomes"]["summary"]["5"]["mean_forward_return_pct"]==pytest.approx(107/102-1)
    requirement=high["plan"]["market_data_requirements"][0]
    assert "volume" in requirement["required_fields"]
    assert high["plan"]["outcome_tail"]["seconds"]==0
    low_request=deepcopy(prepared["next_request"])
    low_request["detector"]["all"][-1]["operator"]="lte"
    low=service.run_research_check(low_request)
    assert low["evidence"]["input_binding"]==high["evidence"]["input_binding"]
    low_result=low["result"]["result"]
    assert low_result["sample_count"]==1 and low_result["complete_sample_count"]==0
    assert low_result["events"][0]["outcomes"]["5"]["reason"]=="gap_in_horizon"
    retained=service.get_research_item(high["check"]["id"])
    assert retained["observation_eligible"] is True
    assert retained["payload"]["result"]["result_hash"]==high["result"]["result_hash"]
    entry=next(f for f in facts if f.open_time==start+timedelta(minutes=2))
    market_data_repo.ingest_candles(series_id=series,source_id=source_id,
        facts=[replace(entry,close=entry.close+.5)],request={"fixture":token,"correction":True})
    for run in (high,low):
        replay=service.replay_research_check(run["check"]["id"])
        assert replay["status"]=="matched" and replay["matches"] is True,replay
        assert replay["provider_call_performed"] is False
        for kind in ("result","evidence","plan"):
            assert replay[f"original_{kind}_hash"]==replay[f"replayed_{kind}_hash"]
