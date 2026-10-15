from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace as NS
import math
import pytest

from market_data.frozen import semantic_hash
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan
from portal.backend.service.research import catalog
from portal.backend.service.research.registry import normalize_check_request
from portal.backend.service.research.liquidity_episode_evaluator import LiquidityEpisodeEvaluator, build_rows, match_distance

START=datetime(2026,9,22,tzinfo=UTC)
def t(i):return START+timedelta(seconds=i)

def record(sid,i,p,key=None):
    return NS(series_id=sid,revision=1,market_commit_seq=i+1,source_identity_key='source-1',
        fact_version_id=f'fact-{sid}-{i}',row_hash='a'*64,
        fact=NS(observation_time=t(i),observation_key=key or str(i),known_at=t(i+45),
            quality={'_qt_trade_flow_quality':{'archive_complete':True,'aggregate_complete':True,
                'canonicalization_complete':True,'late_trade_count':0}},
            source_published_at=t(i+.5),received_at=t(i-.5),accepted_at=t(i+45),
            state='active',payload=p))

def fixture():
    p=deepcopy(catalog._EXAMPLES['event_fact_analysis@14']);p['scope']['end']=t(1200).isoformat()
    definition,request=normalize_check_request(p)
    plan=ResolvedCheckPlan(schema_version=CHECK_PLAN_SCHEMA_VERSION,request_hash=request.request_hash,
        market_data_requirements=(),indicator_graph=(),evaluation_range={'start':t(0).isoformat(),'end_exclusive':t(1200).isoformat()},
        materialization_range={'start':t(0).isoformat(),'end_exclusive':t(1200).isoformat()},warmup={'bars':0},
        outcome_tail={'bars':0},gap_policy='continue_degraded')
    records={a:[] for a in ('bbo','depth','flow','trades')}
    for i in range(1200):
        mid=100*math.exp(.002 if 600<=i<660 else 0)
        base={'bucket_end':t(i+1).isoformat()}
        book={'validity_interval_id':'valid-1','source_state_hash':str(i)}
        records['bbo'].append(record(20,i,{**base,**book,'input_fingerprint':str(i),
            'mid_price':mid,'spread_bps':1.}))
        records['depth'].append(record(21,i,{**base,**book,'bbo_input_fingerprint':str(i),
            'band_bps':5,'bid_base_quantity':4. if 500<=i<520 else 10.,'ask_base_quantity':10.}))
        records['flow'].append(record(2,i,{**base,'trade_count':1 if i==600 else 0,
            'aggressor_buy_base_volume':1. if i==600 else 0.,'aggressor_sell_base_volume':0.}))
    trade=record(1,600,{'aggressor_side':'BUY','maker_side':'SELL','price':100.2,'base_quantity':1.},key='trade1')
    trade.fact.observation_time=t(600.2);records['trades'].append(trade)
    return definition,request,plan,records

def test_retrospective_complete_sequences_no_move_thinning_and_actual_clocks():
    d,r,plan,records=fixture()
    assert d.evaluator_version=='14'
    evaluator=LiquidityEpisodeEvaluator()
    out=evaluator.evaluate(plan=plan,inputs={**r.parameters,'fact_records_by_alias':records})
    assert out['support']['joined_seconds']==1200
    assert len(out['episodes'])==2
    thin=next(e for e in out['episodes'] if e['origin']=='depth_thinning')
    assert thin['event']['sharp_move_in_next_60s'] is False
    assert thin['control'] is not None
    sharp=next(e for e in out['episodes'] if e['origin']=='sharp_move')
    trace=sharp['event']['trades'][0]
    assert trace['received_at']<trace['event_time']<trace['known_at']
    assert len(sharp['event']['seconds'])==241
    assert out['historical_decision'] is False
    assert semantic_hash(out)==semantic_hash(evaluator.evaluate(plan=plan,inputs={**r.parameters,'fact_records_by_alias':records}))

def test_gap_and_flow_coverage_are_not_filled_or_treated_as_quiet():
    _,r,plan,records=fixture();records['flow'][550].fact.quality={}
    out=LiquidityEpisodeEvaluator().evaluate(plan=plan,inputs={**r.parameters,'fact_records_by_alias':records})
    assert out['support']['join_exclusions']['trade_flow_coverage_unproved']==1
    assert not out['episodes']

def test_trade_flow_reconciliation_and_book_join_block_partial_seconds():
    _,r,_,records=fixture();records['trades']=[]
    records['depth'][100].fact.payload['source_state_hash']='wrong'
    joined,_,reasons=build_rows({'fact_records_by_alias':records},t(2000))
    assert 100 not in [int((x-START).total_seconds()) for x in joined]
    assert reasons['book_state_join_mismatch']==1
    assert reasons['raw_trade_flow_reconciliation_failed']==1


def test_recorded_gap_blocks_even_with_dense_rows():
    _,_,_,records=fixture()
    joined,_,reasons=build_rows({'fact_records_by_alias':records,'data_quality':{
        'recorded_gaps':[{'alias':'bbo','start':t(550).isoformat(),'end':t(551).isoformat()}]}},t(2000))
    assert t(550) not in joined and reasons['recorded_source_gap']==1

def test_matching_only_uses_declared_precontext_and_fixed_calipers():
    context={'prior_return':.001,'spread':1.,'depth':10.,'volatility':.001}
    assert match_distance(context,context)==0
    assert match_distance(context,{**context,'depth':21.}) is None
    assert match_distance(context,{**context,'prior_return':-.001}) is None

def test_late_revisions_resource_bounds_and_threshold_tuning_rejected():
    _,r,_,records=fixture()
    future=deepcopy(records['bbo'][100]);future.revision=2;future.fact.known_at=t(3000)
    future.fact.payload['mid_price']=999.;records['bbo'].append(future)
    joined,_,_=build_rows({'fact_records_by_alias':records},t(2000))
    assert joined[t(100)]['mid']==100.
    with pytest.raises(ValueError,match='50000'):
        build_rows({'fact_records_by_alias':{**records,'bbo':[records['bbo'][0]]*50001}},t(2000))
    p=deepcopy(catalog._EXAMPLES['event_fact_analysis@14']);p['outcomes']['sharp_bps']=5
    with pytest.raises(ValueError,match='fixed procedure'):normalize_check_request(p)


def test_owned_history_loader_uses_analysis_cutoff_and_no_extra_price_tail(monkeypatch):
    from portal.backend.service.research import execution,planning
    d,r,_,_=fixture()
    plan=planning.plan_research_check(d,r,inspect_coverage=False,
        instrument_loader=lambda _: {'id':'instrument-id','symbol':'BIP'},
        indicator_planner=lambda *args,**kwargs: {'indicators':[],'requirements':[],'warmup_bars':0})
    assert plan.outcome_tail['seconds']==0
    assert len(plan.market_data_requirements)==5
    assert plan.execution['fact_history_required']
    calls=[]
    class Resolver:
        dataset_binding=None
        def causal_history(self,**kwargs):calls.append(kwargs);return ()
    monkeypatch.setattr(execution.candle_service,'fetch_ohlcv_by_instrument',lambda *a,**kw:None)
    out=execution._load_market_inputs(d,r,plan,resolver=Resolver())
    assert len(calls)==4
    assert all(c['evaluation_time'].isoformat()==datetime.fromisoformat(r.parameters['outcomes']['analysis_as_of'].replace('Z','+00:00')).isoformat() for c in calls)
    assert set(out['fact_records_by_alias'])=={'bbo','depth','flow','trades'}


def test_cancellation_checkpoint_is_not_swallowed(monkeypatch):
    from portal.backend.service.research import liquidity_episode_evaluator as method
    _,_,_,records=fixture()
    class Cancelled(Exception):pass
    def checkpoint():raise Cancelled()
    monkeypatch.setattr(method,'execution_checkpoint',checkpoint)
    with pytest.raises(Cancelled):build_rows({'fact_records_by_alias':records},t(2000))


@pytest.mark.parametrize("feature",["spread","depth","volatility"])
def test_symmetric_factor_two_boundary_and_zero_behavior(feature):
    a={"prior_return":0.,"spread":1.,"depth":10.,"volatility":.001}
    assert match_distance(a,{**a,feature:a[feature]*2}) is not None
    assert match_distance({**a,feature:a[feature]*2},a) is not None
    assert match_distance(a,{**a,feature:a[feature]*2.0001}) is None
    assert match_distance({**a,feature:0.},{**a,feature:0.})==0.
    assert match_distance(a,{**a,feature:0.}) is None
    assert match_distance({**a,feature:0.},a) is None


def test_declared_scope_keeps_pre_post_and_matching_context_inside():
    _,r,plan,records=fixture()
    out=LiquidityEpisodeEvaluator().evaluate(plan=plan,inputs={**r.parameters,'fact_records_by_alias':records})
    assert out['support']['eligible_anchors']==1200-240
    for e in out['episodes']:
        for episode in [e['event'],e['control']]:
            if episode is None:continue
            anchor=datetime.fromisoformat(episode['anchor'].replace('Z','+00:00'))
            assert t(120)<=anchor<=t(1079)
            assert episode['seconds'][0]['offset_seconds']==-120
            assert episode['seconds'][-1]['offset_seconds']==120
        if e['control'] is not None:
            assert datetime.fromisoformat(e['event']['anchor'].replace('Z','+00:00'))>=t(420)
