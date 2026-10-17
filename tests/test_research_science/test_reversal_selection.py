"""Synthetic selection contracts only;never invoke a historical selection."""
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import math
import pytest

from research_science.recognition import RecognitionCandle, RecognitionStage
from research_science.reversal_selection import select_comparisons, moment_weights, stage_availability
from tests.test_research_science.test_recognition import t


def fixture(count=900):
    prices=[100+math.sin((i%96)*math.tau/96) for i in range(count)]
    bars=tuple(RecognitionCandle(str(i),'source',t(i),t(i+1),t(i+1),
        prices[i-1] if i else prices[i],max(prices[i-1] if i else prices[i],prices[i])+.2,
        min(prices[i-1] if i else prices[i],prices[i])-.2,prices[i]) for i in range(count))
    def event(i=500,key='case',n=6):
        return SimpleNamespace(event_id=key,known_at=t(i+1),formation_start=t(i-n+1),
            stages=(RecognitionStage('completion',t(i+1),t(i+1),str(i)),),source_events=())
    return bars,event


def run(bars,events,**kwargs):
    return select_comparisons(bars,events,evaluation_start=t(100),evaluation_end=t(850),**kwargs)


def test_deterministic_matching_keeps_all_chains_and_equal_moments_without_reuse():
    bars,event=fixture()
    result=run(bars,[event(key='a'),event(key='b')])
    assert result==run(bars,[event(key='b'),event(key='a')])
    assert len(result['cases'])==len(result['pairs'])==2
    assert len({p['control']['decision_at'] for p in result['pairs']})==2
    assert sum(p['completion_moment_weight'] for p in result['pairs'])==1
    assert [p['completion_moment_weight'] for p in result['pairs']]==[.5,.5]
    assert result['selection_summary']['fully_matched_completion_clocks']==1
    assert result['selection_summary']['coverage_pass_gate'] is None
    assert not result['outcomes_calculated']
    for p in result['pairs']:
        case=next(x for x in result['cases'] if x['event_id']==p['event_id'])
        control=p['control']
        assert control['support_end']<case['support_start'] or case['support_end']<control['support_start']


def test_moment_weights_do_not_turn_simultaneous_chains_into_extra_moments():
    records=[{'event_id':'a','decision_at':'x'},{'event_id':'b','decision_at':'x'},{'event_id':'c','decision_at':'y'}]
    assert moment_weights(records)=={'a':.25,'b':.25,'c':.5}


def test_partial_moment_does_not_hide_the_excluded_long_chain():
    bars,event=fixture()
    result=run(bars,[event(key='short'),event(key='long',n=501)])
    assert result['selection_summary']['all']['chains']==2
    assert result['selection_summary']['matched']['chains']==1
    assert result['selection_summary']['partially_matched_completion_clocks']==1
    assert result['selection_summary']['retained_full_population_moment_mass']==.5
    assert len(result['cases'])==2 and result['pairs'][0]['completion_moment_weight']==1


def test_later_completion_does_not_remove_an_earlier_control():
    bars,event=fixture()
    before=run(bars,[event()])
    chosen=before['pairs'][0]['control']['decision_at']
    from research_science.recognition import utc
    j=int((utc(chosen)-t(0))/timedelta(minutes=15))-1
    after=run(bars,[event(),event(i=j+1,key='later')])
    pair=next(p for p in after['pairs'] if p['event_id']=='case')
    assert pair['control']['decision_at']==chosen


def test_future_suffix_prices_cannot_change_the_selection():
    bars,event=fixture()
    before=run(bars,[event()])
    changed=tuple(replace(b,open=b.open*10,high=b.high*10,low=b.low*10,close=b.close*10) if i>=850 else b for i,b in enumerate(bars))
    assert run(changed,[event()])==before


def test_decision_body_and_range_are_diagnostics_not_matching_dimensions():
    bars,event=fixture()
    before=run(bars,[event()])
    altered=list(bars);b=altered[500]
    altered[500]=replace(b,open=b.close,high=b.high+20)
    after=run(tuple(altered),[event()])
    assert after['pairs'][0]['control']['decision_at']==before['pairs'][0]['control']['decision_at']
    assert after['pairs'][0]['distance_squared']==before['pairs'][0]['distance_squared']
    assert after['cases'][0]['features']['range']!=before['cases'][0]['features']['range']


def test_no_sigma_or_separate_history_is_reported_without_coverage_rescue():
    bars,event=fixture(180)
    result=select_comparisons(bars,[event(i=120)],evaluation_start=t(100),evaluation_end=t(170))
    assert result['cases'][0]['match_status']=='unmatched' and not result['pairs']
    flat=tuple(replace(b,open=100,high=101,low=99,close=100) for b in bars)
    invalid=select_comparisons(flat,[event(i=120)],evaluation_start=t(100),evaluation_end=t(170))
    assert invalid['cases'][0]['match_status']=='invalid_features_or_risk'
    assert invalid['selection_summary']['all']['chains']==1


def test_cancellation_and_comparison_budget_are_not_silent_partial_results():
    bars,event=fixture()
    with pytest.raises(ValueError,match='comparison budget'):
        run(bars,[event()],max_comparisons=1)
    def stop():raise RuntimeError('cancelled')
    with pytest.raises(RuntimeError,match='cancelled'):run(bars,[event()],checkpoint=stop)


def test_new_stage_view_separates_candle_role_and_parent_clocks():
    _,event=fixture();e=event()
    e.stages=(RecognitionStage('preceding_trough',t(1),t(1),'1'),
        RecognitionStage('rebound_start',t(2),t(2),'2'),RecognitionStage('rebound_end',t(3),t(3),'3'),
        RecognitionStage('reference_confirmed',t(4),t(4),'4'),RecognitionStage('lowest_push',t(5),t(5),'5'),
        RecognitionStage('completion',e.known_at,e.known_at,'500'))
    original=e.stages
    stages={x['name']:x for x in stage_availability(e)['stages']}
    from research_science.recognition import plain
    assert stages['rebound_end']['candle_known_at']==plain(t(3))
    assert stages['rebound_end']['role_known_at']==plain(t(4))
    assert stages['lowest_push']['role_known_at']==plain(e.known_at)
    assert all(x['usable_as_parent_stage_at']==plain(e.known_at) for x in stages.values())
    assert e.stages==original


def test_real_shared_recognizer_events_retain_identity_in_selection_records():
    from research_science.recognition import RecognitionContext
    from research_science.reversal_recognition import recognize_reversals,ReversalSettings
    bars,_=fixture()
    bars=list(bars)
    prices=[100.,99.,99.2,98.8,98.7,99.6,99.9]
    for j,price in enumerate(prices):
        i=500+j;o=prices[j-1] if j else 100.1
        high=max(o,price)+.1
        if j==2:high=99.5
        if j==3:high=99.7
        bars[i]=RecognitionCandle(str(i),'source',t(i),t(i+1),t(i+1),o,high,min(o,price)-.1,price)
    bars=tuple(bars)
    snapshot=recognize_reversals(bars,context=RecognitionContext('instrument',900,t(0),'source'),
        settings=ReversalSettings('rising_leg_plus_retreat','retain_all_provisional',12000,10000))
    assert snapshot.candidates
    result=run(bars,snapshot.candidates)
    expected={e.event_id for e in snapshot.candidates if t(100)<=e.known_at<t(850)}
    assert {r['event_id'] for r in result['cases']}==expected
    assert not result['outcomes_calculated']
    assert all(r['timing']['schema_version']=='reversal_stage_availability.v2' for r in result['cases'])
