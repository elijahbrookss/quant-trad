"""Approved all-population description;no conditional matching or market data."""
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import pytest
from research_science.recognition import RecognitionStage,utc
from research_science.reversal_completed_recovery import measure_completed,recovery_candles,summarize_population
from research_science.reversal_outcomes import first_passage
from tests.test_research_science.test_reversal_population import population,world
from tests.test_research_science.test_recognition import candles,CONTEXT,settings
from research_science.reversal_recognition import recognize_reversals

def test_recovery_uses_typed_observed_low_not_role_known_clock():
    event=recognize_reversals(candles(),context=CONTEXT,settings=settings('rising_leg_plus_retreat')).candidates[0]
    low=next(s for s in event.stages if s.name=='lowest_push')
    assert recovery_candles(event)==1+int((event.known_at-low.observed_at).total_seconds()/900)
    changed=replace(event,event_id="",stages=tuple(replace(s,known_at=event.known_at) if s.name=='lowest_push' else s for s in event.stages))
    assert recovery_candles(changed)==recovery_candles(event)
    one=replace(event,event_id="",stages=tuple(replace(s,observed_at=event.known_at,known_at=event.known_at) if s.name=='lowest_push' else s for s in event.stages))
    assert recovery_candles(one)==1
    with pytest.raises(ValueError,match='lowest_push'):recovery_candles(replace(event,event_id="",stages=()))


def test_all_completed_consumer_retains_every_chain_and_reuses_exact_first_passage(monkeypatch):
    pop=population();bars,snapshot=world(pop)
    events=[]
    for event in snapshot.candidates:
        events.append(replace(event,event_id="",stages=event.stages+(RecognitionStage('lowest_push',event.known_at-timedelta(minutes=30),event.known_at,event.stages[0].candle_id),)))
    snapshot=SimpleNamespace(candidates=tuple(events))
    def forbidden(*a,**k):raise AssertionError('ordinary controls invoked')
    monkeypatch.setattr('research_science.reversal_selection._select_comparisons',forbidden)
    result=measure_completed(bars,snapshot,pop)
    assert len(result['cases'])==len(events)==2
    assert sum(result['populations']['combined']['all_case_weights'].values())==1
    by_open={b.open_time:b for b in bars}
    for record,event in zip(result['cases'],events):
        assert record['outcome']==first_passage(event,bars,by_open=by_open,evaluation_end=pop.end)
        assert record['recovery_candles']==3
    def cancelled():raise RuntimeError('cancelled')
    with pytest.raises(RuntimeError,match='cancelled'):measure_completed(bars,snapshot,pop,checkpoint=cancelled)


def test_label_distributions_keep_all_population_clock_weights_and_missing_N():
    start=utc('2025-05-24T00:00:00Z');end=start+timedelta(days=56)
    def row(key,hour,label,N,missing=False):
        return {'event_id':key,'decision_at':(start+timedelta(hours=hour)).isoformat(),
            'outcome':{'classification':label,'reason':'synthetic'},'recovery_candles':N,
            'context_missing_reasons':['sigma_missing'] if missing else []}
    rows=[row('a',0,'continuation',2,True),row('b',0,'failure',4),
          row('c',1,'continuation',8),row('d',2,'continuation',None),
          row('e',3,'neither',5),row('f',4,'unresolved',9)]
    result=summarize_population(rows,start=start,end=end)
    target=result['outcomes']['continuation']
    assert target['full_population_mass']==pytest.approx(.5)
    assert target['N']['observed_weight']==pytest.approx(.3)
    assert target['N']['missing_weight']==pytest.approx(.2)
    assert target['N']['missing_count']==1
    assert target['N']['mean']==pytest.approx(6)
    assert target['N']['quantiles']['0.5']==8
    assert result['context_missing_mass']==pytest.approx(.1)
    assert sum(v['full_population_mass'] for v in result['outcomes'].values())==pytest.approx(1)
    assert result['context_adjusted_comparison'] is False


def test_empty_population_retains_four_explicit_labels_and_no_inference():
    start=utc('2025-05-24T00:00:00Z')
    result=summarize_population([],start=start,end=start+timedelta(days=28))
    assert len(result['outcomes'])==4 and result['chain_count']==0
    assert all(v['N']['mean'] is None for v in result['outcomes'].values())


def test_recognizer_strict_lower_low_tie_identity_controls_N():
    bars=candles((10.,9.,9.2,8.8,8.7,8.7,9.6,9.9))
    original=recognize_reversals(bars,context=CONTEXT,settings=settings('rising_leg_plus_retreat')).candidates[0]
    low=next(s for s in original.stages if s.name=='lowest_push')
    assert low.observed_at==bars[4].close_time
    changed=list(bars);changed[5]=replace(changed[5],low=8.5)
    later=recognize_reversals(tuple(changed),context=CONTEXT,settings=settings('rising_leg_plus_retreat')).candidates[0]
    assert next(s for s in later.stages if s.name=='lowest_push').observed_at==bars[5].close_time
    assert recovery_candles(original)==recovery_candles(later)+1


def test_missing_context_never_removes_a_completed_chain_or_its_weight():
    pop=population();bars,snapshot=world(pop)
    bars=tuple(replace(b,open=100.,high=101.,low=99.,close=100.) for b in bars)
    events=tuple(replace(e,event_id='',stages=e.stages+(RecognitionStage('lowest_push',e.known_at,e.known_at,e.stages[0].candle_id),)) for e in snapshot.candidates)
    result=measure_completed(bars,SimpleNamespace(candidates=events),pop)
    assert len(result['cases'])==len(events)==2
    assert all(r['context_missing_reasons'] for r in result['cases'])
    view=result['populations']['combined']
    assert view['context_missing_mass']==1
    assert sum(v['full_population_mass'] for v in view['outcomes'].values())==1
    assert 'pairs' not in result and 'contrasts' not in view
