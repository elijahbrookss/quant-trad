"""Synthetic first-passage contracts;no historical scientific outcome runs."""
from dataclasses import replace
import pytest

from research_science.recognition import RecognitionCandle, RecognitionEvent, plain
from research_science.reversal_recognition import recognize_reversals
from research_science.reversal_outcomes import first_passage, overlap_components, summarize
from tests.test_research_science.test_recognition import candles, settings, CONTEXT, t


def path(kind=None, hit=0):
    bars=list(candles())
    event=recognize_reversals(tuple(bars),context=CONTEXT,settings=settings('rising_leg_plus_retreat')).candidates[0]
    for i in range(7,19):
        high,low=10.,9.8
        if i==7+hit:
            if kind in ('continuation','both'):high=9.9+(9.9-8.6)
            if kind in ('failure','both'):low=8.6
        bars.append(RecognitionCandle(str(i),'source',t(i),t(i+1),t(i+1),9.9,high,low,9.9))
    return event,tuple(bars)


def run(event,bars,*,end=None):
    return first_passage(event,bars,by_open={b.open_time:b for b in bars},evaluation_end=end or t(19))


@pytest.mark.parametrize('kind,label,reason',[
    ('continuation','continuation','first_touch'),('failure','failure','first_touch'),
    ('both','unresolved','intrabar_order_unknown'),(None,'neither','no_touch_in_complete_horizon')])
def test_fixed_inclusive_barriers_and_all_four_outcome_classes(kind,label,reason):
    e,b=path(kind);r=run(e,b)
    assert r['anchor_price']==9.9 and r['formation_low']==pytest.approx(8.6)
    assert r['continuation_level']==pytest.approx(11.2)
    assert (r['classification'],r['reason'])==(label,reason)
    assert r['nominal_end']==plain(t(19))


def test_early_first_touch_survives_right_censoring_but_neither_needs_full_path():
    e,b=path('continuation')
    assert run(e,b[:9],end=t(9))['classification']=='continuation'
    e,b=path()
    assert run(e,b[:9],end=t(9))['reason']=='evaluation_boundary'
    assert run(e,b[:8]+b[9:])['reason']=='candle_missing'
    assert run(e,b[:7]+(replace(b[7],known_at=t(9)),)+b[8:])['reason']=='source_or_close_availability_mismatch'


def test_completion_candle_and_later_appends_cannot_rewrite_first_passage():
    e,b=path('failure',hit=2)
    # A completion-candle high does not count as a post-completion touch.
    b=b[:6]+(replace(b[6],high=20.),)+b[7:]
    result=run(e,b)
    assert result['classification']=='failure' and result['observed_bars']==3
    assert run(e,b[:10])==result


def test_overlap_components_use_closed_transitive_fixed_windows_and_equal_component_weights():
    def event(start,end):
        return RecognitionEvent('definition','1','settings',CONTEXT,'chain',t(start),t(end),t(end))
    events=(event(0,7),event(19,20),event(32,33),event(46,47))
    groups=overlap_components(events)
    assert [len(g['event_ids']) for g in groups]==[3,1]
    outcomes={e.event_id:{'classification':label,'reason':'test'} for e,label in
              zip(events,('continuation','failure','failure','neither'))}
    result=summarize(events,outcomes,groups)
    assert result['chain_weighted_shares']['failure']==.5
    assert result['equal_component_weighted_shares']['failure']==pytest.approx(1/3)
    assert result['equal_component_weighted_shares']['neither']==.5
    assert not result['independence_claim']
    assert result['component_chain_mass_hhi']==.625
    assert result['inverse_chain_mass_hhi']==1.6
    assert overlap_components(tuple(reversed(events)))==groups
    assert summarize((),{},())['equal_component_weighted_shares']['neither'] is None


@pytest.mark.parametrize('entry,low,reason',[(10.,10.,'invalid_nonpositive_formation_risk'),
    (1e308,1.,'invalid_nonfinite_continuation_barrier')])
def test_invalid_risk_or_numeric_barrier_is_retained_as_unresolved(entry,low,reason):
    event=RecognitionEvent('definition','1','settings',CONTEXT,'provisional_reversal_candidate',t(0),t(1),t(1))
    bar=RecognitionCandle('anchor','source',t(0),t(1),t(1),entry,entry,low,entry)
    result=run(event,(bar,))
    assert result['classification']=='unresolved' and result['reason']==reason
    assert result['observed_bars']==0 and result['outcome_known_at']==plain(t(1))
