"""Synthetic contracts/prefix qualification;no historical data or outcome search."""
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone, timedelta
import pytest

from research_science.recognition import (RecognitionCandle, RecognitionContext,
    RecognitionEvent, RecognitionLevel, RecognitionStage, link_source_event)
from research_science.reversal_recognition import (ReversalRecognizer, ReversalSettings,
                                                   recognize_reversals)

ORIGIN = datetime(2022, 1, 1, tzinfo=timezone.utc)
CONTEXT = RecognitionContext("instrument", 900, ORIGIN, "source")


def t(i):
    return ORIGIN+timedelta(minutes=15*i)


def candles(closes=(10., 9., 9.2, 8.8, 8.7, 9.6, 9.9)):
    result=[]
    for i,c in enumerate(closes):
        o=closes[i-1] if i else c+.1
        high=max(o,c)+.1
        if i==2:high=9.5
        if i==3:high=9.7
        result.append(RecognitionCandle(str(i),"source",t(i),t(i+1),t(i+1),o,high,min(o,c)-.1,c))
    return tuple(result)


def settings(basis="rising_leg_only", **values):
    return ReversalSettings(basis,"retain_all_provisional",values.get("max_bars",100),values.get("max_candidates",100))


def test_reference_price_clock_precedes_role_and_later_push_is_required():
    state=ReversalRecognizer(CONTEXT,settings())
    for bar in candles()[:4]:state.apply_bar(bar)
    snap=state.snapshot();assert not snap.candidates
    ref=snap.references[0];level=ref.levels[0]
    assert level.price==9.5 and level.observed_at==level.price_known_at==t(3)
    assert level.role_known_at==ref.known_at==t(4)
    # The confirmation candle itself crosses the anchor but is not reused as a later push.
    assert not any(s.name=='lower_low_push' for s in snap.pending[0].stages)
    state.apply_bar(candles()[4]);state.apply_bar(candles()[5])
    event=state.snapshot().candidates[0]
    assert event.occurred_at==event.known_at==t(6)
    assert event.source_events[0].event_id==ref.event_id
    assert event.source_events[0].consumed_at==t(5)
    assert RecognitionEvent.from_dict(event.to_dict())==event


def test_policy_change_is_explicit_and_changes_identity_without_sweeping_outcomes():
    a=recognize_reversals(candles(),context=CONTEXT,settings=settings())
    b=recognize_reversals(candles(),context=CONTEXT,settings=settings('rising_leg_plus_retreat'))
    assert a.references[0].levels[0].price==9.5 and b.references[0].levels[0].price==9.7
    assert a.candidates[0].occurred_at==t(6) and b.candidates[0].occurred_at==t(7)
    assert a.references[0].event_id!=b.references[0].event_id


def test_prefix_append_never_rewrites_completed_event_or_previous_snapshot():
    state=ReversalRecognizer(CONTEXT,settings())
    bars=candles()
    for b in bars[:6]:state.apply_bar(b)
    old=state.snapshot();before=old.candidates[0].to_dict()
    for b in bars[6:]:state.apply_bar(b)
    assert old.candidates[0].to_dict()==before
    assert [e.to_dict() for e in state.snapshot().candidates if e.known_at<=t(6)]==[before]
    with pytest.raises(FrozenInstanceError):old.candidates[0].kind='changed'


def test_viewport_filter_is_separate_and_cropped_input_cannot_silently_reinitialize():
    whole=recognize_reversals(candles(),context=CONTEXT,settings=settings())
    assert [e for e in whole.candidates if t(5)<=e.occurred_at<t(7)]==list(whole.candidates)
    with pytest.raises(ValueError,match='origin'):
        recognize_reversals(candles()[1:],context=CONTEXT,settings=settings())
    moved=replace(CONTEXT,computation_origin=t(1))
    assert not recognize_reversals(candles()[1:6],context=moved,settings=settings()).candidates


def test_ambiguous_references_are_retained_without_an_implicit_winner():
    bars=[];cs=[15.,14.,14.2,13.8,14.,13.5,13.2,14.8]
    for i,c in enumerate(cs):
        o=cs[i-1] if i else c+.1
        bars.append(RecognitionCandle(str(i),'source',t(i),t(i+1),t(i+1),o,max(o,c)+.1,min(o,c)-.1,c))
    result=recognize_reversals(tuple(bars),context=CONTEXT,settings=settings())
    assert len(result.candidates)==2
    assert {e.occurred_at for e in result.candidates}=={t(8)}
    assert len({e.source_events[0].event_id for e in result.candidates})==2


@pytest.mark.parametrize('change',[{'open_time':t(2)},{'known_at':t(3)},{'source_identity_key':'other'}])
def test_gap_delay_and_source_change_fail_not_reset(change):
    bars=candles();state=ReversalRecognizer(CONTEXT,settings());state.apply_bar(bars[0])
    with pytest.raises(ValueError):state.apply_bar(replace(bars[1],**change))


def test_resource_limits_fail_instead_of_truncating_candidates():
    with pytest.raises(ValueError,match='maximum bars'):
        recognize_reversals(candles(),context=CONTEXT,settings=settings(max_bars=3))
    with pytest.raises(ValueError,match='maximum references'):
        recognize_reversals(candles((10.,9.,9.2,8.8,9.,8.5)),context=CONTEXT,settings=settings(max_candidates=1))
    with pytest.raises(ValueError):ReversalSettings.from_dict({'reference_basis':'rising_leg_only'})
    with pytest.raises(ValueError):settings(max_bars=True)
    with pytest.raises(ValueError):ReversalSettings('rising_leg_only','latest_only',100,100)


def event(at, *, known=None, kind='example'):
    return RecognitionEvent('ordinary_python_definition','1','settings',CONTEXT,kind,t(0),t(at),known or t(at))


def test_ordinary_function_composition_requires_explicit_source_clock_and_bound():
    source=event(2);target=event(5,kind='followup')
    linked=link_source_event(target,source,consumed_at=t(3),max_age_seconds=900,relation='uses_context')
    assert linked.source_events[0].event_id==source.event_id and linked.event_id!=target.event_id
    assert RecognitionEvent.from_dict(linked.to_dict())==linked
    with pytest.raises(ValueError):link_source_event(target,source,consumed_at=t(4),max_age_seconds=900,relation='expired')
    with pytest.raises(ValueError):link_source_event(target,event(2,known=t(4)),consumed_at=t(3),max_age_seconds=9000,relation='future')
    with pytest.raises(ValueError):link_source_event(target,event(5),consumed_at=t(5),max_age_seconds=9000,relation='same_clock')
    with pytest.raises(ValueError):link_source_event(linked,source,consumed_at=t(3),max_age_seconds=900,relation='duplicate')
    with pytest.raises(ValueError):link_source_event(target,replace(source,context=replace(CONTEXT,instrument_id='other'),event_id=''),consumed_at=t(3),max_age_seconds=900,relation='cross_market')


def test_bad_clocks_prices_or_tampered_event_identity_fail():
    with pytest.raises(ValueError):replace(candles()[0],high=float('nan'))
    with pytest.raises(ValueError):replace(candles()[0],known_at=t(0))
    with pytest.raises(ValueError):RecognitionStage('future',t(2),t(1),'id')
    with pytest.raises(ValueError):RecognitionLevel('R',10.,t(2),t(3),t(1),'id')
    with pytest.raises(ValueError):RecognitionEvent.from_dict({**event(2).to_dict(),'kind':'tampered'})


def test_material_candle_conversion_normalizes_clocks_and_rejects_source_conflicts():
    b=candles()[0]
    row={**vars(b),'open_time':b.open_time.isoformat(),'known_at':b.known_at.isoformat()}
    a=RecognitionCandle.from_mapping(row)
    assert RecognitionCandle.from_mapping({**row,'open_time':plain_clock(b.open_time)})==a
    assert RecognitionCandle.from_mapping({**row,'close_time':t(0)+timedelta(minutes=10)}).candle_id!=a.candle_id
    with pytest.raises(ValueError,match='conflicting source'):
        RecognitionCandle.from_mapping(row,source_identity_key='other')


def plain_clock(value):
    return value.isoformat().replace('+00:00','Z')


def test_january27_retreat_high_is_locked_at_retreat_close_without_rewriting_old_choice():
    # Exact four-bar excerpt from already-retained January27 Coinbase15m facts.
    # This local reference-rule regression is not a fixed-origin population replay.
    from research_science.recognition import utc
    origin=utc('2022-01-27T01:30:00Z')
    context=replace(CONTEXT,computation_origin=origin)
    prices=((36692.71,36698.76,36162.81,36279.82),
            (36274.92,36318.26,35844.,35897.08),
            (35895.28,35948.70,35778.15,35898.17),
            (35900.12,35996.37,35734.07,35883.26))
    bars=tuple(RecognitionCandle(str(i),'source',origin+timedelta(minutes=15*i),
        origin+timedelta(minutes=15*(i+1)),origin+timedelta(minutes=15*(i+1)),*p) for i,p in enumerate(prices))
    state=ReversalRecognizer(context,settings('rising_leg_plus_retreat'))
    for b in bars[:3]:state.apply_bar(b)
    assert state.snapshot().references==()
    state.apply_bar(bars[3]);ref=state.snapshot().references[0]
    assert ref.levels[0].price==35996.37 and ref.levels[0].candle_id==bars[3].candle_id
    assert ref.levels[0].price_known_at==ref.levels[0].role_known_at==ref.known_at==utc('2022-01-27T02:30:00Z')
    old=recognize_reversals(bars,context=context,settings=settings('rising_leg_only')).references[0]
    assert old.levels[0].price==35948.70 and old.event_id!=ref.event_id
    assert old.configuration_hash!=ref.configuration_hash
    later=RecognitionCandle('later','source',bars[-1].close_time,bars[-1].close_time+timedelta(minutes=15),
        bars[-1].close_time+timedelta(minutes=15),35883.26,37000.,35800.,36000.)
    state.apply_bar(later)
    assert state.snapshot().references[0]==ref
