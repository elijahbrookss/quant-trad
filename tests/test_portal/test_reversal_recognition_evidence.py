"""Visual consumers reuse causal domain identities without retired executors."""
import pytest
from research_science.recognition import plain, utc
from tests.test_research_science.test_recognition import candles, t


def test_visual_consumer_uses_identical_event_identity_and_masks_after_completion():
    from scripts.reporting.reversal_recognition_preview import prepare_preview
    from research_science.reversal_recognition import ReversalSettings
    from research_science.recognition import RecognitionCandle, RecognitionContext
    from research_science.reversal_recognition import recognize_reversals
    snapshot=recognize_reversals(tuple(RecognitionCandle.from_mapping(vars(b)) for b in candles()),context=RecognitionContext('instrument',900,t(0),'source'),
        settings=ReversalSettings('rising_leg_only','retain_all_provisional',100,100))
    data={'start_inclusive':plain(t(0)),
          'series_manifest':{'instrument_id':'instrument','timeframe_seconds':900},
          'records':[{'source_identity_key':'source','fact':{**vars(b)}} for b in candles()]}
    cases=[{'label':'synthetic','start':plain(t(0)),'end':plain(t(7)),
            'reference_known_at':plain(t(4))}]
    preview=prepare_preview(data,cases,settings=ReversalSettings('rising_leg_only','retain_all_provisional',100,100))
    shown=preview['examples'][0]
    assert shown['candidate']['event_id']==snapshot.candidates[0].event_id
    assert utc(shown['as_of'])==t(6) and len(shown['candles'])==6
    assert preview['study_events_admitted']==0 and not preview['outcomes_calculated']
    pending_case={**cases[0],'end':plain(t(5))}
    pending=prepare_preview(data,[pending_case],settings=ReversalSettings('rising_leg_only','retain_all_provisional',100,100))['examples'][0]
    assert pending['candidate'] is None and utc(pending['as_of'])==t(5)
    assert all(utc(s['known_at'])<=t(5) for s in pending['stages'])


def test_preview_chart_labels_validated_instrument_without_assuming_btc(monkeypatch,tmp_path):
    from scripts.reporting.reversal_recognition_preview import prepare_preview,render_preview
    from research_science.reversal_recognition import ReversalSettings
    from matplotlib.figure import Figure
    data={'start_inclusive':plain(t(0)),
          'series_manifest':{'instrument_id':'ETH-USDT','timeframe_seconds':900},
          'records':[{'source_identity_key':'source','fact':vars(b)} for b in candles()]}
    cases=[{'label':'synthetic','start':plain(t(0)),'end':plain(t(7)),
            'reference_known_at':plain(t(4))}]
    preview=prepare_preview(data,cases,settings=ReversalSettings('rising_leg_only','retain_all_provisional',100,100))
    captured={}
    def save(figure,*args,**kwargs):
        captured['title']=figure._suptitle.get_text()
        captured['ylabels']=[axis.get_ylabel() for axis in figure.axes]
    monkeypatch.setattr(Figure,'savefig',save)
    render_preview(preview,tmp_path/'preview.png')
    assert 'instrument ETH-USDT' in captured['title']
    assert 'BTC' not in captured['title'] and captured['ylabels']==['Price']
    invalid={**preview,'context':{**preview['context'],'instrument_id':''}}
    with pytest.raises(ValueError,match='nonempty identity'):
        render_preview(invalid,tmp_path/'invalid.png')


def preparation_fixture():
    from types import SimpleNamespace
    import pandas as pd
    plan=SimpleNamespace(gap_policy='reject',
        market_data_requirements=({'alias':'primary_bars','instrument_id':'instrument','timeframe_seconds':900},),
        evaluation_range={'start':plain(t(1)),'end_exclusive':plain(t(7))},
        materialization_range={'start':plain(t(0))})
    detector={'type':'completed_reversal_candidates','computation_origin':plain(t(0)),
        'reference_basis':'rising_leg_plus_retreat','ownership':'retain_all_provisional',
        'max_bars':100,'max_candidates':100}
    frame=pd.DataFrame([{'timestamp':b.open_time,'close_time':b.close_time,'known_at':b.known_at,
        'source_identity_key':b.source_identity_key,'known_at_method':'interval_close_inferred',
        'open':b.open,'high':b.high,'low':b.low,'close':b.close} for b in candles()])
    return plan,{'detector':detector,'outcomes':{'schema_version':'reversal_recognition.v1'},'candles':frame}


def test_shared_preparation_preserves_exact_recognizer_snapshot_and_source_frame():
    import pandas as pd
    from portal.backend.service.research.recognition_preparation import prepare_recognition
    from research_science.recognition import RecognitionContext
    from research_science.reversal_recognition import recognize_reversals
    plan,inputs=preparation_fixture()
    before=inputs['candles'].copy(deep=True)
    settings,snapshot,rows,start,end,bars=prepare_recognition(plan=plan,inputs=inputs)
    expected=recognize_reversals(bars,context=RecognitionContext('instrument',900,t(0),'source'),settings=settings)
    assert snapshot == expected
    assert start==t(1) and end==t(7) and len(rows)==len(bars)==7
    pd.testing.assert_frame_equal(inputs['candles'],before)


@pytest.mark.parametrize('change', ['interior_gap','missing_origin','late','changed_origin','outcomes','gap_policy'])
def test_shared_preparation_does_not_repair_missing_late_or_unpinned_input(change):
    from portal.backend.service.research.recognition_preparation import prepare_recognition
    plan,inputs=preparation_fixture()
    if change=='interior_gap': inputs['candles']=inputs['candles'].drop(index=3)
    elif change=='missing_origin': inputs['candles']=inputs['candles'].iloc[1:]
    elif change=='late': inputs['candles'].loc[2,'known_at']=t(4)
    elif change=='changed_origin': plan.materialization_range={'start':plain(t(1))}
    elif change=='outcomes': inputs['outcomes']={'schema_version':'different'}
    elif change=='gap_policy': plan.gap_policy='continue_degraded'
    with pytest.raises(ValueError):
        prepare_recognition(plan=plan,inputs=inputs)
