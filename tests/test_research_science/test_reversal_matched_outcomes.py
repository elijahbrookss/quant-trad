"""Synthetic saved-manifest outcomes; never load historical market prices."""
from copy import deepcopy
from datetime import timedelta
import pytest
from research_science.recognition import RecognitionCandle, utc, plain
from research_science.reversal_matched_outcomes import describe_selected_pairs
from research_science.study import stable_hash


def fixture():
    bars=[]
    for origin in ('2025-02-01T00:00:00Z','2025-05-01T00:00:00Z'):
        at=utc(origin)
        for i in range(96):
            opened=at+timedelta(minutes=15*i)
            high,low=100.5,99.5
            if i==7:low=99.
            if i in (27,39):low=99.
            if i==8:
                high=101.
                if '05-01' in origin:low=99.
            if i==28 and '02-01' in origin:low=99.
            bars.append(RecognitionCandle(f'{origin}:{i}','source',opened,opened+timedelta(minutes=15),
                opened+timedelta(minutes=15),100.,high,low,100.))
    def row(date,index):
        at=utc(date)+timedelta(minutes=15*index)
        return {'decision_at':plain(at),'boundary_geometry':{'E':100.,'L':99.,'upper':101.,'known_at':plain(at)}}
    cases=[];pairs=[]
    for key,date,block,ci,weight in [('a','2025-02-01T00:00:00Z',0,28,.25),('b','2025-02-01T00:00:00Z',0,40,.25),('c','2025-05-01T00:00:00Z',3,40,.5)]:
        case={**row(date,8),'event_id':key,'formation_candles':1,'block':block,'match_status':'matched'}
        control={**row(date,ci),'control_id':'control-'+key,'trailing_candles':1}
        cases.append(case);pairs.append({'event_id':key,'decision_at':case['decision_at'],'control':control,'completion_moment_weight':weight})
    dep={'direct_support_and_pair_components':[{}],'ancestry_aware_components':[{'independent_sample_size':None}]}
    selection={'cases':cases,'pairs':pairs,'selection_summary':{'all':{'chains':3,'completion_clocks':2},'matched':{'chains':3,'completion_clocks':2},
        'retained_full_population_moment_mass':1.,'case_status_counts':{'matched':3}},
        'populations':{n:{'dependence':{'matched_cases_and_controls':deepcopy(dep)}} for n in ('old','new','combined')}}
    return selection,tuple(bars),utc('2025-05-01T10:30:00Z')


def test_saved_pair_weights_all_classes_old_new_and_boundary_are_preserved():
    selection,bars,end=fixture();before=deepcopy(selection)
    result=describe_selected_pairs(selection,bars,evaluation_end=end)
    assert selection==before
    assert [p['completion_moment_weight'] for p in result['pairs']]==[.25,.25,.5]
    old=result['matched_outcome_comparison']['old'];new=result['matched_outcome_comparison']['new']
    combined=result['matched_outcome_comparison']['combined']
    assert old['pairs']==2 and old['case_completion_moments']==1
    assert old['case']['weighted_class_fractions']['continuation']==1.
    assert old['control']['weighted_class_fractions']['failure']==.5
    assert old['control']['weighted_class_fractions']['neither']==.5
    assert new['case']['unresolved_reasons']=={'intrabar_order_unknown':1}
    assert new['control']['unresolved_reasons']=={'evaluation_boundary':1}
    assert combined['case']['weighted_class_fractions']['continuation']==.5
    assert combined['control']['weighted_class_fractions']['failure']==.25
    assert combined['case_minus_control']['continuation']==.5
    assert all(sum(combined[arm]['weighted_class_fractions'].values())==1 for arm in ('case','control'))
    assert not result['reselection_performed'] and combined['independent_sample_size'] is None
    assert result['matched_outcome_manifest_hash']==stable_hash({k:v for k,v in result.items() if k!='matched_outcome_manifest_hash'})


@pytest.mark.parametrize('change',[
 lambda s:s['pairs'][0]['control']['boundary_geometry'].update(L=98.),
 lambda s:s['pairs'][0].update(completion_moment_weight=.3),
 lambda s:s['pairs'].pop(),
 lambda s:s['pairs'][0]['control'].update(trailing_candles=2),
 lambda s:s['pairs'][1]['control'].update(decision_at=s['pairs'][0]['control']['decision_at']),
])
def test_changed_barriers_weights_population_or_control_identity_fail(change):
    selection,bars,end=fixture();change(selection)
    with pytest.raises(ValueError,match='matched_outcomes_invalid'):
        describe_selected_pairs(selection,bars,evaluation_end=end)
