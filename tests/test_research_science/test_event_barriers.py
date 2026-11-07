"""Synthetic public breakout events prove the barrier measurement has no pattern dispatch."""
from copy import deepcopy
from datetime import datetime,timedelta,timezone
import math
import pytest
from research_science.recognition import RecognitionCandle,plain
from research_science.event_barriers import measure_events,population_projection,population_summary
from research_science.hashing import stable_hash

ORIGIN=datetime(2024,1,1,tzinfo=timezone.utc)
STEP=timedelta(minutes=15)
SOURCE='test-native-source'

def candle_path(n=20,changes=None):
    changes=changes or{}
    return tuple(RecognitionCandle.from_mapping({'open_time':ORIGIN+i*STEP,'close_time':ORIGIN+(i+1)*STEP,
        'known_at':ORIGIN+(i+1)*STEP,'open':10.,'high':changes.get(i,{}).get('high',10.5),
        'low':changes.get(i,{}).get('low',9.5),'close':10.},source_identity_key=SOURCE)for i in range(n))

def config():return {'horizon_bars':12,'support_lookback_bars':2,
    'reference':{'operator':'event_close'},'failure':{'operator':'event_metadata_number','path':'metadata.stop'},
    'target':{'operator':'event_metadata_number','path':'metadata.target'},'illustrations':[]}

def event(at=None,identity='breakout-one',**metadata):
    at=at or ORIGIN+4*STEP
    return {'indicator_id':'breakout-indicator','output_type':'signal','event_key':'range_breakout',
        'output_name':'breakouts','time':plain(at-STEP),'known_at':plain(at),
        'event':{'pattern_id':identity,'direction':'long','key':'range_breakout','known_at':plain(at),
            'metadata':{'stop':9.,'target':11.,'strength':2.,**metadata}}}

FEATURES=[{'name':'strength','operator':'event_metadata_number','path':'metadata.strength','units':'ratio'}]

def run(changes=None,*,n=20,cfg=None,outputs=None):
    bars=candle_path(n,changes)
    return measure_events(outputs or[event()],bars,config=cfg or config(),features=FEATURES,
        start=plain(ORIGIN+4*STEP),end=plain(ORIGIN+n*STEP),source_key=SOURCE)

def test_non_reversal_breakout_failure_then_strictly_later_target_preserves_label_and_input():
    outputs=[event()];before=deepcopy(outputs)
    result=run({4:{'low':8.5},5:{'high':11.5}},outputs=outputs)
    row=result['cases'][0]
    assert row['outcome']['classification']=='failure'and row['outcome']['observed_bars']==1
    assert row['later_target']['status']=='positive'
    assert row['later_target']['first_later_target']=={'bar_index':2,'close_time':plain(ORIGIN+6*STEP),'known_at':plain(ORIGIN+6*STEP)}
    assert row['later_target']['original_label']=='failure'and row['original_weight']==1.
    assert result['summary']['labels']['failure']['original_mass']==1.
    assert result['later_target_summary']['statuses']['positive']['share_of_failure_mass']==1.
    assert outputs==before

@pytest.mark.parametrize('changes,label',[(None,'neither'),({4:{'high':11.5}},'continuation'),
    ({4:{'low':8.5}},'failure'),({4:{'high':11.5,'low':8.5}},'unresolved')])
def test_all_four_original_classes_and_samebar_double_touch(changes,label):
    row=run(changes)['cases'][0]
    assert row['outcome']['classification']==label
    if label=='unresolved':assert row['outcome']['reason']=='intrabar_order_unknown'and row['later_target']['status']=='not_applicable'


def test_later_double_touch_is_target_observation_without_reordering_original_failure():
    row=run({4:{'low':8.5},5:{'high':11.5,'low':8.5}})['cases'][0]
    assert row['outcome']['classification']=='failure'
    assert row['later_target']['status']=='positive'and row['later_target']['first_later_target']['bar_index']==2


def test_partial_unhit_is_unresolved_and_partial_failure_followup_is_unknown():
    row=run(n=8)['cases'][0]
    assert row['outcome']['classification']=='unresolved'and row['outcome']['reason']=='evaluation_boundary'
    assert row['followup_support']['available_bars']==4 and not row['followup_support']['complete_horizon']
    failed=run({4:{'low':8.5}},n=8)['cases'][0]
    assert failed['outcome']['classification']=='failure'
    assert failed['later_target']['status']=='unknown'and failed['later_target']['observed_postfailure_bars']==3


def test_observed_later_target_remains_positive_when_rest_of_horizon_is_censored():
    row=run({4:{'low':8.5},5:{'high':11.5}},n=8)['cases'][0]
    assert row['later_target']['status']=='positive'and row['later_target']['reason']=='evaluation_boundary'
    assert not row['later_target']['complete_horizon']
    assert row['later_target']['observation_known_at']==plain(ORIGIN+6*STEP)


def test_failure_on_last_bar_is_structural_negative_and_fixed_opportunity_rows_exist():
    result=run({15:{'low':8.5}});row=result['cases'][0]
    assert row['later_target']['status']=='negative'and row['later_target']['first_failure_bar']==12
    assert row['later_target']['structural_no_remaining_opportunity']and row['later_target']['observed_postfailure_bars']==0
    table=result['later_target_summary']['opportunity_by_first_failure_bar']
    assert len(table)==12 and table[0]['count']==0 and table[-1]['count']==1


def test_explicit_breakout_target_differs_from_symmetric_reversal_level():
    result=run({4:{'high':11.5}},outputs=[event(target=12.)])
    assert result['cases'][0]['outcome']['classification']=='neither'
    assert result['cases'][0]['outcome']['continuation_level']==12.


def test_configured_shorter_horizon_and_invalid_geometry_are_not_relabelled():
    cfg={**config(),'horizon_bars':3}
    result=run({7:{'high':11.5}},cfg=cfg)
    assert result['cases'][0]['outcome']['classification']=='neither'
    assert result['cases'][0]['outcome']['observed_bars']==3
    row=run(outputs=[event(stop=10.)])['cases'][0]
    assert row['outcome']['classification']=='unresolved'and row['outcome']['reason']=='invalid_nonpositive_formation_risk'


def test_nominal_support_and_common_origin_remain_outcome_blind():
    a,b=run(),run({4:{'low':8.5},5:{'high':11.5}})
    assert a['population_hash']==b['population_hash']
    assert stable_hash(population_projection(a['cases'],a['dependence']))==a['population_hash']
    assert a['dependence']==b['dependence']
    assert a['cases'][0]['support_end']==plain(ORIGIN+16*STEP)
    assert a['independent_sample_size']is None


def test_unique_ids_and_public_identity_source_clock_guards():
    with pytest.raises(ValueError,match='unique public pattern_id'):run(outputs=[event(),event()])
    cfg={**config(),'identity_path':'metadata.opaque_id'}
    with pytest.raises(ValueError,match='identity_path'):run(cfg=cfg,outputs=[event(opaque_id='different')])
    cfg={**config(),'source_path':'metadata.source'}
    with pytest.raises(ValueError,match='source_path'):run(cfg=cfg,outputs=[event(source='wrong')])


def test_whole_window_low_is_not_reference_price_and_duration_works_without_context():
    cfg={**config(),'window_start_path':'metadata.setup_start','failure':{'operator':'window_low'},
        'target':{'operator':'risk_multiple','multiple':1.}}
    row=run({0:{'low':8.}},cfg=cfg,outputs=[event(setup_start=plain(ORIGIN),reference_price=9.8,strength=None)])['cases'][0]
    assert row['boundary_geometry']['L']==8. and row['boundary_geometry']['upper']==12.
    assert row['reference_window_bars']==4 and row['features']['strength']is None


def test_original_population_clock_weights_survive_missing_fields_and_class_filtering():
    def r(identity,clock,label,value):return {'event_id':identity,'decision_at':plain(ORIGIN+clock*STEP),
        'features':{'x':value},'feature_missing_reasons':{'x':'context_missing'}if value is None else{},'outcome':{'classification':label}}
    rows=[r('a',1,'continuation',2.),r('b',1,'failure',4.),r('c',2,'continuation',None),
          r('d',2,'continuation',4.),r('e',2,'continuation',None)]
    summary=population_summary(rows,[{'name':'x','units':'bars'}],start=ORIGIN,end=ORIGIN+3*STEP,illustrations=[])
    target=summary['labels']['continuation'];dist=target['features']['x']
    assert target['original_mass']==pytest.approx(.75)
    assert dist['observed_mass']==pytest.approx(5/12)and dist['missing_mass']==pytest.approx(1/3)
    assert dist['mean']==pytest.approx(2.8)and dist['quantiles']['0.5']==2.
    contrast=summary['contrasts']['continuation_minus_failure']['x']
    assert contrast['mean_difference']==pytest.approx(-1.2)and contrast['rank_dominance']['value']==pytest.approx(-.6)
    assert summary['labels']['neither']['features']['x']['mean']is None
    assert summary['contrasts']['continuation_minus_neither']['x']['rank_dominance']['value']is None
    assert math.fsum(summary['all_case_weights'].values())==pytest.approx(1.)


def test_empty_population_retains_four_labels_null_summaries_and_empty_illustrations():
    summary=population_summary([],FEATURES,start=ORIGIN,end=ORIGIN+STEP,
        illustrations=[{'classification':'failure','feature':'strength','order':'max'}])
    assert set(summary['labels'])=={'continuation','failure','neither','unresolved'}
    assert summary['chain_count']==0 and summary['all_case_weights']=={}
    assert summary['labels']['failure']['features']['strength']['mean']is None
    assert summary['illustrations'][0]['event_id']is None
