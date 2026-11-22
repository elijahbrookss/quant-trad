"""All-completed reversal composition over shared causal context/path kernels.

Every completed chain remains in its original population. This module performs
no matching, adjusted comparison, threshold search or inferential test.
"""
from collections import Counter
import json
import math
from .completed_event_context import CompletedEventContext
from .recognition import plain,utc
from .reversal_outcomes import CLASSES,first_passage_path
from .reversal_selection import moment_weights,stage_availability,_components
from .hashing import stable_hash
from .weighted_summaries import first_crossing_quantiles,sequential_prefixes

SCHEMA='reversal_completed_recovery.v1'
QUANTILES=(0.,.25,.5,.75,.9,.95,1.)
RULES={
    'version':SCHEMA,
    'population':'All completed retreat-inclusive reversal chains;retain simultaneous chains and missing context',
    'recovery':'N=1+(completion close-final strict-lower-low lowest_push observed_at)/900;inclusive integer>=1;equal lows keep first',
    'context':'Existing strictly prior sigma96,r,z_pre12 and diagnostic geometry;no context eligibility filtering',
    'outcome':'Unchanged first_passage_path;E completion close,L whole formation low,E+(E-L),next12bars,inclusive touch;both first bar unresolved;no tail',
    'weights':'Per reporting view,1/(ALL completion clocks*ALL chains at that clock);never reweight a label subset',
    'summary':'Four separate labels;original full-population label mass,observed/missing N mass,count,mean,first weighted-CDF crossing quantiles',
    'views':'Declared block views plus combined;all rows retained;weights normalize over all completions within each view',
    'dependence':'Prior97/context and whole-formation start to nominal+12bar-end support;common-origin ancestry;no independent-N',
    'comparison':'None;no pairs,calipers,strata,adjusted contrast,classifier,inference or trading claim',
    'limits':{'candles':19104,'wall_seconds':120,'input_bytes':268435456,'result_bytes':16777216},
}


def recovery_candles(event):
    """The recognizer owns final-low identity;do not rediscover it from prices."""
    stages = [s for s in event.stages if s.name == 'lowest_push']
    if len(stages) != 1:
        raise ValueError('completed_recovery_invalid: exact lowest_push stage required')
    seconds = (event.known_at-stages[0].observed_at).total_seconds()
    if seconds < 0 or seconds % 900:
        raise ValueError('completed_recovery_invalid: aligned observed low before completion required')
    return 1+int(seconds/900)


def distribution(values):
    values = sorted(values)
    mass = math.fsum(w for _,w in values)
    return {'observed_weight':mass,'mean':math.fsum(x*w for x,w in values)/mass if mass else None,
        'quantiles':first_crossing_quantiles(values,QUANTILES,total_weight=mass,prefixes=sequential_prefixes),
        'quantile_rule':'first weighted CDF crossing;no interpolation',
        'empty_reason':None if mass else 'no_observed_weight'}


def summarize_population(records,*,start,end):
    selected=[r for r in records if start<=utc(r['decision_at'])<end]
    weights=moment_weights(selected)
    labels={}
    for label in CLASSES:
        members=[r for r in selected if r['outcome']['classification']==label]
        observed=[r for r in members if isinstance(r.get('recovery_candles'),(int,float))
            and math.isfinite(r['recovery_candles'])]
        observed_ids={r['event_id'] for r in observed}
        missing=[r for r in members if r['event_id'] not in observed_ids]
        labels[label]={'chain_count':len(members),
            'full_population_mass':math.fsum(weights[r['event_id']] for r in members),
            'N':{**distribution([(r['recovery_candles'],weights[r['event_id']]) for r in observed]),
                'observed_count':len(observed),'missing_count':len(missing),
                'missing_weight':math.fsum(weights[r['event_id']] for r in missing)}}
    return {'evaluation_range':{'start':plain(start),'end_exclusive':plain(end)},
        'chain_count':len(selected),'completion_clock_count':len({r['decision_at'] for r in selected}),
        'all_case_weights':weights,'outcomes':labels,
        'context_missing_count':sum(bool(r['context_missing_reasons']) for r in selected),
        'context_missing_mass':math.fsum(weights[r['event_id']] for r in selected if r['context_missing_reasons']),
        'unresolved_reasons':dict(Counter(r['outcome']['reason'] for r in selected if r['outcome']['classification']=='unresolved')),
        'independence_claim':False,'context_adjusted_comparison':False}


def measure_completed(candles,snapshot,population,*,checkpoint=lambda:None):
    if (not candles or len(candles)!=population.input_bars or candles[0].open_time!=population.origin
            or candles[-1].close_time!=population.end):
        raise ValueError('completed_recovery_invalid: complete original input required')
    context=CompletedEventContext(candles,snapshot.candidates,evaluation_start=population.start,
        evaluation_end=population.end,checkpoint=checkpoint)
    records=[]
    for event in sorted(snapshot.candidates,key=lambda e:(e.known_at,e.event_id)):
        checkpoint()
        if not population.start<=event.known_at<population.end:continue
        row=context.record(event,timing_view=stage_availability)
        row.pop('match_status')
        row['recovery_candles']=recovery_candles(event)
        row['recovery_missing_reason']=None
        features=row['features'] or {}
        row['context_missing_reasons']=[name+'_missing_or_invalid' for name in ('sigma','r','z_pre12')
            if not isinstance(features.get(name),(int,float)) or not math.isfinite(features[name])
            or (name!='z_pre12' and features[name]<=0)]
        records.append(row)
    weights=moment_weights(records)
    for row in records:row['full_population_moment_weight']=weights[row['event_id']]
    support=_components(records,[],recognition_origin=plain(population.origin))
    dependence={'direct_support_components':support['direct_support_and_pair_components'],
        'common_origin_ancestry':support['ancestry_aware_components'],'independent_sample_size':None,
        'meaning':'Direct context/formation supports and conservative common-origin ancestry;no pairing or independence claim'}
    population_hash=stable_hash(json.loads(json.dumps({'cases_except_outcome':records,'dependence':dependence,
        'population_recipe':population.to_dict()},allow_nan=False)))
    by_open={c.open_time:c for c in candles}
    for row in records:
        checkpoint()
        geometry=row['boundary_geometry']
        row['outcome']=first_passage_path(anchor_at=utc(row['decision_at']),known_at=utc(row['decision_at']),
            entry=geometry['E'],low=geometry['L'],source_identity_key=candles[0].source_identity_key,
            timeframe_seconds=900,by_open=by_open,evaluation_end=population.end,checkpoint=checkpoint)
    views={name:summarize_population(records,start=start,end=end) for name,start,end in population.ranges()}
    return {'rules':RULES,'cases':records,'dependence':dependence,'populations':views,
        'population_recipe':population.to_dict(),'completion_population_hash':population_hash,
        'completion_population_hash_projection':'cases_except_outcome={cases with only outcome removed};dependence;population_recipe;JSON canonical',
        'outcomes_calculated':True}
