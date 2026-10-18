"""First-passage description on an immutable selected population; never rematch."""
from collections import Counter
from datetime import timedelta
import math

from .recognition import utc, plain
from .reversal_outcomes import CLASSES, HORIZON_BARS, first_passage_path
from .study import stable_hash

SCHEMA='reversal_matched_first_passage.v1'
RULES={
 'version':SCHEMA, 'population':'Exactly the saved selection pairs; no recognition or rematching',
 'anchor':'Saved completion/control close E; observational, not an executed fill',
 'failure':'Saved whole-formation low for cases; own trailing-n low for controls',
 'continuation':'E+(E-L); inclusive touches; next candle through12bars',
 'horizon_bars':12, 'both_first_touched_same_bar':'unresolved:intrabar_order_unknown',
 'missing_or_boundary':'unresolved unless first passage already known; never drop',
 'weighting':'Saved equal completion-moment pair weights; renormalize within reporting view',
 'contrast':'Weighted case minus control fractions for each of all four classes',
 'dependence':'Retain saved support/ancestry components; no independent-trial inference',
 'limits':{'candles':19104,'pairs':10000,'wall_seconds':120,'input_bytes':268435456,'result_bytes':16777216}}


def describe_selected_pairs(selection, candles, *, evaluation_end, checkpoint=lambda:None):
    by_open={c.open_time:c for c in candles}
    by_close={c.close_time:c for c in candles}
    if len(by_open)!=len(candles):raise ValueError('matched_outcomes_invalid: duplicate candle clock')
    cases={c['event_id']:c for c in selection['cases']}
    pairs=selection['pairs']
    if len(cases)!=len(selection['cases']) or len(pairs)>RULES['limits']['pairs']:
        raise ValueError('matched_outcomes_invalid: case identity or pair limit')
    if Counter(p['event_id'] for p in pairs)!=Counter(c['event_id'] for c in cases.values() if c['match_status']=='matched'):
        raise ValueError('matched_outcomes_invalid: pair population differs from saved matched cases')
    if len({p['control']['decision_at'] for p in pairs})!=len(pairs):
        raise ValueError('matched_outcomes_invalid: reused control clock')
    if pairs and not math.isclose(math.fsum(p['completion_moment_weight'] for p in pairs),1.,abs_tol=1e-12):
        raise ValueError('matched_outcomes_invalid: saved weights do not sum to one')
    rows=[]
    for pair in pairs:
        checkpoint()
        case=cases[pair['event_id']];control=pair['control']
        if case['decision_at']!=pair['decision_at'] or control['trailing_candles']!=case['formation_candles']:
            raise ValueError('matched_outcomes_invalid: saved pair geometry differs')
        weight=pair['completion_moment_weight']
        if isinstance(weight,bool) or not math.isfinite(weight) or weight<=0:
            raise ValueError('matched_outcomes_invalid: nonpositive saved weight')
        outcomes={}
        for role,record,n in (('case',case,case['formation_candles']),('control',control,control['trailing_candles'])):
            at=utc(record['decision_at']);anchor=by_close.get(at)
            if anchor is None or anchor.known_at!=at:
                raise ValueError('matched_outcomes_invalid: saved anchor unavailable')
            start=at-timedelta(minutes=15*n)
            lows=[by_open.get(start+timedelta(minutes=15*i)) for i in range(n)]
            if not lows or any(v is None for v in lows):
                raise ValueError('matched_outcomes_invalid: saved history unavailable')
            geometry=record['boundary_geometry'];low=min(v.low for v in lows)
            if (geometry['E']!=anchor.close or geometry['L']!=low or
                    geometry['upper']!=anchor.close+(anchor.close-low) or utc(geometry['known_at'])!=at):
                raise ValueError('matched_outcomes_invalid: saved barriers changed')
            outcomes[role]=first_passage_path(anchor_at=at,known_at=anchor.known_at,
                entry=geometry['E'],low=geometry['L'],source_identity_key=anchor.source_identity_key,
                timeframe_seconds=900,by_open=by_open,evaluation_end=evaluation_end,checkpoint=checkpoint)
        rows.append({'event_id':pair['event_id'],'control_id':control['control_id'],
            'decision_at':case['decision_at'],'control_decision_at':control['decision_at'],
            'block':case['block'],'completion_moment_weight':weight,**outcomes})
    def summary(subset):
        mass=math.fsum(p['completion_moment_weight'] for p in subset)
        arms={}
        for role in ('case','control'):
            counts=Counter(p[role]['classification'] for p in subset)
            fractions={k:math.fsum(p['completion_moment_weight'] for p in subset if p[role]['classification']==k)/mass if mass else None for k in CLASSES}
            arms[role]={'class_counts':{k:counts[k] for k in CLASSES},'weighted_class_fractions':fractions,
                'unresolved_reasons':dict(Counter(p[role]['reason'] for p in subset if p[role]['classification']=='unresolved'))}
        return {'pairs':len(subset),'case_completion_moments':len({p['decision_at'] for p in subset}),
            'saved_weight_mass':mass,**arms,'case_minus_control':{k:arms['case']['weighted_class_fractions'][k]-arms['control']['weighted_class_fractions'][k] if mass else None for k in CLASSES},
            'independent_sample_size':None,'inference_claim':False}
    views={'old':[p for p in rows if p['block']<3],'new':[p for p in rows if p['block']>=3],'combined':rows}
    result={'rules':RULES,'outcomes_calculated':True,'pairs':rows,
        'matched_outcome_comparison':{name:summary(p) for name,p in views.items()},
        'by_block':{str(i):summary([p for p in rows if p['block']==i]) for i in range(6)},
        'by_case_completion_day':{day:summary([p for p in rows if p['decision_at'][:10]==day]) for day in sorted({p['decision_at'][:10] for p in rows})},
        'selection_audit':{'all':selection['selection_summary']['all'],'matched':selection['selection_summary']['matched'],
            'retained_full_population_moment_mass':selection['selection_summary']['retained_full_population_moment_mass'],
            'case_status_counts':selection['selection_summary']['case_status_counts']},
        'dependence':{name:{view:{'direct_component_count':len(dep['direct_support_and_pair_components']),
                'ancestry_aware_components':dep['ancestry_aware_components'],'independent_sample_size':None}
            for view,dep in pop['dependence'].items()} for name,pop in selection['populations'].items()},
        'sample_count':len({p['decision_at'] for p in rows}),'sample_count_meaning':'Matched completion moments; not independent trials',
        'reselection_performed':False,'observational_anchor_not_fill':True}
    result['matched_outcome_manifest_fields']=list(result)+['matched_outcome_manifest_fields']
    result['matched_outcome_manifest_hash']=stable_hash({k:result[k] for k in result['matched_outcome_manifest_fields']})
    return result
