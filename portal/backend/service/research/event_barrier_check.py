"""Bounded composition for the existing Indicator-event Check, with no detector branch."""
from copy import deepcopy
from datetime import timedelta
import json
import logging
import math
from time import monotonic

from core.execution_control import execution_checkpoint
from market_data.frozen import semantic_hash
from research_science.recognition import RecognitionCandle,utc,plain
from research_science.event_barriers import measure_events,SCHEMA as RESULT_SCHEMA
from .event_configuration import normalize_event_fact_configuration
from .event_preparation import EventCandlePreparation,select_public_events
from .saved_selection_binding import native_candle_series

SCHEMA='indicator_event_barriers.v1'
RULES={'version':SCHEMA,'event_ownership':'indicator','direction':'long',
    'input':'one exact complete native historical15m frozen path;explicit origin;no gap bridging or extra tail',
    'path':'inclusive high/low;first barrier touch;both first touched samebar unresolved;original label retained',
    'later_target':'original failure only;strictly later bar within original horizon;positive/negative/unknown with original weights',
    'weights':'once per ALL eligible event clocks,split all chains;never reweight labels or observed-feature subsets',
    'summary':'all four labels;original/observed/missing mass;first-CDF crossing;weighted descriptive mean differences and rank dominance',
    'limits':{'input_bars':19104,'events':10000,'features':32,'horizon_bars':12,'wall_seconds':120,'result_bytes':16777216},
    'inference':False,'trading_authority':False}
logger=logging.getLogger(__name__)


def invalid(reason):raise ValueError('event_barriers_invalid: '+reason)


def metadata_path(value):
    path=str(value or '').strip();parts=path.split('.')
    if (not 2<=len(parts)<=9 or parts[0]!='metadata'
            or any(not part or len(part)>128 for part in parts)):
        invalid('bounded public metadata path required')
    return path


def normalize_configuration(*,detector,outcomes,statistics,scope,inputs=(),gap_policy='reject'):
    if inputs or gap_policy!='reject':invalid('candle-only frozen input and reject gap policy required')
    detector_keys={'type','output_name','event_keys'}
    if set(detector)!=detector_keys or detector['type']!='indicator_event':invalid('existing indicator_event detector required')
    d,_,_=normalize_event_fact_configuration(detector=detector,outcomes={'horizons':[1]},statistics={})
    if any(k['direction']!='long'for k in d['event_keys']):invalid('v1 supports long events only')
    allowed={'schema_version','input_origin','horizon_bars','reference','failure','target','window_start_path',
             'identity_path','source_path','known_at_path','support_lookback_bars'}
    if set(outcomes)-allowed or outcomes.get('schema_version')!=SCHEMA:invalid('explicit registered barrier schema/fields required')
    origin=utc(outcomes.get('input_origin'));start,end=utc(scope['start']),utc(scope['end'])
    if (scope.get('timeframe')!='15m' or not origin<=start<end
        or any(t.timestamp()%900 for t in (origin,start,end))or(end-origin).total_seconds()/900>19104):invalid('aligned explicit native15m origin/window within19104bars required')
    if not scope.get('indicator_id'):invalid('existing Indicator instance ID required')
    horizon=outcomes.get('horizon_bars',12);lookback=outcomes.get('support_lookback_bars',0)
    if type(horizon)is not int or not 1<=horizon<=12 or type(lookback)is not int or not 0<=lookback<=19104:invalid('bounded integer horizon/support lookback required')
    result={'schema_version':SCHEMA,'input_origin':plain(origin),'horizon_bars':horizon,'support_lookback_bars':lookback}
    for name,operators in [('reference',{'event_close','event_metadata_number'}),('failure',{'window_low','event_metadata_number'}),('target',{'risk_multiple','event_metadata_number'})]:
        value=outcomes.get(name)
        if not isinstance(value,dict)or value.get('operator')not in operators:invalid('registered '+name+' operator required')
        operator=value['operator'];keys={'operator'}
        if operator=='event_metadata_number':keys.add('path');value={**value,'path':metadata_path(value.get('path'))}
        if operator=='risk_multiple':
            keys.add('multiple');v=value.get('multiple')
            if isinstance(v,bool)or not isinstance(v,(int,float))or not math.isfinite(v)or v<=0:invalid('positive finite risk multiple required')
        if set(value)!=keys:invalid('exact '+name+' fields required')
        result[name]=dict(value)
    if result['failure']['operator']=='window_low'and not outcomes.get('window_start_path'):invalid('window_low requires public window_start_path')
    for key in ('window_start_path','identity_path','source_path','known_at_path'):
        if outcomes.get(key) is not None:result[key]=metadata_path(outcomes[key])
    if not isinstance(statistics,dict)or set(statistics)-{'features','illustrations'}:invalid('bounded descriptive features/illustrations only')
    raw=statistics.get('features')
    if not isinstance(raw,list)or not 1<=len(raw)<=32:invalid('one through32 explicit feature projections required')
    features=[];names=set()
    for spec in raw:
        if not isinstance(spec,dict):invalid('feature object required')
        name=spec.get('name');operator=spec.get('operator');keys={'name','operator','units'}
        if not isinstance(name,str)or not name.strip()or name in names or not isinstance(spec.get('units'),str)or not spec['units']:invalid('unique named feature with explicit units required')
        value=dict(spec)
        if operator=='event_metadata_number':keys.add('path');value['path']=metadata_path(spec.get('path'))
        elif operator!='window_duration_hours':invalid('only public numeric metadata or reference-window duration supported')
        elif not result.get('window_start_path'):invalid('window duration requires explicit window_start_path')
        if 'missing_reason_path'in spec:keys.add('missing_reason_path');value['missing_reason_path']=metadata_path(spec['missing_reason_path'])
        if set(spec)!=keys:invalid('exact feature fields required')
        names.add(name);features.append(value)
    illustrations=statistics.get('illustrations',[])
    if not isinstance(illustrations,list)or len(illustrations)>3:invalid('at most3explicit illustrations')
    for spec in illustrations:
        if (not isinstance(spec,dict)or set(spec)!={'classification','feature','order'}or spec['classification']not in ('continuation','failure','neither','unresolved')or spec['feature']not in names or spec['order']not in ('min','max')):invalid('exact bounded illustration rule required')
    return d,result,{'features':features,'illustrations':deepcopy(illustrations)}


def requirements(request):
    d,o,s=normalize_configuration(detector=request.parameters['detector'],outcomes=request.parameters['outcomes'],
        statistics=request.parameters['statistics'],scope=request.scope,inputs=request.parameters.get('inputs',[]),gap_policy=request.parameters['gap_policy'])
    if request.mode!='evidence'or not request.dataset_id:invalid('explicit frozen evidence Dataset required')
    return {'input_kind':'market_data','indicator_ids':[str(request.scope['indicator_id'])],
        'event_source':'indicator','warmup_floor_bars':int((utc(request.scope['start'])-utc(o['input_origin'])).total_seconds()/900),
        'outcome_horizons':[o['horizon_bars']],'required_outcome_horizons':[o['horizon_bars']],
        'outcome_boundary':'evaluation_end_exclusive','horizon_kind':'bars','entry_lag_bars':0,
        'feature_lookback_bars':0,'fact_history_required':False}


def evaluate(*,plan,inputs):
    began=monotonic()
    def checkpoint():
        execution_checkpoint()
        if monotonic()-began>=120:raise ValueError('event_barriers_limit:120second cooperative evaluator budget exceeded')
    if inputs.get('indicator_gap_rejection'):invalid('Indicator rejected input discontinuity')
    if not plan.indicator_graph or plan.gap_policy!='reject' or plan.outcome_tail['seconds']!=0:invalid('typed Indicator graph,reject gaps,no tail required')
    declared=list(plan.execution.get('indicator_ids')or[])
    if len(declared)!=1:invalid('one declared root Indicator required')
    scope={'start':plan.evaluation_range['start'],'end':plan.evaluation_range['end_exclusive'],'timeframe':'15m','indicator_id':declared[0]}
    d,o,s=normalize_configuration(detector=inputs['detector'],outcomes=inputs['outcomes'],statistics=inputs['statistics'],scope=scope,gap_policy=plan.gap_policy)
    origin,end=utc(o['input_origin']),utc(scope['end'])
    if int(plan.warmup['timeframe_seconds'])!=900 or plain(utc(plan.materialization_range['start']))!=o['input_origin']or utc(plan.materialization_range['end_exclusive'])!=utc(scope['end']):invalid('actual input origin/end differs from declared window')
    binding=inputs.get('frozen_input_binding')
    if not isinstance(binding,dict):invalid('actual frozen binding required')
    primary=[row for row in binding['series']if row['alias']=='primary_bars']
    if len(primary)!=1:invalid('one primary frozen candle series required')
    primary,source_key=native_candle_series({**binding,'series':primary},dataset_id=binding['dataset_id'])
    loaded=dict(primary.get('loaded_range')or{})
    if (utc(primary.get('range_start'))!=origin or utc(primary.get('range_end'))!=end
            or utc(loaded.get('start'))!=origin or utc(loaded.get('end_exclusive'))!=end):
        invalid('actual frozen primary range differs from declared origin/end')
    evidence=inputs.get('indicator_evidence')or{}
    rows,_=EventCandlePreparation().candles(evidence.get('candles'),field='indicator_evidence.candles')
    if not 1<=len(rows)<=19104:invalid('bounded nonempty input required')
    candles=[]
    for row in rows:
        checkpoint();material=dict(row);material['open_time']=material.get('open_time')or material.get('time')
        candles.append(RecognitionCandle.from_mapping(material,source_identity_key=source_key))
    if (len(candles)!=(end-origin).total_seconds()/900 or primary['row_count']!=len(candles)
        or any(c.open_time!=origin+i*timedelta(minutes=15)or c.close_time!=c.open_time+timedelta(minutes=15)or c.known_at!=c.close_time for i,c in enumerate(candles))):invalid('complete exact native clock required;no gap,tail or reset')
    _,_,_,events,_,_=select_public_events(detector=d,detector_type='indicator_event',indicator_evidence=evidence,
        candles=rows,baseline_specs=[],enriched_specs=[],fact_records={},fact_requirements={},fact_gap_evidence=[],evaluation_start=utc(scope['start']),evaluation_end=end)
    if any(row.get('indicator_id')!=declared[0]for row in events):invalid('public event Indicator identity differs')
    logger.info('event_barrier_analysis_started | indicator_id=%s input_rows=%s selected_public_events=%s',declared[0],len(candles),len(events))
    result=measure_events(events,tuple(candles),config={**o,'illustrations':s['illustrations']},features=s['features'],
        start=scope['start'],end=scope['end'],source_key=source_key,checkpoint=checkpoint)
    result.update(check_family='event_fact_analysis',analysis_status='descriptive_indicator_event_barriers',
        rules=RULES,configuration={'detector':d,'outcomes':o,'statistics':s},input_origin=o['input_origin'],
        evaluation_range=dict(plan.evaluation_range),source_identity_key=source_key,
        indicator_graph_hash=semantic_hash({'indicators':[dict(row)for row in plan.indicator_graph]}),
        indicator_graph=[dict(row)for row in plan.indicator_graph],
        frozen_input_binding_hash=semantic_hash(binding),frozen_primary=primary,
        dataset_id=binding['dataset_id'],dataset_hash=binding['dataset_hash'],max_commit_seq=binding['max_commit_seq'],
        data_quality=dict(inputs.get('data_quality')or{}),caveats=['Exploratory descriptive distributions,not adjusted prediction or trading evidence.',
            'All simultaneous chains,missing features and unresolved outcomes retain original clock mass.',
            'Later target touch does not change the original failure label or extend its original horizon.'])
    checkpoint()
    if len(json.dumps(result,allow_nan=False,separators=(',',':')).encode())>16777216:raise ValueError('event_barriers_limit: result exceeds16MiB;no truncation')
    logger.info('event_barrier_analysis_completed | indicator_id=%s events=%s clocks=%s',declared[0],len(result['cases']),result['sample_count'])
    return result
