"""Bounded retrospective event-aligned liquidity sequences with explicit clocks."""
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import timedelta
import math
from statistics import median

from core.execution_control import execution_checkpoint
from market_data.frozen import semantic_hash
from .event_facts import (_causal_fact_records, _record_effective_at,
    _record_known_at, _record_payload_document, _record_event_key, _fact_gap_intersects)
from .event_time import _utc, _iso

SCHEMA = "liquidity_episode.v1"
RESULT_SCHEMA = "liquidity_episode_result.v1"
RULES = {"move_horizon_seconds": 60, "sharp_log_return_bps": 10.,
    "depth_band_bps": 5, "thinning_fraction": .5,
    "pre_seconds": 120, "post_seconds": 120, "refractory_seconds": 300,
    "max_episodes_per_origin": 10, "max_scope_seconds": 1800,
    "max_records_per_alias": 50000, "matching": "earlier_same_hour_no_replacement.v1",
    "match_calipers": {"spread_ratio": 2., "depth_ratio": 2., "volatility_ratio": 2.},
    "resolution_seconds": 1, "revision_selection": "latest_at_declared_analysis_cutoff.v1",
    "control_admission": "precontext_only_before_future_support.v1",
    "dependence": "unique_anchor_closed_window_components.v1",
    "missing_second_policy": "exclude_without_fill", "inference": "retrospective_descriptive_only"}
EXPECTED = {"bbo": ("market.bbo", "market.bbo.v1", 1),
    "depth": ("market.depth_observation", "market.depth_band.v1", 1),
    "flow": ("market.trade_flow", "market.trade_flow.v1", 1),
    "trades": ("market.trade", "market.trade.v1", None)}


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    if detector != {"type": "liquidity_episode"} or set(outcomes) != {"schema_version", "analysis_as_of"} or outcomes.get("schema_version") != SCHEMA:
        raise ValueError("liquidity_episode_invalid: explicit fixed procedure and real analysis cutoff required")
    start,end = (_utc(scope[k],field=k) for k in ("start","end"))
    cutoff = _utc(outcomes["analysis_as_of"],field="analysis_as_of")
    if not timedelta(seconds=300) <= end-start <= timedelta(seconds=RULES["max_scope_seconds"]) or cutoff < end:
        raise ValueError("liquidity_episode_invalid: 5..30 minute scope and cutoff >=end required")
    if scope.get("timeframe") != "1m" or any(t.timestamp()%60 for t in (start,end)):
        raise ValueError("liquidity_episode_invalid: aligned minute source scope required")
    if statistics or scope.get("indicator_id") or scope.get("indicator_param_overrides") or scope.get("warmup_bars",0) or gap_policy != "continue_degraded":
        raise ValueError("liquidity_episode_invalid: no Indicator, warmup, model or configurable statistics")
    if len(inputs)!=4 or {r['alias'] for r in inputs}!=set(EXPECTED):
        raise ValueError("liquidity_episode_invalid: exact bbo/depth/flow/trades aliases required")
    for r in inputs:
        if (r['fact_type'],r['contract_version'],r.get('timeframe_seconds'))!=EXPECTED[r['alias']]:
            raise ValueError("liquidity_episode_invalid: canonical input contract mismatch")
        if r.get('alignment') != ('latest_known' if r['alias']=='trades' else 'exact_interval'):
            raise ValueError("liquidity_episode_invalid: bucketed inputs require exact interval alignment")
        if r.get('source_policy',{}).get('mode')!='exact' or r.get('instrument_id') or r.get('known_at_required') is False:
            raise ValueError("liquidity_episode_invalid: exact primary-instrument sources and retained clocks required")
    return {"type":"liquidity_episode"}, {"schema_version":SCHEMA,"analysis_as_of":_iso(cutoff)}, {}


def number(p,key,positive=False):
    value=p.get(key)
    if value is None or isinstance(value,bool):raise ValueError("liquidity_episode_invalid: unavailable numeric "+key)
    value=float(value)
    if not math.isfinite(value) or (positive and value<=0):raise ValueError("liquidity_episode_invalid: invalid numeric "+key)
    return value


def build_rows(inputs,cutoff):
    by_alias={}
    for alias in EXPECTED:
        records=inputs.get('fact_records_by_alias',{}).get(alias,())
        if len(records)>RULES['max_records_per_alias']:
            raise ValueError('liquidity_episode_limit: alias exceeds 50000 records')
        selected=_causal_fact_records(records,evaluation_time=cutoff)
        by_alias[alias]=selected
    buckets={a:{} for a in ('bbo','depth','flow')};trades=[]
    for alias,records in by_alias.items():
        for r in records:
            execution_checkpoint()
            p=_record_payload_document(r)['payload'];t=_record_effective_at(r)
            if alias=='depth' and p.get('band_bps')!=5:continue
            row={'time':t,'known_at':_record_known_at(r),'record_id':str(r.fact_version_id),
                'event_key':_record_event_key(r),'payload':p,'record':r}
            row['identity']={'series_id':r.series_id,'revision':r.revision,
                'market_commit_seq':r.market_commit_seq,'row_hash':r.row_hash,
                'source_identity_key':r.source_identity_key}
            row['clocks']={k:(_iso(getattr(r.fact,k)) if getattr(r.fact,k,None) is not None else None)
                for k in ('source_published_at','received_at','accepted_at')}
            if alias=='trades':trades.append(row);continue
            if t in buckets[alias]:raise ValueError('liquidity_episode_invalid: ambiguous bucket after revision selection')
            if t.timestamp()%1 or _utc(p['bucket_end'],field='bucket_end')!=t+timedelta(seconds=1):
                raise ValueError('liquidity_episode_invalid: malformed second bucket')
            buckets[alias][t]=row
    raw_by_second=defaultdict(list)
    for r in trades:raw_by_second[r['time'].replace(microsecond=0)].append(r)
    joined={};reasons=Counter()
    for t,b in sorted(buckets['bbo'].items()):
        gaps=inputs.get('data_quality',{}).get('recorded_gaps',())
        if any(_fact_gap_intersects(gaps,alias=a,start=t,end=t+timedelta(seconds=1)) for a in EXPECTED):
            reasons['recorded_source_gap']+=1;continue
        d=buckets['depth'].get(t);f=buckets['flow'].get(t)
        if d is None or f is None:reasons['depth_or_flow_bucket_missing']+=1;continue
        bp,dp,fp=b['payload'],d['payload'],f['payload']
        if (bp['source_state_hash']!=dp['source_state_hash'] or bp['validity_interval_id']!=dp['validity_interval_id']
                or dp['bbo_input_fingerprint']!=bp['input_fingerprint']):
            reasons['book_state_join_mismatch']+=1;continue
        q=f['record'].fact.quality
        quality=q.get('_qt_trade_flow_quality',{})
        if not all(quality.get(k) is True for k in ('archive_complete','aggregate_complete','canonicalization_complete')) or quality.get('late_trade_count')!=0:
            reasons['trade_flow_coverage_unproved']+=1;continue
        raw=raw_by_second[t]
        if (len(raw)!=number(fp,'trade_count') or any(r['payload'].get('aggressor_side') not in ('BUY','SELL') for r in raw)
            or any(not math.isclose(math.fsum(number(r['payload'],'base_quantity') for r in raw if r['payload']['aggressor_side']==side),
                number(fp,key),rel_tol=1e-9,abs_tol=1e-12) for side,key in [('BUY','aggressor_buy_base_volume'),('SELL','aggressor_sell_base_volume')])):
            reasons['raw_trade_flow_reconciliation_failed']+=1;continue
        mid=number(bp,'mid_price',True);spread=number(bp,'spread_bps',True)
        bid=number(dp,'bid_base_quantity');ask=number(dp,'ask_base_quantity')
        if bid<0 or ask<0 or bid+ask<=0:reasons['depth_numeric_unavailable']+=1;continue
        joined[t]={'time':t,'mid':mid,'spread_bps':spread,'bid_depth':bid,'ask_depth':ask,
            'buy_base_volume':number(fp,'aggressor_buy_base_volume'),
            'sell_base_volume':number(fp,'aggressor_sell_base_volume'),
            'trade_count':number(fp,'trade_count'),'validity_interval_id':bp['validity_interval_id'],
            'references':{a:{'record_id':row['record_id'],'known_at':_iso(row['known_at']),**row['identity'],**row['clocks']}
                for a,row in [('bbo',b),('depth',d),('flow',f)]}}
    return joined,sorted(trades,key=lambda r:(r['time'],r['event_key'])),reasons


def supported(joined,t,lower=-120,upper=120):
    # Exact materialized seconds only. Producer quiet proof never manufactures rows.
    window=[joined.get(t+timedelta(seconds=i)) for i in range(lower,upper+1)]
    return (window if all(r is not None for r in window) and
        len({r['validity_interval_id'] for r in window})==1 else None)


def context(joined,t):
    # All matching data end at t-60, before the move's observation interval.
    rows=[joined[t+timedelta(seconds=i)] for i in range(-120,-59)]
    returns=[math.log(b['mid']/a['mid']) for a,b in zip(rows,rows[1:])]
    return {'spread':median(r['spread_bps'] for r in rows),
        'depth':median(r['bid_depth']+r['ask_depth'] for r in rows),
        'volatility':math.hypot(*returns)/math.sqrt(len(returns)),
        'prior_return':math.log(rows[-1]['mid']/rows[0]['mid'])}


def match_distance(a,b):
    if (a['prior_return']>0)!=(b['prior_return']>0):return None
    distances=[]
    for name in ('spread','depth','volatility'):
        x,y=a[name],b[name]
        if x==0 or y==0:
            if x!=y:return None
            distances.append(0.);continue
        ratio=max(x/y,y/x)
        if ratio>2:return None
        distances.append(abs(math.log(x/y)))
    return math.fsum(distances)


def episode_dependence(episodes):
    """Count shared identities and inclusive planned-window overlap, not trials."""
    events=defaultdict(list);controls=defaultdict(list);windows={}
    for case in episodes:
        for role,members in (('event',events),('control',controls)):
            episode=case.get(role)
            if episode is None:continue
            anchor=episode['anchor'];members[anchor].append(case['origin'])
            window=windows.setdefault(anchor,{'anchor':anchor,'roles':set(),'case_ids':set()})
            window['roles'].add(role);window['case_ids'].add(case['event_id'])
    ordered=sorted(windows)
    components=[];pairs=[];right=None
    for anchor in ordered:
        execution_checkpoint()
        instant=_utc(anchor,field='anchor');left=instant-timedelta(seconds=120)
        end=instant+timedelta(seconds=120)
        if right is None or left>right:components.append([])
        right=max(right or end,end)
        row=windows[anchor];row['roles']=sorted(row['roles']);row['case_ids']=sorted(row['case_ids'])
        row['component_id']=len(components);components[-1].append(anchor)
    for i,a in enumerate(ordered):
        for b in ordered[i+1:]:
            if (_utc(b,field='anchor')-_utc(a,field='anchor')).total_seconds()<=240:
                pairs.append({'left_anchor':a,'right_anchor':b})
    shared_events=[a for a,v in sorted(events.items()) if len(set(v))>1]
    shared_controls=[a for a,v in sorted(controls.items()) if len(set(v))>1]
    return {'episode_rows':len(episodes),'unique_event_anchors':len(events),
        'event_anchors_shared_across_origins':len(shared_events),'shared_event_anchors':shared_events,
        'selected_control_occurrences':sum(len(v) for v in controls.values()),
        'unique_control_anchors':len(controls),
        'control_anchors_reused_across_origins':len(shared_controls),'reused_control_anchors':shared_controls,
        'additional_control_occurrences':sum(len(v)-1 for v in controls.values()),
        'anchors_used_as_event_and_control':sorted(set(events)&set(controls)),
        'unique_planned_windows':len(windows),'overlap_pair_count':len(pairs),'overlap_pairs':pairs,
        'overlap_component_count':len(components),'overlap_components':components,
        'windows':[windows[a] for a in ordered],
        'interpretation':'Unique inclusive +/-120s planned windows, coalesced across roles/origins even when support is missing. These counts are not independent observations or an effective sample size.'}


def analyze(joined,trades,*,start,end):
    reasons=Counter();eligible=[];origin={'sharp_move':[],'depth_thinning':[]};last={}
    # The structural grid is fixed by declared inputs, not available future rows.
    grid=[start+timedelta(seconds=i) for i in range(120,int((end-start).total_seconds())-120)]
    precontexts={t:context(joined,t) for t in grid if supported(joined,t,-120,-60) is not None}
    for t in grid:
        execution_checkpoint()
        w=supported(joined,t)
        if w is None:reasons['episode_path_unavailable']+=1;continue
        eligible.append(t)
        move=math.log(joined[t]['mid']/joined[t-timedelta(seconds=60)]['mid'])*10000
        previous_move=math.log(joined[t-timedelta(seconds=1)]['mid']/joined[t-timedelta(seconds=61)]['mid'])*10000
        prior=[joined[t-timedelta(seconds=i)] for i in range(60,0,-1)]
        thin=[side for side in ('bid_depth','ask_depth') if joined[t][side]<=.5*median(r[side] for r in prior)]
        previous_prior=[joined[t-timedelta(seconds=i)] for i in range(61,1,-1)]
        newly_thin=[side for side in thin if joined[t-timedelta(seconds=1)][side]>.5*median(r[side] for r in previous_prior)]
        for name,hit in [('sharp_move',abs(move)>=10 and abs(previous_move)<10),('depth_thinning',bool(newly_thin))]:
            if not hit or len(origin[name])>=10 or (name in last and (t-last[name]).total_seconds()<300):continue
            origin[name].append({'time':t,'move_bps':move,'thinning_sides':thin});last[name]=t
    trade_times=[r['time'] for r in trades]
    def episode(t):
        path=[joined.get(t+timedelta(seconds=i)) for i in range(-120,121)]
        missing=[i-120 for i,r in enumerate(path) if r is None]
        intervals={r['validity_interval_id'] for r in path if r is not None}
        support_reasons=(['materialized_seconds_missing'] if missing else [])+(['book_validity_interval_changed'] if len(intervals)>1 else [])
        w=[r for r in path if r is not None]
        sample=[{'offset_seconds':int((r['time']-t).total_seconds()),
            **{k:v for k,v in r.items() if k not in ('time','validity_interval_id')},
            'bucket_start':_iso(r['time'])} for r in w]
        lo,hi=t-timedelta(seconds=120),t+timedelta(seconds=121)
        selected=trades[bisect_left(trade_times,lo):bisect_left(trade_times,hi)]
        trace=[]
        for r in selected:
            p=r['payload']
            trace.append({'event_time':_iso(r['time']),'known_at':_iso(r['known_at']),
                **r['clocks'],
                **r['identity'],
                'record_id':r['record_id'],'event_key':r['event_key'],
                'aggressor_side':p['aggressor_side'],'maker_side':p['maker_side'],
                'price':number(p,'price',True),'base_quantity':number(p,'base_quantity')})
        future=supported(joined,t,0,60)
        return {'anchor':_iso(t),'context':precontexts[t],'seconds':sample,'trades':trace,
            'path_support_status':'unresolved' if support_reasons else 'complete',
            'path_support_reasons':support_reasons,'missing_offsets_seconds':missing,
            'forward_60s_status':'resolved' if future is not None else 'unresolved',
            'sharp_move_in_next_60s':any(abs(math.log(r['mid']/future[0]['mid']))*10000>=10 for r in future[1:]) if future is not None else None}
    episodes=[]
    for name,anchors in origin.items():
        used=set()
        for anchor in anchors:
            t=anchor['time'];ctx=precontexts[t];candidates=[]
            for c in precontexts:
                if c in used or c.hour!=t.hour or c.date()!=t.date() or c>t-timedelta(seconds=300):continue
                # Earlier anchor need not have a quiet future; matching is outcome-blind.
                score=match_distance(ctx,precontexts[c])
                if score is not None:candidates.append((score,c))
            control=min(candidates)[1] if candidates else None
            if control is not None:used.add(control)
            episodes.append({'origin':name,'event_id':semantic_hash({'origin':name,'time':_iso(t)}),
                'move_bps':anchor['move_bps'],'thinning_sides':anchor['thinning_sides'],
                'event':episode(t),'control':episode(control) if control is not None else None,
                'match_status':'matched' if control is not None else 'no_precontext_match'})
    return episodes,reasons,len(eligible),len(precontexts)


@dataclass(frozen=True)
class LiquidityEpisodeEvaluator:
    evaluator_id: str = 'event_fact_analysis'
    version: str = '14'
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self,*,definition,request):
        return {'input_kind':'market_data','indicator_ids':[],'warmup_floor_bars':0,
            'feature_lookback_bars':0,'feature_windows_seconds_by_alias':{},
            'outcome_horizons':[],'required_outcome_horizons':[],'horizon_kind':'elapsed_time',
            'entry_lag_bars':0,'invalidation_max_bars':0,'outcome_boundary':'evaluation_end_exclusive',
            'fact_history_required':True,'fact_history_boundary':'evaluation_range','primary_candle_required_fields':['close','known_at']}

    def evaluate(self,*,plan,inputs):
        cutoff=_utc(inputs['outcomes']['analysis_as_of'],field='analysis_as_of')
        joined,trades,join_reasons=build_rows(inputs,cutoff)
        # Restrict analysis to exact declared scope, even if an adapter supplies a suffix.
        start,end=(_utc(plan.evaluation_range[k],field=k) for k in ('start','end_exclusive'))
        joined={t:r for t,r in joined.items() if start<=t<end}
        episodes,reasons,eligible,control_candidates=analyze(joined,trades,start=start,end=end)
        return {'schema_version':RESULT_SCHEMA,'check_family':self.evaluator_id,'status':'completed',
            'sample_count':len(episodes),'rules':RULES,'analysis_as_of':_iso(cutoff),
            'evidence_class':'retrospective_event_sequences','historical_decision':False,
            'support':{'joined_seconds':len(joined),'eligible_anchors':eligible,
                'join_exclusions':dict(join_reasons),'episode_exclusions':dict(reasons),
                'by_origin':dict(Counter(e['origin'] for e in episodes)),
                'precontext_control_candidates':control_candidates,
                'matched':sum(e['control'] is not None for e in episodes),
                'selected_control_path_status':dict(Counter(e['control']['path_support_status'] for e in episodes if e['control'] is not None)),
                'selected_control_forward_status':dict(Counter(e['control']['forward_60s_status'] for e in episodes if e['control'] is not None))},
            'dependence':episode_dependence(episodes),
            'episodes':episodes,'data_quality':inputs.get('data_quality',{}),
            'caveats':['Event anchors require complete paths. Controls are selected from pre-context only; missing later support/outcomes remain explicit without replacement. Quiet proof does not create observations.',
                'Depth bands follow the current midpoint. Thinning is not cancellation attribution.',
                'Feed clocks may disagree; sequence numbers across feeds do not establish order. Within-second ordering remains unresolved.',
                'Each reference retains actual known_at. Event-time sequences are not historical decisions or tradable fills.',
                'Matching uses pre-move context only; control futures are unrestricted. Exposed-window descriptive pilot, no causal or independent-trial claim.']}
