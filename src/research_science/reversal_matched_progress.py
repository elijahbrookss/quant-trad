"""Post-outcome progress on fixed selected pairs; no new targets or selections."""
from collections import Counter
from datetime import timedelta
import math

from .recognition import utc, plain
from .reversal_outcomes import first_passage_path, HORIZON_BARS
from .study import stable_hash

SCHEMA = 'reversal_matched_progress.v1'
QUANTILES = (.10, .25, .50, .75, .90)
BINS = ('[0,25)', '[25,50)', '[50,75)', '[75,100)', '[100,infinity)')
RULES = {
    'version': SCHEMA,
    'population': 'Exact saved B3 pairs and v13 first-passage outcomes; no recognition or rematching',
    'horizon_bars': HORIZON_BARS,
    'progress_percent': '100*max(0,max(next12 high)-E)/(U-E); U=E+(E-L); uncapped',
    'full_window': 'Includes after failure; never reclassifies original first passage',
    'pre_failure_bounds': 'Lower: strictly earlier candle highs; upper: also first failure candle high; include zero anchor',
    'intrabar_order': 'Unknown; bounds rather than invented high/low ordering',
    'censoring': 'Partial path observed maximum is a lower bound, excluded from complete-window distribution with count and original mass retained',
    'quantiles': list(QUANTILES),
    'quantile_definition': 'Smallest value whose cumulative original weight reaches p times group mass; no interpolation',
    'bins_percent': list(BINS),
    'weights': 'Original pair weights, normalized only in explicitly named reporting group',
    'interpretation': 'Post-outcome descriptive only; no prediction, inference, optimized target or beyond-three-hour claim',
    'limits': {'candles': 19104, 'pairs': 999, 'wall_seconds': 120,
               'input_bytes': 268435456, 'result_bytes': 16777216},
}


def progress_path(*, saved, source_identity_key, by_open, evaluation_end, checkpoint=lambda: None):
    """Read only the original twelve-bar support, including after first passage."""
    at = utc(saved['anchor_known_at'])
    entry, low, upper = (saved[k] for k in ('anchor_price', 'failure_level', 'continuation_level'))
    if not all(math.isfinite(v) for v in (entry, low, upper)) or entry <= low or upper != entry+(entry-low):
        raise ValueError('matched_progress_invalid: saved positive symmetric geometry required')
    original = first_passage_path(anchor_at=at, known_at=at, entry=entry, low=low,
        source_identity_key=source_identity_key, timeframe_seconds=900, by_open=by_open,
        evaluation_end=evaluation_end, checkpoint=checkpoint)
    if original != saved:
        raise ValueError('matched_progress_invalid: original first passage differs from frozen path')
    peak = entry
    pre_lower = pre_upper = None
    first_failure = None
    observed = 0
    censor_reason = None
    def percent(value):
        return 100*max(0., value-entry)/(upper-entry)
    for offset in range(HORIZON_BARS):
        checkpoint()
        opened = at+timedelta(minutes=15*offset)
        if opened >= evaluation_end:
            censor_reason = 'evaluation_boundary'
            break
        bar = by_open.get(opened)
        if bar is None:
            censor_reason = 'candle_missing'
            break
        if bar.source_identity_key != source_identity_key or bar.known_at != bar.close_time:
            raise ValueError('matched_progress_invalid: source or availability changed')
        if first_failure is None and bar.low <= low:
            first_failure = offset+1
            pre_lower, pre_upper = percent(peak), percent(max(peak, bar.high))
        peak = max(peak, bar.high)
        observed += 1
    complete = observed == HORIZON_BARS
    if complete and first_failure is None:
        pre_lower = pre_upper = percent(peak)
    return {'complete_horizon': complete, 'observed_bars': observed,
        'censor_reason': censor_reason, 'observed_end': plain(at+timedelta(minutes=15*observed)),
        'observed_window_max_progress_lower_bound_percent': percent(peak),
        'full_window_max_progress_percent': percent(peak) if complete else None,
        'first_failure_bar': first_failure,
        'pre_failure_progress_lower_percent': pre_lower,
        'pre_failure_progress_upper_percent': pre_upper,
        'pre_failure_bounds_complete': complete or first_failure is not None,
        'intrabar_bounds_differ': pre_lower != pre_upper if pre_lower is not None else None}


def _mass(rows):
    return math.fsum(r['completion_moment_weight'] for r in rows)


def distribution(rows, role, field):
    """Count and mass remain visible even when an endpoint is censored."""
    available = [r for r in rows if r[role]['progress'][field] is not None]
    mass, total = _mass(available), _mass(rows)
    values = sorted((r[role]['progress'][field], r['completion_moment_weight']) for r in available)
    def quantile(p):
        prefix_weights = []
        for value, weight in values:
            prefix_weights.append(weight)
            if math.fsum(prefix_weights) >= mass*p:
                return value
        return values[-1][0] if values else None
    def bin_index(value):
        return min(int(value//25), 4)
    bins = []
    for index, label in enumerate(BINS):
        subset = [r for r in available if bin_index(r[role]['progress'][field]) == index]
        bins.append({'bin_percent': label, 'count': len(subset), 'saved_weight_mass': _mass(subset),
                     'weighted_fraction_of_available': _mass(subset)/mass if mass else None})
    return {'group_count': len(rows), 'group_saved_weight_mass': total,
        'available_count': len(available), 'available_saved_weight_mass': mass,
        'unavailable_count': len(rows)-len(available),
        'unavailable_saved_weight_mass': _mass([r for r in rows if r[role]['progress'][field] is None]),
        'quantiles_percent': {str(p): quantile(p) for p in QUANTILES}, 'bins': bins}


def describe_progress(selection, saved_outcomes, candles, *, evaluation_end, checkpoint=lambda: None):
    by_open = {c.open_time: c for c in candles}
    if len(by_open) != len(candles):
        raise ValueError('matched_progress_invalid: duplicate candle clock')
    selected = {p['event_id']: p for p in selection['pairs']}
    if len(selected) != len(selection['pairs']) or len(saved_outcomes['pairs']) != len(selected):
        raise ValueError('matched_progress_invalid: pair population differs')
    cases = {c['event_id']: c for c in selection['cases']}
    seen = set()
    rows = []
    for saved in saved_outcomes['pairs']:
        checkpoint()
        key = saved['event_id']
        if key not in selected or key in seen:
            raise ValueError('matched_progress_invalid: pair identity differs')
        seen.add(key)
        pair, case = selected[key], cases[key]
        control = pair['control']
        if (saved['completion_moment_weight'] != pair['completion_moment_weight']
                or saved['control_id'] != control['control_id'] or saved['decision_at'] != pair['decision_at']
                or saved['control_decision_at'] != control['decision_at'] or saved['block'] != case['block']):
            raise ValueError('matched_progress_invalid: pair geometry or weight differs')
        row = {k: saved[k] for k in ('event_id', 'control_id', 'decision_at', 'control_decision_at', 'block', 'completion_moment_weight')}
        for role, record in (('case', case), ('control', control)):
            geometry = record['boundary_geometry']
            path = saved[role]
            if (path['anchor_price'] != geometry['E'] or path['failure_level'] != geometry['L']
                    or path['continuation_level'] != geometry['upper'] or path['anchor_known_at'] != record['decision_at']):
                raise ValueError('matched_progress_invalid: saved barriers or anchor differ')
            row[role] = {'original_outcome': dict(path), 'progress': progress_path(saved=path,
                source_identity_key=selection['source_identity_key'], by_open=by_open,
                evaluation_end=evaluation_end, checkpoint=checkpoint)}
        rows.append(row)
    if rows and (any(not math.isfinite(r['completion_moment_weight']) or r['completion_moment_weight'] <= 0 for r in rows)
                 or not math.isclose(_mass(rows), 1., abs_tol=1e-12)):
        raise ValueError('matched_progress_invalid: original weights invalid')
    fields = ('full_window_max_progress_percent', 'pre_failure_progress_lower_percent', 'pre_failure_progress_upper_percent')
    def summarize(subset, role):
        return {'count': len(subset), 'saved_weight_mass': _mass(subset),
            'original_class_counts': dict(Counter(r[role]['original_outcome']['classification'] for r in subset)),
            'distributions': {f: distribution(subset, role, f) for f in fields},
            'censored_rows': [{**{k: r[k] for k in ('event_id', 'control_id', 'completion_moment_weight')},
                'original_outcome': r[role]['original_outcome'], 'progress': r[role]['progress']}
                for r in subset if not r[role]['progress']['complete_horizon']]}
    views = {}
    for name, subset in (('old', [r for r in rows if r['block'] < 3]),
                         ('new', [r for r in rows if r['block'] >= 3]), ('combined', rows)):
        views[name] = {role: {'all': summarize(subset, role), 'originally_neither':
            summarize([r for r in subset if r[role]['original_outcome']['classification'] == 'neither'], role)}
            for role in ('case', 'control')}
    result = {'rules': RULES, 'pairs': rows, 'progress_comparison': views,
        'sample_count': len({r['decision_at'] for r in rows}),
        'sample_count_meaning': 'Matched completion clocks; not independent trials',
        'original_outcomes_preserved': True, 'reselection_performed': False,
        'post_outcome_description': True, 'inference_claim': False,
        'dependence': saved_outcomes['dependence']}
    result['progress_manifest_fields'] = list(result)+['progress_manifest_fields']
    result['progress_manifest_hash'] = stable_hash({k: result[k] for k in result['progress_manifest_fields']})
    return result
