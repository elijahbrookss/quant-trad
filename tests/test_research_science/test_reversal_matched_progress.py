"""Synthetic fixed-pair path tests; no historical market-data execution."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
import pytest

from research_science.recognition import RecognitionCandle, utc
from research_science.reversal_outcomes import first_passage_path
from research_science.reversal_matched_progress import progress_path, describe_progress, distribution
from research_science.reversal_matched_outcomes import describe_selected_pairs
from research_science.hashing import stable_hash
from tests.test_research_science.test_reversal_matched_outcomes import fixture


def path(highs, lows, *, end_bars=12):
    at = utc('2025-02-01T00:00:00Z')
    bars = {at+timedelta(minutes=15*i): RecognitionCandle(str(i), 's',
        at+timedelta(minutes=15*i), at+timedelta(minutes=15*(i+1)), at+timedelta(minutes=15*(i+1)),
        100., high, low, 100.) for i, (high, low) in enumerate(zip(highs, lows))}
    end = at+timedelta(minutes=15*end_bars)
    saved = first_passage_path(anchor_at=at, known_at=at, entry=100., low=99.,
        source_identity_key='s', timeframe_seconds=900, by_open=bars, evaluation_end=end)
    return saved, progress_path(saved=saved, source_identity_key='s', by_open=bars, evaluation_end=end)


def test_later_rebound_is_full_window_progress_not_reclassified_continuation():
    saved, p = path([100.2, 100.8, 103.]+[100.1]*9, [99.5, 99., 99.5]+[99.5]*9)
    assert saved['classification'] == 'failure'
    assert p['full_window_max_progress_percent'] == 300.
    assert p['first_failure_bar'] == 2
    assert p['pre_failure_progress_lower_percent'] == pytest.approx(20.)
    assert p['pre_failure_progress_upper_percent'] == pytest.approx(80.)
    assert p['intrabar_bounds_differ']


def test_both_touch_candle_preserves_ambiguity_and_uncapped_excursion():
    saved, p = path([102.]+[100.5]*11, [98.]+[99.5]*11)
    assert saved['classification'] == 'unresolved' and saved['reason'] == 'intrabar_order_unknown'
    assert p['full_window_max_progress_percent'] == 200.
    assert p['pre_failure_progress_lower_percent'] == 0.
    assert p['pre_failure_progress_upper_percent'] == 200.


def test_complete_neither_bounds_coincide_and_partial_control_is_censored():
    saved, p = path([100.75]*12, [99.5]*12)
    assert saved['classification'] == 'neither'
    assert p['full_window_max_progress_percent'] == p['pre_failure_progress_lower_percent'] == p['pre_failure_progress_upper_percent'] == 75.
    saved, p = path([100.75]*5, [99.5]*5, end_bars=5)
    assert saved['classification'] == 'unresolved' and p['observed_bars'] == 5
    assert not p['complete_horizon'] and p['censor_reason'] == 'evaluation_boundary'
    assert p['full_window_max_progress_percent'] is None
    assert p['pre_failure_progress_lower_percent'] is None
    assert p['observed_window_max_progress_lower_bound_percent'] == 75.


def test_failure_before_censor_still_has_complete_pre_failure_bounds():
    saved, p = path([100.5]*5, [99.]*5, end_bars=5)
    assert saved['classification'] == 'failure'
    assert p['pre_failure_bounds_complete'] and p['full_window_max_progress_percent'] is None
    assert p['pre_failure_progress_lower_percent'] == 0.
    assert p['pre_failure_progress_upper_percent'] == 50.


def synthetic():
    selection, bars, end = fixture()
    selection['source_identity_key'] = 'source'
    saved = describe_selected_pairs(selection, bars, evaluation_end=end)
    return selection, bars, end, saved


def test_exact_pairs_weights_classes_group_mass_and_json_manifest_preserved():
    selection, bars, end, saved = synthetic()
    before = deepcopy((selection, saved))
    result = describe_progress(selection, saved, bars, evaluation_end=end)
    assert (selection, saved) == before
    for row, old in zip(result['pairs'], saved['pairs']):
        assert row['completion_moment_weight'] == old['completion_moment_weight']
        for role in ('case', 'control'):
            assert row[role]['original_outcome'] == old[role]
    control = result['progress_comparison']['combined']['control']['all']
    dist = control['distributions']['full_window_max_progress_percent']
    assert dist['group_count'] == 3 and dist['group_saved_weight_mass'] == 1.
    assert dist['available_count'] == 2 and dist['available_saved_weight_mass'] == .5
    assert dist['unavailable_count'] == 1 and dist['unavailable_saved_weight_mass'] == .5
    assert len(control['censored_rows']) == 1
    assert sum(b['count'] for b in dist['bins']) == 2
    assert sum(b['saved_weight_mass'] for b in dist['bins']) == .5
    assert result['progress_comparison']['old']['case']['all']['saved_weight_mass'] == .5
    serialized = json.loads(json.dumps(result))
    assert stable_hash({k: serialized[k] for k in serialized['progress_manifest_fields']}) == serialized['progress_manifest_hash']


def test_fixed_quarter_edges_weighted_quantiles_and_missing_mass():
    values = [0., 25., 50., 75., 100., 250., None]
    weights = [.01, .01, .01, .01, .01, .45, .5]
    rows = [{'completion_moment_weight': w, 'case': {'progress': {'x': x}}} for x, w in zip(values, weights)]
    result = distribution(rows, 'case', 'x')
    assert [b['count'] for b in result['bins']] == [1, 1, 1, 1, 2]
    assert result['quantiles_percent']['0.5'] == 250.
    assert result['available_saved_weight_mass'] == .5
    assert result['unavailable_saved_weight_mass'] == .5


@pytest.mark.parametrize('mutation', [
    lambda s: s['pairs'][0].update(completion_moment_weight=.3),
    lambda s: s['pairs'][0].update(control_id='other'),
    lambda s: s['pairs'][0]['case'].update(continuation_level=102.),
    lambda s: s['pairs'][0]['case'].update(classification='failure'),
    lambda s: s['pairs'].pop(),
    lambda s: s['pairs'].__setitem__(1, deepcopy(s['pairs'][0])),
])
def test_changed_saved_population_geometry_or_class_fails(mutation):
    selection, bars, end, saved = synthetic()
    mutation(saved)
    with pytest.raises(ValueError, match='matched_progress_invalid'):
        describe_progress(selection, saved, bars, evaluation_end=end)


def test_changed_source_and_missing_after_passage_do_not_silently_become_complete():
    selection, bars, end, saved = synthetic()
    # A missing candle after an early passage still censors full-window progress.
    missing = tuple(b for b in bars if b.open_time != utc('2025-02-01T04:00:00Z'))
    result = describe_progress(selection, saved, missing, evaluation_end=end)
    assert not result['pairs'][0]['case']['progress']['complete_horizon']
    changed = tuple(replace(b, source_identity_key='other') if b.open_time == utc('2025-02-01T04:00:00Z') else b for b in bars)
    with pytest.raises(ValueError, match='source or availability changed'):
        describe_progress(selection, saved, changed, evaluation_end=end)


def test_weighted_quantile_uses_accurate_prefix_at_exact_boundary():
    rows = [{'completion_moment_weight': w, 'case': {'progress': {'x': float(i)}}}
            for i, w in enumerate([.05]*10+[.5])]
    result = distribution(rows, 'case', 'x')
    assert result['quantiles_percent']['0.5'] == 9.
