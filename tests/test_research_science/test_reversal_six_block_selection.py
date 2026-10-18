"""Synthetic compatibility, timing representation and population accounting."""
from dataclasses import replace
import json
from types import SimpleNamespace
import pytest

from datetime import timedelta
from research_science.recognition import RecognitionContext, RecognitionCandle
from research_science.reversal_recognition import ReversalSettings, recognize_reversals
from research_science.reversal_selection import _select_comparisons, select_comparisons, stage_availability
from research_science.reversal_six_block_selection import (
    RULES, LEGACY_CHOICES, compatibility_order_keys, compact_stage_availability,
    expand_stage_availability, select_six_blocks, ORIGIN, START, SPLIT)
from research_science.study import stable_hash
from tests.test_research_science.test_recognition import candles, CONTEXT, t
from tests.test_research_science.test_reversal_selection import fixture


def snapshots():
    old=recognize_reversals(candles(),context=CONTEXT,settings=ReversalSettings(**LEGACY_CHOICES))
    new=recognize_reversals(candles(),context=CONTEXT,
        settings=ReversalSettings(**{**LEGACY_CHOICES,'max_bars':19104}))
    return old,new


def test_capacity_namespace_changes_real_ids_but_projection_exactly_preserves_B2_order_keys():
    old,new=snapshots()
    assert old.candidates and new.candidates
    assert old.candidates[0].event_id!=new.candidates[0].event_id
    before=[e.to_dict() for e in new.candidates]
    keys=compatibility_order_keys(new)
    assert [keys[e.event_id] for e in new.candidates]==[e.event_id for e in old.candidates]
    assert [e.to_dict() for e in new.candidates]==before
    absent=replace(new.candidates[0],source_events=(replace(new.candidates[0].source_events[0],event_id='missing'),),event_id='')
    with pytest.raises(ValueError,match='missing retained reference'):
        compatibility_order_keys(SimpleNamespace(references=(),candidates=(absent,)))


def test_compact_timing_is_losslessly_decodable_and_smaller_for_actual_recognizer_events():
    _,snapshot=snapshots()
    for event in snapshot.candidates:
        compact=compact_stage_availability(event)
        full=stage_availability(event)
        assert expand_stage_availability(compact)==full
        assert len(json.dumps(compact))<len(json.dumps(full))
        assert compact['parent_known_at']==event.known_at.isoformat().replace('+00:00','Z')
    bad=compact_stage_availability(snapshot.candidates[0]);bad['candle_known_at_rule']='unknown'
    with pytest.raises(ValueError):expand_stage_availability(bad)


def test_B2_same_clock_order_and_control_choices_survive_changed_actual_namespace():
    bars,event=fixture()
    old_events=[event(key='a'),event(key='b')]
    baseline=select_comparisons(bars,old_events,evaluation_start=t(100),evaluation_end=t(850))
    # Deliberately reverse actual lexicographic IDs; compatibility keys restore prior order.
    expanded_events=[event(key='z'),event(key='y')]
    expanded=_select_comparisons(bars,expanded_events,evaluation_start=t(100),evaluation_end=t(850),
        rules=RULES,order_keys={'z':'a','y':'b'})
    assert [c['b2_compatibility_order_key'] for c in expanded['cases']]==['a','b']
    assert [p['control'] for p in expanded['pairs']]==[p['control'] for p in baseline['pairs']]
    assert [p['distance_squared'] for p in expanded['pairs']]==[p['distance_squared'] for p in baseline['pairs']]
    assert expanded['selection_summary']==baseline['selection_summary']


def test_partition_summaries_have_local_denominators_without_overwriting_combined_row_weights():
    bars,event=fixture()
    result=_select_comparisons(bars,[event(i=500,key='a'),event(i=700,key='b'),event(i=700,key='c')],
        evaluation_start=t(100),evaluation_end=t(850),rules=RULES,
        population_ranges=(('old',t(100),t(600)),('new',t(600),t(850)),('combined',t(100),t(850))))
    assert result['populations']['old']['selection_summary']['all']=={'chains':1,'completion_clocks':1}
    assert result['populations']['new']['selection_summary']['all']=={'chains':2,'completion_clocks':1}
    assert result['populations']['combined']['selection_summary']==result['selection_summary']
    assert [r['full_population_moment_weight'] for r in result['cases']]==[.5,.25,.25]
    for name in ('old','new','combined'):
        dep=result['populations'][name]['dependence']['all_cases_and_selected_controls']
        assert dep['ancestry_aware_component_count']==1
        assert dep['independent_sample_size'] is None


def test_historical_v1_synthetic_material_is_unchanged():
    bars,event=fixture()
    result=select_comparisons(bars,[event(key='a'),event(key='b'),event(i=700,key='long',n=501)],
        evaluation_start=t(100),evaluation_end=t(850))
    assert stable_hash(result)=='6982ce45be12cb51619e523aa439025019896484091b538609e80086d15b9a2f'


def test_expanded_input_limit_and_cancellation_fail_without_truncation():
    bars,event=fixture()
    with pytest.raises(ValueError,match='candle count'):
        _select_comparisons(bars*22,[],evaluation_start=t(100),evaluation_end=t(850),rules=RULES)
    def stop():raise RuntimeError('cancelled')
    with pytest.raises(RuntimeError,match='cancelled'):
        _select_comparisons(bars,[event()],evaluation_start=t(100),evaluation_end=t(850),rules=RULES,checkpoint=stop)


def test_full_synthetic_prefix_preserves_structural_events_features_pairs_and_old_summary():
    from tests.test_research_science.test_recognition import candles as pattern
    bars=[]
    for i in range(19104):
        opened=ORIGIN+timedelta(minutes=15*i);price=100+i*.001
        bars.append(RecognitionCandle(str(i),'synthetic',opened,opened+timedelta(minutes=15),
            opened+timedelta(minutes=15),price,price+.01,price-.01,price))
    for start in (4000,13000):
        for offset,bar in enumerate(pattern()):
            bars[start+offset]=replace(bars[start+offset],open=bar.open,high=bar.high,low=bar.low,close=bar.close)
    context=RecognitionContext('instrument',900,ORIGIN,'synthetic')
    old=recognize_reversals(tuple(bars[:11040]),context=context,settings=ReversalSettings(**LEGACY_CHOICES))
    new=recognize_reversals(tuple(bars),context=context,
        settings=ReversalSettings(**{**LEGACY_CHOICES,'max_bars':19104}))
    baseline=select_comparisons(tuple(bars[:11040]),old.candidates,evaluation_start=START,evaluation_end=SPLIT)
    expanded=select_six_blocks(tuple(bars),new)
    prefix=[c for c in expanded['cases'] if c['decision_at']<'2025-04-26T00:00:00Z']
    assert prefix and len(prefix)==len(baseline['cases'])
    for before,after in zip(baseline['cases'],prefix):
        assert after['b2_compatibility_order_key']==before['event_id']
        for key in ('decision_at','formation_start','formation_candles','boundary_geometry','features',
                    'support_start','support_end','match_status','candidate_rejections','eligible_candidate_count'):
            assert after[key]==before[key]
        assert expand_stage_availability(after['timing'])==before['timing']
    assert expanded['populations']['old']['selection_summary']==baseline['selection_summary']
    old_pairs={p['event_id']:p for p in baseline['pairs']}
    aliases={c['event_id']:c['b2_compatibility_order_key'] for c in prefix}
    for pair in expanded['pairs']:
        if pair['event_id'] in aliases:
            before=old_pairs[aliases[pair['event_id']]]
            assert pair['control']==before['control'] and pair['distance_squared']==before['distance_squared']
