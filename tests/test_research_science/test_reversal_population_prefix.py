"""Synthetic clean-prefix invariance; no historical ETH recognition or outcomes."""
from dataclasses import replace
from types import SimpleNamespace
import json
from research_science.recognition import RecognitionContext,RecognitionEvent,RecognitionStage
from research_science.reversal_selection_timing import LEGACY_CHOICES
from research_science.reversal_population import SelectionPopulation,select_population
from research_science.recognition import utc
ORIGIN=utc('2025-01-01T00:00:00Z')
START=utc('2025-02-01T00:00:00Z')
from research_science.hashing import stable_hash
from tests.test_research_science.test_reversal_selection import fixture


def test_two_full_blocks_preserve_cases_control_choices_and_tie_namespace():
    bars,_=fixture(count=19104);delta=ORIGIN-bars[0].open_time
    bars=tuple(replace(b,open_time=b.open_time+delta,close_time=b.close_time+delta,known_at=b.known_at+delta) for b in bars)
    context=RecognitionContext('synthetic',900,ORIGIN,'source')
    events=[]
    for index in (4000,7000,13000):
        at=bars[index].close_time
        events.append(RecognitionEvent('synthetic','1',stable_hash({**LEGACY_CHOICES,'max_bars':19104}),context,
            'provisional_reversal_candidate',bars[index-5].open_time,at,at,
            (RecognitionStage('completion',at,at,bars[index].candle_id),),()))
    full_scope=SelectionPopulation.from_configuration(origin=ORIGIN,start=START,end=utc('2025-07-19T00:00:00Z'),views=[])
    prefix_scope=SelectionPopulation.from_configuration(origin=ORIGIN,start=START,end=utc('2025-03-29T00:00:00Z'),views=[])
    full=select_population(bars,SimpleNamespace(candidates=tuple(events),references=()),full_scope)
    prefix=select_population(bars[:8352],SimpleNamespace(candidates=tuple(events[:2]),references=()),prefix_scope)
    prior=[c for c in full['cases'] if c['block']<2]
    assert len(prefix['cases'])==len(prior)==2 and len(prefix['pairs'])==2
    for before,after in zip(prior,prefix['cases']):
        for key in before.keys()-{'full_population_moment_weight','matched_population_moment_weight'}:
            assert before[key]==after[key]
    full_pairs={p['event_id']:p for p in full['pairs']}
    for p in prefix['pairs']:
        assert p['control']==full_pairs[p['event_id']]['control']
        assert p['distance_squared']==full_pairs[p['event_id']]['distance_squared']
    assert prefix['populations']['combined']['selection_summary']==prefix['selection_summary']
    saved=json.loads(json.dumps(prefix))
    assert saved['selection_manifest_hash']==stable_hash({k:saved[k] for k in saved['selection_manifest_fields']})
