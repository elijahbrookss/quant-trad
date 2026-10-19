"""Configured population output stability and outcome-blind scope invariants."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace

import pytest
from research_science.recognition import RecognitionContext,RecognitionEvent,RecognitionStage,utc
from research_science.reversal_population import SelectionPopulation,select_population,RULES
from research_science.reversal_selection_timing import LEGACY_CHOICES
from research_science.hashing import stable_hash
from tests.test_research_science.test_reversal_selection import fixture


def population(blocks=2, origin='2025-01-01T00:00:00Z', start='2025-02-01T00:00:00Z', views=None):
    return SelectionPopulation.from_configuration(origin=origin,start=start,
        end=utc(start)+timedelta(days=28*blocks),views=[] if views is None else views)


def world(pop):
    bars,_=fixture(count=pop.input_bars);delta=pop.origin-bars[0].open_time
    bars=tuple(replace(b,open_time=b.open_time+delta,close_time=b.close_time+delta,known_at=b.known_at+delta) for b in bars)
    context=RecognitionContext('synthetic',900,pop.origin,'source');events=[]
    for index in (4000,7000,13000):
        if index>=len(bars):continue
        at=bars[index].close_time
        events.append(RecognitionEvent('synthetic','1',stable_hash({**LEGACY_CHOICES,'max_bars':19104}),context,
            'provisional_reversal_candidate',bars[index-5].open_time,at,at,
            (RecognitionStage('completion',at,at,bars[index].candle_id),),()))
    return bars,SimpleNamespace(candidates=tuple(events),references=())


@pytest.mark.parametrize('blocks,views,expected_hash',[
    (2,[{'name':'block1','start_block':0,'block_count':1},{'name':'block2','start_block':1,'block_count':1}],'d538ea98b2792d7c9c0ee6ce17efcf912d3c3bfc9b022941978955189e13cf2f'),
    (6,[{'name':'old','start_block':0,'block_count':3},{'name':'new','start_block':3,'block_count':3}],'42172767fa1a962622f175f388c93e92ffa856f43bd26b26abcd3d3aeffcb4d4'),
])
def test_configured_scope_full_result_hash_preserves_cases_controls_weights_and_dependence(blocks,views,expected_hash):
    pop=population(blocks,views=views);bars,snapshot=world(pop)
    result=select_population(bars,snapshot,pop)
    assert stable_hash(result)==expected_hash
    assert result['cases'] and result['pairs']
    assert result['outcomes_calculated'] is False
    retained=json.loads(json.dumps(result))
    assert stable_hash({k:retained[k] for k in retained['selection_manifest_fields']})==retained['selection_manifest_hash']


def test_another_date_range_uses_the_same_kernel_without_calendar_constants():
    original=population();shifted=population(origin='2024-06-01T00:00:00Z',start='2024-07-02T00:00:00Z')
    assert original.input_bars==shifted.input_bars==8352
    bars,snapshot=world(shifted);result=select_population(bars,snapshot,shifted)
    assert len(result['cases'])==2 and len(result['pairs'])==2
    assert result['population_recipe']['origin']=='2024-06-01T00:00:00Z'
    assert result['population_recipe']['decision_start']=='2024-07-02T00:00:00Z'
    assert result['selection_summary']['independence_claim'] is False


def test_reporting_views_do_not_change_selection_or_root_weights():
    first=population(views=[{'name':'first','start_block':0,'block_count':1}]);bars,snapshot=world(first)
    before=select_population(bars,snapshot,first)
    after=select_population(bars,snapshot,population(views=[{'name':'last','start_block':1,'block_count':1}]))
    for key in ('cases','pairs','dependence','selection_summary'):assert before[key]==after[key]
    assert set(before['populations'])=={'first','combined'} and set(after['populations'])=={'last','combined'}


@pytest.mark.parametrize('change',[
    lambda c:c.update(start='2024-12-31T00:00:00Z'),
    lambda c:c.update(end='2025-03-28T00:00:00Z'),
    lambda c:c.update(origin='2024-01-01T00:00:00Z'),
    lambda c:c.update(origin='2025-01-01T00:01:00Z'),
    lambda c:c.update(end='2025-08-16T00:00:00Z'),
    lambda c:c.update(views=[{'name':'combined','start_block':0,'block_count':1}]),
    lambda c:c.update(views=[{'name':'bad','start_block':0,'block_count':True}]),
    lambda c:c.update(views=[{'name':'bad','start_block':-1,'block_count':1}]),
    lambda c:c.update(views=[{'name':'bad','start_block':1,'block_count':2}]),
    lambda c:c.update(views=[{'name':'bad','start_block':0,'block_count':1,'threshold':.5}]),
    lambda c:c.update(views=[{'name':n,'start_block':0,'block_count':1} for n in ('a','b','c')]),
])
def test_no_unbounded_scope_partial_blocks_extra_thresholds_or_views(change):
    config={'origin':'2025-01-01T00:00:00Z','start':'2025-02-01T00:00:00Z','end':'2025-03-29T00:00:00Z','views':[]}
    change(config)
    with pytest.raises(ValueError,match='reversal_population_invalid'):SelectionPopulation.from_configuration(**config)


def test_existing_resource_and_scientific_limits_stay_fixed():
    from research_science.reversal_selection_timing import RULES as old
    for key in old.keys()-{'version','evaluation','populations'}:assert RULES[key]==old[key]
    pop=population();bars,snapshot=world(pop)
    with pytest.raises(ValueError,match='complete original clock'):select_population(bars[:-1],snapshot,pop)
    def cancelled():raise RuntimeError('cancelled')
    with pytest.raises(RuntimeError,match='cancelled'):select_population(bars,snapshot,pop,checkpoint=cancelled)
