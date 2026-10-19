"""The two historical prefix policies intentionally differ at exact boundaries."""
import math
import pytest
from research_science.weighted_summaries import first_crossing_quantiles,sequential_prefixes,accurate_prefixes
from research_science.reversal_selection import _distribution as selection_distribution
from research_science.reversal_matched_progress import distribution as progress_distribution


def test_no_rounding_policy_is_silently_standardized():
    pairs=list(enumerate([.05]*10+[.5]));mass=math.fsum(w for _,w in pairs)
    assert first_crossing_quantiles(pairs,[.5],total_weight=mass,prefixes=sequential_prefixes)=={'0.5':10}
    assert first_crossing_quantiles(pairs,[.5],total_weight=mass,prefixes=accurate_prefixes)=={'0.5':9}
    records=[{'event_id':str(i),'features':{'x':float(i)}} for i in range(11)]
    weights={str(i):w for i,w in pairs}
    assert selection_distribution(records,weights,'x')['quantiles']['0.5']==10.
    rows=[{'completion_moment_weight':w,'case':{'progress':{'x':float(i)}}} for i,w in pairs]
    assert progress_distribution(rows,'case','x')['quantiles_percent']['0.5']==9.


@pytest.mark.parametrize('prefixes',[sequential_prefixes,accurate_prefixes])
def test_empty_ties_and_mass_conditioning_are_explicit(prefixes):
    assert first_crossing_quantiles([], [.1,.5,.9],total_weight=0.,prefixes=prefixes)=={'0.1':None,'0.5':None,'0.9':None}
    assert first_crossing_quantiles([(2.,.2),(2.,.3),(8.,.5)],[0.,.5,1.],total_weight=1.,prefixes=prefixes)=={'0.0':2.,'0.5':2.,'1.0':8.}


def test_summary_callers_keep_missing_mass_and_bins_outside_shared_math():
    rows=[{'completion_moment_weight':.25,'case':{'progress':{'x':None}}},
          {'completion_moment_weight':.75,'case':{'progress':{'x':50.}}}]
    result=progress_distribution(rows,'case','x')
    assert result['group_saved_weight_mass']==1. and result['available_saved_weight_mass']==.75
    assert result['unavailable_saved_weight_mass']==.25
    assert result['bins'][2]['weighted_fraction_of_available']==1.


def test_existing_matched_kernel_scientific_material_remains_exact():
    from tests.test_research_science.test_reversal_matched_outcomes import fixture
    from research_science.reversal_matched_outcomes import describe_selected_pairs
    from research_science.reversal_matched_progress import describe_progress
    from research_science.hashing import stable_hash
    selection,bars,end=fixture();selection['source_identity_key']='source'
    outcomes=describe_selected_pairs(selection,bars,evaluation_end=end)
    progress=describe_progress(selection,outcomes,bars,evaluation_end=end)
    assert stable_hash(outcomes)=='d12c2f970e6c1f4b0962a50f22d5e9378c10c9808852700e67e514f0e93e4407'
    assert stable_hash(progress)=='644e58b165ad5d41086729ba0b5929c03755ee7aac083e785c04051eba59e486'
