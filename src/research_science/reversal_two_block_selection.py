"""Explicit clean-prefix scope over the unchanged reversal comparison kernel."""
from copy import deepcopy
from .recognition import utc
from .reversal_selection import _select_comparisons
from .reversal_six_block_selection import RULES as SIX_RULES, compatibility_order_keys, compact_stage_availability
from .study import stable_hash

SCHEMA='reversal_two_block_selection.v1'
ORIGIN=utc('2025-01-01T00:00:00Z')
START=utc('2025-02-01T00:00:00Z')
SPLIT=utc('2025-03-01T00:00:00Z')
END=utc('2025-03-29T00:00:00Z')
INPUT_BARS=8352
RULES=deepcopy(SIX_RULES)
RULES.update(version=SCHEMA,
 evaluation='Fixed2025-02-01 through2025-03-29 exclusive;first two original28dayblocks;Jan1recognition origin;no reset',
 populations='block1,block2,combined;summary weights normalize within each reporting population')


def select_two_blocks(candles,snapshot,*,checkpoint=lambda:None):
    result=_select_comparisons(candles,snapshot.candidates,evaluation_start=START,evaluation_end=END,
        rules=RULES,checkpoint=checkpoint,order_keys=compatibility_order_keys(snapshot),timing_view=compact_stage_availability,
        population_ranges=(('block1',START,SPLIT),('block2',SPLIT,END),('combined',START,END)))
    result['compatibility']={'actual_event_ids_preserved':True,
        'order_namespace':'Same B2settings projection used by six-block selection',
        'scope_amendment':'Two complete pre-gap blocks;formation and matching rules unchanged;original six-block evidence preserved',
        'benchmark':'Corresponding first two BTC B3blocks;reuse saved cases/pairs without rematching'}
    fields=['rules','cases','pairs','dependence','populations','compatibility']
    result['selection_manifest_fields']=fields
    # Canonicalize JSON keys before hashing the new version;historical v12 stays intact.
    import json
    result['selection_manifest_hash']=stable_hash(json.loads(json.dumps({k:result[k] for k in fields})))
    return result
