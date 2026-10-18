"""Bounded population configuration over the existing outcome-blind selector."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import re

from .recognition import plain, utc
from .reversal_selection import _select_comparisons
from .reversal_six_block_selection import (
    RULES as SIX_RULES, compatibility_order_keys, compact_stage_availability)
from .study import stable_hash

SCHEMA = 'reversal_population_selection.v1'
STEP = timedelta(minutes=15)
BLOCK = timedelta(days=28)
RULES = deepcopy(SIX_RULES)
RULES.update(version=SCHEMA,
    evaluation='Explicit fixed origin and one through six complete28day decision blocks;no reset',
    populations='Declared block views plus combined;summaries normalize within each view;root pairs retain original combined weights')
MANIFEST_FIELDS = ('rules', 'cases', 'pairs', 'dependence', 'populations', 'compatibility', 'population_recipe')


@dataclass(frozen=True)
class SelectionPopulation:
    origin: datetime
    start: datetime
    end: datetime
    views: tuple

    @classmethod
    def from_configuration(cls, *, origin, start, end, views):
        origin, start, end = utc(origin), utc(start), utc(end)
        if (not origin <= start < end or any(t.timestamp() % 900 for t in (origin,start,end))
                or (end-start) % BLOCK or not 1 <= (end-start)//BLOCK <= 6
                or (end-origin)//STEP > RULES['limits']['candles']):
            raise ValueError('reversal_population_invalid: aligned origin and1through6whole28dayblocks within19104bars required')
        if not isinstance(views, list) or len(views) > 2:
            raise ValueError('reversal_population_invalid: at most two declared views plus combined')
        normalized = []
        names = {'combined'}
        for view in views:
            if not isinstance(view, dict) or set(view) != {'name','start_block','block_count'}:
                raise ValueError('reversal_population_invalid: exact named block view required')
            name, first, count = (view[k] for k in ('name','start_block','block_count'))
            if (not isinstance(name,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,31}',name) or name in names
                    or type(first) is not int or type(count) is not int or first < 0 or count < 1
                    or first+count > (end-start)//BLOCK):
                raise ValueError('reversal_population_invalid: unique bounded block views required')
            names.add(name);normalized.append((name,first,count))
        return cls(origin,start,end,tuple(normalized))

    @property
    def input_bars(self):
        return int((self.end-self.origin)//STEP)

    @property
    def warmup_bars(self):
        return int((self.start-self.origin)//STEP)

    def to_dict(self):
        return {'origin':plain(self.origin),'decision_start':plain(self.start),'decision_end_exclusive':plain(self.end),
            'block_days':28,'block_count':int((self.end-self.start)//BLOCK),'input_bars':self.input_bars,
            'views':[{'name':name,'start_block':first,'block_count':count} for name,first,count in self.views]}

    def ranges(self):
        return tuple((name,self.start+first*BLOCK,self.start+(first+count)*BLOCK)
            for name,first,count in self.views)+(('combined',self.start,self.end),)


def select_population(candles, snapshot, population, *, checkpoint=lambda:None):
    """Only dates/report views vary; recognition and matching policies are unchanged."""
    if (len(candles) != population.input_bars or not candles
            or candles[0].open_time != population.origin or candles[-1].close_time != population.end):
        raise ValueError('reversal_population_invalid: complete original clock range required')
    result = _select_comparisons(candles,snapshot.candidates,evaluation_start=population.start,
        evaluation_end=population.end,rules=RULES,checkpoint=checkpoint,
        order_keys=compatibility_order_keys(snapshot),timing_view=compact_stage_availability,
        population_ranges=population.ranges())
    result['compatibility']={'actual_event_ids_preserved':True,
        'order_namespace':'Same B2settings projection used by the fixed six/two-block profiles',
        'scientific_policy':'Unchanged formation,matching,nominal support,weights and native clock;scope/views are explicit configuration'}
    result['population_recipe']=population.to_dict()
    result['selection_manifest_fields']=list(MANIFEST_FIELDS)
    result['selection_manifest_hash']=stable_hash(json.loads(json.dumps({k:result[k] for k in MANIFEST_FIELDS},allow_nan=False)))
    return result
