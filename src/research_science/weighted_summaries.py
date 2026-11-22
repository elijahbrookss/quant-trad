"""Shared weighted CDF projection with explicit historical accumulation policy.

Callers own filtering, units, denominator/missing mass, quantile choices and bins.
This module never normalizes weights or changes the retained population.
"""
from bisect import bisect_left
import math


def sequential_prefixes(weights):
    """Historical selection summaries: preserve ordinary += floating arithmetic."""
    total=0.
    result=[]
    for weight in weights:
        total+=weight
        result.append(total)
    return result


def accurate_prefixes(weights):
    """Progress summaries: exact same math.fsum prefix used by the v1 protocol."""
    prefix=[]
    result=[]
    for weight in weights:
        prefix.append(weight)
        result.append(math.fsum(prefix))
    return result


def first_crossing_quantiles(sorted_pairs, probabilities, *, total_weight, prefixes):
    """Smallest value reaching q*original mass; no interpolation or reweighting."""
    if not sorted_pairs:
        return {str(q):None for q in probabilities}
    cumulative=prefixes([weight for _,weight in sorted_pairs])
    return {str(q):sorted_pairs[min(bisect_left(cumulative,total_weight*q),len(sorted_pairs)-1)][0]
            for q in probabilities}
