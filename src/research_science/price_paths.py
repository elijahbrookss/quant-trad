"""Reusable fractional price-path arithmetic with explicit compatibility policies.

Callers own event/entry identity, native clocks, horizon availability and path
selection. Extrema are supplied from that admitted path. These functions do not
read candles, invent fills, clamp extrema to the anchor, or calculate outcomes
inside selection. Target-relative progress and barrier first passage retain
their distinct named kernels and policies.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class FractionalPathObservation:
    endpoint_return: float
    direction_signed_return: float
    maximum_favorable_excursion: float
    maximum_adverse_excursion: float


def safe_ratio(numerator:float,denominator:float)->float:
    """Historical preview policy; a zero denominator returns zero."""
    if denominator==0:
        return 0.0
    return float(numerator)/float(denominator)


def ratio_path_observation(*,entry_price,endpoint_price,high,low,direction):
    """Event/fact policy: x/entry-1, no zero-anchor clamp; fractional units."""
    endpoint=(endpoint_price/entry_price)-1.0
    if direction>0:
        favorable=(high/entry_price)-1.0
        adverse=(low/entry_price)-1.0
    else:
        favorable=1.0-(low/entry_price)
        adverse=1.0-(high/entry_price)
    return FractionalPathObservation(endpoint,endpoint*direction,favorable,adverse)


def difference_path_observation(*,entry_price,endpoint_price,high,low,short):
    """Legacy preview policy: (x-entry)/entry; preserve exact subtraction order."""
    endpoint=safe_ratio(endpoint_price-entry_price,entry_price)
    if short:
        favorable=safe_ratio(entry_price-low,entry_price)
        adverse=safe_ratio(entry_price-high,entry_price)
    else:
        favorable=safe_ratio(high-entry_price,entry_price)
        adverse=safe_ratio(low-entry_price,entry_price)
    return FractionalPathObservation(endpoint,endpoint*(-1.0 if short else 1.0),favorable,adverse)


def first_extrema_candles(path, *, direction):
    """First chronological attaining candle, without entry insertion or OHLC ordering.

    The caller supplies its already admitted future path in chronological order.
    Offsets are one-based from the excluded entry candle. A tie keeps the first
    candle; its availability is distinct from the completed horizon's known-at.
    """
    high = max(enumerate(path, 1), key=lambda item: float(item[1]['high']))
    low = min(enumerate(path, 1), key=lambda item: float(item[1]['low']))
    favorable, adverse = (high, low) if direction > 0 else (low, high)
    def located(item):
        offset, candle = item
        return {'bar_offset': offset, 'open_time': candle['open_time'],
                'close_time': candle['close_time'], 'known_at': candle['known_at']}
    return {'favorable': located(favorable), 'adverse': located(adverse)}
