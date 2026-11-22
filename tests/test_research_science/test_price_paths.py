"""Explicit fractional arithmetic policies; no forced rounding/sign unification."""
import math
import pytest
from research_science.price_paths import ratio_path_observation,difference_path_observation,safe_ratio


def test_ratio_and_subtraction_policies_preserve_distinct_historical_float_results():
    ratio=ratio_path_observation(entry_price=100.,endpoint_price=101.,high=102.,low=99.,direction=1.)
    difference=difference_path_observation(entry_price=100.,endpoint_price=101.,high=102.,low=99.,short=False)
    assert ratio.endpoint_return==.010000000000000009
    assert difference.endpoint_return==.01
    assert ratio.maximum_favorable_excursion==.020000000000000018
    assert difference.maximum_favorable_excursion==.02
    assert ratio.maximum_adverse_excursion==-.010000000000000009
    assert difference.maximum_adverse_excursion==-.01


def test_no_anchor_clamp_and_short_subtraction_order_or_signed_zero_change():
    value=ratio_path_observation(entry_price=100.,endpoint_price=98.,high=99.,low=97.,direction=1.)
    assert value.maximum_favorable_excursion<0
    short=difference_path_observation(entry_price=100.,endpoint_price=100.,high=100.,low=100.,short=True)
    assert math.copysign(1.,short.direction_signed_return)==-1.
    assert math.copysign(1.,short.maximum_favorable_excursion)==1.
    assert math.copysign(1.,short.maximum_adverse_excursion)==1.


def test_zero_denominator_policies_remain_distinct():
    assert safe_ratio(2.,0.)==0.
    value=difference_path_observation(entry_price=0.,endpoint_price=1.,high=2.,low=0.,short=False)
    assert value.endpoint_return==value.maximum_favorable_excursion==0.
    with pytest.raises(ZeroDivisionError):ratio_path_observation(entry_price=0.,endpoint_price=1.,high=2.,low=0.,direction=1.)


def test_extrema_first_candle_ties_and_short_direction_are_explicit():
    from research_science.price_paths import first_extrema_candles
    path=[{'open_time':i,'close_time':i+1,'known_at':i+2,'high':high,'low':low}
          for i,(high,low) in enumerate([(105.,98.),(105.,96.),(104.,96.)])]
    long=first_extrema_candles(path,direction=1.)
    short=first_extrema_candles(path,direction=-1.)
    assert long['favorable']['bar_offset']==1 and long['adverse']['bar_offset']==2
    assert short['favorable']==long['adverse'] and short['adverse']==long['favorable']
    value=ratio_path_observation(entry_price=100.,endpoint_price=97.,high=105.,low=96.,direction=-1.)
    assert value.direction_signed_return==-(97./100.-1.)
    assert value.maximum_favorable_excursion==1.-96./100.
    assert value.maximum_adverse_excursion==1.-105./100.<0
