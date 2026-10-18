"""Read-only descriptions of registered methods; normalizers remain authoritative."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from .forward_risk_evaluator import PINNED_PARAMS
from .registry import CHECK_REGISTRY, definition_allows_evidence, normalize_check_request

SCHEMA = "research_check_catalog.v1"

# Documentation examples only. Replace resource bindings with authorized server IDs.
_EXAMPLES = {'event_fact_analysis@4': {'check_family': 'event_fact_analysis',
                           'scope': {'indicator_id': 'indicator-1',
                                     'instrument_id': 'instrument-1',
                                     'timeframe': '1h',
                                     'start': '2022-01-01T00:00:00Z',
                                     'end': '2022-01-02T00:00:00Z'},
                           'detector': {'type': 'indicator_event',
                                        'output_name': 'atr_expansion',
                                        'event_keys': [{'key': 'atr_expansion_long',
                                                        'direction': 'long'}]},
                           'outcomes': {'horizons': [1, 3]},
                           'inputs': [{'alias': 'reference_price',
                                       'fact_type': 'market.reference_price',
                                       'contract_version': 'market.reference_price.v1',
                                       'dimensions': {'quote_currency': 'USD'},
                                       'source_policy': {'mode': 'exact',
                                                         'source_identity_key': 'source-a'}}],
                           'gap_policy': 'reject',
                           'mode': 'preview'},
 'event_fact_analysis@5': {'check_family': 'event_fact_analysis',
                           'scope': {'indicator_id': 'indicator-1',
                                     'instrument_id': 'instrument-1',
                                     'timeframe': '1h',
                                     'start': '2022-01-01T00:00:00Z',
                                     'end': '2022-01-02T00:00:00Z'},
                           'detector': {'type': 'fact_snapshot',
                                        'input_alias': 'bbo',
                                        'evaluation_trigger': 'required_facts_available'},
                           'outcomes': {'horizons': [1, 3]},
                           'inputs': [{'alias': 'bbo',
                                       'fact_type': 'market.bbo',
                                       'contract_version': 'market.bbo.v1',
                                       'timeframe_seconds': 1,
                                       'max_staleness_seconds': 120,
                                       'source_policy': {'mode': 'exact', 'source_identity_key': 'source-identity-id'}}],
                           'gap_policy': 'reject',
                           'mode': 'preview'},
 'event_fact_analysis@6': {'check_family': 'event_fact_analysis',
                           'mode': 'preview',
                           'inputs': [],
                           'gap_policy': 'reject',
                           'scope': {'indicator_id': 'profile-1',
                                     'instrument_id': 'instrument-1',
                                     'timeframe': '1h',
                                     'start': '2022-01-01T00:00:00+00:00',
                                     'end': '2022-01-01T06:00:00+00:00'},
                           'detector': {'type': 'indicator_event',
                                        'output_name': 'raw',
                                        'event_keys': [{'key': 'raw_long', 'direction': 'long'}]},
                           'outcomes': {'horizons': [1, 2], 'primary_horizon': 2, 'entry_lag_bars': 1},
                           'statistics': {'eligibility': {'min_samples': 1}}},
 'event_fact_analysis@7': {'check_family': 'event_fact_analysis',
                           'mode': 'preview',
                           'inputs': [],
                           'gap_policy': 'reject',
                           'scope': {'indicator_id': 'profile-1',
                                     'instrument_id': 'instrument-1',
                                     'timeframe': '1h',
                                     'start': '2022-01-01T00:00:00+00:00',
                                     'end': '2022-01-01T06:00:00+00:00'},
                           'detector': {'type': 'indicator_event',
                                        'output_name': 'raw',
                                        'event_keys': [{'key': 'raw_long', 'direction': 'long'}]},
                           'outcomes': {'horizons': [1, 2],
                                        'primary_horizon': 2,
                                        'entry_lag_bars': 1,
                                        'matched_origin': {'detector': {'type': 'indicator_event',
                                                                        'output_name': 'confirmed',
                                                                        'event_keys': [{'key': 'confirmed_long',
                                                                                        'direction': 'long'}]},
                                                           'origin_time_path': 'metadata.breakout_time',
                                                           'origin_event_key_path': 'metadata.breakout_event_key',
                                                           'reference_path': 'metadata.reference'}},
                           'statistics': {'eligibility': {'min_samples': 1}}},
 'event_fact_analysis@8': {'check_family': 'event_fact_analysis',
                           'mode': 'preview',
                           'inputs': [],
                           'gap_policy': 'reject',
                           'scope': {'indicator_id': 'profile-1',
                                     'instrument_id': 'instrument-1',
                                     'timeframe': '1h',
                                     'start': '2022-01-01T00:00:00+00:00',
                                     'end': '2022-01-01T06:00:00+00:00'},
                           'detector': {'type': 'indicator_event',
                                        'output_name': 'raw',
                                        'event_keys': [{'key': 'raw_long', 'direction': 'long'}]},
                           'outcomes': {'horizons': [2, 3],
                                        'primary_horizon': 2,
                                        'entry_lag_bars': 1,
                                        'matched_origin': {'detector': {'type': 'indicator_event',
                                                                        'output_name': 'confirmed',
                                                                        'event_keys': [{'key': 'confirmed_long',
                                                                                        'direction': 'long'}]},
                                                           'origin_time_path': 'metadata.breakout_time',
                                                           'origin_event_key_path': 'metadata.breakout_event_key',
                                                           'reference_path': 'metadata.reference'},
                                        'required_horizons': [2, 3],
                                        'shared_landmark': {'classification_lag_bars': 1,
                                                            'sample_lag_bars': 2,
                                                            'readiness_contract': 'market_profile.value_location.v1',
                                                            'dependence': 'leave_one_original_profile_out.v1'}},
                           'statistics': {'eligibility': {'min_samples': 1}}},
 'event_fact_analysis@9': {'check_family': 'event_fact_analysis',
                           'mode': 'preview',
                           'inputs': [],
                           'gap_policy': 'reject',
                           'scope': {'indicator_id': 'profile-1',
                                     'instrument_id': 'instrument-1',
                                     'timeframe': '1h',
                                     'start': '2022-01-01T00:00:00+00:00',
                                     'end': '2022-01-01T06:00:00+00:00'},
                           'detector': {'type': 'indicator_event',
                                        'output_name': 'balance_breakout',
                                        'event_keys': [{'key': 'balance_breakout_long',
                                                        'direction': 'long'}]},
                           'outcomes': {'horizons': [2, 3],
                                        'primary_horizon': 2,
                                        'entry_lag_bars': 1,
                                        'required_horizons': [2],
                                        'first_return': {'classification_lag_bars': 1,
                                                         'sample_lag_bars': 2,
                                                         'readiness_contract': 'market_profile.first_return_state.v2',
                                                         'dependence': 'leave_one_original_profile_out.v1'}},
                           'statistics': {'eligibility': {'min_samples': 1}}},
 'event_fact_analysis@10': {'check_family': 'event_fact_analysis',
                            'mode': 'preview',
                            'inputs': [],
                            'gap_policy': 'reset_rewarm',
                            'scope': {'indicator_id': 'stats-1',
                                      'instrument_id': 'instrument-1',
                                      'timeframe': '1m',
                                      'start': '2022-12-30T00:00:00Z',
                                      'end': '2023-01-01T00:00:00Z',
                                      'warmup_bars': 200},
                            'detector': {'type': 'indicator_event',
                                         'output_name': 'atr_expansion',
                                         'event_keys': [{'key': 'atr_expansion_long',
                                                         'direction': 'long'}]},
                            'outcomes': {'horizon_kind': 'elapsed_time',
                                         'horizons': [1800, 7200, 21600],
                                         'primary_horizon': 7200,
                                         'entry_lag_bars': 0,
                                         'forward_risk': {'schema_version': 'candle_risk_comparison.v1',
                                                          'baseline_bars': 120,
                                                          'readiness_contract': 'candle_stats.public_outputs.v1',
                                                          'outcome_boundary': 'evaluation_end_exclusive'}},
                            'statistics': {}},
 'event_fact_analysis@11': {'check_family': 'event_fact_analysis',
                            'mode': 'preview',
                            'inputs': [],
                            'gap_policy': 'reset_rewarm',
                            'scope': {'indicator_id': 'stats-1',
                                      'instrument_id': 'instrument-1',
                                      'timeframe': '1m',
                                      'start': '2022-12-30T00:00:00Z',
                                      'end': '2023-01-01T00:00:00Z',
                                      'warmup_bars': 200},
                            'detector': {'type': 'indicator_event',
                                         'output_name': 'atr_expansion',
                                         'event_keys': [{'key': 'atr_expansion_long',
                                                         'direction': 'long'}]},
                            'outcomes': {'horizon_kind': 'elapsed_time',
                                         'horizons': [1800, 7200, 21600],
                                         'primary_horizon': 7200,
                                         'entry_lag_bars': 0,
                                         'forward_risk': {'schema_version': 'candle_risk_matched_state.v1',
                                                          'baseline_bars': 120,
                                                          'readiness_contract': 'candle_stats.public_outputs.v1',
                                                          'outcome_boundary': 'evaluation_end_exclusive',
                                                          'matching_contract': 'crossing_state_matched_pairs.v1'}},
                            'statistics': {}},
 'raw_forward_outcome@4': {'check_family': 'raw_forward_outcome', 'mode': 'preview',
     'scope': {'instrument_id': 'instrument-id', 'timeframe': '1m',
               'start': '2022-01-01T00:00:00Z', 'end': '2022-02-01T00:00:00Z'},
     'detector': {'type': 'controlled_bullish_reversal'},
     'outcomes': {'schema_version': 'controlled_reversal_comparison.v1'},
     'gap_policy': 'continue_degraded'},
 'raw_forward_outcome@3': {'check_family': 'raw_forward_outcome', 'mode': 'preview',
     'scope': {'instrument_id': 'instrument-id', 'timeframe': '1m',
               'start': '2022-01-01T00:00:00Z', 'end': '2022-01-02T00:00:00Z'},
     'detector': {'type': 'raw_condition', 'field': 'close', 'operator': 'gt', 'value_field': 'previous_close'},
     'outcomes': {'schema_version': 'raw_candle_forward_outcome.v1', 'forward_bars': [5], 'entry_lag_bars': 1},
     'gap_policy': 'continue_degraded'},
 'raw_forward_outcome@2': {'check_family': 'raw_forward_outcome',
                           'mode': 'preview',
                           'scope': {'instrument_id': 'instrument-id',
                                     'indicator_id': 'indicator-id',
                                     'timeframe': '1h',
                                     'start': '2022-01-01T00:00:00Z',
                                     'end': '2022-01-02T00:00:00Z'},
                           'detector': {'type': 'raw_condition',
                                        'field': 'close',
                                        'operator': 'gt',
                                        'value_field': 'previous_close'},
                           'outcomes': {'forward_bars': [1, 3]},
                           'gap_policy': 'reject'},
 'indicator_forward_outcome@2': {'check_family': 'indicator_forward_outcome',
                                 'mode': 'preview',
                                 'scope': {'instrument_id': 'instrument-id',
                                           'indicator_id': 'indicator-id',
                                           'timeframe': '1h',
                                           'start': '2022-01-01T00:00:00Z',
                                           'end': '2022-01-02T00:00:00Z'},
                                 'detector': {'type': 'record_match', 'output_name': 'entry'},
                                 'outcomes': {'forward_bars': [1, 3]},
                                 'gap_policy': 'reject'},
 'signal_audit@2': {'check_family': 'signal_audit',
                    'mode': 'preview',
                    'scope': {'instrument_id': 'instrument-id',
                              'indicator_id': 'indicator-id',
                              'timeframe': '1h',
                              'start': '2022-01-01T00:00:00Z',
                              'end': '2022-01-02T00:00:00Z'},
                    'detector': {'type': 'signal_audit',
                                 'source_output': 'state',
                                 'source_field': 'ready',
                                 'signal_output': 'entry',
                                 'event_key': 'entry_long',
                                 'operator': 'eq',
                                 'value': True},
                    'outcomes': {'forward_bars': [1, 3]},
                    'gap_policy': 'reject'},
 'candidate_lifecycle@2': {'check_family': 'candidate_lifecycle',
                           'mode': 'preview',
                           'scope': {'instrument_id': 'instrument-id',
                                     'indicator_id': 'indicator-id',
                                     'timeframe': '1h',
                                     'start': '2022-01-01T00:00:00Z',
                                     'end': '2022-01-02T00:00:00Z'},
                           'detector': {'type': 'candidate_lifecycle'},
                           'outcomes': {'forward_bars': [1, 3]},
                           'gap_policy': 'reject'},
 'run_signal_summary@2': {'check_family': 'run_signal_summary',
                          'mode': 'preview',
                          'scope': {'instrument_id': 'instrument-id',
                                    'indicator_id': 'indicator-id',
                                    'timeframe': '1h',
                                    'start': '2022-01-01T00:00:00Z',
                                    'end': '2022-01-02T00:00:00Z',
                                    'run_id': 'run-id'},
                          'detector': {'type': 'run_signal_match', 'output_name': 'entry'},
                          'outcomes': {'forward_bars': [1, 3]},
                          'gap_policy': 'reject'},
 'run_decision_trade_comparison@2': {'check_family': 'run_decision_trade_comparison',
                                     'mode': 'preview',
                                     'scope': {'instrument_id': 'instrument-id',
                                               'indicator_id': 'indicator-id',
                                               'timeframe': '1h',
                                               'start': '2022-01-01T00:00:00Z',
                                               'end': '2022-01-02T00:00:00Z',
                                               'run_id': 'run-id'},
                                     'detector': {'type': 'run_decision_match',
                                                  'decision_state': 'accepted'},
                                     'outcomes': {'forward_bars': [1, 3]},
                                     'gap_policy': 'reject'}}

_EXAMPLES["event_fact_analysis@12"] = deepcopy(_EXAMPLES["event_fact_analysis@11"])
_EXAMPLES["event_fact_analysis@12"]["outcomes"]["forward_risk"].update(
    schema_version="candle_risk_matched_state.v2", filter_contract="crossing_state_atr_ratio_filter.v1")

_EXAMPLES["raw_forward_outcome@5"] = deepcopy(_EXAMPLES["raw_forward_outcome@4"])
_EXAMPLES["raw_forward_outcome@5"]["scope"].update(timeframe="15m", warmup_bars=97)
_EXAMPLES["raw_forward_outcome@5"]["outcomes"] = {
    "schema_version": "controlled_reversal_comparison.v2", "interval": "15m",
    "trend_lookback_bars": 12, "volatility_lookback_bars": 96, "entry_lag_bars": 1,
    "outcome_horizon_bars": 12, "matching_policy": "same_utc_close_slot", "comparison_window_days": 7}
_EXAMPLES['raw_forward_outcome@19'] = deepcopy(_EXAMPLES['raw_forward_outcome@5'])
_EXAMPLES['raw_forward_outcome@19']['outcomes']['schema_version'] = 'controlled_reversal_comparison.v3'
_EXAMPLES["event_fact_analysis@13"] = {
    "check_family": "event_fact_analysis", "mode": "preview", "gap_policy": "continue_degraded",
    "scope": {"instrument_id": "coinbase-btc-usd", "timeframe": "1h", "warmup_bars": 15,
        "start": "2026-07-25T00:00:00Z", "end": "2026-09-23T00:00:00Z"},
    "detector": {"type": "fact_snapshot", "input_alias": "long_liquidations"},
    "outcomes": {"horizons": [1], "entry_lag_bars": 0, "retrospective": {
        "schema_version": "retrospective_numeric_comparison.v1",
        "analysis_as_of": "2026-10-13T23:00:00Z", "bucket_time_interpretation": "bucket_start_unverified"}},
    "inputs": [{"alias": side+"_liquidations", "instrument_id": "binance-btc-usdt-perp",
        "fact_type": "derivatives.liquidation_volume", "contract_version": "derivatives.liquidation_volume.v1",
        "max_staleness_seconds": 3600, "alignment": "exact_interval",
        "dimensions": {"liquidated_side": side, "bucket_seconds": 3600, "volume_unit": "BTC",
            "exchange": "Binance", "product": "BTCUSDT_PERP.A"},
        "source_policy": {"mode": "exact", "source_identity_key": "authorized-source"}} for side in ("long", "short")],
    "statistics": {"features": {"baseline": [
        {"name": "recent_return", "operator": "lagged_return", "lookback_bars": 1},
        {"name": "recent_volatility", "operator": "atr_fraction", "period": 14}],
        "enriched": [{"name": "long_liquidations", "operator": "latest_value",
            "input_alias": "long_liquidations"},
            {"name": "short_liquidations", "operator": "latest_value",
            "input_alias": "short_liquidations"}]},
        "model": {"type": "standardized_l2_logistic", "c": 1.0}, "purge_bars": 1,
        "folds": [{"id": "fixed-comparison", "train": {"start": "2026-07-25T00:00:00Z", "end": "2026-09-03T00:00:00Z"},
            "validation": {"start": "2026-09-03T00:00:00Z", "end": "2026-09-23T00:00:00Z"}}]}}

_EXAMPLES["raw_forward_outcome@6"] = deepcopy(_EXAMPLES["raw_forward_outcome@3"])
_EXAMPLES["raw_forward_outcome@6"]["scope"]["warmup_bars"] = 1
_EXAMPLES["raw_forward_outcome@6"]["detector"] = {"all": [
    {"field": "previous_close", "operator": "lt", "value_field": "previous_open"},
    {"field": "close", "operator": "gt", "value_field": "open"},
    {"field": "open", "operator": "lte", "value_field": "previous_close"},
    {"field": "close", "operator": "gte", "value_field": "previous_open"},
    {"any": [{"field": "volume", "operator": "gt", "value_field": "previous_volume"},
             {"field": "volume", "operator": "lte", "value_field": "previous_volume"}]}]}
_EXAMPLES["raw_forward_outcome@6"]["outcomes"]["schema_version"] = "raw_candle_distribution.v1"

_EXAMPLES["raw_forward_outcome@7"] = deepcopy(_EXAMPLES["raw_forward_outcome@6"])
_EXAMPLES["raw_forward_outcome@7"]["outcomes"].update(
    schema_version="volume_cohort_matched.v1", forward_bars=[5], entry_lag_bars=1,
    comparison={"cohort":"volume_gt_previous_vs_lte", "prior_volatility_bars":20,
        "matching_policy":"same_date_hour", "caliper_ratios":{
            "formation_body_fraction":1.25, "formation_range_fraction":1.25, "prior_rms_return":1.25}})

_EXAMPLES["raw_forward_outcome@8"] = deepcopy(_EXAMPLES["raw_forward_outcome@7"])
_EXAMPLES["raw_forward_outcome@8"]["outcomes"]["schema_version"] = "volume_cohort_matched.v2"
_EXAMPLES["raw_forward_outcome@8"]["outcomes"]["comparison"].update(
    matching_policy="same_utc_close_slot", comparison_window_days=3, matching_slot_seconds=3600)

_EXAMPLES["event_fact_analysis@14"] = {
    "check_family":"event_fact_analysis", "mode":"preview", "gap_policy":"continue_degraded",
    "scope":{"instrument_id":"instrument-id","timeframe":"1m","start":"2026-09-22T00:00:00Z","end":"2026-09-22T00:30:00Z"},
    "detector":{"type":"liquidity_episode"},
    "outcomes":{"schema_version":"liquidity_episode.v1","analysis_as_of":"2026-10-14T00:00:00Z"},
    "inputs":[{"alias":alias,"fact_type":fact,"contract_version":contract,
        **({"timeframe_seconds":seconds} if seconds else {}),
        "alignment":"exact_interval" if seconds else "latest_known","max_staleness_seconds":1,"allow_gaps":True,
        "source_policy":{"mode":"exact","source_identity_key":"source-identity-id"}}
        for alias,fact,contract,seconds in [
            ("bbo","market.bbo","market.bbo.v1",1),
            ("depth","market.depth_observation","market.depth_band.v1",1),
            ("flow","market.trade_flow","market.trade_flow.v1",1),
            ("trades","market.trade","market.trade.v1",None)]]}

_EXAMPLES["raw_forward_outcome@9"] = {
    "check_family": "raw_forward_outcome", "mode": "preview", "gap_policy": "reject",
    "scope": {"instrument_id": "instrument-1", "timeframe": "15m",
        "start": "2022-01-02T00:00:00Z", "end": "2022-01-02T06:00:00Z"},
    "detector": {"type": "completed_reversal_candidates",
        "computation_origin": "2022-01-01T00:00:00Z", "reference_basis": "rising_leg_only",
        "ownership": "retain_all_provisional", "max_bars": 1000, "max_candidates": 1000},
    "outcomes": {"schema_version": "reversal_recognition.v1"}}

_EXAMPLES["raw_forward_outcome@10"] = deepcopy(_EXAMPLES["raw_forward_outcome@9"])
_EXAMPLES["raw_forward_outcome@10"]["detector"]["reference_basis"] = "rising_leg_plus_retreat"
_EXAMPLES["raw_forward_outcome@10"]["outcomes"] = {"schema_version": "reversal_first_passage.v1"}

_EXAMPLES["raw_forward_outcome@11"] = deepcopy(_EXAMPLES["raw_forward_outcome@10"])
_EXAMPLES["raw_forward_outcome@11"]["scope"].update(start="2025-02-01T00:00:00Z", end="2025-04-26T00:00:00Z")
_EXAMPLES["raw_forward_outcome@11"]["detector"].update(computation_origin="2025-01-01T00:00:00Z", max_bars=12000, max_candidates=10000)
_EXAMPLES["raw_forward_outcome@11"]["outcomes"] = {"schema_version": "reversal_selection_only.v1"}

_EXAMPLES["raw_forward_outcome@12"] = deepcopy(_EXAMPLES["raw_forward_outcome@11"])
_EXAMPLES["raw_forward_outcome@12"]["scope"]["end"] = "2025-07-19T00:00:00Z"
_EXAMPLES["raw_forward_outcome@12"]["detector"]["max_bars"] = 19104
_EXAMPLES["raw_forward_outcome@12"]["outcomes"] = {"schema_version": "reversal_selection_only.v2"}

from .reversal_matched_outcomes_evaluator import (OUTCOMES as MATCHED_OUTCOMES,
    DETECTOR as MATCHED_DETECTOR, DATASET_ID as MATCHED_DATASET_ID, INSTRUMENT_ID as MATCHED_INSTRUMENT_ID)
_EXAMPLES["raw_forward_outcome@13"] = deepcopy(_EXAMPLES["raw_forward_outcome@12"])
_EXAMPLES["raw_forward_outcome@13"].update(mode="evidence", dataset_id=MATCHED_DATASET_ID,
    detector=deepcopy(MATCHED_DETECTOR), outcomes=deepcopy(MATCHED_OUTCOMES))
_EXAMPLES["raw_forward_outcome@13"]["scope"]["instrument_id"] = MATCHED_INSTRUMENT_ID

_EXAMPLES["raw_forward_outcome@14"] = deepcopy(_EXAMPLES["raw_forward_outcome@12"])
_EXAMPLES["raw_forward_outcome@14"]["scope"]["end"] = "2025-03-29T00:00:00Z"
_EXAMPLES["raw_forward_outcome@14"]["outcomes"] = {"schema_version":"reversal_two_block_selection.v1"}

from .reversal_matched_progress_evaluator import OUTCOMES as PROGRESS_OUTCOMES
_EXAMPLES["raw_forward_outcome@15"] = deepcopy(_EXAMPLES["raw_forward_outcome@13"])
_EXAMPLES["raw_forward_outcome@15"]["outcomes"] = deepcopy(PROGRESS_OUTCOMES)

# IDs/hashes are shape-valid examples; bind the exact retained selection receipt before use.
_EXAMPLES["raw_forward_outcome@16"] = {
    "check_family": "raw_forward_outcome", "mode": "evidence", "dataset_id": "mds_"+"a"*32,
    "scope": {"instrument_id": "instrument-1", "timeframe": "15m",
              "start": "2025-02-01T00:00:00Z", "end": "2025-03-29T00:00:00Z"},
    "detector": {"type": "saved_reversal_selection"}, "gap_policy": "reject",
    "outcomes": {"schema_version": "reversal_saved_measurements.v1",
        "selection_reference": {"source_check_id": "11111111-1111-4111-8111-111111111111",
            "result_hash": "a"*64, "evidence_hash": "b"*64, "selection_manifest_hash": "c"*64},
        "frozen_context": {"dataset_id": "mds_"+"a"*32, "dataset_hash": "a"*64,
            "input_binding_hash": "d"*64, "max_commit_seq": 10, "instrument_id": "instrument-1",
            "input_start": "2025-01-01T00:00:00Z", "input_end_exclusive": "2025-03-29T00:00:00Z",
            "decision_start": "2025-02-01T00:00:00Z", "decision_end_exclusive": "2025-03-29T00:00:00Z",
            "source_identity_key": "e"*64, "input_rows": 8352, "case_count": 2,
            "pair_count": 2, "completion_clock_count": 1}}}

_EXAMPLES["raw_forward_outcome@17"] = deepcopy(_EXAMPLES["raw_forward_outcome@14"])
_EXAMPLES["raw_forward_outcome@17"].update(mode="evidence", dataset_id="mds_"+"a"*32)
_EXAMPLES["raw_forward_outcome@17"]["scope"]["instrument_id"] = "instrument-1"
_EXAMPLES["raw_forward_outcome@17"]["outcomes"] = {"schema_version": "reversal_population_selection.v1",
    "populations": [{"name":"first_block", "start_block":0, "block_count":1},
                    {"name":"second_block", "start_block":1, "block_count":1}]}

_EXAMPLES["raw_forward_outcome@18"] = deepcopy(_EXAMPLES["raw_forward_outcome@16"])
_EXAMPLES["raw_forward_outcome@18"]["outcomes"]["schema_version"] = "reversal_saved_measurements.v2"

_PURPOSES = {
    "raw_forward_outcome@19": "Retain per-arm fractional MFE/MAE and first-attaining candle timing on the existing configurable controlled-reversal future path; unchanged matching and endpoint-return analysis.",
    "raw_forward_outcome@18": "Compose existing first-passage and progress measurements on validated fixed or configured saved populations, retaining original input, weights, support and explicit units/censoring; no reselection.",
    "raw_forward_outcome@17": "Select reversal comparisons with explicit bounded dates, symbol and reporting populations over the existing outcome-blind kernel; fixed scientific matching rules, original weights and nominal support.",
    "raw_forward_outcome@16": "Validate a saved v12/v14 selection and its exact native frozen input, then compose existing twelve-bar first-passage and progress descriptions; no recognition, rematching or target tuning.",
    "raw_forward_outcome@15": "Describe maximum original-target progress on the fixed saved BTC pairs; full-window versus pre-failure bounds, censoring, fixed quarter bins and original weights; no target optimization.",
    "raw_forward_outcome@14": "Assess the first two complete28day blocks from unchangedJan1origin;reuse formation/matching kernel and compare with the savedBTCprefix without gap resets or outcomes.",
    "raw_forward_outcome@13": "Describe twelve-bar first passages on the999saved BTC B3pairs with exact selected barriers and weights;old/new/combined views,no rematching or independent-trial inference.",
    "raw_forward_outcome@12": "Assess the fixed six-block reversal comparison extension without outcomes; report old,new,combined coverage and dependence with B2-compatible tie order and lossless compact timing.",
    "raw_forward_outcome@11": "Assess one fixed smaller-design completed-reversal comparison population without outcomes;equal completion-moment weights,all chains,explicit exclusions and dependence;no numeric coverage pass gates.",
    "raw_forward_outcome@10": "Describe fixed three-hour continuation/failure first passages after completed retreat-inclusive reversal chains,retaining every chain and outcome-blind overlap components.",
    "raw_forward_outcome@9": "Recognize provisional completed-reversal candidate chains from a fixed origin;retain ambiguity without outcome calculations or an admitted event population.",
    "event_fact_analysis@14": "Describe fixed sharp-move and thinning episodes, outcome-blind precontext controls, trade sequences, and actual availability clocks on bounded exposed inputs.",
    "raw_forward_outcome@8": "Compare raw-event volume cohorts using an explicit nearby-date radius and UTC time slot with unchanged feature calipers, support and both-arm date dependence diagnostics.",
    "raw_forward_outcome@7": "Compare configured raw-event volume cohorts with outcome-blind body/range/prior-volatility matching, retaining the full original universe and missingness.",
    "raw_forward_outcome@6": "Retain every raw-candle event and compare volume cohorts with fixed quantiles, tails, day and overlap diagnostics; preserve valid extremes.",
    "raw_forward_outcome@5": "Compare controlled reversal cohorts with explicit interval, prior windows, entry/outcome horizon and matching-time choices.",
    "event_fact_analysis@13": "Compare next-hour direction association beyond return/ATR using late-received exact-bucket numeric features; retrospective only. Optional outcomes.retrospective.input_coverage_policy=complete_paired_rows admits recorded partial custody with missing-row exclusions and fixed boundaries.",
    "raw_forward_outcome@4": "Compare bullish engulfing against matched non-engulfing candles after a fixed prior decline, with outcome-blind pairs and dependence checks.",
    "raw_forward_outcome@3": "Evaluate canonical raw candle predicates with causal close entry and contiguous bounded outcomes.",
    "raw_forward_outcome@2": "Preview raw candle conditions and subsequent outcomes.",
    "indicator_forward_outcome@2": "Preview indicator output conditions and subsequent outcomes.",
    "signal_audit@2": "Audit emitted signals against declared public output expectations.",
    "candidate_lifecycle@2": "Describe candidate progression and terminal stages.",
    "run_signal_summary@2": "Preview signal evidence from an existing run.",
    "run_decision_trade_comparison@2": "Preview run decisions and linked trade evidence.",
    "event_fact_analysis@3": "Replay the historical indicator-event/typed-Fact method.",
    "event_fact_analysis@4": "Analyze indicator events or Fact snapshots with typed Fact inputs.",
    "event_fact_analysis@5": "Analyze Fact snapshots when required Facts become available.",
    "event_fact_analysis@6": "Describe indicator events using canonical candle-only snapshots.",
    "event_fact_analysis@7": "Attribute follow-up outcomes to exact original indicator events.",
    "event_fact_analysis@8": "Compare original events at a shared classification landmark.",
    "event_fact_analysis@9": "Compare first return to the original market-profile range.",
    "event_fact_analysis@10": "Describe fixed Candle Stats ATR-expansion future risk cohorts.",
    "event_fact_analysis@11": "Compare fixed ATR crossings with matched persistent-high states.",
    "event_fact_analysis@12": "Filter the original crossing-state pairs by a fixed 5% public ATR-ratio rule before outcomes.",
}



# Descriptive field metadata only: these values never validate or execute a Check.
def _discovery(family, version):
    if family == 'raw_forward_outcome' and version in {'17','18'}:
        return {'new_requests': True, 'composition_limit': 'Method17 selection may exceed999pairs; method18 rejects the entire saved population above999pairs;no truncation or rematching.',
            'meaning': 'Supported profile admission bounds, not universal scientific limits; native15m and whole28day blocks apply to17/18 only.'}
    if family == 'raw_forward_outcome' and version in {'4','5'}:
        return {'new_requests': False, 'preferred_profile': 'raw_forward_outcome@19',
            'meaning': 'Historical output preserved;19 retains excursions and first-attaining future candle clocks with the same admitted matching choices.'}
    if family == "raw_forward_outcome" and version in {"11", "12", "14"}:
        return {"new_requests": False, "preferred_profile": "raw_forward_outcome@17",
            "meaning": "Fixed historical population profile; exact replay and explicit requests remain supported."}
    if family == "raw_forward_outcome" and version in {"13", "15", "16"}:
        return {"new_requests": False, "preferred_profile": "raw_forward_outcome@18",
            "meaning": "Historical binding/projection contract retained; use the reusable saved-population route for new supported contexts."}
    if (family == "raw_forward_outcome" and version == "2") or (family == "event_fact_analysis" and version == "3") or family == "indicator_forward_outcome":
        return {"new_requests": False, "meaning": "Historical compatibility method; select an admitted current recipe whose scientific semantics match the question. Replay remains available."}
    return {"new_requests": True, "meaning": "Use only the settings and scientific policy admitted by this method; catalog discovery never executes science."}


def _settings_fields(family: str, version: str) -> dict[str, Any]:
    from . import checks, event_fact_evaluator as event
    from research_science.check import ASSERTION_OPERATORS

    fields = {
        "scope": {"fields": ["instrument_id", "indicator_id", "indicator_param_overrides", "symbol", "timeframe", "start", "end", "warmup_bars", "datasource", "exchange"],
                  "constraints": "Bind accessible server IDs; ISO UTC start < exclusive end; warmup is a nonnegative bar count; source/indicator requirements are resolved by requirements/prepare."},
        "inputs[]": {"fields": ["alias", "fact_type", "contract_version", "dimensions", "timeframe_seconds", "max_staleness_seconds", "source_policy", "instrument_id", "alignment", "required", "allow_gaps", "series_required", "required_fields", "known_at_required", "lookback_bars", "lookback_seconds"],
                      "constraints": "Unique nonempty alias; registered Fact type/version/dimensions; positive applicable timeframe; explicit staleness for latest-known snapshot inputs."},
        "inputs[].source_policy": {"choices": ["exact", "allowlist", "current"],
            "constraints": "Evidence excludes current. Exact requires source_identity_key or provider_binding {provider,venue,source_kind,adapter_version}; allowlist requires nonempty source_identity_keys."},
        "gap_policy": {"choices": ["reject", "continue_degraded", "reset_rewarm"],
                       "constraints": "Evidence requires an explicit policy. Fact snapshots do not support reset_rewarm."},
        "gap_rewarm_bars": {"type": "integer", "minimum": 0, "default": 0},
        "assertions[]": {"fields": ["metric_path", "operator", "threshold"],
                         "operators": sorted(ASSERTION_OPERATORS), "constraints": "Nonempty result metric_path and scalar threshold; missing/null metric or incompatible threshold is indeterminate.",
                         "meaning": "Scalar result assertions; no inference or execution permission."},
    }
    if family == "event_fact_analysis":
        fields.update({
            "detector": {"choices": ["indicator_event", "fact_snapshot"] if version in {"4", "5"} else ["indicator_event"],
                "fields": {"indicator_event": ["type", "output_name", "event_keys"], "fact_snapshot": ["type", "input_alias", "sampling", "where", "evaluation_trigger"]},
                "constraints": "Indicator output_name is nonempty; event_keys are unique {key,direction,alias?}, direction long/short. Fact input_alias must resolve; sampling is primary_bar_close; where is at most eight payload.<top_level_field> -> non-null scalar equality filters. Version 5 requires fact_snapshot with required_facts_available; other methods use primary_available."},
            "outcomes": {"fields": ["horizons", "required_horizons", "horizon_kind", "primary_horizon", "entry_lag_bars", "invalidation"],
                "constraints": "Nonempty positive integer horizons; kind bars or elapsed_time (seconds), default bars. Primary defaults to first horizon and must be declared. Required horizons default to all and must be a nonempty subset. Entry lag defaults 0 and is nonnegative. Invalidation is indicator-only {type:close_crosses_event_reference,reference_path:metadata.*,max_bars:positive integer}."},
            "statistics.features": {"fields": ["baseline", "enriched"],
                "baseline_operators": sorted(event._BASELINE_OPERATORS), "enriched_operators": sorted(event._FACT_OPERATORS | event._STRUCTURED_FACT_OPERATORS),
                "constraints": "Unique names; enriched inputs require input_alias. Returns/volume_ratio require positive lookback_bars; atr_fraction period defaults 14; metadata numbers require metadata.* path and finite scale (default 1); payload numbers require one top-level payload.<field> path and at most eight non-null scalar equality filters; update windows require positive window_seconds. Fact snapshots exclude directional features/update_agreement."},
            "statistics.folds": {"fields": ["id", "train.start", "train.end", "validation.start", "validation.end"],
                "constraints": "UTC train.start < train.end <= validation.start < validation.end; walk-forward only."},
            "statistics.model": {"fields": ["type", "c", "fit_intercept", "tolerance", "max_iterations", "seed"],
                "constraints": "Only standardized_l2_logistic, with folds. c>0 (default 1), tolerance>0 (1e-9), max_iterations positive integer (default 1000); fit_intercept boolean (default true, false allowed); seed any integer (default 0); training-fold standardization."},
            "statistics.bootstrap": {"fields": ["method", "replicates", "confidence", "seed"],
                "constraints": "Only utc_day_cluster; positive replicates (default 2000), confidence strictly between 0 and 1 (0.95), integer seed (0)."},
            "statistics.direct_tests": {"fields": ["method", "multiplicity", "target"],
                "constraints": "point_biserial targets primary_binary; pearson targets primary_signed_return for events or primary_forward_return for snapshots. Only Holm multiplicity."},
            "statistics.feature_bins": {"fields": ["method", "quantiles"],
                "constraints": "Only pooled_quantiles over enriched features; distinct sorted quantiles strictly between 0 and 1, default [.25,.5,.75]; ties collapse."},
            "statistics.eligibility": {"fields": ["min_samples", "min_class_count", "min_distinct_utc_days", "min_validation_samples_per_fold", "min_valid_folds"],
                "constraints": "Nonnegative integer thresholds, default 0; eligibility is analytical completeness, not trading authority."},
            "statistics.purge_bars/embargo_bars": {"type": "integer", "minimum": 0, "default": 0},
        })
        if version in {"6", "7", "8", "9", "10", "11", "12"}:
            fields["inputs[]"] = {"fixed": [], "meaning": "Candle-only public Indicator snapshots; no additional Fact aliases."}
        if version in {"7", "8", "9"}:
            for key in list(fields):
                if key.startswith("statistics.") and key != "statistics.eligibility":
                    fields[key] = {"fixed": "empty or zero", "meaning": "Descriptive method; features/models/inference unsupported."}
            fields["statistics.eligibility"] = {"fields": ["min_samples", "min_distinct_utc_days"], "constraints": "Nonnegative, default 0; other eligibility thresholds must be zero."}
            fields["outcomes"]["constraints"] += " This method requires bar horizons."
        if version in {"7", "8"}:
            fields["outcomes.matched_origin"] = {"fields": ["detector", "origin_time_path", "origin_event_key_path", "reference_path"],
                "constraints": "Exactly these keys; distinct follow-up signal output with the same direction set; three explicit metadata.* identity/reference paths. Reference carries immutable key/source/positive price."}
        if version in {"8", "9"}:
            name = "shared_landmark" if version == "8" else "first_return"
            fields[f"outcomes.{name}"] = {"fields": ["classification_lag_bars", "sample_lag_bars", "readiness_contract", "dependence"],
                "constraints": "Integer lags 0..100, sample strictly after classification; sample_lag-entry_lag positive and smaller than every horizon; gap_policy reject. Dependence leave_one_original_profile_out.v1.",
                "readiness_contract": "market_profile.value_location.v1" if version == "8" else "market_profile.first_return_state.v2"}
            if version == "9":
                fields["detector"]["constraints"] = "balance_breakout output; event key balance_breakout_<long|short>; no matched_origin or invalidation."
        if version in {"10", "11", "12"}:
            fields = {key: value for key, value in fields.items() if key in {"scope", "assertions[]", "gap_rewarm_bars"}}
            fields["scope"]["constraints"] = "Instrument and Candle Stats IDs are bindable; parameters remain pinned. Fixed 1m, 200-bar initialization, whole-minute UTC boundaries, single calendar year, <=370 days including seed, no outcome tail."
            fields["method_configuration"] = {"fixed": "See settings.fixed; explicit forward_risk schema selects this method; statistics must be empty/zero. No alternative horizons, matching knobs or inference settings."}
        return fields
    if family == "raw_forward_outcome" and version == "14":
        fields = _settings_fields(family, "12")
        fields["scope"]["constraints"] = "Fixed2025-02-01 through2025-03-29 exclusive;first two original28dayblocks;Jan1origin;8352native15mbars;no outcomes."
        fields["outcomes"] = {"fixed":{"schema_version":"reversal_two_block_selection.v1"},"meaning":"Explicit clean-prefix scope;unchanged formation,calipers,strata,support,reuse,weights and resource namespace."}
        return fields
    if family == "raw_forward_outcome" and version == "18":
        fields = _settings_fields(family, "16")
        fields["outcomes"]["schema_version"] = "reversal_saved_measurements.v2"
        fields["outcomes"]["meaning"] = "Admits saved12/14/17 populations through the same verified binding and twelve-bar measurement kernels; at most999pairs, whole selection rejected above limit; no truncation/rematching."
        return fields
    if family == "raw_forward_outcome" and version == "17":
        return {"scope": {"constraints": "Explicit instrument/native15m dates; one through six complete28day decision blocks; origin-to-end at most19104bars; evidence-only frozen Dataset."},
            "detector": {"meaning": "Existing retreat-inclusive recognizer; retain all chains; fixed19104bar identity namespace and10000candidate bound; explicit computation_origin."},
            "outcomes": {"schema_version": "reversal_population_selection.v1", "populations": "At most two named block views plus combined; these change reporting only, not pairs or root weights.",
                "meaning": "Selection only; no outcomes, thresholds, models or inference."}}
    if family == "raw_forward_outcome" and version == "16":
        return {"scope": {"constraints": "Exact saved v12/v14 decision scope and original frozen native15m Dataset; evidence mode only; no tail."},
            "detector": {"fixed": {"type": "saved_reversal_selection"}},
            "outcomes": {"schema_version": "reversal_saved_measurements.v1",
                "selection_reference": "Exact Check/result/evidence/selection-manifest hashes.",
                "frozen_context": "Declared Dataset/hash/watermark/source/instrument/ranges/counts, all verified against typed saved evidence at execution.",
                "meaning": "Fixed12bar geometry, original pairs/weights and shared kernels; no outcome reference fabricated for in-memory composition."}}
    if family == "raw_forward_outcome" and version == "15":
        fields = _settings_fields(family, "13")
        fields["outcomes"] = {"fixed": deepcopy(PROGRESS_OUTCOMES), "meaning": "Exact saved selection and first-passage references; fixed twelve-bar descriptive progress; no new target or selection."}
        return fields
    if family == "raw_forward_outcome" and version == "13":
        return {"scope": {"constraints": "Exact saved BTC B3 scope and original frozen Dataset; evidence mode only."},
            "detector": {"fixed": deepcopy(MATCHED_DETECTOR)},
            "outcomes": {"fixed": deepcopy(MATCHED_OUTCOMES), "meaning": "Consume saved pairs; twelve-bar symmetric first passage; unresolved retained; no rematching or inference."}}
    if family == "raw_forward_outcome" and version == "12":
        fields = _settings_fields(family, "11")
        fields["scope"]["constraints"] = "Fixed2025-02-01 through2025-07-19 exclusive;six28day blocks;15m;origin2025-01-01;no outcome tail."
        fields["detector"]["constraints"] = "Retreat-inclusive,retain all,no expiry;max_bars=19104,max_candidates=10000. Actual event IDs retained;B2 namespace key preserves same-clock ordering."
        fields["outcomes"] = {"fixed": {"schema_version": "reversal_selection_only.v2"}, "meaning": "Selection only;old/new/combined summaries;unchanged matching rules and no coverage pass gate."}
        return fields
    if family == "raw_forward_outcome" and version == "11":
        fields = _settings_fields(family, "9")
        fields["scope"]["constraints"] = "Fixed84evaluation days in three28day blocks;15m;explicit aligned origin;<=12000input bars;no tail read."
        fields["detector"]["constraints"] = "Fixed retreat-inclusive recognition,retain all chains,no expiry;max_bars<=12000,max_candidates<=10000."
        fields["outcomes"] = {"fixed": {"schema_version": "reversal_selection_only.v1"}, "meaning": "Selection only;no outcome computation. Fixed smaller matching set,within-pair disjoint histories,equal completion-moment weighting,no coverage pass gate."}
        return fields
    if family == "raw_forward_outcome" and version == "10":
        fields = _settings_fields(family, "9")
        fields["detector"]["constraints"] = "Fixed retreat-inclusive reference;retain all chains;origin explicit;15m;integer max_bars/max_candidates<=4096. No size/expiry/winner selection."
        fields["outcomes"] = {"fixed": {"schema_version": "reversal_first_passage.v1"},
            "meaning": "Fixed12bar first passage from completion-close observational anchor;failure formation low;equal-risk continuation;both-touch unresolved;no tail or inference."}
        return fields
    if family == "raw_forward_outcome" and version == "9":
        return {"scope": {"fields": ["instrument_id", "timeframe", "start", "end", "warmup_bars"],
            "constraints": "UTC aligned;origin<=start<end;evaluation<=32days;warmup equals origin distance;no Indicator."},
            "detector": {"fields": list(_EXAMPLES["raw_forward_outcome@9"]["detector"]),
                "constraints": "Explicit fixed computation_origin,reference_basis rising_leg_only/rising_leg_plus_retreat,ownership retain_all_provisional,integer resource bounds. No minimum size or expiry policy."},
            "outcomes": {"fixed": {"schema_version": "reversal_recognition.v1"}, "meaning": "Recognition only;no forward outcomes."},
            "gap_policy": {"fixed": "reject"}, "statistics": {"fixed": {}},
            "assertions[]": {"fixed": []}, "gap_rewarm_bars": {"fixed": 0}}
    if family == "raw_forward_outcome" and version in {"5", "19"}:
        fields["detector"] = {"fixed":{"type":"controlled_bullish_reversal"}}
        fields["inputs[]"] = {"fixed":[]}
        fields["statistics"] = {"fixed":{}}
        fields["gap_policy"] = {"fixed":"continue_degraded"}
        fields["gap_rewarm_bars"] = {"fixed":0}
        fields["outcomes"] = {"fields": list(_EXAMPLES["raw_forward_outcome@5"]["outcomes"]),
            "constraints": "All choices explicit. Interval 1m/15m/30m/1h equals scope; trend <= volatility; integers 1..10000; lag+horizon <=10000. Same UTC close slot radius 1..31 days or same date/hour radius 0 with room for two disjoint closed paths. UTC aligned evaluation <=32 days; warmup exactly volatility+1."}
    if family == "event_fact_analysis" and version == "13":
        fields["scope"]["constraints"] = "Hourly UTC sample <=60 days; no Indicator. Price outcomes use the next close, including the declared one-hour preparation tail."
        fields["statistics.features"] = {"baseline_operators":["lagged_return","atr_fraction"],
            "enriched_operators":["latest_value"], "constraints":"Exactly one-bar recent return and ATR14 baseline; one or two untransformed exact numeric features. Inputs use exact_interval and dimensions.bucket_seconds=3600; real source clocks remain required."}
        fields["statistics.folds"]["constraints"] = "Exactly one chronological train/validation partition within the sample. Retrospective analytical comparison; no historical availability or fresh-holdout claim."
        fields["statistics.purge_bars/embargo_bars"] = {"fixed":{"purge_bars":1,"embargo_bars":0}}
        for name in ("bootstrap","direct_tests","feature_bins"):
            fields["statistics."+name] = {"fixed":{}, "meaning":"No additional inference or comparisons in this bounded contract."}
        fields["gap_rewarm_bars"] = {"fixed":0}
        fields["detector"] = {"fixed": {"type":"fact_snapshot","sampling":"primary_bar_close"}, "constraints":"Unconditional primary-close samples; detector alias names one numeric input, not causal Fact-driven event emission."}
        fields["outcomes.retrospective"] = {"fields": ["schema_version", "analysis_as_of", "bucket_time_interpretation"],
            "constraints": "Explicit retrospective_numeric_comparison.v1; real cutoff >= sample end; bucket_start_unverified. Hourly sample <=60 days, next-hour only, exact bucket inputs, recent return/ATR14 baseline, 1..2 numeric features, one fixed logistic fold."}
    if family == "event_fact_analysis" and version == "14":
        return {"scope":{"fields":["instrument_id","timeframe","start","end"],
            "constraints":"Aligned 1m source; 5..30 minute range. No warmup, Indicator or model."},
            "detector":{"fixed":{"type":"liquidity_episode"}},
            "outcomes":{"fields":["schema_version","analysis_as_of"],"constraints":"Explicit liquidity_episode.v1 and real analysis cutoff >=end; fixed sharp10bps/60s and 50% thinning, 5bps depth, +/-120s paths, 300s refractory, first10 per origin."},
            "inputs[]":{"fields":["alias","fact_type","contract_version","timeframe_seconds","source_policy","alignment","max_staleness_seconds","allow_gaps"],"constraints":"Exact bbo/depth/flow/trades aliases and canonical contracts; complete second paths; no gap fill."},
            "gap_policy":{"fixed":"continue_degraded"}}
    if family == "raw_forward_outcome" and version == "19":
        fields["outcomes"]["schema_version"] = "controlled_reversal_comparison.v3"
    if family == "raw_forward_outcome" and version == "4":
        return {"scope": {"fields": ["instrument_id", "symbol", "timeframe", "start", "end", "warmup_bars", "datasource", "exchange"],
            "constraints": "Fixed 1m, aligned UTC minute boundaries, nonempty range <=32 days; 21 warmup candles when specified. No Indicator or additional Fact inputs."},
            "method_configuration": {"fixed": "Explicit controlled_reversal_comparison.v1 and controlled_bullish_reversal detector. See settings.fixed for all matching/support rules. No configurable thresholds, horizons, statistics or rewarm."},
            "gap_policy": {"fixed": "continue_degraded"}, "assertions[]": fields["assertions[]"]}
    operators = sorted(checks._DETECTOR_OPERATORS)
    if family == "raw_forward_outcome":
        fields["detector"] = {"fields": ["type", "field", "operator", "value", "value_field", "all", "any", "not"], "field_choices": sorted(checks._RAW_DETECTOR_FIELDS), "operators": operators,
            "constraints": "raw_condition with scalar value or another declared candle field; all/any lists and not compose conditions."}
    elif family == "indicator_forward_outcome":
        fields["detector"] = {"fields": ["type", "output_name", "field", "operator", "value", "value_field", "event_key", "symbol", "direction", "all", "any", "not"],
            "choices": sorted(checks._INDICATOR_DETECTOR_TYPES), "operators": operators,
            "constraints": "Output name required except record_match; output-match requires field and value/value_field unless is_true/true; event matches filter public emitted records."}
    elif family == "signal_audit":
        fields["detector.expectations[]"] = {"fields": ["name", "expectation_type", "source_output", "source_field", "signal_output", "event_key", "operator", "value", "value_field", "from", "to", "same_group_by", "record_excluded_candidates", "require_contiguous_source_rows"],
            "operators": operators, "constraints": "type signal_audit; nonempty expectations (or one flattened expectation); condition needs value/value_field except true operators; transition needs from/to. Group fields are string lists; excluded/contiguous flags default true."}
    elif family == "candidate_lifecycle":
        fields["detector"] = {"fields": ["type", "funnel_stages", "terminal_stages", "signal_stages", *sorted(checks._CANDIDATE_LIFECYCLE_FILTER_FIELDS)],
            "constraints": "type candidate_lifecycle; stages are string lists, filter values match recorded lifecycle fields; unknown detector fields rejected."}
    else:
        fields["scope.run_id"] = {"type": "string", "meaning": "Accessible existing run; supported run evidence is loaded by the existing adapter."}
        fields["detector"] = {"choices": sorted(checks._RUN_DETECTOR_TYPES_BY_FAMILY[family]),
            "fields": ["output_name", "event_key", "symbol", "direction", "decision_state", "reason_code", "linked_trade_count"],
            "constraints": "Record filters over existing run evidence; no new market calculation or future reconstruction."}
    if family in {"raw_forward_outcome", "indicator_forward_outcome"}:
        fields["outcomes"] = {"fields": ["forward_bars", "bars", "entry_lag_bars", "direction", "min_sample_count", "min_edge_pct", "max_examples"],
            "defaults": {"forward_bars": [1, 3, 5, 10], "entry_lag_bars": 0, "direction": "long", "min_sample_count": 20, "min_edge_pct": 0.0, "max_examples": 250 if family == "raw_forward_outcome" else 100},
            "constraints": "forward_bars (bars alias) accepts integer/list/comma-separated integers; positive values are deduplicated/sorted, at least one required. Entry lag nonnegative; direction long/short. Sample/edge thresholds are descriptive screening, examples are capped."}
    elif family in {"signal_audit", "candidate_lifecycle"}:
        fields["outcomes"] = {"fields": ["max_examples"], "defaults": {"max_examples": 100}, "meaning": "Issue examples only; audit/lifecycle counts are owned by the method, not predictive return settings."}
    else:
        fields["outcomes"] = {"fields": ["min_sample_count", "max_examples"] + (["bucket_by", "buckets"] if family == "run_signal_summary" else []),
            "defaults": {"min_sample_count": 5, "max_examples": 100}, "constraints": "Sample threshold is descriptive; example cap bounds reporting. Signal-summary bucket_by (buckets alias) lists recorded fields, default symbol/output_name/event_key."}
    if family == "raw_forward_outcome" and version == "8":
        fields["outcomes"] = {"fields":list(_EXAMPLES["raw_forward_outcome@8"]["outcomes"]),
            "constraints":"Explicit volume_cohort_matched.v2; v7 raw/cohort/outcome constraints; comparison adds required day radius 1..31 and UTC close slot 60 or 3600 seconds. Same-slot controls within inclusive elapsed radius and requested input only. Prospective choices, no tuning or inferred acquisition."}
    if family == "raw_forward_outcome" and version == "7":
        fields["outcomes"] = {"fields":list(_EXAMPLES["raw_forward_outcome@7"]["outcomes"]),
            "constraints":"Explicit volume_cohort_matched.v1; single horizon; raw detector, interval, direction and lag supplied by request. comparison explicitly chooses supported volume cohort, prior candle count, per-feature ratio calipers and matching policy. One-bar binding; unavailable prior context retained. No outcome-driven tuning or replay."}
    if family == "raw_forward_outcome" and version == "6":
        fields["outcomes"] = {"fields": list(_EXAMPLES["raw_forward_outcome@6"]["outcomes"]),
            "constraints": "Explicit raw_candle_distribution.v1; raw v3 close/outcome rules; both volume fields required; one warmup bar; complete table <=10000 events; fixed diagnostic rules, no tail removal."}
    if family == "raw_forward_outcome" and version == "3":
        fields["outcomes"] = {"fields": ["schema_version", "forward_bars", "entry_lag_bars", "direction",
            "min_sample_count", "min_edge_pct", "max_examples", "outcome_boundary"],
            "constraints": "Explicit raw_candle_forward_outcome.v1; 1..20 positive integer horizons, total lag+horizon<=10000 bars; canonical close entry and target close strictly before evaluation end; continue_degraded with local exclusions. Returns are fractions despite _pct names."}
    return fields


_PAIRED_SUPPORT = {
    "overall_matching": {"minimum": .70, "unit": "fraction", "population": "feature-frozen eligible crossings"},
    "every_month_matching": {"minimum": .50, "unit": "fraction", "population": "each of all 12 UTC months; empty month fails"},
    "outcome_completeness": {"minimum": .95, "unit": "fraction", "population": "frozen pairs with both future outcomes"},
    "complete_pairs": {"minimum": 200, "unit": "pairs"},
    "every_month_pairs": {"minimum": 10, "unit": "complete pairs per UTC month"},
    "current_z_balance": {"maximum": .10, "unit": "absolute standardized mean difference", "undefined_fails": True},
    "log_prior_rms_balance": {"maximum": .10, "unit": "absolute standardized mean difference", "undefined_fails": True},
}


def _analytical_fields(family: str, version: str) -> dict[str, Any]:
    fields = {
        "status": {"type": "string", "meaning": "Operation completion; not statistical validity."},
        "sample_count": {"type": "integer", "unit": "method-specific observations", "meaning": "Inspect method population and exclusions; not effective independent sample size."},
        "data_quality": {"type": "object", "meaning": "Resolved input/gap evidence, not a result certification."},
    }
    if family == "event_fact_analysis" and version == "14":
        fields["episodes[]"] = {"type":"array","meaning":"Complete second sequences, raw trade traces and immutable known-at references; precontext controls; thinning forward move and no-move outcomes."}
        fields["support"] = {"type":"object","meaning":"Joined seconds, path/quality exclusions, pre-context-selected matches and separate control support states; no minimum significance or power claim."}
        fields["dependence"] = {"type":"object","meaning":"Unique/shared event anchors, cross-origin reused controls and coalesced inclusive-window overlaps/components; not independent trial counts."}
        return fields
    if family == "raw_forward_outcome" and version in {"7", "8"}:
        fields["events[]"] = {"type":"array", "meaning":"All original detector events, outcomes and matching context/missingness, including unmatched cases."}
        fields["volume_cohort_comparison"] = {"type":"object", "unit":"fractional returns", "meaning":"Frozen pair contrast, selected/complete balance, support gates, complete-pair tails, day deletions and joint nonoverlap sensitivity; descriptive only."}
        fields["dependence"] = {"type":"object", "meaning":"Closed outcome and full-feature overlap components across original events and both selected roles; not independent N."}
        return fields
    if family == "raw_forward_outcome" and version == "6":
        fields["events[]"] = {"type":"array","meaning":"Every event with volume cohort, context and overlap IDs; no truncation."}
        fields["distribution"] = {"type":"object","meaning":"Fixed quantiles, full-denominator tail contributions, daily sums and influence diagnostics. Returns are fractions."}
        return fields
    if family == "event_fact_analysis" and version in {"11", "12"}:
        summary = {
            "pair_count": {"type": "integer", "unit": "complete pairs"},
            "raw_risk_contrast": {"type": "number or null", "unit": "mean squared one-minute log return", "meaning": "Mean within-pair crossing minus persistent-high future 120-minute risk; not price return."},
            "crossing_mean/persistent_high_mean": {"type": "number or null", "unit": "mean squared one-minute log return", "meaning": "Arm means over complete frozen pairs."},
            "ratio_contrast": {"type": "number or null", "unit": "dimensionless", "meaning": "Mean within-pair difference of future-risk/prior-risk ratios, using each arm's own prior risk."},
            "relative_raw_contrast": {"type": "number or null", "unit": "fraction", "meaning": "Raw contrast divided by persistent-high mean; null if denominator is zero."},
        }
        fields["crossing_state_comparison"] = {
            "type": "object", "schema_version": "crossing_state_matched_pairs.v1",
            "fields": {
                "analysis_status": {"choices": ["descriptive_only", "unsupported"], "meaning": "Unsupported if any fixed support gate fails, even though computation completed."},
                "primary": {"type": "object", "fields": summary},
                "monthly[]": {"type": "array", "fields": {**summary, "month": "YYYY-MM", "eligible_crossings/matched_pairs/complete_pairs/dropped_whole_pairs": "integer counts", "fraction": "matched/eligible or null", "crossing_outcome_reasons/control_outcome_reasons": "reason -> count"}},
                "matching": {"type": "object", "fields": {"eligible_crossings/eligible_controls/matched_pairs/unmatched_crossings": "integer counts", "match_fraction": "matched/eligible or null", "unmatched_crossing_identities[]": "{source_open_epoch,decision_known_at_epoch,reason}; UTC epoch seconds; exact unpaired identities", "monthly/strata/current_z_support": "feature-only support count rows", "unmatched_current_z/unmatched_prior_rms_bps": "count/mean/minimum/maximum distributions; z units / basis points", "feature_pair_map_sha256_json_lines": "outcome-blind frozen pair identity digest", "matching_uses_future_outcomes": "false"}},
                "outcome_missingness": {"type": "object", "fields": {"dropped_whole_pairs": "integer count", "complete_fraction": "complete/frozen or null", "crossing_reasons/control_reasons": "reason -> count", "rematched": "false; missing either outcome drops the whole pair without rematching"}},
                "balance": {"type": "object", "fields": {"current_atr_zscore/log_prior_rms/atr_ratio_unmatched_diagnostic": "{pair_count,absolute_smd,crossing_mean,control_mean,pooled_population_sd?,undefined_zero_sd?}; absolute_smd dimensionless; undefined unequal zero-SD fails support"}},
                "support_gates": {"type": "object of booleans", "thresholds": deepcopy(_PAIRED_SUPPORT), "meaning": "Fixed support/completeness gates, not significance or causality."},
                "joint_nonoverlap": {"type": "object", "fields": {**summary, "retained_months/rejected_pairs": "integer counts", "interpretation": "joint interval sensitivity, not independent observations"}, "support": {"minimum_pairs": 50, "minimum_months": 6}},
                "leave_one_day_out[]/leave_one_week_out[]": {"type": "arrays", "fields": {**summary, "removed": "UTC period label", "removed_pairs": "whole pairs removed if either arm is in period"}},
                "dependence": {"type": "object", "meaning": "Interval overlap, distinct episodes, interval union/reuse seconds and calendar pair-membership contributions; not effective sample size."},
                "pairs[]": {"type": "array", "meaning": "Frozen crossing/persistent_high arms: source/decision/entry/target epochs (UTC seconds), episode, z, prior_rms_bps, raw_risk (or null), outcome_known_at_epoch/exclusion and both_outcomes_complete."},
                "descriptive_investment_gates": {"type": "object of booleans", "constraints": "Primary raw contrast >0; >=9 positive UTC months; all week-deletion contrasts positive; supported joint nonoverlap (>=50 pairs in >=6 months) with contrast >0."},
                "per_asset_investment_criteria_met": {"type": "boolean", "meaning": "All support and descriptive gates; requires both prespecified assets; never promotion/execution authority."},
            },
        }
        if version == "12":
            comparison = fields["crossing_state_comparison"]
            comparison["schema_version"] = "crossing_state_matched_pairs.v2"
            detail = comparison["fields"]
            detail["support_gates"]["thresholds"].update({
                "overall_atr_filter_retention": {"minimum": .70, "unit": "fraction", "population": "accepted ATR pairs / original matched pairs; zero denominator fails"},
                "every_month_atr_filter_retention": {"minimum": .50, "unit": "fraction", "population": "each of all 12 UTC months; accepted / original matched; zero denominator fails"},
                "atr_ratio_balance": {"maximum": .10, "unit": "absolute standardized mean difference", "population": "complete filtered pairs; raw public ATR ratio", "undefined_fails": True},
            })
            detail["support_gates"]["thresholds"]["outcome_completeness"]["population"] = "both outcomes complete / ATR-accepted pairs"
            detail["balance"]["fields"] = {"current_atr_zscore/log_prior_rms/atr_ratio": "{pair_count,absolute_smd,crossing_mean,control_mean,pooled_population_sd?,undefined_zero_sd?}; complete filtered pairs; equal zero-SD means give zero, unequal fail"}
            detail["atr_ratio_filter"] = {"type": "object", "schema_version": "crossing_state_atr_ratio_filter.v1",
                "fields": {"original_matched_pairs/accepted_pairs/excluded_pairs": "integer counts before outcomes",
                    "retention_fraction": "accepted / original matched; null if empty",
                    "accepted_eligible_fraction": "accepted / original eligible crossings; separate from original matching coverage",
                    "original_pair_map_sha256_json_lines/accepted_pair_map_sha256_json_lines": "original and filtered outcome-blind pair identity digests",
                    "reason_counts": "disjoint accepted/missing/invalid_type/nonfinite/nonpositive/above_1_05 counts; same fixed exclusion precedence",
                    "monthly[]": "all 12 months with original matched, accepted, excluded, retention and accepted/eligible coverage",
                    "pairs[]": "every original pair in order with original ordinal, both source/decision epochs, per-arm ATR ratio or null, per-arm exclusion flags, symmetric quotient or null on overflow, accepted and primary reason; no outcomes",
                    "filter_uses_future_outcomes/rematched": "false"}}
            detail["monthly[]"]["fields"].update({"atr_accepted_pairs/atr_excluded_pairs": "integer counts before outcomes",
                "atr_retention_fraction": "accepted / original matched", "atr_accepted_eligible_fraction": "accepted / original eligible"})
            detail["pairs[]"]["meaning"] += " Only ATR-accepted pairs; both arms include their positive public atr_ratio."
            detail["outcome_missingness"]["fields"]["complete_fraction"] = "complete / ATR-accepted or null"
        return fields
    if family == "event_fact_analysis" and version == "10":
        fields["forward_risk_comparison"] = {"type": "object", "fields": {"coverage": "expected/observed/observable minute counts; undetectable intervals and exclusions, unknown event count remains null", "horizons": "seconds -> cohorts ordinary/shock_crossing/persistent_high: counts, raw_risk distributions (mean squared log return), future_prior_risk_ratio (dimensionless), path-range distributions and unresolved reasons", "months": "UTC monthly versions of cohort comparisons", "observation_ledger": "ordered identity digest and bounded examples, not full row persistence", "interpretation": "descriptive association; overlapping outcomes are not independent"}}
    elif family == "event_fact_analysis" and version in {"7", "8", "9"}:
        name = {"7": "matched_origin_attribution", "8": "shared_landmark_comparison", "9": "first_return_comparison"}[version]
        fields[name] = {"type": "object", "fields": {"origins": "identity/clock/classification/outcome rows; unresolved reasons preserved", "horizons": "declared horizon -> group count/distributions, contrasts, exclusions and sensitivity"},
            "units": "ATR-normalized original-profile distance reduction" if version == "9" else "direction-signed fractional price return",
            "meaning": "Returned minus outside groups" if version == "9" else "Fixed-landmark confirmed minus complete-observation complement; profile-deletion/overlap sensitivity" if version == "8" else "Selection, entry-repricing, endpoint-extension and unmatched-followup decomposition of observed mean difference; no causal attribution"}
    elif family == "event_fact_analysis":
        fields.update({"events[]": {"type": "array", "meaning": "Decision-known event/snapshot identities, features, horizon outcomes and exclusions."},
            "outcome_resolution": {"type": "object", "meaning": "Per-horizon resolved/unresolved counts and exclusion reasons."},
            "statistics": {"type": "object", "meaning": "Baseline/enriched summaries, declared tests, model folds and sensitivity; result hashes pin exact configuration.", "units": "Fractional price returns, binary outcomes, feature-native units and dimensionless statistical scores."},
            "eligibility": {"type": "object", "meaning": "Criteria/reasons and population versus complete-case counts; not automatic Observation admission."}})
    elif family == "signal_audit":
        fields["outcomes.summary"] = {"type": "object", "unit": "signal counts", "meaning": "Expected/emitted/missing/invalid/excluded counts; events carry reconciliation issues."}
    elif family == "candidate_lifecycle":
        fields["outcomes"] = {"type": "object", "unit": "candidate/event counts", "meaning": "Funnel, stage/status/terminal/reason counts and lifecycle-contract issues; not predictive outcomes."}
    elif family == "run_signal_summary":
        fields["outcomes"] = {"type": "object", "unit": "record counts", "meaning": "Declared buckets, decision states and linked trade counts from existing run evidence."}
    elif family == "raw_forward_outcome" and version in {"16", "18"}:
        fields["sample_count"] = {"type": "integer", "meaning": "Matched completion clocks, not independent trials."}
        fields["pairs"] = {"type": "array", "meaning": "Original pair identities/weights, both arms' first passages and full-window/pre-failure progress; censoring retained."}
        fields["measurements"] = {"type": "object", "meaning": "Whole saved-scope class fractions and progress distributions with original mass, fixed quarter bins and quantiles."}
        fields["frozen_context"] = {"type": "object", "meaning": "Context derived from validated immutable saved selection evidence and matched to actual frozen execution input."}
        fields["saved_dependence"] = {"type": "object", "meaning": "Unchanged saved support/ancestry components; no independence claim."}
        if version == "18":
            fields["measurements_by_population"] = {"type": "object", "meaning": "Saved reporting populations use original pair weights, explicit scope, and the same first-passage/progress summary functions; no rematching or recalculation."}
    elif family == "raw_forward_outcome" and version == "15":
        fields["sample_count"] = {"type": "integer", "meaning": "Matched completion clocks; not independent trials."}
        fields["pairs"] = {"type": "array", "meaning": "Saved identities, weights and original classes with full-window maxima and conservative pre-failure bounds; censored paths retained."}
        fields["progress_comparison"] = {"type": "object", "meaning": "Old/new/combined formations and controls, all and originally neither; original-weight quantiles, fixed quarter bins and explicit missing mass."}
    elif family == "raw_forward_outcome" and version == "13":
        fields["sample_count"] = {"type": "integer", "meaning": "Matched completion moments; not independent trials."}
        fields["pairs"] = {"type": "array", "meaning": "Saved pair identities and weights; both arms retain first-passage classes and unresolved reasons."}
        fields["matched_outcome_comparison"] = {"type": "object", "meaning": "Old,new,combined four-class weighted fractions and case-minus-control differences; no inference."}
        fields["outcomes_calculated"] = {"fixed": True}
    elif family == "raw_forward_outcome" and version in {"11", "12", "14", "17"}:
        fields["sample_count"] = {"type": "integer", "meaning": "Matched completion clocks;not independent observations."}
        fields["cases"] = {"type": "array", "meaning": "Every chain retained,explicit selection status,features,weights and versioned availability view."}
        fields["pairs"] = {"type": "array", "meaning": "Deterministic control choices and support;no forward outcomes."}
        fields["selection_summary"] = {"type": "object", "meaning": "All versus matched coverage,partial moments,context shifts and exclusions;no numeric scientific pass gate."}
        fields["outcomes_calculated"] = {"fixed": False}
        if version in {"12", "14", "17"}:
            fields["populations"] = {"type": "object", "meaning": "Caller-named views over1through6whole28day blocks plus combined;original root weights retained;coverage/context and direct/ancestry dependence normalized within each view." if version == "17" else "Block1,block2,combined clean-prefix populations;separately normalized coverage/context and direct/ancestry dependence." if version == "14" else "Old first3blocks,new last3blocks,combined;separately normalized coverage/context and direct/ancestry dependence."}
            fields["compatibility"] = {"type": "object", "meaning": "B2 namespace ordering projection;actual event identities and historical results preserved."}
    elif family == "raw_forward_outcome" and version == "10":
        fields["sample_count"] = {"type": "integer", "meaning": "Transitive overlap components;not independent observations."}
        fields["events"] = {"type": "array", "meaning": "All source-linked completed chains with fixed first-passage class,barriers,availability and unresolved reason."}
        fields["first_passage_summary"] = {"type": "object", "meaning": "All four classes;chain and equal-component shares;fixed overlap map;UTC day counts;no inference."}
    elif family == "raw_forward_outcome" and version == "9":
        fields["sample_count"] = {"fixed": 0, "meaning": "No admitted Study event population."}
        fields["candidate_count"] = {"type": "integer", "unit": "candidate chains", "meaning": "Completion close in [start,end);overlaps retained;not independent events."}
        fields["pending_candidates"] = {"type": "array", "meaning": "Incomplete contextual chains as of snapshot_known_at,including the final input close at evaluation end."}
        fields["events"] = {"type": "array", "meaning": "Typed provisional candidate chains with source-event links,reference price/role clocks and stages;not independent Study events."}
        fields["reference_events"] = {"type": "array", "meaning": "Causally confirmed rebound references,including prior context."}
        fields["simultaneous_candidate_groups"] = {"type": "array", "meaning": "Overlapping candidates retained;no implicit winner or deduplication."}
        fields["outcomes_calculated"] = {"fixed": False}
        fields["study_events_admitted"] = {"fixed": 0}
    elif family == "raw_forward_outcome" and version in {"4", "5", "19"}:
        fields["controlled_reversal_comparison"] = {"type": "object", "unit": "fractional returns, pairs and UTC days",
            "meaning": "Paired mean contrast, arm distributions, population-SMD balance, support gates, day deletion and feature-frozen joint nonoverlap. Unsupported/mixed/nonpositive/positive_descriptive_lead; no causal or inference claim."}
        fields["pairs"] = {"type": "array", "meaning": "Complete frozen identities/features/clocks, fixed subset membership and both outcome resolutions; whole-pair missingness never rematches."}
        fields["feature_frozen_hash"] = {"type": "string", "meaning": "Hash of all feature candidates, matches and unmatched identities before forward resolution."}
        if version == '19':
            fields['pairs[].outcomes.{treated,control}'] = {'type': 'object', 'unit': 'fraction',
                'meaning': 'Status/reason, long entry-close anchor and declared horizon;unclamped MFE/MAE with first chronological attaining future candle offset/open/close/known-at;entry candle excluded;unresolved metrics null;no intrabar ordering or fills.'}
            fields['path_observation_contract'] = {'type': 'object', 'meaning': 'Explicit units, sign, path membership, first-tie, availability and unresolved semantics.'}

    elif family == "run_decision_trade_comparison":
        fields["outcomes.by_decision_state"] = {"type": "object", "unit": "counts and run-reported PnL currency", "meaning": "Trade count/net PnL/average per trade grouped by decision state; descriptive run summary."}
    else:
        fields["outcomes.summary"] = {"type": "object", "unit": "fractional price returns and counts", "meaning": "Per-forward-bar event versus baseline distributions; events are bounded examples; recommendation is descriptive screening."}
    if family == "event_fact_analysis" and version == "13":
        fields["events[]"]["meaning"] = "Historical primary-close sample identities; actual late Fact references, outcomes, features and exclusions. No historical decisions."
        fields["evidence_class"] = {"fixed":"retrospective_conditional_association", "meaning":"No PIT predictive, backtest-certified or scientific-certification claims."}
    return fields


def get_check_definition(definition_id: str, version: str) -> dict[str, Any]:
    """Inspect one registered base version; never select a latest version implicitly."""
    try:
        definition = CHECK_REGISTRY.resolve_definition(definition_id, version)
    except ValueError as exc:
        raise KeyError(f"check_definition_not_found: id={definition_id} version={version}") from exc
    key = f"{definition.definition_id}@{definition.definition_version}"
    if key not in _PURPOSES:
        raise ValueError(f"check_catalog_description_missing: {key}")
    evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
    example = deepcopy(_EXAMPLES.get(key))
    declarations = None
    configured = None
    if example is not None:
        configured, request = normalize_check_request(example)
        if (configured.definition_id != definition.definition_id
                or configured.definition_version.split("+", 1)[0] != definition.definition_version):
            raise ValueError(f"check_catalog_example_version_mismatch: {key}")
        declarations = dict(evaluator.declare_requirements(definition=configured, request=request))
    fixed_risk = definition.definition_id == "event_fact_analysis" and version in {"10", "11", "12"}
    settings = {
        "fixed": deepcopy(dict(definition.material_rules)),
        "configurable": {
            "scope": "Authorized instrument/indicator IDs, timeframe and bounded UTC range supported by this method.",
            "detector": "Only operators/event fields accepted by the existing method normalizer.",
            "outcomes": "Only horizons/outcome fields accepted by the existing method normalizer.",
            "statistics": "Only statistics accepted by the existing method normalizer.",
            "assertions": "Existing scalar assertions; no trading or execution permission.",
        },
        "defaults": {"mode": "preview", "gap_rewarm_bars": 0},
        "constraints": ["Existing request normalizers and requirements/prepare are authoritative.",
                        "Examples are templates, not existing datasets or execution approval."],
    }
    if fixed_risk:
        settings["fixed"].update({
            "indicator_type": "candle_stats", "indicator_parameters": dict(PINNED_PARAMS),
            "timeframe": "1m", "warmup_bars": 200, "gap_policy": "reset_rewarm",
            "detector": deepcopy(example["detector"]), "outcomes": deepcopy(example["outcomes"]),
            "statistics": {},
        })
        settings["configurable"] = {"scope.instrument_id": "Authorized instrument identity.",
                                     "scope.indicator_id": "Existing Candle Stats instance with pinned defaults.",
                                     "scope.start/end": "Aligned UTC range within one calendar year; no outcome tail."}
        settings["constraints"] += ["No configurable inference, matching thresholds or alternate indicators.",
                                     "Descriptive association only; not causality, profitability or promotion."]
        if version in {"11", "12"}:
            settings["fixed"]["matching"] = {"z_caliper": 0.25, "max_prior_rms_ratio": 1.25,
                "calendar_window_days": 7, "primary_horizon_seconds": 7200,
                "control_reuse": False, "same_episode": False,
                "strata": "UTC month x six-hour block x prior RMS bin"}
            settings["constraints"].append("At most 20,000,000 candidate comparisons; result-size and execution budgets still apply.")
    if fixed_risk and version == "12":
        settings["fixed"]["atr_ratio_filter"] = {
            "contract": "crossing_state_atr_ratio_filter.v1", "max_min_ratio_max": 1.05,
            "numeric_semantics": "public binary64 quotient <= 1.05 inclusive; no epsilon or rounding",
            "positive_finite_arms_required": True, "rematch": False,
            "sequence": "original feature map -> whole-pair ATR filter -> future outcomes",
            "support": {"original_matching": "70% overall / 50% each month of eligible crossings",
                "filter_retention": "70% overall / 50% each month of original matched pairs",
                "complete_pairs": "95% of filtered pairs; >=200 total and >=10 each month",
                "balance": "absolute SMD <=0.10 for current z, log prior RMS and raw ATR ratio in complete filtered pairs"},
        }
    settings["configurable"] = _settings_fields(definition.definition_id, version)
    durable = definition_allows_evidence(definition)
    return {
        "schema_version": SCHEMA,
        "definition": definition.to_dict(),
        "purpose": _PURPOSES[key],
        "eligibility": {"request_selectable": example is not None,
                        "modes": ["preview", "evidence"] if durable and example is not None else ["preview"] if example is not None else [],
                        "observation": "Requires completed, replayable durable evidence and explicit admission; never automatic." if durable else "Preview only; cannot create new durable evidence or Observations.",
                        "execution_authority": False, "promotion_authority": False,
                        **({"point_in_time_predictive":False,"scientific_certification_eligible":False,"backtest_certified":False}
                           if (definition.definition_id == "event_fact_analysis" and version in {"13", "14"})
                           or (definition.definition_id == "raw_forward_outcome" and version in {"9", "10", "11", "12"}) else {})},
        "required_inputs": {"declaration": declarations,
                            "evidence_binding": "Exactly one frozen Dataset or supported immutable run evidence; use existing requirements/prepare.",
                            "resource_bindings": {"scope.instrument_id": "Authorized server instrument ID.",
                                                  "scope.indicator_id": "Existing server indicator ID when declared.",
                                                  "inputs[].source_policy": "Authorized source identity/policy for typed inputs.",
                                                  "scope.run_id": "Existing accessible run for run-backed previews."}},
        "settings": settings,
        "result_shape": {"envelope_schema": definition.result_schema_version,
                         "payload_schema": getattr(evaluator, "result_schema_version", {"signal_audit": "signal_audit_result.v1", "candidate_lifecycle": "candidate_lifecycle_result.v1"}.get(definition.definition_id, "research_check_result.v1")),
                         "analytical_fields": _analytical_fields(definition.definition_id, version),
                         "envelope_fields": ["definition_hash", "request_hash", "plan_hash", "evidence_hash",
                                             "evaluator_id", "evaluator_version", "result", "result_hash"],
                         "analysis_field": "measurements" if definition.definition_id == "raw_forward_outcome" and version in {"16", "18"} else "progress_comparison" if definition.definition_id == "raw_forward_outcome" and version == "15" else "matched_outcome_comparison" if definition.definition_id == "raw_forward_outcome" and version == "13" else "selection_summary" if definition.definition_id == "raw_forward_outcome" and version in {"11", "12", "14", "17"} else "first_passage_summary" if definition.definition_id == "raw_forward_outcome" and version == "10" else "volume_cohort_comparison" if definition.definition_id == "raw_forward_outcome" and version in {"7", "8"} else "controlled_reversal_comparison" if definition.definition_id == "raw_forward_outcome" and version in {"4", "5", "19"} else "crossing_state_comparison" if fixed_risk and version in {"11", "12"} else "forward_risk_comparison" if fixed_risk else None,
                         "meaning": "Bounded analytical output; completed does not certify scientific validity."},
        "examples": [] if example is None else [{"request": example,
                        "configured_definition": configured.to_dict(),
                        "instructions": "Bind resource IDs, then send this request to requirements/prepare. Evidence runs use the returned next_request and Dataset ID."}],
        "discovery": _discovery(definition.definition_id, version),
        "compatibility": "Historical replay definition; no current request selects it." if example is None else "Documentation metadata does not change the definition hash or calculation.",
    }


def list_check_definitions() -> dict[str, Any]:
    items = []
    for definition in CHECK_REGISTRY.definitions():
        detail = get_check_definition(definition.definition_id, definition.definition_version)
        items.append({key: detail[key] for key in ("definition", "purpose", "eligibility")})
    return {"schema_version": SCHEMA, "items": items}
