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
                                        'event_keys': [{'key': 'atr_expansion_long', 'direction': 'long'}]},
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
                                       'source_policy': {'mode': 'exact',
                                                         'source_identity_key': 'source-identity-id'}}],
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
                                         'event_keys': [{'key': 'atr_expansion_long', 'direction': 'long'}]},
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
                                         'event_keys': [{'key': 'atr_expansion_long', 'direction': 'long'}]},
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
 'raw_forward_outcome@3': {'check_family': 'raw_forward_outcome',
                           'mode': 'preview',
                           'scope': {'instrument_id': 'instrument-id',
                                     'timeframe': '1m',
                                     'start': '2022-01-01T00:00:00Z',
                                     'end': '2022-01-02T00:00:00Z'},
                           'detector': {'type': 'raw_condition',
                                        'field': 'close',
                                        'operator': 'gt',
                                        'value_field': 'previous_close'},
                           'outcomes': {'schema_version': 'raw_candle_forward_outcome.v1',
                                        'forward_bars': [5],
                                        'entry_lag_bars': 1},
                           'gap_policy': 'continue_degraded'},
 'event_fact_analysis@12': {'check_family': 'event_fact_analysis',
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
                                         'event_keys': [{'key': 'atr_expansion_long', 'direction': 'long'}]},
                            'outcomes': {'horizon_kind': 'elapsed_time',
                                         'horizons': [1800, 7200, 21600],
                                         'primary_horizon': 7200,
                                         'entry_lag_bars': 0,
                                         'forward_risk': {'schema_version': 'candle_risk_matched_state.v2',
                                                          'baseline_bars': 120,
                                                          'readiness_contract': 'candle_stats.public_outputs.v1',
                                                          'outcome_boundary': 'evaluation_end_exclusive',
                                                          'matching_contract': 'crossing_state_matched_pairs.v1',
                                                          'filter_contract': 'crossing_state_atr_ratio_filter.v1'}},
                            'statistics': {}},
 'raw_forward_outcome@19': {'check_family': 'raw_forward_outcome',
                            'mode': 'preview',
                            'scope': {'instrument_id': 'instrument-id',
                                      'timeframe': '15m',
                                      'start': '2022-01-01T00:00:00Z',
                                      'end': '2022-02-01T00:00:00Z',
                                      'warmup_bars': 97},
                            'detector': {'type': 'controlled_bullish_reversal'},
                            'outcomes': {'schema_version': 'controlled_reversal_comparison.v3',
                                         'interval': '15m',
                                         'trend_lookback_bars': 12,
                                         'volatility_lookback_bars': 96,
                                         'entry_lag_bars': 1,
                                         'outcome_horizon_bars': 12,
                                         'matching_policy': 'same_utc_close_slot',
                                         'comparison_window_days': 7},
                            'gap_policy': 'continue_degraded'},
 'event_fact_analysis@13': {'check_family': 'event_fact_analysis',
                            'mode': 'preview',
                            'gap_policy': 'continue_degraded',
                            'scope': {'instrument_id': 'coinbase-btc-usd',
                                      'timeframe': '1h',
                                      'warmup_bars': 15,
                                      'start': '2026-07-25T00:00:00Z',
                                      'end': '2026-09-23T00:00:00Z'},
                            'detector': {'type': 'fact_snapshot', 'input_alias': 'long_liquidations'},
                            'outcomes': {'horizons': [1],
                                         'entry_lag_bars': 0,
                                         'retrospective': {'schema_version': 'retrospective_numeric_comparison.v1',
                                                           'analysis_as_of': '2026-10-13T23:00:00Z',
                                                           'bucket_time_interpretation': 'bucket_start_unverified'}},
                            'inputs': [{'alias': 'long_liquidations',
                                        'instrument_id': 'binance-btc-usdt-perp',
                                        'fact_type': 'derivatives.liquidation_volume',
                                        'contract_version': 'derivatives.liquidation_volume.v1',
                                        'max_staleness_seconds': 3600,
                                        'alignment': 'exact_interval',
                                        'dimensions': {'liquidated_side': 'long',
                                                       'bucket_seconds': 3600,
                                                       'volume_unit': 'BTC',
                                                       'exchange': 'Binance',
                                                       'product': 'BTCUSDT_PERP.A'},
                                        'source_policy': {'mode': 'exact',
                                                          'source_identity_key': 'authorized-source'}},
                                       {'alias': 'short_liquidations',
                                        'instrument_id': 'binance-btc-usdt-perp',
                                        'fact_type': 'derivatives.liquidation_volume',
                                        'contract_version': 'derivatives.liquidation_volume.v1',
                                        'max_staleness_seconds': 3600,
                                        'alignment': 'exact_interval',
                                        'dimensions': {'liquidated_side': 'short',
                                                       'bucket_seconds': 3600,
                                                       'volume_unit': 'BTC',
                                                       'exchange': 'Binance',
                                                       'product': 'BTCUSDT_PERP.A'},
                                        'source_policy': {'mode': 'exact',
                                                          'source_identity_key': 'authorized-source'}}],
                            'statistics': {'features': {'baseline': [{'name': 'recent_return',
                                                                      'operator': 'lagged_return',
                                                                      'lookback_bars': 1},
                                                                     {'name': 'recent_volatility',
                                                                      'operator': 'atr_fraction',
                                                                      'period': 14}],
                                                        'enriched': [{'name': 'long_liquidations',
                                                                      'operator': 'latest_value',
                                                                      'input_alias': 'long_liquidations'},
                                                                     {'name': 'short_liquidations',
                                                                      'operator': 'latest_value',
                                                                      'input_alias': 'short_liquidations'}]},
                                           'model': {'type': 'standardized_l2_logistic', 'c': 1.0},
                                           'purge_bars': 1,
                                           'folds': [{'id': 'fixed-comparison',
                                                      'train': {'start': '2026-07-25T00:00:00Z',
                                                                'end': '2026-09-03T00:00:00Z'},
                                                      'validation': {'start': '2026-09-03T00:00:00Z',
                                                                     'end': '2026-09-23T00:00:00Z'}}]}},
 'raw_forward_outcome@6': {'check_family': 'raw_forward_outcome',
                           'mode': 'preview',
                           'scope': {'instrument_id': 'instrument-id',
                                     'timeframe': '1m',
                                     'start': '2022-01-01T00:00:00Z',
                                     'end': '2022-01-02T00:00:00Z',
                                     'warmup_bars': 1},
                           'detector': {'all': [{'field': 'previous_close',
                                                 'operator': 'lt',
                                                 'value_field': 'previous_open'},
                                                {'field': 'close', 'operator': 'gt', 'value_field': 'open'},
                                                {'field': 'open',
                                                 'operator': 'lte',
                                                 'value_field': 'previous_close'},
                                                {'field': 'close',
                                                 'operator': 'gte',
                                                 'value_field': 'previous_open'},
                                                {'any': [{'field': 'volume',
                                                          'operator': 'gt',
                                                          'value_field': 'previous_volume'},
                                                         {'field': 'volume',
                                                          'operator': 'lte',
                                                          'value_field': 'previous_volume'}]}]},
                           'outcomes': {'schema_version': 'raw_candle_distribution.v1',
                                        'forward_bars': [5],
                                        'entry_lag_bars': 1},
                           'gap_policy': 'continue_degraded'},
 'raw_forward_outcome@7': {'check_family': 'raw_forward_outcome',
                           'mode': 'preview',
                           'scope': {'instrument_id': 'instrument-id',
                                     'timeframe': '1m',
                                     'start': '2022-01-01T00:00:00Z',
                                     'end': '2022-01-02T00:00:00Z',
                                     'warmup_bars': 1},
                           'detector': {'all': [{'field': 'previous_close',
                                                 'operator': 'lt',
                                                 'value_field': 'previous_open'},
                                                {'field': 'close', 'operator': 'gt', 'value_field': 'open'},
                                                {'field': 'open',
                                                 'operator': 'lte',
                                                 'value_field': 'previous_close'},
                                                {'field': 'close',
                                                 'operator': 'gte',
                                                 'value_field': 'previous_open'},
                                                {'any': [{'field': 'volume',
                                                          'operator': 'gt',
                                                          'value_field': 'previous_volume'},
                                                         {'field': 'volume',
                                                          'operator': 'lte',
                                                          'value_field': 'previous_volume'}]}]},
                           'outcomes': {'schema_version': 'volume_cohort_matched.v1',
                                        'forward_bars': [5],
                                        'entry_lag_bars': 1,
                                        'comparison': {'cohort': 'volume_gt_previous_vs_lte',
                                                       'prior_volatility_bars': 20,
                                                       'matching_policy': 'same_date_hour',
                                                       'caliper_ratios': {'formation_body_fraction': 1.25,
                                                                          'formation_range_fraction': 1.25,
                                                                          'prior_rms_return': 1.25}}},
                           'gap_policy': 'continue_degraded'},
 'raw_forward_outcome@8': {'check_family': 'raw_forward_outcome',
                           'mode': 'preview',
                           'scope': {'instrument_id': 'instrument-id',
                                     'timeframe': '1m',
                                     'start': '2022-01-01T00:00:00Z',
                                     'end': '2022-01-02T00:00:00Z',
                                     'warmup_bars': 1},
                           'detector': {'all': [{'field': 'previous_close',
                                                 'operator': 'lt',
                                                 'value_field': 'previous_open'},
                                                {'field': 'close', 'operator': 'gt', 'value_field': 'open'},
                                                {'field': 'open',
                                                 'operator': 'lte',
                                                 'value_field': 'previous_close'},
                                                {'field': 'close',
                                                 'operator': 'gte',
                                                 'value_field': 'previous_open'},
                                                {'any': [{'field': 'volume',
                                                          'operator': 'gt',
                                                          'value_field': 'previous_volume'},
                                                         {'field': 'volume',
                                                          'operator': 'lte',
                                                          'value_field': 'previous_volume'}]}]},
                           'outcomes': {'schema_version': 'volume_cohort_matched.v2',
                                        'forward_bars': [5],
                                        'entry_lag_bars': 1,
                                        'comparison': {'cohort': 'volume_gt_previous_vs_lte',
                                                       'prior_volatility_bars': 20,
                                                       'matching_policy': 'same_utc_close_slot',
                                                       'caliper_ratios': {'formation_body_fraction': 1.25,
                                                                          'formation_range_fraction': 1.25,
                                                                          'prior_rms_return': 1.25},
                                                       'comparison_window_days': 3,
                                                       'matching_slot_seconds': 3600}},
                           'gap_policy': 'continue_degraded'},
 'event_fact_analysis@14': {'check_family': 'event_fact_analysis',
                            'mode': 'preview',
                            'gap_policy': 'continue_degraded',
                            'scope': {'instrument_id': 'instrument-id',
                                      'timeframe': '1m',
                                      'start': '2026-09-22T00:00:00Z',
                                      'end': '2026-09-22T00:30:00Z'},
                            'detector': {'type': 'liquidity_episode'},
                            'outcomes': {'schema_version': 'liquidity_episode.v1',
                                         'analysis_as_of': '2026-10-14T00:00:00Z'},
                            'inputs': [{'alias': 'bbo',
                                        'fact_type': 'market.bbo',
                                        'contract_version': 'market.bbo.v1',
                                        'timeframe_seconds': 1,
                                        'alignment': 'exact_interval',
                                        'max_staleness_seconds': 1,
                                        'allow_gaps': True,
                                        'source_policy': {'mode': 'exact',
                                                          'source_identity_key': 'source-identity-id'}},
                                       {'alias': 'depth',
                                        'fact_type': 'market.depth_observation',
                                        'contract_version': 'market.depth_band.v1',
                                        'timeframe_seconds': 1,
                                        'alignment': 'exact_interval',
                                        'max_staleness_seconds': 1,
                                        'allow_gaps': True,
                                        'source_policy': {'mode': 'exact',
                                                          'source_identity_key': 'source-identity-id'}},
                                       {'alias': 'flow',
                                        'fact_type': 'market.trade_flow',
                                        'contract_version': 'market.trade_flow.v1',
                                        'timeframe_seconds': 1,
                                        'alignment': 'exact_interval',
                                        'max_staleness_seconds': 1,
                                        'allow_gaps': True,
                                        'source_policy': {'mode': 'exact',
                                                          'source_identity_key': 'source-identity-id'}},
                                       {'alias': 'trades',
                                        'fact_type': 'market.trade',
                                        'contract_version': 'market.trade.v1',
                                        'alignment': 'latest_known',
                                        'max_staleness_seconds': 1,
                                        'allow_gaps': True,
                                        'source_policy': {'mode': 'exact',
                                                          'source_identity_key': 'source-identity-id'}}]},
 'raw_forward_outcome@17': {'check_family': 'raw_forward_outcome',
                            'mode': 'evidence',
                            'gap_policy': 'reject',
                            'scope': {'instrument_id': 'instrument-1',
                                      'timeframe': '15m',
                                      'start': '2025-02-01T00:00:00Z',
                                      'end': '2025-03-29T00:00:00Z'},
                            'detector': {'type': 'completed_reversal_candidates',
                                         'computation_origin': '2025-01-01T00:00:00Z',
                                         'reference_basis': 'rising_leg_plus_retreat',
                                         'ownership': 'retain_all_provisional',
                                         'max_bars': 19104,
                                         'max_candidates': 10000},
                            'outcomes': {'schema_version': 'reversal_population_selection.v1',
                                         'populations': [{'name': 'first_block',
                                                          'start_block': 0,
                                                          'block_count': 1},
                                                         {'name': 'second_block',
                                                          'start_block': 1,
                                                          'block_count': 1}]},
                            'dataset_id': 'mds_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'},
 'raw_forward_outcome@18': {'check_family': 'raw_forward_outcome',
                            'mode': 'evidence',
                            'dataset_id': 'mds_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                            'scope': {'instrument_id': 'instrument-1',
                                      'timeframe': '15m',
                                      'start': '2025-02-01T00:00:00Z',
                                      'end': '2025-03-29T00:00:00Z'},
                            'detector': {'type': 'saved_reversal_selection'},
                            'gap_policy': 'reject',
                            'outcomes': {'schema_version': 'reversal_saved_measurements.v2',
                                         'selection_reference': {'source_check_id': '11111111-1111-4111-8111-111111111111',
                                                                 'result_hash': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                                                                 'evidence_hash': 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                                                                 'selection_manifest_hash': 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'},
                                         'frozen_context': {'dataset_id': 'mds_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                                                            'dataset_hash': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                                                            'input_binding_hash': 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
                                                            'max_commit_seq': 10,
                                                            'instrument_id': 'instrument-1',
                                                            'input_start': '2025-01-01T00:00:00Z',
                                                            'input_end_exclusive': '2025-03-29T00:00:00Z',
                                                            'decision_start': '2025-02-01T00:00:00Z',
                                                            'decision_end_exclusive': '2025-03-29T00:00:00Z',
                                                            'source_identity_key': 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                                                            'input_rows': 8352,
                                                            'case_count': 2,
                                                            'pair_count': 2,
                                                            'completion_clock_count': 1}}},
 'raw_forward_outcome@20': {'check_family': 'raw_forward_outcome',
                            'mode': 'evidence',
                            'gap_policy': 'reject',
                            'scope': {'instrument_id': 'instrument-1',
                                      'timeframe': '15m',
                                      'start': '2025-02-01T00:00:00Z',
                                      'end': '2025-03-29T00:00:00Z'},
                            'detector': {'type': 'completed_reversal_candidates',
                                         'computation_origin': '2025-01-01T00:00:00Z',
                                         'reference_basis': 'rising_leg_plus_retreat',
                                         'ownership': 'retain_all_provisional',
                                         'max_bars': 19104,
                                         'max_candidates': 10000},
                            'outcomes': {'schema_version': 'reversal_completed_recovery.v1',
                                         'populations': [{'name': 'first_block',
                                                          'start_block': 0,
                                                          'block_count': 1},
                                                         {'name': 'second_block',
                                                          'start_block': 1,
                                                          'block_count': 1}]},
                            'dataset_id': 'mds_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'}}

_EXAMPLES['event_fact_analysis@15'] = {'check_family': 'event_fact_analysis', 'mode': 'evidence', 'scope': {'instrument_id': 'authorized-instrument-id', 'indicator_id': 'existing-breakout-indicator-id', 'timeframe': '15m', 'start': '2024-01-01T01:00:00Z', 'end': '2024-01-02T00:00:00Z'}, 'detector': {'type': 'indicator_event', 'output_name': 'breakouts', 'event_keys': [{'key': 'breakout_long', 'direction': 'long'}]}, 'outcomes': {'schema_version': 'indicator_event_barriers.v1', 'input_origin': '2024-01-01T00:00:00Z', 'horizon_bars': 12, 'reference': {'operator': 'event_close'}, 'failure': {'operator': 'event_metadata_number', 'path': 'metadata.failure_price'}, 'target': {'operator': 'event_metadata_number', 'path': 'metadata.target_price'}, 'support_lookback_bars': 4}, 'statistics': {'features': [{'name': 'strength', 'operator': 'event_metadata_number', 'path': 'metadata.strength', 'units': 'ratio'}]}, 'gap_policy': 'reject', 'dataset_id': 'mds_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'}

_EXAMPLES['raw_forward_outcome@21'] = {
    'check_family': 'raw_forward_outcome', 'mode': 'evidence',
    'dataset_id': 'mds_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    'scope': {'instrument_id': 'instrument-1', 'timeframe': '15m',
              'start': '2024-01-10T00:00:00Z', 'end': '2024-03-01T00:00:00Z'},
    'detector': {'type': 'candle_close', 'sampling': 'ordinary_native_close'},
    'outcomes': {'schema_version': 'candle_state_paired_comparison.v1',
                 'horizon_seconds': 3600,
                 'primary': {'type': 'forward_close_gt_current', 'flat': 'class_zero'},
                 'continuation': {'lookback_bars': 20, 'zero_prior': 'undefined_prior_direction',
                                  'future_flat': 'separate'},
                 'magnitude': {'endpoint': 'absolute_log_return',
                               'path': 'sum_squared_native_log_returns'},
                 'outcome_boundary': 'evaluation_end_exclusive'},
    'statistics': {
        'features': {
            'baseline': [
                {'name': 'return_1', 'operator': 'lagged_return', 'lookback_bars': 1},
                {'name': 'return_4', 'operator': 'lagged_return', 'lookback_bars': 4},
                {'name': 'return_20', 'operator': 'lagged_return', 'lookback_bars': 20},
                {'name': 'atr_fraction_14', 'operator': 'atr_fraction', 'period': 14},
                {'name': 'volume_ratio_50', 'operator': 'volume_ratio', 'lookback_bars': 50}],
            'enriched': [
                {'name': 'efficiency_20', 'operator': 'indicator_metric',
                 'indicator': {'type': 'candle_stats', 'version': 'v1',
                               'output': 'candle_stats', 'field': 'directional_efficiency',
                               'params': {'atr_short_window': 14, 'atr_long_window': 50,
                                          'atr_z_window': 100, 'directional_efficiency_window': 20,
                                          'slope_window': 20, 'range_window': 20,
                                          'expansion_window': 20, 'volume_window': 50,
                                          'overlap_window': 8, 'slope_stability_lookback': 150,
                                          'warmup_bars': 200,
                                          'atr_expansion_signal_threshold': 2.0}},
                 'transform': 'identity'},
                {'name': 'signed_efficiency_20', 'operator': 'indicator_metric',
                 'indicator': {'type': 'candle_stats', 'version': 'v1',
                               'output': 'candle_stats', 'field': 'directional_efficiency',
                               'params': {'atr_short_window': 14, 'atr_long_window': 50,
                                          'atr_z_window': 100, 'directional_efficiency_window': 20,
                                          'slope_window': 20, 'range_window': 20,
                                          'expansion_window': 20, 'volume_window': 50,
                                          'overlap_window': 8, 'slope_stability_lookback': 150,
                                          'warmup_bars': 200,
                                          'atr_expansion_signal_threshold': 2.0}},
                 'transform': 'times_return_sign', 'sign_lookback_bars': 20,
                 'zero_sign': 'zero'}]},
        'folds': [
            {'id': 'F1', 'train': {'start': '2024-01-10T00:00:00Z', 'end': '2024-01-24T00:00:00Z'},
             'validation': {'start': '2024-01-24T00:00:00Z', 'end': '2024-02-07T00:00:00Z'}},
            {'id': 'F2', 'train': {'start': '2024-01-10T00:00:00Z', 'end': '2024-02-07T00:00:00Z'},
             'validation': {'start': '2024-02-07T00:00:00Z', 'end': '2024-02-21T00:00:00Z'}}],
        'model': {'type': 'standardized_l2_logistic', 'c': 1.0, 'seed': 0,
                  'fit_intercept': True, 'tolerance': 1e-9, 'max_iterations': 1000},
        'purge_bars': 200, 'embargo_bars': 200,
        'bootstrap': {'cluster_days': 3, 'sensitivity_cluster_days': [1],
                      'anchor': '1970-01-01T00:00:00Z', 'replicates': 5000,
                      'confidence': .95, 'seed': 20261030},
        'eligibility': {'min_train_rows': 500, 'min_validation_rows': 180,
                        'min_train_each_class': 20, 'min_validation_each_class': 20,
                        'min_validation_utc_days': 8, 'min_valid_folds': 2,
                        'min_nonempty_clusters': 8},
        'criteria': {'min_delta_log_loss': .005, 'require_interval_above_zero': True,
                     'min_positive_folds': 2, 'interpretation': 'forecast_screen_only'}},
    'gap_policy': 'reset_rewarm'}

_EXAMPLES['raw_forward_outcome@22'] = deepcopy(_EXAMPLES['raw_forward_outcome@21'])
_EXAMPLES['raw_forward_outcome@22']['outcomes']['schema_version'] = (
    'candle_state_paired_comparison.v2'
)
_EXAMPLES['raw_forward_outcome@22']['statistics']['features'] = {
    'baseline': [
        {'name': 'return_1', 'operator': 'lagged_return', 'lookback_bars': 1},
        {'name': 'return_4', 'operator': 'lagged_return', 'lookback_bars': 4},
        {'name': 'return_20', 'operator': 'lagged_return', 'lookback_bars': 20},
        {'name': 'atr_fraction_14', 'operator': 'atr_fraction', 'period': 14}],
    'enriched': [
        {'name': 'volume_ratio_50', 'operator': 'volume_ratio', 'lookback_bars': 50}],
}

_EXAMPLES['raw_forward_outcome@23'] = deepcopy(_EXAMPLES['raw_forward_outcome@22'])
_EXAMPLES['raw_forward_outcome@23']['outcomes']['schema_version'] = (
    'candle_state_paired_comparison.v3'
)
_EXAMPLES['raw_forward_outcome@23']['statistics']['features']['enriched'] = [
    {'name': 'relative_volume_50', 'operator': 'volume_ratio',
     'lookback_bars': 50, 'transform': 'identity'},
    {'name': 'signed_relative_volume_50', 'operator': 'volume_ratio',
     'lookback_bars': 50, 'transform': 'times_return_sign',
     'sign_lookback_bars': 4, 'zero_sign': 'zero'},
]

_EXAMPLES['raw_forward_outcome@24'] = {
    'check_family': 'raw_forward_outcome', 'mode': 'evidence',
    'dataset_id': 'mds_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    'scope': {'instrument_id': 'instrument-1', 'timeframe': '15m',
              'start': '2024-01-01T00:00:00Z', 'end': '2024-03-01T00:00:00Z',
              'warmup_bars': 200},
    'detector': {'type': 'candle_close', 'sampling': 'ordinary_native_close'},
    'outcomes': {
        'schema_version': 'candle_target_benchmark_comparison.v1',
        'horizon_seconds': 3600, 'horizon_bars': 4,
        'outcome_boundary': 'evaluation_end_exclusive', 'complete_path': True,
        'target': {'operator': 'terminal_direction',
                   'definition_version': 'terminal_direction.v1', 'flat': 'class_zero'}},
    'statistics': {
        'features': [
            {'name': 'return_1', 'operator': 'lagged_return', 'lookback_bars': 1},
            {'name': 'return_4', 'operator': 'lagged_return', 'lookback_bars': 4},
            {'name': 'return_20', 'operator': 'lagged_return', 'lookback_bars': 20},
            {'name': 'atr_fraction_14', 'operator': 'atr_fraction', 'period': 14}],
        'forecaster': {'type': 'standardized_l2_logistic',
                       'features': ['return_1', 'return_4', 'return_20', 'atr_fraction_14'],
                       'c': 1.0, 'fit_intercept': True, 'seed': 0,
                       'tolerance': 1e-9, 'max_iterations': 1000,
                       'freeze': 'nominal_training_end'},
        'rolling_history': {
            'window_seconds': 2419200,
            'anchor_window': 'decision_time_left_inclusive_right_exclusive',
            'label_availability': 'outcome_known_at_strictly_before_prediction',
            'minimum_rows': 200, 'minimum_distinct_utc_dates': 7,
            'history_eligibility': 'shared_causal_context_complete_future_path',
            'include_realized_validation_history': True,
            'insufficient_history': 'exclude_common_scoring_clock_with_reason'},
        'benchmarks': [
            {'name': 'rolling_global', 'type': 'rolling_target_mean',
             'prior': {'pseudo_count': 1.0, 'pseudo_mean': .5}},
            {'name': 'rolling_calendar', 'type': 'rolling_calendar_target_mean',
             'timezone': 'UTC', 'bin_seconds': 21600,
             'bin_anchor': '1970-01-01T00:00:00Z', 'minimum_rows': 30,
             'minimum_distinct_utc_dates': 5, 'shrinkage_pseudo_count': 20,
             'fallback': 'current_rolling_global'}],
        'folds': [
            {'id': 'F1', 'train': {'start': '2024-01-01T00:00:00Z', 'end': '2024-01-22T00:00:00Z'},
             'validation': {'start': '2024-01-22T00:00:00Z', 'end': '2024-02-05T00:00:00Z'}},
            {'id': 'F2', 'train': {'start': '2024-01-01T00:00:00Z', 'end': '2024-02-05T00:00:00Z'},
             'validation': {'start': '2024-02-05T00:00:00Z', 'end': '2024-02-19T00:00:00Z'}}],
        'purge_bars': 200, 'embargo_bars': 200,
        'eligibility': {'min_train_rows': 500, 'min_validation_rows': 180,
                        'min_train_direction_each_class': 20,
                        'min_validation_direction_each_class': 20,
                        'min_validation_utc_days': 8, 'min_valid_folds': 2,
                        'min_nonempty_clusters': 8},
        'bootstrap': {'cluster_days': 3, 'anchor': '1970-01-01T00:00:00Z',
                      'replicates': 5000, 'seed': 20261030, 'confidence': .95,
                      'sensitivity_cluster_days': [1],
                      'resampling_unit': 'paired_scoring_losses',
                      'undefined_ratio_draw': 'unsupported_interval'},
        'loss': {'type': 'bernoulli_log_loss', 'probability_epsilon': 1e-12,
                 'gain': 'benchmark_minus_forecaster',
                 'skill': 'one_minus_forecaster_mean_loss_over_benchmark_mean_loss'},
        'criteria': {'against': ['rolling_global', 'rolling_calendar'],
                     'minimum_gain': .005,
                     'require_gain_ci_lower_above_zero': True,
                     'minimum_positive_folds': 2}},
    'gap_policy': 'reset_rewarm'}

_PURPOSES = {'raw_forward_outcome@20': 'Compose all completed reversals,causal context,inclusive recovery duration and '
                           'unchanged12bar first passage;separate full-population label distributions with '
                           'original all-case clock weights;no matching or adjusted inference.',
 'raw_forward_outcome@19': 'Retain per-arm fractional MFE/MAE and first-attaining candle timing on the '
                           'existing configurable controlled-reversal future path; unchanged matching and '
                           'endpoint-return analysis.',
 'raw_forward_outcome@18': 'Compose existing first-passage and progress measurements on validated fixed or '
                           'configured saved populations, retaining original input, weights, support and '
                           'explicit units/censoring; no reselection.',
 'raw_forward_outcome@17': 'Select reversal comparisons with explicit bounded dates, symbol and reporting '
                           'populations over the existing outcome-blind kernel; fixed scientific matching '
                           'rules, original weights and nominal support.',
 'event_fact_analysis@14': 'Describe fixed sharp-move and thinning episodes, outcome-blind precontext '
                           'controls, trade sequences, and actual availability clocks on bounded exposed '
                           'inputs.',
 'raw_forward_outcome@8': 'Compare raw-event volume cohorts using an explicit nearby-date radius and UTC '
                          'time slot with unchanged feature calipers, support and both-arm date dependence '
                          'diagnostics.',
 'raw_forward_outcome@7': 'Compare configured raw-event volume cohorts with outcome-blind '
                          'body/range/prior-volatility matching, retaining the full original universe and '
                          'missingness.',
 'raw_forward_outcome@6': 'Retain every raw-candle event and compare volume cohorts with fixed quantiles, '
                          'tails, day and overlap diagnostics; preserve valid extremes.',
 'event_fact_analysis@13': 'Compare next-hour direction association beyond return/ATR using late-received '
                           'exact-bucket numeric features; retrospective only. Optional '
                           'outcomes.retrospective.input_coverage_policy=complete_paired_rows admits '
                           'recorded partial custody with missing-row exclusions and fixed boundaries.',
 'raw_forward_outcome@3': 'Evaluate canonical raw candle predicates with causal close entry and contiguous '
                          'bounded outcomes.',
 'event_fact_analysis@4': 'Analyze indicator events or Fact snapshots with typed Fact inputs.',
 'event_fact_analysis@5': 'Analyze Fact snapshots when required Facts become available.',
 'event_fact_analysis@6': 'Describe indicator events using canonical candle-only snapshots.',
 'event_fact_analysis@7': 'Attribute follow-up outcomes to exact original indicator events.',
 'event_fact_analysis@8': 'Compare original events at a shared classification landmark.',
 'event_fact_analysis@9': 'Compare first return to the original market-profile range.',
 'event_fact_analysis@10': 'Describe fixed Candle Stats ATR-expansion future risk cohorts.',
 'event_fact_analysis@11': 'Compare fixed ATR crossings with matched persistent-high states.',
 'event_fact_analysis@12': 'Filter the original crossing-state pairs by a fixed 5% public ATR-ratio rule '
                           'before outcomes.'}

_PURPOSES['event_fact_analysis@15'] = 'Measure public Indicator events against locked high/low barriers, original-clock weighted feature profiles and strictly-later target observations; no recognition, rematching or trading claim.'
_PURPOSES['raw_forward_outcome@21'] = ('Compare paired baseline and enriched directional forecasts at every ordinary native candle close, '
    'using causal public Indicator metrics, exact elapsed outcomes, gap resets and configured walk-forward inference; no trading claim.')
_PURPOSES['raw_forward_outcome@22'] = ('Compare paired directional forecasts at ordinary native candle closes with typed causal candle features on '
    'either side, optional public Indicator metrics, exact elapsed outcomes, and an outcome-blind causal selection audit; no trading claim.')
_PURPOSES['raw_forward_outcome@23'] = ('Compare paired directional forecasts at ordinary native candle closes with typed causal candle features on '
    'either side, optional bounded historical-sign transforms, optional public Indicator metrics, exact elapsed outcomes, and an outcome-blind causal '
    'selection audit; no trading claim.')
_PURPOSES['raw_forward_outcome@24'] = ('Score one versioned candle target with a fold-frozen causal forecaster against strictly historical rolling '
    'global and UTC-calendar forecasts on identical ordinary native closes; retain label availability, full scoring rows and no profitability claim.')


def _discovery(family: str, version: str) -> dict[str, Any]:
    result = {"new_requests": True,
        "meaning": "Only current registered procedures are executable; recorded historical evidence is read independently."}
    if family == "raw_forward_outcome" and version in {"17", "18"}:
        result["composition_limit"] = (
            "Method17 selection may exceed999pairs; method18 rejects the whole saved population "
            "above999pairs; no truncation or rematching."
        )
    return result


def _settings_fields(family: str, version: str) -> dict[str, Any]:
    if family == "raw_forward_outcome" and version == "24":
        return {
            "scope": "Authorized instrument, native timeframe and bounded UTC range; existing finite candle/input/result/time limits.",
            "outcomes.target": "Terminal direction or native-log-return sum of squares over a complete exact elapsed path.",
            "statistics.features/forecaster": "Typed causal candle features with a target-specific fold-frozen logistic or nonnegative scaled-feature forecaster.",
            "statistics.rolling_history": "Trailing decision-clock history; labels enter only when their complete outcome known-at is strictly before prediction.",
            "statistics.benchmarks": "Rolling global target mean and fixed UTC calendar bins with explicit support, shrinkage and global fallback.",
            "statistics.folds/buffers": "Configured chronological model-fit ranges; rolling benchmarks may update from already-realized validation labels.",
            "statistics.loss/bootstrap/criteria": "Target-specific paired loss, pooled gain/skill, UTC-cluster uncertainty and forecast screens; no economic claim.",
        }
    if family == "raw_forward_outcome" and version in {"22", "23"}:
        return {
            "scope": "Authorized instrument, native timeframe and bounded UTC range; method limit 19,104 candles.",
            "detector": "Fixed ordinary_native_close sampling; no event or threshold selection.",
            "outcomes": "Configured exact elapsed horizon, up-versus-non-up label, continuation lookback and separate magnitude diagnostics.",
            "statistics.features": (
                "Named lagged return, Wilder ATR fraction and prior-volume ratio features may appear on either side; "
                "enriched may additionally bind public candle_stats metrics, and may be Indicator-free. "
                + ("Version 23 admits identity or times_return_sign on typed candle features with exact contiguous causal history and flat sign zero."
                   if version == "23" else "Version 22 preserves identity-only typed candle features.")
            ),
            "statistics.folds": "Configured expanding/walk-forward train and validation UTC ranges; target endpoints stay inside their nominal ranges.",
            "statistics.model": "Training-only standardized L2 logistic baseline and enriched fits on identical complete rows.",
            "statistics.purge_bars/embargo_bars": "Elapsed native-bar buffers; not an independence guarantee for recursive state.",
            "statistics.bootstrap": "Fixed epoch-anchored UTC calendar clusters plus configured sensitivity spans.",
            "statistics.eligibility/criteria": "Configured support screens and forecast-effect criteria; never execution or profitability authority.",
            "selection_audit": "Outcome-blind causal context for every nominal close slot, including explicit exclusions and missingness; no future labels.",
        }
    if family == "raw_forward_outcome" and version == "21":
        return {
            "scope": "Authorized instrument, native timeframe and bounded UTC range; method limit 19,104 candles.",
            "detector": "Fixed ordinary_native_close sampling; no event or threshold selection.",
            "outcomes": "Configured exact elapsed horizon, up-versus-non-up label, continuation lookback and separate magnitude diagnostics.",
            "statistics.features": "Named lagged return, Wilder ATR fraction and prior-volume ratio baseline features; enriched public candle_stats metric bindings with identity or historical-sign transform.",
            "statistics.folds": "Configured expanding/walk-forward train and validation UTC ranges; target endpoints stay inside their nominal ranges.",
            "statistics.model": "Training-only standardized L2 logistic baseline and enriched fits on identical complete rows.",
            "statistics.purge_bars/embargo_bars": "Elapsed native-bar buffers; not an independence guarantee for recursive state.",
            "statistics.bootstrap": "Fixed epoch-anchored UTC calendar clusters plus configured sensitivity spans.",
            "statistics.eligibility/criteria": "Configured support screens and forecast-effect criteria; never execution or profitability authority.",
        }
    if family == "event_fact_analysis" and version == "15":
        return {"scope":"Existing Indicator/instrument IDs,native15m,explicit bounded UTC range.",
                "detector":"Public long signal output_name/event_keys, no detection algorithm.",
                "outcomes":"indicator_event_barriers.v1;explicit input_origin,1..12bars;event-close/metadata reference,window-low/metadata failure,risk-multiple/metadata target;optional identity/source/clock paths.",
                "statistics":"1..32explicit named/unit metadata or window-duration features;at most3declared min/max illustrations;all-population clock weights and fixed descriptive contrasts."}
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
    if family == "raw_forward_outcome" and version == "18":
        return {"scope": {"constraints": "Exact saved v12/v14/v17 decision scope and original frozen native15m Dataset; evidence mode only; no tail."},
            "detector": {"fixed": {"type": "saved_reversal_selection"}},
            "outcomes": {"schema_version": "reversal_saved_measurements.v2",
                "selection_reference": "Exact Check/result/evidence/selection-manifest hashes.",
                "frozen_context": "Declared Dataset/hash/watermark/source/instrument/ranges/counts, verified against typed original saved evidence.",
                "meaning": "Existing twelve-bar first passage and progress kernels; original pairs/weights. At most999pairs, whole selection rejected above limit; no truncation/rematching."}}
    if family == "raw_forward_outcome" and version == "20":
        return {"scope":{"constraints":"Explicit instrument/native15m frozen input;one through six whole28day decision blocks;origin-to-end<=19104bars;no extra tail."},
            "detector":{"meaning":"Unchanged retreat-inclusive sequential recognition;retain all chains;19104/10000 identity/candidate limits;explicit origin."},
            "outcomes":{"schema_version":"reversal_completed_recovery.v1","populations":"At most two named block views plus combined;all-population weights normalize within each view.",
                "meaning":"All completions,causal sigma96/r/z_pre12,N,unchanged first passage,separate full-population label/N summaries;no matching,tuning or inference."}}
    if family == "raw_forward_outcome" and version == "17":
        return {"scope": {"constraints": "Explicit instrument/native15m dates; one through six complete28day decision blocks; origin-to-end at most19104bars; evidence-only frozen Dataset."},
            "detector": {"meaning": "Existing retreat-inclusive recognizer; retain all chains; fixed19104bar identity namespace and10000candidate bound; explicit computation_origin."},
            "outcomes": {"schema_version": "reversal_population_selection.v1", "populations": "At most two named block views plus combined; these change reporting only, not pairs or root weights.",
                "meaning": "Selection only; no outcomes, thresholds, models or inference."}}
    if family == "raw_forward_outcome" and version == "19":
        fields["detector"] = {"fixed":{"type":"controlled_bullish_reversal"}}
        fields["inputs[]"] = {"fixed":[]}
        fields["statistics"] = {"fixed":{}}
        fields["gap_policy"] = {"fixed":"continue_degraded"}
        fields["gap_rewarm_bars"] = {"fixed":0}
        fields["outcomes"] = {"fields": list(_EXAMPLES["raw_forward_outcome@19"]["outcomes"]),
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
        return fields
    operators = sorted(checks._DETECTOR_OPERATORS)
    if family == "raw_forward_outcome":
        fields["detector"] = {"fields": ["type", "field", "operator", "value", "value_field", "all", "any", "not"], "field_choices": sorted(checks._RAW_DETECTOR_FIELDS), "operators": operators,
            "constraints": "raw_condition with scalar value or another declared candle field; all/any lists and not compose conditions."}
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
    if family == "event_fact_analysis" and version == "15":
        return {"cases":"Original public event IDs,geometry,features,weights,first passage and later-target observation.","population_hash":"Prelabel population/dependence digest.","summary":"Four label masses,weighted feature ECDF/quantiles and two descriptive contrasts.","later_target_summary":"Original failure mass and fixed first-failure opportunity/status rows.","dependence":"Direct support and conservative common-origin ancestry;not independent N."}
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
    if family == "raw_forward_outcome" and version in {"21", "22", "23", "24"}:
        fields.update({
            "opportunities": {"type": "object", "meaning": "Nominal slots, observed closes, readiness, feature, exact future-path and paired-row counts."},
            "exclusions": {"type": "object", "meaning": "Explicit causal/readiness/feature/path exclusion counts; no truncation."},
            "comparison": {"type": "object or null", "meaning": "Paired fold and pooled baseline-versus-enriched forecast scores, fixed calendar-cluster intervals and support screen."},
            "diagnostics": {"type": "object", "meaning": "Direction, continuation, endpoint magnitude and path variation remain separate from the primary label."},
            "provenance": {"type": "object", "meaning": "Typed feature/optional Indicator bindings, population/prediction hashes and exact elapsed label contract."},
            "dependence": {"type": "object", "meaning": "Overlapping outcomes and recursive shared history remain explicit; buffers do not certify independence."},
        })
        if version in {"22", "23", "24"}:
            fields["selection_audit"] = {"type": "object", "meaning": "Outcome-blind causal context, missingness and exclusion reasons for every nominal close slot."}
        if version == "24":
            fields["comparison"] = {"type": "object", "meaning": "Fold-frozen target forecaster versus causal rolling global and UTC-calendar benchmarks on identical retained scoring rows."}
            fields["scoring_rows"] = {"type": "array", "meaning": "Typed per-clock target, forecast, loss and strictly historical benchmark-membership receipts."}
            fields["scoring_exclusions"] = {"type": "object", "meaning": "Target-scoring support exclusions, including insufficient causal rolling history."}
            fields["scoring_opportunities"] = {"type": "object", "meaning": "Shared structural, model-training, validation and common-scoring denominators by fold and pooled."}
            fields["manifests"] = {"type": "object", "meaning": "Structural clocks, paired direction/variance labels and prediction hashes for cross-Check reconciliation."}
            fields["concentration"] = {"type": "object", "meaning": "Fixed target/loss tail and UTC calendar concentration diagnostics; no deletion or refit."}
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
    elif family == "raw_forward_outcome" and version == "20":
        fields["sample_count"] = {"type":"integer","meaning":"All completed formation clocks;not independent observations."}
        fields["cases"] = {"type":"array","meaning":"Every completed chain,causal context,inclusive trough-to-completion N,original clock mass and all four first-passage labels;invalid/unpaired rows retained."}
        fields["populations"] = {"type":"object","meaning":"Four separate label masses/N distributions;observed/missing N weight/count;original all-case clock weights and context missingness."}
        fields["dependence"] = {"type":"object","meaning":"Direct context/formation supports and conservative common-origin ancestry remain distinct;no independent N."}
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
        "defaults": {"mode": "evidence" if definition.definition_id == "event_fact_analysis" and version == "15" else "preview",
                     "gap_rewarm_bars": 0},
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
                        "modes": ["evidence"] if definition.definition_id=="event_fact_analysis" and version=="15" else ["evidence"] if definition.definition_id=="raw_forward_outcome" and version in {"17", "18", "20", "21", "22", "23", "24"} else ["preview", "evidence"] if durable and example is not None else ["preview"] if example is not None else [],
                        "observation": "Requires completed, replayable durable evidence and explicit admission; never automatic." if durable else "Preview only; cannot create new durable evidence or Observations.",
                        "execution_authority": False, "promotion_authority": False,
                        **({"point_in_time_predictive":False,"scientific_certification_eligible":False,"backtest_certified":False}
                           if (definition.definition_id == "event_fact_analysis" and version in {"13", "14"})
                           or (definition.definition_id == "raw_forward_outcome" and version in {"9", "10", "11", "12"}) else {})},
        "required_inputs": {"declaration": declarations,
                            "evidence_binding": "Exactly one frozen Dataset; use existing requirements/prepare.",
                            "resource_bindings": {"scope.instrument_id": "Authorized server instrument ID.",
                                                  "scope.indicator_id": "Existing server indicator ID when declared.",
                                                  "inputs[].source_policy": "Authorized source identity/policy for typed inputs."}},
        "settings": settings,
        "result_shape": {"envelope_schema": definition.result_schema_version,
                         "payload_schema": getattr(evaluator, "result_schema_version", {"signal_audit": "signal_audit_result.v1", "candidate_lifecycle": "candidate_lifecycle_result.v1"}.get(definition.definition_id, "research_check_result.v1")),
                         "analytical_fields": _analytical_fields(definition.definition_id, version),
                         "envelope_fields": ["definition_hash", "request_hash", "plan_hash", "evidence_hash",
                                             "evaluator_id", "evaluator_version", "result", "result_hash"],
                          "analysis_field": "summary" if definition.definition_id=="event_fact_analysis" and version=="15" else "comparison" if definition.definition_id=="raw_forward_outcome" and version in {"21", "22", "23", "24"} else "populations" if definition.definition_id == "raw_forward_outcome" and version == "20" else "measurements" if definition.definition_id == "raw_forward_outcome" and version in {"16", "18"} else "progress_comparison" if definition.definition_id == "raw_forward_outcome" and version == "15" else "matched_outcome_comparison" if definition.definition_id == "raw_forward_outcome" and version == "13" else "selection_summary" if definition.definition_id == "raw_forward_outcome" and version in {"11", "12", "14", "17"} else "first_passage_summary" if definition.definition_id == "raw_forward_outcome" and version == "10" else "volume_cohort_comparison" if definition.definition_id == "raw_forward_outcome" and version in {"7", "8"} else "controlled_reversal_comparison" if definition.definition_id == "raw_forward_outcome" and version in {"4", "5", "19"} else "crossing_state_comparison" if fixed_risk and version in {"11", "12"} else "forward_risk_comparison" if fixed_risk else None,
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
