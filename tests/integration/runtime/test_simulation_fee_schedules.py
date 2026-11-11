from __future__ import annotations

from copy import deepcopy
import json

import pytest

from engines.bot_runtime.core.fees import FeeResolver, FeeSchedule
from engines.bot_runtime.core.simulation_fees import (
    apply_simulation_fee_schedule, normalize_simulation_fee_schedules,
)
from engines.bot_runtime.core.execution_context import (
    ResolvedExecutionContext, resolve_execution_context, validate_context_against_runtime,
)
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile
from engines.bot_runtime.core.execution_assumptions import resolve_execution_assumptions
from market_data.backtest import build_backtest_execution_config_hash
from portal.backend.service.bots.runtime_control_service import _apply_start_overrides, _start_config_hash
from portal.backend.service.bots.startup_service import _bot_run_config_snapshot


def _schedule():
    return FeeSchedule(maker_rate=.005, taker_rate=.009,
        source='simulation:declared-retail-scenario-not-account-tier', version='scenario.v1',
        schedule_id='spot-scenario', venue_profile_id='canonical_bar_simulation',
        fee_currency='USD', rounding_mode='half_up', precision=2).to_dict()


def _config():
    return dict(run_type='backtest', execution_behavior='simulated', economic_claim_intent='exploration',
        simulation_fee_schedules={'btc': _schedule()})


def _instrument():
    return dict(id='btc', symbol='BTC-USD', instrument_type='spot', tick_size=.01,
        contract_size=1., tick_value=.01, base_currency='BTC', quote_currency='USD',
        min_order_size=.001, qty_step=.001, maker_fee_rate=None, taker_fee_rate=None)


def _normalize(raw, **changes):
    mode = dict(run_type='backtest', execution_behavior='simulated', economic_claim_intent='exploration')
    mode.update(changes)
    return normalize_simulation_fee_schedules(raw, **mode)


def test_start_snapshot_worker_replay_accounting_and_provenance():
    original = _instrument()
    saved = deepcopy(original)
    start = _apply_start_overrides({'id': 'bot', 'run_type': 'backtest'}, _config())
    snapshot = json.loads(json.dumps(_bot_run_config_snapshot(start)))
    assert snapshot['simulation_fee_schedules'] == start['simulation_fee_schedules']
    worker_instrument = apply_simulation_fee_schedule(original, snapshot)
    assert original == saved
    profile = compile_series_execution_profile(worker_instrument, execution_semantics='spot')
    assumptions = resolve_execution_assumptions('exploration', snapshot['execution_assumptions'])
    context = resolve_execution_context(profile, assumptions, instrument_payload=worker_instrument)
    restored = ResolvedExecutionContext.from_dict(json.loads(json.dumps(context.to_dict())))
    validate_context_against_runtime(restored, profile=profile, assumptions=assumptions,
        instrument_payload=worker_instrument)
    for role, expected in [('maker', .62), ('taker', 1.11)]:
        fee = FeeResolver(restored.fee_schedule).resolve(role=role, price=123.45, quantity=1, contract_size=1)
        assert fee.fee_paid == expected
        assert fee.currency == 'USD'
        assert fee.source.startswith('simulation:')
        assert fee.version == 'scenario.v1'
        assert fee.schedule_hash == _schedule()['schedule_hash']
    changed = deepcopy(snapshot)
    changed['simulation_fee_schedules']['btc'].update(taker_rate=.01, schedule_hash='')
    altered = apply_simulation_fee_schedule(original, changed)
    with pytest.raises(ValueError, match='runtime_mismatch'):
        validate_context_against_runtime(restored, profile=compile_series_execution_profile(altered,
            execution_semantics='spot'), assumptions=assumptions, instrument_payload=altered)
    assert _start_config_hash(start) != _start_config_hash(changed)
    identity = dict(strategy_id='s', strategy_hash='s-hash', indicator_config_hash='i-hash', execution_policy_hash='e-hash')
    def binding(bot):
        return build_backtest_execution_config_hash(bot=bot, strategy_identity=identity, instrument_config_hash='instrument')
    assert binding(snapshot) == binding(json.loads(json.dumps(snapshot)))
    assert binding(snapshot) != binding(changed)


@pytest.mark.parametrize('field,value', [
    ('maker_rate', True), ('taker_rate', 0), ('maker_rate', -.001), ('taker_rate', 1),
    ('taker_rate', float('nan')), ('taker_rate', float('inf')), ('taker_rate', '0.009'),
    ('taker_rate', 10 ** 400),
    ('configured', False), ('verified_zero', True), ('version', ''), ('source', 'account-tier'),
    ('rounding_mode', 'unknown'), ('precision', True), ('schedule_hash', 'wrong'),
    ('calculation_basis', 'base_quantity'), ('unknown', 'field'),
])
def test_rejects_invalid_or_unconfigured_schedules(field, value):
    schedule = _schedule()
    schedule[field] = value
    with pytest.raises(ValueError):
        _normalize({'btc': schedule})


@pytest.mark.parametrize('raw', [{}, [], {'btc': None}, {' btc': {}}, {1: {}}, {'btc': {}}])
def test_malformed_maps_fail_closed(raw):
    with pytest.raises(ValueError):
        _normalize(raw)


@pytest.mark.parametrize('mode', [
    {'run_type': 'paper'}, {'run_type': 'live'}, {'run_type': 'sim_trade'},
    {'execution_behavior': 'observe_only'}, {'execution_behavior': 'live'},
    {'economic_claim_intent': 'selection'}, {'economic_claim_intent': 'promotion'},
])
def test_mode_boundary_is_enforced_again_at_worker(mode):
    config = _config()
    config.update(mode)
    with pytest.raises(ValueError):
        apply_simulation_fee_schedule(_instrument(), config)


def test_unconfigured_legacy_and_paper_live_are_unchanged_without_override():
    for mode in ['backtest', 'paper', 'live']:
        original = _instrument()
        assert apply_simulation_fee_schedule(original, {'run_type': mode}) == original
    with pytest.raises(ValueError, match='missing for instrument'):
        apply_simulation_fee_schedule({**_instrument(), 'id': 'eth'}, _config())
    assert 'simulation_fee_schedules' not in _bot_run_config_snapshot({'id': 'old'})
    assert _normalize(None) == {}


def test_schedule_order_and_normalization_are_stable():
    one = _normalize({'eth': _schedule(), 'btc': _schedule()})
    two = _normalize({'btc': _schedule(), 'eth': _schedule()})
    assert one == two
    assert list(one) == ['btc', 'eth']
    assert _normalize(one) == one


def test_dataset_preparation_and_start_pin_identical_fee_identity(monkeypatch):
    from types import SimpleNamespace
    from portal.backend.service.bots import bot_service
    bot = dict(id='bot', strategy_id='strategy', run_type='backtest', execution_behavior='simulated')
    original = deepcopy(bot)
    captured = {}
    class Config:
        def get_bot(self, identifier):
            assert identifier == 'bot'
            return bot
        def prepare_startup_artifacts(self, effective):
            captured['startup'] = deepcopy(effective)
            return {'strategy': object()}
    monkeypatch.setattr(bot_service, '_composition', lambda: SimpleNamespace(config_service=Config()))
    def material(**kwargs):
        captured.update(kwargs)
        return {'dataset_id': 'fixture'}
    monkeypatch.setattr(bot_service, 'prepare_backtest_dataset_material', material)
    fees = {'btc': _schedule()}
    result = bot_service.prepare_backtest_dataset('bot', evaluation_start='2024-03-01T00:00:00Z',
        evaluation_end='2024-05-31T00:00:00Z', acquire_missing=False, simulation_fee_schedules=fees)
    assert result == {'dataset_id': 'fixture'}
    assert captured['bot']['simulation_fee_schedules'] == fees
    assert captured['startup']['simulation_fee_schedules'] == fees
    assert bot == original
    start = _apply_start_overrides(bot, dict(economic_claim_intent='exploration', simulation_fee_schedules=fees))
    identity = dict(strategy_id='s', strategy_hash='s-hash', indicator_config_hash='i-hash', execution_policy_hash='e-hash')
    def binding(payload):
        return build_backtest_execution_config_hash(bot=payload, strategy_identity=identity, instrument_config_hash='instrument')
    assert binding(captured['bot']) == binding(start)
    assert binding(bot) != binding(start)
