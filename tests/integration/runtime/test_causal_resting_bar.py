from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from engines.bot_runtime.core.domain import Candle, LadderRiskEngine
from engines.bot_runtime.core.execution_assumptions import (
    resolve_execution_assumptions, CAUSAL_SPOT_BAR_MODEL_VERSION,
)
from engines.bot_runtime.core.bar_execution_clock import validate_causal_run_mode

T = datetime(2024, 2, 1, tzinfo=timezone.utc)


def assumptions(model=CAUSAL_SPOT_BAR_MODEL_VERSION):
    return resolve_execution_assumptions('economic', dict(model_version=model,
        market_slippage_bps=10, stop_slippage_bps=20, passive_fill_policy='strict_penetration',
        fee_policy='instrument_resolved', full_fill_assumption=True,
        cost_stress_scenarios=[dict(id='stress', additional_slippage_bps=10, fee_multiplier=2)]))


def bar(index, *, low=98, high=101, close=100, known=None):
    start = T + timedelta(minutes=index)
    return Candle(time=start, end=start+timedelta(minutes=1),
        known_at=known or start+timedelta(minutes=1), open=100, high=high, low=low, close=close, atr=2)


def engine(*, validity=2, targets=None):
    return LadderRiskEngine(config=dict(initial_stop=dict(atr_multiplier=2),
        take_profit_orders=targets or [dict(id='tp', ticks=10, size_fraction=1)],
        execution_mode='limit_maker', limit_maker=dict(anchor_price='signal_price',
            offset_type='ticks', offset_value=1, validity_window=validity, fallback='cancel')),
        instrument=dict(id='fixture',symbol='SYNTHETIC-SPOT',instrument_type='spot',
            tick_size=1,contract_size=1,tick_value=1,qty_step=1,min_order_size=1,
            base_currency='BTC',quote_currency='USD',maker_fee_rate=.005,taker_fee_rate=.009,
            fee_source='simulation:regression',fee_schedule_version='fixture.v1'),
        risk_config=dict(base_risk_per_trade=4), execution_assumptions=assumptions())


def test_no_signal_range_or_equality_open_fill_and_immutable_entry_facts():
    risk = engine()
    signal = bar(0)
    assert risk.entry_execution.submit_entry(signal, 'long') is None
    pending = risk.entry_execution.pending_entry
    assert pending is not None
    initial_events = len(pending.order_lifecycle.events)
    for _ in range(2):
        assert risk.step(signal) == []
        assert risk.entry_execution.pending_entry is pending
        assert pending.validity_remaining == 2
        assert len(pending.order_lifecycle.events) == initial_events
    # A bar beginning exactly at availability is a guard interval, not +epsilon.
    assert risk.step(bar(1)) == []
    assert pending.validity_remaining == 2
    assert len(pending.order_lifecycle.events) == initial_events
    assert risk.step(bar(1)) == []
    risk.step(bar(2))
    position = risk.active_trade
    assert position is not None
    assert position.entry_time == T + timedelta(minutes=3)
    assert position.entry_time > signal.known_at
    facts = position.entry_fill_facts
    assert facts.quantity == 1 and facts.price == 99 and facts.notional == 99
    assert facts.fee_paid == pytest.approx(.495)
    assert pending.intent.metadata['decision_known_at'] == signal.known_at
    assert pending.intent.metadata['known_at'] == signal.known_at
    assert pending.intent.metadata['arrival_at'] == signal.known_at
    assert position.entry_outcome['filled_at'] == '2024-02-01T00:03:00Z'
    assert position.bars_held == 0
    with pytest.raises(FrozenInstanceError):
        facts.quantity = 0
    before = position.serialize()
    assert risk.step(bar(2)) == []
    assert position.serialize() == before


def test_delayed_source_candle_rejected_before_order_or_lifecycle():
    risk = engine(validity=1)
    known = T+timedelta(minutes=2, seconds=30)
    with pytest.raises(ValueError, match="close_time_availability"):
        risk.entry_execution.submit_entry(bar(0, known=known), "long")
    assert risk.entry_execution.pending_entry is None
    assert risk.active_trade is None and risk.trades == []



def test_one_eligible_evaluation_per_bar_and_gap_failure_is_before_lifecycle():
    risk = engine(validity=2)
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1, low=100))
    risk.step(bar(2, low=100))
    pending = risk.entry_execution.pending_entry
    assert pending.validity_remaining == 1
    events = len(pending.order_lifecycle.events)
    risk.step(bar(2, low=100))
    assert pending.validity_remaining == 1
    assert len(pending.order_lifecycle.events) == events
    with pytest.raises(RuntimeError, match='data_gap'):
        risk.step(bar(4, low=98))
    assert len(pending.order_lifecycle.events) == events
    assert risk.active_trade is None


def test_entry_bar_adverse_stop_is_retained_without_favorable_targets():
    risk = engine()
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1))
    events = risk.step(bar(2, high=120, low=90))
    assert risk.active_trade is None
    position = risk.trades[0]
    assert any(e['type'] == 'stop' for e in events)
    assert not any(e['type'] == 'target' for e in events)
    assert position.entry_fill_facts.quantity == 1
    assert position.entry_fill_facts.fee_paid == pytest.approx(.495)
    assert position.fees_paid > position.entry_fill_facts.fee_paid
    assert position.mfe_ticks == 0
    assert position.bars_held == 0


@pytest.mark.parametrize('mode', ['paper', 'live', 'sim_trade'])
def test_new_model_cannot_change_other_modes(mode):
    with pytest.raises(ValueError, match='backtest-only'):
        validate_causal_run_mode(assumptions(), mode)
    validate_causal_run_mode(assumptions('conservative_bar.v1'), mode)


def test_new_identity_is_hashed_and_incompatible_full_mode_is_rejected():
    new, old = assumptions(), assumptions('conservative_bar.v1')
    assert new.manifest_hash != old.manifest_hash
    assert new.execution_quality_ceiling == 'X2'
    with pytest.raises(ValueError, match='fast bar'):
        validate_causal_run_mode(new, 'backtest', 'full')


def _full_runtime(monkeypatch, root, *, model=CAUSAL_SPOT_BAR_MODEL_VERSION, multi_leg=False, stop_on_entry_bar=False):
    from tests.helpers.builders import persisted_runtime_harness as h
    from engines.bot_runtime.core.fees import FeeSchedule
    real_runtime, real_strategy, real_instrument, real_frame = h.BotRuntime, h._strategy, h._instrument, h.reference_frame
    schedule = FeeSchedule(maker_rate=.005, taker_rate=.009, source='simulation:causal-regression',
        version='fixture.v1', schedule_id='fixture', venue_profile_id='canonical_bar_simulation',
        fee_currency='USD', rounding_mode='half_up', precision=2).to_dict()
    def instrument():
        return {**real_instrument(), 'instrument_type': 'spot', 'can_short': False}
    def strategy():
        spec = real_strategy()
        spec.atm_template.update(execution_mode='limit_maker', limit_maker=dict(anchor_price='signal_price',
            offset_type='ticks', offset_value=1, validity_window=1, fallback='cancel'))
        if multi_leg:
            spec.risk_config['base_risk_per_trade'] = 8
            spec.atm_template['take_profit_orders'] = [dict(id='a', ticks=5, size_fraction=.5),
                dict(id='b', ticks=10, size_fraction=.5)]
        return spec
    def frame(**kwargs):
        result = real_frame(**kwargs)
        # Pure synthetic coverage: recognizing bar has pre-availability penetration.
        # The equality-open guard bar also crosses, but neither can fill the order.
        at = h.EVALUATION_START + timedelta(minutes=3)
        if at in result.index:
            result.loc[at, ['open', 'high', 'low', 'close']] = [101, 102, 99, 100.5]
        at = h.EVALUATION_START + timedelta(minutes=4)
        if at in result.index:
            result.loc[at, ['open', 'high', 'low', 'close']] = [101, 120, 90 if stop_on_entry_bar else 99, 100.5]
        return result
    def runtime(bot_id, config, **kwargs):
        return real_runtime(bot_id, {**config, 'execution_semantics': 'spot', 'execution_behavior': 'simulated',
            'economic_claim_intent': 'economic', 'execution_assumptions': assumptions(model).to_dict(),
            'simulation_fee_schedules': {h.INSTRUMENT_ID: schedule}}, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(h, '_instrument', instrument)
        patch.setattr(h, '_strategy', strategy)
        patch.setattr(h, 'reference_frame', frame)
        patch.setattr(h, 'BotRuntime', runtime)
        return h.run_persisted_reference(monkeypatch=patch, root=root, include_future_suffix=True)


@pytest.mark.parametrize('multi_leg', [False, True])
def test_full_backtest_runtime_causal_fills_and_immutable_accounting(monkeypatch, tmp_path, multi_leg):
    from tests.helpers.builders.persisted_runtime_harness import EVALUATION_START
    result = _full_runtime(monkeypatch, tmp_path, multi_leg=multi_leg)
    stream = result.artifact['runtime_event_stream']
    entries = [e for e in stream if e['event_name'] == 'ENTRY_FILLED']
    exits = [e for e in stream if e['event_name'] == 'EXIT_FILLED']
    assert len(entries) == 1
    entry = entries[0]
    at = lambda minute: (EVALUATION_START + timedelta(minutes=minute)).isoformat().replace('+00:00', 'Z')
    assert entry['event_ts'] == at(5)
    assert entry['event_ts'] > at(3)  # original typed signal known_at
    selected = [a for a in result.artifact['decision_artifacts'] if a.get('emitted_intent') == 'enter_long']
    assert selected[0]['bar_time'] == at(2)
    assert selected[0]['decision_time'] == at(3)
    assert not any(e['event_ts'] <= at(3) for e in entries)
    assert len(exits) == (2 if multi_leg else 1)
    assert all(e['event_ts'] == at(6) for e in exits)
    qty = 2 if multi_leg else 1
    context = entry['context']
    assert context['qty'] == qty and context['notional'] == qty * 100
    assert context['fee_paid'] == qty * .5
    assert context['wallet_delta']['fee_paid'] == context['fee_paid']
    assert context['fee_source'] == 'simulation:causal-regression'
    assert context['fee_schedule_hash']
    total_fees = context['fee_paid'] + sum(e['context']['fee_paid'] for e in exits)
    assert total_fees == pytest.approx(result.trades[0]['fees_paid'])
    trade = result.trades[0]
    assert trade['net_pnl'] == pytest.approx(trade['gross_pnl'] - total_fees)
    assert trade['entry_time'].isoformat().replace('+00:00', 'Z') == at(5)
    assert result.manifest['status'] == 'completed'


def test_partial_outcome_is_rejected_before_entry_settlement(monkeypatch):
    risk = engine()
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1))
    pending = risk.entry_execution.pending_entry
    count = len(pending.order_lifecycle.events)
    real_model = risk._resolve_execution_model()
    class Partial:
        def evaluate(self, intent, **kwargs):
            outcome, rejection = real_model.evaluate(intent, **kwargs)
            return replace(outcome, status='partially_filled', filled_qty=.5), rejection
    monkeypatch.setattr(risk, '_resolve_execution_model', lambda: Partial())
    with pytest.raises(RuntimeError, match='partial_fill_unsupported'):
        risk.step(bar(2))
    assert risk.active_trade is None and risk.trades == []
    assert len(pending.order_lifecycle.events) == count
    assert pending.validity_remaining == 2


def test_future_typed_clock_fails_closed_without_relabeling_source():
    from copy import deepcopy
    from engines.indicator_engine.contracts import RuntimeOutput
    from engines.bot_runtime.core.bar_execution_clock import validate_output_availability
    value = {'events': [{'key': 'long', 'known_at': (T+timedelta(minutes=3)).timestamp()}]}
    original = deepcopy(value)
    output = RuntimeOutput(bar_time=T, ready=True, value=value)
    with pytest.raises(ValueError, match='not_yet_available'):
        validate_output_availability({'signal': output}, T+timedelta(minutes=1))
    assert value == original
    validate_output_availability({'signal': output}, T+timedelta(minutes=3))
    with pytest.raises(ValueError, match='known_at_required'):
        validate_output_availability({'signal': RuntimeOutput(bar_time=T, ready=True,
            value={'events': [{'key': 'unknown'}]})}, T+timedelta(minutes=3))


def test_terminal_pending_expiry_is_once_only_and_never_a_fill():
    risk = engine()
    signal = bar(0)
    risk.entry_execution.submit_entry(signal, 'long')
    pending = risk.entry_execution.pending_entry
    risk.entry_execution.expire_pending_at_end(signal)
    assert risk.entry_execution.pending_entry is None
    assert risk.trades == []
    events = len(pending.order_lifecycle.events)
    assert pending.order_lifecycle.snapshot().state.value == 'expired'
    risk.entry_execution.expire_pending_at_end(signal)
    assert len(pending.order_lifecycle.events) == events


def test_repeating_failed_gap_and_partial_does_not_silently_mark_bar_done(monkeypatch):
    risk = engine()
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1))
    pending = risk.entry_execution.pending_entry
    count = len(pending.order_lifecycle.events)
    for _ in range(2):
        with pytest.raises(RuntimeError, match='data_gap'):
            risk.step(bar(3))
    assert pending.validity_remaining == 2
    assert len(pending.order_lifecycle.events) == count


def test_full_runtime_entry_bar_stop_does_not_corrupt_entry_fee_or_quantity(monkeypatch, tmp_path):
    from tests.helpers.builders.persisted_runtime_harness import EVALUATION_START
    result = _full_runtime(monkeypatch, tmp_path, stop_on_entry_bar=True, multi_leg=True)
    stream = result.artifact['runtime_event_stream']
    entries = [e for e in stream if e['event_name'] == 'ENTRY_FILLED']
    exits = [e for e in stream if e['event_name'] == 'EXIT_FILLED']
    assert len(entries) == 1 and len(exits) == 2
    stamp = (EVALUATION_START+timedelta(minutes=5)).isoformat().replace('+00:00', 'Z')
    assert entries[0]['event_ts'] == stamp
    assert all(e['event_ts'] == stamp and e['context']['reason_code'] == 'EXEC_EXIT_STOP' for e in exits)
    entry = entries[0]['context']
    assert entry['qty'] == 2 and entry['price'] == 100 and entry['notional'] == 200
    assert entry['fee_paid'] == 1 and entry['wallet_delta']['fee_paid'] == 1
    assert result.trades[0]['fees_paid'] == pytest.approx(1+sum(e['context']['fee_paid'] for e in exits))


def test_new_worker_mode_guard_precedes_metadata_and_market_fetch():
    from types import SimpleNamespace
    from engines.bot_runtime.strategy.series_builder import SeriesBuilder
    from tests.test_portal.test_series_builder_incremental import _builder_deps
    def no_read(*args, **kwargs):
        raise AssertionError('A rejected model must not fetch market data or resolve instruments')
    builder = SeriesBuilder(bot_id='fixture', config=dict(economic_claim_intent='economic',
        execution_assumptions=assumptions().to_dict()), run_type='paper',
        deps=_builder_deps(fetch_ohlcv=no_read, get_instrument_record=no_read, resolve_instrument=no_read))
    with pytest.raises(ValueError, match='backtest-only'):
        builder._build_single_series(SimpleNamespace(id='strategy'), SimpleNamespace(instrument_id='fixture'))


def test_delayed_eligible_bar_cannot_create_late_entry_then_earlier_exit():
    risk = engine()
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1))
    pending = risk.entry_execution.pending_entry
    count = len(pending.order_lifecycle.events)
    with pytest.raises(ValueError, match='close_time_availability'):
        risk.step(bar(2, known=T+timedelta(minutes=10)))
    assert risk.active_trade is None and risk.trades == []
    assert pending.validity_remaining == 2
    assert len(pending.order_lifecycle.events) == count
    with pytest.raises(RuntimeError, match='data_gap'):
        risk.step(bar(3, low=90))
    assert risk.active_trade is None and risk.trades == []
    assert len(pending.order_lifecycle.events) == count


def test_holding_gap_is_not_a_silent_favorable_path():
    risk = engine()
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1))
    risk.step(bar(2))
    position = risk.active_trade
    before = position.serialize()
    with pytest.raises(RuntimeError, match='data_gap'):
        risk.step(bar(4, high=200))
    assert risk.active_trade is position and position.serialize() == before


def test_causal_excursions_and_model_artifact_are_labeled_bounds():
    from engines.bot_runtime.core.execution_context import execution_model_artifact_from_assumptions
    risk = engine()
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1))
    risk.step(bar(2))
    risk.step(bar(3, high=200, low=90))
    assert risk.trades[0]._metrics_snapshot()['excursion_measurement'] == 'coarse_bar_bounds_not_realized_path'
    artifact = execution_model_artifact_from_assumptions(assumptions())
    assert artifact.parameters['execution_availability'] == 'interval_close_only_delayed_receipt_unsupported'
    assert artifact.parameters['excursion_measurement'] == 'coarse_bar_bounds_not_realized_path'
    assert execution_model_artifact_from_assumptions(assumptions('conservative_bar.v1')).parameters == {}


def test_unfilled_stop_cannot_fall_through_to_favorable_target(monkeypatch):
    risk = engine()
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1))
    risk.step(bar(2))
    position = risk.active_trade
    monkeypatch.setattr(position, '_apply_stop', lambda candle: [])
    with pytest.raises(RuntimeError, match='stop_not_fully_settled'):
        risk.step(bar(3, high=200, low=90))
    assert all(leg.status == 'open' for leg in position.legs)
