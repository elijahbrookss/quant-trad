from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from engines.bot_runtime.core.domain import Candle, LadderRiskEngine
from engines.bot_runtime.core.execution_assumptions import (
    resolve_execution_assumptions, CAUSAL_SPOT_BAR_MODEL_VERSION,
    TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION,
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
    assert position.serialize()['quantity'] == 1
    assert position.entry_fill_facts.fee_paid == pytest.approx(.495)
    assert position.fees_paid > position.entry_fill_facts.fee_paid
    assert position.mfe_ticks == 0
    assert position.bars_held == 0


@pytest.mark.parametrize('mode', ['paper', 'live', 'sim_trade'])
@pytest.mark.parametrize('model', [CAUSAL_SPOT_BAR_MODEL_VERSION, TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION])
def test_new_model_cannot_change_other_modes(mode, model):
    with pytest.raises(ValueError, match='backtest-only'):
        validate_causal_run_mode(assumptions(model), mode)
    validate_causal_run_mode(assumptions('conservative_bar.v1'), mode)


def test_new_identity_is_hashed_and_incompatible_full_mode_is_rejected():
    new, old = assumptions(), assumptions('conservative_bar.v1')
    assert new.manifest_hash != old.manifest_hash
    assert new.execution_quality_ceiling == 'X2'
    with pytest.raises(ValueError, match='fast bar'):
        validate_causal_run_mode(new, 'backtest', 'full')


def _full_runtime(monkeypatch, root, *, model=CAUSAL_SPOT_BAR_MODEL_VERSION, multi_leg=False, stop_on_entry_bar=False, tick_trailing=False, never_fill=False, native_tick=.01, gap_through=False):
    from tests.helpers.builders import persisted_runtime_harness as h
    from engines.bot_runtime.core.fees import FeeSchedule
    real_runtime, real_strategy, real_instrument, real_frame = h.BotRuntime, h._strategy, h._instrument, h.reference_frame
    schedule = FeeSchedule(maker_rate=.005, taker_rate=.009, source='simulation:causal-regression',
        version='fixture.v1', schedule_id='fixture', venue_profile_id='canonical_bar_simulation',
        fee_currency='USD', rounding_mode='half_up', precision=2).to_dict()
    def instrument():
        result = {**real_instrument(), 'instrument_type': 'spot', 'can_short': False}
        if tick_trailing:
            result.update(tick_size=native_tick, tick_value=native_tick, qty_step=.00000001,
                min_order_size=.00000001, metadata={'info': {'base_increment': '.00000001'}})
        return result
    def strategy():
        spec = real_strategy()
        spec.atm_template.update(execution_mode='limit_maker', limit_maker=dict(anchor_price='signal_price',
            offset_type='ticks', offset_value=1, validity_window=1, fallback='cancel'))
        if multi_leg:
            spec.risk_config['base_risk_per_trade'] = 8
            spec.atm_template['take_profit_orders'] = [dict(id='a', ticks=5, size_fraction=.5),
                dict(id='b', ticks=10, size_fraction=.5)]
        if tick_trailing:
            spec.risk_config['base_risk_per_trade'] = .4
            spec.atm_template['take_profit_orders'] = [dict(id='tp', ticks=10000, size_fraction=1)]
            spec.atm_template['trailing'] = dict(enabled=True, activation_type='r_multiple',
                r_multiple=1, ticks=0, atr_multiplier=.3333)
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
        if tick_trailing:
            at = h.EVALUATION_START + timedelta(minutes=6)
            result.loc[at, ['open', 'high', 'low', 'close']] = [110 if gap_through else 112, 113, 109 if gap_through else 110, 111]
        if never_fill:
            result.loc[result.index >= h.EVALUATION_START + timedelta(minutes=4),
                ['open', 'high', 'low', 'close']] = [102, 103, 101, 102]
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
    for exit_event in exits:
        exit_context = exit_event['context']
        assert exit_context['fee_currency'] == context['fee_currency'] == 'USD'
        assert exit_context['fee_schedule_hash'] == context['fee_schedule_hash']
        assert exit_context['resolved_execution_context_hash'] == context['resolved_execution_context_hash']
        assert exit_context['fee_rounding_mode'] == context['fee_rounding_mode']
        assert exit_context['fee_precision'] == context['fee_precision'] == 2
    total_fees = context['fee_paid'] + sum(e['context']['fee_paid'] for e in exits)
    assert total_fees == pytest.approx(result.trades[0]['fees_paid'])
    trade = result.trades[0]
    assert trade['net_pnl'] == pytest.approx(trade['gross_pnl'] - total_fees)
    assert trade['entry_time'].isoformat().replace('+00:00', 'Z') == at(5)
    assert result.manifest['status'] == 'completed'


def test_v1_unaligned_trailing_fails_closed_but_v2_settles(monkeypatch, tmp_path):
    # Preserve the old model's observable rejection rather than rewriting v1.
    with pytest.raises(RuntimeError, match='causal_stop_not_fully_settled'):
        _full_runtime(monkeypatch, tmp_path / 'v1', tick_trailing=True)
    result = _full_runtime(monkeypatch, tmp_path / 'v2', tick_trailing=True,
        model=TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION)
    fills = [e for e in result.artifact['runtime_event_stream']
        if e['event_name'] in {'ENTRY_FILLED', 'EXIT_FILLED'}]
    assert len(fills) == 2
    entry, exit = fills
    assert exit['context']['reason_code'] == 'EXEC_EXIT_STOP'
    assert exit['context']['qty'] == entry['context']['qty'] > 0
    assert exit['context']['qty'] < 1  # Exercise fractional cash-spot quantity.
    assert exit['event_ts'] > entry['event_ts']
    assert exit['context']['fee_currency'] == entry['context']['fee_currency'] == 'USD'
    assert exit['context']['fee_schedule_hash'] == entry['context']['fee_schedule_hash']
    assert result.trades[0]['status'] == 'closed'
    assert result.trades[0]['contracts'] == entry['context']['qty']
    closed = [fact for batch in result.canonical_fact_batches for fact in batch.get('payload', {}).get('facts', [])
        if fact.get('fact_type') == 'trade_closed']
    assert closed
    assert all(fact['trade']['quantity'] == entry['context']['qty'] for fact in closed)
    assert result.trades[0]['fees_paid'] == pytest.approx(sum(e['context']['fee_paid'] for e in fills))
    assert result.trades[0]['net_pnl'] == pytest.approx(
        result.trades[0]['gross_pnl'] - result.trades[0]['fees_paid'])
    assert assumptions().manifest_hash != assumptions(TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION).manifest_hash


def test_tick_aligned_model_artifact_declares_rounding_without_changing_v1():
    from engines.bot_runtime.core.execution_context import execution_model_artifact_from_assumptions
    old = execution_model_artifact_from_assumptions(assumptions())
    new = execution_model_artifact_from_assumptions(assumptions(TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION))
    assert 'trailing_stop_rounding' not in old.parameters
    assert new.parameters['trailing_stop_rounding'] == 'long_floor_short_ceiling_to_instrument_tick_then_monotonic_clamp'
    assert old.artifact_hash != new.artifact_hash


@pytest.mark.parametrize('tick', [.01, .05, .25])
@pytest.mark.parametrize('gap', [False, True])
def test_v2_trailing_settlement_native_ticks_and_gap_open(monkeypatch, tmp_path, tick, gap):
    from decimal import Decimal
    result = _full_runtime(monkeypatch, tmp_path, tick_trailing=True, native_tick=tick,
        gap_through=gap, model=TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION)
    fills = [e for e in result.artifact['runtime_event_stream']
        if e['event_name'] in {'ENTRY_FILLED', 'EXIT_FILLED'}]
    assert len(fills) == 2
    entry, exit = fills
    assert exit['context']['reason_code'] == 'EXEC_EXIT_STOP'
    reference = Decimal(str(exit['context']['requested_price']))
    assert reference % Decimal(str(tick)) == 0
    if gap:
        assert reference == Decimal('110')  # Adverse open, not the crossed stop.
    assert exit['context']['qty'] == entry['context']['qty'] > 0
    assert result.trades[0]['status'] == 'closed'
    assert result.trades[0]['net_pnl'] == pytest.approx(
        result.trades[0]['gross_pnl'] - sum(e['context']['fee_paid'] for e in fills))


@pytest.mark.parametrize('tick,best,distance,expected', [
    (.01, 112.5, 25, 112.25), (.05, 112.5, 5, 112.25), (.25, 112.5, 1, 112.25),
])
def test_v2_trailing_exact_tick_boundary_and_monotonic_clamp(tick, best, distance, expected):
    risk = engine()
    risk.entry_execution.submit_entry(bar(0), 'long')
    risk.step(bar(1)); risk.step(bar(2))
    position = risk.active_trade
    position.execution_assumptions = assumptions(TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION)
    position.tick_size = tick; position.best_price = best
    position.trailing_distance_ticks = distance; position.trailing_activation_ticks = 0
    position._maybe_trail_stop()
    assert position.stop_price == expected
    position.best_price -= tick
    position._maybe_trail_stop()
    assert position.stop_price == expected  # Never loosen to keep the trade alive.


def test_unfilled_causal_intent_has_linked_terminal_expiry_without_wallet_fill(monkeypatch, tmp_path):
    result = _full_runtime(monkeypatch, tmp_path, never_fill=True)
    stream = result.artifact['runtime_event_stream']
    decisions = [e for e in stream if e['event_name'] == 'DECISION_ACCEPTED']
    expired = [e for e in stream if e['event_name'] == 'ORDER_LIFECYCLE_CHANGED'
        and e['context']['state'] == 'expired']
    assert len(decisions) == len(expired) == 1
    assert expired[0]['parent_id'] == decisions[0]['event_id']
    assert expired[0]['root_id'] == decisions[0]['root_id']
    assert expired[0]['context']['parent_missing'] is False
    assert decisions[0]['context']['wallet_snapshot']['balances']['USD'] == 1000
    # Observation of the wallet is not a reservation, charge or fill.
    assert decisions[0]['context']['wallet_commit_seq'] is None
    assert not any(e['event_name'] in {'ENTRY_FILLED', 'EXIT_FILLED'} for e in stream)
    assert result.trades == ()
    from portal.backend.service.reports.run_research_dataset import _wallet_accounting
    accounting = _wallet_accounting(run={}, decisions=[{
        'decision_id': decisions[0]['context']['decision_id'], 'accepted': True,
        'decision_context': decisions[0]['context'],
    }], events=[{'payload': event} for event in stream])
    assert accounting['missing_wallet_trace_count'] == 0
    assert 'wallet_decision_trace_incomplete' not in accounting['caveats']


def test_causal_lifecycle_does_not_attach_same_correlation_wrong_decision():
    from types import SimpleNamespace
    from uuid import uuid4
    from engines.bot_runtime.core.runtime_events import RuntimeEventName
    from engines.bot_runtime.runtime.mixins.runtime_events import RuntimeEventsMixin
    risk = engine()
    risk.strategy_id = 'strategy'; risk.last_decision_id = 'expected-decision'
    risk.entry_execution.submit_entry(bar(0), 'long')
    lifecycle = risk.entry_execution.pending_entry.order_lifecycle
    assert lifecycle.request.decision_id == 'expected-decision'
    wrong = SimpleNamespace(event_name=RuntimeEventName.DECISION_ACCEPTED,
        correlation_id='same-correlation', event_id=str(uuid4()), root_id=str(uuid4()),
        context=SimpleNamespace(strategy_id='strategy', symbol='SYNTHETIC-SPOT', timeframe='1m',
            trade_id=lifecycle.request.trade_id, decision_id='different-decision'))
    runtime = RuntimeEventsMixin()
    runtime.bot_id = 'bot'; runtime._run_context = SimpleNamespace(run_id='run')
    runtime._runtime_events_reverse = lambda: iter([wrong])
    runtime._bar_correlation_id = lambda series, clock: 'same-correlation'
    runtime._persist_runtime_event = lambda event: None
    series = SimpleNamespace(strategy_id='strategy', symbol='SYNTHETIC-SPOT', timeframe='1m',
        instrument={'id': 'fixture'}, risk_engine=risk)
    event = runtime._emit_order_lifecycle_event(series=series, lifecycle=lifecycle,
        event=lifecycle.events[-1])
    assert event.parent_id is None
    assert event.root_id == event.event_id and event.root_id != wrong.root_id
    assert event.context.parent_missing is True


def test_v1_target_close_delay_is_preserved_but_v2_closes_on_last_fill(monkeypatch, tmp_path):
    parse = lambda value: datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    old = _full_runtime(monkeypatch, tmp_path / 'v1')
    old_exit = next(e for e in old.artifact['runtime_event_stream'] if e['event_name'] == 'EXIT_FILLED')
    assert parse(old.trades[0]['exit_time']) > parse(old_exit['event_ts'])
    new = _full_runtime(monkeypatch, tmp_path / 'v2', model=TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION)
    new_exit = next(e for e in new.artifact['runtime_event_stream'] if e['event_name'] == 'EXIT_FILLED')
    assert parse(new.trades[0]['exit_time']) == parse(new_exit['event_ts'])
    assert new.trades[0]['net_pnl'] == pytest.approx(new.trades[0]['gross_pnl'] - new.trades[0]['fees_paid'])


def test_v2_last_target_releases_next_decision_occupancy_without_resettlement(monkeypatch, tmp_path):
    from tests.helpers.builders.persisted_runtime_harness import EVALUATION_START, STARTING_CASH
    at = lambda minute: (EVALUATION_START + timedelta(minutes=minute)).isoformat().replace('+00:00', 'Z')
    old = _full_runtime(monkeypatch, tmp_path / 'v1')
    new = _full_runtime(monkeypatch, tmp_path / 'v2', model=TICK_ALIGNED_CAUSAL_SPOT_BAR_MODEL_VERSION)
    # A new canonical typed signal on bar 6 is observable after the last
    # target settles on bar 5. V1 still blocks this source bar; v2 releases it.
    old_decisions = [e for e in old.artifact['runtime_event_stream']
        if e['event_name'] in {'DECISION_ACCEPTED', 'DECISION_REJECTED'}
        and e['context']['bar_ts'] == at(6)]
    new_decisions = [e for e in new.artifact['runtime_event_stream']
        if e['event_name'] in {'DECISION_ACCEPTED', 'DECISION_REJECTED'}
        and e['context']['bar_ts'] == at(6)]
    assert len(old_decisions) == len(new_decisions) == 1
    assert old_decisions[0]['event_name'] == 'DECISION_REJECTED'
    assert old_decisions[0]['context']['reason_code'] == 'DECISION_REJECTED'
    old_rejection = old_decisions[0]['context']['rejection_artifact']
    assert old_rejection['rejection_stage'] == 'position_policy'
    assert old_rejection['rejection_reason'] == 'Active trade already open'
    accepted = new_decisions[0]
    assert accepted['event_name'] == 'DECISION_ACCEPTED'
    fills = [e for e in new.artifact['runtime_event_stream']
        if e['event_name'] in {'ENTRY_FILLED', 'EXIT_FILLED'}]
    assert [e['event_name'] for e in fills] == ['ENTRY_FILLED', 'EXIT_FILLED']
    assert [e['context']['wallet_commit_seq'] for e in fills] == [1, 2]
    assert fills[1]['event_ts'] == at(6) < accepted['event_ts'] == at(7)
    assert len(new.trades) == 1  # The new intent has no later eligible fill bar.
    assert fills[0]['context']['qty'] == fills[1]['context']['qty']
    assert new.trades[0]['fees_paid'] == pytest.approx(sum(e['context']['fee_paid'] for e in fills))
    assert accepted['context']['wallet_snapshot']['balances']['USD'] == pytest.approx(
        STARTING_CASH + new.trades[0]['net_pnl'])
    assert accepted['context']['wallet_commit_seq'] is None
    expired = [e for e in new.artifact['runtime_event_stream']
        if e['event_name'] == 'ORDER_LIFECYCLE_CHANGED'
        and e['context']['decision_id'] == accepted['context']['decision_id']
        and e['context']['state'] == 'expired']
    assert len(expired) == 1 and expired[0]['parent_id'] == accepted['event_id']


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
    lifecycle = [fact for batch in result.canonical_fact_batches
        for fact in batch.get('payload', {}).get('facts', [])
        if fact.get('fact_type') in {'trade_opened', 'trade_closed'}]
    assert [(fact['fact_type'], fact['trade']['position_commit_seq']) for fact in lifecycle] == [
        ('trade_opened', 1), ('trade_closed', 2)]


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
