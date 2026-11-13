"""Cross the producer filter and backend serialization, not just worker artifacts."""
from __future__ import annotations

import json

import pytest

from portal.backend.service.bots.botlens_canonical_facts import _build_canonical_fact_rows
from portal.backend.service.reports import report_data
from tests.integration.runtime.test_causal_resting_bar import _full_runtime


def canonical_rows(result):
    rows = []
    for batch in result.canonical_fact_batches:
        _, _, built = _build_canonical_fact_rows(**{
            key: batch[key] for key in ('bot_id', 'run_id', 'seq', 'batch_kind', 'payload')
        })
        # A JSON roundtrip catches reliance on in-memory typed objects.
        rows.extend(json.loads(json.dumps(built)))
    return rows


def readiness(rows, *, stored_trades=None):
    return report_data.get_result_readiness('persisted-reference-run',
        run_row={'run_id': 'persisted-reference-run', 'status': 'completed',
                 'summary': {'net_pnl': 0, 'total_trades': 0}},
        runtime_events=rows, stored_trades=stored_trades or [])


def test_exit_fill_retains_actual_execution_context_across_canonical_projection(monkeypatch, tmp_path):
    from engines.bot_runtime.core.domain.position import LadderPosition
    actual_exit_metadata = []
    original_projection = LadderPosition._execution_evidence_for_fill
    def capture(fill):
        if fill is not None:
            actual_exit_metadata.append(dict(fill.metadata))
        return original_projection(fill)
    monkeypatch.setattr(LadderPosition, '_execution_evidence_for_fill', staticmethod(capture))
    result = _full_runtime(monkeypatch, tmp_path)
    rows = canonical_rows(result)
    fills = [row['payload'] for row in rows
             if row['payload']['event_name'] in {'ENTRY_FILLED', 'EXIT_FILLED'}]
    assert {fill['event_name'] for fill in fills} == {'ENTRY_FILLED', 'EXIT_FILLED'}
    for fill in fills:
        original = next(e for e in result.artifact['runtime_event_stream']
                        if e['event_id'] == fill['context']['source_event_id'])
        for key in ('book_data_capability', 'time_in_force', 'post_only'):
            assert key in original['context']
            assert original['context'][key] is not None
            assert fill['context'][key] == original['context'][key]
            if fill['event_name'] == 'EXIT_FILLED':
                assert fill['context'][key] == actual_exit_metadata[0][key]


def test_expiry_survives_producer_filter_serialization_and_report_readiness(monkeypatch, tmp_path):
    result = _full_runtime(monkeypatch, tmp_path, never_fill=True)
    rows = canonical_rows(result)
    orders = [r for r in rows if r['payload']['event_name'] == 'ORDER_LIFECYCLE_CHANGED']
    assert [r['payload']['context']['state'] for r in orders] == [
        'requested', 'validated', 'accepted', 'open', 'expired']
    assert readiness(rows)['safe_to_compare'] is True
    assert readiness(rows)['accepted_unfilled_terminal_count'] == 1
    # Historical absence must remain incomplete; never synthesize old rows.
    without_terminal = [r for r in rows if r not in orders or r is not orders[-1]]
    assert readiness(without_terminal)['safe_to_compare'] is False


def test_fill_evidence_missing_at_execution_is_not_invented():
    from types import SimpleNamespace
    from engines.bot_runtime.core.domain.position import LadderPosition
    evidence = LadderPosition._execution_evidence_for_fill(SimpleNamespace(metadata={'post_only': False}))
    assert evidence == {'post_only': False}


def test_closed_stored_trade_cannot_be_explained_as_unfilled(monkeypatch, tmp_path):
    rows = canonical_rows(_full_runtime(monkeypatch, tmp_path, never_fill=True))
    decision = next(r for r in rows if r['payload']['event_name'] == 'DECISION_EMITTED')
    trade_id = decision['payload']['context']['trade_id']
    result = readiness(rows, stored_trades=[{'id': trade_id, 'status': 'closed'}])
    assert result['safe_to_compare'] is False
    assert result['missing_trade_opened'] == [trade_id]
    assert result['missing_trade_closed'] == [trade_id]
    assert result['accepted_unfilled_terminal_count'] == 0


@pytest.mark.parametrize('field', ['instrument_id', 'strategy_id', 'series_key', 'symbol'])
@pytest.mark.parametrize('absent', [None, '', '   '])
def test_required_identity_missing_on_both_sides_is_not_a_match(monkeypatch, tmp_path, field, absent):
    rows = canonical_rows(_full_runtime(monkeypatch, tmp_path, never_fill=True))
    for row in rows:
        row['payload']['context'][field] = absent
    assert readiness(rows)['safe_to_compare'] is False


def test_optional_signal_identity_can_be_absent_on_both_sides(monkeypatch, tmp_path):
    # CanonicalOrderRequest and the typed decision/lifecycle projections declare
    # signal_id optional. Exact decision/event/root/correlation still bind it.
    rows = canonical_rows(_full_runtime(monkeypatch, tmp_path, never_fill=True))
    for row in rows:
        row['payload']['context']['signal_id'] = None
    assert readiness(rows)['safe_to_compare'] is True


@pytest.mark.parametrize('terminal', ['expired', 'canceled', 'rejected'])
def test_terminal_unfilled_readiness_requires_exact_complete_lineage(monkeypatch, tmp_path, terminal):
    result = _full_runtime(monkeypatch, tmp_path, never_fill=True)
    rows = canonical_rows(result)
    orders = [r for r in rows if r['payload']['event_name'] == 'ORDER_LIFECYCLE_CHANGED']
    assert orders
    orders[-1]['payload']['context']['state'] = terminal
    assert readiness(rows)['safe_to_compare'] is True
    for key, value in [
        ('decision_id', 'wrong-decision'), ('trade_id', 'wrong-trade'),
        ('run_id', 'wrong-run'), ('order_request_id', 'wrong-request'),
        ('attempt_id', 'wrong-attempt'), ('order_event_seq', 99),
        ('order_cumulative_filled_qty', .5), ('attempt_cumulative_filled_qty', .5),
        ('order_remaining_qty', 0), ('replacement_attempt_id', 'replacement'),
        ('fill_id', 'unexpected-fill'), ('known_at', '2025-01-01T00:00:00Z'),
        ('instrument_id', 'wrong-instrument'), ('signal_id', 'wrong-signal'),
        ('execution_context_hash', 'wrong-context'), ('order_lifecycle_replay_hash', None),
        ('known_at', 'malformed'), ('order_cumulative_filled_qty', 'NaN'),
        ('order_cumulative_filled_qty', False), ('state', 'replaced'),
    ]:
        broken = json.loads(json.dumps(rows))
        broken_order = next(r for r in broken if r['payload']['event_id'] == orders[-1]['payload']['event_id'])
        broken_order['payload']['context'][key] = value
        assert readiness(broken)['safe_to_compare'] is False, key
    for key in ('parent_id', 'root_id', 'correlation_id'):
        broken = json.loads(json.dumps(rows))
        last = next(r for r in broken if r['payload']['event_id'] == orders[-1]['payload']['event_id'])
        last['payload'][key] = 'same-correlation-wrong-parent' if key == 'parent_id' else 'wrong'
        assert readiness(broken)['safe_to_compare'] is False, key
    assert readiness(rows + [json.loads(json.dumps(orders[-1]))])['safe_to_compare'] is False
    # Conflicting attempts, incomplete prefixes, and the wrong accepted parent
    # sharing the same correlation cannot close an intent.
    assert readiness([r for r in rows if r is not orders[1]])['safe_to_compare'] is False
    broken = json.loads(json.dumps(rows))
    decision = next(r for r in broken if r['payload']['event_name'] == 'DECISION_EMITTED')
    decision['payload']['context']['decision_id'] = 'other-same-correlation-decision'
    assert readiness(broken)['safe_to_compare'] is False
    extra = json.loads(json.dumps(orders[0]))
    extra['payload']['context']['trade_id'] = 'other-trade-same-decision'
    extra['payload']['context']['order_request_id'] = 'another-request'
    assert readiness(rows + [extra])['safe_to_compare'] is False
