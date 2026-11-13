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


def readiness(rows):
    return report_data.get_result_readiness('persisted-reference-run',
        run_row={'run_id': 'persisted-reference-run', 'status': 'completed',
                 'summary': {'net_pnl': 0, 'total_trades': 0}},
        runtime_events=rows, stored_trades=[])


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


@pytest.mark.db
@pytest.mark.parametrize('case', ['filled', 'expired', 'wrong_decision', 'missing_terminal'])
def test_runtime_evidence_roundtrips_disposable_postgres_and_report(monkeypatch, tmp_path, case):
    """The normal isolated CI database runs this; no production or mocked DB."""
    from uuid import uuid4
    from sqlalchemy import delete
    from portal.backend.db import db
    from portal.backend.db.models import BotRecord, BotRunRecord, BotRunEventRecord, BotRunEventSeqAllocatorRecord
    from portal.backend.service.bots.botlens_canonical_facts import append_botlens_canonical_fact_batch
    from portal.backend.service.storage.repos import runs
    from portal.backend.service.storage.repos.runtime_events import list_bot_runtime_events
    from tests.helpers.builders import persisted_runtime_harness as h

    bot_id, run_id = str(uuid4()), str(uuid4())
    monkeypatch.setattr(h, 'BOT_ID', bot_id)
    monkeypatch.setattr(h, 'RUN_ID', run_id)
    result = _full_runtime(monkeypatch, tmp_path, never_fill=case != 'filled')
    batches = json.loads(json.dumps(result.canonical_fact_batches))
    for batch in batches:
        facts = batch['payload']['facts']
        for fact in list(facts):
            decision = fact.get('decision', {})
            context = decision.get('context', {})
            if (decision.get('event_name') == 'ORDER_LIFECYCLE_CHANGED'
                    and context.get('state') == 'expired'):
                if case == 'wrong_decision':
                    context['decision_id'] = 'another-decision-same-correlation'
                elif case == 'missing_terminal':
                    facts.remove(fact)
    try:
        with db.session() as session:
            session.add(BotRecord(id=bot_id, name='disposable evidence boundary regression'))
        runs.upsert_bot_run({**result.run_upsert, 'run_id': run_id})
        for batch in batches:
            append_botlens_canonical_fact_batch(**batch)
        persisted = list_bot_runtime_events(bot_id=bot_id, run_id=run_id, limit=5000)
        assert persisted
        if case != 'filled':
            assert report_data.get_result_readiness(run_id)['safe_to_compare'] is (case == 'expired')
            terminal = [r for r in persisted if r['payload']['event_name'] == 'ORDER_LIFECYCLE_CHANGED'
                        and r['payload']['context']['state'] == 'expired']
            assert len(terminal) == (0 if case == 'missing_terminal' else 1)
            if terminal:
                assert terminal[0]['payload']['context']['order_cumulative_filled_qty'] == 0
        else:
            exits = [r['payload']['context'] for r in persisted
                     if r['payload']['event_name'] == 'EXIT_FILLED']
            assert len(exits) == 1
            for key in ('book_data_capability', 'time_in_force', 'post_only'):
                assert exits[0][key] is not None
        # Exact append replay must not duplicate material order/fill evidence.
        before = [(r['event_id'], r['payload']) for r in persisted]
        for batch in batches:
            append_botlens_canonical_fact_batch(**batch)
        after = list_bot_runtime_events(bot_id=bot_id, run_id=run_id, limit=5000)
        assert [(r['event_id'], r['payload']) for r in after] == before
    finally:
        with db.session() as session:
            session.execute(delete(BotRunEventRecord).where(BotRunEventRecord.run_id == run_id))
            session.execute(delete(BotRunEventSeqAllocatorRecord).where(BotRunEventSeqAllocatorRecord.run_id == run_id))
            session.execute(delete(BotRunRecord).where(BotRunRecord.run_id == run_id))
            session.execute(delete(BotRecord).where(BotRecord.id == bot_id))
