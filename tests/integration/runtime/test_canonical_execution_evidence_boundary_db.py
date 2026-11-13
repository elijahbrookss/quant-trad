"""Real disposable PostgreSQL coverage; kept separate from normal unit discovery."""
from __future__ import annotations

import json

import pytest

from portal.backend.service.reports import report_data
from tests.integration.runtime.test_causal_resting_bar import _full_runtime

pytestmark = pytest.mark.db

@pytest.mark.parametrize('case', ['filled', 'expired', 'wrong_decision', 'missing_terminal', 'closed_trade_conflict'])
def test_runtime_evidence_roundtrips_disposable_postgres_and_report(monkeypatch, tmp_path, case):
    """The normal isolated CI database runs this; no production or mocked DB."""
    from uuid import uuid4
    from sqlalchemy import delete
    from portal.backend.db import db
    from portal.backend.db.models import BotRecord, BotRunRecord, BotTradeRecord, BotRunEventRecord, BotRunEventSeqAllocatorRecord
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
        if case == 'closed_trade_conflict':
            decision = next(r for r in persisted if r['payload']['event_name'] == 'DECISION_EMITTED')
            # Deliberately contradictory disposable evidence: a stored closed
            # trade may not be waved away by the zero-fill alternative.
            with db.session() as session:
                session.add(BotTradeRecord(id=decision['payload']['context']['trade_id'],
                    bot_id=bot_id, run_id=run_id, direction='long', status='closed'))
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
            session.execute(delete(BotTradeRecord).where(BotTradeRecord.run_id == run_id))
            session.execute(delete(BotRunRecord).where(BotRunRecord.run_id == run_id))
            session.execute(delete(BotRecord).where(BotRecord.id == bot_id))
