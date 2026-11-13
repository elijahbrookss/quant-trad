"""Fail-closed recognition of durable, exactly linked unfilled entry requests.

This is a read projection, never an order-state authority or a repair of old
records. Replacement/multi-attempt orders and partial fills require their own
complete accounting; they cannot use this narrow zero-fill completion proof.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

from core.events import parse_optional_datetime


def _payload(row: Mapping[str, Any]) -> Mapping[str, Any]:
    value = row.get('payload')
    return value if isinstance(value, Mapping) else {}


def _context(row: Mapping[str, Any]) -> Mapping[str, Any]:
    value = _payload(row).get('context')
    return value if isinstance(value, Mapping) else {}


def _number(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result.is_finite() else None


def _time(value: Any):
    try:
        return parse_optional_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None


def terminal_unfilled_trade_ids(
    *, run_id: str, decision_rows: Sequence[Mapping[str, Any]],
    execution_rows: Sequence[Mapping[str, Any]],
) -> set[str]:
    """Require a contiguous single-request chain linked to the exact decision.

    Hashes here identify the stored request/attempt/context, not independently
    recomputed source manifests. No evidence is inferred from terminal labels,
    correlation alone, absent trades, aggregate summaries or worker log text.
    """
    decisions: dict[str, list[Mapping[str, Any]]] = {}
    for row in decision_rows:
        context = _context(row)
        if context.get('decision_state') == 'accepted' and context.get('trade_id'):
            decisions.setdefault(str(context['trade_id']), []).append(row)
    proven: set[str] = set()
    for trade_id, parents in decisions.items():
        if len(parents) != 1:
            continue
        parent = parents[0]
        pc = _context(parent)
        pp = _payload(parent)
        decision_id = pc.get('decision_id')
        decision_time = _time(pp.get('event_ts'))
        expected_side = {'enter_long': 'buy', 'enter_short': 'sell'}.get(pc.get('intent'))
        if (not decision_id or not decision_time or not expected_side
                or parent.get('run_id') != run_id or pc.get('run_id') != run_id
                or not pc.get('bot_id')
                or not all(pp.get(key) for key in ('event_id', 'root_id', 'correlation_id'))):
            continue
        # Include contradictory identities linked to this decision, rather
        # than hiding them by filtering solely on the expected trade ID.
        related = [row for row in execution_rows
                   if (_context(row).get('trade_id') == trade_id
                       or _context(row).get('decision_id') == decision_id
                       or _payload(row).get('parent_id') == pp['event_id'])]
        if not related or any(_payload(row).get('event_name') != 'ORDER_LIFECYCLE_CHANGED'
                              for row in related):
            continue
        try:
            ordered = sorted(related, key=lambda row: int(_context(row)['order_event_seq']))
        except (KeyError, ValueError, TypeError):
            continue
        first = _context(ordered[0])
        stable = ('order_request_id', 'order_request_manifest_hash', 'attempt_id',
                  'order_attempt_manifest_hash', 'execution_context_hash', 'execution_policy_hash')
        if not all(first.get(key) for key in stable):
            continue
        quantity = _number(first.get('requested_qty'))
        if quantity is None or quantity <= 0:
            continue
        previous_state = None
        previous_time = decision_time
        seen: set[str] = set()
        valid = True
        # Only the zero-fill subset of the canonical lifecycle is accepted here.
        transitions = {None: {'requested'}, 'requested': {'validated', 'rejected'},
                       'validated': {'accepted', 'rejected'},
                       'accepted': {'open', 'expired', 'canceled', 'rejected'},
                       'open': {'expired', 'canceled', 'rejected'}}
        for seq, row in enumerate(ordered, 1):
            p, c = _payload(row), _context(row)
            at = _time(c.get('known_at'))
            event_id = p.get('event_id')
            if (not event_id or event_id in seen
                    or row.get('run_id') != run_id or c.get('run_id') != run_id
                    or c.get('bot_id') != pc.get('bot_id')
                    or any(c.get(key) != pc.get(key) for key in
                           ('instrument_id', 'series_key', 'signal_id', 'strategy_id'))
                    or c.get('decision_id') != decision_id
                    or c.get('trade_id') != trade_id
                    or p.get('parent_id') != pp['event_id']
                    or p.get('root_id') != pp['root_id']
                    or p.get('correlation_id') != pp['correlation_id']
                    or any(c.get(key) != first[key] for key in stable)
                    or isinstance(c.get('order_event_seq'), bool)
                    or c.get('order_event_seq') != seq
                    or c.get('previous_state') != previous_state
                    or c.get('state') not in transitions.get(previous_state, set())
                    or c.get('side') != expected_side
                    or at is None or at < previous_time
                    or _time(p.get('event_ts')) != at
                    or not c.get('order_lifecycle_replay_hash')
                    or c.get('replacement_attempt_id') is not None
                    or any(c.get(key) is not None for key in ('fill_id', 'fill_qty', 'fill_price', 'fill_fee'))
                    or any(_number(c.get(key)) != quantity for key in
                           ('requested_qty', 'attempt_requested_qty', 'attempt_remaining_qty', 'order_remaining_qty'))
                    or any(_number(c.get(key)) != 0 for key in
                           ('attempt_cumulative_filled_qty', 'order_cumulative_filled_qty'))):
                valid = False
                break
            seen.add(event_id)
            previous_state, previous_time = c['state'], at
        if valid and previous_state in {'expired', 'canceled', 'rejected'}:
            proven.add(trade_id)
    return proven
