-- Read-only BotLens forensic invariant checks.
-- Bind :run_id to the target run. Optional :sim_start / :sim_end may be
-- supplied explicitly; otherwise the query reads the run window from
-- portal_bot_runs.

-- Trade hot bar_time must be inside the simulated run window.
with target_run as (
    select
        run_id,
        backtest_start as sim_start,
        backtest_end as sim_end
    from portal_bot_runs
    where run_id = :run_id
)
select e.id, e.event_name, e.event_time, e.bar_time, e.known_at, e.created_at
from portal_bot_run_events e
join target_run r on r.run_id = e.run_id
where e.run_id = :run_id
  and e.event_name in ('TRADE_OPENED', 'TRADE_CLOSED')
  and (
    e.bar_time is null
    or e.bar_time < coalesce(:sim_start, r.sim_start)
    or e.bar_time > coalesce(:sim_end, r.sim_end)
  )
order by e.seq, e.id
limit 200;

-- Trade event time, hot bar_time, and payload context event_time should agree.
select e.id, e.event_name, e.event_time, e.bar_time, e.payload->'context'->>'event_time' as payload_event_time
from portal_bot_run_events e
where e.run_id = :run_id
  and e.event_name in ('TRADE_OPENED', 'TRADE_CLOSED')
  and (
    e.bar_time is null
    or e.event_time is distinct from e.bar_time
    or nullif(e.payload->'context'->>'event_time', '')::timestamp is distinct from e.bar_time
  )
order by e.seq, e.id
limit 200;

-- Rejected decisions must not claim nonexistent trades.
select e.id, e.event_name, e.trade_id, e.reason_code
from portal_bot_run_events e
left join portal_bot_trades t on t.id = e.trade_id
where e.run_id = :run_id
  and e.event_name = 'DECISION_EMITTED'
  and e.payload->'context'->>'decision_state' = 'rejected'
  and e.trade_id is not null
  and t.id is null
order by e.seq, e.id
limit 200;

-- Rejected decisions without trades must carry an explicit attempt/request identity.
select
    e.id,
    e.event_name,
    e.trade_id,
    e.reason_code,
    e.payload->'context'->>'attempt_id' as attempt_id,
    e.payload->'context'->>'entry_request_id' as entry_request_id,
    e.payload->'context'->>'order_request_id' as order_request_id,
    e.payload->'context'->>'settlement_attempt_id' as settlement_attempt_id,
    e.payload->'context'->>'blocking_trade_id' as blocking_trade_id
from portal_bot_run_events e
where e.run_id = :run_id
  and e.event_name = 'DECISION_EMITTED'
  and e.payload->'context'->>'decision_state' = 'rejected'
  and e.trade_id is null
  and coalesce(
      nullif(e.payload->'context'->>'attempt_id', ''),
      nullif(e.payload->'context'->>'entry_request_id', ''),
      nullif(e.payload->'context'->>'order_request_id', ''),
      nullif(e.payload->'context'->>'settlement_attempt_id', ''),
      nullif(e.payload->'context'->>'blocking_trade_id', '')
  ) is null
order by e.seq, e.id
limit 200;

-- Rejected decisions should preserve concrete reason codes.
select reason_code, count(*)
from portal_bot_run_events
where run_id = :run_id
  and event_name = 'DECISION_EMITTED'
  and payload->'context'->>'decision_state' = 'rejected'
group by reason_code
order by count(*) desc, reason_code;

-- Accepted decisions with trades should join to persisted trades.
select e.id, e.decision_id, e.trade_id, e.reason_code
from portal_bot_run_events e
left join portal_bot_trades t on t.id = e.trade_id
where e.run_id = :run_id
  and e.event_name = 'DECISION_EMITTED'
  and e.payload->'context'->>'decision_state' = 'accepted'
  and (e.trade_id is null or t.id is null)
order by e.seq, e.id
limit 200;
