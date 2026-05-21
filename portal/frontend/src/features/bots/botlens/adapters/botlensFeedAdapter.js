export function projectDecisionTraceEntry(entry) {
  if (!entry || typeof entry !== 'object') return null
  const links = entry.links && typeof entry.links === 'object' ? entry.links : {}
  const cursor = entry.cursor && typeof entry.cursor === 'object' ? entry.cursor : {}
  return {
    event_id: entry.entry_id || null,
    domain_event_id: links.event_id || null,
    parent_event_id: links.parent_event_id || null,
    root_event_id: links.root_event_id || null,
    correlation_id: links.correlation_id || null,
    run_id: entry.run_id || null,
    bot_id: entry.bot_id || null,
    seq: Number(cursor.after_seq || 0) || 0,
    row_id: Number(cursor.after_row_id || 0) || 0,
    created_at: entry.recorded_at || null,
    event_ts: entry.occurred_at || null,
    event_type: String(entry.concern || '').trim().toLowerCase() || 'runtime',
    event_subtype: String(entry.entry_type || '').trim().toLowerCase() || 'event',
    symbol: entry.symbol || null,
    timeframe: entry.timeframe || null,
    instrument_id: entry.instrument_id || null,
    trade_id: entry.trade_id || null,
    signal_id: entry.signal_id || null,
    decision_id: entry.decision_id || null,
    direction: entry.direction || null,
    side: entry.side || null,
    qty: entry.qty ?? null,
    price: entry.price ?? null,
    event_impact_pnl: entry.event_impact_pnl ?? null,
    trade_net_pnl: entry.trade_net_pnl ?? null,
    reason_code: entry.reason_code || null,
    reason_detail: entry.reason_detail || null,
    context: entry.context && typeof entry.context === 'object' ? entry.context : {},
  }
}
