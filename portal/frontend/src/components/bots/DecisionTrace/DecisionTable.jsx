import React, { useEffect, useMemo, useState, useCallback } from 'react';
import PropTypes from 'prop-types';
import { X, ChevronLeft, ChevronRight } from 'lucide-react';
import './DecisionTable.css';

const PAGE_SIZE = 25;

/**
 * Format time with month/day and time (MMM DD HH:MM)
 */
const formatTimeWithDate = (value) => {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const month = date.toLocaleDateString('en-US', { month: 'short' });
  const day = date.getDate().toString().padStart(2, '0');
  const time = date.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  return `${month} ${day} ${time}`;
};

/**
 * Format date/time for detail modal
 */
const formatDateTime = (value) => {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
};

/**
 * Get event type badge styling
 */
const getEventBadge = (decision, kind, eventType) => {
  if (kind === 'execution') {
    // Color-code execution events by type
    const type = (eventType || '').toLowerCase();
    if (type === 'entry' || type === 'open') {
      return { label: 'ENTRY', color: 'bg-sky-500/15 text-sky-400 border-sky-500/30' };
    }
    if (type === 'close' || type === 'exit') {
      return { label: 'CLOSE', color: 'bg-violet-500/15 text-violet-400 border-violet-500/30' };
    }
    if (type === 'stop' || type === 'sl') {
      return { label: 'STOP', color: 'bg-rose-500/15 text-rose-400 border-rose-500/30' };
    }
    if (type === 'target' || type === 'tp') {
      return { label: 'TP', color: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30' };
    }
    return { label: 'EXEC', color: 'bg-slate-500/15 text-slate-400 border-slate-500/30' };
  }
  if (decision === 'accepted') {
    return { label: 'ACCEPT', color: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30' };
  }
  if (decision === 'rejected') {
    return { label: 'REJECT', color: 'bg-rose-500/15 text-rose-400 border-rose-500/30' };
  }
  return { label: 'SIGNAL', color: 'bg-slate-500/15 text-slate-400 border-slate-500/30' };
};

/**
 * Format currency value
 */
const formatCurrency = (value, currency) => {
  if (value === undefined || value === null) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  const formatted = num.toFixed(2);
  return currency ? `${formatted} ${currency}` : formatted;
};

/**
 * Format price
 */
const formatPrice = (value) => {
  if (value === undefined || value === null) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return num.toFixed(2);
};

/**
 * Build a contextual event description
 */
const buildEventDescription = (row) => {
  const type = (row.eventType || row.event || '').toLowerCase();
  const price = formatPrice(row.price);
  const direction = row.direction ? (row.direction === 'long' || row.direction === 'buy' ? 'Long' : 'Short') : null;
  const contracts = row.contracts;
  const pnl = row.pnl !== null ? Number(row.pnl) : null;
  const currency = row.currency || '';

  // Entry events
  if (type === 'entry' || type === 'open') {
    const parts = ['Opened'];
    if (direction) parts.push(direction);
    if (contracts) parts.push(`${contracts}x`);
    if (price) parts.push(`@ ${price}`);
    return parts.join(' ');
  }

  // Close events
  if (type === 'close' || type === 'exit') {
    if (pnl !== null) {
      const pnlStr = pnl >= 0 ? `+${pnl.toFixed(2)}` : pnl.toFixed(2);
      return `Closed for ${pnlStr} ${currency}`.trim();
    }
    return price ? `Closed @ ${price}` : 'Position closed';
  }

  // Stop events
  if (type === 'stop' || type === 'sl') {
    if (pnl !== null) {
      const pnlStr = pnl >= 0 ? `+${pnl.toFixed(2)}` : pnl.toFixed(2);
      return `Stop hit · ${pnlStr} ${currency}`.trim();
    }
    return price ? `Stop triggered @ ${price}` : 'Stop triggered';
  }

  // Target events
  if (type === 'target' || type === 'tp') {
    const legName = row.leg || '';
    if (pnl !== null) {
      const pnlStr = pnl >= 0 ? `+${pnl.toFixed(2)}` : pnl.toFixed(2);
      return legName ? `${legName} hit · ${pnlStr} ${currency}`.trim() : `Target hit · ${pnlStr} ${currency}`.trim();
    }
    return legName ? `${legName} hit @ ${price}` : `Target hit @ ${price}`;
  }

  // Signal accepted
  if (row.decision === 'accepted') {
    return direction ? `${direction} signal accepted` : 'Signal accepted';
  }

  // Signal rejected
  if (row.decision === 'rejected') {
    const reason = row.reason ? row.reason.replace(/_/g, ' ') : 'No reason';
    return reason;
  }

  // Fallback
  return row.label || type.replace(/_/g, ' ') || '—';
};

/**
 * Event Detail Modal - Shows full details when clicking a row
 */
function EventDetailModal({ event, onClose }) {
  if (!event) return null;

  const handleBackdropClick = (e) => {
    if (e.target === e.currentTarget) onClose();
  };

  // Extract all available data from the event
  const details = useMemo(() => {
    const items = [];

    // Core info
    if (event.symbol) items.push({ label: 'Symbol', value: event.symbol });
    if (event.direction) items.push({ label: 'Direction', value: event.direction.toUpperCase() });
    if (event.signal_type) items.push({ label: 'Signal Type', value: event.signal_type.replace(/_/g, ' ') });

    // Pricing
    if (event.price !== undefined) items.push({ label: 'Price', value: formatPrice(event.price) });
    if (event.entry_price !== undefined) items.push({ label: 'Entry Price', value: formatPrice(event.entry_price) });
    if (event.stop_price !== undefined) items.push({ label: 'Stop Price', value: formatPrice(event.stop_price) });
    if (event.exit_price !== undefined) items.push({ label: 'Exit Price', value: formatPrice(event.exit_price) });

    // Trade details
    if (event.contracts !== undefined) items.push({ label: 'Contracts', value: event.contracts });
    if (event.size !== undefined) items.push({ label: 'Size', value: event.size });
    if (event.quantity !== undefined) items.push({ label: 'Quantity', value: event.quantity });
    if (event.leverage !== undefined) items.push({ label: 'Leverage', value: `${event.leverage}x` });
    if (event.margin_type) items.push({ label: 'Margin Type', value: event.margin_type });

    // Fees and P&L - check multiple field names
    const feeValue = event.fees_paid ?? event.fees ?? event.fee ?? event.commission;
    if (feeValue !== undefined && feeValue !== null) {
      items.push({ label: 'Fees', value: formatCurrency(feeValue, event.fee_currency || event.currency) });
    }

    // P&L values - show gross first, then net if different
    if (event.gross_pnl !== undefined) {
      items.push({ label: 'Gross P&L', value: formatCurrency(event.gross_pnl, event.currency), tone: Number(event.gross_pnl) >= 0 ? 'positive' : 'negative' });
    }
    const netPnl = event.net_pnl ?? event.pnl;
    if (netPnl !== undefined) {
      items.push({ label: 'Net P&L', value: formatCurrency(netPnl, event.currency), tone: Number(netPnl) >= 0 ? 'positive' : 'negative' });
    }

    // IDs and references
    if (event.trade_id) items.push({ label: 'Trade ID', value: event.trade_id, mono: true });
    if (event.tradeId && event.tradeId !== event.trade_id) items.push({ label: 'Trade ID', value: event.tradeId, mono: true });
    if (event.order_id) items.push({ label: 'Order ID', value: event.order_id, mono: true });
    if (event.strategy_id) items.push({ label: 'Strategy ID', value: event.strategy_id, mono: true });
    if (event.strategy_name) items.push({ label: 'Strategy', value: event.strategy_name });
    if (event.rule_id) items.push({ label: 'Rule ID', value: event.rule_id, mono: true });
    if (event.leg) items.push({ label: 'Leg', value: event.leg });

    // Timing
    if (event.trade_time) items.push({ label: 'Trade Time', value: formatDateTime(event.trade_time) });
    if (event.bar_time) items.push({ label: 'Bar Time', value: formatDateTime(event.bar_time) });
    if (event.event_time) items.push({ label: 'Event Time', value: formatDateTime(event.event_time) });
    if (event.created_at) items.push({ label: 'Created At', value: formatDateTime(event.created_at) });
    if (event.filled_at) items.push({ label: 'Filled At', value: formatDateTime(event.filled_at) });

    // Reason/message
    if (event.reason) items.push({ label: 'Reason', value: event.reason.replace(/_/g, ' '), full: true });
    if (event.message) items.push({ label: 'Message', value: event.message, full: true });

    return items;
  }, [event]);

  // Extract metadata/conditions if present
  const metadata = event.metadata || {};
  const conditions = event.conditions || [];

  return (
    <div
      className="fixed inset-0 z-[60] flex items-center justify-center bg-black/60 p-4 backdrop-blur-sm"
      onClick={handleBackdropClick}
    >
      <div className="relative max-h-[80vh] w-full max-w-lg overflow-hidden rounded-xl border border-slate-700 bg-slate-900 shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
          <div className="flex items-center gap-2">
            <span className={`rounded border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider ${
              getEventBadge(event.decision, event.kind, event.eventType).color
            }`}>
              {getEventBadge(event.decision, event.kind, event.eventType).label}
            </span>
            <span className="text-sm font-medium text-slate-200">
              {buildEventDescription(event)}
            </span>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1.5 text-slate-500 transition hover:bg-slate-800 hover:text-slate-300"
          >
            <X className="size-4" />
          </button>
        </div>

        {/* Content */}
        <div className="max-h-[60vh] overflow-y-auto p-4">
          <div className="grid grid-cols-2 gap-3">
            {details.map((item, idx) => (
              <div
                key={`${item.label}-${idx}`}
                className={item.full ? 'col-span-2' : ''}
              >
                <p className="text-[10px] font-medium uppercase tracking-wider text-slate-500">
                  {item.label}
                </p>
                <p className={`mt-0.5 text-sm ${
                  item.tone === 'positive' ? 'text-emerald-400' :
                  item.tone === 'negative' ? 'text-rose-400' :
                  item.mono ? 'font-mono text-xs text-slate-400' :
                  'text-slate-200'
                }`}>
                  {item.value || '—'}
                </p>
              </div>
            ))}
          </div>

          {/* Conditions */}
          {conditions.length > 0 && (
            <div className="mt-4 border-t border-slate-800 pt-4">
              <p className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Conditions</p>
              <div className="mt-2 space-y-1">
                {conditions.map((cond, idx) => (
                  <div key={idx} className="flex items-center gap-2 text-xs">
                    <span className={`size-1.5 rounded-full ${cond.passed ? 'bg-emerald-500' : 'bg-rose-500'}`} />
                    <span className="text-slate-400">{cond.name || cond.condition || `Condition ${idx + 1}`}</span>
                    {cond.value !== undefined && (
                      <span className="text-slate-500">= {String(cond.value)}</span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Metadata */}
          {Object.keys(metadata).length > 0 && (
            <div className="mt-4 border-t border-slate-800 pt-4">
              <p className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Metadata</p>
              <pre className="mt-2 max-h-40 overflow-auto rounded-lg bg-slate-950 p-2 text-[10px] text-slate-400">
                {JSON.stringify(metadata, null, 2)}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default function DecisionTable({ decisions, executionEvents, onRowClick }) {
  const [selectedEvent, setSelectedEvent] = useState(null);

  const entries = useMemo(() => {
    // Build a map of trade_id -> direction from decisions (signal_accepted events have direction)
    const tradeDirectionMap = new Map();
    for (const decision of decisions) {
      if (decision.trade_id && decision.direction) {
        tradeDirectionMap.set(decision.trade_id, decision.direction);
      }
    }
    // Also check execution events for entry events which have direction
    for (const event of executionEvents) {
      const eventType = (event.event || event.type || '').toLowerCase();
      if (event.trade_id && event.direction && (eventType === 'entry' || eventType === 'open')) {
        tradeDirectionMap.set(event.trade_id, event.direction);
      }
    }

    const decisionRows = decisions.map((decision) => ({
      kind: 'decision',
      time: decision.trade_time || decision.chart_time || decision.bar_time || decision.timestamp,
      chartTime: decision.chart_time || decision.bar_time,
      createdAt: decision.created_at || decision.timestamp,
      symbol: decision.symbol,
      direction: decision.direction || decision.signal_direction,
      decision: decision.decision,
      eventType: decision.event,
      label: decision.event || (decision.decision === 'accepted' ? 'Signal Accepted' : decision.decision === 'rejected' ? 'Signal Rejected' : 'Signal'),
      price: decision.price || decision.signal_price,
      pnl: decision?.metadata?.net_pnl ?? decision?.metadata?.pnl ?? null,
      fees: decision?.metadata?.fees_paid ?? decision?.metadata?.fees ?? null,
      currency: decision?.metadata?.currency ?? null,
      contracts: decision?.metadata?.contracts ?? null,
      tradeId: decision.trade_id,
      reason: decision.reason,
      leg: decision.leg,
      // Pass through all original data for detail modal
      ...decision,
    }));

    const executionRows = executionEvents.map((event) => {
      const eventType = (event.event || event.type || '').toLowerCase();
      // Get direction from event, or look it up from the trade_id map
      let direction = event.direction;
      if (!direction && event.trade_id) {
        direction = tradeDirectionMap.get(event.trade_id);
      }

      // Extract P&L - check multiple possible field names from backend
      // Close events have: net_pnl, gross_pnl, fees_paid
      // Stop/target events have: pnl (at leg level)
      let pnlValue = null;
      if (event.net_pnl !== undefined && event.net_pnl !== null) {
        pnlValue = event.net_pnl;
      } else if (event.pnl !== undefined && event.pnl !== null) {
        pnlValue = event.pnl;
      } else if (event.gross_pnl !== undefined && event.gross_pnl !== null) {
        pnlValue = event.gross_pnl;
      }

      // Extract fees
      let feesValue = null;
      if (event.fees_paid !== undefined && event.fees_paid !== null) {
        feesValue = event.fees_paid;
      } else if (event.fees !== undefined && event.fees !== null) {
        feesValue = event.fees;
      } else if (event.fee !== undefined && event.fee !== null) {
        feesValue = event.fee;
      }

      return {
        kind: 'execution',
        time: event.trade_time || event.event_time || event.bar_time || event.timestamp,
        chartTime: event.chart_time || event.bar_time || event.event_time || event.timestamp,
        createdAt: event.created_at || event.timestamp,
        symbol: event.symbol,
        direction,
        decision: null,
        eventType,
        label: (event.event || event.type || 'execution').replace(/_/g, ' '),
        price: event.price,
        pnl: pnlValue,
        fees: feesValue,
        currency: event.currency ?? event.quote_currency ?? null,
        contracts: event.contracts ?? event.quantity ?? event.size ?? null,
        tradeId: event.trade_id,
        leg: event.leg,
        // Pass through all original data for detail modal
        ...event,
      };
    });

    return [...decisionRows, ...executionRows]
      .filter((row) => row.time)
      .sort((a, b) => new Date(b.createdAt || b.time).getTime() - new Date(a.createdAt || a.time).getTime());
  }, [decisions, executionEvents]);

  const [page, setPage] = useState(0);
  const pageCount = Math.max(1, Math.ceil(entries.length / PAGE_SIZE));

  useEffect(() => {
    setPage(0);
  }, [entries.length]);

  const pageStart = page * PAGE_SIZE;
  const pageEnd = pageStart + PAGE_SIZE;
  const currentRows = entries.slice(pageStart, pageEnd);

  const handleRowClick = useCallback((row) => {
    setSelectedEvent(row);
  }, []);

  const handleRowDoubleClick = useCallback((row) => {
    onRowClick?.(row.chartTime || row.time, row.price, row.symbol);
  }, [onRowClick]);

  return (
    <div className="decision-table">
      <div className="decision-table-header">
        <div>
          <p className="decision-table-kicker">Decision Ledger</p>
          <p className="decision-table-subtitle">Click row for details · Double-click to focus chart</p>
        </div>
        <div className="decision-table-controls">
          <button
            type="button"
            className="decision-table-nav"
            onClick={() => setPage((prev) => Math.max(prev - 1, 0))}
            disabled={page <= 0}
          >
            <ChevronLeft className="size-4" />
          </button>
          <span className="decision-table-page">
            {page + 1} / {pageCount}
          </span>
          <button
            type="button"
            className="decision-table-nav"
            onClick={() => setPage((prev) => Math.min(prev + 1, pageCount - 1))}
            disabled={page >= pageCount - 1}
          >
            <ChevronRight className="size-4" />
          </button>
        </div>
      </div>

      <div className="decision-table-scroll">
        <table>
          <thead>
            <tr>
              <th style={{ width: '110px' }}>Time</th>
              <th style={{ width: '70px' }}>Type</th>
              <th style={{ width: '120px' }}>Symbol</th>
              <th style={{ width: '60px' }}>Side</th>
              <th style={{ width: '90px' }}>Price</th>
              <th style={{ width: '90px' }}>P&L</th>
              <th style={{ width: '70px' }}>Fees</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            {currentRows.length ? (
              currentRows.map((row, idx) => {
                const badge = getEventBadge(row.decision, row.kind, row.eventType);
                const pnlValue = row.pnl !== null ? Number(row.pnl) : null;
                const pnlColor = pnlValue !== null
                  ? pnlValue > 0 ? 'pnl-positive' : pnlValue < 0 ? 'pnl-negative' : ''
                  : '';
                const feesValue = row.fees !== null ? Number(row.fees) : null;
                const description = buildEventDescription(row);

                return (
                  <tr
                    key={`${row.kind}-${row.time}-${idx}`}
                    className={row.kind}
                    onClick={() => handleRowClick(row)}
                    onDoubleClick={() => handleRowDoubleClick(row)}
                  >
                    <td className="tabular-nums">{formatTimeWithDate(row.time)}</td>
                    <td>
                      <span className={`decision-type-badge ${badge.color}`}>
                        {badge.label}
                      </span>
                    </td>
                    <td className="font-medium symbol-cell">{row.symbol || '—'}</td>
                    <td>
                      {row.direction ? (
                        <span className={`decision-pill ${row.direction}`}>
                          {row.direction === 'long' || row.direction === 'buy' ? 'LONG' : 'SHORT'}
                        </span>
                      ) : (
                        '—'
                      )}
                    </td>
                    <td className="tabular-nums">{formatPrice(row.price) || '—'}</td>
                    <td className={`tabular-nums font-medium ${pnlColor}`}>
                      {pnlValue !== null ? formatCurrency(pnlValue, row.currency) : '—'}
                    </td>
                    <td className="tabular-nums text-slate-500">
                      {feesValue !== null && feesValue > 0 ? `-${feesValue.toFixed(2)}` : '—'}
                    </td>
                    <td className="truncate event-label" title={description}>{description}</td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td colSpan={8} className="decision-table-empty">
                  No decision events yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Event Detail Modal */}
      {selectedEvent && (
        <EventDetailModal
          event={selectedEvent}
          onClose={() => setSelectedEvent(null)}
        />
      )}
    </div>
  );
}

DecisionTable.propTypes = {
  decisions: PropTypes.arrayOf(PropTypes.object).isRequired,
  executionEvents: PropTypes.arrayOf(PropTypes.object).isRequired,
  onRowClick: PropTypes.func,
};
