import React, { useEffect, useMemo, useState } from 'react';
import PropTypes from 'prop-types';
import { describeLog, formatTimestamp } from '../botPerformanceFormatters';
import './DecisionTable.css';

const PAGE_SIZE = 25;

const formatDateTime = (value) => {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const dateLabel = date.toLocaleDateString('en-US', {
    month: 'short',
    day: '2-digit',
    year: 'numeric',
  });
  const timeLabel = formatTimestamp(value);
  return `${dateLabel} ${timeLabel}`;
};

const formatDecisionLabel = (decision) => {
  if (decision.decision === 'accepted') return 'Signal Accepted';
  if (decision.decision === 'rejected') return 'Signal Rejected';
  return 'Signal Received';
};

const formatSignalSummary = (decision) => {
  const parts = [];
  if (decision.signal_type) {
    parts.push(decision.signal_type.replace(/_/g, ' '));
  }
  if (decision.price !== undefined && decision.price !== null) {
    parts.push(`@ ${Number(decision.price).toFixed(2)}`);
  }
  return parts.join(' ');
};

const formatDecisionDetail = (decision) => {
  if (decision.decision === 'rejected') {
    return decision.reason || 'Rejected';
  }
  if (decision.decision === 'accepted') {
    return decision.reason || 'Accepted by strategy rules';
  }
  return decision.reason || 'Awaiting decision';
};

const formatPnl = (value, currency) => {
  if (value === undefined || value === null) return '—';
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '—';
  const formatted = numeric.toFixed(2);
  return currency ? `${formatted} ${currency}` : formatted;
};

const formatExecutionLabel = (event) => {
  if (event.event) return event.event.replace(/_/g, ' ');
  if (event.type) return event.type.replace(/_/g, ' ');
  return 'Execution';
};

export default function DecisionTable({ decisions, executionEvents, onRowClick }) {
  const entries = useMemo(() => {
    const decisionRows = decisions.map((decision) => ({
      kind: 'decision',
      time: decision.trade_time || decision.chart_time || decision.bar_time || decision.timestamp,
      chartTime: decision.chart_time || decision.bar_time,
      createdAt: decision.created_at || decision.timestamp,
      symbol: decision.symbol,
      direction: decision.direction,
      label: formatDecisionLabel(decision),
      detail: `${formatSignalSummary(decision)}${formatSignalSummary(decision) ? ' • ' : ''}${formatDecisionDetail(decision)}`,
      tradeId: decision.trade_id,
      price: decision.price,
      pnl: decision?.metadata?.net_pnl ?? decision?.metadata?.pnl ?? null,
      currency: decision?.metadata?.currency ?? null,
    }));

    const executionRows = executionEvents.map((event) => ({
      kind: 'execution',
      time: event.trade_time || event.event_time || event.bar_time || event.timestamp,
      chartTime: event.chart_time || event.bar_time || event.event_time || event.timestamp,
      createdAt: event.created_at || event.timestamp,
      symbol: event.symbol,
      direction: event.direction,
      label: formatExecutionLabel(event),
      detail: describeLog(event),
      tradeId: event.trade_id,
      price: event.price,
      pnl: event.pnl ?? event.net_pnl ?? event.gross_pnl ?? null,
      currency: event.currency ?? event.quote_currency ?? null,
    }));

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

  return (
    <div className="decision-table">
      <div className="decision-table-header">
        <div>
          <p className="decision-table-kicker">Decision Ledger</p>
          <p className="decision-table-subtitle">Chronological, export-ready trace of strategy decisions.</p>
        </div>
        <div className="decision-table-controls">
          <button
            type="button"
            className="decision-table-button"
            onClick={() => setPage((prev) => Math.max(prev - 1, 0))}
            disabled={page <= 0}
          >
            Prev
          </button>
          <span className="decision-table-page">
            Page {page + 1} / {pageCount}
          </span>
          <button
            type="button"
            className="decision-table-button"
            onClick={() => setPage((prev) => Math.min(prev + 1, pageCount - 1))}
            disabled={page >= pageCount - 1}
          >
            Next
          </button>
        </div>
      </div>
      <div className="decision-table-scroll">
        <table>
          <thead>
            <tr>
              <th>Trade time</th>
              <th>Symbol</th>
              <th>Direction</th>
              <th>Event</th>
              <th>Detail</th>
              <th>P/L</th>
              <th>Trade</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {currentRows.length ? (
              currentRows.map((row, idx) => (
                <tr
                  key={`${row.kind}-${row.time}-${idx}`}
                  className={row.kind}
                  onClick={() => onRowClick?.(row.chartTime || row.time, row.price, row.symbol)}
                >
                  <td>{formatDateTime(row.time)}</td>
                  <td>{row.symbol || '—'}</td>
                  <td>
                    {row.direction ? (
                      <span className={`decision-pill ${row.direction}`}>{row.direction.toUpperCase()}</span>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td>{row.label}</td>
                  <td>{row.detail || '—'}</td>
                  <td>{formatPnl(row.pnl, row.currency)}</td>
                  <td>{row.tradeId ? row.tradeId.slice(0, 8) : '—'}</td>
                  <td>{formatDateTime(row.createdAt)}</td>
                </tr>
              ))
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
    </div>
  );
}

DecisionTable.propTypes = {
  decisions: PropTypes.arrayOf(PropTypes.object).isRequired,
  executionEvents: PropTypes.arrayOf(PropTypes.object).isRequired,
  onRowClick: PropTypes.func,
};
