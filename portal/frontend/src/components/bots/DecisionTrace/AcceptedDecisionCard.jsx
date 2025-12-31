import React from 'react';
import PropTypes from 'prop-types';
import { formatTimestamp } from '../botPerformanceFormatters';
import './AcceptedDecisionCard.css';

/**
 * AcceptedDecisionCard - Shows a signal that led to a trade
 *
 * Displays:
 * - Signal details (type, direction, price)
 * - Trade outcome (PnL, R-multiple) if trade is completed
 * - Rule that triggered the signal
 */
export default function AcceptedDecisionCard({ decision, trade, onClick }) {
  const { direction, signal_type, price, rule_id, bar_time, symbol, strategy_name } = decision;
  const fmtPrice = (value) => (Number.isFinite(Number(value)) ? Number(value).toFixed(2) : '—');
  const signalLabel = signal_type ? signal_type.replace(/_/g, ' ') : 'strategy signal';
  const entryPrice = trade?.entry_price ?? price;
  const stopPrice = trade?.stop_price;
  const targets = Array.isArray(trade?.legs)
    ? trade.legs
        .map((leg) => Number(leg?.target_price))
        .filter((value) => Number.isFinite(value))
    : [];
  const targetSummary = targets.length ? targets.map((value) => fmtPrice(value)).join(', ') : '—';
  const planSummaryParts = [
    entryPrice !== undefined ? `Entry ${fmtPrice(entryPrice)}` : null,
    stopPrice !== undefined ? `Stop ${fmtPrice(stopPrice)}` : null,
    targets.length ? `Targets ${targetSummary}` : null,
  ].filter(Boolean);
  const planSummary = planSummaryParts.length ? planSummaryParts.join(' • ') : 'Trade plan pending';

  return (
    <div className="decision-card accepted" onClick={onClick}>
      <div className="decision-header">
        <span className="decision-pill accepted">Accepted</span>
        <span className={`direction-badge ${direction || 'neutral'}`}>
          {direction ? direction.toUpperCase() : 'N/A'}
        </span>
        <span className="signal-type">{signalLabel}</span>
        <span className="timestamp">{formatTimestamp(bar_time)}</span>
      </div>

      <div className="decision-body">
        <div className="decision-row">
          <span className="label">Signal</span>
          <span className="value">
            {(symbol ? `${symbol.toUpperCase()} ` : '') + (price !== undefined ? `@ ${fmtPrice(price)}` : '')}
          </span>
        </div>
        <div className="decision-row">
          <span className="label">Trade Plan</span>
          <span className="value">{planSummary}</span>
        </div>
        <div className="decision-meta">
          {trade?.trade_id ? <span>Trade {trade.trade_id.slice(0, 8)}</span> : null}
          {strategy_name ? <span>{strategy_name}</span> : null}
        </div>
      </div>

      {rule_id && (
        <div className="decision-footer">
          <span className="rule-label">{rule_id}</span>
        </div>
      )}
    </div>
  );
}

AcceptedDecisionCard.propTypes = {
  decision: PropTypes.shape({
    id: PropTypes.string.isRequired,
    direction: PropTypes.string,
    signal_type: PropTypes.string.isRequired,
    price: PropTypes.number,
    rule_id: PropTypes.string,
    bar_time: PropTypes.string.isRequired,
    symbol: PropTypes.string,
    strategy_name: PropTypes.string,
  }).isRequired,
  trade: PropTypes.shape({
    trade_id: PropTypes.string,
    entry_price: PropTypes.number,
    stop_price: PropTypes.number,
    legs: PropTypes.arrayOf(PropTypes.object),
  }),
  onClick: PropTypes.func,
};
