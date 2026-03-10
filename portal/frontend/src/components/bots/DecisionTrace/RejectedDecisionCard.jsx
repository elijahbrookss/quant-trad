import React from 'react';
import PropTypes from 'prop-types';
import { formatTimestamp } from '../botPerformanceFormatters';
import './RejectedDecisionCard.css';

/**
 * RejectedDecisionCard - Shows a signal that did NOT lead to a trade
 *
 * Displays:
 * - Signal details (type, direction, price)
 * - Rejection reason (why the signal was not acted upon)
 */
export default function RejectedDecisionCard({ decision, onClick }) {
  const { direction, signal_type, price, reason, bar_time, symbol, strategy_name } = decision;
  const fmtPrice = (value) => (Number.isFinite(Number(value)) ? Number(value).toFixed(2) : '—');
  const signalLabel = signal_type ? signal_type.replace(/_/g, ' ') : 'strategy signal';
  const signalSummary = [
    symbol ? symbol.toUpperCase() : null,
    price !== undefined ? `@ ${fmtPrice(price)}` : null,
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div className="decision-card rejected" onClick={onClick}>
      <div className="decision-header">
        <span className="decision-pill rejected">Rejected</span>
        <span className="direction-badge neutral">
          {direction ? direction.toUpperCase() : 'N/A'}
        </span>
        <span className="signal-type">{signalLabel}</span>
        <span className="timestamp">{formatTimestamp(bar_time)}</span>
      </div>

      <div className="decision-body">
        <div className="decision-row">
          <span className="label">Signal</span>
          <span className="value">{signalSummary || '—'}</span>
        </div>

        <div className="rejection-reason">
          <span className="label">Reason</span>
          <span className="reason">{reason || 'No reason provided'}</span>
        </div>
        <div className="decision-meta">
          {strategy_name ? <span>{strategy_name}</span> : null}
        </div>
      </div>
    </div>
  );
}

RejectedDecisionCard.propTypes = {
  decision: PropTypes.shape({
    id: PropTypes.string.isRequired,
    direction: PropTypes.string,
    signal_type: PropTypes.string.isRequired,
    price: PropTypes.number,
    reason: PropTypes.string,
    bar_time: PropTypes.string.isRequired,
    symbol: PropTypes.string,
    strategy_name: PropTypes.string,
  }).isRequired,
  onClick: PropTypes.func,
};
