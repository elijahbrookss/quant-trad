import React from 'react';
import PropTypes from 'prop-types';
import { formatTimestamp } from '../botPerformanceFormatters';

export default function SignalDecisionCard({ decision, onClick }) {
  const { direction, signal_type, price, bar_time, symbol } = decision;
  const fmtPrice = (value) => (Number.isFinite(Number(value)) ? Number(value).toFixed(2) : '—');
  const signalLabel = signal_type ? signal_type.replace(/_/g, ' ') : 'strategy signal';
  const signalSummary = [
    symbol ? symbol.toUpperCase() : null,
    price !== undefined ? `@ ${fmtPrice(price)}` : null,
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div className="decision-card signal" onClick={onClick}>
      <div className="decision-header">
        <span className="decision-pill neutral">Signal</span>
        <span className={`direction-badge ${direction || 'neutral'}`}>
          {direction ? direction.toUpperCase() : 'N/A'}
        </span>
        <span className="signal-type">{signalLabel}</span>
        <span className="timestamp">{formatTimestamp(bar_time)}</span>
      </div>
      <div className="decision-body">
        <div className="decision-row">
          <span className="label">Signal</span>
          <span className="value">{signalSummary || 'Awaiting decision'}</span>
        </div>
      </div>
    </div>
  );
}

SignalDecisionCard.propTypes = {
  decision: PropTypes.shape({
    direction: PropTypes.string,
    signal_type: PropTypes.string,
    price: PropTypes.number,
    bar_time: PropTypes.string.isRequired,
    symbol: PropTypes.string,
  }).isRequired,
  onClick: PropTypes.func,
};
