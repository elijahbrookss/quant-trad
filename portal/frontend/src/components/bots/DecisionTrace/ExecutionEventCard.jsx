import React from 'react';
import PropTypes from 'prop-types';
import { describeLog, formatTimestamp } from '../botPerformanceFormatters';

const formatEventLabel = (value) => {
  if (!value) return 'Execution';
  return value.replace(/_/g, ' ');
};

export default function ExecutionEventCard({ event, onClick }) {
  const eventTime = event.event_time || event.bar_time || event.timestamp;
  const label = formatEventLabel(event.event || event.type || 'execution');
  const detail = describeLog(event);
  const rejectionReason = event.reason ? String(event.reason).replace(/_/g, ' ') : null;

  const metaParts = [
    event.trade_id ? `Trade ${event.trade_id.slice(0, 8)}` : null,
    event.leg ? `Leg ${event.leg}` : null,
    event.symbol ? event.symbol : null,
  ].filter(Boolean);

  return (
    <div className="decision-card execution" onClick={onClick}>
      <div className="decision-header">
        <span className="decision-pill execution">Execution</span>
        <span className="signal-type">{label}</span>
        <span className="timestamp">{formatTimestamp(eventTime)}</span>
      </div>
      <div className="decision-body">
        <div className="decision-row">
          <span className="label">Detail</span>
          <span className="value">{detail}</span>
        </div>
        {rejectionReason ? (
          <div className="decision-row">
            <span className="label">Reason</span>
            <span className="value">{rejectionReason}</span>
          </div>
        ) : null}
        {metaParts.length ? (
          <div className="decision-meta">
            {metaParts.map((part) => (
              <span key={part}>{part}</span>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

ExecutionEventCard.propTypes = {
  event: PropTypes.shape({
    event: PropTypes.string,
    type: PropTypes.string,
    event_time: PropTypes.string,
    bar_time: PropTypes.string,
    timestamp: PropTypes.string,
    trade_id: PropTypes.string,
    symbol: PropTypes.string,
    leg: PropTypes.string,
  }).isRequired,
  onClick: PropTypes.func,
};
