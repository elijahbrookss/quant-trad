import React from 'react';
import PropTypes from 'prop-types';
import './RejectionSummary.css';

/**
 * RejectionSummary - Aggregated view of why signals were rejected
 *
 * Shows when signals were detected but no trades were executed,
 * grouped by rejection reason
 */
export default function RejectionSummary({ total, groups }) {
  if (total === 0 || Object.keys(groups).length === 0) {
    return null;
  }

  return (
    <div className="rejection-summary">
      <div className="summary-header">
        <h3>Signal Activity</h3>
        <span className="summary-badge">{total} signal{total !== 1 ? 's' : ''} detected</span>
      </div>

      <p className="summary-text">
        {total} signal{total !== 1 ? 's' : ''} detected, but no trades executed.
      </p>

      <div className="rejection-groups">
        {Object.entries(groups).map(([reason, decisions]) => (
          <div key={reason} className="rejection-group">
            <span className="count">{decisions.length}</span>
            <span className="reason">{reason}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

RejectionSummary.propTypes = {
  total: PropTypes.number.isRequired,
  groups: PropTypes.objectOf(PropTypes.arrayOf(PropTypes.object)).isRequired,
};
