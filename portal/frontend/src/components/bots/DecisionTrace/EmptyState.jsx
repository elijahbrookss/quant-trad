import React from 'react';
import PropTypes from 'prop-types';
import './EmptyState.css';

/**
 * EmptyState - Shown when no decision events have been logged yet
 */
export default function EmptyState({ message }) {
  return (
    <div className="decision-trace-empty">
      <p className="empty-message">{message}</p>
    </div>
  );
}

EmptyState.propTypes = {
  message: PropTypes.string.isRequired,
};
