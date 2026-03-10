import React from 'react';
import PropTypes from 'prop-types';
import DecisionTable from './DecisionTable';
import EmptyState from './EmptyState';
import './DecisionTrace.css';

/**
 * DecisionTrace - Primary focal point showing strategy-level decisions
 *
 * Displays chronological decision ledger with:
 * - Signal events (strategy signals detected)
 * - Accepted decisions (signal → trade)
 * - Rejected decisions (signal → no trade + reason)
 * - Execution events (entry/exit/stop/target)
 */
export default function DecisionTrace({ ledgerEvents = [], onEventClick }) {
  const hasAnyEvents = ledgerEvents.length > 0;

  // If no events yet, show appropriate empty state
  if (!hasAnyEvents) {
    return (
      <div className="decision-trace">
        <EmptyState message="No signals detected yet. Start the bot to see decision events." />
      </div>
    );
  }

  return (
    <div className="decision-trace">
      <DecisionTable
        ledgerEvents={ledgerEvents}
        onRowClick={onEventClick}
      />
    </div>
  );
}

DecisionTrace.propTypes = {
  ledgerEvents: PropTypes.arrayOf(PropTypes.object),
  onEventClick: PropTypes.func,
};
