import React, { useMemo } from 'react';
import PropTypes from 'prop-types';
import DecisionTable from './DecisionTable';
import RejectionSummary from './RejectionSummary';
import EmptyState from './EmptyState';
import { isTradeLog } from '../botPerformanceFormatters';
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
export default function DecisionTrace({ decisions = [], logs = [], onEventClick }) {
  const { signals, accepted, rejected } = useMemo(() => {
    const signals = decisions.filter((d) => (d.event || '').includes('signal'));
    const accepted = decisions.filter((d) => d.decision === 'accepted');
    const rejected = decisions.filter((d) => d.decision === 'rejected');
    return { signals, accepted, rejected };
  }, [decisions]);

  const executionEvents = useMemo(
    () => logs.filter((entry) => isTradeLog(entry)),
    [logs],
  );

  // Group rejection reasons
  const rejectionGroups = useMemo(() => {
    const groups = {};
    rejected.forEach((decision) => {
      const reason = decision.reason || 'Unknown reason';
      if (!groups[reason]) {
        groups[reason] = [];
      }
      groups[reason].push(decision);
    });
    return groups;
  }, [rejected]);

  const hasAnyEvents = decisions.length > 0 || executionEvents.length > 0;

  // If no events yet, show appropriate empty state
  if (!hasAnyEvents) {
    return (
      <div className="decision-trace">
        <EmptyState message="No signals detected yet. Start the bot to see decision events." />
      </div>
    );
  }

  // If signals but no accepted trades, show rejection summary
  const hasSignals = signals.length > 0;
  const hasAccepted = accepted.length > 0;

  return (
    <div className="decision-trace">
      {hasSignals && !hasAccepted && (
        <RejectionSummary total={rejected.length} groups={rejectionGroups} />
      )}

      <DecisionTable
        decisions={decisions}
        executionEvents={executionEvents}
        onRowClick={onEventClick}
      />
    </div>
  );
}

DecisionTrace.propTypes = {
  decisions: PropTypes.arrayOf(
    PropTypes.shape({
      id: PropTypes.string.isRequired,
      event: PropTypes.string.isRequired,
      timestamp: PropTypes.string.isRequired,
      bar_time: PropTypes.string.isRequired,
      chart_time: PropTypes.string,
      trade_time: PropTypes.string,
      created_at: PropTypes.string,
      strategy_id: PropTypes.string.isRequired,
      strategy_name: PropTypes.string,
      symbol: PropTypes.string.isRequired,
      signal_type: PropTypes.string.isRequired,
      direction: PropTypes.string,
      price: PropTypes.number,
      rule_id: PropTypes.string,
      decision: PropTypes.string,
      reason: PropTypes.string,
      trade_id: PropTypes.string,
      conditions: PropTypes.array,
      metadata: PropTypes.object,
    })
  ),
  logs: PropTypes.arrayOf(PropTypes.object),
  onEventClick: PropTypes.func,
};
