import React, { useMemo } from 'react';
import PropTypes from 'prop-types';
import AcceptedDecisionCard from './AcceptedDecisionCard';
import RejectedDecisionCard from './RejectedDecisionCard';
import ExecutionEventCard from './ExecutionEventCard';
import SignalDecisionCard from './SignalDecisionCard';
import './DecisionTimeline.css';

/**
 * DecisionTimeline - Chronological list of all decision events
 *
 * Shows accepted and rejected signals in timeline order
 */
export default function DecisionTimeline({ decisions = [], trades = [], executionEvents = [], onEventClick }) {
  const timelineEntries = useMemo(() => {
    const decisionEntries = decisions.map((decision) => ({
      kind: decision.decision === 'accepted' ? 'accepted' : decision.decision === 'rejected' ? 'rejected' : 'signal',
      time: decision.bar_time,
      decision,
    }));

    const executionEntries = executionEvents.map((event) => ({
      kind: 'execution',
      time: event.event_time || event.bar_time || event.timestamp,
      event,
    }));

    return [...decisionEntries, ...executionEntries]
      .filter((entry) => entry.time)
      .sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime());
  }, [decisions, executionEvents]);

  if (timelineEntries.length === 0) {
    return null;
  }

  return (
    <div className="decision-timeline">
      <h3 className="timeline-header">Decision Ledger</h3>
      <div className="timeline-list">
        {timelineEntries.map((entry, idx) => {
          if (entry.kind === 'accepted') {
            const decision = entry.decision;
            const trade = trades.find((t) => t.trade_id === decision.trade_id);
            return (
              <AcceptedDecisionCard
                key={decision.id || `decision-${idx}`}
                decision={decision}
                trade={trade}
                onClick={() =>
                  onEventClick && onEventClick(decision.bar_time, decision.price, decision.symbol)
                }
              />
            );
          }

          if (entry.kind === 'rejected') {
            const decision = entry.decision;
            return (
              <RejectedDecisionCard
                key={decision.id || `decision-${idx}`}
                decision={decision}
                onClick={() =>
                  onEventClick && onEventClick(decision.bar_time, decision.price, decision.symbol)
                }
              />
            );
          }

          if (entry.kind === 'execution') {
            const event = entry.event;
            return (
              <ExecutionEventCard
                key={event.id || `execution-${idx}`}
                event={event}
                onClick={() => onEventClick && onEventClick(entry.time, event.price, event.symbol)}
              />
            );
          }

          const decision = entry.decision;
          return (
            <SignalDecisionCard
              key={decision.id || `signal-${idx}`}
              decision={decision}
              onClick={() =>
                onEventClick && onEventClick(decision.bar_time, decision.price, decision.symbol)
              }
            />
          );
        })}
      </div>
    </div>
  );
}

DecisionTimeline.propTypes = {
  decisions: PropTypes.arrayOf(PropTypes.object),
  trades: PropTypes.arrayOf(PropTypes.object),
  executionEvents: PropTypes.arrayOf(PropTypes.object),
  onEventClick: PropTypes.func,
};
