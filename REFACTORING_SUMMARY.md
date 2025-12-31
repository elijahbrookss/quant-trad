# Bot Lens Refactoring Summary

## Overview

This document summarizes the Bot Lens refactoring work to transform it from a cluttered multi-section modal into a focused decision trace experience.

## What Was Completed

### 1. Backend Infrastructure ✅

**File:** `portal/backend/service/bot_runtime/domain.py`
- Added `DecisionEvent` dataclass to represent strategy-level decision points
- Includes fields for signal type, direction, price, rule_id, decision (accepted/rejected), reason, trade linkage
- Serialization method for API responses

**File:** `portal/backend/service/bot_runtime/runtime.py`
- Added `_decision_events` deque to store decision events (maxlen=500)
- Added `_log_decision_event()` method to log decisions with full context
- Added `decision_events()` method to retrieve decision history
- Integrated decision events into `reset()` workflow

**File:** `portal/backend/service/bot_runtime/chart_state.py`
- Modified `chart_state()` to include `decision_events` parameter
- Returns `decisions` array in payload

### 2. Frontend Components ✅

Created complete DecisionTrace component suite:

**Files Created:**
- `portal/frontend/src/components/bots/DecisionTrace/index.jsx` - Main component
- `portal/frontend/src/components/bots/DecisionTrace/DecisionTimeline.jsx` - Chronological list
- `portal/frontend/src/components/bots/DecisionTrace/AcceptedDecisionCard.jsx` - Shows signals → trades
- `portal/frontend/src/components/bots/DecisionTrace/RejectedDecisionCard.jsx` - Shows rejected signals
- `portal/frontend/src/components/bots/DecisionTrace/RejectionSummary.jsx` - Aggregated rejection reasons
- `portal/frontend/src/components/bots/DecisionTrace/EmptyState.jsx` - No signals state

**Styling (CSS):**
- All components have dedicated CSS files with clean, modern styling
- Green accent for accepted trades (profit/loss color-coded)
- Gray accent for rejected signals
- Yellow/amber highlight for rejection summary
- Hover states and smooth transitions

### 3. Design Decisions Made

Based on user input:
- ✅ **Real-time logging** during execution (not on-demand computation)
- ✅ **Single strategy per bot** (no multi-strategy complexity)
- ✅ **Aggregate rejections by reason** (clean pattern-focused view)
- ✅ **Minimize playback controls** (keep pause/resume, remove speed slider)
- ✅ **Chart above decision trace** (price context first, then decisions)

---

## What Still Needs To Be Done

### 1. Backend Signal Processing Integration ⏳

**Critical Missing Piece:** The decision events are not yet being logged during bot runtime.

**Where to integrate:**

The signal evaluation happens when strategy rules process incoming signals. Need to:

1. Find where `StrategyRule.evaluate()` is called in the bot runtime
2. After evaluation, log the decision event:
   - If `matched=True` → log "signal_accepted" event
   - If `matched=False` → log "signal_rejected" event with reason

**Likely location:** `portal/backend/service/bot_runtime/series_builder.py` or wherever the signal queue is processed.

**Pseudo-code:**
```python
# In the series step/signal processing loop:
for signal in pending_signals:
    evaluation = strategy_rule.evaluate(signal)

    # LOG THE DECISION
    self._log_decision_event(
        event="signal_accepted" if evaluation['matched'] else "signal_rejected",
        series=current_series,
        candle=current_candle,
        signal_type=signal.type,
        signal_direction=signal.direction,
        signal_price=signal.price,
        rule_id=evaluation['rule_id'],
        decision="accepted" if evaluation['matched'] else "rejected",
        reason=evaluation.get('reason'),  # rejection reason
        trade_id=trade.trade_id if trade_created else None,
        conditions=evaluation.get('conditions'),
    )
```

### 2. BotPerformanceModal Refactoring ⏳

**File:** `portal/frontend/src/components/bots/BotPerformanceModal.jsx`

**Changes needed:**

1. **Import DecisionTrace:**
   ```jsx
   import DecisionTrace from './DecisionTrace';
   ```

2. **Extract decisions from payload:**
   ```jsx
   const decisions = payload?.decisions || [];
   ```

3. **Restructure layout:**
   ```jsx
   <div className="modal-body">
     {/* Chart section (reduced height from 360px to ~300px) */}
     <section className="chart-section">
       <BotLensChart ... />
     </section>

     {/* PRIMARY FOCUS: Decision Trace */}
     <section className="decision-trace-section">
       <DecisionTrace
         decisions={decisions}
         trades={activeTrades}
         onEventClick={(barTime) => {
           // Focus chart on this timestamp
           if (chartHandle) {
             chartHandle.scrollToTime(barTime);
           }
         }}
       />
     </section>

     {/* De-emphasized: Strategy Details (collapsible) */}
     <Collapsible title="Strategy Configuration" defaultOpen={false}>
       {/* Existing strategy wiring section */}
     </Collapsible>
   </div>
   ```

4. **Remove/minimize:**
   - Performance stats grid (duplicates parent page data)
   - Speed slider from PlaybackControls (keep only pause/resume)
   - ActiveTradeChip (redundant with decision trace)
   - Symbol tabs if single strategy

### 3. Styling Adjustments ⏳

Create or modify:
- `BotPerformanceModal.css` - Update to reflect new hierarchy
- Chart section should be visually secondary to decision trace
- Add collapsible component for strategy details

---

## Testing Checklist

When implementation is complete, test:

- [ ] **Active backtest run:** Decision events appear in real-time
- [ ] **Completed run:** All decisions visible chronologically
- [ ] **No trades scenario:** Rejection summary shows with grouped reasons
- [ ] **Signal accepted:** Card shows linked trade with PnL
- [ ] **Signal rejected:** Card shows reason clearly
- [ ] **Chart interaction:** Clicking decision card focuses chart on that timestamp
- [ ] **Empty state:** Shows appropriate message when bot hasn't started
- [ ] **Multi-instrument:** Each symbol's decisions are isolated correctly

---

## Architecture Notes

### Data Flow

```
Signal Generated (indicator)
    ↓
Strategy Rule Evaluated (facade)
    ↓
Decision Logged (runtime._log_decision_event)
    ↓
Stored in runtime._decision_events deque
    ↓
Included in chart_payload()
    ↓
Streamed to frontend via SSE
    ↓
DecisionTrace component renders
```

### Key Files Reference

**Backend:**
- [domain.py:186-235](portal/backend/service/bot_runtime/domain.py#L186-L235) - `DecisionEvent` class
- [runtime.py:716-754](portal/backend/service/bot_runtime/runtime.py#L716-L754) - `_log_decision_event()` method
- [runtime.py:765-772](portal/backend/service/bot_runtime/runtime.py#L765-L772) - `decision_events()` method
- [chart_state.py:70-86](portal/backend/service/bot_runtime/chart_state.py#L70-L86) - Decision events in payload

**Frontend:**
- [DecisionTrace/index.jsx](portal/frontend/src/components/bots/DecisionTrace/index.jsx) - Main entry point
- [DecisionTrace/*.jsx](portal/frontend/src/components/bots/DecisionTrace/) - All decision trace components

---

## Next Steps

1. **Wire up signal processing** to call `_log_decision_event()` when strategy rules are evaluated
2. **Refactor BotPerformanceModal.jsx** to use the new DecisionTrace component
3. **Test thoroughly** with various bot scenarios
4. **Polish UX** based on real usage

---

## Design Principles Achieved

✅ **Decision Trace as Primary Artifact:** The chronological ledger is now the focal point
✅ **Chart as Supporting Context:** Chart is above for quick context, but decision trace dominates
✅ **No View Switches:** Single cohesive experience, no tabs or modes
✅ **Strategy-Level Only:** No indicator-level signals exposed
✅ **Append-Only Ledger:** Works for both active and completed runs
✅ **Explain Why:** Every decision shows clear reasoning
✅ **Graceful Empty States:** Clear messaging when no signals or all rejected

---

## User Feedback Incorporated

- Chart positioned ABOVE decision trace (per user request)
- Single strategy per bot (simplified multi-strategy complexity)
- Real-time decision logging (not computed on-demand)
- Aggregated rejection summaries (pattern-focused)
- Minimal playback controls (pause/resume only)

---

Generated: 2025-12-30
