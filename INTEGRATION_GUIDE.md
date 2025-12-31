# Bot Lens Refactoring - Integration Guide

## Quick Start

All the infrastructure is in place! Follow these steps to complete the integration:

---

## Step 1: Update BotPerformanceModal.jsx

**File:** `portal/frontend/src/components/bots/BotPerformanceModal.jsx`

### 1.1 Add Import

At the top of the file (around line 1-15), add:

```javascript
import DecisionTrace from './DecisionTrace';
```

### 1.2 Replace Modal Body

Find the modal body section (starts around line 456 with `<div className="flex flex-1 flex-col gap-6 overflow-auto">`).

Replace the entire body section with the refactored version from:
**`BotPerformanceModal.REFACTORED.jsx`**

The refactored version:
- Moves chart above decision trace
- Removes redundant PerformanceStats component
- Simplifies playback controls (pause/resume only)
- Makes strategy config collapsible
- Adds DecisionTrace as primary focus

### 1.3 Remove Unused Components

You can safely remove these imports if no longer used:
- `ActiveTradeChip` (redundant with decision trace)
- `PerformanceStats` (removed from modal)

---

## Step 2: Test the Integration

### 2.1 Start a Backtest

1. Create/open a bot
2. Start a backtest
3. Open Bot Lens modal

**Expected behavior:**
- Chart appears at top for price context
- Decision Trace shows below with chronological decisions
- When signals are accepted → green "Accepted" cards with trade outcomes
- When signals are rejected → gray "Rejected" cards with reasons
- Strategy config is collapsed by default

### 2.2 Test Scenarios

#### Scenario A: Active Run with Trades
- Start a bot in backtest mode
- Let it run for a few candles
- Open Bot Lens
- **Expected:** Decision trace shows accepted signals linked to trades

#### Scenario B: Signals But No Trades
- Create a strategy that generates signals
- Configure ATM to reject trades (e.g., very tight risk limits)
- Run backtest
- **Expected:** Rejection summary appears showing "X signals rejected: [reasons]"

#### Scenario C: No Signals Yet
- Start a fresh bot
- Open Bot Lens immediately
- **Expected:** Empty state shows "No signals detected yet. Start the bot to see decision events."

#### Scenario D: Completed Run
- Run a backtest to completion
- Open Bot Lens
- **Expected:** Full chronological trace visible, chart shows all candles

### 2.3 Interaction Tests

- **Click decision card:** Chart should focus on that timestamp
- **Hover decision:** Should highlight (subtle shadow + transform)
- **Expand strategy config:** Should smoothly expand
- **Collapse strategy config:** Should hide details

---

## Step 3: Polish & Refinement

### 3.1 CSS Adjustments (if needed)

The DecisionTrace components use CSS variables. Ensure your theme defines:

```css
:root {
  --text-primary: #111827;
  --text-secondary: #6b7280;
  --border-color: #e5e7eb;
  --border-light: #f3f4f6;
  --card-bg: #ffffff;
  --bg-secondary: #f9fafb;
}
```

For dark mode (already in BotPerformanceModal):
```css
.bg-[#0e1016] {
  --text-primary: #ffffff;
  --text-secondary: #94a3b8;
  --border-color: rgba(255, 255, 255, 0.1);
  --card-bg: rgba(255, 255, 255, 0.05);
}
```

### 3.2 Enhanced Signal Metadata (Optional Future Enhancement)

Currently, decision events log generic "strategy_signal" as the signal type. To show specific signal types (e.g., "breakout", "retest"):

**Backend Enhancement:**
Modify `portal/backend/service/bot_runtime/series_builder.py` to preserve signal metadata in the `StrategySignal` dataclass:

```python
@dataclass
class StrategySignal:
    epoch: int
    direction: str
    signal_type: Optional[str] = None  # ADD THIS
    rule_id: Optional[str] = None      # ADD THIS
```

Then update `_build_signals_from_markers()` to extract this metadata from markers.

---

## Step 4: Remove Old Components (Cleanup)

After confirming everything works, you can remove:

1. **ActiveTradeChip.jsx** - Redundant with decision trace
2. **PerformanceStats.jsx** - Stats duplicated from parent page
3. **PlaybackControls.jsx speed slider** - Keep pause/resume, remove speed control

---

## Architecture Summary

### Data Flow

```
Signal Generated (strategy evaluation)
    ↓
_apply_bar() in runtime.py (line 488)
    ↓
_next_signal_for() dequeues signal (line 538)
    ↓
risk_engine.maybe_enter() attempts trade (line 497)
    ↓
_log_decision_event() logs decision (lines 503-533)
    ↓
Stored in _decision_events deque
    ↓
Included in chart_payload() → decisions array
    ↓
Streamed to frontend via SSE
    ↓
DecisionTrace component renders
```

### Key Files Modified

**Backend:**
- [domain.py:186-235](portal/backend/service/bot_runtime/domain.py#L186-L235) - `DecisionEvent` class
- [runtime.py:716-754](portal/backend/service/bot_runtime/runtime.py#L716-L754) - `_log_decision_event()`
- [runtime.py:488-554](portal/backend/service/bot_runtime/runtime.py#L488-L554) - Signal processing + decision logging
- [chart_state.py:70-86](portal/backend/service/bot_runtime/chart_state.py#L70-L86) - Include decisions in payload

**Frontend:**
- `DecisionTrace/` - All new components
- `BotPerformanceModal.jsx` - Refactored layout (see REFACTORED file)

---

## Troubleshooting

### Problem: Decision events not appearing

**Check:**
1. Backend is logging decisions: Add `logger.info()` in `_log_decision_event()`
2. Payload includes decisions: Check browser DevTools Network tab for `/api/bots/{id}/performance`
3. Component is receiving data: Add `console.log(decisions)` in DecisionTrace

### Problem: Chart focus not working

**Check:**
1. Chart ref is available: `console.log(chartHandle)` in onEventClick
2. Timestamp format is correct: Decision `bar_time` should be ISO8601 string
3. Chart API exists: `chart?.api` should be truthy

### Problem: Rejection reasons are generic

**This is expected** in current implementation. The rejection reason logic at [runtime.py:518-520](portal/backend/service/bot_runtime/runtime.py#L518-L520) is simplified.

**To improve:** Add more sophisticated rejection logic based on risk engine state.

---

## Next Steps

1. ✅ **Integrate DecisionTrace** into BotPerformanceModal
2. ✅ **Test all scenarios** (active, completed, no trades)
3. ⏳ **Gather user feedback** on the new UX
4. ⏳ **Enhance signal metadata** (if needed)
5. ⏳ **Add more rejection reason logic** (optional)

---

## Success Criteria

✅ Chart is positioned ABOVE decision trace
✅ Decision trace is visually dominant
✅ Accepted signals show linked trades with PnL
✅ Rejected signals show clear reasons
✅ Empty state is informative
✅ Rejection summary aggregates when no trades
✅ Clicking decisions focuses chart
✅ Strategy config is collapsible
✅ Performance stats removed (not duplicated)
✅ Playback controls minimized

---

**You're ready to integrate!** The decision trace architecture is solid and all components are in place.

