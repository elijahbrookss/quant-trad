# QuantLab Signal Overlay Time Drift (2026-03-23)

## Incident

- Scope: QuantLab indicator overlay preview, signal debugging, market profile merged value areas
- Symptom: a signal bubble could reference a historical `VAH`/`VAL` that was valid when the signal fired, while the visible overlay showed a later merged profile boundary
- User-facing effect: signal diagnostics looked internally inconsistent and made visual debugging untrustworthy
- Engineering effect: QuantLab preview violated the runtime contract by mixing a historical signal with a final-frame overlay snapshot

This was a correctness incident, not a data-corruption incident.

## What Happened

QuantLab signal preview already walked the indicator runtime bar-by-bar and captured signal events at the bar where they became true.

QuantLab overlay preview did not.

The overlay worker replayed the runtime correctly, but the service collected overlays only on the terminal walk-forward step for the entire requested window. For indicators whose overlays evolve after a signal fires, especially merged market-profile value areas, the final overlay state could be different from the state that existed at the signal bar.

That produced a misleading chart:

- signal bubble: historical truth
- overlay box: end-of-window truth

The two artifacts came from different times on the same runtime timeline.

## Root Cause

The service path under `portal/backend/service/indicators/indicator_service/api.py` treated all QuantLab overlay requests as “latest state” requests and only collected overlays on the final bar.

That was acceptable for the default current-state preview.

It was wrong for signal debugging because signal inspection is inherently historical. The UI had no canonical point-in-time overlay request, so the chart could only show the final merged overlay state.

## Fix

The system now supports canonical point-in-time overlay inspection:

- QuantLab overlay requests may include `cursor_epoch`
- the worker replays the same runtime timeline but collects overlays on the requested bar instead of the terminal bar
- `cursor_epoch` must align to a candle in the requested window; misaligned requests fail loud
- QuantLab signal inspection uses that cursor-time overlay request instead of inferring history from the current chart or from signal metadata alone

The default overlay preview still shows one state at a time: the latest state for the requested window.

QuantLab signal inspection pins that indicator’s overlay slice to the signal bar until inspection is cleared.

## Why We Did Not Render All Historical Overlay Revisions

Rendering every prior merged overlay revision simultaneously would preserve history, but it would also make the chart materially harder to read.

The chosen model keeps correctness and readability together:

- normal mode: latest/current overlay state
- inspect mode: overlay state at the selected signal time

This preserves one runtime timeline without turning the chart into a stack of overlapping historical ghosts.

## Permanent Lessons

- Historical debugging needs a canonical point-in-time read path, not a UI-side inference.
- Signals and overlays must resolve from the same bar on the same runtime timeline when they are inspected together.
- “Latest state” and “state at time T” are different products and must be modeled explicitly.
