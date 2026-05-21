# Market Profile Timing Contract

## Scope

This document defines Market Profile-specific timing semantics.

## Core Semantics

For each profile, distinguish:
- structural span: region represented by the profile
- known-at time: when profile is valid for use

Historical span does not imply early availability.

## Consumption Rule

Signals and overlays should consume the same profile timeline.
A profile influences output when `known_at <= evaluation_time`.

## Strategy-Timeframe Projection Rule

Market Profile profiles are built from source-session data on the source timeframe.

When consumed on a strategy timeframe:
- `formed_at` remains the original source-session end,
- profile boundaries are projected to the strategy timeframe,
- `known_at` becomes the first closed strategy bar that can observe the profile.

Example:
- source session/profile ends at `2025-01-01T10:30:00Z`
- strategy timeframe is `1h`
- `formed_at = 10:30`
- projected profile end / `known_at = 11:00`

This preserves source-session computation while keeping runtime truth aligned to the strategy bar timeline.

## Merge Rule

Merged profiles become valid when merge criteria are satisfied in time.
Known-at gating applies equally to merged and unmerged profiles.

Merge chains are contiguous.
- A later profile may extend the current merged cluster only if it overlaps the current active cluster when it becomes known.
- If an intervening profile breaks the overlap chain, the earlier merged cluster is closed.
- Later profiles must not reopen a closed cluster even if they overlap that earlier cluster's range.
