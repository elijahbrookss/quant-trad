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

### V4 observed closed sessions

V4 is an explicit alternative to v3's complete-native-day requirement. It admits
the observed subset of a closed UTC day, without claiming missing bars were
quiet or closure-verified. Native source clocks and one source identity are
required. Unknown availability fails preparation; explicit inferred-close
availability remains labeled as an assumption.

`profile_quality` distinguishes `complete_observed_slots` from
`observed_incomplete`, retains exact missing slots and per-session lineage, and
always reports `predictive_qualification=false`. Ready output is usable observed
context, not predictive certification. Missing-bar price impact is unknown.
Leave-one-session-out POC sensitivity describes observed member influence only.

Late-known source rows, when present in the selected source input, create later
daily revisions at their real availability projected to the chart grid. A
revision replaces its day in the current merge set, never adds another day or
changes past snapshots. Revised lineage is part of profile identity. Unfinished
days remain unavailable; no gaps are filled. The canonical frozen-reader revision
selection remains unchanged. See [ADR 0086](../../../../docs/architecture/decisions/0086-retain-observed-incomplete-market-profile-sessions.md).

### V3 source availability

Research requiring complete native UTC days selects an explicit `market_profile` v3 instance. V1 and v2
retain the historical source-open timing for reproducibility; they must not be
used to support new point-in-time predictive claims.

V3 preserves native 30-minute TPO calculations, value-area levels, structural
span projection and merge arithmetic. A daily input must contain the 48 unique
aligned native candles for its UTC day. An incomplete day is withheld and listed
in `runtime_diagnostics().profile_availability`; no candle is filled or derived.
This prevents a truncated prefix from advertising a partial daily profile that
would change when its future source suffix arrives. This version does not
interpret exchange calendars or certify sparse daily profiles.
V3 also requires a fixed finite positive `bin_size`; whole-window automatic
inference could otherwise change earlier profiles when future prices arrive.
The existing explicit default bin size is unchanged.

`source_start` and `source_end` remain the first and last source candle **opens**.
They describe geometry, never availability. `formed_at` is the UTC session close.
`source_known_at` is the maximum of session close and every included source
candle's canonical `known_at`. Missing availability columns on legacy frames use
the native interval close and disclose `inferred_native_close`; present but
missing, invalid or earlier-than-close clocks fail. An explicit `close_time`
must agree with the native 30-minute interval.

`known_at` is the ceiling of `source_known_at` to the chart timeframe. Runtime
adopts the profile only when that watermark is no later than the completed
chart candle's canonical availability, falling back to its interval close when
availability is absent. Missing chart interval end fails. A 23:30 source candle
closes at 00:00; a 15-minute chart first observes the profile on the 23:45 candle
at its 00:00 close. It cannot observe that profile at 23:45.

Overlays share this admission clock. Whole-input continuity diagnostics remain
build diagnostics and are excluded from the v3 per-bar overlay, since they
contain future-window information. Typed outputs retain candle-open observation
identity; signals/lifecycle retain completed-candle availability. The v2
first-return output schema remains unchanged in v3.

### Historical V1/V2 projection

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

This describes retained legacy replay behavior. The source-session end in that
implementation was the final native candle open. Its projection could admit a
profile before all source candles closed; v3 corrects that defect explicitly.

## Merge Rule

Merged profiles become valid when merge criteria are satisfied in time.
Known-at gating applies equally to merged and unmerged profiles.

Merge chains are contiguous.
- A later profile may extend the current merged cluster only if it overlaps the current active cluster when it becomes known.
- If an intervening profile breaks the overlap chain, the earlier merged cluster is closed.
- Later profiles must not reopen a closed cluster even if they overlap that earlier cluster's range.
