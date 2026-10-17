# Completed reversal: six-block comparison selection

`raw_forward_outcome@12` extends the completed B2 comparison to one fixed
168-day decision window. It selects comparisons only. It calculates no forward
returns, first touches, continuation/failure classes, significance or power.

## Fixed population and data

Use provider-native Coinbase BTC-USD spot 15-minute candles. Recognition starts
at **2025-01-01 00:00 UTC** and continues without resets. Completion decisions
belong to **[2025-02-01 00:00, 2025-07-19 00:00) UTC**. Exactly 19,104 input
candles cover origin to end; selection needs no future suffix. Reject gaps,
changed source or non-close-available input. Historical availability is inferred
from interval close, not measured publication or receipt latency.

Keep B2's original three blocks and append three:

| Block | Start inclusive | End exclusive | Reporting population |
|---|---|---|---|
| 1 | February 1 | March 1 | Old |
| 2 | March 1 | March 29 | Old |
| 3 | March 29 | April 26 | Old |
| 4 | April 26 | May 24 | New |
| 5 | May 24 | June 21 | New |
| 6 | June 21 | July 19 | New |

All dates are 2025 at midnight UTC. Preserve B1, the original B2 Check, its
failed first attempt, selection manifest, Dataset and publication history.
The old interval has exposed recognition/features/selection results; its new
labels are not an untouched confirmation sample. Registered Check/backtest
metadata had no later 2025 exposure, which cannot prove external nonexposure.

## Unchanged scientific comparison

Reuse the [original procedure](reversal-selection-only.md): retreat-inclusive
recognition, every chain, no expiry/size/winner filter; match only prior RMS
volatility, fractional boundary distance and strictly prior 12-bar movement.
Retain the original calipers, same 28-day block, UTC six-hour slot and prior
completion stratum, 14-day radius, no reused control clock, own trailing-n low,
and disjoint closed within-pair history through nominal decision-plus-12-bars.
Later completions do not exclude or censor controls. Nominal support is
metadata; future prices or first-touch times are never inspected.

Extra blocks cannot provide controls for old-block cases. Formation histories
of at least 1,332 candles (333 hours) cannot fit disjoint support within the
14-day radius. More matches do not establish greater coverage of all formations
or independence. Do not change those restrictions to improve the result.

## Preserve B2 tie order while extending capacity

The recognizer includes resource settings in its configuration hash. Raising
`max_bars` therefore changes actual event IDs. Those new event IDs remain intact.
For deterministic same-clock matching order only, project each reference and
completion into B2's original settings namespace (`max_bars=12000`,
`max_candidates=10000`, with unchanged scientific choices). Store the resulting
`b2_compatibility_order_key` separately. It equals the original B2 event ID for
the identical prefix and preserves its relative tie order. It is not the actual
expanded event identity. Missing reference ancestry or key collisions fail.

Verify prefix candle identity and structural events before interpreting any
comparison with B2. Even with unchanged old pair choices, combined weights and
connected components change when the reporting population expands.

## Capacity and timing representation

Fixed limits are 19,104 candles, 10,000 references, 20 million candidate
comparisons, 120 seconds method wall time, 256 MiB method input and 16 MiB
scientific result. Exceeding a limit fails the attempt; never truncate, chunk,
retry, change dates or increase a limit to rescue coverage.

Timing view `reversal_stage_availability.v3` stores each stage's name, candle ID,
observed time and role-known time, with the parent-known time once per event.
The duplicated candle-known time equals observed time; usable parent-stage time
is max(role-known time, parent-known time). The explicit decoder reconstructs the
entire historical v2 timing view exactly. This removes repeated fields, not
scientific information. Historical v1 method output remains unchanged.

## Report old, new and combined populations

One admitted Check owns recognition, selection and all three summaries. Root
case and pair rows appear once and carry combined-population weights. Each
`populations.old`, `.new` and `.combined` summary separately normalizes equal
completion-moment weights across its own population. Partial moments keep
unmatched chains; report retained full-population weight, fully/partially/no-match
moments, duration/context shifts, day/block concentration and candidate-level
rejections. Do not treat rejection comparisons as disjoint excluded cases.

Report direct-support/pair and conservative ancestry components for all cases
plus selected controls and for matched cases plus controls, within each reporting
population. Recognition ancestry still starts at January 1. Neither pairs,
moments, days nor component counts establish an independent sample size.

Freeze exact native input/source/protocol identities, qualify the producing
source and refresh normal global/duplicate/attempt admission before one
selection-only run. No outcome run or further allocation follows automatically.
Keep operational approvals and attempt accounting in their separate receipts;
scientific summaries contain findings, methods and limitations.
