# Reversal selection on the clean two-block prefix

`raw_forward_outcome@14` explicitly narrows selection to the first two complete
blocks of the original six-block design. This changes the evaluation/input
boundary and reporting population, while retaining the January recognition
origin and the scientific formation/matching procedure. Historical versions
and their evidence remain unchanged.

Use native Coinbase ETH-USD spot 15-minute candles in
`[2025-01-01 00:00,2025-03-29 00:00)` UTC: exactly 8,352 bars.
Completion decisions belong to `[2025-02-01,2025-03-29)` UTC. The two blocks are
February 1-March 1 and March 1-March 29. No future outcome tail is read.
No post-gap segment or implicit reset is part of this method.

## Unchanged procedure

Reuse canonical Candle preparation, the same sequential recognizer and the
same `_select_comparisons` kernel. Retain all retreat-inclusive chains without
expiry, size filters, deduplication or winner selection. Keep prior96 RMS sigma,
fractional boundary distance and strictly prior12bar movement; calipers1.25,
1.10 and0.25; same28day block, UTC6h slot and prior-completion stratum;14day
radius; own trailing-n control low; disjoint closed history through nominal
decision-plus12bar support; no control-clock reuse; equal completion-moment
weights. Later control completions do not censor a comparison.

Keep max_bars19,104 and max_candidates10,000 in recognition settings, preserving
the original resource-derived event namespace and B2-compatible tie-order
projection. Actual delivered input is exactly8,352 bars. Retain20million
comparison,120second,256MiB input and16MiB result limits. Reject missing bars,
source changes and delayed-close input; never compress or fill a gap.

Report block1,block2 and combined summaries, each normalized within its own
population. Preserve every unmatched chain, partial moment, candidate rejection,
context distribution and direct/ancestry component. No numeric coverage gate or
independent sample-size claim is introduced. The new internal selection manifest
canonicalizes JSON keys before hashing and declares its material projection;
the historical v12 manifest remains untouched.

## Compare with saved BTC evidence

Use the corresponding first two blocks from BTC B3 Check
`c31f3aed-e4d1-47d4-ae07-3387166960f7`; do not rerun BTC recognition/matching.
Same-block control restrictions make these two complete blocks self-contained.
Retain saved case/control choices, then normalize reported weights for the
two-block population. Compare symbol-specific coverage, formation/context mix,
calendar concentration and dependence. Do not pool symbols as independent
replications or infer outcomes from shorter-duration selection.

ETH's later April25 native gap remains recorded in the full-window Dataset.
This exact prefix does not cross it; no new acquisition or candle aggregation
is needed. Freeze/bind the prefix separately and verify its source, row count,
hashes and gap evidence before admission. Recorded earlier ETH research and
the gap-driven scope amendment remain visible; no unseen-confirmation claim.

Use normal qualified-source, global/duplicate/attempt admission and one
selection-only Check with saved evidence/readback. Stop on failure or completion;
no automatic replay, date/caliper change, outcome calculation or post-gap
initialization follows from this method.
