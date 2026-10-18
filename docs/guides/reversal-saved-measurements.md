# Saved selection measurements

`raw_forward_outcome@16` consumes a retained v12 six-block or v14 two-block
selection Check and its exact original frozen native 15-minute input. One new
version composes the existing `describe_selected_pairs` and `describe_progress`
functions. It does not recognize, rematch, change weights or choose new barriers.
Historical v13/v15 BTC consumers, definitions, results and replay stay unchanged.

## Binding and discovery

Use the existing method catalog/requirements/prepare/dispatch interfaces. The
catalog example is shape-valid metadata, not an executable reference to a real
Study. Bind `selection_reference` to the saved `source_check_id`, typed
`result_hash`, `evidence_hash` and `selection_manifest_hash`. Declare the exact
`frozen_context`: Dataset ID/hash, input binding hash, global watermark,
instrument, source identity, original input and decision bounds, input rows,
case/pair counts and matched completion clocks. Request scope and Dataset must
match that context; evidence mode, reject gaps and no assertions are fixed.

Declaration remains a pure planning operation and does not read saved Checks.
At execution, the adapter reloads the immutable source through the existing
repository, validates typed definition/request/plan/result/evidence linkage and
the registered pinned source protocol, then derives and compares every context
field. It checks the internal selection manifest, fixed rules, counts, original
completion-moment weights, pair identities and nominal support. Only the v12
histogram keys are losslessly restored to integers for its historical internal
hash; v14 uses its declared JSON-canonical projection. No historical object is
rewritten. The Scientific Attempt resolver is not called because it replays.

The actual ordinary frozen read binding must equal the saved binding, including
Dataset material/provenance, native source, subject, range and watermark. The
source must be one exact historical native candle source, with no gaps. Validate
contiguous input clocks and inferred close availability. No alternate provider,
internal aggregation, reset, synthetic fill or future tail is admitted.

## Fixed measurements

Keep every saved pair, both arms' own E/L/U geometry and original weights. E is
the observational close, U=E+(E-L), and the horizon is the next twelve 15-minute bars.
Inclusive touches, same-bar ambiguity, neither and boundary censoring follow the
unchanged first-passage kernel. Its computed result is passed directly in memory
to the unchanged progress kernel under explicit `computed_in_this_check`
provenance. No persisted outcome Check or ID is fabricated and the historical
saved-outcome validator is not weakened.

Report first-passage fractions and fixed progress distributions over the whole
saved scope, with saved block/day first-passage views. Progress retains fixed
quarter-target bins and p10/p25/p50/p75/p90 original-weight quantiles, both arms,
all pairs and each arm's originally-neither subset. Full-window highs may follow
failure and never reclassify first passage. Pre-failure OHLC bounds preserve
unknown intrabar ordering; censored counts and original weight mass remain.
The result keeps saved support/ancestry components and an explicit JSON-stable
measurement manifest. It makes no pooled independence or inferential claim.

For a BTC/ETH same-period report, use the already saved BTC per-pair results on
the aligned February 1–March 29 scope with original weights. Keep full six-block
BTC context separate. This adapter does not rerun BTC, pool symbols, or turn
selection feasibility into independent replication.

## Limits and activation

Maximum 19,104 candles, 10,000 cases, 999 pairs, 120 combined method seconds, 256 MiB
input and 16 MiB result. Fail rather than truncate, tune, expand limits or acquire
a tail. Registration and synthetic tests do not authorize scientific execution.
Use the normal qualified-source release, explicit activation decision, fresh
admission and single-attempt dispatch/readback path; stop on uncertain outcome.


## Configured populations

For new supported requests use `raw_forward_outcome@18` with
`reversal_saved_measurements.v2`. It uses the same binding and measurement
implementation and additionally admits a saved v17 configured selection. Method16
keeps its v12/v14-only contract. Take the exact original Dataset/context and
reference hashes from the saved source Check; never substitute a new scope,
instrument, watermark or pair count. The declared population recipe and internal
manifest must agree with the pinned source request. No recognition or matching
is repeated.

Measurements still require at most 999 saved pairs, 19,104 candles, the original
12-bar horizon and fixed geometry/summary policies. A valid larger selection
can exist without fitting this measurement bound; do not truncate it, expand the
limit or rematch. First-passage and progress remain different observations, with
explicit outcome provenance and missing mass. The 120-second evaluator budget
is cooperative: it cannot interrupt a synchronous DB call and does not include
input loading before evaluation. No latency claim follows from registration.

Method18 also projects each saved reporting population through the same reusable
first-passage and progress summary functions. Original root pair weights remain
unchanged; each named view reports its own original mass and conditional
distributions, including empty views and missing endpoints. Projection does not
read another candle, recalculate paths, or change selection membership.
