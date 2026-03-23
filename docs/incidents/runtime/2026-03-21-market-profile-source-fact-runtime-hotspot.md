# Market Profile Source-Fact Runtime Hotspot (2026-03-21)

## Incident

- Scope: QuantLab Market Profile overlay execution
- Symptom: Market Profile overlay runs were semantically correct but unacceptably slow on larger windows
- User-facing effect: QuantLab felt heavy and sticky when refreshing or recomputing Market Profile overlays
- Primary evidence: a latest Market Profile run logged `duration_source_facts_ms=25313.503` while the walk-forward engine work was only `duration_engine_ms=1128.743`

This was not a correctness incident. It was a runtime-performance incident on the source-fact path.

## What We Observed

### 1. Walk-forward merge semantics were behaving correctly

The runtime logs showed:

- `known_profiles` increasing progressively
- `merged_profiles` increasing progressively
- no evidence of premerged future clusters appearing at the start of walk-forward execution

That confirmed the earlier runtime resolver change was working as intended.

### 2. The hot path was source-fact construction, not walk-forward resolution

The relevant runtime line showed:

- `duration_fetch_ms` for source candles in the low hundreds of milliseconds
- `duration_source_facts_ms` around 25 seconds
- `duration_engine_ms` around 1.1 seconds

That meant the expensive step was building daily session profiles before walk-forward execution began.

### 3. The main compute hotspot was the TPO histogram builder

`src/indicators/market_profile/compute/internal/computation.py`
was still using:

- `DataFrame.iterrows()`
- nested Python loops
- repeated float math and float rounding for each bucket touched by each bar

This scales badly with:

- longer lookback windows
- more sessions
- wider session price ranges
- smaller bin sizes

### 4. Session grouping was doing more work than necessary

`src/indicators/market_profile/compute/engine.py`
was copying the full DataFrame, normalizing the index again, and grouping with `df.groupby(df.index.date)`.

That was correct, but it added avoidable overhead before the real TPO work even started.

## Root Cause

The slowdown came from an implementation mismatch, not from the intended semantics.

We wanted:

- canonical session profile construction
- stable value-area extraction
- correct walk-forward known-at timing

But the source-fact implementation was still doing that work in the slowest practical form:

- row-wise pandas iteration
- float bucket stepping inside Python loops
- extra grouping and copying overhead around the session builder

The architecture was sound. The implementation under the compute seam was not efficient enough.

## Resolution

We kept the semantics and changed the implementation.

### 1. Rewrote TPO histogram building around integer bin indexes

`build_tpo_histogram()` now:

- reads `low/high` columns as arrays
- normalizes bounds vector-wise
- computes integer start bins and run lengths
- counts visited buckets from integer bin indexes
- converts back to rounded price keys only at the end

This preserves the prior bucket semantics while reducing Python float work substantially.

### 2. Removed `iterrows()`

The histogram path no longer pays per-row pandas object construction overhead.

### 3. Tightened daily session grouping

`_compute_daily_profiles()` now:

- normalizes the index once
- sorts only if needed
- finds session boundaries from the normalized UTC index
- slices sessions by integer boundaries instead of `groupby(df.index.date)`
- avoids the unconditional full DataFrame copy

## What We Did Not Change

We did not:

- change Market Profile merge semantics
- change walk-forward known-at behavior
- introduce a generic candle cache
- premerge future clusters
- bypass the canonical source-fact seam

This was a compute optimization pass, not a semantic rewrite.

## Remaining Work

The next worthwhile optimizations are:

1. Reuse the source-frame candle set when chart timeframe and source timeframe are identical.
2. Add focused perf logging around session histogram build totals so large regressions are obvious.
3. Consider optional compaction of stored per-profile artifacts if `tpo_histogram` remains unused downstream.

## Outcome

The system stays aligned with the platform contract:

- source facts are built once from canonical candles
- walk-forward execution still owns when profiles become known
- merged clusters still evolve only as new profiles become known

The incident was caused by a slow compute implementation, not by the walk-forward design itself.
