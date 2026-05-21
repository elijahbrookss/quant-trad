# BotLens

BotLens is the runtime inspection and playback surface for bot runs.

## What It Is

BotLens lets operators inspect what a run knew, decided, executed, and reported. It is a debugger for runtime truth, not a separate execution engine and not a demo-only chart.

## What It Shows

BotLens focuses on:

- selected-symbol chart state,
- trade overlays and markers,
- accepted and rejected decisions,
- runtime events and diagnostics,
- execution mode and intrabar fallback context,
- projection/readiness state.

## Selected-Symbol Inspection

Normal selected-symbol switching reads projector-backed run and symbol snapshots. If projected state is unavailable, BotLens should say that explicitly instead of fabricating an empty valid chart.

Readiness has separate meanings: catalog discovery, snapshot readiness, symbol live state, and run live state. These states should be exposed instead of inferred from cache presence.

## Debugging Purpose

BotLens is useful when asking:

- What data was visible at this bar?
- Which rule accepted or rejected a decision?
- Why did a trade open or not open?
- Did FULL execution fall back to pessimistic resolution?
- Is a symbol projection unavailable, stale, or live?
- Are candle gaps provider gaps, ingestion gaps, projection gaps, or unknown gaps?

## How It Fits

BotLens consumes runtime facts and read models. It should not replay the ledger as a normal interaction path, reinterpret indicator-local overlays, or change execution semantics.

## Next

- Source of truth: [runtime contract](../contracts/platform/01_runtime_contract.md).
- Deep design: [BotLens projection boundary](../architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md).
- Runtime events and storage: [persistence boundary](../architecture/persistence/PERSISTENCE_BOUNDARY.md).
- Observability overview: [observability](../engineering/observability.md).
