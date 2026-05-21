# Reporting Datasets

Reporting datasets turn durable run truth into analysis-ready views.

## What It Is

`RunResearchDataset v1` is the canonical run-level data product for research, reports, comparisons, diagnostics, external analysis tools, and downloadable artifacts.

It is rebuilt from durable database and read-model truth rather than local chart state.

## Why It Exists

Reports and comparisons need a stable data product that can be regenerated and inspected. The dataset provides that boundary so report views do not become the source of readiness, metrics, or comparison semantics.

## Dataset Sources

The dataset can use:

- run metadata,
- trades and trade lifecycle rows,
- runtime events and decision facts,
- execution mode and fallback diagnostics,
- fee and wallet accounting facts,
- candle continuity summaries,
- runtime step timing when present,
- observability diagnostics relevant to the run.

## Views Over The Dataset

Reports, compare views, exports, and narrative summaries should be treated as views over the dataset. They may present different slices, but they should not invent alternate run semantics.

## Current Sections

`RunResearchDataset v1` contains:

- metadata and readiness,
- summary and portfolio metrics,
- canonical timeseries,
- trades, decisions, and signals,
- context/world-state datasets when captured by runtime facts,
- candle catalog without full candle rows,
- diagnostics and operational health,
- export and comparison availability metadata.

Full candles are accessed through bounded reporting candle routes or optional
research exports. They are not embedded in the default report payload.

## How It Fits

```text
Runtime facts -> Durable storage/read models -> RunResearchDataset -> Reports / Compare / Export / Diagnostics
```

## Next

- Deep design: [reporting boundary](../architecture/reporting/REPORTING_BOUNDARY.md).
- Runtime storage: [persistence boundary](../architecture/persistence/PERSISTENCE_BOUNDARY.md).
- Runtime relationship: [runtime timeline](runtime-timeline.md).
