# Architecture Decision Records

This folder backfills the durable architecture decisions already present in the
contracts, code, tests, and boundary docs.

The format is intentionally slim:

- status and date,
- context,
- decision,
- consequences,
- references.

These records do not replace the platform contracts. Contracts remain normative;
ADRs explain why the current shape exists and what tradeoffs future changes must
respect.

## Records

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-use-boundary-first-architecture-docs.md) | Use boundary-first architecture documentation | Accepted, backfilled |
| [0002](0002-use-one-walk-forward-runtime-timeline.md) | Use one walk-forward runtime timeline | Accepted, backfilled |
| [0003](0003-preserve-data-boundary-source-facts.md) | Preserve data boundary source facts and gap evidence | Accepted, backfilled |
| [0004](0004-separate-indicator-truth-from-projections.md) | Separate indicator typed outputs from overlays and details | Accepted, backfilled |
| [0005](0005-keep-strategy-decisions-separate-from-execution.md) | Keep strategy decisions separate from execution | Accepted, backfilled |
| [0006](0006-keep-execution-semantics-independent-from-playback.md) | Keep execution semantics independent from playback | Accepted, backfilled |
| [0007](0007-use-scoped-causal-clocks-for-runtime-replay.md) | Use scoped causal clocks for runtime replay | Accepted, backfilled |
| [0008](0008-treat-botlens-as-projection-debugger.md) | Treat BotLens as a projection debugger | Accepted, backfilled |
| [0009](0009-use-one-postgres-persistence-boundary-and-retained-event-ledger.md) | Use one Postgres persistence boundary and a retained event ledger | Accepted, backfilled |
| [0010](0010-use-run-research-dataset-as-reporting-contract.md) | Use RunResearchDataset v1 as the reporting contract | Accepted, backfilled |
| [0011](0011-keep-observability-bounded-and-non-canonical.md) | Keep observability bounded and non-canonical | Accepted, backfilled |
| [0012](0012-use-runtime-composition-root-for-mode-aware-wiring.md) | Use a runtime composition root for mode-aware wiring | Accepted, backfilled |
| [0013](0013-use-market-time-ordering-for-shared-wallet-backtests.md) | Use market-time ordering for shared-wallet backtests | Accepted |
| [0014](0014-use-shared-wallet-arbitration-policy-by-runtime-mode.md) | Use shared-wallet arbitration policy by runtime mode | Accepted |
| [0015](0015-split-semantic-and-operational-golden-fingerprints.md) | Split semantic and operational golden fingerprints | Accepted |
| [0016](0016-treat-runtime-event-ledger-order-as-operational-evidence.md) | Treat runtime event ledger order as operational evidence | Accepted |
| [0017](0017-use-api-backed-cli-for-research-orchestration.md) | Use an API-backed CLI for research orchestration | Accepted |
| [0018](0018-use-output-filters-as-strategy-variant-contract.md) | Use output filters as the strategy variant contract | Accepted |
| [0019](0019-use-file-backed-sequential-experiment-plans.md) | Use file-backed sequential experiment plans | Accepted |
| [0020](0020-use-budgeted-market-data-stream-reconnect-policy.md) | Use budgeted market data stream reconnect policy | Accepted |
| [0021](0021-use-runner-clock-gap-sentinel.md) | Use runner clock gap sentinel | Accepted |
| [0022](0022-capture-docker-container-lifecycle-as-runner-agnostic-observability.md) | Capture Docker container lifecycle as runner-agnostic observability | Accepted |
| [0023](0023-persist-watchdog-degradation-diagnostics.md) | Persist watchdog degradation diagnostics | Accepted |
| [0024](0024-use-provider-credential-references.md) | Use provider credential references | Accepted |
| [0025](0025-use-per-run-leases.md) | Use per-run leases | Accepted |
