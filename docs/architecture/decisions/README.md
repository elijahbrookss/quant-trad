# Architecture Decision Records

This folder backfills the durable architecture decisions already present in the
contracts, code, tests, and boundary docs.

The format is intentionally concise. Durable cleanup decisions include:

- status and date, with retroactive decisions clearly marked,
- context and decision,
- invariants,
- consequences,
- rejected alternatives,
- enforcing tests or evidence,
- references.

Use ADRs for choices that constrain future architecture or safety. Do not write
an ADR for routine file movement, symbol renaming, or a mechanical extraction.
A proposed ADR records a required direction whose enforcement is incomplete; it
must not be cited as completed behavior.

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
| [0026](0026-use-control-plane-telemetry-flush.md) | Use control-plane telemetry flush | Accepted |
| [0027](0027-use-execution-profiles-as-runtime-instrument-authority.md) | Use execution profiles as runtime instrument authority | Accepted |
| [0028](0028-use-bounded-projection-dispatch-for-botlens-live-facts.md) | Use bounded projection dispatch for BotLens live facts | Accepted |
| [0029](0029-batch-and-degrade-botlens-projection-drain.md) | Batch and degrade BotLens projection drain | Accepted |
| [0030](0030-keep-portal-bots-definition-only.md) | Keep portal bots definition only | Accepted |
| [0031](0031-fingerprint-reports-and-slim-runtime-storage.md) | Fingerprint reports and slim runtime storage | Accepted |
| [0032](0032-use-field-owned-version-and-provenance-contracts.md) | Use field-owned version and provenance contracts | Accepted |
| [0033](0033-use-promtail-as-runtime-loki-ingress.md) | Use Promtail as runtime Loki ingress | Accepted |
| [0034](0034-use-research-checks-as-analytical-memory-evidence.md) | Use research checks as analytical memory evidence | Accepted |
| [0035](0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md) | Use complete output catalogs and split strategy read contracts | Accepted |
| [0036](0036-anchor-market-profile-retests-on-raw-breakouts.md) | Anchor Market Profile retests on raw breakouts | Accepted |
| [0037](0037-keep-research-presentations-metric-contract-driven.md) | Keep research presentations metric-contract driven | Accepted |
| [0038](0038-decouple-visual-overlay-projection-from-runtime-push.md) | Decouple visual overlay projection from runtime push | Accepted |
| [0039](0039-use-shared-async-jobs-for-research-dispatch.md) | Use shared async jobs for research dispatch | Accepted |
| [0040](0040-use-runtime-exit-plans-and-liquidity-roles.md) | Use runtime exit plans and liquidity roles | Accepted |
| [0041](0041-use-canonical-execution-plan-and-order-fill-semantics.md) | Use canonical execution plans and order fill semantics | Accepted |
| [0042](0042-use-runtime-event-ledger-as-lifecycle-truth.md) | Use the runtime event ledger as lifecycle truth | Accepted, retroactive cleanup |
| [0043](0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md) | Reconcile accounting from canonical fills and wallet ledger | Accepted, retroactive cleanup |
| [0044](0044-enforce-known-at-prefix-invariance.md) | Enforce known-at prefix invariance | Accepted, retroactive cleanup |
| [0045](0045-require-explicit-execution-and-exit-policy.md) | Require explicit execution and exit policy | Accepted, retroactive cleanup |
| [0046](0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md) | Fingerprint exact candle inputs and keep quality separate | Accepted, retroactive cleanup |
| [0047](0047-fence-async-job-ownership.md) | Fence async job ownership | Accepted |
| [0048](0048-gate-agent-mutation-and-research-promotion.md) | Gate agent mutation and research promotion | Proposed |
| [0049](0049-keep-live-order-submission-closed.md) | Keep live order submission closed | Accepted, retroactive cleanup |
| [0050](0050-use-one-canonical-append-only-market-data-store.md) | Use one canonical append-only market-data store | Accepted, retroactive cleanup |
| [0051](0051-require-frozen-datasets-for-canonical-backtests.md) | Require frozen datasets for canonical backtests | Accepted |
