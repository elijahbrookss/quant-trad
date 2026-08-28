# QT Today

QT is a local quantitative-trading research platform. Quantitative trading
means turning a trading idea into explicit rules, applying those rules to
market evidence, and measuring what would have happened instead of relying on
memory or intuition alone.

QT helps a trader or researcher move from an idea to evidence that can be
replayed and inspected. It collects and normalizes market information, computes
measurements, evaluates research questions, runs strategies through historical
data one step at a time, and preserves the decisions, simulated fills,
accounting, and explanations produced by a run.

Today QT supports reproducible research, backtests, walk-forward evaluation,
bounded observe-only paper simulation, reports, comparisons, and detailed run
inspection. It does not submit orders to an external venue. It is an
active-development research system, not production live-capital infrastructure.

## From Market Evidence To An Explainable Result

The normal flow is:

```text
external market state
  -> provider adapter
  -> canonical market facts
  -> frozen evidence
  -> indicators and research checks
  -> strategy decisions
  -> simulated execution and accounting
  -> BotLens and reports
```

## Interface Map

QT exposes application behavior through four deliberate surfaces. They do not
all have feature parity; overlapping operations share the same contracts:

| Surface | Intended use | Authority boundary |
|---|---|---|
| `qt` CLI | Primary operator and automation workflow for data, research, strategies, runs, reports, and comparisons. | Uses backend contracts and bounded local workflow helpers; it does not define alternate market, strategy, or execution semantics. |
| Backend API | Canonical application interface used to validate requests, orchestrate services, and read or write durable state. | Domain services and PostgreSQL remain the underlying authorities; an HTTP response is not a second source of truth. |
| `qt mcp serve` | Stdio adapter that gives agent hosts read resources and guarded tools. | Delegates to the CLI and API contracts. It has no MCP-only research language, runtime, cache, or remote service authority. |
| Frontend V2 | Human setup, operation, visualization, BotLens inspection, playback, and reporting. | Reads and acts through the API. Browser state and projections may explain durable truth but cannot create a separate execution or research history. |

The surface changes how a person or tool reaches QT, not what QT means. When
the UI, CLI, and MCP expose the same operation, they must meet the same backend
contract.

A provider adapter acquires outside information and translates it into QT's
provider-neutral market model. One immutable market observation in that model
is a **Canonical Fact**. Corrections append a new **Fact revision** instead of
rewriting the earlier record, and `known_at` records when QT could actually have
known the information.

When research needs a stable source, QT saves the exact Fact revisions, ranges,
hashes, sources, gaps, and causal watermark in a **Frozen Dataset**. Replay then
uses that frozen evidence without consulting the original provider. A **Frozen
Market Data Read Binding** records the exact slice a durable consumer used.

An **Indicator** turns known market evidence into a typed measurement, state, or
signal. A **Check** asks a bounded analytical question. A Check preview is only
exploration; durable Check evidence is frozen and replayable. A promising result
can be admitted deliberately as a **Research Observation**, which is research
memory rather than market truth.

A **Strategy** consumes typed Indicator outputs and produces decision artifacts.
It does not create fills, fees, wallet changes, or order history. The bot runtime
owns those execution and accounting facts. Every run advances through the same
walk-forward sequence—`initialize -> apply_bar -> snapshot`—so future data
cannot appear early merely because the run is historical.

BotLens and reports are views over durable run truth. BotLens helps answer why a
strategy acted or did not act at a particular point. Reports summarize and
compare results through the **RunResearchDataset** read model. Neither surface
is allowed to invent a second execution history.

For a practical, non-technical walkthrough, continue to the
[core research workflow](guides/research-workflow.md). For standardized wording,
use the [platform glossary](contracts/platform/04_glossary.md).

## How QT Pins Meaning, Inputs, And Results

QT does not use one generic “version” field for everything. Each identifier
answers a different reproducibility question:

| Identifier | What it identifies | What it does not identify |
|---|---|---|
| **Payload schema version** | How the fields in a typed Fact or artifact must be interpreted. | Which market observation was selected or whether that observation was later corrected. |
| **Fact revision** | The exact append-only correction of one logical observation. Revisions sharing an observation key remain distinct records. | A change to the payload's meaning; that requires a schema-version change. |
| **Dataset fingerprint** | The immutable Frozen Dataset manifest: exact Fact revisions plus its ranges, sources, provenance, quality, gaps, and causal boundary. | The decision rules or software that consume the Dataset. |
| **Strategy version or effective strategy hash** | The exact decision-rule graph or the materialized strategy configuration selected for work. Research graphs have immutable versions and hashes; bot runs pin the effective strategy configuration and hash. | The market evidence, execution context, or implementation build. |
| **Run snapshot** | The run-start bundle that binds or references the chosen Dataset, effective strategy and Indicator configuration, bot settings, and resolved execution context. | Changes made after the run starts, or by itself the source code that interpreted the bundle. |
| **Code revision** | The source revision whose implementation interpreted the pinned inputs. Durable Check evidence records this separately. | The data or configuration used by a particular run. |
| **Evidence hashes** | Content commitments over normalized inputs, outputs, and composite durable evidence, allowing mutation or disagreement to be detected. | Correctness by themselves; matching bytes can still embody a bad rule or assumption. |

In compact form: schema versions make values interpretable; exact Fact
revisions become a fingerprinted Frozen Dataset; the Dataset and effective
Strategy are bound into a run snapshot; and the code revision plus content
hashes make the resulting durable evidence traceable. Reproducing a result
requires those identities to agree, not merely one matching ID.

## Six Promises Guide Important Changes

QT protects six high-consequence outcomes:

1. **Causal and canonical truth.** Historical results use only information that
   was knowable at the time, and canonical observations are not silently
   rewritten.
2. **Frozen research authority.** Durable research identifies and can replay the
   exact market evidence it used; exploratory previews do not silently become
   durable evidence.
3. **Decision and execution authority.** Indicators and Strategies can explain
   a decision, while the runtime alone owns execution and accounting truth.
4. **Capital, order, and accounting integrity.** Order history, settlement,
   wallet state, and reconciliation follow one internally consistent path, and
   external-order submission remains closed.
5. **Credential confinement.** Provider secrets stay behind credential
   references and do not belong in normal configuration, logs, or research
   artifacts.
6. **Durable persistence and recovery.** PostgreSQL is the one durable source of
   platform truth, and destructive recovery is treated as a real operational
   obligation rather than assumed from configuration alone.

The [core promises](core-promises.md) connect these outcomes to their platform
contracts, accepted decisions, and normal engineering checks.

## What Normal Development Protects

QT uses ordinary engineering validation as its default protection:

- Python tests cover contracts, causal behavior, research, runtime, accounting,
  interfaces, and configuration.
- Disposable isolated-database tests cover behavior that depends on PostgreSQL
  or TimescaleDB without touching a production database.
- Frontend tests and the production build protect supported views and adapters;
  the frontend remains a projection rather than a source of truth.
- Documentation, glossary, architecture metadata, and internal links are
  checked for consistency.
- Deployment and configuration checks inspect supported topology without
  deploying to a live system.
- Network, real-credential, production-data, and external-order boundaries stay
  closed during normal validation.

The complete suites are the regression unit. An individual test protects the
behavior it actually exercises; it does not establish universal correctness for
every environment.

Stronger verification is reserved for a real trust boundary—for example, an
external release consumer, security review, legal requirement, recovery
commitment, or capital authorization. Exact-build provenance and formal
evidence publication are not part of ordinary QT development today.

## Honest Current Limits

- External order submission is deliberately closed. Paper operation is bounded
  and does not authorize live venue trading.
- Destructive recovery has static guards, but a complete disposable
  source-to-restore rehearsal may be unavailable until the required isolated
  environment exists. QT reports that honestly rather than treating a
  configuration check as a restore result.
- Some broad properties have representative rather than universal test
  coverage. The [testing strategy](engineering/testing/testing-strategy.md)
  describes the supported validation boundary.
- The current deployment model has a local/private trust boundary. Multi-user,
  remotely exposed, regulated, independently certified, or live-capital use
  would require explicit product and security decisions plus stronger evidence.
- QT is evolving. Missing provider, indicator, or presentation support is a
  limitation; violating causal, authority, persistence, credential, or
  accounting boundaries is a defect.

## Where To Read Next

- New to quantitative trading: [core research workflow](guides/research-workflow.md)
- Ready to run QT: [getting started](getting-started.md)
- Looking up a QT word: [platform glossary](contracts/platform/04_glossary.md)
- Learning the system boundaries: [architecture overview](engineering/architecture.md)
  and [system model](architecture/system/SYSTEM_MODEL.md)
- Changing code: [agent and contributor context](../AGENTS.md), then the
  [architecture component index](architecture/ARCHITECTURE_COMPONENT_INDEX.md)
- Operating a durable host: [operator handbook](operators/README.md)
