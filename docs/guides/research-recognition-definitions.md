# Composing research recognition definitions

Research questions often need a definition completed first, then another event
that can consume its known context. The code boundary in
[`recognition.py`](../../src/research_science/recognition.py) gives ordinary Python
definitions immutable event records, explicit source linkage and clocks. Existing
versioned Check evidence remains responsible for persistence and input custody.
There is no second registry, expression language or workflow engine.

This public page describes reusable code contracts. Actual recognition runs,
bound cases, selected populations, visual outputs and study decisions are private
research records under the [public infrastructure/private research contract](../contracts/platform/00_system_contract.md#public-infrastructure-and-private-research).
Use synthetic inputs when publishing examples or fixtures.

## Start with typed, available facts

`RecognitionCandle.from_mapping` converts canonical closed-candle rows into the
same material identity for a Check or an offline visual. Input binding retains
frozen revisions and provenance; a candle identity hashes the source, normalized
open/close/availability clocks and OHLC actually consumed. `RecognitionContext`
records instrument, interval, source and computation origin. Origin belongs to
the definition input, independently of the displayed window.

`RecognitionEvent` records definition/version, configuration hash, context,
formation start, occurrence and availability, stages, numeric levels and source
event links. An event ID hashes that complete immutable material. Serialization
uses `research.recognition_event.v1`; reconstruction validates the identity.
Stage observations cannot precede origin or follow the event occurrence, and
availability cannot precede observation. A reference price has separate
`price_known_at` and `role_known_at`: seeing a high is different from knowing
that a completed formation assigned it a reference role.

Definitions use `initialize -> apply_bar -> snapshot`. Completed records and
earlier snapshots never change when more bars arrive. A viewport filters the
output; it does not initialize, crop or reset state. A deliberately changed
origin is a changed input with different context and left censoring.

## A reusable sequential recognizer

[`reversal_recognition.py`](../../src/research_science/reversal_recognition.py)
implements `completed_reversal_candidates`, version `provisional.1`. All four
settings are required, without unknown keys:

| Setting | Supported meaning |
| --- | --- |
| `reference_basis` | `rising_leg_only` uses highs of the rising-close rebound; `rising_leg_plus_retreat` explicitly includes its first retreat candle. Each choice is part of immutable configuration identity. |
| `ownership` | Only `retain_all_provisional`; recognition does not establish an independent analytical population. |
| `max_bars` | Integer 1–100000; fail at the bound rather than truncate. |
| `max_candidates` | Integer 1–10000; bounds all confirmed reference chains, including completed ones. |

One lower-close step starts a decline, its first higher close starts a rebound,
and its first lower close confirms that rebound's fixed reference. Flat closes
extend an existing leg. The preceding trough is the minimum low in the decline.
A strictly later candle must push below that trough. A later higher close
starts recovery; the first recovery close above the fixed reference completes
that chain. Recovery can itself be the completion candle. No intra-candle order
is inferred. A confirmation candle cannot simultaneously be the later push.
There is no minimum size, maximum formation length, expiry, implicit reset or
winner selection. Each reference can complete once, but this bookkeeping does
not settle which overlapping chains should become independent events.

This recognizer requires contiguous, single-source candles available at their close,
starting exactly at the supplied origin. It rejects gaps and delayed availability
rather than reinterpret execution timing. This is narrower than the generic
event contract, which can describe delayed knowledge. Resource limits stop the
whole call; partial state after an exception must be discarded.

A Check adapter must explicitly admit the recognizer and pin its settings,
origin and input binding. Discover currently supported adapters through
`qt research check definitions`; retired method descriptions do not confer
execution support. Recognition bookkeeping is not an outcome calculation,
independent event population or scientific authorization.

## Compose definitions with ordinary functions

Select an exact upstream event in Python, run the next definition on facts
available to it, and link its result explicitly:

```python
linked = link_source_event(
    followup_event, reference_event,
    consumed_at=first_consuming_candle.known_at,
    max_age_seconds=3600,  # chosen protocol bound, not a fitted default
    relation="uses_reference",
)
```

The link checks equal context, strict source occurrence before target occurrence,
source availability by consumption, consumption by target availability, explicit
source age and duplicate identity. It returns a new immutable target identity.
It neither selects the nearest source nor joins cross-market or different-origin
events. The caller owns deterministic source choice and keeps the referenced
event record alongside the link. Directly constructed link metadata is not a
certificate that an external event exists. Future cross-context composition
needs a separately reviewed clock/join contract.

To add another definition, write its domain function or small sequential class
using these inputs/outputs. Validate its supported settings in a frozen settings
type; include them in event configuration identity. Reuse input preparation and
outcome procedures where their semantics fit. Add a thin evaluator and an
explicit version to the existing Check registry/catalogue only when a new
scientific procedure is actually needed. Never change historical version rules
or results to adopt this boundary. Test causal timing, prefix stability, bounds,
ambiguity and rejected settings with synthetic facts before requesting science.

## Keep visuals and runtime consumers honest

[`reversal_recognition_preview.py`](../../scripts/reporting/reversal_recognition_preview.py)
consumes a bounded, already-retained input JSON and at most four prebound cases
(`label`, `start`, `end`, `reference_known_at`). It calls the same recognizer and
captures each as-of snapshot during the single origin-based walk. It masks the
display at that exact reference's completion, or the requested as-of if pending.
Other completed chains and the first simultaneous group remain in JSON evidence.
Binding a historical illustration to a reference preserves that illustration;
it is not a population ownership rule or a representative-sample claim.

```sh
PYTHONPATH=src:. python scripts/reporting/reversal_recognition_preview.py \
  retained-candles.json bound-cases.json new-preview-directory \
  --reference-basis rising_leg_only --max-bars 3073 --max-candidates 1000
```

The script reads local files only, bounds input bytes, refuses an existing output
directory and calculates no outcomes. It needs explicit retained input custody;
it has no provider or production access. A real-data preview still needs its
research scope approved, even though the renderer itself cannot dispatch a Check.

The runtime
[`SignalOutputEvent`](../../src/engines/indicator_engine/signal_output.py)
contract is a separate boundary: key, direction, pattern, known-at and metadata
are published by the canonical Indicator engine. Research recognition records
do not become runtime signal items merely because their shapes are similar.
Any adapter must preserve the Indicator timeline, publication contract and
execution separation. Forward-path outcomes remain Check-owned analytical
measurements rather than fills.
