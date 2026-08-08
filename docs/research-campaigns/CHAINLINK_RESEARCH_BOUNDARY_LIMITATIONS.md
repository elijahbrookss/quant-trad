# Chainlink Historical Research Boundary Notice

This notice preserves the earlier Chainlink dossiers, protocol JSON, result
JSON, Check rows, and Dataset rows unchanged. Those artifacts are historical
evidence of what the earlier workflow reported; they are not retroactively
represented as canonical frozen Check evidence.

## Historical Classification

- The July population of 24 breakout candidates was calculated through an
  external research workflow that reconstructed Market Profile breakout
  semantics and forward outcomes. The registered canonical Indicator was not
  executed from a `FrozenMarketDataReadBinding` by that workflow.
- The six-month population of 119 candidates was calculated before an admitted
  six-month Dataset existed. It is therefore an ad hoc population projection,
  not durable replayable Check evidence.
- The protocol/result JSON files, feature-matrix hashes, selected-fact hashes,
  and dossiers were primary outputs of that workflow rather than projections of
  one canonical `event_fact_analysis` record.
- The six-month Dataset freeze stopped on the known Coinbase candle gap because
  freeze and consumer readiness were conflated. Current Dataset semantics
  preserve the facts and recorded gap; Market Profile and Check make explicit,
  versioned readiness decisions.
- Historical source revisions remain as recorded. They are not rewritten to
  match later documentation or code commits.

The old records remain useful for provenance and comparison, but their results
must not be cited as `frozen_replayable` or used as Scientific Attempt evidence.

## Canonical Rerun Contract

The replacement workflow uses the provider-neutral
`market.reference_price.v1` fact with an exact Chainlink source binding, the
registered Market Profile Indicator, an explicit gap policy, a frozen Dataset,
the registered generic `event_fact_analysis` evaluator, a durable Check result,
an evidence-backed Observation, and an identical-hash replay.

The rerun may legitimately differ from 24 or 119 because it enforces:

- corrected candle close-time `known_at`;
- Indicator-owned gap reset and re-warm;
- exact evaluation-range visibility;
- predecessor-bearing causal fact features;
- explicit unresolved outcomes; and
- the current versioned evaluator and definition hashes.

Any new result must report its actual population, eligibility, unresolved
outcomes, statistical status, Dataset/source bindings, code revision, result
hash, and replay outcome. A failed or indeterminate Check verdict grants no
promotion or execution authority.
