---
component: adr-schema-registered-canonical-facts
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - market-data
  - canonical-facts
  - typed-payloads
  - schema-versioning
  - provider-neutral
  - known-at
  - provenance
  - datasets
  - migration
code_paths:
  - src/market_data/contracts.py
  - src/market_data/fact_registry.py
  - src/market_data/store.py
  - src/data_providers
  - portal/backend/db/market_data_models.py
  - portal/backend/db/session.py
  - portal/backend/service/market
  - portal/backend/service/research
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - cli/mcp_server.py
  - scripts/db
  - tests/test_market_data
---
# ADR 0063: Use Schema-Registered Canonical Facts

## Status

Accepted on 2026-08-09 after the offline migration and validation completed on
a verified backup restore. All active readers and writers use canonical Fact
storage, the 17 superseded version tables were deleted, static enforcement
rejects their reintroduction, and provider-disabled structured replay was
proved through Dataset, Indicator, and Check evidence.

ADR 0061 remains the historical decision for exact scalar acquisition. Its
provider authorization, finality, coverage, and gap rules still apply, but its
retired scalar table is no longer an active persistence architecture.

## Context

QT has one logical market-series namespace, one append-only commit clock, and
provider-free frozen datasets, but no single canonical Fact representation.
Candles, open interest, funding, exact numeric observations, trades, L2 state,
aggregates, and derived market-state features use incompatible envelopes,
repositories, identity rules, and dataset dispatch branches.

The exact numeric `NumericFact` path proved that a provider-neutral requirement
can drive explicit acquisition, exact persistence, frozen evidence, replay,
Indicators, and Checks. It also exposed a hard limit: the payload is permanently
one scalar value. Financial observations such as reserve state, NAV, backing,
or other multi-variable provider state are atomic observations whose fields
cannot be split safely into unrelated scalar rows.

A universal unvalidated JSON document would remove useful type pressure and
make deterministic interpretation depend on whatever consumer reads it. A
table per provider would move provider identity below canonicalization. Keeping
all current family repositories would continue the existing split architecture.

## Decision

Use one immutable canonical Fact envelope and one schema-registered typed
payload contract for every dataset-eligible market observation.

The envelope owns:

- stable fact-version, logical series, observation, revision, and shared commit
  identities;
- canonical fact type and payload schema version;
- observation/effective time and method, optional source publication and
  platform receipt, platform acceptance, causal known-at and method;
- canonical source identity and optional ingestion-run identity;
- external event, group, and component identities where the source supplies
  them;
- active/invalidation state;
- payload, material, provenance, quality, and row hashes;
- versioned provenance, transformation, and quality documents.

The payload is stored as JSONB only under a registered immutable schema. It is
not an open document field. Each admitted schema must provide:

1. a globally unique schema ID and one canonical fact type;
2. a strict field contract with required/optional fields, exact JSON kinds,
   decimal and timestamp encodings, enums, domains, and
   `additional_properties=false`;
3. a stable contract hash persisted in the database and verified against code;
4. a typed Python payload class and codec that reject binary-float entry where
   the schema promises exact decimals;
5. deterministic canonical encoding, decoding, material hashing, row hashing,
   observation identity, and causal-selection rules;
6. explicit unit, dimension, atomicity, dataset-eligibility, and invalidation
   semantics;
7. declared query fields and required PostgreSQL expression indexes or typed
   relational projections for operational queries.

The database validates that every payload schema is registered, its payload is
an object, its required and allowed fields agree with the persisted contract,
and its primitive encodings match the declared types. Application codecs apply
the stronger cross-field and domain rules. Migrations use the same registry
contracts and run independent old/new equivalence queries before cutover.

Exact decimals use canonical decimal strings in JSONB. This prevents a Python
JSON encoder from introducing binary-float loss and supports explicit
`numeric` expression indexes. Historical float-based candle/OI/funding v1
payload schemas preserve their already accepted finite binary-float evidence
with the existing canonical-number encoding; they do not relabel it exact.

All Fact reads return one `FactRecord` envelope with a registered typed payload.
Consumers select fields through schema-owned typed accessors or registered
projections. They do not branch on provider and do not assume a universal
`.value` member. Storage routing, validation, decoding, and hashing branches
belong only in the core fact registry/repository boundary.

## Atomicity And Structured State

One external state observation remains one Fact even when it contains multiple
related fields. A reserve observation may retain reserve asset, reserve and
liability quantities, collateralization ratio, attestation time, and any
source-declared supply/backing fields under one schema and one revision.

Large repeated children may use a schema-owned typed child relation when JSONB
would make bounded queries or storage unreasonable. The parent Fact remains the
atomic revision and owns the child-set hash. L2 levels are the primary example.
This is a payload implementation detail, not a separate Fact envelope or
provider subsystem.

## Versioning

Fact type expresses canonical meaning; payload schema version expresses the
exact historical interpretation. Schema rows and codecs are immutable once a
Fact references them. A new meaning, unit rule, field set, exactness promise,
or hash algorithm requires a new schema version.

Historical v1 schemas remain readable through the canonical registry for as
long as retained facts or frozen evidence reference them. That is ordinary
schema-version interpretation, not a legacy table read, compatibility adapter,
or fallback path. Only one repository and one causal read algorithm are active.

New Coinbase OI and funding intake will use exact-decimal v2 payload schemas
that preserve provider raw text. Migrated v1 rows remain explicitly v1. The
active writer contract changes once; it does not dual-write v1 and v2.

## Provenance And Causal Time

Provider/source identity remains evidence but stops selecting downstream
behavior. Acquisition adapters translate provider objects into canonical
payloads and envelope fields. The persisted source, external object/request
identity, raw-value evidence, transformation/adapter version, source clocks,
receipt/acceptance clocks, finality evidence, and schema contract identify how
QT interpreted the observation.

Known-at remains the only causal availability clock. Provider effective,
funding, attestation, valuation, or publication timestamps remain distinct
fields and never substitute for known-at without a schema-owned causal rule.

Gap evidence and acquisition coverage remain separate append-only relations
linked to canonical series/source and commit scope. A missing observation is
not synthesized as a payload. Invalidations remain explicit Fact revisions.

## Dataset And Research Semantics

Dataset planning resolves provider-neutral requirements to canonical series.
Freeze reads the complete accepted revision chain from the one Fact repository
and pins schema ID/contract hash, watermark, row count, material, provenance,
quality, sources, and gaps. Frozen replay reconstructs typed payloads from local
evidence without provider adapters, manifests, credentials, or network
authority.

Dataset material and provenance hashing dispatch through the payload schema
registry. Historical v1 handlers reproduce the existing algorithms exactly so
existing dataset IDs, hashes, durable Check bindings, Observations, and research
links remain valid. A schema cannot silently change its hash algorithm.

Indicators may project a structured payload into scalar features. Checks
consume declared projections or typed Indicator outputs; they never inspect
provider identity to interpret the fact.

## Migration And Cutover

Use one explicit offline hard-cutover migration with writers stopped and a
verified backup available:

1. create the immutable schema registry and canonical fact storage;
2. register every retained historical payload schema and contract hash;
3. transform every old revision while preserving series IDs, source/ingestion
   identities, observation keys, revisions, commit sequences, clocks,
   provenance, quality, invalidations, and meaningful version IDs;
4. validate per-schema rows, causal selections, watermarks, material hashes,
   provenance hashes, quality hashes, dataset IDs/hashes, archive references,
   Check bindings, Observation links, and orphan/malformed records;
5. switch every writer and reader, including collectors, dataset preparation,
   runtime, Indicators, Checks, CLI, API, MCP, reports, and workers;
6. prove provider-disabled replay for candles, OI, funding, Chainlink scalar,
   and a structured Fact;
7. delete old fact tables, family repositories, direct SQL, compatibility
   adapters, fallback reads, dual-write code, and obsolete tests/docs;
8. leave only immutable historical migration scripts as lineage.

Temporary validation relations may exist only inside the controlled migration
window. The committed runtime must never support both old and new stores.

The accepted cutover migrated and compared 283,795 legacy rows across candles,
open interest, funding, exact numeric facts, trades, trade-flow aggregates,
Level 2 parents, BBO/depth/flow features, futures/spot basis, derivative state,
market response, and normalized features. Six unused normalization schemas had
zero references and were explicitly excluded. The final validator then removed
the 17 old version relations transactionally and proved canonical registry,
series, payload, source, clock, hash, and child-count agreement.

## Consequences

Adding a fact family requires an explicit schema, typed codec, semantic tests,
query plan, and migration—not a provider-specific downstream branch. Structured
provider state can remain atomic and reproducible. Frozen research receives one
causal read and hashing model.

JSONB provides a common atomic serialization and GIN inspection, but it does
not excuse arbitrary fields or slow casts. Declared hot fields require
expression indexes or typed child/projection tables, and query-plan tests must
prove the intended access pattern.

The migration is larger than an additive compatibility change because existing
evidence is real and hash-addressed. That cost is accepted in exchange for one
final architecture.

## Rejected Alternatives

- Continue extending `NumericFact`: cannot represent atomic structured state.
- Flatten each structured observation into scalar rows: destroys atomicity and
  can produce causally inconsistent combinations.
- Use an unrestricted JSONB payload: untyped, weakly queryable, and unstable
  under consumer-specific interpretation.
- Create one table/subsystem per provider: leaks provider identity below
  canonicalization.
- Keep separate active family repositories behind a facade: preserves multiple
  truths and fact-specific dataset algorithms.
- Dual-write or retain fallback readers during normal runtime: makes
  equivalence impossible to reason about and violates the hard-cutover goal.
- Convert historical floats to alleged exact provider decimals: fabricates
  precision that the retained v1 evidence does not contain.

## Enforcing Evidence

The acceptance evidence must include registry/codec contract tests, database
shape and immutability tests, complete migration/equivalence tests from a
pre-cutover fixture, causal prefix tests, existing dataset-hash reproduction,
provider-disabled frozen replay, structured Fact Indicator/Check evidence,
forbidden legacy-symbol/table scans, query-plan tests for declared indexes, and
full repository/CLI/API/MCP regression tests.

See [Generalized Fact Data Plane](../data/GENERALIZED_FACT_DATA_PLANE.md) and
[Canonical Fact Migration Discovery](../../engineering/canonical-fact-migration-discovery.md),
[Migration Validation](../../engineering/canonical-fact-migration-validation.md),
and [Chainlink Structured Facts](../data/CHAINLINK_STRUCTURED_FACTS.md).
