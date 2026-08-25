# Guarantee Attestations

This directory is reserved for immutable, commit-bound verification records.
There is intentionally no attestation for the Phase 2A calibration: existing
test files were inspected as named proof, but inspection is not a fresh PASS.

When verification is authorized, write one JSON document per source commit and
verification session under:

```text
attestations/<40-character-source-commit>/<attestation-id>.json
```

One attestation may compose multiple catalog environment profiles. Its
top-level `environments` array records each profile actually used; the
attestation ID suffix is that profile ID when there is one environment and
`multi` when there is more than one. Every proof result names its
`environment_profile_id`.

The document must conform to `../schemas/attestation.v1.schema.json` and bind:

- the exact Git commit and whether the worktree was clean;
- an assurance-material hash that covers the registry's claim semantics, proof
  catalog, every actually referenced proposed or adopted glossary source, validator,
  authority/enforcement/proof-selector files, and lockfiles, while excluding
  attestation outputs and activation-only registry fields;
- `registry_semantics_sha256`, `proof_catalog_sha256`, a `glossary_inputs`
  array of `{source_kind, path, sha256}` bindings, per-guarantee
  `guarantee_material_sha256` bindings over cited authority/enforcement
  material, and per-required-proof `required_proof_material_sha256` bindings;
- tool and required-service identities for every bound environment;
- start/end timestamps and per-proof collection, exit, and output evidence,
  including the exact shell-free `executed_argv` for attempted automated runs,
  explicit pass/fail/skip/xfail/xpass counts for pytest, and the admitted
  runner-specific typed result fields for native Node tests;
- attestation- and proof-scoped typed result-summary/output artifacts under
  `docs/assurance/guarantees/evidence/<attestation-id>/<proof-id>/`, with every
  artifact's kind, path, and hash cross-checked against the result envelope;
- an operator identity for manual work and an independent reviewer identity
  for manual PASS or FAIL;
- conservative, derived per-guarantee results.

The registry-semantics projection deliberately excludes activation status and
its decision/attestation references. This lets a clean pre-activation commit be
attested and then cited by a later reviewed activation decision without a hash
cycle. Claim text, scope, failure semantics, wording constraints, authority,
enforcement, terms, findings, and proof mappings remain inside the bound
assurance material. The validator resolves a cited historical attestation from
its recorded Git commit rather than requiring that commit to remain current
HEAD.

Version 1 validates repository paths, Git-blob and artifact hashes, typed result
summaries, and agreement among runner arguments, counts, exit state, and the
attestation envelope. Native `node --test` results use the catalog-bound
`qt.node_test_events.v1` reporter transport and a separately hashed
`qt.node_test_result.v1` summary. That summary must identify every collected
target file, the exact selected test-name set, and every name excluded only
because it did not match `--test-name-pattern`. Excluded nonmatches are not
selected skips and do not weaken an otherwise complete result.

The model does not cryptographically prove that a runner or reviewer identity
performed the stated action, or establish that the named reviewer has
authority under the repository hierarchy. A distinct external activation
review is therefore the trust boundary: it must verify execution provenance,
reviewer authority, and the exact attestation ID and hash it reviewed.
Structural validation alone cannot authorize activation.

Version 1 also binds the full registry semantic projection and proof catalog,
not only one claim's slice. Any later semantic or catalog addition therefore
requires re-attestation before an existing activation can remain valid. This is
an intentional conservative coupling, not a claim that unrelated additions
changed the product behavior. A change to authority or enforcement material for
one guarantee also changes that guarantee's material binding and requires
re-attestation before activation.

Only attestations may use result states. For automated proofs, `PASS` and
`FAIL` bind the exact catalog runner. For manual proofs, `PASS` and `FAIL`
require timed, hashed evidence plus distinct operator and reviewer identities.
Version 1 permits automated `PASS` for pytest and for the admitted native Node
runner. Other runner kinds may be cataloged, but cannot satisfy a guarantee
until runner-specific PASS rules are reviewed and modeled.

For a native Node result, `PASS` additionally requires all of the following:

- the bound reporter transport, exact target-file set, and exact expected
  selected test-name set agree with the proof definition;
- the selected outcomes account exactly for the collected selected tests;
- every selected test passed, with zero failed, cancelled, todo, or explicitly
  skipped selected results;
- the process exited zero; and
- stdout, stderr, and the typed result summary are each present as a uniquely
  named, hashed artifact.

A test that the native runner reports as skipped solely because its name did
not match the cataloged pattern is recorded under
`excluded_nonmatch_test_names`. It is outside the selected proof denominator;
an explicit skip of an expected selected test remains inside that denominator
and forbids `PASS`.

- `PASS`: the complete required automated selector set ran at the bound clean
  commit without failures, skips, or expected/unexpected outcome escapes, or a
  manual procedure was independently reviewed and accepted.
- `FAIL`: a required automated assertion failed, or an independently reviewed
  manual procedure failed its acceptance criteria.
- `NOT_RUN`: the proof was not attempted.
- `MANUAL`: a cataloged human procedure and its evidence are recorded but have
  not received the independent acceptance required for manual PASS.
- `PARTIAL`: work was attempted but required coverage is incomplete and no
  contradiction was observed.
- `UNAVAILABLE`: a concrete prerequisite is absent and identified.

Aggregation is conservative: any required FAIL yields FAIL; all required PASS
yields PASS; homogeneous NOT_RUN, UNAVAILABLE, or MANUAL preserves that state;
every other mixture yields PARTIAL. Supporting proofs do not determine the
aggregate.

Do not keep a mutable `latest.json`, put demonstration results here, or copy a
status into the durable registry. Test fixtures for this model belong under
`tests/fixtures/`.
