# Assurance Execution Lifecycle

## Boundary

This model governs automated proof execution for the cataloged
`isolated_container` and `isolated_database` profiles. It does not run a manual
recovery rehearsal, enable external order submission, create activation
authority, or convert a proof result into an active guarantee.

The pre-execution admission and the post-cleanup attestation admission are
different records:

- `qt.assurance_execution_admission.v1` is an operator-supplied, source-bound
  permission to use an exact local Docker tool, daemon, runner image, and any
  service images. It contains no DSN, credential, proof result, or cleanup
  claim.
- the attestation's `profile_admission` is built only after the executor has
  observed the actual session, collected typed evidence, and proven cleanup.

An old `qt.assurance_profile_admission.v1` input is not silently reinterpreted
as an execution admission.

## Immutable Session Records

Before the first Docker resource is created, the executor reserves the
attestation session and writes one canonical `execution_draft` evidence record
per executable profile. The draft binds the exact clean source commit,
requested profiles, start time, execution-admission digest, runtime-definition
digest, environment-instance identity, Docker control-plane identity, and
planned disposable resources. It is created exclusively and is never
overwritten.

After the proof loop stops, the executor writes an `execution_manifest`. It
binds the draft hash, actual resource identities, exact executed-proof set, the
candidate proof-result digest, timestamps, and one of `complete`,
`interrupted`, or `executor_error`.

Cleanup produces a `cleanup_manifest`. A successful manifest binds the draft
and execution-manifest hashes, the same environment and control-plane
identities, cleanup timestamps, output hashes, and a typed resource inventory.
Every planned or observed resource must be explicitly absent and the
session-label query must return an empty set.

Only a `complete` execution with a successful first cleanup and exact absence
inventory is finalizable. A proof assertion may still be attested as `FAIL`;
cleanup failure is an environment-lifecycle failure and is not rewritten as a
product contradiction.

## Docker Isolation

The executor uses the Docker CLI as an argv array with `shell=False`. It never
invokes Compose, loads a repository `.env`, inherits provider credentials,
pulls an image, or builds an image during proof execution.

The execution admission names an exact local runner image and expected Docker
daemon. Immediately before provisioning, the executor resolves the admitted
reference to an immutable `sha256:...` image ID, verifies the daemon/context,
and records both in the draft. Docker create/exec uses the immutable image ID,
never the mutable tag. The exact check is repeated before every provision. The
TimescaleDB service is treated the same way and must match its catalog-bound
digest.

An `isolated_container` runner has no network, a read-only source bind, a
read-only root filesystem, and writable temporary storage outside the source.
An `isolated_database` session uses a new internal Docker bridge and a unique
database/container identity. Its only host publication is an ephemeral port on
`127.0.0.1`; the proof runner reaches the database on the internal bridge and
has no external route.

The child environment is an allowlist. It includes
`PYTHONDONTWRITEBYTECODE=1`, `PYTEST_ADDOPTS=-p no:cacheprovider`, UTC and stable
hash settings, and the assurance-mode flags. Database credentials are generated
for the disposable session, conveyed through a private temporary file outside
the source and evidence trees, and never copied into argv, logs, or durable
evidence.

## Interruption And Finalization

`SIGINT` and `SIGTERM` stop the active process group and enter the same cleanup
path as an ordinary run. An abrupt host/process failure is recoverable because
the draft and Docker labels exist before side effects. A later finalize attempt
may perform idempotent cleanup, but an interrupted or executor-error session
cannot emit an attestation.

The final attestation is assembled only from cleanup-verified records, checked
against the historical source model, written to a new temporary file, flushed,
and atomically renamed to its immutable destination. Existing final paths are
never overwritten.

Manual recovery remains a separately reviewed procedure. The automated
executor creates no recovery resources and reports its proof as `UNAVAILABLE`
when it is composed with a successfully finalized automated session. A
manual-recovery-only request cannot satisfy attestation v1's environment
binding and therefore produces no attestation.
