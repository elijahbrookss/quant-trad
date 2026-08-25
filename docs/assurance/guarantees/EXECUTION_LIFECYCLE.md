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

The supported entry points are explicit about every output and private path:

```text
python scripts/assurance/verify_guarantees.py inspect-admission --source-commit S --docker ABS_DOCKER --runner-image LOCAL_TAG_OR_ID --runner-build-definition docker/assurance/frontend-node.Dockerfile --profile PROFILE --output ABS_REVIEW_PACKET
python scripts/assurance/verify_guarantees.py run --source-commit S --stage-root ABS_STAGE --private-root ABS_PRIVATE --execution-admission ABS_REVIEWED_ADMISSION --profile PROFILE --profile manual-recovery
python scripts/assurance/verify_guarantees.py validate-staged --attestation ABS_ATTESTATION --evidence-root ABS_STAGE
```

The shell-free `inspect-admission` command writes an explicit output file
outside the source tree. Its wrapper is
`qt.assurance_execution_admission_inspection.v1`, carries
`review_required: true`, and deliberately uses an invalid
`<OWNER-REVIEW-REQUIRED>` admission identity. It cannot authorize execution.
An owner must review `candidate_execution_admission`, replace that identity,
and save the resulting standalone `qt.assurance_execution_admission.v1` file
outside the source, evidence stage, and private runtime roots. There is no
auto-approval or auto-promotion command.

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

Every cleanup attempt produces an immutable, numbered `cleanup_manifest`. The
record binds the draft and execution-manifest hashes, the same environment and
control-plane identities, cleanup timestamps, the exact stdout/stderr bytes and
their hashes, and a typed resource inventory. Failed and interrupted attempts
remain durable. A passing record additionally requires every planned or
observed resource to be explicitly absent and the session-label query to return
an empty set.

Only a `complete` execution with a successful first cleanup and exact absence
inventory is finalizable. A proof assertion may still be attested as `FAIL`;
cleanup failure is an environment-lifecycle failure and is not rewritten as a
product contradiction.

## Docker Isolation

The executor uses the Docker CLI as an argv array with `shell=False`. It never
invokes Compose, loads a repository `.env`, inherits provider credentials,
pulls an image, or builds an image during proof execution.

The execution admission names an exact local runner image and expected Docker
daemon. Before the draft, the executor resolves every admitted image to an
immutable `sha256:...` image ID and verifies the daemon/context. Docker
create/exec uses that immutable image ID, never the mutable tag. The exact
daemon/context and image-ID checks are repeated before every provision. The
TimescaleDB admission records both its catalog-bound repository digest and the
locally resolved image ID; both must match and only the image ID is passed to
Docker create.
The runner image also carries an externally applied, inspected
`com.quant-trad.assurance.build-definition-sha256` image-config label equal to
the bound runner-build definition digest. This build definition is distinct
from a profile runtime definition (notably the database profile JSON), so one
content-addressed runner may truthfully serve multiple profiles. The Dockerfile
does not attempt to contain its own digest. An admission may not merely list an
unrelated local image ID and definition side by side; the retained image
inspection binds the label through the immutable image ID.

An `isolated_container` runner has no network, a read-only source bind, a
read-only root filesystem, and writable temporary storage outside the source.
An `isolated_database` session uses a new internal Docker bridge and a unique
database/container identity. Its only host publication is an ephemeral port on
`127.0.0.1`; the proof runner reaches the database on the internal bridge and
has no external route.

The proof child receives an exact allowlist even if the local image has benign
base-image environment metadata. The effective container environment is
inspected and any unadmitted or credential-like key is rejected. The child
allowlist includes
`PYTHONDONTWRITEBYTECODE=1`, `PYTEST_ADDOPTS=-p no:cacheprovider`, UTC and stable
hash settings, and the assurance-mode flags. Database credentials are generated
for the disposable session, conveyed through a private temporary file outside
the source and evidence trees, and never copied into argv, logs, or durable
evidence.

## Interruption And Finalization

`SIGINT` and `SIGTERM` enter the same cleanup path as an ordinary run. A timed
out in-container proof has its child process group terminated; if the outer
host fail-safe fires, the runner itself is stopped and no later proof runs.
Signal handlers remain installed through manifest publication and every
cleanup attempt. Repeated signals are recorded but cannot bypass cleanup.

An abrupt host/process death that prevents this handler from running is not
automatically recoverable by this command version. Its immutable draft and
full session/profile/instance Docker labels support owner inspection, but the
session is non-finalizable and must stop for separately reviewed cleanup. The
executor does not claim a later finalize/cleanup-resume command that it does
not yet provide. An interrupted or executor-error session cannot emit an
attestation.

The final attestation is assembled only from cleanup-verified records, checked
against the historical source model, and atomically and exclusively published
with flushed file and directory state. Existing final paths are never
overwritten. Publishing staged evidence into the repository is a separate,
reviewed mechanical copy; this executor neither copies nor commits it.

One run admits exactly one automated profile. A final review may validate
multiple independent attestations. To retain the honest recovery result, a run
may select the automated profile and `manual-recovery` together while the
execution-admission file contains only the automated profile; the recovery
proof is then `UNAVAILABLE` with its separate-review reason.

Manual recovery remains a separately reviewed procedure. The automated
executor creates no recovery resources and reports its proof as `UNAVAILABLE`
when it is composed with a successfully finalized automated session. A
manual-recovery-only request cannot satisfy attestation v1's environment
binding and therefore produces no attestation.
