# Assurance Runtime Profiles

These definitions support commit-bound proof execution. They are not product
containers, deployment instructions, proof results, attestations, remediation
closures, or guarantee-activation decisions.

## Frontend Node Profile

`frontend-node.Dockerfile` combines two immutable Linux/amd64 base images:

- Node 20.20.2 from
  `node@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0`;
- Python 3.12.14 from
  `python@sha256:a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134`.

The image copies the Node executable into the Python image and installs the
Python distributions pinned by `requirements.lock`. The source-owned
`python-wheel-manifest.lock.json` binds the reviewed 91-wheel closure by
normalized distribution/version, safe canonical filename, byte size, SHA-256,
wheel tags, and Python compatibility. Wheel bytes are deliberately external to
Git.

`runner-build.profile.json` and `scripts/assurance/build_runner.py` own the
only admitted construction path. The materializer finds matching existing
cache bytes by content, snapshots each selected artifact once, validates its
ZIP/metadata/tag/RECORD integrity, and creates closed deterministic wheelhouse
and build-context tars outside the source tree. Durable output contains no
cache paths. The builder passes the verified context tar on standard input to
an exact shell-free Docker argv with `--network=none`, `--pull=false`, and
`--no-cache`. Pip uses `--no-index`, `--no-deps`,
`--only-binary=:all:`, and `--require-hashes`; there is no network or source
distribution fallback.

The immutable successful build record binds the source commit/tree, all build
source hashes, exact wheel and context identities, Docker executable and daemon,
both pinned local base-image identities, exact invocation and canonical argv
hash, log, output image ID, and the complete required label set. Build and
materialization are separate from
proof execution. The assurance executor never builds, pulls, tags, or relabels
the image: it accepts only a separately owner-reviewed execution admission that
hash-binds the validated build record and exact immutable local image.

Proof execution itself uses no network, a read-only mount of the exact clean
source commit, and a writable temporary directory outside that source mount.
The attestation records the exact build-record hash and archived record,
inspected image labels/ID, base-image digests and IDs, Docker version, Node and
Python versions, source-mount mode, network mode, container identity, and
cleanup.

The profile supplies only the native `node --test` assurance runner. The
separately supported Vitest/jsdom component suite is validation, not one of the
eight cataloged Node proof definitions.

The runner intentionally does not add `psql`. `QT-PROOF-002` and
`QT-PROOF-314` therefore remain honestly `UNAVAILABLE` unless a separately
reviewed source-bound runner prerequisite is approved. A successful runner
build is not a proof result, remediation closure, attestation, or guarantee
activation.

## Disposable Database Profile

`python-db-isolated.profile.json` is the executable admission contract for the
seven cataloged database proofs. Each attestation session receives a new
container and database identity from the exact TimescaleDB image. Only an
ephemeral port bound to `127.0.0.1` may be published. The session creates and
records the `timescaledb` and `pgcrypto` extensions, runs no non-proof workload,
and removes the exact container and database resources afterward.

The source commit, Python lockfile, container image digest, server and extension
versions, database identity, bootstrap output, exact proof arguments, result
summaries, and cleanup output are all evidence inputs. A shared development,
live, or production database is forbidden. A missing exact image, extension,
version, isolation boundary, or cleanup capability makes this profile
`UNAVAILABLE`; it does not permit a fallback.

## Recovery And Deployment

Recovery uses the separately reviewed
`docs/assurance/guarantees/procedures/isolated-recovery-rehearsal.md` and requires
two disposable resource identities. No environment is created merely to avoid
an `UNAVAILABLE` result.

QT has no admitted deployment-execution profile in this directory. Static
deployment-contract checks remain distinct from a real Compose deployment,
rollback, migration-negative, or recovery rehearsal. Until the required
deployment, operations, and security review admits such a fixture, deployment
execution remains unresolved rather than silently represented by the static
check.
