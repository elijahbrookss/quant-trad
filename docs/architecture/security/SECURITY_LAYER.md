---
component: security-layer
subsystem: security
layer: boundary
doc_type: architecture
status: active
tags:
  - security
  - credentials
  - providers
  - post-quantum
  - runners
code_paths:
  - src/data_providers/services/credential_store.py
  - src/data_providers/registry.py
  - src/data_providers/providers
  - src/core/settings.py
  - portal/backend/controller/providers.py
  - portal/backend/service/providers
  - portal/backend/db/session.py
  - portal/backend/service/bots/runner.py
  - cli/setup.py
  - cli/main.py
  - cli/audit.py
---
# Security Layer

## Purpose

This document is the working security reference for Quant-Trad. It is
intentionally honest: it records what the system protects today, which trust
assumptions those protections depend on, and which gaps matter most before
remote or multi-tenant hosting.

## Current Trust Boundaries

| Boundary | Current Assumption |
| --- | --- |
| Local developer machine | Trusted operator environment. Shell history, process memory, and local files are operator-owned. |
| Backend API | Trusted local control plane. Do not expose it to untrusted networks without auth, TLS, and network policy. |
| Postgres | Canonical persistence boundary via `PG_DSN`; stores encrypted credential blobs and metadata. |
| Bot runner/container | Trusted worker process. It may receive `PG_DSN` and the provider credential encryption key when provider-backed work requires it. |
| Provider APIs | External trust boundary. Provider SDKs, TLS, API-key scopes, and provider auth semantics are outside Quant-Trad's control. |
| CLI audit logs | Local operational evidence. Secret-bearing CLI arguments are redacted before audit files are written. |

## What Is Secure Today

- Provider API keys are not centralized settings fields.
- Legacy direct env bindings for Alpaca and CCXT API keys are removed.
- Provider credentials are written through one backend API and stored as
  encrypted `credential_ref` records.
- Credential API reads return metadata only; secret values are never returned.
- CLI credential add supports no-echo prompts, stdin JSON, and env-var mapping.
- CLI audit logs redact `--secrets-json` and secret-bearing argument fields.
- Portal DB lifecycle logs redact `PG_DSN` userinfo and sensitive query values.
- Provider registry metadata declares required and optional credential keys so
  UI, CLI, agents, and adapters share one capability contract.
- Provider adapters resolve credentials through the credential store rather than
  reading provider-specific API key env vars.
- Provider credential references do not grant venue-trading authority. Current
  runtime execution is simulated or observe-only; production order submission
  is a closed boundary.
- Credential refs can be validated structurally and revoked without deleting
  audit metadata.
- The `portal_provider_credential_refs` table and lookup index are owned by
  portal schema bootstrap. Credential helpers validate the bootstrapped table
  shape before use; they do not create a duplicate credential schema at runtime.

## Known Gaps

- Backend API authentication is not a production-grade security boundary today.
  Treat the backend as local/trusted unless an auth layer and network policy are
  added.
- `QT_SECURITY_PROVIDER_CREDENTIAL_KEY` is an env-held master key. Anyone with
  that key plus `PG_DSN` can decrypt active provider credential blobs.
- Bot runners remain trusted workers. A compromised runner with DB and
  encryption-key access can read credentials beyond the single run it is
  executing.
- There is no per-run credential lease, scoped decrypt token, external vault,
  KMS/HSM envelope encryption, or provider-key rotation workflow yet.
- Secrets necessarily exist in process memory while the backend saves them and
  while adapters initialize provider SDK clients.
- Provider API permissions are operator-managed. Quant-Trad cannot currently
  prove least-privilege scopes on third-party API keys.
- `RuntimeMode.LIVE` is a reserved composition seam rather than an enforced
  production-trading security boundary. Live order submission remains
  unsupported and requires separate admission, authorization, risk, and
  reconciliation controls.
- The legacy `portal_provider_credentials` table may exist in old local
  databases. Current code no longer writes or reads it; the manual migration
  imports those rows into credential refs when the table exists.

## Provider Credential Model

Credentials are stored as `credential_ref` records:

- `credential_ref`: stable operator-facing reference,
- `provider_id` and `venue_id`: provider boundary scope,
- `environment`: default `paper`, with room for `live`/`sandbox`,
- `status`, `validation`, timestamps, and revocation metadata,
- encrypted secret payload.

Typical human workflow:

```bash
qt setup provider coinbase
```

Agent-safe workflow:

```bash
printf '%s\n' '{"COINBASE_API_KEY":"...","COINBASE_API_SECRET":"..."}' \
  | qt setup provider coinbase \
      --secrets-json - \
      --no-input
```

Avoid inline secret arguments. Audit logs redact them, but shell history may
still capture the original command before Quant-Trad sees it.

## Cryptographic maintenance

Credential encryption, TLS, SSH and provider signing depend on maintained
cryptographic libraries and operator-managed keys. Keep dependencies current
and inventory those dependencies before changing algorithms or deployment trust.
Use supported ecosystem migration paths rather than implementing cryptography
inside QT. The external references below explain post-quantum migration;
they are guidance, not a claim that QT has completed a cryptographic inventory.

Immediate hosted-deployment work remains authenticated operator identity, TLS
and network policy, scoped runner credentials, and managed key rotation. Those
controls must be qualified against the actual deployment rather than inferred
from encryption at rest.

## References

- [NIST: Post-Quantum Cryptography](https://www.nist.gov/programs-projects/post-quantum-cryptography)
- [NIST: First finalized post-quantum encryption standards](https://www.nist.gov/news-events/news/2024/08/nist-releases-first-3-finalized-post-quantum-encryption-standards)
- [NIST NCCoE: Migration to Post-Quantum Cryptography](https://www.nccoe.nist.gov/applied-cryptography/migration-to-pqc)
- [NCSC: Migrating to Post-Quantum Cryptography](https://www.ncsc.gov.uk/blog-post/migrating-to-post-quantum-cryptography-pqc)
- [ADR 0024: Provider Credential References](../decisions/0024-use-provider-credential-references.md)
- [ADR 0048: Gate Agent Mutation And Research Promotion](../decisions/0048-gate-agent-mutation-and-research-promotion.md)
- [ADR 0049: Keep Live Order Submission Closed](../decisions/0049-keep-live-order-submission-closed.md)

## Runtime archive filesystem access

For the single-node server, the trusted Docker runner grants bot containers a
read-only bind of the configured archive directory and, in dedicated drive mode,
read-only udev identity metadata. It does not share the backend's Docker socket,
database files or live spool. The host path comes from operator-owned
QT_MARKET_DATA_ROOT, never bot parameters. Mount path validation rejects root,
relative, traversal and mount-option separator values. Archive readers retain
checksum and filesystem UUID checks. This narrow read view does not change the
existing trusted-worker database and credential boundaries described above.
