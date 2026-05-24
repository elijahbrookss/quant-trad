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
  - portal/backend/service/bots/runner.py
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
- Provider registry metadata declares required and optional credential keys so
  UI, CLI, agents, and adapters share one capability contract.
- Provider adapters resolve credentials through the credential store rather than
  reading provider-specific API key env vars.
- Credential refs can be validated structurally and revoked without deleting
  audit metadata.

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
python -m cli.main providers credentials add --provider COINBASE --venue COINBASE_DIRECT
```

Agent-safe workflow:

```bash
printf '%s\n' '{"COINBASE_API_KEY":"...","COINBASE_API_SECRET":"..."}' \
  | python -m cli.main providers credentials add \
      --provider COINBASE \
      --venue COINBASE_DIRECT \
      --secrets-json - \
      --no-input
```

Avoid inline secret arguments. Audit logs redact them, but shell history may
still capture the original command before Quant-Trad sees it.

## Post-Quantum Risk Points

NIST released FIPS 203, FIPS 204, and FIPS 205 for ML-KEM, ML-DSA, and
SLH-DSA. NIST also recommends beginning migration planning now, including
finding where quantum-vulnerable public-key cryptography is used.

Quant-Trad risk points:

- TLS connections to provider APIs, backend endpoints, GitHub, package indexes,
  Docker registries, and future VPS/home-server endpoints depend on ecosystem
  TLS stacks and certificates. Classical public-key key exchange and signatures
  are the main post-quantum exposure.
- Provider authentication may rely on provider-specific signatures, JWTs, RSA,
  ECDSA, or HMAC. The exact post-quantum exposure is provider-specific and must
  be inventoried per provider.
- SSH keys for VPS/home-server access, Git remotes, deployment automation, and
  image signing can become migration blockers if they remain RSA/ECDSA-only.
- Stored encrypted credential blobs are primarily symmetric-crypto protected.
  Symmetric encryption is not the main Shor-algorithm failure mode, but
  long-lived encrypted secrets should still plan for stronger symmetric margins,
  key rotation, and envelope encryption.
- Store-now-decrypt-later matters for long-lived traffic captures: any provider
  API keys, JWTs, or session tokens sent over non-PQ TLS could be exposed later
  if the transport is captured and broken.

## PQ Migration Posture

- Build and maintain a cryptographic inventory for TLS, SSH, provider auth,
  package signing, Docker image signing, credential encryption, and database
  connections.
- Prefer ecosystem-provided hybrid/PQ TLS, SSH, and signing support when it is
  stable. Do not hand-roll PQ cryptography inside Quant-Trad.
- Keep Python, OpenSSL, provider SDKs, Docker, and OS packages current so PQ
  support can arrive through maintained stacks.
- Rotate provider API keys after any transport or host compromise, and again
  after future PQ migration milestones when providers support safer auth.
- For hosted deployments, prioritize auth, TLS, network policy, scoped runner
  permissions, and an external secret manager before adding more runners.

## High-Value Hardening Backlog

1. Backend auth and operator identity for local-to-remote deployments.
2. Per-run scoped credential leases so runners cannot decrypt unrelated refs.
3. Envelope encryption using KMS/Vault/SOPS-age instead of one env-held Fernet key.
4. Provider permission checks and docs for least-privilege API-key scopes.
5. Secret rotation command and revoked-ref audit reporting.
6. Crypto inventory with PQ readiness status per dependency and provider.

## References

- [NIST: Post-Quantum Cryptography](https://www.nist.gov/programs-projects/post-quantum-cryptography)
- [NIST: First finalized post-quantum encryption standards](https://www.nist.gov/news-events/news/2024/08/nist-releases-first-3-finalized-post-quantum-encryption-standards)
- [NIST NCCoE: Migration to Post-Quantum Cryptography](https://www.nccoe.nist.gov/applied-cryptography/migration-to-pqc)
- [NCSC: Migrating to Post-Quantum Cryptography](https://www.ncsc.gov.uk/blog-post/migrating-to-post-quantum-cryptography-pqc)
- [ADR 0024: Provider Credential References](../decisions/0024-use-provider-credential-references.md)
