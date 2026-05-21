---
component: adr-provider-credential-references
subsystem: security
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - security
  - credentials
  - providers
  - cli
code_paths:
  - src/data_providers/services/credential_store.py
  - src/data_providers/registry.py
  - src/data_providers/providers
  - src/core/settings.py
  - portal/backend/controller/providers.py
  - portal/backend/service/providers
  - cli/main.py
  - cli/audit.py
  - config/defaults.yaml
  - scripts/db/manual_migration_provider_credential_refs_v1.sql
---
# ADR 0024: Use Provider Credential References

## Status

Accepted on 2026-05-19.

## Context

Provider capability and credential handling had two competing paths:

- the provider registry described providers and venues,
- some adapters still read provider API keys from settings/env,
- the encrypted credential store keyed secrets only by provider/venue.

That shape made runners smarter than they should be. It also made credential
usage harder to audit because runtime config could accidentally become a
secret transport surface.

## Decision

Provider credentials are now stored and referenced by explicit
`credential_ref` records.

The credential store owns encrypted secret payloads, safe metadata, validation
metadata, last-used timestamps, status, environment, and revocation state. API
and CLI surfaces accept secret values only on write and return only metadata.

Provider registry metadata declares required and optional secret keys. Adapters
resolve credentials only through the credential store; provider-specific API key
settings and env bindings are not a supported runtime path.

Runners remain provider-agnostic. They do not declare which symbols or
providers they support. They run assigned work, emit instrumentation, and rely
on the backend/provider boundary to resolve capabilities and credentials.

## Consequences

- Provider API keys no longer live in centralized settings or bot run config.
- CLI, UI, and IDE/agent workflows use the same backend credential API.
- Agents can add credentials safely with stdin/env workflows while CLI audit
  logs redact secret-bearing arguments.
- Credential metadata can be listed, validated, and revoked without exposing
  secrets.
- A trusted runner that has both `PG_DSN` and
  `QT_SECURITY_PROVIDER_CREDENTIAL_KEY` can still decrypt active provider
  credentials. Per-run credential leases or an external secret manager remain
  future hardening work.

## References

- [Security Layer](../security/SECURITY_LAYER.md)
- [Data Boundary](../data/DATA_BOUNDARY.md)
- [Engineering Contract](../../contracts/platform/03_engineering_contract.md)
