---
component: cli-setup-boundary
subsystem: cli
layer: boundary
doc_type: architecture
status: active
tags:
  - cli
  - setup
  - onboarding
  - credentials
code_paths:
  - cli/setup.py
  - cli/main.py
  - scripts/qt
  - Makefile
  - secrets.env.example
  - docs/getting-started.md
---
# CLI Setup Boundary

## Purpose

`qt setup` is the canonical local readiness and provider-onboarding layer. It
owns operator-facing checks for Python, the project virtualenv, editable
install, `secrets.env`, `PG_DSN`, provider credential encryption key, backend
health probing, and provider onboarding commands.

Make owns local dependency installation and Docker/infra mechanics. `qt setup`
does not create, delete, or reinstall the project virtualenv.

## Boundary Rules

- `qt setup env` creates or repairs `secrets.env` without storing provider API
  keys in that file.
- `qt setup doctor` emits machine-readable readiness checks with explicit
  remediation.
- `qt setup provider <provider>` composes provider-specific onboarding through
  canonical provider APIs.
- Provider secrets still flow through the backend credential API and encrypted
  credential-ref store.
- `scripts/qt` is only a dispatcher into `qt` or `python -m cli.main`; it is
  not a setup implementation.
- `make deps` owns venv creation and editable installation.

## Ownership Split

| Surface | Owns |
| --- | --- |
| `qt setup` | Setup doctor checks, local operator env values, provider onboarding composition |
| `qt providers` | Provider metadata, credential refs, validation, stream smoke checks |
| Make | Python deps, Docker stack, tests, logs, docs sync, DB helpers, forensic helpers |
| UI | Human visualization and inspection |

## Credential Handling

`secrets.env` contains local infrastructure values and the provider credential
encryption key. Provider API keys are not long-lived env settings; they are
accepted only during credential writes and persisted as encrypted
`credential_ref` records through the backend provider API.

The setup layer can generate a missing placeholder encryption key during local
bootstrap. It must not silently rotate a non-placeholder key because existing
credential refs may become undecryptable.
