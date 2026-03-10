# Docs Map

This tree is organized by document intent first.

Use these folders as hard boundaries:

- `contracts/`: normative platform rules and behavior contracts
- `architecture/`: subsystem design and composition reference docs
- `engineering/`: contributor-facing working standards, testing policy, and doc-writing rules
- `incidents/`: dated incident writeups and debugging records

## Read Order

1. `contracts/README.md`
2. `contracts/platform/00_system_contract.md`
3. `contracts/platform/01_runtime_contract.md`
4. `contracts/platform/02_execution_playback_contract.md`
5. `contracts/platform/03_engineering_contract.md`

Then move into `architecture/` for subsystem-specific design details.

## Folder Rules

### `contracts/`

- Holds stable, normative rules.
- If code disagrees with these docs, the docs win until corrected.
- Do not mix in process notes, testing policy, or one-off incidents.

### `architecture/`

- Holds component and subsystem design docs.
- Organized by subsystem, not by date.
- If a doc describes runtime, providers, storage, signals, or reporting design, it belongs here.

### `engineering/`

- Holds how-we-work documents.
- Testing policy, CI topology, and documentation standards belong here.
- This folder is for contributor discipline, not system behavior contracts.

### `incidents/`

- Holds dated writeups for specific failures, investigations, and remediation notes.
- If the document is anchored to a concrete incident or debugging session, it belongs here.
- These are historical records, not source-of-truth contracts.

## Naming Guidance

- Keep contract and architecture doc filenames stable once linked broadly.
- Use subsystem folders instead of inventing new top-level categories.
- Put new incident notes under `incidents/<subsystem>/YYYY-MM-DD-...md`.
