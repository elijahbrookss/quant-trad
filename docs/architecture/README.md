# Architecture Docs

This folder is partitioned by subsystem.

Use subsystem folders instead of adding more top-level doc categories:

- `engine/`
- `indicators/`
- `market/`
- `providers/`
- `reporting/`
- `runtime/`
- `signals/`
- `storage/`

Rules:

- Component and subsystem design docs belong here.
- Put docs in the folder that matches the owning subsystem, not the team or date.
- Keep `ARCHITECTURE_COMPONENT_INDEX.md` at the root of this folder.
- If a doc has architecture frontmatter, it should live somewhere under this tree.
