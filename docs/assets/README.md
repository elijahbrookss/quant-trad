# Documentation Asset Lineage

This directory records documentation assets whose lineage is distinct from the
canonical architecture-diagram workflow described by the
[architecture navigation hub](../architecture/README.md). The
[historical architecture documentation model](../architecture/ARCHITECTURE_DOCS_MODEL.md)
records how that workflow was established.

## Architecture Diagram Policy

Mermaid `.mmd` files under `docs/architecture/` are the diagram sources of
record. A same-name SVG written beside an `.mmd` file is an optional rendered
derivative. An SVG need not exist when rendering tooling is unavailable, and a
rendered derivative does not change the semantic authority of its source.

## Retained Unverified Asset

`quant-trad-platform-flow.svg` is retained in place. Its source, generator, and
reviewed lineage are unknown, and no current documentation page embeds or uses
it as architecture evidence. Its repository presence does not establish that
it is generated, current, canonical, or authoritative.

Status: **retained, unreferenced as architecture evidence, and unverified**.

This record does not delete, recreate, promote, or infer lineage for the asset.
Recovering a source or changing this disposition requires a separate explicit
maintenance decision.
