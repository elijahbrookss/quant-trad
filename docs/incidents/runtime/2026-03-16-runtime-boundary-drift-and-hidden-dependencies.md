# Runtime Boundary Drift and Hidden Dependencies (2026-03-16)

This was an architecture investigation, not a production outage. The documented
runtime boundary appeared clear, but following imports required tracing portal
services, compatibility wrappers and lazy package exports.

## What the dependency graph revealed

The nominally canonical runtime reached upward into portal code for storage,
reports, candles, Indicator access and strategy-series construction. Re-export
wrappers preserved a second namespace for the same runtime concepts. Lazy
package facades concealed ownership and made normal source search less useful.

Some indirection had legitimate migration and optional-dependency reasons.
The failure was allowing those temporary seams to become long-lived structure
without enforcing the dependency direction they claimed to represent.
Adding composition classes alone had not made the boundary real.

## Consequential design choice

Keep runtime-domain behavior with the runtime and application composition with
the application. Portal can compose the runtime; runtime code should receive
narrow collaborators at real external boundaries rather than importing portal
service internals.

Database-backed loading, source data, persistence and execution adapters are
real seams. Ordinary collaboration inside the owning runtime does not need a
general dependency-injection framework. Prefer explicit imports and small
dependency bundles; retire compatibility namespaces when their migration ends.

The investigation proposed staged extraction and import cleanup. That sequence
is historical reasoning, not an outstanding migration checklist or evidence that
every proposal was completed. Current ownership belongs to the
[runtime composition boundary](../../architecture/execution-runtime/RUNTIME_COMPOSITION_ROOT.md)
and [engineering contract](../../contracts/platform/03_engineering_contract.md).

## Lasting lesson

An architectural claim needs an enforceable dependency direction. More wrappers,
folders and facades can increase reviewer effort while leaving the coupling
unchanged. Treat difficulty locating the authoritative implementation as
engineering evidence, then inspect the graph before adding another abstraction.
