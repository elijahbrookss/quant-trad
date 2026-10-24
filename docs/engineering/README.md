# Engineering Manual

Use contributor workflow for commands and validation; open a component boundary
when a change affects product meaning.

- [Developer workflow](developer-workflow.md): environment, command surfaces,
  qualification and handoff.
- [Testing strategy](testing/testing-strategy.md) and
  [CI topology](testing/ci-test-topology.md): consequence-based checks and proof scope.
- [Documentation standard](documentation/component-documentation-standard.md):
  authority, current meaning and diagrams.
- [Implementation map](architecture.md), [runtime](runtime-engine.md) and
  [data layer](data-layer.md): find code and owning boundaries.
- [Observability](observability.md): logs, capacity, dashboards and diagnosis.
- [Server deployment](server-deployment.md): install, promote and recover.
- [Dated Fact headers](fact-header-layout-v2.md): identity/routing and preserving dependencies.
- [Storage qualification and recovery](storage-tiering-validation.md): restore
  ordering, legacy cutovers, actual-drive measurement and failure testing.

For daily operation start with the [operator handbook](../operators/README.md).
Exact platform meaning lives in [contracts](../contracts/README.md); component
navigation lives in the [architecture index](../architecture/ARCHITECTURE_COMPONENT_INDEX.md).
Public documentation describes reusable engineering and operating procedures.
Actual research and installation-specific records belong in their appropriate
private stores, not a public implementation diary.
