# Testing Strategy

## Purpose

Tests in Quant-Trad exist to protect critical behavior, not to maximize coverage.

They are guardrails for:

- correctness in financial and runtime behavior
- determinism in backtests and derived artifacts
- safe refactoring of core engine paths
- catching regressions that would otherwise be expensive to rediscover

We do not write tests just because a function exists. A test should protect a meaningful contract, a meaningful risk boundary, or a bug we do not want to pay for twice.

## Default Posture

The default posture in this repo is:

- tests are not added automatically
- tests must be justified by risk, contract protection, or regression value
- if a proposed test does not protect a real system outcome, it should usually not be written

That means "I changed code, so I need to add tests somewhere" is not sufficient reasoning.

Good reasons to add a test:

- the change affects deterministic backtest behavior
- the change affects trade decisions, fills, wallet state, margin, fees, or other money logic
- the change protects a runtime contract or module boundary that is easy to break
- the test locks in a real regression we have already paid for

Bad reasons to add a test:

- the file had no tests
- the function is new
- coverage would go up
- an assistant generated a test by following control flow line by line

## Current Phase

This project is in an engine-first phase.

Current priorities are:

- building a stable engine
- making backtests reproducible
- improving performance
- enabling future strategy research and AI integration

That means the testing strategy is intentionally selective right now.

- Broad integration coverage is mostly deferred until runtime and backtest architecture are more stable.
- Unit tests are still valuable, but only when they protect behavior that matters.
- Temporary test gaps are acceptable when they are intentional, visible, and tied to active architectural churn.

The rule is simple: do not pretend the system is more stable than it is. Test the parts where failure would distort the engine, corrupt research, or make refactors unsafe.

## What Should Be Tested Now

### Deterministic Backtest Behavior

If the same inputs, params, versions, and market data produce different outputs, the platform becomes untrustworthy.

High-value examples:

- identical backtest inputs producing identical trade sequences
- stable ordering of state updates and emitted events
- known-at timing behavior that does not drift across refactors
- reproducible snapshots and derived runtime outputs when inputs are fixed
- stable handling of intrabar execution when the same scenario is replayed

### Financial Math Correctness

Financial math errors are dangerous because they can look plausible while being wrong.

High-value examples:

- PnL calculations
- fee and slippage calculations
- position sizing and quantity rounding
- wallet reservation math
- margin, liquidation, and contract-multiplier handling
- partial-fill accounting

### State Transition Correctness

This repo is full of stateful systems. State bugs are often worse than obvious exceptions because they silently distort runtime behavior.

High-value examples:

- order lifecycle transitions
- wallet and reservation updates
- partial-fill to filled/cancelled transitions
- runtime state-engine transitions across `initialize -> apply_bar -> snapshot`
- monotonic event sequencing
- run-status transitions and failure handling

### Import, Bootstrap, and Module-Boundary Regressions

Some breakages are architectural, not mathematical. A bad import path, startup dependency, or module coupling can take out the runtime even when the core logic is fine.

High-value examples:

- module import tests that prevent accidental bootstrap failures
- startup/config wiring that affects runtime availability
- boundary regressions where one subsystem starts depending on the wrong layer
- tests that prevent the backend or worker bootstrap path from accidentally requiring the full runtime in the wrong place

### Pure Logic With Real Business Value

Pure logic is worth testing only when it materially affects:

- trade decisions
- risk or exposure logic
- state transitions
- research outputs
- reproducibility

This does not mean "any pure function."

It means logic such as:

- signal classification that changes whether a trade is taken
- regime logic that changes overlays, execution policy, or research interpretation
- normalization rules that change the meaning of derived data
- scheduling or selection logic that changes what the engine sees and when

If the logic is pure but does not materially affect system behavior, backtest meaning, runtime contracts, or research output, it usually does not need a test right now.

## What Should Not Usually Be Tested Right Now

Do not spend test budget on low-signal coverage.

Usually not worth testing:

- trivial helpers
- obvious pass-through wrappers
- getters, setters, and formatting glue
- tests that mirror implementation line by line
- tests built from excessive mocking where the mock interactions are the whole assertion
- snapshot-style tests that fail on every refactor without protecting a meaningful outcome
- tests whose main effect is to freeze internal structure that is still changing

Examples of low-value tests in this phase:

- asserting that a wrapper calls one internal helper with the same arguments and returns the same object
- mocking five collaborators just to prove a branch executed
- testing a tiny conversion helper whose behavior is already obvious from one line of code
- asserting the exact shape of an unstable internal snapshot payload when only one derived value matters
- freezing the order of internal helper calls in a runtime path that is still being refactored

## When It Is Acceptable To Not Add Tests

It is acceptable to not add tests when that choice is deliberate and technically honest.

Common acceptable cases:

- the code is exploratory and the contract is not settled yet
- the area is in active architectural churn and any test would be tightly coupled to implementation details
- the change is a refactor where the only possible new tests would fossilize unstable internals
- the logic is temporary and will likely be replaced once the engine boundary is clarified
- the change is better protected later by a higher-value deterministic or integration-style test that does not exist yet

This is not an excuse to skip tests for risky behavior.

It is acceptable to skip tests when the alternative would be low-value noise. In that case, the task or PR should say so clearly:

- what changed
- why tests were not added
- what future stable contract should eventually carry the protection

If a change touches determinism, money logic, state transitions, runtime contracts, or a real regression surface, the bar for skipping tests is much higher.

## Decision Framework Before Adding a Test

Before adding a test, ask:

- If this breaks, does it corrupt backtests?
- If this breaks, does it affect money, fills, margin, wallet state, or risk logic?
- If this breaks, does it introduce nondeterministic behavior?
- If this breaks, does it damage a critical state transition?
- If this breaks, does it change known-at timing or artifact visibility?
- If this breaks, would a future refactor likely miss it?
- If this breaks, would debugging it later be expensive or misleading?

If none of those apply, the test may not be worth adding.

## Preferred Test Hierarchy For This Stage

### Highest Priority

- deterministic behavior tests
- correctness tests for financial math
- state transition and sequencing tests
- tests that protect known-at timing and runtime semantics

### Medium Priority

- module contract tests for critical architecture boundaries
- import/bootstrap regression tests that protect runtime startup
- focused regression tests for bugs that previously escaped

### Lower Priority

- isolated unit tests for stable pure logic with clear business impact

### Deferred For Now

- broad integration suites across the full runtime
- end-to-end orchestration coverage for every service path
- large scenario matrices that will churn with the architecture

We will need those later. They are just not the highest-leverage tests right now.

## Unit Test Style Rules

### Test Behavior, Not Implementation

Assert outcomes, invariants, and contracts. Do not assert every internal step unless the internal step is itself the contract.

Bad pattern:

- test knows the exact helper sequence, exact private method flow, and exact internal data shape

Better pattern:

- test proves the engine emitted the correct state, trade, timing gate, reservation amount, or calculation result

### Keep Fixtures Small

Use the smallest amount of state needed to make the behavior obvious.

Good unit tests should be readable without scrolling through a factory maze or a giant JSON fixture to find the one field that matters.

### Prefer In-Memory State Over Heavy Mocks

If a test can use a small real object, a small runtime state, or a small fake in-memory structure, prefer that over mocking the world.

### Avoid Mocking the World

Mocks are acceptable at real boundaries:

- storage
- external providers
- process/container orchestration
- network calls

Mocks are not a substitute for understanding behavior. If the test mostly verifies that mocks were called, it is probably too implementation-focused.

### Keep Scaffolding Small

Do not introduce large fixture, factory, or mock systems unless they are clearly reused and actually reduce complexity.

One bug should not create a large permanent test framework.

If a regression can be protected with one focused test and a small in-memory setup, do that. Do not respond to a narrow bug by building a miniature testing platform around it.

### Make The Contract Obvious

A reader should be able to answer:

- what behavior is protected?
- what regression would this catch?
- why does this matter to the engine or backtest system?

If that is not obvious, tighten the test.

## Examples Of Good Tests For This Phase

- a deterministic backtest test proving the same bars and config produce the same fills, PnL, and event sequence twice
- a known-at timing test proving an overlay or signal is not visible before its contract says it should exist
- a wallet reservation test proving reserved capital is reduced correctly across order creation, partial fill, and cancellation
- a partial-fill accounting test proving quantity, average fill price, fees, and remaining order state stay coherent
- a state transition test proving `initialize -> apply_bar -> snapshot` produces the expected runtime state at a critical edge case
- an import/bootstrap regression test proving a backend or worker module still imports without pulling in the wrong runtime dependencies
- a margin or liquidation math test proving futures exposure and contract-size math stays correct after a refactor

## Examples Of Low-Value Tests For This Phase

- a test that asserts a helper called another helper with the same arguments and returned the same result
- a test that snapshots a large internal runtime payload even though only one field matters to correctness
- a mock-heavy test for a bot service where every collaborator is stubbed and the only real assertion is that mocks were invoked
- a test that freezes the exact structure of unstable internal state during an ongoing engine refactor
- a test for a trivial parser or formatter that has no effect on trades, risk, timing, or reproducibility
- a test that reproduces implementation branches instead of protecting a contract, such as asserting each internal method was called in sequence during a backtest step

## Regression Tests

When a real bug happens, a targeted regression test is usually worth adding.

This is especially true for:

- determinism bugs
- import/bootstrap failures
- state transition bugs
- calculation bugs
- runtime contract mismatches

The regression test should be minimal and focused.

Good regression test:

- reproduces the failing input or state
- proves the intended behavior after the fix
- stays narrow enough that future refactors can still move code around

Bad regression response:

- building a giant new test scaffold around one bug
- freezing unrelated internals because one path broke once
- turning a one-off failure into a permanent factory/mocking system nobody wants to maintain

## What We Are Intentionally Not Optimizing For

Right now we are not optimizing for:

- maximum code coverage
- testing every file
- premature integration coverage
- enterprise process for its own sake
- large test suites that create maintenance drag without protecting real outcomes

Coverage is a side effect of testing important behavior well. It is not the goal.

## How To Evolve This Policy Later

As the engine and runtime architecture stabilize, this policy should evolve toward broader system confidence.

Next layers to add over time:

- integration tests around runtime flows and storage boundaries
- backtest scenario suites for representative market conditions
- strategy validation harnesses for research workflows
- reproducibility benchmarks
- performance benchmarks on critical engine paths

The order matters. First stabilize the runtime contracts, then expand the coverage surface.

## Testing Rules Of Thumb

- Test where failure would make results wrong, not merely inconvenient.
- Prefer one sharp test over five noisy tests.
- If a test breaks every refactor, it should justify that cost.
- If a bug changed money, timing, state, or determinism, write a regression test.
- If a test mostly proves that mocks were configured correctly, rewrite or delete it.
- "No test added" is acceptable when the alternative is low-value implementation coupling.

## PR Test Expectations

For normal feature work in the current phase:

- add or update tests when the change affects determinism, financial math, state transitions, runtime contracts, or a known regression surface
- do not add low-value tests just to pad coverage
- it is acceptable to ship without new tests when the area is still in active churn, as long as that choice is deliberate
- if tests are skipped, the PR or task notes should say why the gap is acceptable right now

Current CI note:

- GitHub PR CI is currently a fast host-run regression screen, not a full container-runtime proof.
- Do not write tests that assume the PR gate already reproduces all Docker/runtime behavior.
- If a change is only meaningful under container/runtime wiring, treat that as a separate validation problem instead of forcing low-value unit coverage.

Expected standard:

- enough testing to protect meaningful risk
- not enough testing to fossilize unstable internals
