# Quant-Trad frontend

The frontend is Quant-Trad's human presentation and inspection boundary. It
composes typed backend read models with browser-local navigation and display
state. Runtime, service, persistence, and data-plane owners remain authoritative
for execution, lifecycle, evidence, and market truth; frontend state is never
workflow or domain truth.

## Supported operator surface

Operator Console v2 starts at `v2.html` through `src/main-v2.jsx`. Its two
primary rooms are:

- **Overview** for bounded attention and activity read models;
- **Operations** for run, market, collector, and research evidence discovery.

Those primary rooms are GET/read-only. Routed lenses may expose an approved
action without taking ownership of the underlying subsystem. In particular,
collector actions use only the canonical
`/api/market-data/operations/collectors/.../actions/...` boundary backed by
`CollectorOperationsService`. The browser does not supervise collectors,
translate that contract into provider-specific lifecycle calls, or treat a
requested action as evidence that the action succeeded.

The exact V2-owned source roots covered by the read-only-surface contract test
are:

- `src/v2`;
- `src/features/overview`;
- `src/features/operations`;
- `src/features/collectors`;
- `src/features/bots/botlens`.

See
[`OPERATOR_CONSOLE_V2.md`](../../docs/architecture/frontend/OPERATOR_CONSOLE_V2.md)
for the active architecture boundary and `docs/contracts/platform/` for the
higher product authority.

## Install and run

From this directory:

```bash
npm ci
npm run dev
```

Vite also builds `index.html`; `v2.html` is the supported V2 operator entry.
The development server proxies `/api` to `VITE_API_PROXY_TARGET` or, when it is
unset, `http://localhost:8000`.

## Tests and build

The frontend intentionally has two test runners:

```bash
npm run test:node  # Node-native model, adapter, and source-contract suites
npm run test:jsx   # the two tracked React component suites in jsdom
npm test           # both test groups
npm run build      # Vite production compilation for both HTML entries
```

`test:node` is the shell-free `node --test` path. `test:jsx` is a pinned
Vitest/jsdom profile that discovers exactly:

- `src/components/__tests__/DeleteIndicatorModal.test.jsx`;
- `src/components/__tests__/IndicatorCard.test.jsx`.

From the repository root, `make frontend-check` runs both test groups and then
the production build. `npm run lint` remains a separate diagnostic command and
is not part of `frontend-check`.

These checks cover only their bounded assertions and the compilation contract.
They do not run a real backend, a browser or cross-browser/E2E suite, production
deployment, live collector or order activity, or accessibility conformance.
