# Quant-Trad

Quant-Trad is a trading research system for taking a strategy you already trade,
or one you are shaping into rules, and making it measurable. It tests indicator
logic and strategy variants against provider candle data, canonical instruments,
and exchange-shaped execution assumptions before the idea is treated like
something that can be automated.

It does not assume one workflow. You can work through the UI, the CLI, reports,
or the MCP adapter for agent hosts. The important part is that each surface sits
over the same contracts, so a run can be replayed, inspected, exported, and
compared without changing what happened.

```text
Data -> Indicators -> Strategy decisions -> Bot runtime -> BotLens / Reports
```

BotLens is the run inspection view: candles, overlays, decisions, trades, and
runtime diagnostics for a bot run. Quant-Trad is research and paper-trading
oriented today; it does not expose live trade execution yet.

The capabilities cluster around the work a strategy has to survive before it
deserves more trust:

| Area | What Quant-Trad gives you |
| --- | --- |
| Indicator authoring | Create indicator definitions and configs, validate typed outputs, and replay indicators over market windows. |
| Strategy authoring | Create strategy logic and variants from indicator outputs, compile them, and preview decisions before running bots. |
| Provider and data layer | Add providers, manage credentials and instruments, and check candle coverage before a run depends on them. |
| Runtime research | Run walk-forward backtests and bounded paper sessions with fills, fees, margin, wallet state, settlement, lifecycle, and events. |
| Inspection | Use BotLens, report exports, and comparisons to understand what happened and how a candidate differs from a baseline. |
| Research orchestration | Create experiment plans across instruments and variants; suite steps run sequentially today, while configured backtest work can use bounded parallel workers when the host supports it. |
| Agent workflow | Expose the same workflow boundary through MCP when an agent host needs protocol tools instead of direct CLI/API calls. |

Use the docs by what you are trying to do next:

| Need | First read | Useful next |
| --- | --- | --- |
| Understand the system shape | [Overview](docs/overview.md) | [Runtime timeline](docs/concepts/runtime-timeline.md), [execution model](docs/concepts/execution-model.md) |
| Start the current stack | [Getting started](docs/getting-started.md) | [Docs home](docs/index.md) |
| Operate workflows | [Command and workflow surface](docs/engineering/developer-audit-workflow.md) | [MCP research server](docs/architecture/research-orchestration/MCP_RESEARCH_SERVER.md) |
| Build strategy logic | [Strategies and signals](docs/concepts/strategies-and-signals.md) | [Creating an indicator](docs/guides/creating-an-indicator.md), [creating a strategy](docs/guides/creating-a-strategy.md) |
| Inspect run evidence | [BotLens](docs/concepts/botlens.md) | [Reporting datasets](docs/concepts/reporting-datasets.md) |
| Extend the system | [Adding a provider](docs/guides/adding-a-provider.md) | [Coinbase derivatives paper setup](docs/guides/coinbase-derivatives-paper-setup.md), [architecture contracts](docs/contracts/README.md) |

Contracts are the source of truth when code and explanatory docs disagree.

Quant-Trad is in active development. The system is intended for research,
backtesting, paper trading, and controlled environments unless you have
independently reviewed the execution path, provider configuration, and risk
controls for your use case.
