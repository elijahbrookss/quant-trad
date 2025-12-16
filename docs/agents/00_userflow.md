# Quant-Trad Userflow & Core Lenses

This document defines the canonical userflow and conceptual boundaries of the system.
Agents MUST preserve these boundaries.

---

## Core Lenses (Do Not Blur These)

Quant-Trad operates through three distinct lenses:

### 1) QuantLab (Research Lens)
Purpose:
- Explore market data
- Create indicators
- Visualize overlays
- Generate indicator signals

QuantLab is a sandbox for experimentation.

Outputs:
- Indicator definitions
- Indicator parameters
- Exploratory signals

It does NOT:
- Execute trades
- Apply risk or ATM logic
- Perform realistic execution simulation

---

### 2) Strategy (Decision Lens)
Purpose:
- Define *what to trade and why*
- Attach indicators created in QuantLab
- Configure rule logic that interprets indicator signals

Strategy signal preview:
- Validates rule correctness
- May ignore execution realism
- Must not apply fees, slippage, or ATM logic

A strategy defines *decisions*, not *execution*.

---

### 3) Bot (Execution Lens)
Purpose:
- Execute a strategy through time as if trading live
- Apply:
  - Risk
  - ATM templates
  - Fee simulation
  - Execution rules

Bots produce:
- Trades
- Metrics
- Features
- Playback visualizations

Bots are the ONLY place where realistic execution occurs.

---

## Canonical Userflow

1) User explores candles and builds indicators in QuantLab
2) User creates a Strategy and attaches existing indicators
3) User previews strategy signals (logic validation only)
4) User creates a Bot and attaches a Strategy
5) Bot runs in one of:
   - Backtest (walk-forward)
   - Paper-sim
   - Live

---

## Invariant Rules
- Indicators are created once and reused
- Strategies consume indicator outputs
- Bots execute strategies
- No layer may skip ahead or peek into the future

If an agent collapses these layers, it is a bug.
