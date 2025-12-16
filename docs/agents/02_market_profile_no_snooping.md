# Market Profile (MPF) — No Data Snooping Rule

Market Profile indicators are especially prone to data-snooping.
This document defines mandatory behavior.

---

## The Core Problem

Market Profile merges may produce profiles that appear to:
- Start at time X
- Cover a historical range

However, in live trading:
- That profile would only be known AFTER sufficient future data exists

Rendering it earlier is data-snooping.

---

## Absolute Rule

Price must NEVER walk into a Market Profile that was not yet known at that time.

A “brand new” MPF must NOT appear in front of price.

---

## Correct Mental Model

- MPFs are created behind price
- Price frequently revisits old MPFs
- New MPFs form in the shadow of price movement

Live traders discover profiles after they form, not before.

---

## Required Implementation Behavior

For every MPF:
- Distinguish between:
  - logical start (where it spans)
  - known_at time (when it becomes valid)

Rendering rules:
- MPF overlays are visible ONLY when `known_at <= playback_time`
- Signals referencing MPFs must obey the same rule

---

## Agent Guardrail

If an MPF overlay suddenly appears ahead of price during bot playback,
it is a bug.

Prefer delayed visibility over early visibility.
