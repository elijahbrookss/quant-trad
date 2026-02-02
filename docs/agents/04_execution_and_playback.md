# Execution & BotLens Playback Rules

BotLens is not cosmetic.
It is a correctness debugger.

---

## Playback Purpose

Playback must allow a human to verify:
- indicator timing
- signal placement
- trade entry and exit
- stop and target behavior

If playback hides mistakes, it is lying.

---

## Intrabar Realism

When strategy timeframes are coarse:
- Bots may simulate intrabar behavior using lower-timeframe data
- Stops and targets must resolve realistically

Execution shortcuts are not allowed in bots.

---

## Speed vs Truth

Playback may be slow.
Playback must never be incorrect.

Correctness beats smoothness.
