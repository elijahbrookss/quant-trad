# Execution & Playback Contract

## Bot Execution Contract

Bot runtime owns:
- order decisions in time
- fills and execution effects
- risk and protection behavior
- execution metrics

## Playback Contract

Playback is an audit/debug surface for execution semantics.
It should make visible:
- what was known
- what decision was made
- what execution outcome occurred

## Alignment Rule

Playback views should be derivable from runtime state transitions.
When visualization and runtime disagree, runtime semantics are source of truth.

## Intrabar Policy

Intrabar simulation may be used to improve execution fidelity on coarse strategy timeframes.
