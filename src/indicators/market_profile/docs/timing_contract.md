# Market Profile Timing Contract

## Scope

This document defines Market Profile-specific timing semantics.

## Core Semantics

For each profile, distinguish:
- structural span: region represented by the profile
- known-at time: when profile is valid for use

Historical span does not imply early availability.

## Consumption Rule

Signals and overlays should consume the same profile timeline.
A profile influences output when `known_at <= evaluation_time`.

## Merge Rule

Merged profiles become valid when merge criteria are satisfied in time.
Known-at gating applies equally to merged and unmerged profiles.
