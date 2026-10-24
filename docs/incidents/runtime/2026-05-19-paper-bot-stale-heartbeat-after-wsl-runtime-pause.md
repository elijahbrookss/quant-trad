# Paper Bot Stale Heartbeat After WSL Runtime Pause (2026-05-19)

A paper run was marked degraded after a long silence across backend and bot
logs. WSL clock-change observations surrounded the gap. On resumption the
watchdog found an expired heartbeat; later manual recovery stopped and restarted
services.

## Evidence and uncertainty

The coincident silence was consistent with host sleep, WSL/Docker pause or an
equivalent runtime suspension. The stale row alone did not establish which
mechanism paused the runner. Later WebSocket close errors and a transient Loki
ring-health failure were symptoms; they did not explain the first degradation.

The watchdog classification was appropriate for stale ownership. The missing
capability was durable operational evidence connecting the expired heartbeat to
runner time gaps and nearby container lifecycle facts. The investigation did not
identify a reason to change strategy, Indicator, wallet or playback semantics.

## Corrective boundary

The response added runner clock-gap detection, Docker lifecycle observations
and bounded watchdog diagnostics. A separate event observer can retain container
exit evidence when the backend itself is gone. Runner identity owns pause
correlation; Docker facts describe process lifecycle rather than canonical
execution truth.

These decisions are recorded in
[ADR 0021](../../architecture/decisions/0021-use-runner-clock-gap-sentinel.md),
[ADR 0022](../../architecture/decisions/0022-capture-docker-container-lifecycle-as-runner-agnostic-observability.md)
and [ADR 0023](../../architecture/decisions/0023-persist-watchdog-degradation-diagnostics.md).
Use the current [observability boundary](../../architecture/observability/OBSERVABILITY_BOUNDARY.md)
for implementation and operating guidance.

## Lasting lesson

Runner liveness needs its own evidence. A stale heartbeat should retain its age,
previous owner, detecting runner and available pause/container context without
pretending to prove more than those observations establish.
