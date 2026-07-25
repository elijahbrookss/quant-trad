"""Series execution runners for bot runtime."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, List, Optional, Protocol
import concurrent.futures

class SeriesState(Protocol):
    """Minimal series state surface needed by runners."""

    series: object
    bar_index: int
    total_bars: int
    done: bool
    next_step_at: Optional[datetime]


@dataclass(frozen=True)
class SeriesRunnerContext:
    """Callbacks and shared flags used by series runners."""

    stop_event: threading.Event
    pause_event: threading.Event
    live_mode: bool
    mode: str
    due_series_states: Callable[[datetime], List[SeriesState]]
    next_step_time: Callable[[], Optional[datetime]]
    step_series_state: Callable[[SeriesState], None]
    append_live_candles_if_needed: Callable[[], bool]
    append_live_candles_for_state: Callable[[SeriesState], bool]
    pace: Callable[[float, bool], None]
    series_states: Callable[[], List[SeriesState]]
    thread_name: Callable[[SeriesState, int], str]
    log_debug: Callable[[str, Optional[SeriesState], Optional[dict]], None]
    log_info: Callable[[str, Optional[SeriesState], Optional[dict]], None]
    log_error: Callable[[str, Optional[SeriesState], Optional[dict]], None] = field(
        default=lambda *_args, **_kwargs: None
    )
    degrade_series_on_error: bool = False
    live_idle_interval_seconds: Callable[[], float] = field(default=lambda: 0.5)


class SeriesRunner(Protocol):
    """Runner contract for executing series."""

    def run(self) -> None:
        ...

    def stop(self) -> None:
        ...


class InlineSeriesRunner:
    """Single-threaded runner that steps series sequentially."""

    def __init__(self, ctx: SeriesRunnerContext) -> None:
        self._ctx = ctx

    def run(self) -> None:
        stop_event = self._ctx.stop_event
        pause_event = self._ctx.pause_event
        while not stop_event.is_set():
            if not pause_event.wait(timeout=0.2):
                continue
            now = datetime.now(timezone.utc)
            due_states = self._ctx.due_series_states(now)
            if not due_states:
                if self._ctx.live_mode and self._ctx.append_live_candles_if_needed():
                    continue
                next_at = self._ctx.next_step_time()
                if next_at:
                    interval = max((next_at - now).total_seconds(), 0)
                    self._ctx.pace(interval, True)
                    continue
                if self._ctx.live_mode:
                    self._ctx.pace(self._ctx.live_idle_interval_seconds(), True)
                    continue
                break
            for state in due_states:
                if not _safe_step(self._ctx, state):
                    stop_event.set()
                    break

    def stop(self) -> None:
        return


class PoolSeriesRunner:
    """Runner that uses a fixed-size worker pool to step due series states."""

    def __init__(self, ctx: SeriesRunnerContext, *, max_workers: int) -> None:
        self._ctx = ctx
        self._max_workers = max(max_workers, 1)

    def run(self) -> None:
        stop_event = self._ctx.stop_event
        pause_event = self._ctx.pause_event
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            while not stop_event.is_set():
                if not pause_event.wait(timeout=0.2):
                    continue
                now = datetime.now(timezone.utc)
                due_states = self._ctx.due_series_states(now)
                if not due_states:
                    if self._ctx.live_mode and self._ctx.append_live_candles_if_needed():
                        continue
                    next_at = self._ctx.next_step_time()
                    if next_at:
                        interval = max((next_at - now).total_seconds(), 0)
                        self._ctx.pace(interval, True)
                        continue
                    if self._ctx.live_mode:
                        self._ctx.pace(self._ctx.live_idle_interval_seconds(), True)
                        continue
                    break
                futures = [executor.submit(_safe_step, self._ctx, state) for state in due_states]
                for future, state in zip(futures, due_states):
                    if not future.result():
                        stop_event.set()
                        for pending in futures:
                            pending.cancel()
                        break

    def stop(self) -> None:
        return


def _safe_step(ctx: SeriesRunnerContext, state: SeriesState) -> bool:
    try:
        ctx.step_series_state(state)
    except Exception as exc:
        if ctx.degrade_series_on_error:
            state.done = True
            state.next_step_at = None
            ctx.log_error(
                "series_step_degraded",
                state,
                {"error": str(exc), "exception": repr(exc)},
            )
            return True
        ctx.log_error(
            "series_step_failed",
            state,
            {"error": str(exc), "exception": repr(exc)},
        )
        return False
    return True
