"""Series execution runners for bot runtime."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
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
    log_error: Callable[[str, Optional[SeriesState], Optional[dict]], None]


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
                break
            for state in due_states:
                if not _safe_step(self._ctx, state):
                    stop_event.set()
                    break

    def stop(self) -> None:
        return


class ThreadedSeriesRunner:
    """One thread per series with independent pacing. (Deprecated: prefer PoolSeriesRunner.)"""

    def __init__(self, ctx: SeriesRunnerContext) -> None:
        self._ctx = ctx
        self._threads: List[threading.Thread] = []
        self._last_wait_log: dict[int, float] = {}
        self._last_step_log: dict[int, float] = {}

    def run(self) -> None:
        self._threads = []
        states = self._ctx.series_states()
        self._ctx.log_info("series_runner_threads_starting", None, {"series_count": len(states)})
        for idx, state in enumerate(states):
            thread = threading.Thread(
                target=self._run_series,
                name=self._ctx.thread_name(state, idx),
                args=(state,),
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()
        while any(thread.is_alive() for thread in self._threads):
            if self._ctx.stop_event.wait(timeout=0.2):
                break
        self._ctx.log_info("series_runner_threads_complete", None, {"series_count": len(states)})
        self._join_threads()

    def stop(self) -> None:
        self._join_threads()

    def _join_threads(self) -> None:
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=0.2)

    def _run_series(self, state: SeriesState) -> None:
        stop_event = self._ctx.stop_event
        pause_event = self._ctx.pause_event
        self._ctx.log_debug(
            "series_worker_start",
            state,
            {"thread": threading.current_thread().name},
        )
        while not stop_event.is_set():
            if not pause_event.wait(timeout=0.2):
                continue
            now = datetime.now(timezone.utc)
            next_at = state.next_step_at
            if next_at and now < next_at:
                delay = max((next_at - now).total_seconds(), 0)
                self._log_wait_if_needed(state, delay, next_at)
                time.sleep(min(0.25, delay))
                continue
            if state.done or state.bar_index >= state.total_bars:
                if self._ctx.live_mode and self._ctx.append_live_candles_for_state(state):
                    continue
                self._ctx.log_debug(
                    "series_worker_done",
                    state,
                    {"bar_index": state.bar_index, "total_bars": state.total_bars},
                )
                break
            if not _safe_step(self._ctx, state):
                stop_event.set()
                break
            self._log_step_heartbeat(state)

    def _log_wait_if_needed(self, state: SeriesState, delay: float, next_at: datetime) -> None:
        key = id(state)
        now = time.time()
        last = self._last_wait_log.get(key, 0.0)
        if delay < 1.0 or (now - last) < 30.0:
            return
        self._last_wait_log[key] = now
        self._ctx.log_debug(
            "series_waiting_next_step",
            state,
            {"delay_seconds": round(delay, 2), "next_step_at": next_at.isoformat()},
        )

    def _log_step_heartbeat(self, state: SeriesState) -> None:
        key = id(state)
        now = time.time()
        last = self._last_step_log.get(key, 0.0)
        if (now - last) < 30.0:
            return
        self._last_step_log[key] = now
        self._ctx.log_debug(
            "series_step_heartbeat",
            state,
            {"bar_index": state.bar_index, "total_bars": state.total_bars},
        )


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
        ctx.log_error(
            "series_step_failed",
            state,
            {"error": str(exc), "exception": repr(exc)},
        )
        return False
    return True
