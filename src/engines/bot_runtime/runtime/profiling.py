"""Opt-in standard-library profiling for targeted backtest workers."""

from __future__ import annotations

import cProfile
import pstats
import resource
import sys
import time
from typing import Any, Mapping


def _process_peak_rss_bytes() -> int | None:
    """Return the process peak RSS without enabling allocation tracing."""

    try:
        peak_rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (AttributeError, OSError, TypeError, ValueError):
        return None
    if peak_rss < 0:
        return None
    # Linux reports KiB while macOS reports bytes.
    return peak_rss if sys.platform == "darwin" else peak_rss * 1024


class PythonProfileSession:
    """Capture one bounded cProfile summary without affecting normal runs."""

    def __init__(
        self,
        *,
        enabled: bool,
        context: Mapping[str, Any] | None = None,
        work_units: int | None = None,
        top_limit: int = 20,
    ) -> None:
        self.enabled = bool(enabled)
        self.context = dict(context or {})
        self.work_units = max(int(work_units or 0), 0)
        self.top_limit = max(int(top_limit), 1)
        self.summary: dict[str, Any] | None = None
        self._profile: cProfile.Profile | None = None
        self._started_wall: float | None = None
        self._started_cpu: float | None = None
        self._started = False

    def start(self) -> None:
        if not self.enabled or self._started:
            return
        self._started = True
        self._profile = cProfile.Profile()
        self._started_wall = time.perf_counter()
        self._started_cpu = time.process_time()
        self._profile.enable()

    @staticmethod
    def _function_rows(
        stats: pstats.Stats,
        *,
        cumulative: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for (filename, line, function), values in stats.stats.items():
            primitive_calls, total_calls, self_seconds, cumulative_seconds, _callers = values
            rows.append(
                {
                    "function": f"{filename}:{line}({function})",
                    "primitive_calls": int(primitive_calls),
                    "total_calls": int(total_calls),
                    "self_seconds": round(float(self_seconds), 9),
                    "cumulative_seconds": round(float(cumulative_seconds), 9),
                }
            )
        primary = "cumulative_seconds" if cumulative else "self_seconds"
        secondary = "self_seconds" if cumulative else "cumulative_seconds"
        rows.sort(
            key=lambda row: (row[primary], row[secondary]),
            reverse=True,
        )
        return rows[:limit]

    def stop(self, *, error: str | None = None) -> dict[str, Any] | None:
        if not self.enabled or not self._started:
            return self.summary
        if self.summary is not None:
            return self.summary
        profile = self._profile
        if profile is None:
            return None
        profile.disable()
        wall_seconds = max(
            time.perf_counter() - float(self._started_wall or 0.0),
            0.0,
        )
        cpu_seconds = max(
            time.process_time() - float(self._started_cpu or 0.0),
            0.0,
        )
        peak_bytes = _process_peak_rss_bytes()
        stats = pstats.Stats(profile).strip_dirs()
        throughput = (
            float(self.work_units) / wall_seconds
            if self.work_units > 0 and wall_seconds > 0.0
            else None
        )
        self.summary = {
            "schema_version": "python_profile.v1",
            "status": "failed" if error else "completed",
            "error": str(error) if error else None,
            "scope": "series_worker_prepare_execute_flush_and_artifact",
            "wall_seconds": round(wall_seconds, 9),
            "cpu_seconds": round(cpu_seconds, 9),
            "peak_memory_bytes": int(peak_bytes) if peak_bytes is not None else None,
            "current_memory_bytes": None,
            "memory_scope": "process_peak_rss",
            "work_units": self.work_units,
            "work_units_per_second": round(throughput, 6) if throughput is not None else None,
            "primitive_call_count": int(stats.prim_calls),
            "total_call_count": int(stats.total_calls),
            "top_by_cumulative_time": self._function_rows(
                stats,
                cumulative=True,
                limit=self.top_limit,
            ),
            "top_by_self_time": self._function_rows(
                stats,
                cumulative=False,
                limit=self.top_limit,
            ),
            "context": dict(self.context),
            "caveats": [
                "profiling_is_opt_in_and_adds_measurement_overhead",
                "peak_memory_is_process_lifetime_rss_not_profile_session_allocations",
                "report_materialization_runs_in_the_backend_and_is_timed_separately",
            ],
        }
        return self.summary

    def record_failure(self, error: Exception) -> dict[str, Any] | None:
        """Preserve a bounded failure artifact without failing the trading run."""

        if not self.enabled:
            return None
        profile = self._profile
        if profile is not None:
            profile.disable()
        message = f"{type(error).__name__}: {error}"[:1000]
        self.summary = {
            "schema_version": "python_profile.v1",
            "status": "failed",
            "error": message,
            "scope": "series_worker_prepare_execute_flush_and_artifact",
            "work_units": self.work_units,
            "context": dict(self.context),
            "caveats": [
                "profiling_is_opt_in_and_adds_measurement_overhead",
                "profiling_artifact_finalization_failed",
                "trading_run_outcome_is_authoritative_over_profile_failure",
            ],
        }
        return self.summary


__all__ = ["PythonProfileSession"]
