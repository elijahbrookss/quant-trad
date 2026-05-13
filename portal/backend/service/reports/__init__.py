"""Report service package."""

from __future__ import annotations

from typing import Any

__all__ = ["build_run_research_dataset"]


def __getattr__(name: str) -> Any:
    if name == "build_run_research_dataset":
        from . import run_research_dataset

        return getattr(run_research_dataset, name)
    raise AttributeError(name)
