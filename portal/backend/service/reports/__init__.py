"""Report service package."""

from __future__ import annotations

from typing import Any

__all__ = ["compare_reports", "get_report", "list_reports"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from . import report_service

        return getattr(report_service, name)
    raise AttributeError(name)
