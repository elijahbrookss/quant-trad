"""Startup context for a bot runtime before live execution begins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class StartContext:
    """Explicit worker startup identity shared across warm-up and live runtime."""

    bot_id: str
    run_id: str
    worker_id: Optional[str] = None


__all__ = ["StartContext"]
