"""Minimal fallback implementation of python-dotenv's :func:`load_dotenv`."""

from __future__ import annotations

import os
from pathlib import Path
import logging
from typing import Iterable

logger = logging.getLogger(__name__)
_WARNED_DEFAULTS: set[str] = set()


def _iter_lines(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            yield line.strip()


def load_dotenv(path: str | os.PathLike[str] | None = None) -> bool:
    """Load simple ``KEY=VALUE`` pairs from *path* into ``os.environ``.

    The implementation is intentionally lightweight and only supports the
    minimal syntax required by the test-suite. Comments and blank lines are
    ignored and existing variables are preserved.
    """

    target = Path(path) if path is not None else Path(".env")
    if not target.exists():
        return False

    loaded = False
    for line in _iter_lines(target):
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip('"').strip("'")
        value = os.path.expandvars(value)
        if key not in os.environ and key not in _WARNED_DEFAULTS:
            logger.warning("dotenv_default_applied | key=%s", key)
            _WARNED_DEFAULTS.add(key)
        os.environ.setdefault(key, value)
        loaded = True
    return loaded


__all__ = ["load_dotenv"]
