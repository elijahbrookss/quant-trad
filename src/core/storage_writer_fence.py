"""Process-held interlock for the fixed, pre-migration source runtime.

The operator supplies the existing source root, never a marker or new data
path. Supported source publishers retain shared directory flock ownership;
the held final transition acquires exclusive ownership. This is cooperative
image-qualified exclusion, not protection against arbitrary privileged actors.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from core.storage_mounts import configured_archive_root

logger = logging.getLogger(__name__)
_HELD: tuple[int, tuple[str, int, int]] | None = None


def retain_source_writer_fence() -> tuple[int, ...]:
    """Acquire once before publishing or spawning API/workers; retain to exit.

    The explicit operator input is deliberately limited to the old, co-located
    source layout. Ordinary startup without it is unchanged. Missing, changed
    or contended roots refuse without creating/chmod/chown or waiting. No
    persisted receipt, process exit code or flag grants exclusive ownership.
    """
    global _HELD
    value = os.environ.get("QT_STORAGE_SOURCE_FENCE_ROOT")
    if value is None:
        if _HELD is not None:
            raise RuntimeError("storage_source_fence_configuration_changed")
        return ()
    import fcntl

    root = Path(value)
    archive = configured_archive_root()
    working = Path(os.environ.get("MARKET_STRUCTURE_WORKING_ROOT") or archive)
    if (not value or not root.is_absolute() or root == Path("/")
            or root.resolve(strict=True) != root
            or archive.resolve(strict=True) != root or working.resolve(strict=True) != root):
        raise RuntimeError("storage_source_fence_root_invalid")
    info = root.stat()
    binding = (str(root), info.st_dev, info.st_ino)
    if _HELD is not None:
        descriptor, original = _HELD
        current = os.fstat(descriptor)
        if original != binding or (current.st_dev, current.st_ino) != binding[1:]:
            raise RuntimeError("storage_source_fence_binding_changed")
        return (descriptor,)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        current = os.fstat(descriptor)
        if (current.st_dev, current.st_ino) != binding[1:]:
            raise RuntimeError("storage_source_fence_binding_changed")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("storage_source_writer_excluded_by_handoff") from None
        if (root.stat().st_dev, root.stat().st_ino) != binding[1:]:
            raise RuntimeError("storage_source_fence_binding_changed")
    except BaseException:
        os.close(descriptor)
        raise
    # Do not register an unlock callback: supervised children share this open
    # file description via pass_fds and must retain ownership after parent exit.
    _HELD = (descriptor, binding)
    logger.info("storage_source_writer_fence_held | device=%s inode=%s", *binding[1:])
    return (descriptor,)


def source_writer_fds() -> tuple[int, ...]:
    """Explicit inheritance by the existing backend process supervisor."""
    return retain_source_writer_fence()
