"""Process-held archive namespace exclusion for cooperating QT object stores.

Linux directory flock follows the inode across bind-mount aliases. It does not
restrain arbitrary filesystem writers or replace host/image admission, SQL
fences or the live content leases used by a storage handoff.
"""
from contextlib import contextmanager
import os
from pathlib import Path
import stat


@contextmanager
def archive_namespace(root: Path, *, exclusive: bool = False):
    """Fail immediately on contention; retain exclusion until this context ends.

    Ordinary publication/deletion takes a shared lock, so publishers remain
    concurrent. A final migration takes exclusive ownership after its last copy
    page and retains it through outcome reconciliation. No lock file, permissions
    change, retry, polling deadline or persisted authority is introduced.
    """
    import fcntl

    root = Path(root)
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise RuntimeError("market_archive_namespace_root_invalid")
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    owner = os.getpid()
    try:
        initial = os.fstat(fd)
        identity = (initial.st_dev, initial.st_ino)

        def check():
            current = root.lstat()
            if (os.getpid() != owner or root.resolve(strict=True) != root
                    or not stat.S_ISDIR(current.st_mode)
                    or (current.st_dev, current.st_ino) != identity
                    or (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != identity):
                raise RuntimeError("market_archive_namespace_binding_changed")

        try:
            fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("market_archive_namespace_busy") from None
        check()
        yield check
        check()
    finally:
        os.close(fd)
