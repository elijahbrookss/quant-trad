"""Keep already verified recovery material under one live receiver's custody."""

from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat

from market_data.fact_archive import archive_evidence_hash
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root, _stamp


_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_ARCHIVE = "archive"
_PGDATA = "pgdata"
_CHECK_INTERVAL = 128


def _metadata(info):
    return (*_stamp(info), info.st_mode, info.st_uid, info.st_gid)


@dataclass(frozen=True)
class _Root:
    role: str
    path: Path
    identity: tuple[int, int]
    fd: int

    def assert_present(self):
        if _identity(self.path.lstat()) != self.identity or self.path.resolve(strict=True) != self.path:
            raise RuntimeError("canonical_prepared_custody_root_changed")


def _digest_tree(root, *, spaces, take):
    digest = hashlib.sha256()
    linked = set()

    def record(parts, kind, info, target=None):
        row = (parts, kind, _metadata(info), target)
        digest.update((json.dumps(row, separators=(",", ":")) + "\n").encode("ascii"))

    def visit(fd, parts, expected):
        before = os.fstat(fd)
        if _metadata(before) != _metadata(expected) or before.st_dev != root.identity[0]:
            raise RuntimeError("canonical_prepared_custody_directory_changed")
        record(parts, "directory", before)
        with os.scandir(fd) as scan:
            names = []
            for entry in scan:
                take()
                names.append(entry.name)
        for name in sorted(names):
            take(check_only=True)
            child = (*parts, name)
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if info.st_dev != root.identity[0] or (root.role == _PGDATA and child == ("postmaster.pid",)):
                raise RuntimeError("canonical_prepared_custody_unsafe_member")
            if root.role == _PGDATA and parts == ("pg_tblspc",):
                if not stat.S_ISLNK(info.st_mode) or not name.isascii() or not name.isdecimal() or not 0 < int(name) < 2**32:
                    raise RuntimeError("canonical_prepared_custody_tablespace_link_invalid")
                link = os.readlink(name, dir_fd=fd)
                target = spaces.get(link)
                if (target is None or link in linked
                        or _identity(os.stat(name, dir_fd=fd)) != target.identity
                        or _metadata(os.stat(name, dir_fd=fd, follow_symlinks=False)) != _metadata(info)):
                    raise RuntimeError("canonical_prepared_custody_tablespace_link_changed")
                linked.add(link)
                record(child, "tablespace", info, link)
            elif stat.S_ISDIR(info.st_mode):
                child_fd = os.open(name, _DIR_FLAGS, dir_fd=fd)
                try:
                    visit(child_fd, child, info)
                finally:
                    os.close(child_fd)
            elif (stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                  and (root.role != _PGDATA or child != ("pg_tblspc",))):
                record(child, "file", info)
            else:
                raise RuntimeError("canonical_prepared_custody_unsafe_member")
        if _metadata(os.fstat(fd)) != _metadata(before):
            raise RuntimeError("canonical_prepared_custody_directory_changed")

    root.assert_present()
    take()
    visit(root.fd, (), os.fstat(root.fd))
    root.assert_present()
    if root.role == _PGDATA and linked != spaces.keys():
        raise RuntimeError("canonical_prepared_custody_tablespace_links_incomplete")
    return digest.hexdigest()


def _inventory(roots, max_entries, check):
    entries = visited = 0

    def take(*, check_only=False):
        nonlocal entries, visited
        visited += 1
        if visited % _CHECK_INTERVAL == 0:
            check()
        if not check_only:
            entries += 1
            if entries > max_entries:
                raise RuntimeError("canonical_prepared_custody_entry_budget_exceeded")

    spaces = {str(root.path): root for root in roots if root.role not in (_PGDATA, _ARCHIVE)}
    trees = {}
    try:
        for root in roots:
            check()
            trees[root.role] = {"path": str(root.path), "device": root.identity[0], "inode": root.identity[1],
                                "sha256": _digest_tree(root, spaces=spaces, take=take)}
        check()
    except OSError as error:
        raise RuntimeError("canonical_prepared_custody_tree_unavailable") from error
    return trees, entries


class PreparedCustody:
    def __init__(self, roots, max_entries, check):
        self._roots = roots
        self._max_entries = max_entries
        self._trees, self.entries = _inventory(roots, max_entries, check)
        self.sha256 = archive_evidence_hash({"roots": self._trees, "entries": self.entries})
        self._active = True

    def assert_held(self, check):
        """Verify the stopped PostgreSQL target, tablespaces and restored archives."""
        self._assert_unchanged(self._roots, check)

    def assert_archive_held(self, check):
        """Verify archive custody after PostgreSQL starts writing its own files."""
        self._assert_unchanged(tuple(root for root in self._roots if root.role == _ARCHIVE), check)

    def _assert_unchanged(self, roots, check):
        if not self._active:
            raise RuntimeError("canonical_prepared_custody_closed")
        actual, _ = _inventory(roots, self._max_entries, check)
        if any(actual[root.role] != self._trees[root.role] for root in roots):
            raise RuntimeError("canonical_prepared_custody_tree_changed")


@contextmanager
def capture_prepared_custody(*, pgdata, tablespaces, archive_root, max_entries, check):
    """Pin exact metadata for private roots previously materialized by the receiver.

    Keep this context alive through the named restore. The locks coordinate
    receivers using these roots. Native verification establishes content
    integrity before custody; these rechecks detect filesystem changes
    between that verification and use without rereading the prepared bytes.
    """
    if (type(max_entries) is not int or max_entries < 1 or not callable(check) or not isinstance(tablespaces, dict)
            or any(not isinstance(name, str) or not name for name in tablespaces)):
        raise ValueError("canonical_prepared_custody_invalid_scope")
    paths = {_PGDATA: pgdata, **{f"tablespace:{name}": path for name, path in tablespaces.items()}, _ARCHIVE: archive_root}
    observed = [(role, *_private_root(path)) for role, path in paths.items()]
    if (len({identity for _, _, identity in observed}) != len(observed)
            or any(left.is_relative_to(right) or right.is_relative_to(left)
                   for index, (_, left, _) in enumerate(observed) for _, right, _ in observed[index + 1:])):
        raise ValueError("canonical_prepared_custody_overlapping_roots")

    with ExitStack() as stack:
        roots = []
        for role, path, identity in observed:
            check()
            fd = os.open(path, _DIR_FLAGS)
            stack.callback(os.close, fd)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("canonical_prepared_custody_busy") from None
            info = os.fstat(fd)
            if _identity(info) != identity or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise RuntimeError("canonical_prepared_custody_root_changed")
            roots.append(_Root(role, path, identity, fd))
        custody = PreparedCustody(tuple(roots), max_entries, check)
        try:
            yield custody
        finally:
            custody._active = False
