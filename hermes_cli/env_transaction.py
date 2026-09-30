"""Cooperative dotenv transaction protocol v1 (POSIX local filesystems).

``with env_transaction(target, timeout=10) as canonical:`` must enclose the
FIRST read/existence check, mutation computation, atomic replacement and memory
publication. Never bring a candidate/snapshot read before acquisition into it.
The persistent lock is canonical.parent / (canonical.name + '.lock'), i.e.
'.env.lock'. Resolve symlink aliases BEFORE choosing the lock AND writing; never
lock the replaceable dotenv inode, unlink a lock, or steal a timed-out lock.

Independently packaged writers must import this module from a revision-pinned
Hermes installation (stdlib only), or project these exact source bytes. Missing
module/platform primitives are an error, never an unlocked fallback. Parsers and
atomic writers remain caller-owned. All participating writers must use this
protocol; it cannot exclude editors, shell redirections, backup restores or
other uncooperative writers. Admission must exclude those known owners first.

Parent directory must be owned by the effective uid and not group/world writable;
its ancestors and symlinks must be trusted/stable during the transaction. Target
hardlinks and nonregular files are refused. Network filesystems and concurrently
renamed directories are outside this contract. Thread reentry is supported;
separate threads/processes open distinct descriptions. Forked children must start
a NEW transaction, not continue the parent's context. This is mutual exclusion,
not rollback: an exception after a caller's commit does not undo that commit.
"""
from contextlib import contextmanager
import errno
import math
import os
from pathlib import Path
import stat
import threading
import time

try:
    import fcntl
except ImportError:
    fcntl = None

_local = threading.local()
_fds = set()


def _after_fork():
    global _local, _fds
    for fd in _fds:
        os.close(fd)  # close the inherited description, do NOT unlock the parent
    _fds = set()
    _local = threading.local()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_after_fork)


def _validate_target(target):
    parent = target.parent.stat()
    if parent.st_uid != os.geteuid() or parent.st_mode & 0o022:
        raise PermissionError('Dotenv transaction requires an owner-controlled directory')
    try:
        info = target.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError('Dotenv transaction requires a regular, non-hardlinked target')
    if info.st_uid != os.geteuid():
        raise PermissionError('Dotenv target must belong to the effective user')


def require_env_transaction(target):
    """Reject an atomic writer invoked without a covering read/modify lock."""
    canonical = Path(target).resolve()
    if canonical not in getattr(_local, 'held', {}):
        raise RuntimeError('Dotenv write requires env_transaction before reading')
    return canonical


@contextmanager
def env_transaction(target, *, timeout=10.0):
    """Yield a canonical Path under bounded, reentrant process exclusion.

    TimeoutError leaves the target untouched; permission/type/OS errors propagate.
    Acquisition never truncates the persistent owner-only lock file.
    """
    if fcntl is None or not hasattr(os, 'O_NOFOLLOW'):
        raise RuntimeError('Dotenv transactions require POSIX flock and O_NOFOLLOW')
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError('timeout must be finite and nonnegative')
    canonical = Path(target).resolve()
    canonical.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _validate_target(canonical)
    held = getattr(_local, 'held', None)
    if held is None:
        held = _local.held = {}
    if canonical in held:
        yield canonical
        return
    lock = canonical.with_name(canonical.name + '.lock')
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, 0o600)
    _fds.add(fd)
    pid = os.getpid()
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
            raise PermissionError('Unsafe dotenv lock: require regular owner-only 0600 file')
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EACCES):
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('Timed out waiting for dotenv transaction') from exc
                time.sleep(min(0.05, remaining))
        current = lock.lstat()
        if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
            raise PermissionError('Dotenv lock identity changed during acquisition')
        _validate_target(canonical)
        held[canonical] = fd
        try:
            yield canonical
        finally:
            held.pop(canonical, None)
    finally:
        if os.getpid() == pid:
            _fds.discard(fd)
            os.close(fd)  # last close releases flock, including on exceptions
