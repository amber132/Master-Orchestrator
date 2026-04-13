"""Cross-platform advisory file lock."""

from __future__ import annotations

import os
import time
from pathlib import Path

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

try:
    import msvcrt
except ImportError:
    msvcrt = None  # type: ignore[assignment]


class FileLock:
    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._fh = None

    def acquire(self, *, timeout_seconds: float | None = None, poll_interval_seconds: float = 0.1) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fh = self._path.open("a+b")
        if fh.tell() == 0 and fh.seek(0, os.SEEK_END) == 0:
            fh.write(b"0")
            fh.flush()
        deadline = None if timeout_seconds is None else time.monotonic() + max(0.0, timeout_seconds)
        while True:
            try:
                if fcntl is not None:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                elif msvcrt is not None:
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                self._fh = fh
                return
            except (BlockingIOError, PermissionError, OSError):
                if deadline is not None and time.monotonic() >= deadline:
                    fh.close()
                    raise TimeoutError(f"timed out acquiring file lock: {self._path}")
                time.sleep(max(0.01, poll_interval_seconds))

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.release()
        return False
