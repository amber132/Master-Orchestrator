"""Cross-process execution leases for simple mode."""

from __future__ import annotations

import os
import socket
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .file_lock import FileLock

_LEASE_BUSY_TIMEOUT_MS = 30_000
_LEASE_LOCK_RETRIES = 8
_LEASE_LOCK_BACKOFF_BASE_SECONDS = 0.05


@dataclass
class ExecutionLease:
    lease_id: str
    owner_id: str
    run_id: str
    item_id: str
    expires_at: datetime


class SimpleExecutionLeaseManager:
    def __init__(
        self,
        db_path: str | Path,
        *,
        max_leases: int,
        ttl_seconds: int = 3600,
        owner_id: str | None = None,
    ):
        self._db_path = Path(db_path).resolve()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._max_leases = max_leases
        self._ttl_seconds = max(30, ttl_seconds)
        self._owner_id = owner_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False, timeout=30)
        init_lock_path = Path(f"{self._db_path}.init.lock")
        with FileLock(init_lock_path):
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute(f"PRAGMA busy_timeout={_LEASE_BUSY_TIMEOUT_MS}")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS simple_execution_leases (
                    lease_id    TEXT PRIMARY KEY,
                    owner_id    TEXT NOT NULL,
                    run_id      TEXT NOT NULL,
                    item_id     TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    expires_at  TEXT NOT NULL
                )
                """
            )
            self._conn.commit()
        self._cleanup_stale_leases()

    @property
    def enabled(self) -> bool:
        return self._max_leases > 0

    @property
    def owner_id(self) -> str:
        return self._owner_id

    def acquire(self, run_id: str, item_id: str, *, timeout_seconds: float = 0.0, poll_interval: float = 0.2) -> ExecutionLease | None:
        if not self.enabled:
            return None
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while True:
            lease = self._try_acquire(run_id, item_id)
            if lease is not None:
                return lease
            if timeout_seconds <= 0.0 or time.monotonic() >= deadline:
                return None
            time.sleep(poll_interval)

    def _try_acquire(self, run_id: str, item_id: str) -> ExecutionLease | None:
        now = datetime.now()
        expires_at = now + timedelta(seconds=self._ttl_seconds)
        with self._lock:
            for attempt in range(_LEASE_LOCK_RETRIES):
                try:
                    self._conn.execute("BEGIN IMMEDIATE")
                    self._prune_stale_leases_locked(now)
                    active_count = self._conn.execute(
                        "SELECT COUNT(*) FROM simple_execution_leases"
                    ).fetchone()[0]
                    if active_count >= self._max_leases:
                        self._conn.rollback()
                        return None
                    lease_id = uuid.uuid4().hex
                    self._conn.execute(
                        "INSERT INTO simple_execution_leases "
                        "(lease_id, owner_id, run_id, item_id, acquired_at, heartbeat_at, expires_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            lease_id,
                            self._owner_id,
                            run_id,
                            item_id,
                            now.isoformat(),
                            now.isoformat(),
                            expires_at.isoformat(),
                        ),
                    )
                    self._conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    try:
                        self._conn.rollback()
                    except sqlite3.Error:
                        pass
                    if "database is locked" not in str(e).lower() or attempt >= _LEASE_LOCK_RETRIES - 1:
                        raise
                    time.sleep(_LEASE_LOCK_BACKOFF_BASE_SECONDS * (2 ** attempt))
                except Exception:
                    self._conn.rollback()
                    raise
        return ExecutionLease(
            lease_id=lease_id,
            owner_id=self._owner_id,
            run_id=run_id,
            item_id=item_id,
            expires_at=expires_at,
        )

    def renew(self, lease: ExecutionLease) -> ExecutionLease | None:
        if not self.enabled:
            return lease
        now = datetime.now()
        expires_at = now + timedelta(seconds=self._ttl_seconds)
        with self._lock:
            for attempt in range(_LEASE_LOCK_RETRIES):
                try:
                    cur = self._conn.execute(
                        "UPDATE simple_execution_leases "
                        "SET heartbeat_at = ?, expires_at = ? WHERE lease_id = ? AND owner_id = ?",
                        (now.isoformat(), expires_at.isoformat(), lease.lease_id, self._owner_id),
                    )
                    self._conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    try:
                        self._conn.rollback()
                    except sqlite3.Error:
                        pass
                    if "database is locked" not in str(e).lower() or attempt >= _LEASE_LOCK_RETRIES - 1:
                        raise
                    time.sleep(_LEASE_LOCK_BACKOFF_BASE_SECONDS * (2 ** attempt))
        if cur.rowcount <= 0:
            return None
        lease.expires_at = expires_at
        return lease

    def release(self, lease: ExecutionLease | None) -> None:
        if not self.enabled or lease is None:
            return
        with self._lock:
            for attempt in range(_LEASE_LOCK_RETRIES):
                try:
                    self._conn.execute(
                        "DELETE FROM simple_execution_leases WHERE lease_id = ? AND owner_id = ?",
                        (lease.lease_id, self._owner_id),
                    )
                    self._conn.commit()
                    return
                except sqlite3.OperationalError as e:
                    try:
                        self._conn.rollback()
                    except sqlite3.Error:
                        pass
                    if "database is locked" not in str(e).lower() or attempt >= _LEASE_LOCK_RETRIES - 1:
                        raise
                    time.sleep(_LEASE_LOCK_BACKOFF_BASE_SECONDS * (2 ** attempt))

    def active_count(self) -> int:
        if not self.enabled:
            return 0
        now = datetime.now()
        with self._lock:
            for attempt in range(_LEASE_LOCK_RETRIES):
                try:
                    self._prune_stale_leases_locked(now)
                    count = self._conn.execute(
                        "SELECT COUNT(*) FROM simple_execution_leases"
                    ).fetchone()[0]
                    self._conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    try:
                        self._conn.rollback()
                    except sqlite3.Error:
                        pass
                    if "database is locked" not in str(e).lower() or attempt >= _LEASE_LOCK_RETRIES - 1:
                        raise
                    time.sleep(_LEASE_LOCK_BACKOFF_BASE_SECONDS * (2 ** attempt))
        return count

    def _cleanup_stale_leases(self) -> None:
        if not self.enabled:
            return
        now = datetime.now()
        with self._lock:
            for attempt in range(_LEASE_LOCK_RETRIES):
                try:
                    self._conn.execute("BEGIN IMMEDIATE")
                    self._prune_stale_leases_locked(now)
                    self._conn.commit()
                    return
                except sqlite3.OperationalError as e:
                    try:
                        self._conn.rollback()
                    except sqlite3.Error:
                        pass
                    if "database is locked" not in str(e).lower() or attempt >= _LEASE_LOCK_RETRIES - 1:
                        raise
                    time.sleep(_LEASE_LOCK_BACKOFF_BASE_SECONDS * (2 ** attempt))
                except Exception:
                    self._conn.rollback()
                    raise

    def _prune_stale_leases_locked(self, now: datetime) -> None:
        self._conn.execute(
            "DELETE FROM simple_execution_leases WHERE expires_at <= ?",
            (now.isoformat(),),
        )
        stale_lease_ids: list[str] = []
        for lease_id, owner_id in self._conn.execute(
            "SELECT lease_id, owner_id FROM simple_execution_leases"
        ).fetchall():
            if self._owner_is_dead(owner_id):
                stale_lease_ids.append(lease_id)
        for lease_id in stale_lease_ids:
            self._conn.execute(
                "DELETE FROM simple_execution_leases WHERE lease_id = ?",
                (lease_id,),
            )

    def _owner_is_dead(self, owner_id: str) -> bool:
        parts = owner_id.split(":", 2)
        if len(parts) < 3:
            return False
        hostname, pid_text, _suffix = parts
        if hostname != socket.gethostname():
            return False
        try:
            pid = int(pid_text)
        except ValueError:
            return False
        if pid <= 0 or pid == os.getpid():
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        except OSError:
            return False
        return False

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "SimpleExecutionLeaseManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
