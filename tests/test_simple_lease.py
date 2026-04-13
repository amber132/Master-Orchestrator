from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from claude_orchestrator.simple_lease import SimpleExecutionLeaseManager


def test_simple_execution_leases_respect_global_limit(tmp_path: Path) -> None:
    lease_db = tmp_path / "leases.sqlite3"
    manager_a = SimpleExecutionLeaseManager(lease_db, max_leases=1, ttl_seconds=60, owner_id="a")
    manager_b = SimpleExecutionLeaseManager(lease_db, max_leases=1, ttl_seconds=60, owner_id="b")

    lease_a = manager_a.acquire("run-a", "item-a")
    assert lease_a is not None
    assert manager_a.active_count() == 1

    lease_b = manager_b.acquire("run-b", "item-b")
    assert lease_b is None

    manager_a.release(lease_a)
    lease_b = manager_b.acquire("run-b", "item-b")
    assert lease_b is not None
    assert manager_b.active_count() == 1

    manager_b.release(lease_b)
    manager_a.close()
    manager_b.close()


def test_simple_execution_leases_prune_dead_local_owners(tmp_path: Path, monkeypatch) -> None:
    lease_db = tmp_path / "leases.sqlite3"
    conn = sqlite3.connect(lease_db)
    conn.execute(
        """
        CREATE TABLE simple_execution_leases (
            lease_id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            acquired_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
        """
    )
    now = datetime.now()
    conn.execute(
        """
        INSERT INTO simple_execution_leases (
            lease_id, owner_id, run_id, item_id, acquired_at, heartbeat_at, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "stale-lease",
            "test-host:999999:dead",
            "run-stale",
            "item-stale",
            now.isoformat(),
            now.isoformat(),
            (now + timedelta(hours=1)).isoformat(),
        ),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr("claude_orchestrator.simple_lease.socket.gethostname", lambda: "test-host")
    monkeypatch.setattr("claude_orchestrator.simple_lease.os.getpid", lambda: 12345)

    def fake_kill(pid: int, sig: int) -> None:
        if pid == 999999:
            raise ProcessLookupError

    monkeypatch.setattr("claude_orchestrator.simple_lease.os.kill", fake_kill)

    manager = SimpleExecutionLeaseManager(lease_db, max_leases=1, ttl_seconds=60, owner_id="test-host:12345:live")
    lease = manager.acquire("run-live", "item-live")

    assert lease is not None
    assert manager.active_count() == 1

    manager.release(lease)
    manager.close()


def test_simple_execution_leases_retry_locked_begin_immediate(tmp_path: Path, monkeypatch) -> None:
    lease_db = tmp_path / "leases.sqlite3"
    manager = SimpleExecutionLeaseManager(lease_db, max_leases=1, ttl_seconds=60, owner_id="owner-a")

    class DummyCursor:
        def __init__(self, *, fetchone_value=None) -> None:
            self._fetchone_value = fetchone_value
            self.rowcount = 1

        def fetchone(self):
            return self._fetchone_value

    class FakeConn:
        def __init__(self) -> None:
            self.begin_calls = 0
            self.rollbacks = 0
            self.commits = 0

        def execute(self, sql, params=()):
            if sql == "BEGIN IMMEDIATE":
                self.begin_calls += 1
                if self.begin_calls == 1:
                    raise sqlite3.OperationalError("database is locked")
                return DummyCursor()
            if sql.startswith("SELECT COUNT(*) FROM simple_execution_leases"):
                return DummyCursor(fetchone_value=(0,))
            if sql.startswith("INSERT INTO simple_execution_leases"):
                return DummyCursor()
            return DummyCursor(fetchone_value=(0,))

        def commit(self) -> None:
            self.commits += 1

        def rollback(self) -> None:
            self.rollbacks += 1

    fake_conn = FakeConn()
    monkeypatch.setattr(manager, "_conn", fake_conn)
    monkeypatch.setattr(manager, "_prune_stale_leases_locked", lambda now: None)

    lease = manager._try_acquire("run-a", "item-a")

    assert lease is not None
    assert fake_conn.begin_calls == 2
    assert fake_conn.rollbacks == 1
    assert fake_conn.commits == 1
