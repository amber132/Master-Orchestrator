from __future__ import annotations

import multiprocessing
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from claude_orchestrator.store import Store


def _open_store_process(db_path: str) -> int:
    with Store(db_path):
        return 0


def test_store_init_is_safe_under_concurrent_first_open(tmp_path) -> None:
    db_path = tmp_path / "shared.db"
    barrier = Barrier(6)

    def open_store() -> None:
        barrier.wait(timeout=5)
        with Store(db_path):
            pass

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(open_store) for _ in range(6)]
        for future in futures:
            future.result(timeout=10)

    with Store(db_path) as store:
        assert store._get_current_version() >= 1


def test_store_init_is_safe_under_concurrent_first_open_across_processes(tmp_path) -> None:
    db_path = tmp_path / "shared-process.db"
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(processes=4) as pool:
        results = pool.map(_open_store_process, [str(db_path)] * 4)

    assert results == [0, 0, 0, 0]
    with Store(db_path) as store:
        assert store._get_current_version() >= 1


def test_store_execute_write_retries_locked_database(tmp_path, monkeypatch) -> None:
    store = Store(tmp_path / "shared.db")

    class DummyCursor:
        def __init__(self) -> None:
            self.rowcount = 1

    class FakeConn:
        def __init__(self) -> None:
            self.calls = 0
            self.rollbacks = 0
            self.commits = 0

        def execute(self, sql, params=()):
            self.calls += 1
            if self.calls == 1:
                raise sqlite3.OperationalError("database is locked")
            return DummyCursor()

        def commit(self) -> None:
            self.commits += 1

        def rollback(self) -> None:
            self.rollbacks += 1

    fake_conn = FakeConn()
    monkeypatch.setattr(store, "_conn", fake_conn)
    monkeypatch.setattr(store, "_maybe_checkpoint", lambda: None)

    cur = store._execute_write("INSERT INTO x VALUES (?)", ("v",), operation="test_write")

    assert isinstance(cur, DummyCursor)
    assert fake_conn.calls == 2
    assert fake_conn.rollbacks == 1
    assert fake_conn.commits == 1
