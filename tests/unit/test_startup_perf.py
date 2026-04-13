from __future__ import annotations
import time
from claude_orchestrator.startup import parallel_init, StartupProfile


def test_parallel_init_runs_all_tasks():
    results = parallel_init({
        "a": lambda: 1,
        "b": lambda: 2,
        "c": lambda: 3,
    })
    assert results["a"] == 1
    assert results["b"] == 2
    assert results["c"] == 3


def test_parallel_init_handles_failure():
    results = parallel_init({
        "good": lambda: 42,
        "bad": lambda: 1 / 0,
    })
    assert results["good"] == 42
    assert results["bad"] is None


def test_parallel_init_faster_than_sequential():
    """并行应比串行快。"""
    def slow_task():
        time.sleep(0.1)
        return True

    start = time.monotonic()
    results = parallel_init({
        "a": slow_task,
        "b": slow_task,
        "c": slow_task,
    })
    elapsed = (time.monotonic() - start) * 1000

    assert all(r is True for r in results.values())
    # 3 个 100ms 任务并行应 < 300ms（留 100ms 余量）
    assert elapsed < 400, f"并行执行耗时 {elapsed:.0f}ms，应小于 400ms"


def test_startup_profile_summary():
    profile = StartupProfile()
    profile.record("test", time.monotonic())
    s = profile.summary()
    assert "stages" in s
    assert "test" in s["stages"]
