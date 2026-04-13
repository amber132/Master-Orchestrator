from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import claude_orchestrator.orchestrator as orchestrator_module
from claude_orchestrator.model import TaskResult, TaskStatus
from claude_orchestrator.orchestrator import Orchestrator


class _Recorder:
    def __init__(self):
        self.items = []

    def record(self, item):
        self.items.append(item)


class _Diagnostics:
    def __init__(self):
        self.calls = []

    def record_task_lifecycle(self, **kwargs):
        self.calls.append(kwargs)


class _OutputCache:
    def __init__(self):
        self.calls = []

    def put(self, *args):
        self.calls.append(args)


class _TaskCache:
    def __init__(self):
        self.calls = []

    def put(self, *args):
        self.calls.append(args)


def _make_orchestrator() -> Orchestrator:
    orch = Orchestrator.__new__(Orchestrator)
    orch._metrics_collector = _Recorder()
    orch._diagnostics = _Diagnostics()
    orch._output_cache = _OutputCache()
    orch._task_cache = _TaskCache()
    orch._run_info = SimpleNamespace(run_id="run-1")
    return orch


def test_record_task_metric_writes_expected_metric():
    orch = _make_orchestrator()
    start = datetime(2026, 4, 10, 12, 0, 0)
    end = start + timedelta(seconds=2)
    result = TaskResult(
        task_id="task-1",
        status=TaskStatus.SUCCESS,
        duration_seconds=1.5,
        token_input=12,
        token_output=4,
    )

    orch._record_task_metric(
        task_id="task-1",
        task_start_time=start,
        end_time=end,
        result=result,
        retry_count=1,
        status="success",
    )

    assert len(orch._metrics_collector.items) == 1
    metric = orch._metrics_collector.items[0]
    assert metric.task_id == "task-1"
    assert metric.retry_count == 1
    assert metric.status == "success"
    assert metric.token_input == 12
    assert metric.token_output == 4
    assert metric.duration_ms == 2000.0


def test_record_task_lifecycle_diagnostic_writes_expected_payload(monkeypatch):
    orch = _make_orchestrator()
    start = datetime.now() - timedelta(seconds=1)
    result = TaskResult(task_id="task-1", status=TaskStatus.FAILED, cost_usd=0.3, error="boom")
    monkeypatch.setattr(
        orchestrator_module,
        "DiagnosticEventType",
        SimpleNamespace(TASK_COMPLETE="complete", TASK_FAIL="fail"),
    )

    orch._record_task_lifecycle_diagnostic(
        event_type="fail",
        task_id="task-1",
        model="sonnet",
        attempt=2,
        task_start_time=start,
        result=result,
        include_error=True,
    )

    assert len(orch._diagnostics.calls) == 1
    payload = orch._diagnostics.calls[0]
    assert payload["run_id"] == "run-1"
    assert payload["task_id"] == "task-1"
    assert payload["model"] == "sonnet"
    assert payload["attempt"] == 2
    assert payload["cost_usd"] == 0.3
    assert payload["error"] == "boom"


def test_cache_successful_result_updates_output_and_task_cache():
    orch = _make_orchestrator()
    task_node = SimpleNamespace(id="task-1", idempotent=True)
    result = TaskResult(
        task_id="task-1",
        status=TaskStatus.SUCCESS,
        parsed_output={"ok": True},
        cost_usd=0.25,
    )

    orch._cache_successful_result(
        task_node=task_node,
        cache_key="cache-key",
        prompt="prompt",
        model="sonnet",
        result=result,
    )

    assert orch._output_cache.calls == [("cache-key", '{"ok": true}', 0.25)]
    assert orch._task_cache.calls == [("task-1", "prompt", "sonnet", result)]
