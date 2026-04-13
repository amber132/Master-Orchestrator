from __future__ import annotations

import logging

from claude_orchestrator.auto_model import Phase
from claude_orchestrator.autonomous_helpers import log_orchestrator_result_summary
from claude_orchestrator.model import DAG, TaskResult, TaskStatus


def test_log_orchestrator_result_summary_returns_status_counts(caplog):
    phase = Phase(id="p1", name="Phase 1", description="desc", order=1)
    dag = DAG(name="test")
    dag.tasks = {"a": object(), "b": object(), "c": object()}
    results = {
        "a": TaskResult(task_id="a", status=TaskStatus.SUCCESS),
        "b": TaskResult(task_id="b", status=TaskStatus.FAILED),
    }

    with caplog.at_level(logging.INFO):
        summary = log_orchestrator_result_summary(
            phase,
            dag,
            results,
            lru_max_results=10,
        )

    assert summary == {"success": 1, "failed": 1}
    assert "status_distribution={'success': 1, 'failed': 1}" in caplog.text
    assert "结果数量(2) < DAG任务数量(3)" in caplog.text


def test_log_orchestrator_result_summary_handles_empty_results(caplog):
    phase = Phase(id="p2", name="Phase 2", description="desc", order=2)
    dag = DAG(name="test")

    with caplog.at_level(logging.INFO):
        summary = log_orchestrator_result_summary(
            phase,
            dag,
            {},
            lru_max_results=5,
        )

    assert summary == {}
    assert "total_results=0" in caplog.text
