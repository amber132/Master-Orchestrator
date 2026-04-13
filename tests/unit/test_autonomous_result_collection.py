from __future__ import annotations

from claude_orchestrator.auto_model import TaskError
from claude_orchestrator.autonomous_helpers import collect_task_outputs_and_errors
from claude_orchestrator.model import TaskResult, TaskStatus


def test_collect_task_outputs_and_errors_uses_raw_output_fallback():
    results = {
        "task-1": TaskResult(
            task_id="task-1",
            status=TaskStatus.SUCCESS,
            output="raw fallback",
            parsed_output=None,
        )
    }

    outputs, errors, empty_successes = collect_task_outputs_and_errors(results)

    assert outputs == {"task-1": "raw fallback"}
    assert errors == []
    assert empty_successes == 0


def test_collect_task_outputs_and_errors_skips_empty_success_and_builds_task_error():
    results = {
        "empty-success": TaskResult(
            task_id="empty-success",
            status=TaskStatus.SUCCESS,
            output="",
            parsed_output=None,
        ),
        "failed-task": TaskResult(
            task_id="failed-task",
            status=TaskStatus.FAILED,
            error=None,
            attempt=3,
        ),
    }

    outputs, errors, empty_successes = collect_task_outputs_and_errors(
        results,
        failed_task_default_error="修正任务失败",
    )

    assert outputs == {}
    assert empty_successes == 1
    assert errors == [TaskError(task_id="failed-task", error="修正任务失败", attempt=3)]
