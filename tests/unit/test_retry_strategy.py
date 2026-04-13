from __future__ import annotations

from types import SimpleNamespace

from claude_orchestrator.model import TaskResult, TaskStatus
from claude_orchestrator.orchestrator import RetryStrategy


def _make_strategy() -> RetryStrategy:
    strategy = RetryStrategy.__new__(RetryStrategy)
    strategy._model_overload_counts = {}
    strategy._model_overload_threshold = 2
    strategy._dag = SimpleNamespace(hooks=None)
    strategy._execute_hook = lambda *args, **kwargs: None
    return strategy


def test_update_model_pressure_tracks_and_resets_overload_counts():
    strategy = _make_strategy()

    strategy._update_model_pressure("sonnet", "Error: 529 overloaded")
    strategy._update_model_pressure("sonnet", "server overloaded")

    assert strategy._model_overload_counts["sonnet"] == 2

    strategy._update_model_pressure("sonnet", "different failure")

    assert strategy._model_overload_counts["sonnet"] == 0


def test_abort_decision_returns_abort_retry_decision():
    strategy = _make_strategy()
    task_node = SimpleNamespace(id="task-1")
    result = TaskResult(task_id="task-1", status=TaskStatus.FAILED, error="boom")

    decision = strategy._abort_decision(
        task_id_display="task-1",
        task_node=task_node,
        result=result,
        log_message="Task '%s' aborted: %s",
        log_args=("boom",),
    )

    assert decision.action == "abort"
    assert decision.result is result
