from __future__ import annotations

from types import SimpleNamespace

from master_orchestrator.model import TaskNode, TaskResult, TaskStatus
from master_orchestrator.orchestrator import Orchestrator


def test_execute_single_attempt_passes_full_config_to_agent_cli(monkeypatch, tmp_path) -> None:
    controller = Orchestrator.__new__(Orchestrator)
    full_config = SimpleNamespace(
        claude=SimpleNamespace(default_model="sonnet"),
        codex=SimpleNamespace(default_model="gpt-5.4"),
        routing=SimpleNamespace(
            default_provider="codex",
            phase_defaults={"execute": "codex"},
        ),
        limits=SimpleNamespace(),
    )
    guard = SimpleNamespace(check=lambda *_args, **_kwargs: SimpleNamespace(passed=True, violations=[]))

    controller._config = full_config
    controller._budget = None
    controller._audit_logger = None
    controller._working_dir = str(tmp_path)
    controller._input_guardrail = guard
    controller._output_guardrail = guard

    task = TaskNode(
        id="agent-task",
        prompt_template="hello",
        type="agent_cli",
        provider="codex",
        model="gpt-5.4",
        executor_config={"phase": "execute"},
    )
    ctx = SimpleNamespace(
        task_node=task,
        task_id_display=task.id,
        current_prompt="hello",
        on_progress=None,
        effective_max_attempts=1,
    )
    shutdown_manager = SimpleNamespace(register_subprocess=lambda *_args, **_kwargs: None)
    captured: dict[str, object] = {}

    class FakeExecutor:
        def execute(self, **kwargs):
            captured["config"] = kwargs["claude_config"]
            return TaskResult(task_id=kwargs["task"].id, status=TaskStatus.SUCCESS)

    monkeypatch.setattr(
        "master_orchestrator.orchestrator.PluginRegistry.get_executor",
        lambda _type: FakeExecutor(),
    )

    result = Orchestrator._execute_single_attempt(
        controller,
        ctx,
        1,
        task,
        full_config.claude,
        SimpleNamespace(),
        shutdown_manager,
    )

    assert result.status == TaskStatus.SUCCESS
    assert captured["config"] is full_config
