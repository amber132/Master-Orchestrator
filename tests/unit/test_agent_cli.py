from __future__ import annotations

from types import SimpleNamespace

from master_orchestrator.agent_cli import run_agent_task
from master_orchestrator.model import TaskNode, TaskResult, TaskStatus


def _config() -> object:
    return SimpleNamespace(
        claude=SimpleNamespace(default_model="sonnet", cli_path="claude"),
        codex=SimpleNamespace(default_model="gpt-5.4-pro", cli_path="codex"),
        routing=SimpleNamespace(
            default_provider="auto",
            auto_fallback=True,
            phase_defaults={
                "decompose": "claude",
                "review": "claude",
                "discover": "claude",
                "execute": "codex",
                "simple": "codex",
                "self_improve": "claude",
                "requirement": "claude",
            },
        ),
    )


def test_run_agent_task_routes_execute_phase_to_codex(monkeypatch) -> None:
    calls: list[str] = []

    def fake_claude(**kwargs):
        calls.append("claude")
        return TaskResult(task_id=kwargs["task"].id, status=TaskStatus.SUCCESS)

    def fake_codex(**kwargs):
        calls.append("codex")
        return TaskResult(task_id=kwargs["task"].id, status=TaskStatus.SUCCESS)

    monkeypatch.setattr("master_orchestrator.agent_cli.run_claude_task", fake_claude)
    monkeypatch.setattr("master_orchestrator.agent_cli.run_codex_task", fake_codex)

    result = run_agent_task(
        task=TaskNode(
            id="t1",
            prompt_template="x",
            executor_config={"phase": "execute"},
        ),
        prompt="hello",
        config=_config(),
        limits=SimpleNamespace(),
        budget_tracker=None,
        working_dir=None,
        on_progress=None,
    )

    assert calls == ["codex"]
    assert result.provider_used == "codex"


def test_run_agent_task_falls_back_only_for_auto_provider(monkeypatch) -> None:
    calls: list[str] = []

    def fake_claude(**kwargs):
        calls.append("claude")
        return TaskResult(task_id=kwargs["task"].id, status=TaskStatus.SUCCESS)

    def fake_codex(**kwargs):
        calls.append("codex")
        return TaskResult(task_id=kwargs["task"].id, status=TaskStatus.FAILED, error="boom")

    monkeypatch.setattr("master_orchestrator.agent_cli.run_claude_task", fake_claude)
    monkeypatch.setattr("master_orchestrator.agent_cli.run_codex_task", fake_codex)

    result = run_agent_task(
        task=TaskNode(
            id="t1",
            prompt_template="x",
            provider="auto",
            executor_config={"phase": "execute"},
        ),
        prompt="hello",
        config=_config(),
        limits=SimpleNamespace(),
        budget_tracker=None,
        working_dir=None,
        on_progress=None,
    )

    assert calls == ["codex", "claude"]
    assert result.provider_used == "claude"
