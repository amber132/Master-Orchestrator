from __future__ import annotations

from codex_orchestrator.codex_cli import _build_codex_exec_command
from codex_orchestrator.model import TaskNode


def test_build_codex_exec_command_omits_dangerous_bypass_in_restricted_mode() -> None:
    cmd = _build_codex_exec_command(
        task=TaskNode(id="task_1", prompt_template="prompt"),
        cli_path="codex",
        model="gpt-5.4",
        cwd=None,
        execution_security_mode="restricted",
    )

    assert "--dangerously-bypass-approvals-and-sandbox" not in cmd


def test_build_codex_exec_command_keeps_dangerous_bypass_in_trusted_local_mode() -> None:
    cmd = _build_codex_exec_command(
        task=TaskNode(id="task_1", prompt_template="prompt"),
        cli_path="codex",
        model="gpt-5.4",
        cwd=None,
        execution_security_mode="trusted_local",
    )

    assert "--dangerously-bypass-approvals-and-sandbox" in cmd
