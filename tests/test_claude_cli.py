from __future__ import annotations

import io
import time

from claude_orchestrator.claude_cli import run_claude_task
from claude_orchestrator.config import ClaudeConfig, LimitsConfig
from claude_orchestrator.model import RetryPolicy, TaskNode, TaskStatus


class _FakeStdin(io.StringIO):
    def close(self) -> None:
        # Keep buffer readable after the task writes the prompt.
        pass


class _FakeProc:
    def __init__(self, cmd, stdout_text=None):
        self.cmd = cmd
        self.pid = 43210
        self.returncode = None
        self.stdin = _FakeStdin()
        self.stdout = io.StringIO(stdout_text or (
            '{"type":"item.completed","item":{"type":"agent_message","text":"ok"}}\n'
            '{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}\n'
        ))
        self.stderr = io.StringIO("")

    def wait(self, timeout=None):
        deadline = time.time() + min(timeout or 1, 0.2)
        while self.returncode is None and time.time() < deadline:
            time.sleep(0.01)
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def poll(self):
        return self.returncode


def test_run_claude_task_enforces_max_turns_without_cli_flag(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        proc = _FakeProc(cmd)
        captured["stdin"] = proc.stdin
        return proc

    monkeypatch.setattr("claude_orchestrator.claude_cli.subprocess.Popen", fake_popen)

    task = TaskNode(
        id="task1",
        prompt_template="{prompt}",
        retry_policy=RetryPolicy(max_attempts=1),
        working_dir="/tmp/workdir",
        output_format="text",
        max_turns=7,
    )

    result = run_claude_task(
        task,
        "annotate file",
        ClaudeConfig(cli_path="claude", default_model="sonnet"),
        LimitsConfig(),
        working_dir="/tmp/workdir",
    )

    assert result.status == TaskStatus.SUCCESS
    assert "--max-turns" in captured["cmd"]
    # max_turns=7 应作为 CLI 参数传递
    idx = captured["cmd"].index("--max-turns")
    assert captured["cmd"][idx + 1] == "7"
    assert result.cli_duration_ms is not None
    assert result.cli_duration_ms >= 0  # mock 环境下可能为 0


def test_run_claude_task_fails_when_turn_watchdog_exceeded(monkeypatch) -> None:
    # 使用与实际解析器匹配的事件类型：assistant 计为 turn_started，result 计为 turn_completed
    stream = "\n".join([
        '{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}',
        '{"type":"result","usage":{"input_tokens":11,"output_tokens":3}}',
        '{"type":"assistant","message":{"content":[{"type":"text","text":"world"}]}}',
    ]) + "\n"

    def fake_popen(cmd, **kwargs):
        return _FakeProc(cmd, stdout_text=stream)

    def fake_kill(proc, task_id=""):
        proc.returncode = 137

    monkeypatch.setattr("claude_orchestrator.claude_cli.subprocess.Popen", fake_popen)
    monkeypatch.setattr("claude_orchestrator.claude_cli._kill_process_tree", fake_kill)

    task = TaskNode(
        id="task2",
        prompt_template="{prompt}",
        retry_policy=RetryPolicy(max_attempts=1),
        working_dir="/tmp/workdir",
        output_format="text",
        max_turns=1,
    )

    result = run_claude_task(
        task,
        "annotate file",
        ClaudeConfig(cli_path="claude", default_model="sonnet"),
        LimitsConfig(),
        working_dir="/tmp/workdir",
    )

    assert result.status == TaskStatus.FAILED
    assert result.max_turns_exceeded is True
    assert result.turn_started == 2
    assert result.turn_completed == 1
