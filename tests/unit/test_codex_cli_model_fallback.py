from __future__ import annotations

import io
import time

from codex_orchestrator.codex_cli import run_codex_task
from codex_orchestrator.config import CodexConfig, LimitsConfig
from codex_orchestrator.model import RetryPolicy, TaskNode, TaskStatus


class _FakeStdin(io.StringIO):
    def close(self) -> None:
        pass


class _FakeProc:
    def __init__(self, cmd, stdout_text="", stderr_text="", returncode=None):
        self.cmd = cmd
        self.pid = 50123
        self.returncode = returncode
        self.stdin = _FakeStdin()
        self.stdout = io.StringIO(stdout_text)
        self.stderr = io.StringIO(stderr_text)

    def wait(self, timeout=None):
        deadline = time.time() + min(timeout or 1, 0.2)
        while self.returncode is None and time.time() < deadline:
            time.sleep(0.01)
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def poll(self):
        return self.returncode


def test_run_codex_task_falls_back_on_invalid_model_error(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_popen(cmd, **kwargs):
        calls.append(cmd)
        if len(calls) == 1:
            stderr_text = 'API Error: 400 {"error":{"code":"1211","message":"model_not_found"}}\n'
            return _FakeProc(cmd, stdout_text="", stderr_text=stderr_text, returncode=1)
        return _FakeProc(
            cmd,
            stdout_text='{"type":"item.completed","item":{"type":"agent_message","text":"ok"}}\n{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}\n',
            stderr_text="",
            returncode=0,
        )

    monkeypatch.setattr("codex_orchestrator.codex_cli.subprocess.Popen", fake_popen)

    task = TaskNode(
        id="fallback-task",
        prompt_template="{prompt}",
        retry_policy=RetryPolicy(max_attempts=1),
        working_dir="/tmp/workdir",
        output_format="text",
        model="gpt-5.4",
    )

    result = run_codex_task(
        task,
        "annotate file",
        CodexConfig(cli_path="codex", default_model="gpt-5.4"),
        LimitsConfig(),
        working_dir="/tmp/workdir",
    )

    assert result.status == TaskStatus.SUCCESS
    assert result.model_used == "gpt-5.3-codex"
    assert len(calls) == 2
    assert calls[0][calls[0].index("--model") + 1] == "gpt-5.4"
    assert calls[1][calls[1].index("--model") + 1] == "gpt-5.3-codex"
