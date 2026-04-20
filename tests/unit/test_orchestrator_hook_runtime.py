from __future__ import annotations

import sys

from master_orchestrator.orchestrator import Orchestrator


def test_execute_hook_normalizes_python_command(monkeypatch) -> None:
    recorded: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        recorded["cmd"] = cmd
        recorded["kwargs"] = kwargs

        class Result:
            returncode = 0
            stderr = ""

        return Result()

    monkeypatch.setattr("master_orchestrator.orchestrator.subprocess.run", fake_run)

    orchestrator = Orchestrator.__new__(Orchestrator)
    orchestrator._execute_hook('python -c "print(1)"', "task-1", "starting")

    assert recorded["cmd"][0] == sys.executable
    assert recorded["kwargs"]["shell"] is False
    assert recorded["kwargs"]["env"]["TASK_ID"] == "task-1"
    assert recorded["kwargs"]["env"]["TASK_STATUS"] == "starting"
