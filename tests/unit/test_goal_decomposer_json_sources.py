from __future__ import annotations

import json
import os
import time
from pathlib import Path

from claude_orchestrator.auto_model import AutoConfig
from claude_orchestrator.config import ClaudeConfig, LimitsConfig
from claude_orchestrator.goal_decomposer import GoalDecomposer


def _decomposer(workdir: Path) -> GoalDecomposer:
    return GoalDecomposer(ClaudeConfig(), LimitsConfig(), AutoConfig(), working_dir=str(workdir))


def _touch_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")
    now = time.time()
    os.utime(path, (now, now))


def _valid_payload() -> dict:
    return {
        "phases": [
            {
                "id": "phase_1",
                "name": "phase",
                "description": "desc",
                "tasks": [{"id": "task_1", "prompt": "implement"}],
            }
        ]
    }


def test_extract_json_from_output_ignores_generic_recent_json_files(tmp_path: Path) -> None:
    _touch_json(tmp_path / "plan.json", _valid_payload())

    data = _decomposer(tmp_path)._extract_json_from_output("not json")

    assert data is None


def test_extract_json_from_output_accepts_decompose_prefixed_recent_json_files(tmp_path: Path) -> None:
    _touch_json(tmp_path / "decompose_result.json", _valid_payload())

    data = _decomposer(tmp_path)._extract_json_from_output("not json")

    assert data == _valid_payload()


def test_extract_json_from_output_rejects_incomplete_decomposition_payload(tmp_path: Path) -> None:
    _touch_json(
        tmp_path / "decompose_result.json",
        {"phases": [{"id": "phase_1", "tasks": [{"id": "task_1"}]}]},
    )

    data = _decomposer(tmp_path)._extract_json_from_output("not json")

    assert data is None
