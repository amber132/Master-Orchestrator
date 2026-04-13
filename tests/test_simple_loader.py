from __future__ import annotations

import json
from pathlib import Path

from claude_orchestrator.simple_loader import load_simple_work_items


def test_load_simple_work_items_dedupes_same_target_and_instruction(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")

    result = load_simple_work_items(
        repo,
        "annotate file",
        files=["a.py"],
        globs=["a.py"],
    )

    assert len(result.items) == 1
    assert result.items[0].target == "a.py"
    assert result.source_summary["files"] == 1
    assert result.source_summary["globs"] == 0


def test_load_simple_work_items_parses_jsonl_task_file(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    task_file = tmp_path / "tasks.jsonl"
    task_file.write_text(
        json.dumps(
            {
                "target": "a.py",
                "instruction": "annotate",
                "priority": 3,
                "verify_commands": ["{python} -m py_compile {target}"],
                "require_patterns": ["print"],
            },
            ensure_ascii=False,
        ) + "\n",
        encoding="utf-8",
    )

    result = load_simple_work_items(repo, "fallback", task_file=str(task_file))

    assert len(result.items) == 1
    item = result.items[0]
    assert item.priority == 3
    assert item.validation_profile.verify_commands == ["{python} -m py_compile {target}"]
    assert item.validation_profile.require_patterns == ["print"]


def test_load_simple_work_items_skips_task_file_targets_outside_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    outside = tmp_path / "outside.py"
    outside.write_text("print('x')\n", encoding="utf-8")
    task_file = tmp_path / "tasks.jsonl"
    task_file.write_text(
        json.dumps({"target": str(outside), "instruction": "annotate"}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    result = load_simple_work_items(repo, "fallback", task_file=str(task_file))

    assert result.items == []
    assert any("不在工作目录内" in warning for warning in result.warnings)


def test_load_simple_work_items_skips_missing_task_file_targets(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    task_file = tmp_path / "tasks.jsonl"
    task_file.write_text(
        json.dumps({"target": "missing.py", "instruction": "annotate"}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    result = load_simple_work_items(
        repo,
        "fallback",
        task_file=str(task_file),
        validate_task_file_targets=True,
    )

    assert result.items == []
    assert any("不存在" in warning for warning in result.warnings)


def test_load_simple_work_items_trusts_explicit_task_file_targets_by_default(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    task_file = tmp_path / "tasks.jsonl"
    task_file.write_text(
        json.dumps({"target": "missing.py", "instruction": "annotate"}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    result = load_simple_work_items(repo, "fallback", task_file=str(task_file))

    assert len(result.items) == 1
    assert result.items[0].target == "missing.py"
    assert result.warnings == []
