from __future__ import annotations

from pathlib import Path

from claude_orchestrator.config import Config
from claude_orchestrator.model import TaskResult, TaskStatus
from claude_orchestrator.simple_isolation import PreparedItemWorkspace, _file_hash
from claude_orchestrator.simple_model import AttemptState, SimpleItemType, SimpleWorkItem, ValidationReport
from claude_orchestrator.simple_validation import SimpleValidationPipeline, classify_simple_failure


def _prepared_file(repo: Path, target: str) -> PreparedItemWorkspace:
    item = SimpleWorkItem(
        item_id="item-1",
        item_type=SimpleItemType.FILE,
        target=target,
        bucket="src",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(attempt=1, max_attempts=3),
        timeout_seconds=30,
    )
    target_path = (repo / target).resolve()
    baseline_bytes = target_path.read_bytes() if target_path.exists() else None
    return PreparedItemWorkspace(
        item=item,
        requested_mode="none",
        effective_mode="none",
        cwd=repo,
        target_path=target_path,
        source_target_path=target_path,
        git_root=None,
        source_baseline_hash=_file_hash(target_path),
        target_baseline_hash=_file_hash(target_path),
        source_baseline_bytes=baseline_bytes,
    )


def test_validation_classifies_target_path_mismatch_for_wrong_file_only_change(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "b.py").write_text("# note\nprint('b')\n", encoding="utf-8")

    pipeline = SimpleValidationPipeline(Config())
    prepared = _prepared_file(repo, "a.py")
    result = TaskResult(task_id="item-1", status=TaskStatus.SUCCESS, output="ok")

    report = pipeline.validate(prepared, result, ["b.py"])

    assert report.passed is False
    assert report.failure_code == "target_path_mismatch"
    assert report.unauthorized_changes == ["b.py"]
    assert classify_simple_failure(result, report) == "target_path_mismatch"


def test_validation_classifies_unauthorized_side_files_when_target_is_modified(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "b.py").write_text("print('b')\n", encoding="utf-8")

    prepared = _prepared_file(repo, "a.py")
    (repo / "a.py").write_text("# note\nprint('a')\n", encoding="utf-8")
    (repo / "b.py").write_text("# side\nprint('b')\n", encoding="utf-8")

    pipeline = SimpleValidationPipeline(Config())
    result = TaskResult(task_id="item-1", status=TaskStatus.SUCCESS, output="ok")

    report = pipeline.validate(prepared, result, ["a.py", "b.py"])

    assert report.passed is False
    assert report.failure_code == "unauthorized_side_files"
    assert report.target_touched is True
    assert report.unauthorized_changes == ["b.py"]
    assert classify_simple_failure(result, report) == "unauthorized_side_files"


def test_validation_classifies_missing_target_after_exec(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")

    prepared = _prepared_file(repo, "a.py")
    (repo / "a.py").unlink()

    pipeline = SimpleValidationPipeline(Config())
    result = TaskResult(task_id="item-1", status=TaskStatus.SUCCESS, output="ok")

    report = pipeline.validate(prepared, result, ["a.py"])

    assert report.passed is False
    assert report.target_exists_after is False
    assert report.failure_code == "target_missing_after_exec"
    assert classify_simple_failure(result, report) == "target_missing_after_exec"


def test_classify_simple_failure_prefers_max_turns_watchdog() -> None:
    result = TaskResult(
        task_id="item-1",
        status=TaskStatus.FAILED,
        error="TASK_MAX_TURNS_EXCEEDED",
        max_turns_exceeded=True,
    )

    report = ValidationReport(
        passed=False,
        failure_code="execution_failed",
        failure_reason="execution failed",
    )

    assert classify_simple_failure(result, report) == "max_turns_exceeded"
