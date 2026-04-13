from pathlib import Path

from claude_orchestrator.repo_profile import RepoProfiler
from claude_orchestrator.task_classifier import TaskClassifier
from claude_orchestrator.task_intake import TaskIntakeRequest
from claude_orchestrator.task_contract import TaskType


def _profile(repo: Path):
    return RepoProfiler().profile(repo)


def test_task_classifier_detects_bugfix(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pom.xml").write_text("<project></project>", encoding="utf-8")

    profile = _profile(repo)
    request = TaskIntakeRequest(goal="修复登录接口 500 错误", document_paths=[], repo_root=repo)
    classification = TaskClassifier().classify(request, profile)

    assert classification.task_type is TaskType.BUGFIX
    assert "backend" in classification.affected_areas


def test_task_classifier_detects_refactor_from_document(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    doc = repo / "docs" / "refactor-plan.md"
    doc.parent.mkdir()
    doc.write_text("架构重构计划", encoding="utf-8")

    profile = _profile(repo)
    request = TaskIntakeRequest(goal="", document_paths=[doc], repo_root=repo)
    classification = TaskClassifier().classify(request, profile)

    assert classification.task_type is TaskType.REFACTOR
