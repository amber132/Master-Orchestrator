from __future__ import annotations

from pathlib import Path

from claude_orchestrator.repo_profile import RepoProfile
from claude_orchestrator.task_classifier import TaskClassification
from claude_orchestrator.task_contract import TaskInputType, TaskType
from claude_orchestrator.task_intake import build_task_contract, normalize_request
from claude_orchestrator.task_templates import build_document_execution_rules


def test_document_paths_are_summarized_into_contract(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    guide = repo_root / "CLAUDE_STUDY_ANNOTATION_GUIDE.md"
    guide.write_text(
        "# LangGraph 学习注释 Guide\n\n"
        "## 第一优先级\n"
        "- `libs/langgraph/langgraph/graph`\n"
        "- `libs/langgraph/langgraph/pregel`\n"
        "\n"
        "## 复杂联动链路\n"
        "- graph -> pregel -> checkpoint\n",
        encoding="utf-8",
    )

    request = normalize_request("", [str(guide)], repo_root)

    assert request.input_type is TaskInputType.DOCUMENT_PATH
    assert request.document_paths == [guide]
    assert request.document_briefs
    assert "LangGraph 学习注释 Guide" in request.document_context
    assert "libs/langgraph/langgraph/graph" in request.document_context

    contract = build_task_contract(
        request=request,
        profile=RepoProfile(root=repo_root),
        classification=TaskClassification(task_type=TaskType.FEATURE, confidence=0.9, reasons=["test"]),
    )

    assert contract.document_paths == [str(guide)]
    assert contract.has_document_context
    assert "LangGraph 学习注释 Guide" in contract.document_context
    assert "document_context" in contract.metadata

    rules = build_document_execution_rules(contract)
    assert "文档驱动执行约束" in rules
    assert str(guide) in rules


def test_goal_path_is_promoted_to_document_input(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    plan = repo_root / "docs" / "plan.md"
    plan.parent.mkdir()
    plan.write_text("# Plan\n\n- step 1\n- step 2\n", encoding="utf-8")

    request = normalize_request(str(plan), [], repo_root)

    assert request.goal.startswith("根据文档执行任务:")
    assert request.document_paths == [plan]
    assert request.input_type is TaskInputType.DOCUMENT_PATH
