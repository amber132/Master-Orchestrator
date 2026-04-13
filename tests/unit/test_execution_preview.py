from claude_orchestrator.execution_preview import ExecutionPreview
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType


def test_execution_preview_renders_expected_summary() -> None:
    contract = TaskContract(
        source_goal="根据 docs/refactor/README.md 做模板复制联调",
        normalized_goal="根据 docs/refactor/README.md 做模板复制联调",
        input_type=TaskInputType.MIXED,
        task_type=TaskType.INTEGRATION,
        data_risk=DataRisk.FILES,
        affected_areas=["frontend", "backend"],
        document_paths=["docs/refactor/README.md"],
    )

    preview = ExecutionPreview.from_contract(
        contract,
        verification_commands=["./mvnw test", "npm run build"],
    )

    rendered = preview.render_text()
    assert "联调" in rendered
    assert "docs/refactor/README.md" in rendered
    assert "需要备份" in rendered
    assert "./mvnw test" in rendered
