from pathlib import Path

from claude_orchestrator.repo_profile import RepoProfile
from claude_orchestrator.task_classifier import TaskClassification
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType
from claude_orchestrator.task_intake import TaskIntakeRequest, build_task_contract


def test_refactor_task_contract_enables_strict_guardrails() -> None:
    contract = TaskContract(
        source_goal="持续重构后端",
        normalized_goal="持续重构后端",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.NONE,
        affected_areas=["backend"],
        document_paths=[],
    )

    assert contract.strict_refactor_mode is True
    assert contract.max_service_families_per_phase == 1
    assert contract.max_prod_files_per_iteration == 8
    assert contract.forbid_state_file_edits is True
    assert contract.require_guardrail_tests_before_service_moves is True
    assert contract.uses_native_phase_closure is True



def test_build_task_contract_populates_refactor_allowed_roots() -> None:
    profile = RepoProfile(
        root=Path('.'),
        has_backend=True,
        backend_dir=Path('.'),
        detected_frameworks=['spring-boot'],
        backend_commands=['mvnw.cmd -q -DskipTests compile'],
    )
    classification = TaskClassification(
        task_type=TaskType.REFACTOR,
        confidence=0.95,
        reasons=['命中重构关键词'],
        affected_areas=['backend'],
    )
    request = TaskIntakeRequest(
        goal='对当前后端进行持续重构',
        document_paths=[],
        repo_root=Path('.'),
    )

    contract = build_task_contract(request, profile, classification)

    assert contract.strict_refactor_mode is True
    assert 'src/main' in contract.allowed_refactor_roots
    assert 'src/test' in contract.allowed_refactor_roots
    assert 'pom.xml' in contract.allowed_refactor_roots
