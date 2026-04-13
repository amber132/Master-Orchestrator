from claude_orchestrator.auto_model import Phase
from claude_orchestrator.closure_planner import ClosurePlanner
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType


def test_closure_planner_inserts_required_nodes() -> None:
    phase = Phase(
        id="phase_1",
        name="实现登录修复",
        description="desc",
        order=0,
        raw_tasks=[{"id": "fix_login", "prompt": "修复登录接口", "depends_on": []}],
    )
    contract = TaskContract(
        source_goal="修复登录接口",
        normalized_goal="修复登录接口",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.BUGFIX,
        data_risk=DataRisk.NONE,
        affected_areas=["backend"],
        document_paths=[],
    )

    planned = ClosurePlanner().plan_phase(phase, contract)
    task_ids = [task["id"] for task in planned.raw_tasks]

    assert task_ids[0].startswith("analyze_")
    assert any(task_id.startswith("scope_") for task_id in task_ids)
    assert any(task_id.startswith("verify_") for task_id in task_ids)
    assert any(task_id.startswith("handoff_") for task_id in task_ids)


def test_closure_planner_adds_backup_node_when_needed() -> None:
    phase = Phase(id="phase_2", name="数据修复", description="desc", order=0, raw_tasks=[])
    contract = TaskContract(
        source_goal="修复数据",
        normalized_goal="修复数据",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.DATABASE,
        affected_areas=["database"],
        document_paths=[],
    )

    planned = ClosurePlanner().plan_phase(phase, contract)
    task_ids = [task["id"] for task in planned.raw_tasks]
    assert any(task_id.startswith("backup_") for task_id in task_ids)


def test_closure_planner_uses_native_closure_for_strict_refactor() -> None:
    phase = Phase(
        id="phase_3",
        name="收敛 BatchReplaceService",
        description="desc",
        order=0,
        raw_tasks=[{"id": "refactor_service", "prompt": "仅重构 BatchReplaceService", "depends_on": []}],
    )
    contract = TaskContract(
        source_goal="持续重构后端",
        normalized_goal="持续重构后端",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.NONE,
        affected_areas=["backend"],
        document_paths=[],
    )

    planned = ClosurePlanner().plan_phase(phase, contract)
    task_ids = [task["id"] for task in planned.raw_tasks]

    assert any(task_id.startswith("analyze_") for task_id in task_ids)
    assert any(task_id.startswith("scope_") for task_id in task_ids)
    assert not any(task_id.startswith("verify_") for task_id in task_ids)
    assert not any(task_id.startswith("handoff_") for task_id in task_ids)
    implement_task = next(task for task in planned.raw_tasks if task["id"] == "refactor_service")
    assert "strict_refactor" in implement_task["tags"]


def test_closure_planner_adds_native_verification_for_strict_refactor() -> None:
    phase = Phase(
        id="phase_4",
        name="收敛 sample.py 头部注释",
        description="desc",
        order=0,
        acceptance_criteria=["sample.py 顶部新增目标注释，且 Python 语法有效"],
        raw_tasks=[{"id": "edit_sample", "prompt": "仅编辑 sample.py 头部注释", "depends_on": []}],
    )
    contract = TaskContract(
        source_goal="对 sample.py 做极小重构",
        normalized_goal="对 sample.py 做极小重构",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.NONE,
        affected_areas=["backend"],
        document_paths=[],
    )

    planned = ClosurePlanner().plan_phase(phase, contract)
    task_ids = [task["id"] for task in planned.raw_tasks]

    assert any(task_id.startswith("native_verify_") for task_id in task_ids)
