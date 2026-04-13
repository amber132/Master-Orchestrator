from pathlib import Path

from codex_orchestrator.model import TaskNode, TaskStatus
from codex_orchestrator.operation_executor import OperationExecutor


def test_operation_executor_returns_structured_architecture_refs(tmp_path: Path) -> None:
    task = TaskNode(
        id="playbook_wave_cutover_operation",
        prompt_template="执行切流检查",
        type="operation",
        output_format="json",
        executor_config={
            "rollback_refs": ["rollback_cutover"],
            "cutover_gates": ["基线回归验证通过。", "部署配置 dry-run 通过。"],
            "commands": [
                {
                    "id": "baseline",
                    "command": "python -c \"print('baseline-ok')\"",
                    "evidence_refs": ["baseline_pass"],
                    "satisfies_gates": ["基线回归验证通过。"],
                },
                {
                    "id": "compose",
                    "command": "python -c \"print('compose-ok')\"",
                    "evidence_refs": ["rollout_checklist"],
                    "satisfies_gates": ["部署配置 dry-run 通过。"],
                },
            ],
        },
    )

    result = OperationExecutor().execute(
        task=task,
        prompt=task.prompt_template,
        codex_config=None,
        limits=None,
        budget_tracker=None,
        working_dir=str(tmp_path),
        on_progress=None,
    )

    assert result.status is TaskStatus.SUCCESS
    assert result.parsed_output is not None
    assert result.parsed_output["EvidenceRefs"] == ["baseline_pass", "rollout_checklist"]
    assert result.parsed_output["RollbackRefs"] == ["rollback_cutover"]
    assert result.parsed_output["UnmetCutoverGates"] == []
    assert "EvidenceRefs:" in (result.output or "")


def test_operation_executor_renders_backup_output_template(tmp_path: Path) -> None:
    task = TaskNode(
        id="playbook_wave_data_operation",
        prompt_template="执行备份演练",
        type="operation",
        output_format="json",
        executor_config={
            "rollback_refs": ["rollback_data_path"],
            "commands": [
                {
                    "id": "backup",
                    "command": (
                        "python -c \"from pathlib import Path; "
                        "Path(r'{output}').write_text('snapshot', encoding='utf-8')\""
                    ),
                    "output_file": "evidence/backups/{task_id}_{command_id}.sql",
                    "evidence_refs": ["backup_snapshot"],
                }
            ],
        },
    )

    result = OperationExecutor().execute(
        task=task,
        prompt=task.prompt_template,
        codex_config=None,
        limits=None,
        budget_tracker=None,
        working_dir=str(tmp_path),
        on_progress=None,
    )

    assert result.status is TaskStatus.SUCCESS
    command_result = result.parsed_output["operation"]["command_results"][0]
    output_path = Path(command_result["output_file"])
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == "snapshot"
    assert result.parsed_output["EvidenceRefs"] == ["backup_snapshot"]


def test_operation_executor_returns_failed_result_when_command_fails(tmp_path: Path) -> None:
    task = TaskNode(
        id="playbook_wave_retry_operation",
        prompt_template="执行失败任务",
        type="operation",
        output_format="json",
        executor_config={
            "commands": [
                {
                    "id": "fail_once",
                    "command": "python -c \"import sys; sys.exit(1)\"",
                }
            ],
        },
    )

    result = OperationExecutor().execute(
        task=task,
        prompt=task.prompt_template,
        codex_config=None,
        limits=None,
        budget_tracker=None,
        working_dir=str(tmp_path),
        on_progress=None,
    )

    assert result.status is TaskStatus.FAILED
    assert "operation command failed" in (result.error or "")
