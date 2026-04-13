from codex_orchestrator.auto_model import AutoConfig, CorrectiveAction, Phase, ReviewResult, ReviewVerdict
from codex_orchestrator.dag_generator import DAGGenerator


def test_dag_generator_preserves_operation_task_metadata() -> None:
    phase = Phase(
        id="wave_cutover",
        name="Cutover",
        description="准备切流",
        order=0,
        raw_tasks=[
            {
                "id": "playbook_wave_cutover_operation",
                "prompt": "执行切流前置检查",
                "type": "operation",
                "tags": ["architecture_playbook", "cutover", "operation"],
                "executor_config": {
                    "rollback_refs": ["rollback_cutover"],
                    "cutover_gates": ["基线回归验证通过。"],
                    "commands": [
                        {
                            "id": "baseline",
                            "command": "python -c \"print('baseline')\"",
                            "evidence_refs": ["baseline_pass"],
                            "satisfies_gates": ["基线回归验证通过。"],
                        }
                    ],
                },
            }
        ],
    )

    dag = DAGGenerator(AutoConfig()).generate(
        phase=phase,
        project_context="Spring Boot backend",
    )

    node = dag.tasks["playbook_wave_cutover_operation"]
    assert node.type == "operation"
    assert node.output_format == "json"
    assert node.executor_config is not None
    assert node.executor_config["rollback_refs"] == ["rollback_cutover"]
    assert node.executor_config["commands"][0]["command"] == "python -c \"print('baseline')\""


def test_dag_generator_preserves_operation_corrective_action_metadata() -> None:
    phase = Phase(
        id="wave_cutover",
        name="Cutover",
        description="准备切流",
        order=0,
    )
    review = ReviewResult(
        phase_id=phase.id,
        verdict=ReviewVerdict.MAJOR_ISSUES,
        score=0.4,
        summary="需要重跑操作检查",
        corrective_actions=[
            CorrectiveAction(
                action_id="arch_refresh_operations_wave_cutover",
                description="重新执行架构操作检查",
                prompt_template="重跑结构化操作检查",
                action_type="operation",
                executor_config={
                    "rollback_refs": ["rollback_cutover"],
                    "commands": [
                        {
                            "id": "baseline",
                            "command": "python -c \"print('retry')\"",
                            "evidence_refs": ["rollout_checklist"],
                        }
                    ],
                },
            )
        ],
    )

    dag = DAGGenerator(AutoConfig()).generate_correction_dag(
        phase=phase,
        review=review,
        project_context="Python backend",
    )

    node = dag.tasks["arch_refresh_operations_wave_cutover"]
    assert node.type == "operation"
    assert node.output_format == "json"
    assert node.executor_config is not None
    assert node.executor_config["commands"][0]["command"] == "python -c \"print('retry')\""
