from pathlib import Path

from codex_orchestrator.architecture_execution import build_architecture_execution_report
from codex_orchestrator.auto_model import AutoConfig, Phase
from codex_orchestrator.config import Config
from codex_orchestrator.dag_generator import DAGGenerator
from codex_orchestrator.orchestrator import Orchestrator
from codex_orchestrator.store import Store


def test_operation_executor_output_flows_into_architecture_execution_report(tmp_path: Path) -> None:
    cfg = Config()
    cfg.checkpoint.db_path = str(tmp_path / "state.db")
    cfg.health.memory_percent_max = 100
    cfg.limits.min_disk_space_mb = 0
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
                "executor_config": {
                    "rollback_refs": ["rollback_cutover"],
                    "cutover_gates": ["基线回归验证通过。"],
                    "commands": [
                        {
                            "id": "baseline",
                            "command": "python -c \"print('baseline-ok')\"",
                            "evidence_refs": ["rollout_checklist"],
                            "satisfies_gates": ["基线回归验证通过。"],
                        }
                    ],
                },
                "tags": ["architecture_playbook", "cutover", "operation"],
            }
        ],
        metadata={
            "architecture_execution_playbook_id": "playbook_service_extraction",
            "architecture_wave_id": "wave_cutover",
            "architecture_gate_scope": "cutover",
            "architecture_playbook_steps": [
                {
                    "step_id": "step_wave_cutover",
                    "stage": "cutover",
                    "title": "Cutover",
                    "objective": "准备切流",
                    "evidence_required": ["rollout_checklist"],
                    "rollback_action_ids": ["rollback_cutover"],
                }
            ],
            "architecture_cutover_gates": ["基线回归验证通过。"],
        },
    )

    dag = DAGGenerator(AutoConfig()).generate(
        phase=phase,
        project_context="Python backend",
    )

    with Store(cfg.checkpoint.db_path) as store:
        orchestrator = Orchestrator(
            dag=dag,
            config=cfg,
            store=store,
            working_dir=str(tmp_path),
        )
        run_info = orchestrator.run()

    assert run_info.status.value == "completed"
    task_outputs = {
        task_id: result.parsed_output or result.output
        for task_id, result in orchestrator.results.items()
        if result.status.value == "success"
    }

    report = build_architecture_execution_report(phase, task_outputs)

    assert report is not None
    assert report.status == "complete"
    assert report.gate_status == "ready"
    assert report.reported_evidence_refs == ["rollout_checklist"]
    assert report.reported_rollback_refs == ["rollback_cutover"]
