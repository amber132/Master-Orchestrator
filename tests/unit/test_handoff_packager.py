from pathlib import Path

from claude_orchestrator.auto_model import GoalState, GoalStatus
from claude_orchestrator.delivery_manifest import DeliveryManifest
from claude_orchestrator.handoff_packager import HandoffPackager
from claude_orchestrator.runtime_layout import RuntimeLayout


def test_handoff_packager_writes_json_and_markdown(tmp_path: Path) -> None:
    layout = RuntimeLayout.create(tmp_path / "run")
    state = GoalState(goal_text="修复登录接口", status=GoalStatus.CONVERGED)
    manifest = DeliveryManifest(
        run_id="run-1",
        status="success",
        branch_names=["orchestrator/bugfix/run-1"],
        worktree_paths=[str(layout.workspace)],
        backup_summary="无",
        verification_summary="2/2 通过",
        recommended_merge_order=["orchestrator/bugfix/run-1"],
    )

    result = HandoffPackager().write(layout, state, manifest)

    assert result.json_path.exists()
    assert result.markdown_path.exists()
