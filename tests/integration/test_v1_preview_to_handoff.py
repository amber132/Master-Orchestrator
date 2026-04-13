from pathlib import Path

from claude_orchestrator.delivery_manifest import DeliveryManifest
from claude_orchestrator.execution_preview import ExecutionPreview
from claude_orchestrator.handoff_packager import HandoffPackager
from claude_orchestrator.repo_profile import RepoProfiler
from claude_orchestrator.runtime_layout import RuntimeLayout
from claude_orchestrator.task_classifier import TaskClassifier
from claude_orchestrator.task_intake import TaskIntakeRequest, build_task_contract
from claude_orchestrator.verification_planner import VerificationPlanner


def test_preview_to_handoff_flow(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pom.xml").write_text("<project></project>", encoding="utf-8")
    (repo / "mvnw").write_text("", encoding="utf-8")
    frontend = repo / "frontend"
    frontend.mkdir()
    (frontend / "package.json").write_text('{"scripts":{"build":"vite build","lint":"eslint ."}}', encoding="utf-8")

    request = TaskIntakeRequest(goal="修复模板复制联调问题", document_paths=[], repo_root=repo)
    profile = RepoProfiler().profile(repo)
    classification = TaskClassifier().classify(request, profile)
    contract = build_task_contract(request, profile, classification)
    preview = ExecutionPreview.from_contract(contract, [item.command for item in VerificationPlanner().plan(contract, profile).commands])
    layout = RuntimeLayout.create(tmp_path / "runtime")

    manifest = DeliveryManifest(
        run_id="run-1",
        status="success",
        branch_names=["orchestrator/integration/run-1"],
        worktree_paths=[str(layout.workspace)],
        backup_summary="未涉及数据",
        verification_summary=preview.verification_summary,
        recommended_merge_order=["orchestrator/integration/run-1"],
    )

    result = HandoffPackager().write(layout, None, manifest)
    assert result.markdown_path.exists()
    assert "修复模板复制联调问题" in preview.render_text()
