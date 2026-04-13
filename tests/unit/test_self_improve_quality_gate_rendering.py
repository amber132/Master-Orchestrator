from __future__ import annotations

from pathlib import Path

from master_orchestrator.self_improve import SelfImproveController


def test_render_quality_gate_command_expands_workspace_and_source_paths(tmp_path: Path) -> None:
    controller = SelfImproveController.__new__(SelfImproveController)
    controller._execution_repo_dir = tmp_path / "workspace"
    controller._source_repo_dir = tmp_path / "source"

    rendered = controller._render_quality_gate_command(
        "python tool.py --repo-root {workspace_dir} --toolchain-root {source_repo}"
    )

    assert str(controller._execution_repo_dir) in rendered
    assert str(controller._source_repo_dir) in rendered
