from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from master_orchestrator.self_improve import SelfImproveController
from master_orchestrator.workspace_manager import WorkspaceSession
from master_orchestrator.runtime_layout import RuntimeLayout


def test_merge_workspace_to_source_fallback_copies_master_package_and_docs(tmp_path: Path) -> None:
    source = tmp_path / "source"
    workspace = tmp_path / "workspace"
    source.mkdir()
    workspace.mkdir()
    (source / "master_orchestrator").mkdir()
    (source / "docs").mkdir()
    (workspace / "master_orchestrator").mkdir()
    (workspace / "docs").mkdir()

    (workspace / "master_orchestrator" / "sample.py").write_text("value = 1\n", encoding="utf-8")
    (workspace / "docs" / "note.md").write_text("updated\n", encoding="utf-8")

    controller = SelfImproveController.__new__(SelfImproveController)
    controller._source_repo_dir = source
    controller._execution_repo_dir = workspace
    controller._state = SimpleNamespace(session_id="sess-1")
    controller._workspace_session = WorkspaceSession(
        source_repo=source,
        layout=RuntimeLayout(
            root=tmp_path / "run",
            workspace=workspace,
            state=tmp_path / "run" / "state",
            logs=tmp_path / "run" / "logs",
            cache=tmp_path / "run" / "cache",
            evidence=tmp_path / "run" / "evidence",
            backups=tmp_path / "run" / "backups",
            handoff=tmp_path / "run" / "handoff",
        ),
        branch_names=[],
        worktree_paths=[workspace],
    )

    controller._merge_workspace_to_source()

    assert (source / "master_orchestrator" / "sample.py").read_text(encoding="utf-8") == "value = 1\n"
    assert (source / "docs" / "note.md").read_text(encoding="utf-8") == "updated\n"
