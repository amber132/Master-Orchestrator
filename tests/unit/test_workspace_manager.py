from pathlib import Path

from claude_orchestrator.config import Config
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType
from claude_orchestrator.workspace_manager import WorkspaceManager


def test_workspace_manager_creates_runtime_layout_and_worktree(git_repo: Path, tmp_path: Path) -> None:
    cfg = Config()
    cfg.workspace.root_dir = str(tmp_path / "runs")
    manager = WorkspaceManager(cfg.workspace)
    contract = TaskContract(
        source_goal="修复后端 bug",
        normalized_goal="修复后端 bug",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.BUGFIX,
        data_risk=DataRisk.NONE,
        affected_areas=["backend"],
        document_paths=[],
    )

    session = manager.create_session(git_repo, contract)

    assert session.layout.root.exists()
    assert session.layout.workspace.exists()
    assert (session.layout.root / "logs").exists()
    assert session.branch_names


def test_workspace_manager_falls_back_to_copied_workspace_when_head_missing(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "master_orchestrator").mkdir()
    (repo / "master_orchestrator" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "README.md").write_text("demo\n", encoding="utf-8")

    import subprocess

    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)

    cfg = Config()
    cfg.workspace.root_dir = str(tmp_path / "runs")
    manager = WorkspaceManager(cfg.workspace)
    contract = TaskContract(
        source_goal="修复后端 bug",
        normalized_goal="修复后端 bug",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.BUGFIX,
        data_risk=DataRisk.NONE,
        affected_areas=["backend"],
        document_paths=[],
    )

    session = manager.create_session(repo, contract)

    assert session.layout.workspace.exists()
    assert (session.layout.workspace / "README.md").read_text(encoding="utf-8") == "demo\n"
    assert (session.layout.workspace / "master_orchestrator" / "__init__.py").exists()
    assert session.branch_names == []
    assert session.worktree_paths == [session.layout.workspace]

