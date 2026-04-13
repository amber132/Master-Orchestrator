from pathlib import Path

from claude_orchestrator.autonomous import AutonomousController
from claude_orchestrator.auto_model import GoalStatus
from claude_orchestrator.config import Config
from claude_orchestrator.repo_profile import RepoProfile
from claude_orchestrator.store import Store


def test_execute_uses_repo_profile_context_without_llm_analysis(tmp_path: Path, monkeypatch) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    cfg = Config()
    cfg.checkpoint.db_path = str(tmp_path / "state.db")
    profile = RepoProfile(
        root=Path("D:/example/sample_app3"),
        has_backend=True,
        has_frontend=True,
        detected_frameworks=["react", "spring-boot"],
        package_managers=["maven", "npm"],
        backend_commands=["mvnw.cmd -q -DskipTests compile", "mvnw.cmd test"],
        frontend_commands=["npm run build", "npm run lint"],
        file_backup_paths=["uploads"],
        database_backup_commands=["pg_dump --file {output}"],
    )

    with Store(cfg.checkpoint.db_path) as store:
        controller = AutonomousController(
            goal="修复前后端联调问题",
            working_dir=str(workdir),
            config=cfg,
            store=store,
            repo_profile=profile,
        )

        monkeypatch.setattr(controller, "_run_preflight_checks", lambda: None)
        monkeypatch.setattr(controller, "_analyze_project", lambda: (_ for _ in ()).throw(AssertionError("should not call llm project analysis")))
        monkeypatch.setattr(controller._decomposer, "decompose", lambda goal, context, task_contract=None: [])
        monkeypatch.setattr(controller, "_execute_phases_parallel", lambda: None)
        monkeypatch.setattr(controller, "_finalize_state", lambda: setattr(controller._state, "status", GoalStatus.CONVERGED))

        state = controller.execute()

    assert state.status is GoalStatus.CONVERGED
    assert "仓库画像" in state.project_context
    assert "uploads" in state.project_context
    assert "pg_dump" in state.project_context
