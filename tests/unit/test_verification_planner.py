from pathlib import Path

from claude_orchestrator.repo_profile import RepoProfiler
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType
from claude_orchestrator.verification_planner import VerificationPlanner


def test_verification_planner_selects_backend_checks(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pom.xml").write_text("<project></project>", encoding="utf-8")
    (repo / "mvnw").write_text("", encoding="utf-8")

    profile = RepoProfiler().profile(repo)
    contract = TaskContract(
        source_goal="修复后端接口异常",
        normalized_goal="修复后端接口异常",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.BUGFIX,
        data_risk=DataRisk.NONE,
        affected_areas=["backend"],
        document_paths=[],
    )

    plan = VerificationPlanner().plan(contract, profile)
    commands = [item.command for item in plan.commands]
    assert any("mvn" in command for command in commands)


def test_verification_planner_selects_mixed_checks(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pom.xml").write_text("<project></project>", encoding="utf-8")
    (repo / "mvnw").write_text("", encoding="utf-8")
    frontend = repo / "frontend"
    frontend.mkdir()
    (frontend / "package.json").write_text('{"scripts":{"build":"vite build","lint":"eslint ."}}', encoding="utf-8")

    profile = RepoProfiler().profile(repo)
    contract = TaskContract(
        source_goal="实现模板复制前后端联调",
        normalized_goal="实现模板复制前后端联调",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.INTEGRATION,
        data_risk=DataRisk.NONE,
        affected_areas=["backend", "frontend"],
        document_paths=[],
    )

    plan = VerificationPlanner().plan(contract, profile)
    commands = [item.command for item in plan.commands]
    assert any("mvn" in command for command in commands)
    assert any("npm run build" in command for command in commands)
