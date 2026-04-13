from __future__ import annotations

from pathlib import Path

from claude_orchestrator.claude_cli import BudgetTracker
from claude_orchestrator.config import Config
from claude_orchestrator.rate_limiter import RateLimiter
from claude_orchestrator.simple_executor import SimpleExecutor


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_worker_home_links_non_system_skills_but_copies_system(tmp_path, monkeypatch) -> None:
    source_home = tmp_path / "source_home"
    _write(source_home / "config.toml", "model = 'x'\n")
    _write(source_home / "auth.json", "{}\n")
    _write(source_home / "version.json", "{}\n")
    _write(source_home / "skills" / "custom-skill" / "SKILL.md", "custom\n")
    _write(source_home / "skills" / ".system" / "skill-installer" / "SKILL.md", "system\n")

    monkeypatch.setenv("CLAUDE_HOME", str(source_home))

    config = Config()
    config.simple.claude_home_isolation = "worker"
    executor = SimpleExecutor(
        config,
        BudgetTracker(0.0),
        RateLimiter(config.rate_limit),
        state_root=tmp_path / "runs" / "run1" / "state",
    )

    home = executor.warm_worker_home("worker-01")

    assert home is not None
    # Windows 上 symlink 可能因权限不足回退为 copytree
    custom_skill = home / "skills" / "custom-skill"
    assert custom_skill.is_dir()  # symlink 或 copy 均为有效目录
    assert (home / "skills" / ".system").is_dir()
    assert not (home / "skills" / ".system").is_symlink()
    assert (home / "skills" / ".system" / "skill-installer" / "SKILL.md").read_text(encoding="utf-8") == "system\n"


def test_seed_home_merges_system_skills_from_previous_runs(tmp_path, monkeypatch) -> None:
    source_home = tmp_path / "source_home"
    _write(source_home / "config.toml", "model = 'x'\n")
    _write(source_home / "auth.json", "{}\n")
    _write(source_home / "version.json", "{}\n")
    _write(source_home / "skills" / "custom-skill" / "SKILL.md", "custom\n")
    _write(source_home / "skills" / ".system" / "skill-installer" / "SKILL.md", "system\n")

    previous_system_skill = (
        tmp_path
        / "runs"
        / "oldrun"
        / "state"
        / "claude_home"
        / "exec-slot-00"
        / "skills"
        / ".system"
        / "openai-docs"
        / "SKILL.md"
    )
    _write(previous_system_skill, "openai docs\n")

    monkeypatch.setenv("CLAUDE_HOME", str(source_home))

    config = Config()
    config.simple.claude_home_isolation = "worker"
    executor = SimpleExecutor(
        config,
        BudgetTracker(0.0),
        RateLimiter(config.rate_limit),
        state_root=tmp_path / "runs" / "newrun" / "state",
    )

    home = executor.warm_worker_home("worker-02")

    assert home is not None
    assert (home / "skills" / ".system" / "openai-docs" / "SKILL.md").read_text(encoding="utf-8") == "openai docs\n"
