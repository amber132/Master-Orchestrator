"""项目级配置发现测试：验证 .claude-orchestrator/config.toml 的发现与合并逻辑。"""

from __future__ import annotations

from pathlib import Path

from claude_orchestrator.config import load_config


def test_project_config_overrides_global(tmp_path: Path):
    """项目级 config.toml 覆盖全局默认值。"""
    project_dir = tmp_path / "my_project"
    project_dir.mkdir()
    config_dir = project_dir / ".claude-orchestrator"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        '[orchestrator]\nmax_parallel = 99\n', encoding="utf-8"
    )

    cfg = load_config(project_dir=str(project_dir))
    assert cfg.orchestrator.max_parallel == 99


def test_no_project_config_uses_defaults(tmp_path: Path):
    """没有项目级配置时不崩溃，回退到全局配置文件或内置默认值。"""
    project_dir = tmp_path / "empty_project"
    project_dir.mkdir()
    cfg = load_config(project_dir=str(project_dir))
    # max_parallel 应为正整数（来自全局 config.toml 或内置默认值 4）
    assert cfg.orchestrator.max_parallel > 0


def test_env_var_overrides_project_config(tmp_path: Path, monkeypatch):
    """环境变量优先级高于项目级配置。"""
    project_dir = tmp_path / "env_test"
    project_dir.mkdir()
    config_dir = project_dir / ".claude-orchestrator"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        '[orchestrator]\nmax_parallel = 99\n', encoding="utf-8"
    )

    monkeypatch.setenv("ORCHESTRATOR_MAX_PARALLEL", "50")
    cfg = load_config(project_dir=str(project_dir))
    assert cfg.orchestrator.max_parallel == 50


def test_explicit_path_overrides_project_config(tmp_path: Path):
    """显式指定 -c/--config 时优先使用该文件，忽略项目级配置。"""
    project_dir = tmp_path / "explicit_project"
    project_dir.mkdir()
    # 项目级配置
    config_dir = project_dir / ".claude-orchestrator"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        '[orchestrator]\nmax_parallel = 99\n', encoding="utf-8"
    )
    # 显式指定的配置文件
    explicit_cfg = tmp_path / "my_config.toml"
    explicit_cfg.write_text(
        '[orchestrator]\nmax_parallel = 42\n', encoding="utf-8"
    )

    cfg = load_config(path=str(explicit_cfg), project_dir=str(project_dir))
    assert cfg.orchestrator.max_parallel == 42


def test_project_dir_none_is_backward_compatible():
    """不传 project_dir 时行为不变（向后兼容）。"""
    cfg = load_config()
    assert cfg.orchestrator.max_parallel > 0
