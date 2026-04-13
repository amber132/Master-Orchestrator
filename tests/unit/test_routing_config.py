from __future__ import annotations

import os

from master_orchestrator.config import load_config


def test_load_config_reads_dual_provider_env_vars(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[claude]
default_model = "sonnet"

[codex]
default_model = "gpt-5.4-pro"

[routing.phase_defaults]
execute = "codex"
review = "claude"
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("CLAUDE_DEFAULT_MODEL", "opus")
    monkeypatch.setenv("CODEX_CLI_PATH", "codex-custom")

    config = load_config(str(config_path))

    assert config.claude.default_model == "opus"
    assert config.codex.cli_path == "codex-custom"
    assert config.routing.phase_defaults["execute"] == "codex"

    monkeypatch.delenv("CLAUDE_DEFAULT_MODEL", raising=False)
    monkeypatch.delenv("CODEX_CLI_PATH", raising=False)
