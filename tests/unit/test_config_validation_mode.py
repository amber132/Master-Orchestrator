from __future__ import annotations

import logging

import pytest

from claude_orchestrator.config import load_config
from claude_orchestrator.exceptions import ConfigValidationError


def test_load_config_warns_on_unknown_fields_by_default(tmp_path, caplog) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[claude]
default_model = "sonnet"
unexpected_option = "x"

[mystery]
foo = "bar"
""".strip(),
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING):
        cfg = load_config(config_path)

    assert cfg.claude.default_model == "sonnet"
    assert "unknown config keys" in caplog.text.lower()


def test_load_config_strict_mode_rejects_unknown_fields(tmp_path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[codex]
default_model = "gpt-5.4"
extra = true
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigValidationError):
        load_config(config_path, unknown_key_mode="strict")


def test_load_config_reads_codex_execution_security_mode(tmp_path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[codex]
execution_security_mode = "trusted_local"
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.codex.execution_security_mode == "trusted_local"
