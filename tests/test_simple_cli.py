from __future__ import annotations

import argparse
import json
from pathlib import Path

from claude_orchestrator.simple_cli import (
    _preflight_simple_provider,
    _print_simple_json_error,
    _resolve_simple_preflight_provider,
    add_simple_subcommands,
    normalize_multi_value_args,
    resolve_instruction,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    def add_log_args(target: argparse.ArgumentParser) -> None:
        target.add_argument("--log-file", default=None)
        target.add_argument("--log-dir", default=None)

    add_simple_subcommands(sub, add_log_args)
    return parser


def test_simple_run_accepts_prompt_file_without_instruction(tmp_path: Path) -> None:
    prompt_file = tmp_path / "prompt.md"
    prompt_file.write_text("annotate from prompt\n", encoding="utf-8")

    parser = _build_parser()
    args = parser.parse_args(["simple", "run", "--prompt-file", str(prompt_file), "-d", str(tmp_path)])

    assert args.command == "simple"
    assert args.simple_command == "run"
    assert args.instruction == ""
    assert resolve_instruction(args) == "annotate from prompt"


def test_normalize_multi_value_args_flattens_repeated_option_values() -> None:
    values = [["**/*.py", "**/*.js"], ["src/**/*.ts"], "README.md", []]

    assert normalize_multi_value_args(values) == ["**/*.py", "**/*.js", "src/**/*.ts", "README.md"]


def test_simple_cancel_command_is_registered() -> None:
    parser = _build_parser()
    args = parser.parse_args(["simple", "cancel", "--run-id", "run-123"])

    assert args.command == "simple"
    assert args.simple_command == "cancel"
    assert args.run_id == "run-123"


def test_simple_reconcile_command_is_registered() -> None:
    parser = _build_parser()
    args = parser.parse_args(["simple", "reconcile", "--run-id", "run-123"])

    assert args.command == "simple"
    assert args.simple_command == "reconcile"
    assert args.run_id == "run-123"


def test_resolve_simple_preflight_provider_uses_simple_phase_default() -> None:
    parser = _build_parser()
    args = parser.parse_args(["simple", "run", "-d", "/tmp/demo"])

    config = type(
        "Config",
        (),
        {
            "routing": type(
                "Routing",
                (),
                {"default_provider": "auto", "phase_defaults": {"simple": "codex"}},
            )()
        },
    )()

    assert _resolve_simple_preflight_provider(args, config) == "codex"


def test_preflight_simple_provider_invokes_cli_preflight(monkeypatch, tmp_path: Path) -> None:
    parser = _build_parser()
    args = parser.parse_args(["simple", "run", "--provider", "codex", "-d", str(tmp_path)])

    config = type(
        "Config",
        (),
        {
            "routing": type(
                "Routing",
                (),
                {"default_provider": "auto", "phase_defaults": {"simple": "codex"}},
            )()
        },
    )()
    captured: dict[str, object] = {}

    def fake_preflight(work_dir, *, provider, config):
        captured["work_dir"] = work_dir
        captured["provider"] = provider
        captured["config"] = config

    monkeypatch.setattr("claude_orchestrator.cli._preflight_check", fake_preflight)
    monkeypatch.setattr("master_orchestrator.cli._preflight_check", fake_preflight)

    _preflight_simple_provider(args, config)

    assert captured["work_dir"] == tmp_path.resolve()
    assert captured["provider"] == "codex"
    assert captured["config"] is config


def test_print_simple_json_error_formats_payload(capsys) -> None:
    _print_simple_json_error("status", "No simple runs found.")

    payload = json.loads(capsys.readouterr().out)
    assert payload == {"command": "status", "error": "No simple runs found."}
