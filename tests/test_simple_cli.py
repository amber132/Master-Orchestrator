from __future__ import annotations

import argparse
from pathlib import Path

from claude_orchestrator.simple_cli import add_simple_subcommands, normalize_multi_value_args, resolve_instruction


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
