from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

from claude_orchestrator.cli import _build_parser, _resolve_command_aliases


def test_top_level_help_prefers_simplified_commands() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "claude_orchestrator.cli", "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )

    assert result.returncode == 0
    assert "do" in result.stdout
    assert "runs" in result.stdout
    assert "improve" in result.stdout
    assert "retry-failed" not in result.stdout
    assert "simple" not in result.stdout


def test_do_resolves_existing_dag_to_run(tmp_path: Path) -> None:
    dag_file = tmp_path / "workflow.toml"
    dag_file.write_text(
        "[dag]\nname='demo'\nmax_parallel=1\n\n[tasks.echo]\nprompt='hi'\n",
        encoding="utf-8",
    )

    parser = _build_parser()
    args = parser.parse_args(["do", str(dag_file), "-d", str(tmp_path)])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "run"
    assert resolved.dag == str(dag_file)


def test_do_resume_with_existing_dag_resolves_to_resume(tmp_path: Path) -> None:
    dag_file = tmp_path / "workflow.toml"
    dag_file.write_text(
        "[dag]\nname='demo'\nmax_parallel=1\n\n[tasks.echo]\nprompt='hi'\n",
        encoding="utf-8",
    )

    parser = _build_parser()
    args = parser.parse_args(["do", str(dag_file), "--resume", "-d", str(tmp_path)])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "resume"
    assert resolved.dag == str(dag_file)


def test_do_with_simple_inputs_resolves_to_simple_run(tmp_path: Path) -> None:
    target_file = tmp_path / "sample.py"
    target_file.write_text("print('hi')\n", encoding="utf-8")

    parser = _build_parser()
    args = parser.parse_args(
        ["do", "annotate this file", "-d", str(tmp_path), "--files", str(target_file)]
    )

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "simple"
    assert resolved.simple_command == "run"
    assert resolved.instruction == "annotate this file"


def test_do_with_goal_text_resolves_to_auto(tmp_path: Path) -> None:
    parser = _build_parser()
    args = parser.parse_args(["do", "fix login bug", "-d", str(tmp_path)])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "auto"
    assert resolved.goal == "fix login bug"


def test_runs_defaults_to_status() -> None:
    parser = _build_parser()
    args = parser.parse_args(["runs"])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "status"


def test_runs_graph_resolves_to_visualize(tmp_path: Path) -> None:
    dag_file = tmp_path / "workflow.toml"
    dag_file.write_text(
        "[dag]\nname='demo'\nmax_parallel=1\n\n[tasks.echo]\nprompt='hi'\n",
        encoding="utf-8",
    )

    parser = _build_parser()
    args = parser.parse_args(["runs", str(dag_file), "--graph"])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "visualize"
    assert resolved.dag == str(dag_file)


def test_runs_retry_resolves_to_retry_failed(tmp_path: Path) -> None:
    dag_file = tmp_path / "workflow.toml"
    dag_file.write_text(
        "[dag]\nname='demo'\nmax_parallel=1\n\n[tasks.echo]\nprompt='hi'\n",
        encoding="utf-8",
    )

    parser = _build_parser()
    args = parser.parse_args(["runs", str(dag_file), "--retry"])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "retry-failed"
    assert resolved.dag == str(dag_file)


def test_improve_alias_resolves_to_self_improve(tmp_path: Path) -> None:
    parser = _build_parser()
    args = parser.parse_args(["improve", "-d", str(tmp_path)])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "self-improve"


def test_pyproject_registers_short_mo_script() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    assert data["project"]["scripts"]["mo"] == "master_orchestrator.cli:main"
