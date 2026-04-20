from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from master_orchestrator.cli import _build_parser, _resolve_command_aliases


def test_top_level_help_prefers_master_commands() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "master_orchestrator.cli", "--help"],
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
    assert "master-orchestrator" in result.stdout
    assert "retry-failed" not in result.stdout


def test_do_accepts_provider_override(tmp_path: Path) -> None:
    parser = _build_parser()
    args = parser.parse_args(["do", "fix login bug", "--provider", "codex", "-d", str(tmp_path)])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "auto"
    assert resolved.goal == "fix login bug"
    assert resolved.provider == "codex"


def test_provider_subcommand_sets_provider(tmp_path: Path) -> None:
    parser = _build_parser()
    args = parser.parse_args(["codex", "do", "implement paging", "-d", str(tmp_path)])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "auto"
    assert resolved.goal == "implement paging"
    assert resolved.provider == "codex"


def test_auto_accepts_phase_provider_overrides(tmp_path: Path) -> None:
    parser = _build_parser()
    args = parser.parse_args(
        [
            "do",
            "refactor payment flow",
            "-d",
            str(tmp_path),
            "--phase-provider",
            "execute=codex",
            "--phase-provider",
            "review=claude",
        ]
    )

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "auto"
    assert resolved.phase_provider == ["execute=codex", "review=claude"]


def test_visualize_uses_ascii_dependency_marker(tmp_path: Path) -> None:
    dag_file = tmp_path / "workflow.toml"
    repo_root = Path(__file__).resolve().parents[1]
    dag_file.write_text(
        "[dag]\nname='demo'\nmax_parallel=1\n\n[tasks.first]\nprompt='hi'\ntimeout=60\n\n[tasks.second]\nprompt='bye'\ntimeout=60\ndepends_on=['first']\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, "-m", "master_orchestrator.cli", "visualize", str(dag_file)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        cwd=repo_root,
    )

    assert result.returncode == 0
    assert "second <- first" in result.stdout


def test_do_at_dag_routes_to_run(tmp_path: Path) -> None:
    dag_file = tmp_path / "workflow.toml"
    dag_file.write_text("[dag]\nname='demo'\nmax_parallel=1\n", encoding="utf-8")
    parser = _build_parser()

    args = parser.parse_args(["do", f"@{dag_file}", "-d", str(tmp_path)])

    resolved = _resolve_command_aliases(args)

    assert resolved.command == "run"
    assert resolved.dag == str(dag_file)
    assert resolved.doc == []


def test_runs_json_rejects_action_flags(tmp_path: Path) -> None:
    dag_file = tmp_path / "workflow.toml"
    dag_file.write_text("[dag]\nname='demo'\nmax_parallel=1\n", encoding="utf-8")
    parser = _build_parser()
    args = parser.parse_args(["runs", str(dag_file), "--resume", "--json"])

    try:
        _resolve_command_aliases(args)
    except ValueError as exc:
        assert "runs --json 仅支持状态查询" in str(exc)
    else:
        raise AssertionError("expected ValueError for runs --resume --json")
